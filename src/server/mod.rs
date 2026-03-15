pub mod api;
pub mod auth;
pub(crate) mod transport_security;
pub mod ws;

use anyhow::Result;
use axum::{
    extract::{Extension, FromRef, State},
    http::Method,
    middleware,
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::Mutex;

use crate::config::ServerConfig;
use crate::db::Database;
use crate::events::ServerEvent;
use crate::restore::RestoreCoordinator;
use crate::runtime_bundle::{RuntimeBundle, RuntimeBundleConfig};
use crate::subsystem::{SubsystemHealth, SubsystemManager};
use crate::server::transport_security::TransportSecurityPolicy;
use auth::{auth_middleware, host_validation_middleware, AuthToken};

#[non_exhaustive]
pub struct ServerBootstrap {
    pub config: ServerConfig,
    pub full_config: crate::config::Config,
    pub database: Arc<Database>,
    pub uhoh_dir: PathBuf,
    pub event_tx: broadcast::Sender<ServerEvent>,
    pub restore_coordinator: RestoreCoordinator,
    pub sidecar_manager: crate::ai::SidecarManager,
    pub subsystem_manager: Arc<Mutex<SubsystemManager>>,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct AppState {
    pub api: ApiState,
    pub health: HealthState,
    pub mcp: crate::mcp::McpHttpState,
    pub ws: WsState,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct ApiState {
    pub runtime: RuntimeBundle,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct HealthState {
    pub subsystem_manager: Arc<Mutex<SubsystemManager>>,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct WsState {
    pub event_tx: broadcast::Sender<ServerEvent>,
    pub transport_policy: TransportSecurityPolicy,
    pub cached_token: Option<String>,
}

impl FromRef<AppState> for ApiState {
    fn from_ref(input: &AppState) -> Self {
        input.api.clone()
    }
}

impl FromRef<AppState> for HealthState {
    fn from_ref(input: &AppState) -> Self {
        input.health.clone()
    }
}

impl FromRef<AppState> for crate::mcp::McpHttpState {
    fn from_ref(input: &AppState) -> Self {
        input.mcp.clone()
    }
}

impl FromRef<AppState> for WsState {
    fn from_ref(input: &AppState) -> Self {
        input.ws.clone()
    }
}

pub async fn start_server(bootstrap: ServerBootstrap) -> Result<tokio::task::JoinHandle<()>> {
    let ServerBootstrap {
        config,
        full_config,
        database,
        uhoh_dir,
        event_tx,
        restore_coordinator,
        sidecar_manager,
        subsystem_manager,
    } = bootstrap;
    // Reuse existing token if present, only generate on first run
    let token_path = uhoh_dir.join("server.token");
    let auth_token = if token_path.exists() {
        let existing = std::fs::read_to_string(&token_path).unwrap_or_default();
        let trimmed = existing.trim().to_string();
        if trimmed.is_empty() {
            let new_token = auth::generate_token();
            auth::write_token_file(&uhoh_dir, &new_token)?;
            new_token
        } else {
            trimmed
        }
    } else {
        let new_token = auth::generate_token();
        auth::write_token_file(&uhoh_dir, &new_token)?;
        new_token
    };

    let transport_policy = TransportSecurityPolicy::from_server_config(&config);
    let runtime = RuntimeBundle::new(RuntimeBundleConfig {
        database,
        uhoh_dir: uhoh_dir.clone(),
        config: full_config,
        event_tx: Some(event_tx.clone()),
        restore_coordinator,
        sidecar_manager: Some(sidecar_manager),
    });
    let state = AppState {
        api: ApiState {
            runtime: runtime.clone(),
        },
        health: HealthState {
            subsystem_manager: subsystem_manager.clone(),
        },
        mcp: crate::mcp::McpHttpState {
            application: crate::mcp::build_application(runtime.clone()),
        },
        ws: WsState {
            event_tx,
            transport_policy,
            cached_token: Some(auth_token.clone()),
        },
    };

    let app = build_app(state.clone(), &config, &auth_token, transport_policy);

    let bind_addr = format!("{}:{}", config.bind_address, config.port);
    let addr: SocketAddr = bind_addr
        .parse()
        .map_err(|_| anyhow::anyhow!("Invalid bind address"))?;
    if !addr.ip().is_loopback() {
        anyhow::bail!("Server must bind to loopback for security");
    }

    let handle = tokio::spawn(async move {
        let listener = match tokio::net::TcpListener::bind(addr).await {
            Ok(listener) => {
                let _ = auth::write_port_file(state.api.runtime.uhoh_dir(), addr.port());
                listener
            }
            Err(e) => {
                tracing::warn!("Could not bind server: {}", e);
                return;
            }
        };

        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("Server error: {}", e);
        }
    });

    Ok(handle)
}

fn build_app(
    state: AppState,
    config: &ServerConfig,
    auth_token: &str,
    transport_policy: TransportSecurityPolicy,
) -> Router {
    let mut app = Router::new();
    if config.mcp_enabled {
        app = app
            .route("/mcp", post(crate::mcp::handle_http_request))
            .route("/mcp", get(crate::mcp::get_not_supported))
            .route(
                "/mcp",
                axum::routing::delete(crate::mcp::delete_not_supported),
            );
    }

    app = app
        .route("/api/v1/projects", get(api::list_projects))
        .route(
            "/api/v1/projects/{hash}/snapshots",
            get(api::list_snapshots).post(api::create_snapshot),
        )
        .route(
            "/api/v1/projects/{hash}/snapshots/{id}",
            get(api::get_snapshot).post(api::set_snapshot_pin),
        )
        .route(
            "/api/v1/projects/{hash}/snapshots/{id}/files",
            get(api::get_snapshot_files),
        )
        .route(
            "/api/v1/projects/{hash}/snapshots/{id}/diff",
            get(api::get_diff),
        )
        .route(
            "/api/v1/projects/{hash}/snapshots/{id}/file/{*path}",
            get(api::get_file_content),
        )
        .route(
            "/api/v1/projects/{hash}/restore/{id}",
            post(api::restore_snapshot),
        )
        .route("/api/v1/search", get(api::search))
        .route("/api/v1/projects/{hash}/timeline", get(api::get_timeline))
        .route("/ws", get(ws::websocket_handler))
        .route("/health", get(health_check))
        .route("/api/v1/health", get(health_check));

    if config.ui_enabled {
        app = app.fallback(get(serve_ui));
    }

    app = app.route_layer(middleware::from_fn_with_state(
        transport_policy,
        host_validation_middleware,
    ));

    // Apply Origin validation to HTTP API and MCP surfaces for browser-originated
    // requests. Requests without Origin remain allowed for local CLI/agent clients.
    app = app.route_layer(middleware::from_fn_with_state(
        transport_policy,
        origin_validation_middleware,
    ));

    if config.require_auth || config.mcp_require_auth {
        app = app
            .route_layer(middleware::from_fn_with_state(
                transport_policy,
                auth_middleware,
            ))
            .layer(Extension(AuthToken(auth_token.to_string())));
    }
    app.with_state(state)
}

async fn health_check(State(state): State<HealthState>) -> axum::Json<serde_json::Value> {
    // Acquire the manager lock, take the snapshot, and drop the lock before
    // doing any further processing to avoid holding it across awaits.
    let snapshot = {
        let manager = state.subsystem_manager.lock().await;
        manager.health_snapshot().await
    };

    let subsystems = snapshot
        .into_iter()
        .map(|(name, health)| {
            let status = match health {
                SubsystemHealth::Healthy => "healthy".to_string(),
                SubsystemHealth::HealthyWithAudit(source) => {
                    format!("healthy:audit={}", audit_source_label(&source))
                }
                SubsystemHealth::Degraded(msg) => format!("degraded:{msg}"),
                SubsystemHealth::DegradedWithAudit { message, source } => {
                    format!("degraded:{message};audit={}", audit_source_label(&source))
                }
                SubsystemHealth::Failed(msg) => format!("failed:{msg}"),
            };
            serde_json::json!({"name": name, "status": status})
        })
        .collect::<Vec<_>>();

    axum::Json(serde_json::json!({
        "status": "ok",
        "service": "uhoh",
        "version": env!("CARGO_PKG_VERSION"),
        "subsystems": subsystems,
    }))
}

async fn serve_ui(request: axum::extract::Request) -> axum::response::Response {
    use axum::response::IntoResponse;
    // Return 404 JSON for unknown API paths instead of serving HTML
    if request.uri().path().starts_with("/api/") {
        return (
            axum::http::StatusCode::NOT_FOUND,
            axum::Json(serde_json::json!({"error": "Not found"})),
        )
            .into_response();
    }
    let html = include_str!("../../assets/timemachine.html");
    axum::response::Html(html).into_response()
}

fn audit_source_label(source: &crate::subsystem::AuditSource) -> &'static str {
    match source {
        crate::subsystem::AuditSource::None => "none",
        crate::subsystem::AuditSource::Fanotify => "fanotify",
    }
}

async fn origin_validation_middleware(
    State(transport_policy): State<TransportSecurityPolicy>,
    headers: axum::http::HeaderMap,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = request.uri().path();
    let method = request.method();
    let should_validate =
        path.starts_with("/api/") || path == "/mcp" || (path == "/ws" && *method == Method::GET);

    if should_validate && !transport_policy.validate_origin(&headers) {
        return (
            axum::http::StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({ "error": "Invalid Origin header" })),
        )
            .into_response();
    }

    next.run(request).await
}

#[cfg(test)]
mod tests {
    use super::{build_app, ApiState, AppState, HealthState, WsState};
    use axum::body::{to_bytes, Body};
    use axum::http::header::{AUTHORIZATION, CONTENT_TYPE, HOST, ORIGIN};
    use axum::http::HeaderValue;
    use axum::http::{Method, Request, StatusCode};
    use serde_json::{json, Value};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::sync::{broadcast, Mutex};
    use tower::ServiceExt;

    use crate::db::Database;
    use crate::runtime_bundle::{RuntimeBundle, RuntimeBundleConfig};
    use crate::subsystem::SubsystemManager;
    use crate::server::transport_security::TransportSecurityPolicy;

    fn test_app() -> (TempDir, crate::config::Config, axum::Router) {
        let temp = tempfile::tempdir().expect("tempdir");
        let database = Arc::new(Database::open(&temp.path().join("uhoh.db")).expect("open db"));
        let (event_tx, _) = broadcast::channel(8);
        let config = crate::config::Config::default();
        let transport_policy = TransportSecurityPolicy::from_server_config(&config.server);
        let runtime = RuntimeBundle::new(RuntimeBundleConfig {
            database,
            uhoh_dir: temp.path().to_path_buf(),
            config: config.clone(),
            event_tx: Some(event_tx.clone()),
            restore_coordinator: crate::restore::RestoreCoordinator::new(),
            sidecar_manager: None,
        });
        let state = AppState {
            api: ApiState {
                runtime: runtime.clone(),
            },
            health: HealthState {
                subsystem_manager: Arc::new(Mutex::new(SubsystemManager::new(
                    3,
                    Duration::from_secs(60),
                ))),
            },
            mcp: crate::mcp::McpHttpState {
                application: crate::mcp::build_application(runtime.clone()),
            },
            ws: WsState {
                event_tx,
                transport_policy,
                cached_token: Some("test-token".to_string()),
            },
        };
        let app = build_app(state, &config.server, "test-token", transport_policy);
        (temp, config, app)
    }

    async fn response_json(response: axum::response::Response) -> Value {
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        serde_json::from_slice(&bytes).expect("json response")
    }

    fn request(method: Method, uri: &str, host: &str, body: Body) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header(HOST, host)
            .body(body)
            .expect("request")
    }

    #[tokio::test]
    async fn router_health_endpoint_allows_loopback_and_rejects_bad_host() {
        let (_temp, config, app) = test_app();
        let host = format!("127.0.0.1:{}", config.server.port);

        let response = app
            .clone()
            .oneshot(request(Method::GET, "/health", &host, Body::empty()))
            .await
            .expect("health response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response_json(response).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["service"], "uhoh");

        let forbidden = app
            .oneshot(request(
                Method::GET,
                "/health",
                "evil.example",
                Body::empty(),
            ))
            .await
            .expect("forbidden response");
        assert_eq!(forbidden.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn router_origin_validation_covers_api_ws_and_mcp() {
        let (_temp, config, app) = test_app();
        let host = format!("127.0.0.1:{}", config.server.port);

        let mut api_request = request(Method::GET, "/api/v1/projects", &host, Body::empty());
        api_request
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static("http://evil.example"));
        let api = app
            .clone()
            .oneshot(api_request)
            .await
            .expect("api response");
        assert_eq!(api.status(), StatusCode::FORBIDDEN);

        let mut ws_request = request(Method::GET, "/ws", &host, Body::empty());
        ws_request
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static("http://evil.example"));
        let ws = app.clone().oneshot(ws_request).await.expect("ws response");
        assert_eq!(ws.status(), StatusCode::FORBIDDEN);

        let mut mcp_request = request(
            Method::POST,
            "/mcp",
            &host,
            Body::from(
                serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize"
                }))
                .unwrap(),
            ),
        );
        mcp_request
            .headers_mut()
            .insert(ORIGIN, HeaderValue::from_static("http://evil.example"));
        mcp_request
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer test-token"));
        mcp_request
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let mcp = app.oneshot(mcp_request).await.expect("mcp response");
        assert_eq!(mcp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn router_requires_auth_for_writes_but_allows_reads() {
        let (_temp, config, app) = test_app();
        let host = format!("127.0.0.1:{}", config.server.port);

        let read = app
            .clone()
            .oneshot(request(
                Method::GET,
                "/api/v1/projects",
                &host,
                Body::empty(),
            ))
            .await
            .expect("read response");
        assert_eq!(read.status(), StatusCode::OK);

        let unauthorized = app
            .clone()
            .oneshot(request(
                Method::POST,
                "/api/v1/projects/missing/restore/1",
                &host,
                Body::empty(),
            ))
            .await
            .expect("unauthorized response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let mut authorized_request = request(
            Method::POST,
            "/api/v1/projects/missing/restore/1",
            &host,
            Body::empty(),
        );
        authorized_request
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer test-token"));
        let authorized = app
            .oneshot(authorized_request)
            .await
            .expect("authorized response");
        assert_ne!(authorized.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn router_ui_fallback_and_api_404_are_behaviorally_tested() {
        let (_temp, config, app) = test_app();
        let host = format!("127.0.0.1:{}", config.server.port);

        let ui = app
            .clone()
            .oneshot(request(Method::GET, "/timeline", &host, Body::empty()))
            .await
            .expect("ui response");
        assert_eq!(ui.status(), StatusCode::OK);
        let content_type = ui
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default();
        assert!(content_type.starts_with("text/html"));

        let api_missing = app
            .oneshot(request(Method::GET, "/api/unknown", &host, Body::empty()))
            .await
            .expect("api missing response");
        assert_eq!(api_missing.status(), StatusCode::NOT_FOUND);
        let body = response_json(api_missing).await;
        assert_eq!(body["error"], "Not found");
    }

    #[tokio::test]
    async fn router_mcp_transport_handles_initialize_and_method_limits() {
        let (_temp, config, app) = test_app();
        let host = format!("127.0.0.1:{}", config.server.port);

        let mut get_request = request(Method::GET, "/mcp", &host, Body::empty());
        get_request
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer test-token"));
        let get = app
            .clone()
            .oneshot(get_request)
            .await
            .expect("mcp get response");
        assert_eq!(get.status(), StatusCode::METHOD_NOT_ALLOWED);
        let get_body = response_json(get).await;
        assert_eq!(get_body["error"], "MCP HTTP transport supports POST only");

        let mut delete_request = request(Method::DELETE, "/mcp", &host, Body::empty());
        delete_request
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer test-token"));
        let delete = app
            .clone()
            .oneshot(delete_request)
            .await
            .expect("mcp delete response");
        assert_eq!(delete.status(), StatusCode::METHOD_NOT_ALLOWED);

        let mut initialize_request = request(
            Method::POST,
            "/mcp",
            &host,
            Body::from(
                serde_json::to_vec(&json!({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize"
                }))
                .unwrap(),
            ),
        );
        initialize_request
            .headers_mut()
            .insert(AUTHORIZATION, HeaderValue::from_static("Bearer test-token"));
        initialize_request
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        let initialize = app
            .oneshot(initialize_request)
            .await
            .expect("mcp initialize response");
        assert_eq!(initialize.status(), StatusCode::OK);
        let body = response_json(initialize).await;
        assert_eq!(body["result"]["protocolVersion"], "2025-06-18");
        assert_eq!(body["result"]["serverInfo"]["name"], "uhoh");
    }
}
