pub mod api;
pub mod auth;
pub mod events;
pub mod mcp;
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
use crate::subsystem::{SubsystemHealth, SubsystemManager};
use auth::{auth_middleware, host_validation_middleware, AuthConfig, AuthToken};
use events::ServerEvent;

#[derive(Clone)]
pub struct AppState {
    pub database: Arc<Database>,
    pub uhoh_dir: PathBuf,
    pub config: crate::config::Config,
    pub event_tx: broadcast::Sender<ServerEvent>,
    pub restore_in_progress: Arc<std::sync::atomic::AtomicBool>,
    pub restore_locks: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    pub subsystem_manager: Arc<Mutex<SubsystemManager>>,
    /// Cached server auth token, read once at startup.
    pub cached_token: Option<String>,
}

#[derive(Clone)]
pub struct ApiState {
    pub database: Arc<Database>,
    pub uhoh_dir: PathBuf,
    pub config: crate::config::Config,
    pub event_tx: broadcast::Sender<ServerEvent>,
    pub restore_in_progress: Arc<std::sync::atomic::AtomicBool>,
    pub restore_locks: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

#[derive(Clone)]
pub struct HealthState {
    pub subsystem_manager: Arc<Mutex<SubsystemManager>>,
}

#[derive(Clone)]
pub struct McpState {
    pub port: u16,
    pub runtime: crate::mcp_tools::McpRuntime,
}

#[derive(Clone)]
pub struct WsState {
    pub event_tx: broadcast::Sender<ServerEvent>,
    pub require_auth: bool,
    pub cached_token: Option<String>,
}

impl FromRef<AppState> for ApiState {
    fn from_ref(input: &AppState) -> Self {
        Self {
            database: input.database.clone(),
            uhoh_dir: input.uhoh_dir.clone(),
            config: input.config.clone(),
            event_tx: input.event_tx.clone(),
            restore_in_progress: input.restore_in_progress.clone(),
            restore_locks: input.restore_locks.clone(),
        }
    }
}

impl FromRef<AppState> for HealthState {
    fn from_ref(input: &AppState) -> Self {
        Self {
            subsystem_manager: input.subsystem_manager.clone(),
        }
    }
}

impl FromRef<AppState> for McpState {
    fn from_ref(input: &AppState) -> Self {
        Self {
            port: input.config.server.port,
            runtime: crate::mcp_tools::McpRuntime {
                tools: crate::mcp_tools::McpToolContext {
                    database: input.database.clone(),
                    uhoh_dir: input.uhoh_dir.clone(),
                    config: input.config.clone(),
                    event_tx: Some(input.event_tx.clone()),
                    restore_in_progress: Some(crate::mcp_tools::RestoreInProgressFlag::Shared(
                        input.restore_in_progress.clone(),
                    )),
                    restore_locks: Some(input.restore_locks.clone()),
                },
                executor: crate::mcp_tools::McpToolExecutor::SpawnBlocking,
            },
        }
    }
}

impl FromRef<AppState> for WsState {
    fn from_ref(input: &AppState) -> Self {
        Self {
            event_tx: input.event_tx.clone(),
            require_auth: input.config.server.require_auth,
            cached_token: input.cached_token.clone(),
        }
    }
}

pub async fn start_server(
    config: &ServerConfig,
    full_config: crate::config::Config,
    database: Arc<Database>,
    uhoh_dir: PathBuf,
    event_tx: broadcast::Sender<ServerEvent>,
    restore_in_progress: Arc<std::sync::atomic::AtomicBool>,
    subsystem_manager: Arc<Mutex<SubsystemManager>>,
) -> Result<tokio::task::JoinHandle<()>> {
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

    let state = AppState {
        database,
        uhoh_dir: uhoh_dir.clone(),
        config: full_config,
        event_tx,
        restore_in_progress,
        restore_locks: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        subsystem_manager,
        cached_token: Some(auth_token.clone()),
    };

    let mut app = Router::new();
    if config.mcp_enabled {
        app = app
            .route("/mcp", post(mcp::handle_mcp))
            .route("/mcp", get(mcp::mcp_get_not_supported))
            .route("/mcp", axum::routing::delete(mcp::mcp_delete_not_supported));
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
        config.port,
        host_validation_middleware,
    ));

    // Apply Origin validation to HTTP API and MCP surfaces for browser-originated
    // requests. Requests without Origin remain allowed for local CLI/agent clients.
    app = app.route_layer(middleware::from_fn(origin_validation_middleware));

    if config.require_auth || config.mcp_require_auth {
        app = app
            .route_layer(middleware::from_fn_with_state(
                AuthConfig {
                    require_auth: config.require_auth,
                    mcp_require_auth: config.mcp_require_auth,
                },
                auth_middleware,
            ))
            .layer(Extension(AuthToken(auth_token.clone())));
    }
    let app = app.with_state(state.clone());

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
                let _ = auth::write_port_file(&state.uhoh_dir, addr.port());
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

async fn health_check(State(state): State<HealthState>) -> axum::Json<serde_json::Value> {
    let manager = state.subsystem_manager.lock().await;
    let subsystems = manager
        .health_snapshot()
        .await
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
    headers: axum::http::HeaderMap,
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let path = request.uri().path();
    let method = request.method();
    let should_validate =
        path.starts_with("/api/") || path == "/mcp" || (path == "/ws" && *method == Method::GET);

    if should_validate && !auth::validate_origin(&headers) {
        return (
            axum::http::StatusCode::FORBIDDEN,
            axum::Json(serde_json::json!({ "error": "Invalid Origin header" })),
        )
            .into_response();
    }

    next.run(request).await
}
