pub mod api;
pub mod auth;
pub mod events;
pub mod mcp;
pub mod ws;

use anyhow::Result;
use axum::{
    extract::Extension,
    middleware,
    routing::{get, post},
    Router,
};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};

use crate::config::ServerConfig;
use crate::db::Database;
use auth::{auth_middleware, host_validation_middleware, AuthToken};
use events::ServerEvent;

#[derive(Clone)]
pub struct AppState {
    pub database: Arc<Database>,
    pub uhoh_dir: PathBuf,
    pub config: crate::config::Config,
    pub event_tx: broadcast::Sender<ServerEvent>,
    pub restore_in_progress: Arc<std::sync::atomic::AtomicBool>,
    pub restore_locks: Arc<tokio::sync::Mutex<std::collections::HashSet<String>>>,
}

pub async fn start_server(
    config: &ServerConfig,
    full_config: crate::config::Config,
    database: Arc<Database>,
    uhoh_dir: PathBuf,
    event_tx: broadcast::Sender<ServerEvent>,
    restore_in_progress: Arc<std::sync::atomic::AtomicBool>,
) -> Result<tokio::task::JoinHandle<()>> {
    let auth_token = auth::generate_token();
    auth::write_token_file(&uhoh_dir, &auth_token)?;

    let state = AppState {
        database,
        uhoh_dir: uhoh_dir.clone(),
        config: full_config,
        event_tx,
        restore_in_progress,
        restore_locks: Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::new())),
    };

    let mut app = Router::new();
    if config.mcp_enabled {
        app = app.route("/mcp", post(mcp::handle_mcp));
    }

    app = app
        .route("/api/v1/projects", get(api::list_projects))
        .route(
            "/api/v1/projects/:hash/snapshots",
            get(api::list_snapshots).post(api::create_snapshot),
        )
        .route(
            "/api/v1/projects/:hash/snapshots/:id/files",
            get(api::get_snapshot_files),
        )
        .route(
            "/api/v1/projects/:hash/snapshots/:id/diff",
            get(api::get_diff),
        )
        .route(
            "/api/v1/projects/:hash/snapshots/:id/file/*path",
            get(api::get_file_content),
        )
        .route(
            "/api/v1/projects/:hash/restore/:id",
            post(api::restore_snapshot),
        )
        .route("/api/v1/search", get(api::search))
        .route("/api/v1/projects/:hash/timeline", get(api::get_timeline))
        .route("/ws", get(ws::websocket_handler))
        .route("/health", get(health_check));

    if config.ui_enabled {
        app = app.fallback(get(serve_ui));
    }

    app = app.layer(
        CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any),
    );

    app = app.route_layer(middleware::from_fn_with_state(
        config.port,
        host_validation_middleware,
    ));

    if config.require_auth {
        app = app
            .layer(middleware::from_fn(auth_middleware))
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

async fn health_check() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ok",
        "service": "uhoh",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

async fn serve_ui() -> impl axum::response::IntoResponse {
    let html = include_str!("../../assets/timemachine.html");
    axum::response::Html(html)
}
