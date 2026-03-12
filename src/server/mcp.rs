use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

use super::AppState;
use crate::mcp_protocol::{JsonRpcRequest, JsonRpcResponse};

pub async fn mcp_get_not_supported() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
            "error": "MCP HTTP transport supports POST only",
            "details": "GET SSE streams and session endpoints are not implemented"
        })),
    )
}

pub async fn mcp_delete_not_supported() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
            "error": "MCP HTTP transport supports POST only",
            "details": "Session termination via DELETE is not implemented"
        })),
    )
}

pub async fn handle_mcp(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> axum::response::Response {
    if !super::auth::validate_host(&headers, state.config.server.port) {
        return (
            StatusCode::FORBIDDEN,
            Json(JsonRpcResponse::error(
                request.id,
                -32600,
                "Invalid Host header".to_string(),
            )),
        )
            .into_response();
    }

    if !super::auth::validate_origin(&headers) {
        return (
            StatusCode::FORBIDDEN,
            Json(JsonRpcResponse::error(
                request.id,
                -32600,
                "Invalid Origin header".to_string(),
            )),
        )
            .into_response();
    }

    let runtime = crate::mcp_tools::McpRuntime {
        tools: crate::mcp_tools::McpToolContext {
            database: state.database.clone(),
            uhoh_dir: state.uhoh_dir.clone(),
            config: state.config.clone(),
            event_tx: Some(state.event_tx.clone()),
            restore_in_progress: Some(crate::mcp_tools::RestoreInProgressFlag::Shared(
                state.restore_in_progress.clone(),
            )),
            restore_locks: Some(state.restore_locks.clone()),
        },
        executor: crate::mcp_tools::McpToolExecutor::SpawnBlocking,
    };

    match crate::mcp_tools::handle_json_rpc_request(runtime, request).await {
        crate::mcp_tools::McpTransportResponse::Notification => StatusCode::ACCEPTED.into_response(),
        crate::mcp_tools::McpTransportResponse::Response(response) => {
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}
