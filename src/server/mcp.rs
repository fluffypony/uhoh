use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::{json, Value};

use super::AppState;
use crate::mcp_protocol::{dispatch_protocol_request, JsonRpcRequest, JsonRpcResponse, ProtocolAction};

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

    match dispatch_protocol_request(request) {
        ProtocolAction::Notification => StatusCode::ACCEPTED.into_response(),
        ProtocolAction::Response(response) => (StatusCode::OK, Json(response)).into_response(),
        ProtocolAction::ToolsList { id } => {
            let response = crate::mcp_tools::tools_list_response(id);
            (StatusCode::OK, Json(response)).into_response()
        }
        ProtocolAction::ToolsCall { id, params } => {
            let response = handle_tools_call(state, id, params).await;
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}

async fn handle_tools_call(
    state: AppState,
    id: Option<Value>,
    params: Option<Value>,
) -> JsonRpcResponse {
    let request_id = id.clone();
    let tool_context = crate::mcp_tools::McpToolContext {
        database: state.database.clone(),
        uhoh_dir: state.uhoh_dir.clone(),
        config: state.config.clone(),
        event_tx: Some(state.event_tx.clone()),
        restore_in_progress: Some(crate::mcp_tools::RestoreInProgressFlag::Shared(
            state.restore_in_progress.clone(),
        )),
        restore_locks: Some(state.restore_locks.clone()),
    };

    let result = tokio::task::spawn_blocking(move || {
        crate::mcp_tools::tools_call_response(&tool_context, id, params)
    })
    .await;

    match result {
        Ok(response) => response,
        Err(err) => JsonRpcResponse::error(request_id, -32000, format!("Internal error: {err}")),
    }
}
