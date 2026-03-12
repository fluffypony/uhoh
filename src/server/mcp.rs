use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

use super::McpState;
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
    State(state): State<McpState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> axum::response::Response {
    if !super::auth::validate_host(&headers, state.port) {
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

    match crate::mcp_app::handle_json_rpc_request(state.application.clone(), request).await {
        crate::mcp_app::McpTransportResponse::Notification => StatusCode::ACCEPTED.into_response(),
        crate::mcp_app::McpTransportResponse::Response(response) => {
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}
