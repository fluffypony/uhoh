use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

use crate::server::McpState;

use super::protocol::{JsonRpcRequest, JsonRpcResponse};

pub async fn get_not_supported() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
            "error": "MCP HTTP transport supports POST only",
            "details": "GET SSE streams and session endpoints are not implemented"
        })),
    )
}

pub async fn delete_not_supported() -> impl IntoResponse {
    (
        StatusCode::METHOD_NOT_ALLOWED,
        Json(json!({
            "error": "MCP HTTP transport supports POST only",
            "details": "Session termination via DELETE is not implemented"
        })),
    )
}

pub async fn handle_http_request(
    State(state): State<McpState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> axum::response::Response {
    if !crate::server::auth::validate_host(&headers, state.port) {
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

    if !crate::server::auth::validate_origin(&headers) {
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

    match super::handle_json_rpc_request(state.application.clone(), request).await {
        super::McpTransportResponse::Notification => StatusCode::ACCEPTED.into_response(),
        super::McpTransportResponse::Response(response) => {
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}
