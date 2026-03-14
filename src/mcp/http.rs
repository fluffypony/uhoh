use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

use crate::transport_security::TransportSecurityPolicy;

use super::{
    protocol::{JsonRpcRequest, JsonRpcResponse},
    McpApplication,
};

#[derive(Clone)]
pub struct McpHttpState {
    pub application: McpApplication,
    pub transport_policy: TransportSecurityPolicy,
}

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
    State(state): State<McpHttpState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> axum::response::Response {
    // Host and Origin validation is handled by server middleware.

    match super::handle_json_rpc_request(state.application.clone(), request).await {
        super::McpTransportResponse::Notification => StatusCode::ACCEPTED.into_response(),
        super::McpTransportResponse::Response(response) => {
            (StatusCode::OK, Json(response)).into_response()
        }
    }
}
