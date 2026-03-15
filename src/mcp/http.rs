use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde_json::json;

use super::{protocol::JsonRpcRequest, McpApplication};

#[derive(Clone)]
#[non_exhaustive]
pub struct McpHttpState {
    pub application: McpApplication,
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::response::IntoResponse;

    #[tokio::test]
    async fn get_not_supported_returns_method_not_allowed() {
        let response = get_not_supported().await.into_response();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn delete_not_supported_returns_method_not_allowed() {
        let response = delete_not_supported().await.into_response();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
}

pub async fn handle_http_request(
    State(state): State<McpHttpState>,
    _headers: axum::http::HeaderMap,
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
