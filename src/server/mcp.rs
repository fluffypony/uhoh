use axum::{extract::State, http::StatusCode, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::AppState;

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

#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
pub struct JsonRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl JsonRpcResponse {
    pub fn success(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub fn error(id: Option<Value>, code: i64, message: String) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data: None,
            }),
            id,
        }
    }
}

pub async fn handle_mcp(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    Json(request): Json<JsonRpcRequest>,
) -> axum::response::Response {
    // JSON-RPC notifications (no id) must not receive JSON-RPC responses.
    // Check this first so invalid host/origin/jsonrpc metadata on notifications
    // does not produce `id: null` error payloads.
    if request.id.is_none() {
        return StatusCode::ACCEPTED.into_response();
    }

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

    if request.jsonrpc != "2.0" {
        return (
            StatusCode::BAD_REQUEST,
            Json(JsonRpcResponse::error(
                request.id,
                -32600,
                "Invalid jsonrpc version".to_string(),
            )),
        )
            .into_response();
    }

    let response = match request.method.as_str() {
        "initialize" => JsonRpcResponse::success(
            request.id,
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "uhoh",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        ),
        "ping" => JsonRpcResponse::success(request.id, json!({})),
        "notifications/initialized" => JsonRpcResponse::success(request.id, json!({})),
        "tools/list" => JsonRpcResponse::success(request.id, crate::mcp_tools::tool_definitions()),
        "tools/call" => handle_tools_call(state, request.id, request.params).await,
        _ => JsonRpcResponse::error(
            request.id,
            -32601,
            format!("Method not found: {}", request.method),
        ),
    };

    (StatusCode::OK, Json(response)).into_response()
}

async fn handle_tools_call(
    state: AppState,
    id: Option<Value>,
    params: Option<Value>,
) -> JsonRpcResponse {
    let (tool_name, args) = match crate::mcp_tools::parse_tool_call(params) {
        Ok(parsed) => parsed,
        Err(err) => return JsonRpcResponse::error(id, err.code, err.message),
    };

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
        crate::mcp_tools::dispatch_tool_call(&tool_context, &tool_name, args)
    })
    .await;

    match result {
        Ok(Ok(value)) => JsonRpcResponse::success(id, value),
        Ok(Err(err)) => JsonRpcResponse::error(id, err.code, err.message),
        Err(err) => JsonRpcResponse::error(id, -32000, format!("Internal error: {err}")),
    }
}
