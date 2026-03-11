use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

pub enum ProtocolAction {
    Notification,
    Response(JsonRpcResponse),
    ToolsList { id: Option<Value> },
    ToolsCall { id: Option<Value>, params: Option<Value> },
}

pub fn dispatch_protocol_request(request: JsonRpcRequest) -> ProtocolAction {
    if request.id.is_none() {
        return ProtocolAction::Notification;
    }

    if request.jsonrpc != "2.0" {
        return ProtocolAction::Response(JsonRpcResponse::error(
            request.id,
            -32600,
            "Invalid jsonrpc version".to_string(),
        ));
    }

    match request.method.as_str() {
        "initialize" => ProtocolAction::Response(JsonRpcResponse::success(
            request.id,
            json!({
                "protocolVersion": "2025-06-18",
                "capabilities": { "tools": {} },
                "serverInfo": {
                    "name": "uhoh",
                    "version": env!("CARGO_PKG_VERSION")
                }
            }),
        )),
        "ping" | "notifications/initialized" => {
            ProtocolAction::Response(JsonRpcResponse::success(request.id, json!({})))
        }
        "tools/list" => ProtocolAction::ToolsList { id: request.id },
        "tools/call" => ProtocolAction::ToolsCall {
            id: request.id,
            params: request.params,
        },
        _ => ProtocolAction::Response(JsonRpcResponse::error(
            request.id,
            -32601,
            format!("Method not found: {}", request.method),
        )),
    }
}
