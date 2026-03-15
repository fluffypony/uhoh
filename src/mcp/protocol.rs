use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
#[non_exhaustive]
pub struct JsonRpcRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
#[non_exhaustive]
pub struct JsonRpcResponse {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonRpcError>,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
#[non_exhaustive]
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
        Self::error_with_data(id, code, message, None)
    }

    pub fn error_with_data(
        id: Option<Value>,
        code: i64,
        message: String,
        data: Option<Value>,
    ) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message,
                data,
            }),
            id,
        }
    }
}

pub enum ProtocolAction {
    Notification,
    Response(JsonRpcResponse),
    ToolsList {
        id: Option<Value>,
    },
    ToolsCall {
        id: Option<Value>,
        params: Option<Value>,
    },
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_request(method: &str, id: Option<Value>) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: method.to_string(),
            params: None,
            id,
        }
    }

    // ── JsonRpcResponse ──

    #[test]
    fn success_response_has_result() {
        let resp = JsonRpcResponse::success(Some(json!(1)), json!({"ok": true}));
        assert_eq!(resp.jsonrpc, "2.0");
        assert!(resp.result.is_some());
        assert!(resp.error.is_none());
        assert_eq!(resp.id, Some(json!(1)));
    }

    #[test]
    fn error_response_has_error() {
        let resp = JsonRpcResponse::error(Some(json!(1)), -32600, "bad".to_string());
        assert!(resp.result.is_none());
        assert!(resp.error.is_some());
        let err = resp.error.unwrap();
        assert_eq!(err.code, -32600);
        assert_eq!(err.message, "bad");
        assert!(err.data.is_none());
    }

    #[test]
    fn error_with_data_includes_data() {
        let resp = JsonRpcResponse::error_with_data(
            None,
            -32000,
            "oops".to_string(),
            Some(json!({"detail": "more info"})),
        );
        let err = resp.error.unwrap();
        assert!(err.data.is_some());
        assert_eq!(err.data.unwrap()["detail"], "more info");
    }

    #[test]
    fn success_serializes_without_error() {
        let resp = JsonRpcResponse::success(Some(json!(1)), json!("ok"));
        let json_str = serde_json::to_string(&resp).unwrap();
        assert!(json_str.contains("\"result\""));
        assert!(!json_str.contains("\"error\""));
    }

    #[test]
    fn error_serializes_without_result() {
        let resp = JsonRpcResponse::error(Some(json!(1)), -1, "fail".to_string());
        let json_str = serde_json::to_string(&resp).unwrap();
        assert!(json_str.contains("\"error\""));
        assert!(!json_str.contains("\"result\""));
    }

    // ── dispatch_protocol_request ──

    #[test]
    fn dispatch_notification_no_id() {
        let req = make_request("anything", None);
        assert!(matches!(dispatch_protocol_request(req), ProtocolAction::Notification));
    }

    #[test]
    fn dispatch_invalid_jsonrpc_version() {
        let req = JsonRpcRequest {
            jsonrpc: "1.0".to_string(),
            method: "initialize".to_string(),
            params: None,
            id: Some(json!(1)),
        };
        match dispatch_protocol_request(req) {
            ProtocolAction::Response(resp) => {
                assert!(resp.error.is_some());
                assert_eq!(resp.error.unwrap().code, -32600);
            }
            _ => panic!("Expected error response for invalid version"),
        }
    }

    #[test]
    fn dispatch_initialize() {
        let req = make_request("initialize", Some(json!(1)));
        match dispatch_protocol_request(req) {
            ProtocolAction::Response(resp) => {
                assert!(resp.result.is_some());
                let result = resp.result.unwrap();
                assert!(result["capabilities"]["tools"].is_object());
                assert_eq!(result["serverInfo"]["name"], "uhoh");
            }
            _ => panic!("Expected response for initialize"),
        }
    }

    #[test]
    fn dispatch_ping() {
        let req = make_request("ping", Some(json!(2)));
        match dispatch_protocol_request(req) {
            ProtocolAction::Response(resp) => {
                assert!(resp.result.is_some());
            }
            _ => panic!("Expected response for ping"),
        }
    }

    #[test]
    fn dispatch_tools_list() {
        let req = make_request("tools/list", Some(json!(3)));
        assert!(matches!(dispatch_protocol_request(req), ProtocolAction::ToolsList { .. }));
    }

    #[test]
    fn dispatch_tools_call() {
        let req = make_request("tools/call", Some(json!(4)));
        assert!(matches!(dispatch_protocol_request(req), ProtocolAction::ToolsCall { .. }));
    }

    #[test]
    fn dispatch_unknown_method() {
        let req = make_request("nonexistent/method", Some(json!(5)));
        match dispatch_protocol_request(req) {
            ProtocolAction::Response(resp) => {
                assert!(resp.error.is_some());
                assert_eq!(resp.error.unwrap().code, -32601);
            }
            _ => panic!("Expected error response for unknown method"),
        }
    }

    #[test]
    fn dispatch_notifications_initialized() {
        let req = make_request("notifications/initialized", Some(json!(6)));
        match dispatch_protocol_request(req) {
            ProtocolAction::Response(resp) => {
                assert!(resp.result.is_some());
            }
            _ => panic!("Expected response for notifications/initialized"),
        }
    }
}
