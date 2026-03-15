use crate::runtime_bundle::RuntimeBundle;

use super::protocol::{dispatch_protocol_request, JsonRpcRequest, JsonRpcResponse, ProtocolAction};
use super::tools::{mcp_tool_call_response, mcp_tools_list_response};

#[derive(Clone)]
#[non_exhaustive]
pub struct McpApplication {
    pub runtime: RuntimeBundle,
}

pub enum McpTransportResponse {
    Notification,
    Response(JsonRpcResponse),
}

pub fn build_application(runtime: RuntimeBundle) -> McpApplication {
    McpApplication { runtime }
}

pub async fn handle_json_rpc_request(
    application: McpApplication,
    request: JsonRpcRequest,
) -> McpTransportResponse {
    match dispatch_protocol_request(request) {
        ProtocolAction::Notification => McpTransportResponse::Notification,
        ProtocolAction::Response(response) => McpTransportResponse::Response(response),
        ProtocolAction::ToolsList { id } => {
            McpTransportResponse::Response(mcp_tools_list_response(id))
        }
        ProtocolAction::ToolsCall { id, params } => {
            let response = dispatch_mcp_tool_request(application, id, params).await;
            McpTransportResponse::Response(response)
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn mcp_transport_response_variants() {
        // Just verify the enum variants exist and are constructible
        let _notif = McpTransportResponse::Notification;
        let resp = super::super::protocol::JsonRpcResponse::success(None, serde_json::json!({}));
        let _response = McpTransportResponse::Response(resp);
    }
}

async fn dispatch_mcp_tool_request(
    application: McpApplication,
    id: Option<serde_json::Value>,
    params: Option<serde_json::Value>,
) -> JsonRpcResponse {
    let request_id = id.clone();
    let runtime = application.runtime.clone();
    let result =
        tokio::task::spawn_blocking(move || mcp_tool_call_response(&runtime, id, params)).await;
    match result {
        Ok(response) => response,
        Err(err) => JsonRpcResponse::error(request_id, -32000, format!("Internal error: {err}")),
    }
}
