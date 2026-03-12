use std::path::PathBuf;
use std::sync::Arc;

use crate::config::Config;
use crate::db::Database;
use crate::mcp_protocol::{
    dispatch_protocol_request, JsonRpcRequest, JsonRpcResponse, ProtocolAction,
};
use crate::server::events::ServerEvent;

#[derive(Clone, Copy)]
pub enum McpExecutor {
    Inline,
    SpawnBlocking,
}

#[derive(Clone)]
pub struct McpApplication {
    pub tools: crate::mcp_tools::McpToolContext,
    pub executor: McpExecutor,
}

pub enum McpTransportResponse {
    Notification,
    Response(JsonRpcResponse),
}

pub fn build_application(
    database: Arc<Database>,
    uhoh_dir: PathBuf,
    config: Config,
    event_tx: Option<tokio::sync::broadcast::Sender<ServerEvent>>,
    restore_coordinator: Option<crate::restore_runtime::RestoreCoordinator>,
    executor: McpExecutor,
) -> McpApplication {
    let restore_runtime = build_restore_runtime(
        database.clone(),
        uhoh_dir.clone(),
        event_tx.clone(),
        restore_coordinator,
    );
    McpApplication {
        tools: crate::mcp_tools::McpToolContext {
            database,
            uhoh_dir,
            config,
            event_tx,
            restore_runtime,
        },
        executor,
    }
}

pub async fn handle_json_rpc_request(
    application: McpApplication,
    request: JsonRpcRequest,
) -> McpTransportResponse {
    match dispatch_protocol_request(request) {
        ProtocolAction::Notification => McpTransportResponse::Notification,
        ProtocolAction::Response(response) => McpTransportResponse::Response(response),
        ProtocolAction::ToolsList { id } => {
            McpTransportResponse::Response(crate::mcp_tools::tools_list_response(id))
        }
        ProtocolAction::ToolsCall { id, params } => {
            let response = handle_tools_call(application, id, params).await;
            McpTransportResponse::Response(response)
        }
    }
}

async fn handle_tools_call(
    application: McpApplication,
    id: Option<serde_json::Value>,
    params: Option<serde_json::Value>,
) -> JsonRpcResponse {
    match application.executor {
        McpExecutor::Inline => {
            crate::mcp_tools::tools_call_response(&application.tools, id, params)
        }
        McpExecutor::SpawnBlocking => {
            let request_id = id.clone();
            let result = tokio::task::spawn_blocking(move || {
                crate::mcp_tools::tools_call_response(&application.tools, id, params)
            })
            .await;
            match result {
                Ok(response) => response,
                Err(err) => {
                    JsonRpcResponse::error(request_id, -32000, format!("Internal error: {err}"))
                }
            }
        }
    }
}

fn build_restore_runtime(
    database: Arc<Database>,
    uhoh_dir: PathBuf,
    event_tx: Option<tokio::sync::broadcast::Sender<ServerEvent>>,
    restore_coordinator: Option<crate::restore_runtime::RestoreCoordinator>,
) -> crate::restore_runtime::RestoreRuntime {
    let runtime = crate::restore_runtime::RestoreRuntime::new(database, uhoh_dir);
    let runtime = if let Some(event_tx) = event_tx {
        runtime.with_event_tx(event_tx)
    } else {
        runtime
    };
    if let Some(coordinator) = restore_coordinator {
        runtime.with_coordinator(coordinator)
    } else {
        runtime
    }
}
