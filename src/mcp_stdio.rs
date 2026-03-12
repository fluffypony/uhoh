use anyhow::Result;
use serde_json::json;
use std::io::{self, BufRead, Write};
use std::sync::Arc;

use crate::config::Config;
use crate::db::Database;
use crate::mcp_protocol::JsonRpcRequest;

pub fn run_stdio_mcp(config: &Config) -> Result<()> {
    let uhoh_dir = crate::uhoh_dir();
    let database = Database::open(&uhoh_dir.join("uhoh.db"))?;
    let database_handle = Arc::new(database.clone_handle());
    let runtime_handle = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let application = crate::mcp_app::build_application(
        database_handle,
        uhoh_dir.clone(),
        config.clone(),
        None,
        None,
        crate::mcp_app::McpExecutor::Inline,
    );
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();

    for line in stdin.lock().lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let request: JsonRpcRequest = match serde_json::from_str(&line) {
            Ok(req) => req,
            Err(e) => {
                let parse_error = json!({
                    "jsonrpc": "2.0",
                    "error": { "code": -32700, "message": format!("Parse error: {}", e) },
                    "id": null
                });
                writeln!(stdout, "{parse_error}")?;
                stdout.flush()?;
                continue;
            }
        };

        match runtime_handle.block_on(crate::mcp_app::handle_json_rpc_request(
            application.clone(),
            request,
        )) {
            crate::mcp_app::McpTransportResponse::Notification => continue,
            crate::mcp_app::McpTransportResponse::Response(response) => {
                writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
                stdout.flush()?;
            }
        }
    }

    Ok(())
}
