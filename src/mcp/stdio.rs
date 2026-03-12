use anyhow::Result;
use serde_json::json;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::Arc;

use crate::db::Database;
use crate::runtime_bundle::{RuntimeBundle, RuntimeBundleConfig};

use super::{JsonRpcRequest, McpTransportResponse};

pub fn run_stdio_server(uhoh_dir: &Path) -> Result<()> {
    let config = crate::config::Config::load(&uhoh_dir.join("config.toml"))?;
    let database = Database::open(&uhoh_dir.join("uhoh.db"))?;
    let database_handle = Arc::new(database.clone_handle());
    let runtime_handle = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let runtime = RuntimeBundle::new(RuntimeBundleConfig {
        database: database_handle,
        uhoh_dir: uhoh_dir.to_path_buf(),
        config,
        event_tx: None,
        restore_coordinator: None,
    });
    let application = super::build_application(runtime);
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

        match runtime_handle.block_on(super::handle_json_rpc_request(application.clone(), request))
        {
            McpTransportResponse::Notification => continue,
            McpTransportResponse::Response(response) => {
                writeln!(stdout, "{}", serde_json::to_string(&response)?)?;
                stdout.flush()?;
            }
        }
    }

    Ok(())
}
