use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;

use anyhow::{Context, Result};

use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn run_proxy(ctx: &SubsystemContext) -> Result<()> {
    let addr = format!("127.0.0.1:{}", ctx.config.agent.mcp_proxy_port);
    let listener = TcpListener::bind(&addr)
        .with_context(|| format!("Failed to bind MCP proxy listener on {addr}"))?;
    listener
        .set_nonblocking(true)
        .context("Failed setting MCP proxy listener non-blocking")?;

    let mut event = new_event("agent", "mcp_proxy_started", "info");
    event.detail = Some(format!("addr={addr}"));
    let _ = ctx.event_ledger.append(event);

    loop {
        match listener.accept() {
            Ok((stream, addr)) => {
                let peer = stream
                    .peer_addr()
                    .ok()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let mut event = new_event("agent", "mcp_proxy_client_connected", "info");
                event.detail = Some(format!("peer={peer}"));
                let _ = ctx.event_ledger.append(event);
                let upstream = resolve_upstream_addr();
                if let Err(e) =
                    handle_connection(stream, &upstream, &ctx.uhoh_dir, &ctx.event_ledger)
                {
                    let mut event = new_event("agent", "mcp_proxy_connection_failed", "warn");
                    event.detail = Some(format!("peer={}, error={}", addr, e));
                    let _ = ctx.event_ledger.append(event);
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => {
                let mut event = new_event("agent", "mcp_proxy_accept_failed", "warn");
                event.detail = Some(e.to_string());
                let _ = ctx.event_ledger.append(event);
                break;
            }
        }
    }
    Ok(())
}

fn handle_connection(
    mut client_stream: TcpStream,
    upstream_addr: &str,
    uhoh_dir: &Path,
    ledger: &crate::event_ledger::EventLedger,
) -> Result<()> {
    let mut upstream = TcpStream::connect(upstream_addr)
        .with_context(|| format!("Failed connecting MCP upstream: {upstream_addr}"))?;
    let mut client_reader = BufReader::new(client_stream.try_clone()?);
    let mut upstream_reader = BufReader::new(upstream.try_clone()?);

    let mut line = String::new();
    loop {
        line.clear();
        let n = client_reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }

        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) {
            if json
                .get("method")
                .and_then(|v| v.as_str())
                .map(|m| m == "tools/call")
                .unwrap_or(false)
            {
                intercept_tool_call(&json, uhoh_dir, ledger)?;
            }
        }

        upstream.write_all(line.as_bytes())?;
        upstream.flush()?;

        let mut response = String::new();
        let n = upstream_reader.read_line(&mut response)?;
        if n == 0 {
            break;
        }

        client_stream.write_all(response.as_bytes())?;
        client_stream.flush()?;
    }
    Ok(())
}

fn intercept_tool_call(
    call: &serde_json::Value,
    uhoh_dir: &Path,
    ledger: &crate::event_ledger::EventLedger,
) -> Result<()> {
    let tool = call
        .pointer("/params/name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown-tool");
    let args = call
        .pointer("/params/arguments")
        .cloned()
        .unwrap_or_default();

    let mut pre_state_ref = None;
    let mut path = None;
    if matches!(tool, "write" | "apply_patch") {
        let candidate_path = args
            .get("path")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| {
                args.get("file")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
            });
        if let Some(candidate_path) = candidate_path {
            path = Some(candidate_path.clone());
            let p = Path::new(&candidate_path);
            if p.exists() {
                let content = std::fs::read(p)?;
                let (hash, _) = crate::cas::store_blob(&uhoh_dir.join("blobs"), &content)?;
                pre_state_ref = Some(hash);
            }
        }
    }

    let mut event = new_event("agent", "tool_call", "info");
    event.path = path;
    event.pre_state_ref = pre_state_ref;
    event.detail = Some(
        serde_json::json!({
            "tool": tool,
            "args": args,
        })
        .to_string(),
    );
    let _ = ledger.append(event);
    Ok(())
}

fn resolve_upstream_addr() -> String {
    std::env::var("UHOH_AGENT_MCP_UPSTREAM").unwrap_or_else(|_| "127.0.0.1:22824".to_string())
}
