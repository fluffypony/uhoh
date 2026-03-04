use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rand::RngCore;
use subtle::ConstantTimeEq;
use zeroize::Zeroize;

use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

const PROXY_TOKEN_FILE: &str = "server.token";
const APPROVAL_RESPONSE_FILE_SUFFIX: &str = ".approved.json";

pub fn run_proxy(ctx: &SubsystemContext) -> Result<()> {
    let _ = ensure_proxy_token(&ctx.uhoh_dir)?;
    let addr = format!("127.0.0.1:{}", ctx.config.agent.mcp_proxy_port);
    let listener = TcpListener::bind(&addr)
        .with_context(|| format!("Failed to bind MCP proxy listener on {addr}"))?;

    let mut event = new_event("agent", "mcp_proxy_started", "info");
    event.detail = Some(format!("addr={addr}"));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append mcp_proxy_started event: {err}");
    }

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
                if let Err(err) = ctx.event_ledger.append(event) {
                    tracing::error!("failed to append mcp_proxy_client_connected event: {err}");
                }
                let upstream = resolve_upstream_addr();
                let uhoh_dir = ctx.uhoh_dir.clone();
                let event_ledger = ctx.event_ledger.clone();
                let config = ctx.config.clone();
                std::thread::spawn(move || {
                    if let Err(e) =
                        handle_connection(stream, &upstream, &uhoh_dir, &event_ledger, &config)
                    {
                        let mut event = new_event("agent", "mcp_proxy_connection_failed", "warn");
                        event.detail = Some(format!("peer={}, error={}", addr, e));
                        if let Err(err) = event_ledger.append(event) {
                            tracing::error!(
                                "failed to append mcp_proxy_connection_failed event: {err}"
                            );
                        }
                    }
                });
            }
            Err(e) => {
                let mut event = new_event("agent", "mcp_proxy_accept_failed", "warn");
                event.detail = Some(e.to_string());
                if let Err(err) = ctx.event_ledger.append(event) {
                    tracing::error!("failed to append mcp_proxy_accept_failed event: {err}");
                }
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
    config: &crate::config::Config,
) -> Result<()> {
    let mut upstream = TcpStream::connect(upstream_addr)
        .with_context(|| format!("Failed connecting MCP upstream: {upstream_addr}"))?;
    let mut client_reader = BufReader::new(client_stream.try_clone()?);
    let mut upstream_reader = BufReader::new(upstream.try_clone()?);

    let mut line = String::new();
    let mut authed = false;
    let expected_token = ensure_proxy_token(uhoh_dir)?;
    loop {
        line.clear();
        let n = client_reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }

        if !authed {
            authed = validate_auth_line(&line, &expected_token)?;
            if !authed {
                return Err(anyhow::anyhow!("MCP proxy authentication failed"));
            }
            continue;
        }

        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) {
            if json
                .get("method")
                .and_then(|v| v.as_str())
                .map(|m| m == "tools/call")
                .unwrap_or(false)
            {
                if intercept_tool_call(&json, uhoh_dir, ledger, config)? {
                    // Pause mode: hold and do not forward this call.
                    continue;
                }
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
    config: &crate::config::Config,
) -> Result<bool> {
    let session_id = call
        .pointer("/params/arguments/session_id")
        .or_else(|| call.pointer("/params/arguments/session"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
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

    let is_dangerous =
        is_dangerous_tool_call(tool, path.as_deref(), &config.agent.dangerous_patterns);
    if is_dangerous {
        let approval_id = format!("approval-{}", uuid_like_id());
        let challenge = random_hex(16);
        let approval_hmac =
            build_approval_response(&ensure_proxy_token(uhoh_dir)?, &approval_id, &challenge);
        let mut danger = new_event("agent", "dangerous_agent_action", "critical");
        danger.path = path.clone();
        danger.pre_state_ref = pre_state_ref.clone();
        danger.detail = Some(
            serde_json::json!({
                "approval_id": approval_id,
                "challenge": challenge,
                "session_id": session_id.clone(),
                "tool": tool,
                "args": args,
                "on_dangerous_change": config.agent.on_dangerous_change,
            })
            .to_string(),
        );
        if let Err(err) = ledger.append(danger) {
            tracing::error!("failed to append dangerous_agent_action event: {err}");
        }

        if config
            .agent
            .on_dangerous_change
            .eq_ignore_ascii_case("pause")
        {
            write_pending_approval(
                uhoh_dir,
                &approval_id,
                &challenge,
                tool,
                path.as_deref(),
                &args,
                config.agent.pause_timeout_seconds,
            )?;

            let approved = wait_for_approval(
                uhoh_dir,
                &approval_id,
                &approval_hmac,
                config.agent.pause_timeout_seconds,
                ledger,
            )?;
            if !approved {
                let mut timeout_event = new_event("agent", "dangerous_action_timeout", "warn");
                timeout_event.path = path.clone();
                timeout_event.detail = Some(
                    serde_json::json!({
                        "approval_id": approval_id,
                        "tool": tool,
                        "timeout_seconds": config.agent.pause_timeout_seconds,
                        "action": "auto_resumed",
                    })
                    .to_string(),
                );
                if let Err(err) = ledger.append(timeout_event) {
                    tracing::error!("failed to append dangerous_action_timeout event: {err}");
                }
            }
        }
    }

    let mut event = new_event("agent", "tool_call", "info");
    event.path = path;
    event.pre_state_ref = pre_state_ref;
    event.detail = Some(
        serde_json::json!({
            "session_id": session_id,
            "tool": tool,
            "args": args,
        })
        .to_string(),
    );
    if let Err(err) = ledger.append(event) {
        tracing::error!("failed to append tool_call event: {err}");
    }
    Ok(false)
}

fn is_dangerous_tool_call(tool: &str, path: Option<&str>, patterns: &[String]) -> bool {
    let tool_l = tool.to_ascii_lowercase();
    let path_l = path.unwrap_or_default().to_ascii_lowercase();
    patterns.iter().any(|pattern| {
        let p = pattern.trim().to_ascii_lowercase();
        if p.is_empty() {
            return false;
        }
        if let Some(raw) = p.strip_prefix("tool:") {
            return tool_l == raw.trim();
        }
        if let Some(raw) = p.strip_prefix("path:") {
            return path_l == raw.trim();
        }
        tool_l == p || path_l == p
    })
}

fn resolve_upstream_addr() -> String {
    std::env::var("UHOH_AGENT_MCP_UPSTREAM").unwrap_or_else(|_| "127.0.0.1:22824".to_string())
}

pub fn auth_handshake_line(token: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": "uhoh-auth",
        "method": "uhoh/auth",
        "params": {
            "token": token,
        }
    })
    .to_string()
}

#[derive(serde::Serialize)]
struct PendingApproval {
    approval_id: String,
    created_at: String,
    tool: String,
    path: Option<String>,
    timeout_seconds: u64,
    challenge: String,
    args: serde_json::Value,
}

#[derive(serde::Deserialize)]
struct ApprovalResponse {
    approval_id: String,
    response: String,
}

fn runtime_dir(uhoh_dir: &Path) -> PathBuf {
    uhoh_dir.join("agents").join("runtime")
}

fn pending_approval_path(uhoh_dir: &Path, approval_id: &str) -> PathBuf {
    runtime_dir(uhoh_dir).join(format!("{approval_id}.pending.json"))
}

fn approved_path(uhoh_dir: &Path, approval_id: &str) -> PathBuf {
    runtime_dir(uhoh_dir).join(format!("{approval_id}{APPROVAL_RESPONSE_FILE_SUFFIX}"))
}

fn write_pending_approval(
    uhoh_dir: &Path,
    approval_id: &str,
    challenge: &str,
    tool: &str,
    path: Option<&str>,
    args: &serde_json::Value,
    timeout_seconds: u64,
) -> Result<()> {
    let runtime = runtime_dir(uhoh_dir);
    ensure_secure_runtime_dir(&runtime)?;

    let pending = PendingApproval {
        approval_id: approval_id.to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        tool: tool.to_string(),
        path: path.map(str::to_string),
        timeout_seconds,
        challenge: challenge.to_string(),
        args: args.clone(),
    };
    let serialized = serde_json::to_vec_pretty(&pending)?;
    atomic_write_secure(
        &pending_approval_path(uhoh_dir, approval_id),
        &serialized,
        "pending",
    )?;
    Ok(())
}

fn wait_for_approval(
    uhoh_dir: &Path,
    approval_id: &str,
    expected_response: &str,
    timeout_seconds: u64,
    ledger: &crate::event_ledger::EventLedger,
) -> Result<bool> {
    let pending = pending_approval_path(uhoh_dir, approval_id);
    let approved = approved_path(uhoh_dir, approval_id);
    let runtime = runtime_dir(uhoh_dir);
    let timeout = std::time::Duration::from_secs(timeout_seconds.max(1));
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if let Ok(meta) = std::fs::symlink_metadata(&approved) {
            if meta.file_type().is_symlink() {
                let _ = std::fs::remove_file(&approved);
                continue;
            }
        }
        if let Some(response) = read_approval_response(&approved)? {
            if !secure_eq(response.approval_id.as_bytes(), approval_id.as_bytes())
                || !secure_eq(response.response.as_bytes(), expected_response.as_bytes())
            {
                let _ = std::fs::remove_file(&approved);
                continue;
            }
            let mut event = new_event("agent", "dangerous_action_approved", "info");
            event.detail = Some(
                serde_json::json!({
                    "approval_id": approval_id,
                })
                .to_string(),
            );
            if let Err(err) = ledger.append(event) {
                tracing::error!("failed to append dangerous_action_approved event: {err}");
            }
            let _ = std::fs::remove_file(&approved);
            let _ = std::fs::remove_file(&pending);
            return Ok(true);
        }
        if std::time::Instant::now() >= deadline {
            let _ = ensure_secure_runtime_dir(&runtime);
            let _ = std::fs::write(runtime.join("resume.pid"), std::process::id().to_string());
            let _ = std::fs::remove_file(&pending);
            return Ok(false);
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}

fn validate_auth_line(line: &str, expected_token: &str) -> Result<bool> {
    let trimmed = line.trim();
    if secure_eq(trimmed.as_bytes(), expected_token.as_bytes()) {
        return Ok(true);
    }
    let Ok(json) = serde_json::from_str::<serde_json::Value>(line) else {
        return Ok(false);
    };
    if json
        .get("method")
        .and_then(|v| v.as_str())
        .map(|m| m != "uhoh/auth")
        .unwrap_or(true)
    {
        return Ok(false);
    }
    let provided = json
        .pointer("/params/token")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    Ok(secure_eq(provided.as_bytes(), expected_token.as_bytes()))
}

fn read_approval_response(path: &Path) -> Result<Option<ApprovalResponse>> {
    if !path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read(path)
        .with_context(|| format!("Failed reading approval response: {}", path.display()))?;
    let parsed: ApprovalResponse = match serde_json::from_slice(&raw) {
        Ok(value) => value,
        Err(e) => {
            let _ = std::fs::remove_file(path);
            tracing::warn!(
                "Ignoring invalid approval response payload at {}: {}",
                path.display(),
                e
            );
            return Ok(None);
        }
    };
    Ok(Some(parsed))
}

fn ensure_secure_runtime_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)
        .with_context(|| format!("Failed to create runtime dir: {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
            .with_context(|| format!("Failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn atomic_write_secure(path: &Path, payload: &[u8], label: &str) -> Result<()> {
    let tmp = path.with_extension(format!("{}.tmp", label));
    std::fs::write(&tmp, payload)
        .with_context(|| format!("Failed writing temporary file: {}", tmp.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to set permissions on {}", tmp.display()))?;
    }
    std::fs::rename(&tmp, path)
        .with_context(|| format!("Failed finalizing pending approval file: {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to set permissions on {}", path.display()))?;
    }
    Ok(())
}

fn secure_eq(left: &[u8], right: &[u8]) -> bool {
    left.ct_eq(right).into()
}

fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

fn uuid_like_id() -> String {
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

pub fn build_approval_response(token: &str, approval_id: &str, challenge: &str) -> String {
    let mut key = [0u8; 32];
    if hex::decode_to_slice(token, &mut key).is_err() {
        key.copy_from_slice(blake3::hash(token.as_bytes()).as_bytes());
    }
    let mut payload = Vec::with_capacity(approval_id.len() + challenge.len() + 2);
    payload.extend_from_slice(approval_id.as_bytes());
    payload.push(b':');
    payload.extend_from_slice(challenge.as_bytes());
    let digest = blake3::keyed_hash(&key, &payload);
    let digest_hex = digest.to_hex().to_string();
    key.zeroize();
    payload.zeroize();
    digest_hex
}

pub fn ensure_proxy_token(uhoh_dir: &Path) -> Result<String> {
    std::fs::create_dir_all(uhoh_dir)
        .with_context(|| format!("Failed to create uhoh dir: {}", uhoh_dir.display()))?;
    let token_path = uhoh_dir.join(PROXY_TOKEN_FILE);
    if token_path.exists() {
        let token = std::fs::read_to_string(&token_path)
            .with_context(|| format!("Failed reading {}", token_path.display()))?;
        let token = token.trim().to_string();
        if token.is_empty() {
            anyhow::bail!("Agent proxy token file is empty: {}", token_path.display());
        }
        return Ok(token);
    }

    let token = random_hex(32);
    std::fs::write(&token_path, format!("{token}\n"))
        .with_context(|| format!("Failed writing {}", token_path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to set permissions on {}", token_path.display()))?;
    }
    Ok(token)
}
