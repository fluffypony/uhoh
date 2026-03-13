use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rand::RngCore;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use zeroize::Zeroize;

use crate::db::{LedgerSeverity, LedgerSource};
use crate::event_ledger::new_event;
use crate::subsystem::AgentContext;
use tokio_util::sync::CancellationToken;

const PROXY_TOKEN_FILE: &str = "server.token";
const APPROVAL_RESPONSE_FILE_SUFFIX: &str = ".approved.json";

pub async fn run_proxy(ctx: AgentContext, shutdown: CancellationToken) -> Result<()> {
    let _ = ensure_proxy_token(&ctx.uhoh_dir)?;
    let addr = format!("127.0.0.1:{}", ctx.config.agent.mcp_proxy_port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("Failed to bind MCP proxy listener on {addr}"))?;

    let mut event = new_event(
        LedgerSource::Agent,
        "mcp_proxy_started",
        LedgerSeverity::Info,
    );
    event.detail = Some(format!("addr={addr}"));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append mcp_proxy_started event: {err}");
    }

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accept = listener.accept() => match accept {
            Ok((stream, addr)) => {
                let peer = stream
                    .peer_addr()
                    .ok()
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let mut event = new_event(
                    LedgerSource::Agent,
                    "mcp_proxy_client_connected",
                    LedgerSeverity::Info,
                );
                event.detail = Some(format!("peer={peer}"));
                if let Err(err) = ctx.event_ledger.append(event) {
                    tracing::error!("failed to append mcp_proxy_client_connected event: {err}");
                }
                let upstream = resolve_upstream_addr();
                let uhoh_dir = ctx.uhoh_dir.clone();
                let event_ledger = ctx.event_ledger.clone();
                let config = ctx.config.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_connection_async(
                        stream,
                        upstream,
                        uhoh_dir,
                        event_ledger.clone(),
                        config,
                    )
                    .await
                    {
                        let mut event = new_event(
                            LedgerSource::Agent,
                            "mcp_proxy_connection_failed",
                            LedgerSeverity::Warn,
                        );
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
                let mut event = new_event(
                    LedgerSource::Agent,
                    "mcp_proxy_accept_failed",
                    LedgerSeverity::Warn,
                );
                event.detail = Some(e.to_string());
                if let Err(err) = ctx.event_ledger.append(event) {
                    tracing::error!("failed to append mcp_proxy_accept_failed event: {err}");
                }
                break;
            }
            }
        }
    }
    Ok(())
}

async fn handle_connection_async(
    client_stream: tokio::net::TcpStream,
    upstream_addr: String,
    uhoh_dir: PathBuf,
    ledger: crate::event_ledger::EventLedger,
    config: crate::config::Config,
) -> Result<()> {
    // Authenticate client BEFORE connecting upstream to prevent
    // unauthenticated clients from receiving upstream data.
    let (client_reader, client_writer) = client_stream.into_split();
    let mut client_lines = BufReader::new(client_reader).lines();
    let client_writer = std::sync::Arc::new(tokio::sync::Mutex::new(client_writer));

    let mut authed = !config.agent.mcp_proxy_require_auth;
    let expected_token = ensure_proxy_token(&uhoh_dir)?;

    if !authed {
        // Read the first line for auth handshake before connecting upstream
        let auth_line =
            tokio::time::timeout(std::time::Duration::from_secs(30), client_lines.next_line())
                .await
                .context("MCP proxy auth timeout")??;
        if let Some(line) = auth_line {
            authed = validate_auth_line(&line, &expected_token)?;
        }
        if !authed {
            return Err(anyhow::anyhow!("MCP proxy authentication failed"));
        }
    }

    // Now connect upstream after successful auth
    let upstream = tokio::net::TcpStream::connect(&upstream_addr)
        .await
        .with_context(|| format!("Failed connecting MCP upstream: {upstream_addr}"))?;

    let (upstream_reader, mut upstream_writer) = upstream.into_split();
    let mut upstream_lines = BufReader::new(upstream_reader).lines();

    let client_writer_relay = client_writer.clone();
    let upstream_to_client = tokio::spawn(async move {
        while let Some(line) = upstream_lines.next_line().await? {
            let mut w = client_writer_relay.lock().await;
            w.write_all(line.as_bytes()).await?;
            w.write_all(b"\n").await?;
            w.flush().await?;
        }
        Result::<(), anyhow::Error>::Ok(())
    });
    while let Some(line) = tokio::time::timeout(
        std::time::Duration::from_secs(300),
        client_lines.next_line(),
    )
    .await
    .context("MCP proxy read timeout waiting for client data")??
    {
        let mut should_forward = true;

        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&line) {
            if json
                .get("method")
                .and_then(|v| v.as_str())
                .map(|m| m == "tools/call")
                .unwrap_or(false)
            {
                match intercept_tool_call(&json, &uhoh_dir, &ledger, &config).await? {
                    InterceptResult::Forward => {}
                    InterceptResult::Block { id, reason } => {
                        let error_response = serde_json::json!({
                            "jsonrpc": "2.0",
                            "error": {
                                "code": -32000,
                                "message": format!("Action blocked by uhoh safety policy: {}", reason)
                            },
                            "id": id
                        });
                        let mut err_line = serde_json::to_string(&error_response)?;
                        err_line.push('\n');
                        let mut w = client_writer.lock().await;
                        w.write_all(err_line.as_bytes()).await?;
                        w.flush().await?;
                        should_forward = false;
                    }
                }
            }
        }

        if should_forward {
            upstream_writer.write_all(line.as_bytes()).await?;
            upstream_writer.write_all(b"\n").await?;
            upstream_writer.flush().await?;
        }
    }

    upstream_to_client.abort();
    Ok(())
}

/// Result of intercepting a tool call.
pub enum InterceptResult {
    /// Not dangerous or approved — forward normally.
    Forward,
    /// Rejected or timed out — send error response back to client.
    Block {
        id: serde_json::Value,
        reason: String,
    },
}

enum ApprovalDecision {
    Approved,
    Denied,
    TimedOut,
}

async fn intercept_tool_call(
    call: &serde_json::Value,
    uhoh_dir: &Path,
    ledger: &crate::event_ledger::EventLedger,
    config: &crate::config::Config,
) -> Result<InterceptResult> {
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
        let mut danger = new_event(
            LedgerSource::Agent,
            "dangerous_agent_action",
            LedgerSeverity::Critical,
        );
        danger.path = path.clone();
        danger.pre_state_ref = pre_state_ref.clone();
        danger.detail = Some(
            serde_json::json!({
                "approval_id": approval_id,
                "challenge": challenge,
                "session_id": session_id.clone(),
                "tool": tool,
                "args": args,
                "on_dangerous_change": config.agent.on_dangerous_change.as_str(),
            })
            .to_string(),
        );
        if let Err(err) = ledger.append(danger) {
            tracing::error!("failed to append dangerous_agent_action event: {err}");
        }

        if matches!(
            config.agent.on_dangerous_change,
            crate::config::DangerousChangePolicy::Pause
        ) {
            write_pending_approval(
                uhoh_dir,
                &approval_id,
                &challenge,
                tool,
                path.as_deref(),
                &args,
                config.agent.pause_timeout_seconds,
            )?;

            let approval = wait_for_approval(
                uhoh_dir,
                &approval_id,
                &approval_hmac,
                config.agent.pause_timeout_seconds,
                ledger,
            )
            .await?;
            match approval {
                ApprovalDecision::Approved => {
                    // Approved — fall through to proceed with the call
                }
                ApprovalDecision::TimedOut => {
                    // Timeout: auto-resume the action (matching documented behavior)
                    // and log a timeout event for audit purposes.
                    let mut timeout_event = new_event(
                        LedgerSource::Agent,
                        "dangerous_action_timeout",
                        LedgerSeverity::Warn,
                    );
                    timeout_event.path = path.clone();
                    timeout_event.detail = Some(
                        serde_json::json!({
                            "approval_id": approval_id,
                            "tool": tool,
                            "action": "auto_resumed",
                            "timeout_seconds": config.agent.pause_timeout_seconds,
                        })
                        .to_string(),
                    );
                    if let Err(err) = ledger.append(timeout_event) {
                        tracing::error!("failed to append timeout event: {err}");
                    }
                    tracing::warn!(
                        "Dangerous tool call '{}' auto-resumed after {} second timeout",
                        tool,
                        config.agent.pause_timeout_seconds
                    );
                    // Fall through to proceed with the call
                }
                ApprovalDecision::Denied => {
                    let mut block_event = new_event(
                        LedgerSource::Agent,
                        "dangerous_action_denied",
                        LedgerSeverity::Warn,
                    );
                    block_event.path = path.clone();
                    block_event.detail = Some(
                        serde_json::json!({
                            "approval_id": approval_id,
                            "tool": tool,
                            "action": "blocked",
                        })
                        .to_string(),
                    );
                    if let Err(err) = ledger.append(block_event) {
                        tracing::error!("failed to append denied event: {err}");
                    }
                    let call_id = call.get("id").cloned().unwrap_or(serde_json::Value::Null);
                    return Ok(InterceptResult::Block {
                        id: call_id,
                        reason: format!("Dangerous tool call '{}' was explicitly denied", tool),
                    });
                }
            }
        }
    }

    let mut event = new_event(LedgerSource::Agent, "tool_call", LedgerSeverity::Info);
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
    Ok(InterceptResult::Forward)
}

fn is_dangerous_tool_call(tool: &str, path: Option<&str>, patterns: &[String]) -> bool {
    let tool_l = tool.to_ascii_lowercase();
    let actual_path = path.unwrap_or_default();
    patterns.iter().any(|pattern| {
        let p = pattern.trim();
        if p.is_empty() {
            return false;
        }
        let p_l = p.to_ascii_lowercase();
        if let Some(raw) = p_l.strip_prefix("tool:") {
            return tool_l == raw.trim();
        }
        if let Some(raw) = p.strip_prefix("path:").or_else(|| p.strip_prefix("PATH:")) {
            let raw = raw.trim();
            if raw.is_empty() {
                return false;
            }
            // Use platform-appropriate case comparison for path matching.
            // On Unix, filesystems are case-sensitive; on Windows, case-insensitive.
            let file_path = std::path::Path::new(actual_path);
            let pattern_path = std::path::Path::new(raw);
            #[cfg(unix)]
            {
                return file_path.ends_with(pattern_path);
            }
            #[cfg(not(unix))]
            {
                let file_lower = actual_path.to_ascii_lowercase();
                let raw_lower = raw.to_ascii_lowercase();
                return std::path::Path::new(&file_lower)
                    .ends_with(std::path::Path::new(&raw_lower));
            }
        }
        tool_l == p_l || actual_path == p
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

struct PendingApprovalFile {
    stem: String,
    approval_id: String,
    challenge: String,
}

fn pending_approval_path(uhoh_dir: &Path, approval_id: &str) -> PathBuf {
    runtime_dir(uhoh_dir).join(format!("{approval_id}.pending.json"))
}

fn approved_path(uhoh_dir: &Path, approval_id: &str) -> PathBuf {
    runtime_dir(uhoh_dir).join(format!("{approval_id}{APPROVAL_RESPONSE_FILE_SUFFIX}"))
}

pub fn approve_pending_actions(uhoh_dir: &Path, strict: bool) -> Result<usize> {
    let runtime = runtime_dir(uhoh_dir);
    ensure_secure_runtime_dir(&runtime)?;
    let token = ensure_proxy_token(uhoh_dir)?;
    let mut approved_count = 0;

    for pending in list_pending_approval_files(&runtime, strict)? {
        let response = build_approval_response(&token, &pending.approval_id, &pending.challenge);
        let body = serde_json::json!({
            "approval_id": pending.approval_id,
            "response": response,
        });
        write_approved_response(&runtime, &pending.stem, &serde_json::to_vec_pretty(&body)?)?;
        approved_count += 1;
    }

    Ok(approved_count)
}

pub fn deny_pending_actions(uhoh_dir: &Path) -> Result<usize> {
    let runtime = runtime_dir(uhoh_dir);
    let mut denied_count = 0;
    if let Ok(entries) = std::fs::read_dir(&runtime) {
        for entry in entries.flatten() {
            let path = entry.path();
            if is_pending_approval_path(&path) && std::fs::remove_file(&path).is_ok() {
                denied_count += 1;
            }
        }
    }
    Ok(denied_count)
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

async fn wait_for_approval(
    uhoh_dir: &Path,
    approval_id: &str,
    expected_response: &str,
    timeout_seconds: u64,
    ledger: &crate::event_ledger::EventLedger,
) -> Result<ApprovalDecision> {
    let pending = pending_approval_path(uhoh_dir, approval_id);
    let approved = approved_path(uhoh_dir, approval_id);
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
            let mut event = new_event(
                LedgerSource::Agent,
                "dangerous_action_approved",
                LedgerSeverity::Info,
            );
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
            return Ok(ApprovalDecision::Approved);
        }
        // Explicit denial: if pending file was deleted externally, treat as rejection
        if !pending.exists() {
            return Ok(ApprovalDecision::Denied);
        }
        if std::time::Instant::now() >= deadline {
            let _ = std::fs::remove_file(&pending);
            return Ok(ApprovalDecision::TimedOut);
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
}

fn validate_auth_line(line: &str, expected_token: &str) -> Result<bool> {
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
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => {
            return Err(err)
                .with_context(|| format!("Failed stating approval response: {}", path.display()));
        }
    };
    if meta.file_type().is_symlink() {
        let _ = std::fs::remove_file(path);
        return Ok(None);
    }

    #[cfg(unix)]
    let raw = {
        use std::io::Read as _;
        use std::os::unix::fs::OpenOptionsExt;

        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
            .with_context(|| format!("Failed opening approval response: {}", path.display()))?;
        let mut raw = Vec::new();
        let mut reader = std::io::BufReader::new(file);
        reader
            .read_to_end(&mut raw)
            .with_context(|| format!("Failed reading approval response: {}", path.display()))?;
        raw
    };

    #[cfg(not(unix))]
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

fn list_pending_approval_files(runtime: &Path, strict: bool) -> Result<Vec<PendingApprovalFile>> {
    let mut approvals = Vec::new();
    let entries = match std::fs::read_dir(runtime) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(approvals),
        Err(err) => return Err(err.into()),
    };

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(err) if !strict => {
                tracing::warn!("Ignoring unreadable pending approval entry: {err}");
                continue;
            }
            Err(err) => return Err(err.into()),
        };
        let path = entry.path();
        if !is_pending_approval_path(&path) {
            continue;
        }
        match parse_pending_approval_file(&path) {
            Ok(Some(pending)) => approvals.push(pending),
            Ok(None) => {}
            Err(err) if !strict => {
                tracing::warn!(
                    "Ignoring invalid pending approval {}: {err}",
                    path.display()
                );
            }
            Err(err) => return Err(err),
        }
    }

    Ok(approvals)
}

fn parse_pending_approval_file(path: &Path) -> Result<Option<PendingApprovalFile>> {
    let Some(stem) = path
        .file_name()
        .and_then(|v| v.to_str())
        .and_then(|v| v.strip_suffix(".pending.json"))
    else {
        return Ok(None);
    };
    let pending_raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed reading {}", path.display()))?;
    let pending_json: serde_json::Value = serde_json::from_str(&pending_raw)
        .with_context(|| format!("Invalid pending approval JSON: {}", path.display()))?;
    let approval_id = pending_json
        .get("approval_id")
        .and_then(|v| v.as_str())
        .context("Pending approval missing approval_id")?;
    let challenge = pending_json
        .get("challenge")
        .and_then(|v| v.as_str())
        .context("Pending approval missing challenge")?;

    Ok(Some(PendingApprovalFile {
        stem: stem.to_string(),
        approval_id: approval_id.to_string(),
        challenge: challenge.to_string(),
    }))
}

fn is_pending_approval_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|v| v.to_str())
        .map(|v| v.ends_with(".pending.json"))
        .unwrap_or(false)
}

fn write_approved_response(runtime: &Path, stem: &str, payload: &[u8]) -> Result<()> {
    let path = runtime.join(format!("{stem}{APPROVAL_RESPONSE_FILE_SUFFIX}"));
    if path
        .symlink_metadata()
        .map(|meta| meta.file_type().is_symlink())
        .unwrap_or(false)
    {
        anyhow::bail!("Refusing to write approval response through symlink");
    }
    atomic_write_secure(&path, payload, "approved")
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
    std::fs::rename(&tmp, path).with_context(|| {
        format!(
            "Failed finalizing pending approval file: {}",
            path.display()
        )
    })?;
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

    // Try to read existing token first
    if let Ok(token) = std::fs::read_to_string(&token_path) {
        let token = token.trim().to_string();
        if !token.is_empty() {
            return Ok(token);
        }
    }

    // Atomic creation: create_new(true) fails if the file already exists,
    // preventing race conditions where two callers generate different tokens.
    let token = random_hex(32);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&token_path)
    {
        Ok(mut f) => {
            use std::io::Write;
            f.write_all(format!("{token}\n").as_bytes())
                .with_context(|| format!("Failed writing {}", token_path.display()))?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&token_path, std::fs::Permissions::from_mode(0o600))
                    .with_context(|| {
                        format!("Failed to set permissions on {}", token_path.display())
                    })?;
            }
            Ok(token)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            // Another caller created it first — read their token
            let token = std::fs::read_to_string(&token_path)
                .with_context(|| format!("Failed reading {}", token_path.display()))?;
            let token = token.trim().to_string();
            if token.is_empty() {
                anyhow::bail!("Agent proxy token file is empty: {}", token_path.display());
            }
            Ok(token)
        }
        Err(e) => Err(e).with_context(|| format!("Failed creating {}", token_path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::{is_dangerous_tool_call, random_hex, read_approval_response, validate_auth_line};

    #[test]
    fn dangerous_tool_pattern_exact_and_substring() {
        let patterns = vec!["write".to_string()];
        assert!(is_dangerous_tool_call(
            "write",
            Some("/tmp/file"),
            &patterns
        ));
        assert!(!is_dangerous_tool_call(
            "apply_patch",
            Some("/workspace/write-target"),
            &patterns
        ));
    }

    #[test]
    fn dangerous_tool_pattern_with_prefixes() {
        let patterns = vec!["tool:apply_patch".to_string(), "path:/tmp/a".to_string()];
        assert!(is_dangerous_tool_call(
            "apply_patch",
            Some("/other"),
            &patterns
        ));
        assert!(is_dangerous_tool_call("other", Some("/tmp/a"), &patterns));
        assert!(!is_dangerous_tool_call("other", Some("/tmp/b"), &patterns));
    }

    #[test]
    fn auth_accepts_jsonrpc_handshake_and_rejects_raw_token() {
        let expected_auth_value = random_hex(16);
        let handshake = serde_json::json!({
            "jsonrpc": "2.0",
            "id": "uhoh-auth",
            "method": "uhoh/auth",
            "params": { "token": expected_auth_value }
        })
        .to_string();
        assert!(validate_auth_line(&handshake, &expected_auth_value).expect("valid handshake"));
        assert!(
            !validate_auth_line(&expected_auth_value, &expected_auth_value)
                .expect("raw token should fail")
        );
    }

    #[cfg(unix)]
    #[test]
    fn approval_reader_rejects_symlink_payloads() {
        let temp = tempfile::tempdir().expect("tempdir");
        let target = temp.path().join("real.approved.json");
        let link = temp.path().join("approval.approved.json");
        std::fs::write(&target, r#"{"approval_id":"a","response":"b"}"#).expect("write target");
        std::os::unix::fs::symlink(&target, &link).expect("symlink");

        assert!(read_approval_response(&link)
            .expect("read approval")
            .is_none());
        assert!(!link.exists());
    }
}
