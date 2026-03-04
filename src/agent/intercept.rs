use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

use anyhow::Result;

use crate::db::AgentEntry;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn run_session_tailers(ctx: &SubsystemContext, agents: &[AgentEntry]) -> Result<()> {
    let mut offsets: HashMap<String, u64> = HashMap::new();
    loop {
        for agent in agents {
            let profile_toml = expand_home(&agent.profile_path);
            let profile_path = Path::new(&profile_toml);
            if !profile_path.exists() {
                continue;
            }

            let Ok(profile) = crate::agent::profiles::load_agent_profile(profile_path) else {
                continue;
            };
            let Ok(Some(session_path)) =
                crate::agent::profiles::resolve_session_log_path(&profile.session_log_pattern)
            else {
                continue;
            };

            let entry = offsets.entry(agent.name.clone()).or_insert(0);
            *entry = tail_one_file(ctx, agent, &session_path, *entry)?;
        }
        tokio::runtime::Handle::try_current()
            .map(|handle| {
                handle.block_on(tokio::time::sleep(std::time::Duration::from_millis(500)));
            })
            .unwrap_or_else(|_| std::thread::sleep(std::time::Duration::from_millis(500)));
    }
}

fn tail_one_file(
    ctx: &SubsystemContext,
    agent: &AgentEntry,
    path: &Path,
    offset: u64,
) -> Result<u64> {
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);
    reader.seek(SeekFrom::Start(offset))?;

    let mut new_offset = offset;
    let mut carry = String::new();
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        new_offset += bytes as u64;
        carry.push_str(&line);
        if !carry.ends_with('\n') {
            continue;
        }
        let raw = std::mem::take(&mut carry);
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(raw.trim()) {
            process_jsonl_event(ctx, agent, &json)?;
        }
    }

    Ok(new_offset)
}

fn process_jsonl_event(
    ctx: &SubsystemContext,
    agent: &AgentEntry,
    event: &serde_json::Value,
) -> Result<()> {
    let tool = event
        .get("tool")
        .or_else(|| event.get("name"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown-tool");
    let target_path = event
        .get("path")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let session_id = event
        .get("session_id")
        .or_else(|| event.get("session"))
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let mut pre_state_ref = None;
    if let Some(path) = target_path.as_deref() {
        let p = Path::new(path);
        if p.exists() {
            let bytes = std::fs::read(p)?;
            let (hash, _) = crate::cas::store_blob(&ctx.uhoh_dir.join("blobs"), &bytes)?;
            pre_state_ref = Some(hash);
        }
    }

    let mut ledger_event = new_event("agent", "session_tool_call", "info");
    ledger_event.agent_name = Some(agent.name.clone());
    ledger_event.path = target_path;
    ledger_event.pre_state_ref = pre_state_ref;
    ledger_event.detail = Some(
        serde_json::json!({
            "tool": tool,
            "session_id": session_id,
            "raw": event,
        })
        .to_string(),
    );
    if let Err(err) = ctx.event_ledger.append(ledger_event) {
        tracing::error!("failed to append session_tool_call event: {err}");
    }
    Ok(())
}

fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).display().to_string();
        }
    }
    path.to_string()
}
