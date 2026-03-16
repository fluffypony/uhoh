use std::collections::HashMap;
use std::io::SeekFrom;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, BufReader};

use crate::db::{AgentEntry, LedgerEventType, LedgerSeverity, LedgerSource};
use crate::event_ledger::new_event;
use crate::subsystem::AgentContext;

pub async fn run_session_tailers_async(ctx: &AgentContext, agents: &[AgentEntry]) -> Result<()> {
    let mut offsets: HashMap<String, u64> = load_offsets(&ctx.uhoh_dir).unwrap_or_default();
    loop {
        let mut offsets_changed = false;
        for agent in agents {
            let profile_toml = super::expand_home(&agent.profile_path);
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

            let key = offset_key(&agent.name, &session_path);
            let entry = offsets.entry(key).or_insert(0);
            match async_tail_one_file(ctx, agent, &session_path, *entry).await {
                Ok(new_offset) => {
                    if *entry != new_offset {
                        *entry = new_offset;
                        offsets_changed = true;
                    }
                }
                Err(err) => {
                    tracing::warn!(
                        "Agent log tailing error for {} at {}: {}",
                        agent.name,
                        session_path.display(),
                        err
                    );
                }
            }
        }
        if offsets_changed {
            if let Err(err) = persist_offsets(&ctx.uhoh_dir, &offsets) {
                tracing::warn!("Failed to persist agent tail offsets: {err}");
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

fn offset_key(agent_name: &str, session_path: &Path) -> String {
    format!("{}|{}", agent_name, session_path.display())
}

fn offsets_path(uhoh_dir: &Path) -> PathBuf {
    uhoh_dir
        .join("agents")
        .join("runtime")
        .join("tail_offsets.json")
}

fn load_offsets(uhoh_dir: &Path) -> Result<HashMap<String, u64>> {
    let path = offsets_path(uhoh_dir);
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let raw = std::fs::read_to_string(&path)?;
    let parsed = serde_json::from_str::<HashMap<String, u64>>(&raw)?;
    Ok(parsed)
}

fn persist_offsets(uhoh_dir: &Path, offsets: &HashMap<String, u64>) -> Result<()> {
    let path = offsets_path(uhoh_dir);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    let tmp = path.with_extension("json.tmp");
    let data = serde_json::to_vec(offsets)?;
    std::fs::write(&tmp, data)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600));
    }
    std::fs::rename(&tmp, &path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

async fn async_tail_one_file(
    ctx: &AgentContext,
    agent: &AgentEntry,
    path: &Path,
    offset: u64,
) -> Result<u64> {
    let file = tokio::fs::File::open(path).await?;
    let file_len = file.metadata().await?.len();
    let mut reader = BufReader::new(file);
    // If file was truncated/rotated, reset to beginning
    let actual_offset = if offset > file_len { 0 } else { offset };
    reader.seek(SeekFrom::Start(actual_offset)).await?;

    let mut new_offset = actual_offset;
    let mut carry = String::new();
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).await?;
        if bytes == 0 {
            // EOF hit — do not advance offset past incomplete data in carry
            // so the next poll re-reads the partial line.
            if !carry.is_empty() {
                new_offset -= carry.len() as u64;
            }
            break;
        }
        new_offset += bytes as u64;
        carry.push_str(&line);
        if !carry.ends_with('\n') {
            continue;
        }
        let raw = std::mem::take(&mut carry);
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(raw.trim()) {
            process_jsonl_event(ctx, agent, &json).await?;
        }
    }

    Ok(new_offset)
}

async fn process_jsonl_event(
    ctx: &AgentContext,
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
        .map(std::string::ToString::to_string);
    let session_id = event
        .get("session_id")
        .or_else(|| event.get("session"))
        .and_then(|v| v.as_str())
        .map(str::to_string);

    let mut pre_state_ref = None;
    if let Some(path) = target_path.as_deref() {
        let p = Path::new(path);
        if p.exists() {
            match tokio::fs::read(p).await {
                Ok(bytes) => match crate::cas::store_blob(&ctx.uhoh_dir.join("blobs"), &bytes) {
                    Ok((hash, _)) => {
                        pre_state_ref = Some(hash);
                    }
                    Err(err) => {
                        tracing::warn!(
                            "failed to store pre-state blob for {}: {}",
                            p.display(),
                            err
                        );
                    }
                },
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                    tracing::debug!("pre-state source disappeared before read: {}", p.display());
                }
                Err(err) => {
                    tracing::warn!("failed to read pre-state source {}: {}", p.display(), err);
                }
            }
        }
    }

    let mut ledger_event = new_event(
        LedgerSource::Agent,
        LedgerEventType::SessionToolCall,
        LedgerSeverity::Info,
    );
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
    if let Err(err) = ctx.event_ledger.append(&ledger_event) {
        tracing::error!("failed to append session_tool_call event: {err}");
    }
    Ok(())
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_key_format() {
        let key = offset_key("my-agent", Path::new("/tmp/session.log"));
        assert_eq!(key, "my-agent|/tmp/session.log");
    }

    #[test]
    fn offset_key_empty_agent() {
        let key = offset_key("", Path::new("/path"));
        assert_eq!(key, "|/path");
    }

    #[test]
    fn offsets_path_location() {
        let path = offsets_path(Path::new("/home/user/.uhoh"));
        assert_eq!(
            path,
            PathBuf::from("/home/user/.uhoh/agents/runtime/tail_offsets.json")
        );
    }

    #[test]
    fn load_offsets_missing_dir_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let offsets = load_offsets(tmp.path()).unwrap();
        assert!(offsets.is_empty());
    }

    #[test]
    fn persist_and_load_offsets_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut offsets = HashMap::new();
        offsets.insert("agent1|/log1".to_string(), 42u64);
        offsets.insert("agent2|/log2".to_string(), 100u64);

        persist_offsets(tmp.path(), &offsets).unwrap();
        let loaded = load_offsets(tmp.path()).unwrap();
        assert_eq!(loaded, offsets);
    }

    #[cfg(unix)] // rename-replace is not atomic on Windows
    #[test]
    fn persist_offsets_overwrites_previous() {
        let tmp = tempfile::tempdir().unwrap();
        let mut offsets1 = HashMap::new();
        offsets1.insert("key".to_string(), 10u64);
        persist_offsets(tmp.path(), &offsets1).unwrap();

        let mut offsets2 = HashMap::new();
        offsets2.insert("key".to_string(), 20u64);
        persist_offsets(tmp.path(), &offsets2).unwrap();

        let loaded = load_offsets(tmp.path()).unwrap();
        assert_eq!(loaded["key"], 20);
    }

    #[test]
    fn persist_offsets_empty_map() {
        let tmp = tempfile::tempdir().unwrap();
        let offsets = HashMap::new();
        persist_offsets(tmp.path(), &offsets).unwrap();
        let loaded = load_offsets(tmp.path()).unwrap();
        assert!(loaded.is_empty());
    }
}
