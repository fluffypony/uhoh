use std::path::Path;

use anyhow::{Context, Result};

use crate::cli::AgentAction;
use crate::db::{self, Database};

pub fn handle_cli_action(uhoh_dir: &Path, database: &Database, action: &AgentAction) -> Result<()> {
    match action {
        AgentAction::Add { name, profile } => {
            let profile_path = profile
                .clone()
                .unwrap_or_else(|| format!("~/.uhoh/agents/{name}.toml"));
            let resolved_profile = crate::util::expand_home(&profile_path);
            if !Path::new(&resolved_profile).exists() {
                anyhow::bail!("Agent profile not found: {resolved_profile}");
            }
            let _ = crate::agent::load_agent_profile(Path::new(&resolved_profile))?;
            database.add_agent(name, &profile_path, None)?;
            println!("Added agent '{name}'");
        }
        AgentAction::Remove { name } => {
            database.remove_agent(name)?;
            println!("Removed agent '{name}'");
        }
        AgentAction::List => {
            let agents = database.list_agents()?;
            if agents.is_empty() {
                println!("No agents registered");
            } else {
                for agent in agents {
                    println!("{} [{}]", agent.name, agent.profile_path);
                }
            }
        }
        AgentAction::Log { name, session } => {
            let events = database.event_ledger_recent(
                Some("agent"),
                None,
                name.as_deref(),
                session.as_deref(),
                100,
            )?;
            for event in events {
                let session_out = event
                    .detail
                    .as_deref()
                    .and_then(extract_session_id)
                    .unwrap_or_else(|| "-".to_string());
                let tool = event
                    .detail
                    .as_deref()
                    .and_then(|detail| extract_detail_field(detail, "tool"))
                    .unwrap_or_else(|| "-".to_string());
                let path = event.path.clone().unwrap_or_else(|| "-".to_string());
                println!(
                    "#{} {} [{}] {} session={} tool={} path={}",
                    event.id, event.ts, event.severity, event.event_type, session_out, tool, path
                );
            }
        }
        AgentAction::Undo {
            event_id,
            session,
            cascade,
        } => {
            if let Some(root_id) = *cascade {
                if let Some(session_id) = session.as_deref() {
                    let root_event = database
                        .event_ledger_get(root_id)?
                        .context("Root event not found")?;
                    if !session_matches_event(&root_event, session_id) {
                        anyhow::bail!(
                            "Root event #{} does not belong to session {}",
                            root_id,
                            session_id
                        );
                    }
                    let changed = database
                        .event_ledger_mark_resolved_cascade_with_session(root_id, session_id)?;
                    println!(
                        "Marked {} session-matching event(s) as resolved for cascade root #{}",
                        changed, root_id
                    );
                    return Ok(());
                }
                let changed = database.event_ledger_mark_resolved_cascade(root_id)?;
                println!(
                    "Marked event #{} and {} downstream event(s) as resolved (acknowledged only; this does not revert filesystem or DB state)",
                    root_id,
                    changed.saturating_sub(1)
                );
            } else if let Some(id) = event_id {
                if let Some(session_id) = session.as_deref() {
                    let event = database.event_ledger_get(*id)?.context("Event not found")?;
                    if !session_matches_event(&event, session_id) {
                        anyhow::bail!("Event #{} does not belong to session {}", id, session_id);
                    }
                }
                let ledger_db = std::sync::Arc::new(database.clone_handle());
                let ledger = crate::event_ledger::EventLedger::new(ledger_db);
                crate::agent::resolve_event(database, &ledger, uhoh_dir, *id)?;
                println!("Reverted event #{} and marked it as resolved", id);
            } else {
                anyhow::bail!("Provide event id or --cascade");
            }
        }
        AgentAction::Approve => {
            if crate::agent::approve_pending_actions(uhoh_dir, true)? > 0 {
                println!("Approved pending agent action");
            } else {
                println!("No pending agent actions found");
            }
        }
        AgentAction::Deny => {
            if crate::agent::deny_pending_actions(uhoh_dir)? > 0 {
                println!("Denied pending agent action(s)");
            } else {
                println!("No pending agent actions found to deny");
            }
        }
        AgentAction::Resume => {
            if crate::agent::approve_pending_actions(uhoh_dir, false)? > 0 {
                println!("Approved pending agent action(s); proxy resumed processing");
                return Ok(());
            }
            println!("No pending agent approvals found to resume");
        }
        AgentAction::Setup => {
            let agents_dir = uhoh_dir.join("agents");
            std::fs::create_dir_all(&agents_dir)?;
            let template_path = agents_dir.join("default.toml");
            if !template_path.exists() {
                std::fs::write(
                    &template_path,
                    r#"name = "default"
session_log_pattern = ""
tool_call_format = "jsonl"
"#,
                )?;
            }
            println!("Agent profiles initialized at {}", agents_dir.display());
            println!(
                "Edit {} to configure agent monitoring.",
                template_path.display()
            );
        }
        AgentAction::Test { name } => {
            let agent = database
                .list_agents()?
                .into_iter()
                .find(|agent| agent.name == *name);
            let Some(agent) = agent else {
                anyhow::bail!("Agent not registered");
            };
            println!("Agent '{name}' is registered");

            let profile_path = Path::new(&agent.profile_path);
            if profile_path.exists() {
                match crate::agent::load_agent_profile(profile_path) {
                    Ok(profile) => {
                        println!("  Profile: {} (OK)", profile_path.display());
                        if !profile.session_log_pattern.is_empty() {
                            match crate::agent::resolve_session_log_path(
                                &profile.session_log_pattern,
                            ) {
                                Ok(Some(path)) => {
                                    println!("  Session log: {} (found)", path.display())
                                }
                                Ok(None) => {
                                    println!(
                                        "  Session log: no match for pattern '{}'",
                                        profile.session_log_pattern
                                    )
                                }
                                Err(err) => println!("  Session log: error resolving: {err}"),
                            }
                        }
                    }
                    Err(err) => println!("  Profile: {} (ERROR: {err})", profile_path.display()),
                }
            } else {
                println!("  Profile: {} (NOT FOUND)", profile_path.display());
            }
        }
        AgentAction::Init => {
            let profile_dir = uhoh_dir.join("agents");
            std::fs::create_dir_all(&profile_dir)?;
            let default_profile = profile_dir.join("generic.toml");
            if !default_profile.exists() {
                std::fs::write(
                    &default_profile,
                    r#"name = "generic"
process_names = ["node", "python", "uhoh"]
session_log_pattern = "~/.uhoh/agent-intent.jsonl"
tool_names_write = ["write", "apply_patch"]
tool_names_exec = ["exec", "bash", "shell"]
tool_call_format = "jsonl"
"#,
                )?;
                println!("Initialized default profile: {}", default_profile.display());
            } else {
                println!(
                    "Default profile already exists: {}",
                    default_profile.display()
                );
            }
        }
    }
    Ok(())
}

fn session_matches_event(entry: &db::EventLedgerEntry, session_id: &str) -> bool {
    entry
        .detail
        .as_deref()
        .and_then(extract_session_id)
        .map(|value| value == session_id)
        .unwrap_or(false)
}

fn extract_session_id(detail: &str) -> Option<String> {
    if !detail.trim_start().starts_with('{') {
        return None;
    }
    let json = serde_json::from_str::<serde_json::Value>(detail).ok()?;
    json.get("session_id")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn extract_detail_field(detail: &str, key: &str) -> Option<String> {
    let json = serde_json::from_str::<serde_json::Value>(detail).ok()?;
    json.get(key)
        .and_then(|value| value.as_str())
        .map(str::to_string)
}
