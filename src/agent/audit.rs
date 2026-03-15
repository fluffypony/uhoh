use crate::db::{AgentEntry, LedgerEventType, LedgerSeverity, LedgerSource};
use crate::event_ledger::new_event;
use crate::subsystem::AgentContext;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum AuditEvent {
    Heartbeat {
        agent: String,
        scope: crate::config::AgentAuditScope,
    },
    FanotifyPreImage {
        path: String,
        pid: i32,
        pid_start_time_ticks: u64,
        pre_state_ref: String,
    },
    Overflow {
        dropped: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_event_heartbeat_serde_roundtrip() {
        let event = AuditEvent::Heartbeat {
            agent: "test-agent".to_string(),
            scope: crate::config::AgentAuditScope::Project,
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: AuditEvent = serde_json::from_str(&json).unwrap();
        match parsed {
            AuditEvent::Heartbeat { agent, .. } => assert_eq!(agent, "test-agent"),
            _ => panic!("Expected Heartbeat"),
        }
    }

    #[test]
    fn audit_event_overflow_serde_roundtrip() {
        let event = AuditEvent::Overflow { dropped: 42 };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: AuditEvent = serde_json::from_str(&json).unwrap();
        match parsed {
            AuditEvent::Overflow { dropped } => assert_eq!(dropped, 42),
            _ => panic!("Expected Overflow"),
        }
    }

    #[test]
    fn audit_event_fanotify_serde_roundtrip() {
        let event = AuditEvent::FanotifyPreImage {
            path: "/tmp/file.rs".to_string(),
            pid: 1234,
            pid_start_time_ticks: 999,
            pre_state_ref: "abc123".to_string(),
        };
        let json = serde_json::to_string(&event).unwrap();
        let parsed: AuditEvent = serde_json::from_str(&json).unwrap();
        match parsed {
            AuditEvent::FanotifyPreImage { path, pid, .. } => {
                assert_eq!(path, "/tmp/file.rs");
                assert_eq!(pid, 1234);
            }
            _ => panic!("Expected FanotifyPreImage"),
        }
    }
}

pub fn tick_audit(ctx: &AgentContext, agents: &[AgentEntry]) {
    for agent in agents {
        let session_id = format!("agent:{}", agent.name);
        let mut event = new_event(LedgerSource::Agent, LedgerEventType::AuditTick, LedgerSeverity::Info);
        event.agent_name = Some(agent.name.clone());
        let payload = AuditEvent::Heartbeat {
            agent: agent.name.clone(),
            scope: ctx.config.agent.audit_scope,
        };
        event.detail = Some(
            serde_json::json!({
                "session_id": session_id,
                "audit": payload,
            })
            .to_string(),
        );
        if let Err(err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append audit_tick event: {err}");
        }
    }
}
