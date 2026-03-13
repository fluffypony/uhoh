use anyhow::Result;

use crate::db::{AgentEntry, LedgerSeverity, LedgerSource};
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

pub fn tick_audit(ctx: &AgentContext, agents: &[AgentEntry]) -> Result<()> {
    for agent in agents {
        let session_id = format!("agent:{}", agent.name);
        let mut event = new_event(LedgerSource::Agent, "audit_tick", LedgerSeverity::Info);
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
    Ok(())
}
