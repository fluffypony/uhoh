use anyhow::Result;

use crate::db::AgentEntry;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn tick_audit(ctx: &SubsystemContext, agents: &[AgentEntry]) -> Result<()> {
    for agent in agents {
        let mut event = new_event("agent", "audit_tick", "info");
        event.agent_name = Some(agent.name.clone());
        event.detail = Some(format!("scope={}", ctx.config.agent.audit_scope));
        if let Err(err) = ctx.event_ledger.append(event) {
            tracing::error!("failed to append audit_tick event: {err}");
        }
    }
    Ok(())
}
