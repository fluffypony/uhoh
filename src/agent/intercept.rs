use anyhow::Result;

use crate::db::AgentEntry;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn tick_session_tailers(ctx: &SubsystemContext, agents: &[AgentEntry]) -> Result<()> {
    for agent in agents {
        let mut event = new_event("agent", "session_tailer_tick", "info");
        event.agent_name = Some(agent.name.clone());
        event.detail = Some("session log tailer heartbeat".to_string());
        let _ = ctx.event_ledger.append(event);
    }
    Ok(())
}
