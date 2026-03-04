use anyhow::Result;

use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn tick_proxy(ctx: &SubsystemContext) -> Result<()> {
    let mut event = new_event("agent", "mcp_proxy_tick", "info");
    event.detail = Some(format!("port={}", ctx.config.agent.mcp_proxy_port));
    let _ = ctx.event_ledger.append(event);
    Ok(())
}
