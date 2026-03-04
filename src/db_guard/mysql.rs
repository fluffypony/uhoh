use anyhow::Result;

use crate::db::DbGuardEntry;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn tick_mysql_guard(ctx: &SubsystemContext, guard: &DbGuardEntry) -> Result<()> {
    let mut event = new_event("db_guard", "mysql_tick", "warn");
    event.guard_name = Some(guard.name.clone());
    event.detail =
        Some("MySQL binlog streaming is deferred; running schema polling mode".to_string());
    let _ = ctx.event_ledger.append(event);
    Ok(())
}
