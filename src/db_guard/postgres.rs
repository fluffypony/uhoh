use anyhow::Result;

use crate::db::DbGuardEntry;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn tick_postgres_guard(ctx: &SubsystemContext, guard: &DbGuardEntry) -> Result<()> {
    let mut event = new_event("db_guard", "postgres_tick", "info");
    event.guard_name = Some(guard.name.clone());
    event.detail = Some(format!(
        "mode={}, dsn_ref={}",
        guard.mode,
        scrub_ref(&guard.connection_ref)
    ));
    let _ = ctx.event_ledger.append(event);
    Ok(())
}

fn scrub_ref(connection_ref: &str) -> String {
    connection_ref
        .split('@')
        .last()
        .unwrap_or(connection_ref)
        .to_string()
}
