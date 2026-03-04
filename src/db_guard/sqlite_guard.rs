use anyhow::Result;

use crate::db::DbGuardEntry;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

pub fn tick_sqlite_guard(ctx: &SubsystemContext, guard: &DbGuardEntry) -> Result<()> {
    let path = normalize_sqlite_path(&guard.connection_ref);
    let mut event = new_event("db_guard", "sqlite_tick", "info");
    event.guard_name = Some(guard.name.clone());
    event.path = Some(path.clone());
    event.detail = Some(format!("watch_path={path}"));
    let _ = ctx.event_ledger.append(event);
    Ok(())
}

fn normalize_sqlite_path(connection_ref: &str) -> String {
    if let Some(stripped) = connection_ref.strip_prefix("sqlite://") {
        stripped.to_string()
    } else {
        connection_ref.to_string()
    }
}
