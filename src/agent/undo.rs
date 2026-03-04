use anyhow::Result;

use crate::event_ledger::EventLedger;

pub fn resolve_event(event_ledger: &EventLedger, event_id: i64) -> Result<()> {
    event_ledger.mark_resolved(event_id)
}
