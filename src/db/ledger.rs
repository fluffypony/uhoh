use super::{EventLedgerEntry, NewEventLedgerEntry};
use rusqlite::Row;

pub fn map_event_ledger_entry(row: &Row<'_>) -> rusqlite::Result<EventLedgerEntry> {
    Ok(EventLedgerEntry {
        id: row.get(0)?,
        ts: row.get(1)?,
        source: row.get(2)?,
        event_type: row.get(3)?,
        severity: row.get(4)?,
        project_hash: row.get(5)?,
        agent_name: row.get(6)?,
        guard_name: row.get(7)?,
        path: row.get(8)?,
        detail: row.get(9)?,
        pre_state_ref: row.get(10)?,
        post_state_ref: row.get(11)?,
        causal_parent: row.get(13)?,
        resolved: row.get::<_, i32>(14)? != 0,
    })
}

pub fn compute_event_chain_hash(prev_hash: &str, event: &NewEventLedgerEntry, ts: &str) -> String {
    let payload = serde_json::json!({
        "prev_hash": prev_hash,
        "ts": ts,
        "source": event.source,
        "event_type": event.event_type,
        "severity": event.severity,
        "project_hash": event.project_hash,
        "agent_name": event.agent_name,
        "guard_name": event.guard_name,
        "path": event.path,
        "detail": event.detail,
        "pre_state_ref": event.pre_state_ref,
        "post_state_ref": event.post_state_ref,
        "causal_parent": event.causal_parent,
    })
    .to_string();
    blake3::hash(payload.as_bytes()).to_hex().to_string()
}
