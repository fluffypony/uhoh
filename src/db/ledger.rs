use super::{EventLedgerEntry, NewEventLedgerEntry};
use rusqlite::types::Value;
use rusqlite::Row;

pub struct LedgerRecentFilters<'a> {
    pub source: Option<&'a str>,
    pub guard_name: Option<&'a str>,
    pub agent_name: Option<&'a str>,
    pub session: Option<&'a str>,
}

pub fn build_recent_query(filters: LedgerRecentFilters<'_>, limit: i64) -> (String, Vec<Value>) {
    let mut where_clauses = Vec::new();
    let mut params = Vec::new();

    if let Some(source) = filters.source {
        where_clauses.push("source = ?");
        params.push(Value::from(source.to_string()));
    }
    if let Some(guard_name) = filters.guard_name {
        where_clauses.push("guard_name = ?");
        params.push(Value::from(guard_name.to_string()));
    }
    if let Some(agent_name) = filters.agent_name {
        where_clauses.push("agent_name = ?");
        params.push(Value::from(agent_name.to_string()));
    }
    if let Some(session) = filters.session {
        where_clauses.push("session_id = ?");
        params.push(Value::from(session.to_string()));
    }

    let mut sql = String::from(
        "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
         FROM event_ledger",
    );
    if !where_clauses.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clauses.join(" AND "));
    }
    sql.push_str(" ORDER BY id DESC LIMIT ?");
    params.push(Value::Integer(limit.max(1)));

    (sql, params)
}

impl<'r> TryFrom<&Row<'r>> for EventLedgerEntry {
    type Error = rusqlite::Error;

    fn try_from(row: &Row<'r>) -> Result<Self, Self::Error> {
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
}

pub fn map_event_ledger_entry(row: &Row<'_>) -> rusqlite::Result<EventLedgerEntry> {
    EventLedgerEntry::try_from(row)
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
