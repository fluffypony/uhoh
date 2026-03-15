use anyhow::{Context, Result};
use rusqlite::types::Value;
use rusqlite::{params, OptionalExtension, Row};

use super::{
    checked_usize_u64, Database, EventLedgerEntry, EventLedgerTraceResult, LedgerSeverity,
    LedgerSource, NewEventLedgerEntry,
};

#[non_exhaustive]
#[derive(Default)]
pub struct LedgerRecentFilters<'a> {
    pub source: Option<LedgerSource>,
    pub guard_name: Option<&'a str>,
    pub agent_name: Option<&'a str>,
    pub session: Option<&'a str>,
    pub since: Option<&'a str>,
}

pub fn build_recent_query(filters: LedgerRecentFilters<'_>, limit: i64) -> (String, Vec<Value>) {
    let mut where_clauses = Vec::new();
    let mut params = Vec::new();

    if let Some(source) = filters.source {
        where_clauses.push("source = ?");
        params.push(Value::from(source.as_str().to_string()));
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
    if let Some(since) = filters.since {
        where_clauses.push("ts >= ?");
        params.push(Value::from(since.to_string()));
    }

    let mut sql = String::from(
        "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
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
        let source_raw: String = row.get(2)?;
        let event_type_raw: String = row.get(3)?;
        let severity_raw: String = row.get(4)?;
        Ok(EventLedgerEntry {
            id: row.get(0)?,
            ts: row.get(1)?,
            source: LedgerSource::parse_persisted(&source_raw, 2)?,
            event_type: super::LedgerEventType::parse(&event_type_raw),
            severity: LedgerSeverity::parse_persisted(&severity_raw, 4)?,
            project_hash: row.get(5)?,
            agent_name: row.get(6)?,
            guard_name: row.get(7)?,
            path: row.get(8)?,
            detail: row.get(9)?,
            pre_state_ref: row.get(10)?,
            post_state_ref: row.get(11)?,
            causal_parent: row.get(12)?,
            resolved: row.get::<_, i32>(13)? != 0,
        })
    }
}

pub fn map_event_ledger_entry(row: &Row<'_>) -> rusqlite::Result<EventLedgerEntry> {
    EventLedgerEntry::try_from(row)
}

pub fn compute_event_chain_hash_with_id(
    prev_hash: &str,
    id: i64,
    event: &NewEventLedgerEntry,
    ts: &str,
) -> String {
    compute_event_chain_hash_with_id_raw(
        prev_hash,
        id,
        event.source.as_str(),
        event.event_type.as_str(),
        event.severity.as_str(),
        event,
        ts,
    )
}

pub fn compute_event_chain_hash_with_id_raw(
    prev_hash: &str,
    id: i64,
    source: &str,
    event_type: &str,
    severity: &str,
    event: &NewEventLedgerEntry,
    ts: &str,
) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(prev_hash.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(id.to_string().as_bytes());
    hasher.update(&[0u8]);
    hasher.update(ts.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(source.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(event_type.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(severity.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(event.project_hash.as_deref().unwrap_or("").as_bytes());
    hasher.update(&[0u8]);
    hasher.update(event.agent_name.as_deref().unwrap_or("").as_bytes());
    hasher.update(&[0u8]);
    hasher.update(event.guard_name.as_deref().unwrap_or("").as_bytes());
    hasher.update(&[0u8]);
    hasher.update(event.path.as_deref().unwrap_or("").as_bytes());
    hasher.update(&[0u8]);
    if let Some(detail) = event.detail.as_deref() {
        hasher.update(detail.as_bytes());
    }
    hasher.update(&[0u8]);
    hasher.update(event.pre_state_ref.as_deref().unwrap_or("").as_bytes());
    hasher.update(&[0u8]);
    hasher.update(event.post_state_ref.as_deref().unwrap_or("").as_bytes());
    hasher.update(&[0u8]);
    hasher.update(
        event
            .causal_parent
            .map(|v| v.to_string())
            .unwrap_or_default()
            .as_bytes(),
    );
    hasher.finalize().to_hex().to_string()
}

impl Database {
    /// Remove event ledger entries older than `ttl_days` to prevent unbounded growth.
    pub fn prune_event_ledger_ttl(&self, ttl_days: i64) -> Result<u64> {
        let conn = self.conn()?;
        let cutoff = chrono::Utc::now() - chrono::Duration::days(ttl_days);
        let cutoff_s = cutoff.to_rfc3339();
        let affected = conn.execute(
            "DELETE FROM event_ledger WHERE ts < ?1 AND resolved = 1",
            params![cutoff_s],
        )?;
        let affected = checked_usize_u64(affected, "event_ledger.delete_count")?;
        if affected > 0 {
            tracing::debug!(
                "Pruned {} resolved event ledger entries older than {} days",
                affected,
                ttl_days
            );
        }
        Ok(affected)
    }

    pub fn insert_event_ledger(&self, event: &NewEventLedgerEntry) -> Result<i64> {
        let mut conn = self.conn()?;
        let ts = chrono::Utc::now().to_rfc3339();
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let prev_hash = match event.prev_hash.clone() {
            Some(value) => value,
            None => self.latest_ledger_hash_with_conn(&tx)?.unwrap_or_default(),
        };
        tx.execute(
            "INSERT INTO event_ledger (
                ts, source, event_type, severity, project_hash, agent_name, guard_name,
                path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 0)",
            params![
                ts,
                event.source.as_str(),
                event.event_type.as_str(),
                event.severity.as_str(),
                event.project_hash,
                event.agent_name,
                event.guard_name,
                event.path,
                event.detail,
                event.pre_state_ref,
                event.post_state_ref,
                "",
                event.causal_parent,
            ],
        )?;
        let actual_id = tx.last_insert_rowid();
        let chain_hash = compute_event_chain_hash_with_id(&prev_hash, actual_id, event, &ts);
        tx.execute(
            "UPDATE event_ledger SET prev_hash = ?1 WHERE id = ?2",
            params![chain_hash, actual_id],
        )?;
        tx.commit()?;
        Ok(actual_id)
    }

    pub fn insert_event_ledger_batch(&self, events: &[NewEventLedgerEntry]) -> Result<usize> {
        if events.is_empty() {
            return Ok(0);
        }
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let mut prev_hash = self.latest_ledger_hash_with_conn(&tx)?.unwrap_or_default();
        for event in events {
            let ts = chrono::Utc::now().to_rfc3339();
            tx.execute(
                "INSERT INTO event_ledger (
                    ts, source, event_type, severity, project_hash, agent_name, guard_name,
                    path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, 0)",
                params![
                    ts,
                    event.source.as_str(),
                    event.event_type.as_str(),
                    event.severity.as_str(),
                    event.project_hash,
                    event.agent_name,
                    event.guard_name,
                    event.path,
                    event.detail,
                    event.pre_state_ref,
                    event.post_state_ref,
                    "",
                    event.causal_parent,
                ],
            )?;
            let actual_id = tx.last_insert_rowid();
            let chain_hash = compute_event_chain_hash_with_id(&prev_hash, actual_id, event, &ts);
            tx.execute(
                "UPDATE event_ledger SET prev_hash = ?1 WHERE id = ?2",
                params![chain_hash, actual_id],
            )?;
            prev_hash = chain_hash;
        }
        tx.commit()?;
        Ok(events.len())
    }

    pub fn verify_event_ledger_chain(&self) -> Result<(usize, Vec<i64>)> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                    path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
             FROM event_ledger ORDER BY id ASC",
        )?;
        let mut rows = stmt.query([])?;
        let mut prev_hash = String::new();
        let mut count = 0usize;
        let mut broken = Vec::new();
        while let Some(row) = rows.next()? {
            let id: i64 = row.get(0)?;
            let ts: String = row.get(1)?;
            let source_raw: String = row.get(2)?;
            let event_type_raw: String = row.get(3)?;
            let severity_raw: String = row.get(4)?;
            let event = NewEventLedgerEntry {
                source: LedgerSource::parse_persisted(&source_raw, 2)?,
                event_type: super::LedgerEventType::parse(&event_type_raw),
                severity: LedgerSeverity::parse_persisted(&severity_raw, 4)?,
                project_hash: row.get(5)?,
                agent_name: row.get(6)?,
                guard_name: row.get(7)?,
                path: row.get(8)?,
                detail: row.get(9)?,
                pre_state_ref: row.get(10)?,
                post_state_ref: row.get(11)?,
                prev_hash: None,
                causal_parent: row.get(13)?,
            };
            let stored_hash: Option<String> = row.get(12)?;
            let expected = compute_event_chain_hash_with_id_raw(
                &prev_hash,
                id,
                &source_raw,
                &event_type_raw,
                &severity_raw,
                &event,
                &ts,
            );
            if stored_hash.as_deref() != Some(expected.as_str()) {
                broken.push(id);
            }
            prev_hash = stored_hash.unwrap_or_default();
            count += 1;
        }
        Ok((count, broken))
    }

    pub fn event_ledger_recent(
        &self,
        filters: LedgerRecentFilters<'_>,
        limit: usize,
    ) -> Result<Vec<EventLedgerEntry>> {
        let conn = self.conn()?;
        let (sql, param_values) = build_recent_query(
            filters,
            limit as i64,
        );
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(
            rusqlite::params_from_iter(param_values.iter()),
            map_event_ledger_entry,
        )?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn event_ledger_get(&self, id: i64) -> Result<Option<EventLedgerEntry>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT id, ts, source, event_type, severity, project_hash, agent_name, guard_name,
                    path, detail, pre_state_ref, post_state_ref, causal_parent, resolved
             FROM event_ledger WHERE id = ?1",
            params![id],
            map_event_ledger_entry,
        )
        .optional()
        .context("Failed to fetch ledger event")
    }

    pub fn event_ledger_trace(&self, id: i64) -> Result<EventLedgerTraceResult> {
        let mut chain = Vec::new();
        let mut current = Some(id);
        let mut guard = 0usize;
        let mut truncated = false;
        while let Some(current_id) = current {
            if guard > 1024 {
                truncated = true;
                break;
            }
            if let Some(entry) = self.event_ledger_get(current_id)? {
                current = entry.causal_parent;
                chain.push(entry);
            } else {
                break;
            }
            guard += 1;
        }
        Ok(EventLedgerTraceResult {
            entries: chain,
            truncated,
        })
    }

    pub fn event_ledger_mark_resolved(&self, id: i64) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "UPDATE event_ledger SET resolved = 1 WHERE id = ?1",
            params![id],
        )?;
        Ok(())
    }

    /// Return the event ID and any descendants linked via causal_parent.
    /// Uses a recursive CTE with a hard depth/row guard to avoid runaway recursion
    /// on malformed graphs.
    pub fn event_ledger_descendant_ids(&self, root_id: i64) -> Result<Vec<i64>> {
        let limit = 10_000i64;
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT COUNT(*) FROM descendants",
            params![root_id, limit],
            |row| row.get(0),
        )?;
        if count >= limit {
            anyhow::bail!(
                "Descendant expansion reached limit of {limit} entries for root event #{root_id}"
            );
        }

        let mut stmt = conn.prepare(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT id FROM descendants",
        )?;
        let rows = stmt.query_map(params![root_id, limit], |row| row.get::<_, i64>(0))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    pub fn event_ledger_mark_resolved_cascade(&self, root_id: i64) -> Result<usize> {
        let limit = 10_000i64;
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT COUNT(*) FROM descendants",
            params![root_id, limit],
            |row| row.get(0),
        )?;
        if count >= limit {
            anyhow::bail!(
                "Cascade descendant expansion reached limit of {limit} entries for root event #{root_id}"
            );
        }
        let changed = conn.execute(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             UPDATE event_ledger
             SET resolved = 1
             WHERE id IN (SELECT id FROM descendants)",
            params![root_id, limit],
        )?;
        Ok(changed)
    }

    pub fn event_ledger_mark_resolved_cascade_with_session(
        &self,
        root_id: i64,
        session_id: &str,
    ) -> Result<usize> {
        let limit = 10_000i64;
        let conn = self.conn()?;
        let count: i64 = conn.query_row(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             SELECT COUNT(*)
             FROM event_ledger
             WHERE id IN (SELECT id FROM descendants)
               AND session_id = ?3",
            params![root_id, limit, session_id],
            |row| row.get(0),
        )?;
        if count >= limit {
            anyhow::bail!(
                "Cascade descendant expansion reached limit of {limit} entries for root event #{root_id}"
            );
        }
        let changed = conn.execute(
            "WITH RECURSIVE descendants(id, depth) AS (
                 SELECT ?1, 0
                 UNION ALL
                 SELECT e.id, d.depth + 1
                 FROM event_ledger e
                 JOIN descendants d ON e.causal_parent = d.id
                 WHERE d.depth < ?2
             )
             UPDATE event_ledger
             SET resolved = 1
             WHERE id IN (
                 SELECT id
                 FROM event_ledger
                 WHERE id IN (SELECT id FROM descendants)
                   AND session_id = ?3
             )",
            params![root_id, limit, session_id],
        )?;
        Ok(changed)
    }
}

#[cfg(test)]
mod tests {
    use super::compute_event_chain_hash_with_id;
    use super::{LedgerSeverity, LedgerSource, NewEventLedgerEntry};

    fn test_event(causal_parent: Option<i64>) -> NewEventLedgerEntry {
        NewEventLedgerEntry {
            source: LedgerSource::Agent,
            event_type: crate::db::LedgerEventType::ToolCall,
            severity: LedgerSeverity::Info,
            project_hash: Some("project".to_string()),
            agent_name: Some("agent".to_string()),
            guard_name: None,
            path: Some("src/lib.rs".to_string()),
            detail: Some("detail".to_string()),
            pre_state_ref: None,
            post_state_ref: None,
            causal_parent,
            prev_hash: None,
        }
    }

    #[test]
    fn chain_hash_changes_when_causal_parent_changes() {
        let ts = "2026-03-12T00:00:00Z";
        let first = compute_event_chain_hash_with_id("", 1, &test_event(Some(1)), ts);
        let second = compute_event_chain_hash_with_id("", 1, &test_event(Some(2)), ts);
        assert_ne!(first, second);
    }
}
