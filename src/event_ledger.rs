use std::sync::Arc;

use anyhow::Result;

use crate::db::{Database, EventLedgerEntry, NewEventLedgerEntry};

#[derive(Clone)]
pub struct EventLedger {
    db: Arc<Database>,
}

impl EventLedger {
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }

    pub fn append(&self, event: NewEventLedgerEntry) -> Result<i64> {
        self.db.insert_event_ledger(&event)
    }

    pub fn recent(
        &self,
        source: Option<&str>,
        guard_name: Option<&str>,
        agent_name: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventLedgerEntry>> {
        self.db
            .event_ledger_recent(source, guard_name, agent_name, limit)
    }

    pub fn trace(&self, id: i64) -> Result<Vec<EventLedgerEntry>> {
        self.db.event_ledger_trace(id)
    }

    pub fn mark_resolved(&self, id: i64) -> Result<()> {
        self.db.event_ledger_mark_resolved(id)
    }
}

pub fn new_event(source: &str, event_type: &str, severity: &str) -> NewEventLedgerEntry {
    NewEventLedgerEntry {
        source: source.to_string(),
        event_type: event_type.to_string(),
        severity: severity.to_string(),
        project_hash: None,
        agent_name: None,
        guard_name: None,
        path: None,
        detail: None,
        pre_state_ref: None,
        post_state_ref: None,
        causal_parent: None,
    }
}
