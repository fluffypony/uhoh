use std::sync::Arc;

use anyhow::Result;
use tokio::sync::broadcast;

use crate::db::{Database, EventLedgerEntry, EventLedgerTraceResult, NewEventLedgerEntry};
use crate::server::events::ServerEvent;

#[derive(Clone)]
pub struct EventLedger {
    db: Arc<Database>,
    server_event_tx: Option<broadcast::Sender<ServerEvent>>,
}

impl EventLedger {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            server_event_tx: None,
        }
    }

    /// Attach a broadcast sender so that persisted events are automatically
    /// surfaced as `ServerEvent`s (for WebSocket, notifications, webhooks).
    pub fn with_server_event_tx(mut self, tx: broadcast::Sender<ServerEvent>) -> Self {
        self.server_event_tx = Some(tx);
        self
    }

    pub fn append(&self, event: NewEventLedgerEntry) -> Result<i64> {
        let id = self.db.insert_event_ledger(&event)?;

        // Bridge: map persisted event to ServerEvent and broadcast
        if let Some(ref tx) = self.server_event_tx {
            if let Some(server_event) = map_to_server_event(&event) {
                let _ = tx.send(server_event);
            }
        }

        Ok(id)
    }

    pub fn recent(
        &self,
        source: Option<&str>,
        guard_name: Option<&str>,
        agent_name: Option<&str>,
        session: Option<&str>,
        limit: usize,
    ) -> Result<Vec<EventLedgerEntry>> {
        self.db
            .event_ledger_recent(source, guard_name, agent_name, session, limit)
    }

    pub fn trace(&self, id: i64) -> Result<EventLedgerTraceResult> {
        self.db.event_ledger_trace(id)
    }

    pub fn mark_resolved(&self, id: i64) -> Result<()> {
        self.db.event_ledger_mark_resolved(id)
    }
}

/// Map a persisted event ledger entry to a `ServerEvent` for real-time broadcast.
/// Only critical/warn-level events from db_guard and agent sources are bridged,
/// since other event types (snapshot, restore, etc.) are already emitted directly.
fn map_to_server_event(event: &NewEventLedgerEntry) -> Option<ServerEvent> {
    match (event.source.as_str(), event.severity.as_str()) {
        ("db_guard", "critical" | "warn" | "error") => Some(ServerEvent::DbGuardAlert {
            guard_name: event.guard_name.clone().unwrap_or_default(),
            event_type: event.event_type.clone(),
            severity: event.severity.clone(),
            detail: event.detail.clone().unwrap_or_default(),
        }),
        ("agent", "critical" | "warn" | "error") => Some(ServerEvent::AgentAlert {
            agent_name: event.agent_name.clone().unwrap_or_default(),
            event_type: event.event_type.clone(),
            severity: event.severity.clone(),
            detail: event.detail.clone().unwrap_or_default(),
        }),
        _ => None,
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
        prev_hash: None,
        causal_parent: None,
    }
}
