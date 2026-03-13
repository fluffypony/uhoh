use std::sync::Arc;

use anyhow::Result;
use tokio::sync::broadcast;

use crate::db::{Database, LedgerSeverity, LedgerSource, NewEventLedgerEntry};
use crate::events::{publish_ledger_event, ServerEvent};

#[derive(Clone)]
pub struct EventLedger {
    db: Arc<Database>,
    event_publisher: Option<broadcast::Sender<ServerEvent>>,
}

impl EventLedger {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            event_publisher: None,
        }
    }

    /// Attach a broadcast sender so that persisted events are automatically
    /// surfaced as `ServerEvent`s (for WebSocket, notifications, webhooks).
    pub fn with_event_publisher(mut self, tx: broadcast::Sender<ServerEvent>) -> Self {
        self.event_publisher = Some(tx);
        self
    }

    pub fn append(&self, event: NewEventLedgerEntry) -> Result<i64> {
        let id = self.db.insert_event_ledger(&event)?;

        // Bridge: map persisted event to ServerEvent and broadcast
        if let Some(ref tx) = self.event_publisher {
            publish_ledger_event(tx, &event);
        }

        Ok(id)
    }
}

pub fn new_event(
    source: LedgerSource,
    event_type: &str,
    severity: LedgerSeverity,
) -> NewEventLedgerEntry {
    NewEventLedgerEntry {
        source,
        event_type: event_type.to_string(),
        severity,
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
