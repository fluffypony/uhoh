use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use crossbeam::queue::ArrayQueue;

use crate::db::{Database, EventLedgerEntry, EventLedgerTraceResult, NewEventLedgerEntry};

#[derive(Clone)]
pub struct EventLedger {
    db: Arc<Database>,
    queue: Arc<ArrayQueue<NewEventLedgerEntry>>,
    started: Arc<AtomicBool>,
}

impl EventLedger {
    pub fn new(db: Arc<Database>) -> Self {
        Self {
            db,
            queue: Arc::new(ArrayQueue::new(8192)),
            started: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start_flusher(&self) {
        if self.started.swap(true, Ordering::SeqCst) {
            return;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            return;
        }
        let db = self.db.clone();
        let queue = self.queue.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let mut batch = Vec::new();
                while let Some(event) = queue.pop() {
                    batch.push(event);
                    if batch.len() >= 256 {
                        break;
                    }
                }
                if batch.is_empty() {
                    continue;
                }
                let _ = db.insert_event_ledger_batch(&batch);
            }
        });
    }

    pub fn append(&self, event: NewEventLedgerEntry) -> Result<i64> {
        if self.queue.push(event.clone()).is_err() {
            self.db.insert_event_ledger(&event)
        } else {
            Ok(0)
        }
    }

    pub fn flush(&self) -> Result<usize> {
        let mut batch = Vec::new();
        while let Some(event) = self.queue.pop() {
            batch.push(event);
            if batch.len() >= 1024 {
                break;
            }
        }
        self.db.insert_event_ledger_batch(&batch)
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
