use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::mpsc;

use crate::db::{Database, EventLedgerEntry, EventLedgerTraceResult, NewEventLedgerEntry};

#[derive(Clone)]
pub struct EventLedger {
    db: Arc<Database>,
    queue_tx: mpsc::UnboundedSender<NewEventLedgerEntry>,
    queue_rx: Arc<std::sync::Mutex<Option<mpsc::UnboundedReceiver<NewEventLedgerEntry>>>>,
    started: Arc<AtomicBool>,
}

impl EventLedger {
    pub fn new(db: Arc<Database>) -> Self {
        let (queue_tx, queue_rx) = mpsc::unbounded_channel();
        Self {
            db,
            queue_tx,
            queue_rx: Arc::new(std::sync::Mutex::new(Some(queue_rx))),
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
        let mut queue = match self.queue_rx.lock() {
            Ok(mut guard) => match guard.take() {
                Some(rx) => rx,
                None => return,
            },
            Err(_) => return,
        };
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(200)).await;
                let mut batch = Vec::new();
                while let Ok(event) = queue.try_recv() {
                    batch.push(event);
                    if batch.len() >= 256 {
                        break;
                    }
                }
                if batch.is_empty() {
                    continue;
                }
                if let Err(err) = db.insert_event_ledger_batch(&batch) {
                    tracing::error!("failed to flush event ledger batch: {err}");
                }
            }
        });
    }

    pub fn append(&self, event: NewEventLedgerEntry) -> Result<i64> {
        // Always insert directly to return a valid event ID.
        // The flusher handles async batching of events that don't need IDs.
        self.db.insert_event_ledger(&event)
    }

    pub fn flush(&self) -> Result<usize> {
        let mut batch = Vec::new();
        if let Ok(mut guard) = self.queue_rx.lock() {
            if let Some(queue) = guard.as_mut() {
                while let Ok(event) = queue.try_recv() {
                    batch.push(event);
                    if batch.len() >= 1024 {
                        break;
                    }
                }
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
