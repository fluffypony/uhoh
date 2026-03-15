use std::sync::Arc;

use anyhow::Result;
use tokio::sync::broadcast;

use crate::db::{Database, LedgerEventType, LedgerSeverity, LedgerSource, NewEventLedgerEntry};
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
    event_type: impl Into<LedgerEventType>,
    severity: LedgerSeverity,
) -> NewEventLedgerEntry {
    NewEventLedgerEntry {
        source,
        event_type: event_type.into(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_event_sets_required_fields() {
        let e = new_event(LedgerSource::Agent, LedgerEventType::ToolCall, LedgerSeverity::Info);
        assert_eq!(e.source, LedgerSource::Agent);
        assert_eq!(e.event_type, LedgerEventType::ToolCall);
        assert_eq!(e.severity, LedgerSeverity::Info);
    }

    #[test]
    fn new_event_optional_fields_are_none() {
        let e = new_event(LedgerSource::Fs, LedgerEventType::EmergencyDeleteDetected, LedgerSeverity::Critical);
        assert!(e.project_hash.is_none());
        assert!(e.agent_name.is_none());
        assert!(e.guard_name.is_none());
        assert!(e.path.is_none());
        assert!(e.detail.is_none());
        assert!(e.pre_state_ref.is_none());
        assert!(e.post_state_ref.is_none());
        assert!(e.prev_hash.is_none());
        assert!(e.causal_parent.is_none());
    }

    #[test]
    fn new_event_accepts_str_event_type() {
        let e = new_event(LedgerSource::Daemon, "custom_event", LedgerSeverity::Warn);
        assert_eq!(e.event_type, LedgerEventType::Other("custom_event".to_string()));
    }

    #[test]
    fn new_event_accepts_known_str_event_type() {
        let e = new_event(LedgerSource::Daemon, "tool_call", LedgerSeverity::Info);
        // "tool_call" as &str converts via LedgerEventType::parse, which returns Other if
        // the From<&str> impl goes through parse
        assert_eq!(e.event_type, LedgerEventType::ToolCall);
    }

    #[test]
    fn new_event_each_source_variant() {
        for source in [
            LedgerSource::Agent,
            LedgerSource::DbGuard,
            LedgerSource::Daemon,
            LedgerSource::Fs,
            LedgerSource::Mlx,
        ] {
            let e = new_event(source, LedgerEventType::AuditTick, LedgerSeverity::Info);
            assert_eq!(e.source, source);
        }
    }

    #[test]
    fn new_event_each_severity_variant() {
        for sev in [
            LedgerSeverity::Info,
            LedgerSeverity::Warn,
            LedgerSeverity::Error,
            LedgerSeverity::Critical,
        ] {
            let e = new_event(LedgerSource::Fs, LedgerEventType::AuditTick, sev);
            assert_eq!(e.severity, sev);
        }
    }

    #[test]
    fn event_ledger_new_creates_instance() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = std::sync::Arc::new(Database::open(&db_path).unwrap());
        let ledger = EventLedger::new(db.clone());

        // Append an event
        let event = new_event(LedgerSource::Fs, LedgerEventType::EmergencyDeleteDetected, LedgerSeverity::Critical);
        let id = ledger.append(event).unwrap();
        assert!(id > 0);
    }

    #[test]
    fn event_ledger_with_publisher() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let db = std::sync::Arc::new(Database::open(&db_path).unwrap());
        let (tx, _rx) = tokio::sync::broadcast::channel(16);
        let ledger = EventLedger::new(db.clone()).with_event_publisher(tx);

        let event = new_event(LedgerSource::Agent, LedgerEventType::ToolCall, LedgerSeverity::Info);
        let id = ledger.append(event).unwrap();
        assert!(id > 0);
    }
}
