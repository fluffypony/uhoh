use tokio::sync::broadcast;

use crate::db::{LedgerSeverity, LedgerSource, NewEventLedgerEntry};

use super::ServerEvent;

pub fn publish_event(event_tx: &broadcast::Sender<ServerEvent>, event: ServerEvent) {
    let _ = event_tx.send(event);
}

pub fn publish_ledger_event(
    event_tx: &broadcast::Sender<ServerEvent>,
    event: &NewEventLedgerEntry,
) {
    if let Some(server_event) = map_ledger_event(event) {
        publish_event(event_tx, server_event);
    }
}

fn map_ledger_event(event: &NewEventLedgerEntry) -> Option<ServerEvent> {
    match (event.source, event.severity) {
        (
            LedgerSource::DbGuard,
            LedgerSeverity::Critical | LedgerSeverity::Warn | LedgerSeverity::Error,
        ) => Some(ServerEvent::DbGuardAlert {
            guard_name: event.guard_name.clone().unwrap_or_default(),
            event_type: event.event_type.as_str().to_string(),
            severity: event.severity,
            detail: event.detail.clone().unwrap_or_default(),
        }),
        (
            LedgerSource::Agent,
            LedgerSeverity::Critical | LedgerSeverity::Warn | LedgerSeverity::Error,
        ) => Some(ServerEvent::AgentAlert {
            agent_name: event.agent_name.clone().unwrap_or_default(),
            event_type: event.event_type.as_str().to_string(),
            severity: event.severity,
            detail: event.detail.clone().unwrap_or_default(),
        }),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::db::LedgerEventType;
    use crate::event_ledger::new_event;

    #[test]
    fn map_db_guard_critical_to_alert() {
        let event = new_event(
            LedgerSource::DbGuard,
            LedgerEventType::DropTable,
            LedgerSeverity::Critical,
        );
        let result = map_ledger_event(&event);
        assert!(result.is_some());
        match result.unwrap() {
            ServerEvent::DbGuardAlert { severity, .. } => {
                assert_eq!(severity, LedgerSeverity::Critical);
            }
            other => panic!("Expected DbGuardAlert, got {other:?}"),
        }
    }

    #[test]
    fn map_db_guard_warn_to_alert() {
        let event = new_event(
            LedgerSource::DbGuard,
            LedgerEventType::SchemaChange,
            LedgerSeverity::Warn,
        );
        assert!(map_ledger_event(&event).is_some());
    }

    #[test]
    fn map_db_guard_info_returns_none() {
        let event = new_event(
            LedgerSource::DbGuard,
            LedgerEventType::GuardStarted,
            LedgerSeverity::Info,
        );
        assert!(map_ledger_event(&event).is_none());
    }

    #[test]
    fn map_agent_critical_to_alert() {
        let mut event = new_event(
            LedgerSource::Agent,
            LedgerEventType::DangerousAgentAction,
            LedgerSeverity::Critical,
        );
        event.agent_name = Some("test-agent".to_string());
        let result = map_ledger_event(&event);
        assert!(result.is_some());
        match result.unwrap() {
            ServerEvent::AgentAlert { agent_name, .. } => {
                assert_eq!(agent_name, "test-agent");
            }
            other => panic!("Expected AgentAlert, got {other:?}"),
        }
    }

    #[test]
    fn map_agent_info_returns_none() {
        let event = new_event(
            LedgerSource::Agent,
            LedgerEventType::ToolCall,
            LedgerSeverity::Info,
        );
        assert!(map_ledger_event(&event).is_none());
    }

    #[test]
    fn map_fs_event_returns_none() {
        let event = new_event(
            LedgerSource::Fs,
            LedgerEventType::EmergencyDeleteDetected,
            LedgerSeverity::Critical,
        );
        assert!(map_ledger_event(&event).is_none());
    }

    #[test]
    fn map_daemon_event_returns_none() {
        let event = new_event(
            LedgerSource::Daemon,
            LedgerEventType::ConfigReloadFailed,
            LedgerSeverity::Error,
        );
        assert!(map_ledger_event(&event).is_none());
    }

    #[test]
    fn publish_event_does_not_panic_on_no_receivers() {
        let (tx, _rx) = broadcast::channel(1);
        drop(_rx);
        publish_event(&tx, ServerEvent::DbGuardAlert {
            guard_name: "test".to_string(),
            event_type: "test".to_string(),
            severity: LedgerSeverity::Info,
            detail: String::new(),
        });
        // Should not panic even with no receivers
    }
}
