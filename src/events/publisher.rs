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
