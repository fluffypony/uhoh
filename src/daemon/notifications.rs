use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, Mutex};

use crate::config::{NotificationsConfig, WebhookEventKind};
use crate::events::{ServerEvent, ServerEventKind};

#[derive(Clone)]
pub struct NotificationPipeline {
    cfg: NotificationsConfig,
    dedup: Arc<Mutex<HashMap<String, Instant>>>,
}

impl NotificationPipeline {
    pub fn new(cfg: NotificationsConfig) -> Self {
        Self {
            cfg,
            dedup: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn spawn(&self, event_tx: broadcast::Sender<ServerEvent>) {
        let mut rx = event_tx.subscribe();
        let me = self.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(event) => {
                        me.process_event(event).await;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!("Notification pipeline lagged, missed {} events", n);
                        continue;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    }

    async fn process_event(&self, event: ServerEvent) {
        let event_kind = event.kind();
        let summary = event.summary();
        // Include project/guard/agent identifiers in dedupe key to prevent
        // one project's alert from suppressing another's during cooldown
        let dedupe_key = Self::dedupe_key(&event);
        if !self.should_emit(&dedupe_key).await {
            return;
        }

        if self.cfg.desktop {
            let title = if event_kind.is_emergency() {
                "uhoh EMERGENCY"
            } else {
                "uhoh"
            };
            if let Err(e) = send_desktop_notification(title, &summary).await {
                tracing::warn!("Desktop notification failed: {e}");
            }
        }

        if !self.cfg.webhook_url.trim().is_empty()
            && Self::webhook_kind(&event_kind).is_some_and(|kind| {
                self.cfg
                    .webhook_events
                    .iter()
                    .any(|configured| configured == &kind)
            })
        {
            let payload = serde_json::json!({
                "event_type": event_kind.as_str(),
                "summary": summary,
                "event": event,
            });
            match reqwest::Client::new()
                .post(self.cfg.webhook_url.clone())
                .json(&payload)
                .send()
                .await
            {
                Ok(resp) if !resp.status().is_success() => {
                    tracing::warn!("Webhook delivery returned {}", resp.status());
                }
                Err(e) => {
                    tracing::warn!("Webhook delivery failed: {e}");
                }
                _ => {}
            }
        }
    }

    fn dedupe_key(event: &ServerEvent) -> String {
        match event {
            ServerEvent::SnapshotCreated { project_hash, .. } => {
                format!("snapshot_created:{project_hash}")
            }
            ServerEvent::SnapshotRestored { project_hash, .. } => {
                format!("snapshot_restored:{project_hash}")
            }
            ServerEvent::EmergencyDeleteDetected { project_hash, .. } => {
                format!("emergency_delete_detected:{project_hash}")
            }
            ServerEvent::DbGuardAlert {
                guard_name,
                event_type,
                ..
            } => format!("db_guard_alert:{guard_name}:{event_type}"),
            ServerEvent::AgentAlert {
                agent_name,
                event_type,
                ..
            } => format!("agent_alert:{agent_name}:{event_type}"),
            other => other.kind().as_str().to_string(),
        }
    }

    fn webhook_kind(kind: &ServerEventKind) -> Option<WebhookEventKind> {
        match kind {
            ServerEventKind::DbGuard(event_type) => match event_type.as_str() {
                "mass_delete" => Some(WebhookEventKind::MassDelete),
                "mass_delete_pct" => Some(WebhookEventKind::MassDeletePct),
                "drop_table" => Some(WebhookEventKind::DropTable),
                "drop_column" => Some(WebhookEventKind::DropColumn),
                "truncate" => Some(WebhookEventKind::Truncate),
                _ => None,
            },
            ServerEventKind::Agent(event_type) if event_type == "dangerous_agent_action" => {
                Some(WebhookEventKind::DangerousAgentAction)
            }
            ServerEventKind::MlxUpdateFailed => Some(WebhookEventKind::MlxUpdateFailed),
            ServerEventKind::EmergencyDeleteDetected => {
                Some(WebhookEventKind::EmergencyDeleteDetected)
            }
            _ => None,
        }
    }

    async fn should_emit(&self, key: &str) -> bool {
        let mut guard = self.dedup.lock().await;
        let now = Instant::now();
        let cooldown = Duration::from_secs(self.cfg.cooldown_seconds.max(1));
        match guard.get(key) {
            Some(last) if now.duration_since(*last) < cooldown => false,
            _ => {
                guard.insert(key.to_string(), now);
                true
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use crate::db::LedgerSeverity;

    #[test]
    fn dedupe_key_snapshot_created() {
        let event = ServerEvent::SnapshotCreated {
            project_hash: "abc".to_string(),
            snapshot_id: "1".to_string(),
            timestamp: String::new(),
            trigger: crate::db::SnapshotTrigger::Auto,
            file_count: 0,
            message: None,
        };
        assert_eq!(
            NotificationPipeline::dedupe_key(&event),
            "snapshot_created:abc"
        );
    }

    #[test]
    fn dedupe_key_emergency() {
        let event = ServerEvent::EmergencyDeleteDetected {
            project_hash: "xyz".to_string(),
            deleted_count: 10,
            baseline_count: 100,
            ratio: 0.1,
            threshold: 0.3,
            min_files: 5,
            cooldown_suppressed: false,
            cooldown_remaining_secs: None,
        };
        assert_eq!(
            NotificationPipeline::dedupe_key(&event),
            "emergency_delete_detected:xyz"
        );
    }

    #[test]
    fn dedupe_key_db_guard_includes_guard_and_type() {
        let event = ServerEvent::DbGuardAlert {
            guard_name: "myguard".to_string(),
            event_type: "drop_table".to_string(),
            severity: LedgerSeverity::Critical,
            detail: String::new(),
        };
        assert_eq!(
            NotificationPipeline::dedupe_key(&event),
            "db_guard_alert:myguard:drop_table"
        );
    }

    #[test]
    fn dedupe_key_agent_includes_agent_and_type() {
        let event = ServerEvent::AgentAlert {
            agent_name: "claude".to_string(),
            event_type: "dangerous_agent_action".to_string(),
            severity: LedgerSeverity::Warn,
            detail: String::new(),
        };
        assert_eq!(
            NotificationPipeline::dedupe_key(&event),
            "agent_alert:claude:dangerous_agent_action"
        );
    }

    #[test]
    fn webhook_kind_mass_delete() {
        let kind = ServerEventKind::DbGuard("mass_delete".to_string());
        assert_eq!(
            NotificationPipeline::webhook_kind(&kind),
            Some(WebhookEventKind::MassDelete)
        );
    }

    #[test]
    fn webhook_kind_drop_table() {
        let kind = ServerEventKind::DbGuard("drop_table".to_string());
        assert_eq!(
            NotificationPipeline::webhook_kind(&kind),
            Some(WebhookEventKind::DropTable)
        );
    }

    #[test]
    fn webhook_kind_unknown_db_guard() {
        let kind = ServerEventKind::DbGuard("unknown_event".to_string());
        assert_eq!(NotificationPipeline::webhook_kind(&kind), None);
    }

    #[test]
    fn webhook_kind_dangerous_agent() {
        let kind = ServerEventKind::Agent("dangerous_agent_action".to_string());
        assert_eq!(
            NotificationPipeline::webhook_kind(&kind),
            Some(WebhookEventKind::DangerousAgentAction)
        );
    }

    #[test]
    fn webhook_kind_emergency_delete() {
        let kind = ServerEventKind::EmergencyDeleteDetected;
        assert_eq!(
            NotificationPipeline::webhook_kind(&kind),
            Some(WebhookEventKind::EmergencyDeleteDetected)
        );
    }

    #[test]
    fn webhook_kind_snapshot_created_returns_none() {
        let kind = ServerEventKind::SnapshotCreated;
        assert_eq!(NotificationPipeline::webhook_kind(&kind), None);
    }

    #[test]
    fn notification_pipeline_new() {
        let cfg = NotificationsConfig::default();
        let pipeline = NotificationPipeline::new(cfg);
        // Should construct without panic
        let _ = pipeline;
    }
}

async fn send_desktop_notification(title: &str, message: &str) -> std::io::Result<()> {
    let is_critical = title.contains("EMERGENCY");

    #[cfg(target_os = "macos")]
    {
        let script = if is_critical {
            format!(
                "display notification \"{}\" with title \"{}\" sound name \"Sosumi\"",
                message.replace('"', "\\\""),
                title.replace('"', "\\\"")
            )
        } else {
            format!(
                "display notification \"{}\" with title \"{}\"",
                message.replace('"', "\\\""),
                title.replace('"', "\\\"")
            )
        };
        tokio::process::Command::new("osascript")
            .arg("-e")
            .arg(script)
            .status()
            .await
            .map(|_| ())
    }

    #[cfg(target_os = "linux")]
    {
        let mut cmd = tokio::process::Command::new("notify-send");
        if is_critical {
            cmd.arg("--urgency=critical");
        }
        cmd.arg(title).arg(message).status().await.map(|_| ())
    }

    #[cfg(target_os = "windows")]
    {
        let script = format!(
            "[Windows.UI.Notifications.ToastNotificationManager, Windows.UI.Notifications, ContentType = WindowsRuntime] > $null; \
             [Windows.Data.Xml.Dom.XmlDocument, Windows.Data.Xml.Dom.XmlDocument, ContentType = WindowsRuntime] > $null; \
             $template = '<toast><visual><binding template=\"ToastGeneric\"><text>{}</text><text>{}</text></binding></visual></toast>'; \
             $xml = New-Object Windows.Data.Xml.Dom.XmlDocument; $xml.LoadXml($template); \
             $toast = [Windows.UI.Notifications.ToastNotification]::new($xml); \
             [Windows.UI.Notifications.ToastNotificationManager]::CreateToastNotifier('uhoh').Show($toast)",
            title.replace('"', "'"),
            message.replace('"', "'")
        );
        tokio::process::Command::new("powershell")
            .arg("-NoProfile")
            .arg("-ExecutionPolicy")
            .arg("Bypass")
            .arg("-Command")
            .arg(script)
            .status()
            .await
            .map(|_| ())
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let _ = (title, message);
        Ok(())
    }
}
