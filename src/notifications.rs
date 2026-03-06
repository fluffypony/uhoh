use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, Mutex};

use crate::config::NotificationsConfig;
use crate::server::events::ServerEvent;

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
            while let Ok(event) = rx.recv().await {
                me.process_event(event).await;
            }
        });
    }

    async fn process_event(&self, event: ServerEvent) {
        let event_type = event.kind();
        let summary = event.summary();
        // Include project/guard/agent identifiers in dedupe key to prevent
        // one project's alert from suppressing another's during cooldown
        let dedupe_key = Self::dedupe_key(&event);
        if !self.should_emit(&dedupe_key).await {
            return;
        }

        if self.cfg.desktop {
            let title = if event_type == "emergency_delete_detected" {
                "uhoh EMERGENCY"
            } else {
                "uhoh"
            };
            let _ = send_desktop_notification(title, &summary).await;
        }

        if !self.cfg.webhook_url.trim().is_empty()
            && self
                .cfg
                .webhook_events
                .iter()
                .any(|v| v.eq_ignore_ascii_case(&event_type))
        {
            let payload = serde_json::json!({
                "event_type": event_type,
                "summary": summary,
                "event": event,
            });
            let _ = reqwest::Client::new()
                .post(self.cfg.webhook_url.clone())
                .json(&payload)
                .send()
                .await;
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
            other => other.kind(),
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
