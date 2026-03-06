use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    SnapshotCreated {
        project_hash: String,
        snapshot_id: String,
        timestamp: String,
        trigger: String,
        file_count: usize,
        message: Option<String>,
    },
    SnapshotRestored {
        project_hash: String,
        snapshot_id: String,
        files_modified: usize,
        files_deleted: usize,
    },
    AiSummaryCompleted {
        project_hash: String,
        snapshot_id: String,
        summary: String,
    },
    SidecarUpdated {
        old_version: Option<String>,
        new_version: String,
    },
    MlxUpdateStatus {
        status: String,
        detail: String,
    },
    MlxUpdateFailed {
        status: String,
        detail: String,
    },
    DbGuardAlert {
        guard_name: String,
        event_type: String,
        severity: String,
        detail: String,
    },
    AgentAlert {
        agent_name: String,
        event_type: String,
        severity: String,
        detail: String,
    },
    ProjectAdded {
        project_hash: String,
        path: String,
    },
    ProjectRemoved {
        project_hash: String,
    },
    EmergencyDeleteDetected {
        project_hash: String,
        deleted_count: usize,
        baseline_count: u64,
        ratio: f64,
        threshold: f64,
        min_files: usize,
        cooldown_suppressed: bool,
        cooldown_remaining_secs: Option<u64>,
    },
}

impl ServerEvent {
    pub fn kind(&self) -> String {
        match self {
            ServerEvent::SnapshotCreated { .. } => "snapshot_created".to_string(),
            ServerEvent::SnapshotRestored { .. } => "snapshot_restored".to_string(),
            ServerEvent::AiSummaryCompleted { .. } => "ai_summary_completed".to_string(),
            ServerEvent::SidecarUpdated { .. } => "sidecar_updated".to_string(),
            ServerEvent::MlxUpdateStatus { .. } => "mlx_update_status".to_string(),
            ServerEvent::MlxUpdateFailed { .. } => "mlx_update_failed".to_string(),
            ServerEvent::DbGuardAlert { event_type, .. } => event_type.clone(),
            ServerEvent::AgentAlert { event_type, .. } => event_type.clone(),
            ServerEvent::ProjectAdded { .. } => "project_added".to_string(),
            ServerEvent::ProjectRemoved { .. } => "project_removed".to_string(),
            ServerEvent::EmergencyDeleteDetected { .. } => "emergency_delete_detected".to_string(),
        }
    }

    pub fn summary(&self) -> String {
        match self {
            ServerEvent::SnapshotCreated {
                project_hash,
                snapshot_id,
                ..
            } => {
                format!("Snapshot {snapshot_id} created for {project_hash}")
            }
            ServerEvent::SnapshotRestored {
                project_hash,
                snapshot_id,
                ..
            } => {
                format!("Snapshot {snapshot_id} restored for {project_hash}")
            }
            ServerEvent::AiSummaryCompleted { snapshot_id, .. } => {
                format!("AI summary completed for snapshot {snapshot_id}")
            }
            ServerEvent::SidecarUpdated {
                old_version,
                new_version,
            } => {
                format!("Sidecar updated: {:?} -> {}", old_version, new_version)
            }
            ServerEvent::MlxUpdateStatus { status, detail } => {
                format!("MLX update {status}: {detail}")
            }
            ServerEvent::MlxUpdateFailed { status, detail } => {
                format!("MLX update failed {status}: {detail}")
            }
            ServerEvent::DbGuardAlert {
                guard_name,
                event_type,
                severity,
                ..
            } => {
                format!("DB guard {guard_name}: {event_type} ({severity})")
            }
            ServerEvent::AgentAlert {
                agent_name,
                event_type,
                severity,
                ..
            } => {
                format!("Agent {agent_name}: {event_type} ({severity})")
            }
            ServerEvent::ProjectAdded { path, .. } => format!("Project added: {path}"),
            ServerEvent::ProjectRemoved { project_hash } => {
                format!("Project removed: {project_hash}")
            }
            ServerEvent::EmergencyDeleteDetected {
                deleted_count,
                baseline_count,
                ratio,
                cooldown_suppressed,
                cooldown_remaining_secs,
                ..
            } => {
                if *cooldown_suppressed {
                    let remaining = cooldown_remaining_secs.unwrap_or(0);
                    format!(
                        "Emergency threshold exceeded but cooldown active — {}/{} files ({:.1}%), {}s remaining",
                        deleted_count,
                        baseline_count,
                        ratio * 100.0,
                        remaining
                    )
                } else {
                    format!(
                        "Emergency: mass delete detected — {}/{} files ({:.1}%)",
                        deleted_count,
                        baseline_count,
                        ratio * 100.0
                    )
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ServerEvent;

    #[test]
    fn server_event_kind_matches_expected_values() {
        let ev = ServerEvent::ProjectAdded {
            project_hash: "abc".to_string(),
            path: "/tmp/demo".to_string(),
        };
        assert_eq!(ev.kind(), "project_added");

        let ev = ServerEvent::DbGuardAlert {
            guard_name: "g".to_string(),
            event_type: "mass_delete".to_string(),
            severity: "critical".to_string(),
            detail: "{}".to_string(),
        };
        assert_eq!(ev.kind(), "mass_delete");
    }
}
