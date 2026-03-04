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
    ProjectAdded {
        project_hash: String,
        path: String,
    },
    ProjectRemoved {
        project_hash: String,
    },
}
