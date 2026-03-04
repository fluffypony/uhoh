CREATE TABLE IF NOT EXISTS pending_ai_summaries (
    snapshot_rowid INTEGER PRIMARY KEY,
    project_hash TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(snapshot_rowid) REFERENCES snapshots(rowid) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ai_queue_time ON pending_ai_summaries(queued_at);

ALTER TABLE snapshots ADD COLUMN file_count INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_hash TEXT NOT NULL,
    label TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    start_snapshot_id INTEGER,
    end_snapshot_id INTEGER,
    FOREIGN KEY(project_hash) REFERENCES projects(hash) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_operations_project_started ON operations(project_hash, started_at DESC);

PRAGMA user_version = 4;
