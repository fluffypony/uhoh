-- Consolidated uhoh schema (single-file, no incremental migrations)

CREATE TABLE IF NOT EXISTS projects (
    hash TEXT PRIMARY KEY,
    current_path TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    project_hash TEXT NOT NULL REFERENCES projects(hash) ON DELETE CASCADE,
    snapshot_id INTEGER NOT NULL CHECK (snapshot_id > 0),
    timestamp TEXT NOT NULL,
    trigger TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    pinned INTEGER NOT NULL DEFAULT 0,
    ai_summary TEXT,
    file_count INTEGER NOT NULL DEFAULT 0,
    UNIQUE(project_hash, snapshot_id)
);

CREATE TABLE IF NOT EXISTS snapshot_files (
    snapshot_rowid INTEGER NOT NULL REFERENCES snapshots(rowid) ON DELETE CASCADE,
    path TEXT NOT NULL,
    hash TEXT NOT NULL,
    size INTEGER NOT NULL,
    stored INTEGER NOT NULL DEFAULT 1,
    executable INTEGER NOT NULL DEFAULT 0,
    mtime INTEGER,
    storage_method INTEGER NOT NULL DEFAULT 1,
    is_symlink INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (snapshot_rowid, path)
);

CREATE TABLE IF NOT EXISTS snapshot_deleted (
    snapshot_rowid INTEGER NOT NULL REFERENCES snapshots(rowid) ON DELETE CASCADE,
    path TEXT NOT NULL,
    hash TEXT NOT NULL,
    size INTEGER NOT NULL,
    stored INTEGER NOT NULL DEFAULT 1,
    storage_method INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (snapshot_rowid, path)
);

CREATE TABLE IF NOT EXISTS pending_ai_summaries (
    snapshot_rowid INTEGER PRIMARY KEY,
    project_hash TEXT NOT NULL,
    queued_at TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    FOREIGN KEY(snapshot_rowid) REFERENCES snapshots(rowid) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS operations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_hash TEXT NOT NULL,
    label TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    first_snapshot_id INTEGER,
    last_snapshot_id INTEGER,
    FOREIGN KEY(project_hash) REFERENCES projects(hash) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS stats (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);

CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
    snapshot_rowid UNINDEXED,
    project_hash,
    trigger_type,
    message,
    ai_summary,
    file_paths
);

CREATE TABLE IF NOT EXISTS event_ledger (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    source TEXT NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    project_hash TEXT,
    agent_name TEXT,
    guard_name TEXT,
    path TEXT,
    detail TEXT,
    pre_state_ref TEXT,
    post_state_ref TEXT,
    prev_hash TEXT,
    causal_parent INTEGER REFERENCES event_ledger(id),
    resolved INTEGER NOT NULL DEFAULT 0,
    session_id TEXT GENERATED ALWAYS AS (
        CASE WHEN json_valid(detail) THEN json_extract(detail, '$.session_id') ELSE NULL END
    ) VIRTUAL
);

CREATE TABLE IF NOT EXISTS db_guards (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    engine TEXT NOT NULL,
    connection_ref TEXT NOT NULL,
    tables_csv TEXT NOT NULL,
    mode TEXT NOT NULL DEFAULT 'triggers',
    created_at TEXT NOT NULL,
    last_baseline_at TEXT,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS agents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    profile_path TEXT NOT NULL,
    profile_version INTEGER NOT NULL DEFAULT 1,
    data_dir TEXT,
    registered_at TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_snapshot_project ON snapshots(project_hash, timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshot_files_hash ON snapshot_files(hash);
CREATE INDEX IF NOT EXISTS idx_snapshot_deleted_hash ON snapshot_deleted(hash);
CREATE INDEX IF NOT EXISTS idx_file_path ON snapshot_files(path, snapshot_rowid);
CREATE INDEX IF NOT EXISTS idx_ai_queue_time ON pending_ai_summaries(queued_at);
CREATE INDEX IF NOT EXISTS idx_operations_project_started ON operations(project_hash, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_event_ledger_ts ON event_ledger(ts);
CREATE INDEX IF NOT EXISTS idx_event_ledger_source ON event_ledger(source);
CREATE INDEX IF NOT EXISTS idx_event_ledger_agent ON event_ledger(agent_name);
CREATE INDEX IF NOT EXISTS idx_event_ledger_guard ON event_ledger(guard_name);
CREATE INDEX IF NOT EXISTS idx_event_ledger_path ON event_ledger(path);
CREATE INDEX IF NOT EXISTS idx_event_ledger_causal ON event_ledger(causal_parent);
CREATE INDEX IF NOT EXISTS idx_event_ledger_severity ON event_ledger(severity);
CREATE INDEX IF NOT EXISTS idx_event_ledger_prev_hash ON event_ledger(prev_hash);
CREATE INDEX IF NOT EXISTS idx_event_ledger_session ON event_ledger(session_id);

INSERT OR IGNORE INTO stats (key, value) VALUES ('blob_bytes', 0);
