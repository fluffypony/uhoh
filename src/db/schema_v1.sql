CREATE TABLE IF NOT EXISTS projects (
    hash TEXT PRIMARY KEY,
    current_path TEXT NOT NULL,
    created_at TEXT NOT NULL,
    next_snapshot_id INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS project_history (
    project_hash TEXT NOT NULL REFERENCES projects(hash) ON DELETE CASCADE,
    old_path TEXT NOT NULL,
    changed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshots (
    rowid INTEGER PRIMARY KEY AUTOINCREMENT,
    project_hash TEXT NOT NULL REFERENCES projects(hash) ON DELETE CASCADE,
    snapshot_id INTEGER NOT NULL CHECK (snapshot_id > 0),
    timestamp TEXT NOT NULL,
    trigger TEXT NOT NULL,
    message TEXT NOT NULL DEFAULT '',
    pinned INTEGER NOT NULL DEFAULT 0,
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

CREATE TABLE IF NOT EXISTS snapshot_tree (
    snapshot_rowid INTEGER NOT NULL REFERENCES snapshots(rowid) ON DELETE CASCADE,
    dir_path TEXT NOT NULL,
    tree_hash TEXT NOT NULL,
    PRIMARY KEY (snapshot_rowid, dir_path)
);

CREATE INDEX IF NOT EXISTS idx_snapshot_project ON snapshots(project_hash, timestamp);
CREATE INDEX IF NOT EXISTS idx_snapshot_files_hash ON snapshot_files(hash);
CREATE INDEX IF NOT EXISTS idx_snapshot_deleted_hash ON snapshot_deleted(hash);
CREATE INDEX IF NOT EXISTS idx_file_path ON snapshot_files(path, snapshot_rowid);

PRAGMA user_version = 1;
