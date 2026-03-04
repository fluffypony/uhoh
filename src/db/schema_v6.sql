CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
    snapshot_rowid UNINDEXED,
    project_hash,
    trigger_type,
    message,
    ai_summary,
    file_paths
);
PRAGMA user_version = 6;
