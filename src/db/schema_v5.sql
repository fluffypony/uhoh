CREATE TABLE IF NOT EXISTS stats (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO stats (key, value) VALUES ('blob_bytes', 0);
PRAGMA user_version = 5;
