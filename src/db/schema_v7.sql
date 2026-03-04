BEGIN;

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
    resolved INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_event_ledger_ts ON event_ledger(ts);
CREATE INDEX IF NOT EXISTS idx_event_ledger_source ON event_ledger(source);
CREATE INDEX IF NOT EXISTS idx_event_ledger_agent ON event_ledger(agent_name);
CREATE INDEX IF NOT EXISTS idx_event_ledger_guard ON event_ledger(guard_name);
CREATE INDEX IF NOT EXISTS idx_event_ledger_path ON event_ledger(path);
CREATE INDEX IF NOT EXISTS idx_event_ledger_causal ON event_ledger(causal_parent);
CREATE INDEX IF NOT EXISTS idx_event_ledger_severity ON event_ledger(severity);
CREATE INDEX IF NOT EXISTS idx_event_ledger_prev_hash ON event_ledger(prev_hash);

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

PRAGMA user_version = 7;

COMMIT;
