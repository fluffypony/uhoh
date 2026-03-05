pub mod agent;
pub mod ai;
pub mod cas;
pub mod cli;
pub mod compaction;
pub mod config;
pub mod daemon;
pub mod emergency;
pub mod db;
pub mod db_guard;
pub mod diff_view;
pub mod event_ledger;
pub mod gc;
pub mod git;
pub mod ignore_rules;
pub mod marker;
pub mod mcp_stdio;
pub mod notifications;
pub mod operations;
pub mod platform;
pub mod resolve;
pub mod restore;
pub mod server;
pub mod snapshot;
pub mod subsystem;
pub mod update;
pub mod watcher;

// Single source of truth for ~/.uhoh directory
pub fn uhoh_dir() -> std::path::PathBuf {
    crate::platform::uhoh_dir()
}
