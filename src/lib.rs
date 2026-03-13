pub mod agent;
pub mod ai;
pub mod cas;
pub mod cli;
pub mod commands;
pub mod config;
pub(crate) mod daemon;
pub mod db;
pub mod db_guard;
pub mod diff_view;
pub mod emergency;
pub mod event_ledger;
pub mod events;
pub mod gc;
pub mod git;
pub mod ignore_rules;
pub mod marker;
pub(crate) mod mcp;
pub mod notifications;
pub mod operations;
pub mod platform;
pub(crate) mod project_service;
pub mod resolve;
pub mod restore;
pub(crate) mod runtime_bundle;
pub(crate) mod server;
pub mod snapshot;
pub mod storage;
pub mod subsystem;
pub(crate) mod transport_security;
pub mod update;
pub mod util;

// Single source of truth for ~/.uhoh directory
pub fn uhoh_dir() -> std::path::PathBuf {
    crate::platform::uhoh_dir()
}
