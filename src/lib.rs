//! Local-first filesystem snapshot and recovery system for AI-assisted
//! development workflows.

pub(crate) mod agent;
pub(crate) mod ai;
pub mod cas;
pub mod cli;
pub mod commands;
pub mod config;
pub mod encoding;
pub(crate) mod daemon;
pub mod db;
pub(crate) mod db_guard;
pub(crate) mod diff_view;
pub(crate) mod emergency;
pub mod event_ledger;
pub(crate) mod events;
pub(crate) mod gc;
pub(crate) mod ignore_rules;
pub(crate) mod marker;
pub(crate) mod mcp;
pub mod platform;
pub(crate) mod project_service;
pub mod resolve;
pub mod restore;
pub(crate) mod runtime_bundle;
pub(crate) mod server;
pub mod snapshot;
pub mod storage;
pub mod subsystem;
pub(crate) mod update;

pub fn uhoh_dir() -> std::path::PathBuf {
    crate::platform::uhoh_dir()
}
