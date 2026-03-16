pub(crate) mod git;
pub mod ledger;
pub(crate) mod operations;
pub mod project;
pub mod runtime;
pub mod shared;

use std::path::Path;

use anyhow::Result;

use crate::cli::{Commands, LedgerAction};
use crate::db;

/// Dispatches a CLI command to the appropriate handler.
///
/// # Errors
///
/// Returns an error if the dispatched command handler fails.
pub async fn dispatch(uhoh: &Path, database: db::Database, command: Commands) -> Result<()> {
    match command {
        Commands::Add { path } => project::add(uhoh, &database, path)?,
        Commands::Remove { target } => project::remove(&database, target)?,
        Commands::List => project::list(&database)?,
        Commands::Snapshots { target } => project::snapshots(&database, target)?,
        Commands::Commit { message, trigger } => {
            project::commit(uhoh, &database, message, trigger)?;
        }
        Commands::Restore {
            id,
            target,
            dry_run,
            force,
        } => project::restore_snapshot(uhoh, &database, &id, target, dry_run, force)?,
        Commands::Gitstash { id, target } => project::gitstash(uhoh, &database, &id, target)?,
        Commands::Diff { id1, id2 } => project::diff(uhoh, &database, id1, id2)?,
        Commands::Cat { path, id } => project::cat(uhoh, &database, &path, &id)?,
        Commands::Log { path } => project::log(&database, &path)?,
        Commands::Mcp => crate::mcp::run_stdio_server(uhoh)?,
        Commands::Start { service } => runtime::start(uhoh, &database, service).await?,
        Commands::Stop => runtime::stop(uhoh)?,
        Commands::Restart => runtime::restart(uhoh)?,
        Commands::Hook { action } => runtime::hook(action)?,
        Commands::Config { action } => runtime::config_action(uhoh, action)?,
        Commands::Gc => runtime::run_gc(uhoh, &database)?,
        Commands::Update => runtime::update(uhoh).await?,
        Commands::Doctor {
            fix,
            restore_latest,
            verify_install,
        } => runtime::doctor(uhoh, database, fix, restore_latest, verify_install).await?,
        Commands::Status => runtime::status(uhoh, &database).await?,
        Commands::Mark { label } => project::mark(&database, &label)?,
        Commands::Undo { target } => project::undo(uhoh, &database, target)?,
        Commands::Operations { target } => project::operations(&database, target)?,
        Commands::ServiceInstall => runtime::install_service()?,
        Commands::ServiceRemove => runtime::remove_service()?,
        Commands::Db { action } => crate::db_guard::handle_db_guard_action(uhoh, &database, &action)?,
        Commands::Agent { action } => crate::agent::handle_agent_action(uhoh, &database, &action)?,
        Commands::Trace { event_id } => ledger::trace(&database, event_id)?,
        Commands::Blame { path } => ledger::blame(&database, &path)?,
        Commands::Timeline { source, since } => ledger::timeline(&database, source, since)?,
        Commands::Ledger { action } => match action {
            LedgerAction::Verify => ledger::verify(&database)?,
        },
        Commands::Run { command } => runtime::run_wrapped_command(uhoh, command).await?,
    }

    Ok(())
}
