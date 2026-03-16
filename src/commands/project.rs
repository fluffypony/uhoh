use std::path::Path;

use anyhow::{Context, Result};
use tracing::warn;

use crate::config;
use crate::encoding;
use crate::db;
use crate::diff_view;
use super::git;
use crate::marker;
use crate::commands::operations;
use crate::restore;
use crate::snapshot;

use super::shared::{
    confirm_restore_delete, maybe_start_daemon, resolve_project_path, resolve_target_project,
};

/// Registers a project directory with uhoh, starting the daemon if needed.
///
/// # Errors
///
/// Returns an error if the daemon cannot be started, the path cannot be resolved,
/// or project registration fails.
pub fn add(uhoh: &Path, database: &db::Database, path: Option<String>) -> Result<()> {
    maybe_start_daemon(uhoh)?;
    let project_path = resolve_project_path(path)?;
    register_project(uhoh, database, project_path)
}

/// Prints project status if the current directory is registered, otherwise registers it.
///
/// # Errors
///
/// Returns an error if the current directory cannot be resolved, a database query fails,
/// the daemon cannot be started, or project registration fails.
pub fn default_action(uhoh: &Path, database: &db::Database) -> Result<()> {
    let cwd = dunce::canonicalize(std::env::current_dir()?)?;

    if let Some(project) = database.find_project_by_path(&cwd)? {
        print_project_status(database, &project.hash)?;
        return Ok(());
    }

    maybe_start_daemon(uhoh)?;
    register_project(uhoh, database, cwd)
}

fn register_project(
    uhoh: &Path,
    database: &db::Database,
    project_path: std::path::PathBuf,
) -> Result<()> {
    if uhoh.starts_with(&project_path) {
        anyhow::bail!(
            "Refusing to watch parent directory '{}' because it contains {}",
            project_path.display(),
            uhoh.display()
        );
    }

    if let Some(existing_hash) = marker::read_marker(&project_path)? {
        if let Some(existing) = database.get_project(&existing_hash)? {
            let canonical = dunce::canonicalize(&project_path)?;
            if existing.current_path != canonical.to_string_lossy().as_ref() {
                database.update_project_path(&existing_hash, &canonical.to_string_lossy())?;
                println!("Updated project path: {}", canonical.display());
            } else {
                println!("Already registered: {}", canonical.display());
            }
            return Ok(());
        }
    }

    let git_dir = project_path.join(".git");
    if !git_dir.exists() {
        warn!(
            "Not a git repo. Marker at {0}/.uhoh — add to your ignore file.",
            project_path.display()
        );
        eprintln!("⚠ Warning: Not a git repo. Add `.uhoh` to your ignore file.");
    }

    let project_hash = marker::create_marker(&project_path)?;
    let canonical = dunce::canonicalize(&project_path)?;
    database.add_project(&project_hash, &canonical.to_string_lossy())?;
    println!("Registered: {}", canonical.display());

    create_initial_snapshot(uhoh, database, &project_hash, &canonical)?;
    println!("Initial snapshot created.");
    Ok(())
}

fn create_initial_snapshot(
    uhoh: &Path,
    database: &db::Database,
    project_hash: &str,
    project_path: &Path,
) -> Result<()> {
    let cfg = config::Config::load(&uhoh.join("config.toml"))?;
    let snapshot_runtime = snapshot::SnapshotRuntime::from_config(&cfg);
    snapshot::create_snapshot(
        uhoh,
        database,
        &snapshot_runtime,
        snapshot::CreateSnapshotRequest {
            project_hash,
            project_path,
            trigger: crate::db::SnapshotTrigger::Manual,
            message: Some("Initial snapshot"),
            changed_paths: None,
        },
    )?;
    Ok(())
}

fn print_project_status(database: &db::Database, project_hash: &str) -> Result<()> {
    let snaps = database.list_snapshots(project_hash)?;
    println!("uhoh is active in this directory.");
    if let Some(latest) = snaps.first() {
        println!(
            "Latest snapshot: {} ({})",
            encoding::id_to_base58(latest.snapshot_id),
            latest.timestamp
        );
        println!("Total snapshots: {}", snaps.len());
    } else {
        println!("No snapshots yet.");
    }
    println!("\nTo undo the last AI operation: uhoh undo");
    println!("To restore a snapshot:         uhoh restore <id>");
    println!("To see recent changes:         uhoh log");
    Ok(())
}

/// Unregisters a project and removes its marker file.
///
/// # Errors
///
/// Returns an error if the target path cannot be resolved, the hash prefix is ambiguous,
/// no matching project is found, or the database removal fails.
pub fn remove(database: &db::Database, target: Option<String>) -> Result<()> {
    let project = if let Some(ref target) = target {
        let path = Path::new(target);
        if path.exists() || path.is_absolute() {
            let canonical = dunce::canonicalize(target)?;
            database.find_project_by_path(&canonical)?
        } else {
            let projects = database.list_projects()?;
            let matches: Vec<_> = projects
                .iter()
                .filter(|project| project.hash.starts_with(target.as_str()))
                .collect();
            match matches.len() {
                0 => {
                    let canonical = dunce::canonicalize(target)?;
                    database.find_project_by_path(&canonical)?
                }
                1 => Some(matches[0].clone()),
                _ => anyhow::bail!(
                    "Ambiguous hash prefix '{}': matches {} projects",
                    target,
                    matches.len()
                ),
            }
        }
    } else {
        let cwd = dunce::canonicalize(std::env::current_dir()?)?;
        database.find_project_by_path(&cwd)?
    }
    .context("Project not found")?;

    let project_path = Path::new(&project.current_path);
    let marker_git = project_path.join(".git/.uhoh");
    let marker_root = project_path.join(".uhoh");
    if marker_git.exists() {
        std::fs::remove_file(&marker_git).ok();
    }
    if marker_root.exists() {
        std::fs::remove_file(&marker_root).ok();
    }
    database.remove_project(&project.hash)?;
    println!("Removed: {}", project.current_path);
    Ok(())
}

/// Lists all registered projects and their snapshot counts.
///
/// # Errors
///
/// Returns an error if listing projects or querying snapshot counts from the database fails.
pub fn list(database: &db::Database) -> Result<()> {
    let projects = database.list_projects()?;
    if projects.is_empty() {
        println!("No registered projects. Use `uhoh add` to register one.");
        return Ok(());
    }

    for project in &projects {
        let exists = Path::new(&project.current_path).exists();
        let status = if exists { "✓" } else { "✗ MISSING" };
        let count = database.snapshot_count(&project.hash)?;
        println!(
            "  {} {} ({} snapshots) [{}]",
            status,
            project.current_path,
            count,
            &project.hash[..project.hash.len().min(12)]
        );
    }
    Ok(())
}

/// Lists all snapshots for the target project, including their files.
///
/// # Errors
///
/// Returns an error if the target project cannot be resolved, or if any database query fails.
pub fn snapshots(database: &db::Database, target: Option<String>) -> Result<()> {
    let project = resolve_target_project(database, target.as_deref())?;
    let snapshots = database.list_snapshots(&project.hash)?;
    if snapshots.is_empty() {
        println!("No snapshots.");
        return Ok(());
    }

    for snapshot in &snapshots {
        let id_str = encoding::id_to_base58(snapshot.snapshot_id);
        let pin = if snapshot.pinned { " 📌" } else { "" };
        let msg = if snapshot.message.is_empty() {
            String::new()
        } else {
            format!(" — {}", snapshot.message)
        };
        println!(
            "  {} [{}] {}{}{}",
            snapshot.timestamp, id_str, snapshot.trigger, pin, msg
        );
        let files = database.get_snapshot_files(snapshot.rowid)?;
        for file in files.iter().take(10) {
            let method = file.storage_method.display_name();
            println!("       {:>8}  {:>7}  {}", file.size, method, file.path);
        }
        if files.len() > 10 {
            println!("       ... and {} more", files.len() - 10);
        }
    }

    Ok(())
}

/// Creates a manual snapshot of the current project.
///
/// # Errors
///
/// Returns an error if the daemon cannot be started, the current directory is not a
/// registered project, the config cannot be loaded, or the snapshot fails.
pub fn commit(
    uhoh: &Path,
    database: &db::Database,
    message: Option<String>,
    trigger: Option<String>,
) -> Result<()> {
    maybe_start_daemon(uhoh)?;
    let project_path = dunce::canonicalize(std::env::current_dir()?)?;
    let project = database
        .find_project_by_path(&project_path)?
        .context("Not registered")?;
    let trigger = trigger
        .as_deref()
        .and_then(db::SnapshotTrigger::parse)
        .unwrap_or(db::SnapshotTrigger::Manual);
    let cfg = config::Config::load(&uhoh.join("config.toml"))?;
    let snapshot_runtime = snapshot::SnapshotRuntime::from_config(&cfg);
    snapshot::create_snapshot(
        uhoh,
        database,
        &snapshot_runtime,
        snapshot::CreateSnapshotRequest {
            project_hash: &project.hash,
            project_path: &project_path,
            trigger,
            message: message.as_deref(),
            changed_paths: None,
        },
    )?;
    println!("Snapshot created.");
    Ok(())
}

/// Restores the project to the state of the given snapshot ID.
///
/// # Errors
///
/// Returns an error if the target project cannot be resolved, the config cannot be loaded,
/// the user declines the confirmation prompt, or the restore operation fails.
pub fn restore_snapshot(
    uhoh: &Path,
    database: &db::Database,
    id: &str,
    target: Option<String>,
    dry_run: bool,
    force: bool,
) -> Result<()> {
    let project = resolve_target_project(database, target.as_deref())?;
    let cfg = config::Config::load(&uhoh.join("config.toml"))?;
    let snapshot_runtime = snapshot::SnapshotRuntime::from_config(&cfg);
    let pre_restore_message = format!("Before restore to {id}");
    let confirm_large_delete = |count| confirm_restore_delete(count);
    let outcome = restore::restore_project(
        uhoh,
        database,
        &project,
        restore::RestoreRequest {
            snapshot_id: id,
            target_path: None,
            dry_run,
            force,
            pre_restore_snapshot: Some(restore::PreRestoreSnapshot {
                trigger: db::SnapshotTrigger::PreRestore,
                message: Some(pre_restore_message),
                snapshot_runtime: &snapshot_runtime,
            }),
            confirm_large_delete: Some(&confirm_large_delete),
        },
    )?;
    if outcome.dry_run {
        println!("Dry run — changes that would be applied:");
        for path in &outcome.files_to_delete {
            println!("  DELETE {}", Path::new(path).display());
        }
        for path in &outcome.files_to_restore {
            println!("  RESTORE {}", Path::new(path).display());
        }
    } else if outcome.applied {
        println!(
            "Restored to snapshot {} ({} files restored, {} deleted)",
            outcome.snapshot_id, outcome.files_restored, outcome.files_deleted
        );
    }
    Ok(())
}

/// Restores a snapshot into a git stash entry instead of touching the working tree.
///
/// # Errors
///
/// Returns an error if the target project cannot be resolved or the git stash operation fails.
pub fn gitstash(
    uhoh: &Path,
    database: &db::Database,
    id: &str,
    target: Option<String>,
) -> Result<()> {
    let project = resolve_target_project(database, target.as_deref())?;
    git::cmd_gitstash(uhoh, database, &project, id)
}

/// Prints a diff between two snapshots, or between a snapshot and the working tree.
///
/// # Errors
///
/// Returns an error if the current directory is not a registered project or the diff fails.
pub fn diff(
    uhoh: &Path,
    database: &db::Database,
    id1: Option<String>,
    id2: Option<String>,
) -> Result<()> {
    let project_path = dunce::canonicalize(std::env::current_dir()?)?;
    let project = database
        .find_project_by_path(&project_path)?
        .context("Not registered")?;
    diff_view::cmd_diff(uhoh, database, &project, id1.as_deref(), id2.as_deref())
}

/// Prints the contents of a file as it existed in the given snapshot.
///
/// # Errors
///
/// Returns an error if the current directory is not a registered project or reading the
/// file from the snapshot store fails.
pub fn cat(uhoh: &Path, database: &db::Database, path: &str, id: &str) -> Result<()> {
    let project_path = dunce::canonicalize(std::env::current_dir()?)?;
    let project = database
        .find_project_by_path(&project_path)?
        .context("Not registered")?;
    diff_view::cmd_cat(uhoh, database, &project, path, id)
}

/// Prints the snapshot history for a specific file path in the current project.
///
/// # Errors
///
/// Returns an error if the current directory is not a registered project or the log query fails.
pub fn log(database: &db::Database, path: &str) -> Result<()> {
    let project_path = dunce::canonicalize(std::env::current_dir()?)?;
    let project = database
        .find_project_by_path(&project_path)?
        .context("Not registered")?;
    diff_view::cmd_log(database, &project, path)
}

/// Attaches a named label to the most recent snapshot of the current project.
///
/// # Errors
///
/// Returns an error if the current directory is not a registered project or the mark
/// operation fails.
pub fn mark(database: &db::Database, label: &str) -> Result<()> {
    let project_path = dunce::canonicalize(std::env::current_dir()?)?;
    let project = database
        .find_project_by_path(&project_path)?
        .context("Not registered")?;
    operations::cmd_mark(database, &project, label)
}

/// Reverts the project to the snapshot before the last AI-initiated operation.
///
/// # Errors
///
/// Returns an error if the target project cannot be resolved or the undo operation fails.
pub fn undo(uhoh: &Path, database: &db::Database, target: Option<String>) -> Result<()> {
    let project = resolve_target_project(database, target.as_deref())?;
    operations::cmd_undo(uhoh, database, &project)
}

/// Lists recorded AI operations and their associated snapshots for the target project.
///
/// # Errors
///
/// Returns an error if the target project cannot be resolved or the database query fails.
pub fn operations(database: &db::Database, target: Option<String>) -> Result<()> {
    let project = resolve_target_project(database, target.as_deref())?;
    crate::commands::operations::cmd_list_operations(database, &project)
}
