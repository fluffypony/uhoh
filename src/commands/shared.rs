use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::daemon;
use crate::db;
use crate::platform;

#[must_use] 
pub fn is_daemon_running(uhoh: &Path) -> bool {
    let pid_path = uhoh.join("daemon.pid");
    match std::fs::read_to_string(&pid_path) {
        Ok(pid_str) => {
            let mut parts = pid_str.split_whitespace();
            let Some(pid_raw) = parts.next() else {
                return false;
            };
            let Ok(pid) = pid_raw.parse::<u32>() else {
                return false;
            };
            let expected_start = parts.next().and_then(|v| v.parse::<u64>().ok());
            platform::is_uhoh_process_alive_with_start(pid, expected_start)
        }
        Err(_) => false,
    }
}

pub fn maybe_start_daemon(uhoh: &Path) -> Result<()> {
    if !is_daemon_running(uhoh) {
        tracing::info!("Daemon not running, starting automatically...");
        daemon::spawn_detached_daemon(uhoh)?;
    }
    Ok(())
}

pub fn resolve_project_path(path: Option<String>) -> Result<PathBuf> {
    match path {
        Some(path) => {
            dunce::canonicalize(&path).with_context(|| format!("Cannot resolve path: {path}"))
        }
        None => dunce::canonicalize(std::env::current_dir()?)
            .context("Cannot resolve current directory"),
    }
}

pub fn resolve_target_project(
    database: &db::Database,
    target: Option<&str>,
) -> Result<db::ProjectEntry> {
    match target {
        Some(target) => {
            let as_path = PathBuf::from(target);
            if as_path.exists() {
                let canonical = dunce::canonicalize(&as_path)?;
                return database
                    .find_project_by_path(&canonical)?
                    .context("Not a registered uhoh project");
            }
            database
                .find_project_by_hash_prefix(target)?
                .context("No project matching that identifier")
        }
        None => {
            let cwd = dunce::canonicalize(std::env::current_dir()?)?;
            database
                .find_project_by_path(&cwd)?
                .context("Not a registered uhoh project. Run `uhoh add` first.")
        }
    }
}

pub fn confirm_restore_delete(count: usize) -> Result<bool> {
    use std::io::{self, IsTerminal, Write};

    if !io::stdin().is_terminal() {
        anyhow::bail!(
            "Refusing to delete {count} files without confirmation. Use --force or run in an interactive terminal."
        );
    }

    eprintln!(
        "⚠ This will delete {count} tracked files. Use --force to skip this prompt."
    );
    eprint!("Continue? [y/N] ");
    io::stdout().flush()?;

    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(input.trim().eq_ignore_ascii_case("y"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_daemon_running_no_pid_file() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(!is_daemon_running(tmp.path()));
    }

    #[test]
    fn is_daemon_running_invalid_pid_file() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("daemon.pid"), "not_a_number").unwrap();
        assert!(!is_daemon_running(tmp.path()));
    }

    #[test]
    fn is_daemon_running_empty_pid_file() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("daemon.pid"), "").unwrap();
        assert!(!is_daemon_running(tmp.path()));
    }

    #[test]
    fn is_daemon_running_stale_pid() {
        let tmp = tempfile::tempdir().unwrap();
        // Use PID 1 which exists but isn't uhoh, and a start time that won't match
        std::fs::write(tmp.path().join("daemon.pid"), "999999999 99999999999").unwrap();
        assert!(!is_daemon_running(tmp.path()));
    }

    #[test]
    fn resolve_project_path_explicit() {
        let tmp = tempfile::tempdir().unwrap();
        let result = resolve_project_path(Some(tmp.path().to_str().unwrap().to_string()));
        assert!(result.is_ok());
        // Should canonicalize
        let resolved = result.unwrap();
        assert!(resolved.is_absolute());
    }

    #[test]
    fn resolve_project_path_none_uses_cwd() {
        let result = resolve_project_path(None);
        assert!(result.is_ok());
        assert!(result.unwrap().is_absolute());
    }

    #[test]
    fn resolve_project_path_nonexistent() {
        let result = resolve_project_path(Some("/nonexistent/path/that/does/not/exist".to_string()));
        assert!(result.is_err());
    }
}
