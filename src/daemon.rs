mod emergency;
mod maintenance;
pub(crate) mod notifications;
mod runtime;
mod snapshots;
mod watcher;

use anyhow::{Context, Result};
use std::path::Path;

use crate::db::Database;

/// Spawn daemon as a detached background process.
pub fn spawn_detached_daemon(uhoh_dir: &Path) -> Result<()> {
    let exe = std::env::current_exe()?;

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let log_path = uhoh_dir.join("daemon.log");
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open daemon log: {}", log_path.display()))?;
        let err_file = log_file.try_clone().with_context(|| {
            format!("Failed to clone daemon log handle: {}", log_path.display())
        })?;
        let mut cmd = std::process::Command::new(&exe);
        cmd.args(["start", "--service"]);
        // Detach from controlling terminal
        // SAFETY: CommandExt::pre_exec requires unsafe because the closure runs between
        // fork and exec. This closure calls setsid() (async-signal-safe) and eprintln!
        // (not async-signal-safe but acceptable in single-threaded CLI context). setsid()
        // failure is non-fatal — the child continues regardless.
        unsafe {
            cmd.pre_exec(|| {
                if libc::setsid() == -1 {
                    eprintln!("Warning: setsid() failed; daemon may not fully detach");
                }
                Ok(())
            });
        }
        cmd.stdout(std::process::Stdio::from(log_file))
            .stderr(std::process::Stdio::from(err_file))
            .stdin(std::process::Stdio::null());
        cmd.spawn().context("Failed to spawn daemon")?;
    }

    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const DETACHED_PROCESS: u32 = 0x00000008;
        let log_path = uhoh_dir.join("daemon.log");
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .with_context(|| format!("Failed to open daemon log: {}", log_path.display()))?;
        let err_file = log_file.try_clone().with_context(|| {
            format!("Failed to clone daemon log handle: {}", log_path.display())
        })?;
        std::process::Command::new(&exe)
            .args(["start", "--service"])
            .creation_flags(DETACHED_PROCESS)
            .stdout(std::process::Stdio::from(log_file))
            .stderr(std::process::Stdio::from(err_file))
            .stdin(std::process::Stdio::null())
            .spawn()
            .context("Failed to spawn daemon")?;
    }

    println!("Daemon started.");
    Ok(())
}

/// Stop the daemon by reading PID file and sending signal.
pub fn stop_daemon(uhoh_dir: &Path) -> Result<()> {
    let pid_path = uhoh_dir.join("daemon.pid");
    let pid_str = std::fs::read_to_string(&pid_path).context("Daemon not running (no PID file)")?;
    let mut parts = pid_str.split_whitespace();
    let pid: u32 = parts
        .next()
        .context("Invalid PID file")?
        .parse()
        .context("Invalid PID file")?;
    let expected_start = parts.next().and_then(|v| v.parse::<u64>().ok());

    if !crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
        std::fs::remove_file(&pid_path).ok();
        println!("Daemon was not running (stale PID file cleaned up).");
        return Ok(());
    }

    #[cfg(unix)]
    // SAFETY: libc::kill with SIGTERM on a pid we verified is a running uhoh process
    // via is_uhoh_process_alive_with_start above; sending SIGTERM is a graceful
    // shutdown request and is safe even if the process exits between the check and
    // the signal (the signal is simply discarded by the kernel).
    unsafe {
        libc::kill(pid as i32, libc::SIGTERM);
    }

    #[cfg(unix)]
    {
        // Wait briefly for clean shutdown before removing the PID file.
        for _ in 0..50 {
            if !crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
            eprintln!("Warning: daemon process {} did not exit after SIGTERM. PID file removed but process may still be running.", pid);
        }
    }

    #[cfg(windows)]
    {
        // Try graceful termination first
        let _ = std::process::Command::new("taskkill")
            .args(["/PID", &pid.to_string(), "/T"])
            .status();
        // Wait briefly for process to end
        for _ in 0..50 {
            if !crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        if crate::platform::is_uhoh_process_alive_with_start(pid, expected_start) {
            // Force kill as fallback
            let _ = std::process::Command::new("taskkill")
                .args(["/F", "/PID", &pid.to_string(), "/T"])
                .status();
        }
    }

    std::fs::remove_file(&pid_path).ok();
    println!("Daemon stopped.");
    Ok(())
}

/// Run the daemon in the foreground (called with --service flag).
pub async fn run_foreground(uhoh_dir: &Path, database: std::sync::Arc<Database>) -> Result<()> {
    runtime::run_foreground(uhoh_dir, database).await
}
