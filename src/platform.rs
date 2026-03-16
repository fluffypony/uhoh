use anyhow::{Context, Result};
#[cfg(target_os = "macos")]
use chrono::TimeZone;
use std::path::PathBuf;

#[must_use] 
pub fn uhoh_dir() -> PathBuf {
    if let Some(override_dir) = std::env::var_os("UHOH_DIR") {
        return PathBuf::from(override_dir);
    }

    let home = dirs::home_dir()
        .or_else(|| std::env::var_os("HOME").map(PathBuf::from))
        .or_else(|| std::env::current_dir().ok());
    match home {
        Some(base) => base.join(".uhoh"),
        None => std::env::temp_dir().join("uhoh"),
    }
}

pub fn install_service() -> Result<()> {
    #[cfg(target_os = "macos")]
    install_launchagent()?;

    #[cfg(target_os = "linux")]
    install_systemd_user_unit()?;

    #[cfg(target_os = "windows")]
    install_windows_task()?;

    Ok(())
}

pub fn remove_service() -> Result<()> {
    #[cfg(target_os = "macos")]
    remove_launchagent()?;

    #[cfg(target_os = "linux")]
    remove_systemd_user_unit()?;

    #[cfg(target_os = "windows")]
    remove_windows_task()?;

    Ok(())
}

#[cfg(target_os = "macos")]
fn install_launchagent() -> Result<()> {
    let exe = std::env::current_exe()?;
    let uhoh = uhoh_dir();

    // Use proper XML escaping for paths
    fn xml_escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
    }

    let exe_escaped = xml_escape(&exe.to_string_lossy());
    let log_escaped = xml_escape(&uhoh.join("daemon.log").to_string_lossy());

    let plist = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.uhoh.daemon</string>
    <key>ProgramArguments</key>
    <array>
        <string>{exe_escaped}</string>
        <string>start</string>
        <string>--service</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>StandardOutPath</key>
    <string>{log_escaped}</string>
    <key>StandardErrorPath</key>
    <string>{log_escaped}</string>
</dict>
</plist>"#,
    );

    let plist_path = dirs::home_dir()
        .context("No home dir")?
        .join("Library/LaunchAgents/com.uhoh.daemon.plist");

    std::fs::write(&plist_path, plist)?;

    // Use modern launchctl API
    // SAFETY: getuid() is always safe to call — it has no preconditions and
    // simply returns the real user ID of the calling process.
    let uid = unsafe { libc::getuid() };
    let status = std::process::Command::new("launchctl")
        .args(["bootstrap", &format!("gui/{uid}")])
        .arg(&plist_path)
        .status()
        .context("Failed to execute launchctl bootstrap")?;

    if !status.success() {
        anyhow::bail!(
            "launchctl bootstrap failed with exit code {:?}",
            status.code()
        );
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn remove_launchagent() -> Result<()> {
    let plist_path = dirs::home_dir()
        .context("No home dir")?
        .join("Library/LaunchAgents/com.uhoh.daemon.plist");

    // SAFETY: getuid() is always safe to call — it has no preconditions and
    // simply returns the real user ID of the calling process.
    let uid = unsafe { libc::getuid() };
    let _ = std::process::Command::new("launchctl")
        .args([
            "bootout",
            &format!("gui/{uid}"),
            &plist_path.to_string_lossy(),
        ])
        .status();

    std::fs::remove_file(&plist_path).ok();
    Ok(())
}

#[cfg(target_os = "linux")]
fn install_systemd_user_unit() -> Result<()> {
    let current_exe = std::env::current_exe()?;
    let uhoh = uhoh_dir();
    // Ensure we have a stable binary location at ~/.uhoh/bin/uhoh for service registration
    let bin_dir = uhoh.join("bin");
    std::fs::create_dir_all(&bin_dir)?;
    let target_bin = bin_dir.join("uhoh");
    std::fs::copy(&current_exe, &target_bin)
        .with_context(|| format!("Failed to copy binary to {}", target_bin.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&target_bin, std::fs::Permissions::from_mode(0o755));
    }

    let unit = format!(
        r#"[Unit]
Description=uhoh snapshot daemon

[Service]
Type=simple
ExecStart="{exe}" start --service
Restart=on-failure
RestartSec=5
StandardOutput=append:{log}
StandardError=append:{log}

[Install]
WantedBy=default.target
"#,
        exe = target_bin.display(),
        log = uhoh.join("daemon.log").display(),
    );

    let unit_dir = dirs::config_dir()
        .context("No config dir")?
        .join("systemd/user");
    std::fs::create_dir_all(&unit_dir)?;

    let unit_path = unit_dir.join("uhoh.service");
    std::fs::write(&unit_path, unit)?;

    let daemon_reload = std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()?;
    if !daemon_reload.success() {
        anyhow::bail!(
            "systemctl --user daemon-reload failed with status {:?}",
            daemon_reload.code()
        );
    }
    let enable_now = std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", "uhoh.service"])
        .status()?;
    if !enable_now.success() {
        anyhow::bail!(
            "systemctl --user enable --now uhoh.service failed with status {:?}",
            enable_now.code()
        );
    }

    Ok(())
}

#[cfg(target_os = "linux")]
fn remove_systemd_user_unit() -> Result<()> {
    std::process::Command::new("systemctl")
        .args(["--user", "disable", "--now", "uhoh.service"])
        .status()
        .ok();

    let unit_path = dirs::config_dir()
        .context("No config dir")?
        .join("systemd/user/uhoh.service");
    std::fs::remove_file(&unit_path).ok();

    std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()
        .ok();

    Ok(())
}

/// Cross-platform check whether the given PID is a running uhoh process.
/// macOS: use `ps -p <pid> -o comm=` and check name contains "uhoh".
/// Linux: read `/proc/<pid>/cmdline` and check name contains "uhoh".
/// Windows: OpenProcess + GetModuleFileNameExW, check path contains "uhoh".
#[must_use] 
pub fn is_uhoh_process_alive(pid: u32) -> bool {
    #[cfg(target_os = "macos")]
    {
        match std::process::Command::new("ps")
            .args(["-p", &pid.to_string(), "-o", "args="])
            .output()
        {
            Ok(out) if out.status.success() => {
                let name = String::from_utf8_lossy(&out.stdout).to_lowercase();
                return name.contains("uhoh");
            }
            _ => return false,
        }
    }

    #[cfg(target_os = "linux")]
    {
        let path = format!("/proc/{}/cmdline", pid);
        match std::fs::read(&path) {
            Ok(data) => {
                let s = String::from_utf8_lossy(&data).to_lowercase();
                return s.contains("uhoh");
            }
            Err(_) => return false,
        }
    }

    #[cfg(target_os = "windows")]
    {
        use winapi::um::handleapi::CloseHandle;
        use winapi::um::processthreadsapi::OpenProcess;
        use winapi::um::psapi::GetModuleFileNameExW;
        use winapi::um::winnt::{PROCESS_QUERY_INFORMATION, PROCESS_VM_READ};
        // SAFETY: OpenProcess with QUERY_INFORMATION|VM_READ opens a read-only handle
        // to the target process; null is checked. GetModuleFileNameExW writes into a
        // stack buffer of known size. CloseHandle releases the handle before returning.
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, 0, pid);
            if handle.is_null() {
                return false;
            }
            let mut buf = [0u16; 32767];
            let len = GetModuleFileNameExW(handle, std::ptr::null_mut(), buf.as_mut_ptr(), 32767);
            CloseHandle(handle);
            if len > 0 {
                let name = String::from_utf16_lossy(&buf[..len as usize]).to_lowercase();
                return name.contains("uhoh");
            }
            return false;
        }
    }

    #[allow(unreachable_code)]
    false
}

/// Stronger daemon-process check that validates both process identity and start-time
/// to defend against PID reuse after crashes/restarts.
#[must_use] 
pub fn is_uhoh_process_alive_with_start(pid: u32, expected_start: Option<u64>) -> bool {
    if !is_uhoh_process_alive(pid) {
        return false;
    }
    if let Some(expected) = expected_start {
        if expected == 0 {
            return true;
        }
        return read_process_start_ticks(pid).is_some_and(|actual| actual == expected);
    }
    true
}

/// Capture OS process start time in kernel ticks for PID-reuse-safe identity checks.
#[must_use] 
pub fn read_process_start_ticks(pid: u32) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let stat_path = format!("/proc/{pid}/stat");
        let stat = std::fs::read_to_string(stat_path).ok()?;
        let close = stat.rfind(')')?;
        let rest = stat.get(close + 2..)?;
        let fields: Vec<&str> = rest.split_whitespace().collect();
        return fields.get(19).and_then(|v| v.parse::<u64>().ok());
    }

    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("ps")
            .env("LC_ALL", "C")
            .args(["-p", &pid.to_string(), "-o", "lstart="])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let raw = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if raw.is_empty() {
            return None;
        }
        // Normalize whitespace: %e can produce double spaces for single-digit days
        let normalized: String = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        // Use %e instead of %d to handle unpadded single-digit days after
        // whitespace normalization collapses " 6" into "6".
        let parsed =
            chrono::NaiveDateTime::parse_from_str(&normalized, "%a %b %e %H:%M:%S %Y").ok()?;
        let dt = chrono::Local.from_local_datetime(&parsed).single()?;
        return u64::try_from(dt.timestamp()).ok();
    }

    #[cfg(target_os = "windows")]
    {
        use winapi::shared::minwindef::FILETIME;
        use winapi::um::handleapi::CloseHandle;
        use winapi::um::processthreadsapi::{GetProcessTimes, OpenProcess};
        use winapi::um::winnt::PROCESS_QUERY_LIMITED_INFORMATION;

        // Initialize FILETIME structs with safe zero-valued struct literals instead
        // of mem::zeroed(). FILETIME contains only two u32 fields, so this is
        // equivalent but requires no unsafe block for initialization.
        let mut creation = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
        let mut exit_time = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
        let mut kernel = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };
        let mut user = FILETIME { dwLowDateTime: 0, dwHighDateTime: 0 };

        // SAFETY: OpenProcess with QUERY_LIMITED_INFORMATION opens a minimal handle;
        // null is checked. GetProcessTimes writes into properly aligned stack
        // variables. CloseHandle releases the handle before returning.
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
            if handle.is_null() {
                return None;
            }
            let ok = GetProcessTimes(
                handle,
                &mut creation,
                &mut exit_time,
                &mut kernel,
                &mut user,
            );
            CloseHandle(handle);
            if ok == 0 {
                return None;
            }
            let high = (creation.dwHighDateTime as u64) << 32;
            let low = creation.dwLowDateTime as u64;
            return Some((high | low) / 10_000);
        }
    }

    #[allow(unreachable_code)]
    None
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_start_ticks_support_pid_reuse_safe_checks() {
        let pid = std::process::id();
        let ticks = read_process_start_ticks(pid).unwrap_or(0);
        assert!(is_uhoh_process_alive(pid));
        assert!(is_uhoh_process_alive_with_start(pid, Some(ticks)));
        assert!(!is_uhoh_process_alive_with_start(
            pid,
            Some(ticks.saturating_add(1))
        ));
    }

    #[test]
    fn uhoh_dir_default_uses_home_dot_uhoh() {
        // Save and clear UHOH_DIR to test default behavior
        let saved = std::env::var_os("UHOH_DIR");
        std::env::remove_var("UHOH_DIR");

        let dir = uhoh_dir();
        // Should end with .uhoh
        assert!(
            dir.ends_with(".uhoh"),
            "Expected dir to end with .uhoh, got: {}",
            dir.display()
        );

        // Restore
        if let Some(v) = saved {
            std::env::set_var("UHOH_DIR", v);
        }
    }

    #[test]
    fn uhoh_dir_respects_override_env() {
        let saved = std::env::var_os("UHOH_DIR");
        std::env::set_var("UHOH_DIR", "/custom/uhoh/dir");

        let dir = uhoh_dir();
        assert_eq!(dir, std::path::PathBuf::from("/custom/uhoh/dir"));

        // Restore
        match saved {
            Some(v) => std::env::set_var("UHOH_DIR", v),
            None => std::env::remove_var("UHOH_DIR"),
        }
    }

    #[test]
    fn dead_pid_is_not_alive() {
        // PID 0 is never a uhoh process
        assert!(!is_uhoh_process_alive(0));
    }

    #[test]
    fn dead_pid_with_start_is_not_alive() {
        assert!(!is_uhoh_process_alive_with_start(0, Some(12345)));
        assert!(!is_uhoh_process_alive_with_start(0, None));
    }

    #[test]
    fn alive_with_zero_expected_start_skips_time_check() {
        let pid = std::process::id();
        // When expected_start is Some(0), should skip the time check and return true
        // if the process is alive
        assert!(is_uhoh_process_alive_with_start(pid, Some(0)));
    }

    #[test]
    fn alive_with_none_expected_start_skips_time_check() {
        let pid = std::process::id();
        // When expected_start is None, should skip the time check and return true
        // if the process is alive
        assert!(is_uhoh_process_alive_with_start(pid, None));
    }

    #[test]
    fn read_process_start_ticks_current_process() {
        let pid = std::process::id();
        let ticks = read_process_start_ticks(pid);
        // On macOS and Linux we should get Some value for the current process
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        assert!(ticks.is_some(), "Expected start ticks for current process");
        // On other platforms it may return None, which is fine
        let _ = ticks;
    }

    #[test]
    fn read_process_start_ticks_dead_pid() {
        // PID 0 should not have start ticks
        let ticks = read_process_start_ticks(0);
        assert!(ticks.is_none());
    }

    #[test]
    fn uhoh_dir_returns_path_buf() {
        let dir = uhoh_dir();
        // Just verify it returns a non-empty path
        assert!(!dir.as_os_str().is_empty());
    }
}

#[cfg(target_os = "windows")]
fn install_windows_task() -> Result<()> {
    let exe = std::env::current_exe()?;
    let status = std::process::Command::new("schtasks")
        .args([
            "/Create",
            "/TN",
            "uhoh-daemon",
            "/TR",
            &format!("\"{}\" start --service", exe.display()),
            "/SC",
            "ONLOGON",
            "/RL",
            "LIMITED",
            "/F",
        ])
        .status()?;
    if !status.success() {
        anyhow::bail!("schtasks /Create failed with status {:?}", status.code());
    }
    Ok(())
}

#[cfg(target_os = "windows")]
fn remove_windows_task() -> Result<()> {
    std::process::Command::new("schtasks")
        .args(["/Delete", "/TN", "uhoh-daemon", "/F"])
        .status()
        .ok();
    Ok(())
}
