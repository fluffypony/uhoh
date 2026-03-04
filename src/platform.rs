use anyhow::{Context, Result};
use std::path::PathBuf;

pub fn uhoh_dir() -> PathBuf {
    // Keep a single definition consistent with main; both resolve to ~/.uhoh
    dirs::home_dir()
        .expect("Cannot determine home directory")
        .join(".uhoh")
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
        <string>{exe}</string>
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
    <string>{log}</string>
    <key>StandardErrorPath</key>
    <string>{log}</string>
</dict>
</plist>"#,
        exe = exe_escaped,
        log = log_escaped,
    );

    let plist_path = dirs::home_dir()
        .context("No home dir")?
        .join("Library/LaunchAgents/com.uhoh.daemon.plist");

    std::fs::write(&plist_path, plist)?;

    // Use modern launchctl API
    let uid = unsafe { libc::getuid() };
    std::process::Command::new("launchctl")
        .args(["bootstrap", &format!("gui/{}", uid)])
        .arg(&plist_path)
        .status()
        .or_else(|_| {
            // Fallback to legacy API
            std::process::Command::new("launchctl")
                .args(["load", "-w"])
                .arg(&plist_path)
                .status()
        })?;

    Ok(())
}

#[cfg(target_os = "macos")]
fn remove_launchagent() -> Result<()> {
    let plist_path = dirs::home_dir()
        .context("No home dir")?
        .join("Library/LaunchAgents/com.uhoh.daemon.plist");

    let uid = unsafe { libc::getuid() };
    std::process::Command::new("launchctl")
        .args(["bootout", &format!("gui/{}", uid), &plist_path.to_string_lossy()])
        .status()
        .or_else(|_| {
            std::process::Command::new("launchctl")
                .args(["unload", "-w"])
                .arg(&plist_path)
                .status()
        })
        .ok();

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
    std::fs::copy(&current_exe, &target_bin).ok();
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

    std::process::Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .status()?;
    std::process::Command::new("systemctl")
        .args(["--user", "enable", "--now", "uhoh.service"])
        .status()?;

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
        use winapi::um::processthreadsapi::OpenProcess;
        use winapi::um::winnt::{PROCESS_QUERY_INFORMATION, PROCESS_VM_READ};
        use winapi::um::handleapi::CloseHandle;
        use winapi::um::psapi::GetModuleFileNameExW;
        unsafe {
            let handle = OpenProcess(PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, 0, pid);
            if handle.is_null() { return false; }
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

#[cfg(target_os = "windows")]
fn install_windows_task() -> Result<()> {
    let exe = std::env::current_exe()?;
    std::process::Command::new("schtasks")
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
