use anyhow::{Context, Result};
use std::path::PathBuf;

fn uhoh_dir() -> PathBuf {
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
    let exe = std::env::current_exe()?;
    let uhoh = uhoh_dir();

    let unit = format!(
        r#"[Unit]
Description=uhoh snapshot daemon

[Service]
Type=simple
ExecStart={exe} start --service
Restart=on-failure
RestartSec=5
StandardOutput=append:{log}
StandardError=append:{log}

[Install]
WantedBy=default.target
"#,
        exe = exe.display(),
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
