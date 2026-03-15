#[cfg(all(unix, target_os = "linux"))]
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::cas;
use crate::cli::{ConfigAction, HookAction};
use crate::config;
use crate::daemon;
use crate::db;
use crate::gc;
use super::git;
use crate::platform;
use crate::update;

use super::shared::is_daemon_running;

pub async fn start(uhoh: &Path, database: &db::Database, service: bool) -> Result<()> {
    if service {
        daemon::run_foreground(uhoh, Arc::new(database.clone_handle())).await?;
    } else {
        daemon::spawn_detached_daemon(uhoh)?;
    }
    Ok(())
}

pub fn stop(uhoh: &Path) -> Result<()> {
    daemon::stop_daemon(uhoh)
}

pub fn restart(uhoh: &Path) -> Result<()> {
    daemon::stop_daemon(uhoh).ok();
    std::thread::sleep(std::time::Duration::from_secs(1));
    daemon::spawn_detached_daemon(uhoh)
}

pub fn hook(action: HookAction) -> Result<()> {
    let project_path = dunce::canonicalize(std::env::current_dir()?)?;
    match action {
        HookAction::Install => git::install_hook(&project_path),
        HookAction::Remove => git::remove_hook(&project_path),
    }
}

pub fn config_action(uhoh: &Path, action: Option<ConfigAction>) -> Result<()> {
    let config_path = uhoh.join("config.toml");
    match action {
        Some(ConfigAction::Edit) => {
            let _ = config::Config::load_or_initialize(&config_path)?;
            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
            std::process::Command::new(&editor)
                .arg(&config_path)
                .status()?;
        }
        Some(ConfigAction::Set { key, value }) => {
            let content = if config_path.exists() {
                std::fs::read_to_string(&config_path)?
            } else {
                String::new()
            };
            let mut doc: toml_edit::DocumentMut = content
                .parse()
                .unwrap_or_else(|_| toml_edit::DocumentMut::new());
            let parts: Vec<&str> = key.split('.').collect();
            match parts.as_slice() {
                [key] => {
                    doc[*key] = toml_edit::value(parse_toml_value(&value));
                }
                [section, key] => {
                    if !doc.contains_key(section) {
                        doc[*section] = toml_edit::Item::Table(toml_edit::Table::new());
                    }
                    doc[*section][*key] = toml_edit::value(parse_toml_value(&value));
                }
                _ => anyhow::bail!("Key nesting deeper than 2 levels is not supported"),
            }
            std::fs::write(&config_path, doc.to_string())?;
            println!("Set {key} = {value}");
        }
        Some(ConfigAction::Get { key }) => {
            let content = if config_path.exists() {
                std::fs::read_to_string(&config_path)?
            } else {
                String::new()
            };
            let doc: toml_edit::DocumentMut = content
                .parse()
                .unwrap_or_else(|_| toml_edit::DocumentMut::new());
            let parts: Vec<&str> = key.split('.').collect();
            let out = match parts.as_slice() {
                [key] => doc
                    .get(key)
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default(),
                [section, key] => doc
                    .get(section)
                    .and_then(|table| table.get(*key))
                    .map(std::string::ToString::to_string)
                    .unwrap_or_default(),
                _ => String::new(),
            };
            println!("{out}");
        }
        None => {
            let cfg = config::Config::load(&config_path)?;
            println!("{}", toml::to_string_pretty(&cfg)?);
        }
    }
    Ok(())
}

pub fn run_gc(uhoh: &Path, database: &db::Database) -> Result<()> {
    gc::run_gc(uhoh, database)
}

pub async fn update(uhoh: &Path) -> Result<()> {
    update::check_and_apply_update(uhoh).await
}

pub async fn doctor(
    uhoh_dir: &Path,
    database: db::Database,
    fix: bool,
    restore_latest: bool,
    verify_install_requested: bool,
) -> Result<()> {
    if verify_install_requested {
        return verify_install().await;
    }
    run_doctor(uhoh_dir, database, fix, restore_latest).await
}

pub async fn status(uhoh: &Path, database: &db::Database) -> Result<()> {
    let running = is_daemon_running(uhoh);
    println!("Daemon: {}", if running { "running" } else { "stopped" });
    let projects = database.list_projects()?;
    println!("Projects: {}", projects.len());
    let total: u64 = projects
        .iter()
        .map(|project| match database.snapshot_count(&project.hash) {
            Ok(count) => count,
            Err(e) => {
                tracing::warn!("Failed to count snapshots for {}: {e}", project.hash);
                0
            }
        })
        .sum();
    println!("Snapshots: {total}");
    match database.blob_bytes() {
        Ok(size) => println!("Blob storage: {:.1} MB", size as f64 / 1_048_576.0),
        Err(e) => println!("Blob storage: unavailable ({e})"),
    }
    let cfg = match config::Config::load(&uhoh.join("config.toml")) {
        Ok(c) => c,
        Err(e) => {
            println!("Config: failed to load ({e}), using defaults");
            config::Config::default()
        }
    };
    println!(
        "AI: {}",
        if cfg.ai.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    for project in &projects {
        let project_path = Path::new(&project.current_path);
        if uhoh.starts_with(project_path) {
            println!(
                "Warning: Project {} includes the uhoh data directory; this may cause snapshot loops.",
                project.current_path
            );
            break;
        }
    }

    if running {
        let health_msg = match fetch_subsystem_health(uhoh).await {
            Ok(subsystems) => {
                let mut lines = vec!["Subsystems:".to_string()];
                for item in &subsystems {
                    let name = item
                        .get("name")
                        .and_then(|value| value.as_str())
                        .unwrap_or("unknown");
                    let status = item
                        .get("status")
                        .and_then(|value| value.as_str())
                        .unwrap_or("unknown");
                    lines.push(format!("  - {name}: {status}"));
                }
                lines.join("\n")
            }
            Err(e) => format!("Subsystems: unavailable ({e})"),
        };
        println!("{health_msg}");
    }

    Ok(())
}

async fn fetch_subsystem_health(uhoh: &Path) -> Result<Vec<serde_json::Value>> {
    let port_raw = std::fs::read_to_string(uhoh.join("server.port"))
        .context("could not read server.port")?;
    let port: u16 = port_raw.trim().parse().context("invalid port in server.port")?;
    let url = format!("http://127.0.0.1:{port}/health");
    let resp = reqwest::get(&url).await.context("could not reach health endpoint")?;
    let json: serde_json::Value = resp.json().await.context("invalid health response")?;
    json.get("subsystems")
        .and_then(|value| value.as_array())
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no subsystems field in health response"))
}

pub async fn run_wrapped_command(uhoh: &Path, command: Vec<String>) -> Result<()> {
    if command.is_empty() {
        anyhow::bail!("No command provided");
    }
    let cfg = config::Config::load(&uhoh.join("config.toml")).unwrap_or_default();
    let mut cmd = std::process::Command::new(&command[0]);
    cmd.args(&command[1..]);

    if !super::shared::is_daemon_running(uhoh) {
        eprintln!(
            "Warning: uhoh daemon is not running. Start it with `uhoh start` for full protection."
        );
    }
    if cfg.agent.mcp_proxy_enabled {
        let proxy_token = crate::agent::ensure_proxy_token(uhoh)?;
        let auth_line = crate::agent::auth_handshake_line(&proxy_token);
        cmd.env(
            "UHOH_MCP_PROXY_ADDR",
            format!("127.0.0.1:{}", cfg.agent.mcp_proxy_port),
        );
        cmd.env("UHOH_MCP_PROXY_TOKEN", &proxy_token);
        cmd.env("UHOH_MCP_PROXY_AUTH_LINE", auth_line);
        cmd.env(
            "UHOH_AGENT_MCP_UPSTREAM",
            std::env::var("UHOH_AGENT_MCP_UPSTREAM")
                .unwrap_or_else(|_| "127.0.0.1:22824".to_string()),
        );
    }

    if cfg.agent.sandbox_enabled {
        if !crate::agent::sandbox_supported() {
            anyhow::bail!("Sandbox requested in config but unsupported on this platform/build");
        }
        cmd.env("UHOH_SANDBOX_ENABLED", "1");

        #[cfg(target_os = "linux")]
        // SAFETY: CommandExt::pre_exec requires unsafe because the closure runs between
        // fork and exec where only async-signal-safe operations are strictly safe. This
        // closure performs file I/O and allocation which is technically not async-signal-safe,
        // but is acceptable here because: (1) this is a single-threaded CLI context with no
        // other threads that could hold locks, and (2) the operations complete before exec.
        unsafe {
            cmd.pre_exec(|| {
                let profile_path = std::env::var("UHOH_AGENT_PROFILE").unwrap_or_else(|_| {
                    format!(
                        "{}/.uhoh/agents/generic.toml",
                        dirs::home_dir().unwrap_or_default().display()
                    )
                });
                let profile =
                    crate::agent::load_agent_profile(std::path::Path::new(&profile_path))
                        .map_err(|e| std::io::Error::other(e.to_string()))?;
                crate::agent::apply_landlock(&profile)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
                Ok(())
            });
        }
    }

    cmd.env("UHOH_AGENT_RUNTIME_DIR", uhoh.join("agents/runtime"));

    #[cfg(target_os = "linux")]
    {
        let mut child = cmd
            .spawn()
            .with_context(|| format!("Failed to run command: {}", command[0]))?;
        let pid_u32 = child.id();
        let pid = i32::try_from(pid_u32).unwrap_or(i32::MAX);
        let mut pidfd = -1;
        // SAFETY: pidfd_open is called with a valid pid obtained from child.id();
        // failure returns -1 which is checked immediately below.
        unsafe {
            pidfd = libc::syscall(libc::SYS_pidfd_open, pid, 0) as i32;
        }
        if pidfd >= 0 {
            tracing::info!("pidfd supervision enabled for pid {}", pid);
            // SAFETY: MaybeUninit<siginfo_t> does not require initialization; waitid writes
            // into the pointer on success. We zero-initialize defensively so that even on
            // failure the memory is in a well-defined state.
            let mut status = std::mem::MaybeUninit::<libc::siginfo_t>::zeroed();
            // SAFETY: waitid with P_PIDFD on a valid pidfd obtained above; status points to
            // a properly aligned, sufficiently sized MaybeUninit<siginfo_t> buffer.
            let waited = unsafe {
                libc::waitid(
                    libc::P_PIDFD,
                    pidfd as u32,
                    status.as_mut_ptr(),
                    libc::WEXITED | libc::WNOWAIT,
                )
            };
            // SAFETY: closing a valid pidfd obtained from SYS_pidfd_open above;
            // the fd is not used after this point.
            unsafe {
                libc::close(pidfd);
            }
            if waited != 0 {
                tracing::warn!(
                    "pidfd wait failed for pid {}, falling back to child.wait",
                    pid
                );
            }
            let exit_status = child
                .wait()
                .with_context(|| format!("Failed to collect status for {}", command[0]))?;
            if !exit_status.success() {
                // process::exit is intentional: we must forward the wrapped child's exact
                // exit code to the parent without printing spurious error output.
                std::process::exit(exit_status.code().unwrap_or(1));
            }
        } else {
            tracing::warn!(
                "pidfd_open unavailable; falling back to standard child wait for pid {}",
                pid
            );
            let status = child
                .wait()
                .with_context(|| format!("Failed to wait for command: {}", command[0]))?;
            if !status.success() {
                // process::exit is intentional: forward the child's exact exit code.
                std::process::exit(status.code().unwrap_or(1));
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let status = cmd
            .status()
            .with_context(|| format!("Failed to run command: {}", command[0]))?;
        if !status.success() {
            // process::exit is intentional: forward the child's exact exit code.
            std::process::exit(status.code().unwrap_or(1));
        }
    }

    Ok(())
}

pub fn install_service() -> Result<()> {
    platform::install_service()?;
    println!("Service installed.");
    Ok(())
}

pub fn remove_service() -> Result<()> {
    platform::remove_service()?;
    println!("Service removed.");
    Ok(())
}

fn parse_toml_value(s: &str) -> toml_edit::Value {
    if s.eq_ignore_ascii_case("true") {
        return toml_edit::Value::from(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return toml_edit::Value::from(false);
    }
    if let Ok(i) = s.parse::<i64>() {
        return toml_edit::Value::from(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return toml_edit::Value::from(f);
    }
    toml_edit::Value::from(s.to_string())
}

async fn run_doctor(
    uhoh_dir: &Path,
    database: db::Database,
    fix: bool,
    restore_latest: bool,
) -> Result<()> {
    let mut integrity_ok = true;
    {
        let conn = rusqlite::Connection::open(uhoh_dir.join("uhoh.db"))?;
        let ok: String = conn
            .prepare("PRAGMA integrity_check;")?
            .query_row([], |row| row.get(0))?;
        if ok != "ok" {
            integrity_ok = false;
            eprintln!("Database integrity check FAILED: {ok}");
        } else {
            println!("Database integrity: ok");
        }
    }

    let database = if !integrity_ok && restore_latest {
        drop(database);
        let backups = uhoh_dir.join("backups");
        if backups.exists() {
            let mut files: Vec<_> = std::fs::read_dir(&backups)?.flatten().collect();
            files.sort_by_key(std::fs::DirEntry::file_name);
            if let Some(last) = files.last() {
                let src = last.path();
                let dst = uhoh_dir.join("uhoh.db");
                std::fs::copy(&src, &dst)?;
                println!("Restored database from {}", src.display());
                let _ = std::fs::remove_file(uhoh_dir.join("uhoh.db-wal"));
                let _ = std::fs::remove_file(uhoh_dir.join("uhoh.db-shm"));
                if let Ok(conn) = rusqlite::Connection::open(uhoh_dir.join("uhoh.db")) {
                    if let Err(e) = conn
                        .execute_batch("INSERT INTO search_index(search_index) VALUES('rebuild');")
                    {
                        tracing::warn!("FTS5 index rebuild after restore failed: {e}");
                    } else {
                        println!("FTS5 search index rebuilt.");
                        println!("Note: search results may be incomplete for snapshots that existed in the original DB but not in the backup.");
                    }
                }
            } else {
                eprintln!("No backups found to restore.");
            }
        }
        db::Database::open(&uhoh_dir.join("uhoh.db"))?
    } else {
        database
    };

    let blob_root = uhoh_dir.join("blobs");
    let referenced = database.all_referenced_blob_hashes()?;
    let mut missing = Vec::new();
    for h in &referenced {
        let p = blob_root.join(&h[..h.len().min(2)]).join(h);
        if !p.exists() {
            missing.push(h.clone());
        }
    }
    println!(
        "Referenced blobs: {}, missing: {}",
        referenced.len(),
        missing.len()
    );
    if !missing.is_empty() {
        for m in missing.iter().take(10) {
            println!("  missing {}...", &m[..m.len().min(12)]);
        }
    }

    let mut orphans = Vec::new();
    if blob_root.exists() {
        for pref in std::fs::read_dir(&blob_root)? {
            let Ok(pref) = pref else { continue };
            if !pref.file_type()?.is_dir() || pref.file_name() == "tmp" {
                continue;
            }
            for entry in std::fs::read_dir(pref.path())? {
                let Ok(entry) = entry else { continue };
                let name = entry.file_name().to_string_lossy().to_string();
                if !referenced.contains(&name) {
                    orphans.push(entry.path());
                }
            }
        }
    }
    println!("Orphaned blobs: {}", orphans.len());
    if fix && !orphans.is_empty() {
        for orphan in &orphans {
            let _ = std::fs::remove_file(orphan);
        }
        println!("Removed {} orphaned blobs", orphans.len());
    }

    let mut corrupted = Vec::new();
    for h in &referenced {
        let p = blob_root.join(&h[..h.len().min(2)]).join(h);
        if !p.exists() {
            continue;
        }
        match cas::read_blob(&blob_root, h) {
            Ok(Some(_)) => {}
            Ok(None) => corrupted.push((h.clone(), p.clone())),
            Err(e) => {
                tracing::warn!("Failed to read blob {}: {}", &h[..h.len().min(12)], e);
                corrupted.push((h.clone(), p.clone()));
            }
        }
    }
    println!("Corrupted blobs (hash mismatch): {}", corrupted.len());
    for (hash, _) in corrupted.iter().take(10) {
        println!("  corrupt {}...", &hash[..hash.len().min(12)]);
    }
    if fix && !corrupted.is_empty() {
        let quarantine = uhoh_dir.join("quarantine");
        std::fs::create_dir_all(&quarantine).ok();
        let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
        for (hash, path) in &corrupted {
            let target = quarantine.join(format!(
                "corrupt-{}-{}.blob",
                &hash[..hash.len().min(12)],
                ts
            ));
            let _ = std::fs::rename(path, target);
        }
        println!(
            "Moved {} corrupted blobs to {}",
            corrupted.len(),
            quarantine.display()
        );
    }

    println!("\nBinary integrity check:");
    let exe_path = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("uhoh"));
    let local_hash = std::fs::read(&exe_path)
        .map(|bytes| blake3::hash(&bytes).to_hex().to_string())
        .unwrap_or_else(|_| String::from("unknown"));
    let version = env!("CARGO_PKG_VERSION");
    let asset_name = format!("uhoh-{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    let dns = crate::update::dns_verify_hash(version, &asset_name)
        .await
        .ok();
    match dns {
        Some(expected) => {
            if expected.eq_ignore_ascii_case(&local_hash) {
                println!("  \u{2713} Binary hash matches DNS record");
            } else {
                println!("  \u{26A0} DNS hash mismatch");
                println!("    Local:    {}", &local_hash[..local_hash.len().min(16)]);
                println!("    Expected: {}", &expected[..expected.len().min(16)]);
            }
        }
        None => {
            println!("  \u{26A0} Could not verify via DNS (network/DNS unavailable)");
        }
    }

    Ok(())
}

async fn verify_install() -> Result<()> {
    let exe_path = std::env::current_exe().context("Could not determine path to running binary")?;
    let exe_bytes = std::fs::read(&exe_path).context("Could not read running binary")?;

    let local_hash = blake3::hash(&exe_bytes).to_hex().to_string();
    let version = env!("CARGO_PKG_VERSION");
    let asset_name = format!("uhoh-{}-{}", std::env::consts::OS, std::env::consts::ARCH);

    println!("Binary:  {}", exe_path.display());
    println!("Version: {version}");
    println!("Hash:    {local_hash}");
    println!("Asset:   {asset_name}");

    match crate::update::dns_verify_hash(version, &asset_name).await {
        Ok(expected) => {
            if expected.eq_ignore_ascii_case(&local_hash) {
                println!("\u{2713} Binary hash matches DNS record.");
                Ok(())
            } else {
                // Exit code 2 is the install-script contract for hash mismatch.
                // process::exit is intentional: the specific exit code is part of the
                // external protocol and cannot be expressed through anyhow::Result.
                eprintln!("Binary hash does not match DNS record!");
                eprintln!("  Local:    {local_hash}");
                eprintln!("  Expected: {expected}");
                std::process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("Could not verify hash via DNS: {e}");
            Ok(())
        }
    }
}
