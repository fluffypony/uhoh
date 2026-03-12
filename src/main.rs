use anyhow::{Context, Result};
use clap::Parser;
// RootCertStore no longer needed here — TLS connector is shared from db_guard::postgres
#[cfg(all(unix, target_os = "linux"))]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;

use uhoh::cas;
use uhoh::cli::{Cli, Commands, LedgerAction};
use uhoh::commands::{
    ledger as ledger_commands, project as project_commands, runtime as runtime_commands,
    shared as command_shared,
};
use uhoh::config;
use uhoh::db;

// Deduplicated: use library function for ~/.uhoh
fn uhoh_dir() -> PathBuf {
    std::env::var_os("UHOH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(uhoh::uhoh_dir)
}

fn ensure_uhoh_dir() -> Result<PathBuf> {
    let dir = uhoh_dir();
    if !dir.exists() {
        std::fs::create_dir_all(&dir).context("Failed to create ~/.uhoh directory")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    let blobs_dir = dir.join("blobs");
    if !blobs_dir.exists() {
        std::fs::create_dir_all(&blobs_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&blobs_dir, std::fs::Permissions::from_mode(0o700))?;
        }
    }
    Ok(dir)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    // Zero-verb convenience: running `uhoh` with no args performs:
    // - If current folder is not registered: register and create an initial snapshot
    // - If it is registered: create a quick snapshot and revert to the previous snapshot
    if std::env::args().len() == 1 {
        return run_zero_verb();
    }

    let cli = Cli::parse();
    #[cfg(windows)]
    if let Some(old_pid) = cli.takeover {
        // Wait briefly for previous daemon to exit during self-update restart
        for _ in 0..50 {
            if !platform::is_uhoh_process_alive(old_pid) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    let uhoh = ensure_uhoh_dir()?;
    let database = db::Database::open(&uhoh.join("uhoh.db"))?;

    match cli.command {
        Commands::Add { path } => project_commands::add(&uhoh, &database, path)?,
        Commands::Remove { target } => project_commands::remove(&database, target)?,
        Commands::List => project_commands::list(&database)?,
        Commands::Snapshots { target } => project_commands::snapshots(&database, target)?,
        Commands::Commit { message, trigger } => {
            project_commands::commit(&uhoh, &database, message, trigger)?
        }
        Commands::Restore {
            id,
            target,
            dry_run,
            force,
        } => project_commands::restore_snapshot(&uhoh, &database, &id, target, dry_run, force)?,
        Commands::Gitstash { id, target } => {
            project_commands::gitstash(&uhoh, &database, &id, target)?
        }
        Commands::Diff { id1, id2 } => project_commands::diff(&uhoh, &database, id1, id2)?,
        Commands::Cat { path, id } => project_commands::cat(&uhoh, &database, &path, &id)?,
        Commands::Log { path } => project_commands::log(&database, &path)?,
        Commands::Mcp => uhoh::mcp::run_stdio_server(&uhoh)?,
        Commands::Start { service } => runtime_commands::start(&uhoh, &database, service).await?,
        Commands::Stop => runtime_commands::stop(&uhoh)?,
        Commands::Restart => runtime_commands::restart(&uhoh)?,
        Commands::Hook { action } => runtime_commands::hook(&action)?,
        Commands::Config { action } => runtime_commands::config_action(&uhoh, action)?,
        Commands::Gc => runtime_commands::run_gc(&uhoh, &database)?,
        Commands::Update => runtime_commands::update(&uhoh).await?,
        Commands::Doctor {
            fix,
            restore_latest,
            verify_install,
        } => {
            if verify_install {
                return run_verify_install().await;
            }
            // Drop the main database handle so run_doctor has sole ownership
            // of the pool — necessary for safe backup restore on Windows.
            drop(database);
            let doctor_db = db::Database::open(&uhoh.join("uhoh.db"))?;
            run_doctor(&uhoh, doctor_db, fix, restore_latest).await?;
        }
        Commands::Status => runtime_commands::status(&uhoh, &database).await?,
        Commands::Mark { label } => project_commands::mark(&database, &label)?,
        Commands::Undo { target } => project_commands::undo(&uhoh, &database, target)?,
        Commands::Operations { target } => project_commands::operations(&database, target)?,
        Commands::ServiceInstall => runtime_commands::install_service()?,
        Commands::ServiceRemove => runtime_commands::remove_service()?,

        Commands::Db { action } => {
            uhoh::db_guard::handle_cli_action(&database, &uhoh, &action)?;
        }

        Commands::Agent { action } => {
            uhoh::agent::handle_cli_action(&uhoh, &database, &action)?;
        }

        Commands::Trace { event_id } => ledger_commands::trace(&database, event_id)?,
        Commands::Blame { path } => ledger_commands::blame(&database, &path)?,
        Commands::Timeline { source, since } => {
            ledger_commands::timeline(&database, source, since)?
        }
        Commands::Ledger { action } => match action {
            LedgerAction::Verify => ledger_commands::verify(&database)?,
        },

        Commands::Run { command } => {
            if command.is_empty() {
                anyhow::bail!("No command provided");
            }
            let cfg = config::Config::load(&uhoh.join("config.toml")).unwrap_or_default();
            let mut cmd = std::process::Command::new(&command[0]);
            cmd.args(&command[1..]);

            // Verify daemon is running before exporting proxy env vars
            if !command_shared::is_daemon_running(&uhoh) {
                eprintln!("Warning: uhoh daemon is not running. Start it with `uhoh start` for full protection.");
            }
            if cfg.agent.mcp_proxy_enabled {
                let proxy_token = uhoh::agent::ensure_proxy_token(&uhoh)?;
                let auth_line = uhoh::agent::proxy_auth_handshake_line(&proxy_token);
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
                if !uhoh::agent::sandbox_supported() {
                    anyhow::bail!(
                        "Sandbox requested in config but unsupported on this platform/build"
                    );
                }
                cmd.env("UHOH_SANDBOX_ENABLED", "1");

                #[cfg(target_os = "linux")]
                unsafe {
                    cmd.pre_exec(|| {
                        let profile_path =
                            std::env::var("UHOH_AGENT_PROFILE").unwrap_or_else(|_| {
                                format!(
                                    "{}/.uhoh/agents/generic.toml",
                                    dirs::home_dir().unwrap_or_default().display()
                                )
                            });
                        let profile =
                            uhoh::agent::load_agent_profile(std::path::Path::new(&profile_path))
                                .map_err(|e| {
                                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                                })?;
                        uhoh::agent::apply_landlock(&profile).map_err(|e| {
                            std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                        })?;
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
                // Linux >=5.3 exposes pidfd_open for stable process handles.
                unsafe {
                    pidfd = libc::syscall(libc::SYS_pidfd_open, pid, 0) as i32;
                }
                if pidfd >= 0 {
                    tracing::info!("pidfd supervision enabled for pid {}", pid);
                    let mut status: libc::siginfo_t = unsafe { std::mem::zeroed() };
                    // Wait via pidfd so PID recycling cannot affect process tracking.
                    let waited = unsafe {
                        libc::waitid(
                            libc::P_PIDFD,
                            pidfd as u32,
                            &mut status,
                            libc::WEXITED | libc::WNOWAIT,
                        )
                    };
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
                        std::process::exit(status.code().unwrap_or(1));
                    }
                }
            }

            #[cfg(not(target_os = "linux"))]
            {
                let status = cmd
                    .status()
                    .with_context(|| format!("Failed to run command: {}", command[0]))?;
                std::process::exit(status.code().unwrap_or(1));
            }
        }
    }

    Ok(())
}

fn run_zero_verb() -> Result<()> {
    let uhoh = ensure_uhoh_dir()?;
    let database = db::Database::open(&uhoh.join("uhoh.db"))?;
    let cwd = dunce::canonicalize(std::env::current_dir()?)?;

    // Already registered?
    if let Some(project) = database.find_project_by_path(&cwd)? {
        // Show status instead of performing a destructive restore
        let snaps = database.list_snapshots(&project.hash)?; // newest-first
        println!("uhoh is active in this directory.");
        if let Some(latest) = snaps.first() {
            println!(
                "Latest snapshot: {} ({})",
                cas::id_to_base58(latest.snapshot_id),
                latest.timestamp
            );
            println!("Total snapshots: {}", snaps.len());
        } else {
            println!("No snapshots yet.");
        }
        println!("\nTo undo the last AI operation: uhoh undo");
        println!("To restore a snapshot:         uhoh restore <id>");
        println!("To see recent changes:         uhoh log");
        return Ok(());
    }

    project_commands::add(&uhoh, &database, Some(cwd.to_string_lossy().into_owned()))
}

async fn run_doctor(
    uhoh_dir: &std::path::Path,
    database: db::Database,
    fix: bool,
    restore_latest: bool,
) -> Result<()> {
    // 1) SQLite integrity check
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

    // Drop the database pool before restore to release all connections.
    // This prevents file-locking conflicts (especially on Windows) and
    // ensures no stale connections reference the old DB/WAL state.
    let database = if !integrity_ok && restore_latest {
        drop(database);
        let backups = uhoh_dir.join("backups");
        if backups.exists() {
            let mut files: Vec<_> = std::fs::read_dir(&backups)?.flatten().collect();
            files.sort_by_key(|e| e.file_name());
            if let Some(last) = files.last() {
                let src = last.path();
                let dst = uhoh_dir.join("uhoh.db");
                std::fs::copy(&src, &dst)?;
                println!("Restored database from {}", src.display());
                let _ = std::fs::remove_file(uhoh_dir.join("uhoh.db-wal"));
                let _ = std::fs::remove_file(uhoh_dir.join("uhoh.db-shm"));
                // Rebuild FTS5 search index after database restore
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
        // Re-open pool after restore
        db::Database::open(&uhoh_dir.join("uhoh.db"))?
    } else {
        database
    };

    // 2) Blob store cross-check
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

    // 3) Orphan detection
    let mut orphans = Vec::new();
    if blob_root.exists() {
        for pref in std::fs::read_dir(&blob_root)? {
            let pref = match pref {
                Ok(p) => p,
                Err(_) => continue,
            };
            if !pref.file_type()?.is_dir() {
                continue;
            }
            if pref.file_name() == "tmp" {
                continue;
            }
            for e in std::fs::read_dir(pref.path())? {
                let e = match e {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let name = e.file_name().to_string_lossy().to_string();
                if !referenced.contains(&name) {
                    orphans.push(e.path());
                }
            }
        }
    }
    println!("Orphaned blobs: {}", orphans.len());
    if fix && !orphans.is_empty() {
        for o in &orphans {
            let _ = std::fs::remove_file(o);
        }
        println!("Removed {} orphaned blobs", orphans.len());
    }

    // 4) Hash verification for referenced blobs (detect on-disk corruption)
    // Uses read_blob() which handles decompression and integrity verification
    let mut corrupted = Vec::new();
    for h in &referenced {
        let p = blob_root.join(&h[..h.len().min(2)]).join(h);
        if !p.exists() {
            continue;
        }
        match cas::read_blob(&blob_root, h) {
            Ok(Some(_)) => { /* blob is valid */ }
            Ok(None) => {
                // read_blob returns None on hash mismatch
                corrupted.push((h.clone(), p.clone()));
            }
            Err(e) => {
                tracing::warn!("Failed to read blob {}: {}", &h[..h.len().min(12)], e);
                corrupted.push((h.clone(), p.clone()));
            }
        }
    }
    println!("Corrupted blobs (hash mismatch): {}", corrupted.len());
    for (h, _) in corrupted.iter().take(10) {
        println!("  corrupt {}...", &h[..h.len().min(12)]);
    }
    if fix && !corrupted.is_empty() {
        let quarantine = uhoh_dir.join("quarantine");
        std::fs::create_dir_all(&quarantine).ok();
        let ts = chrono::Utc::now().format("%Y%m%d-%H%M%S");
        for (h, p) in &corrupted {
            let target = quarantine.join(format!("corrupt-{}-{}.blob", &h[..h.len().min(12)], ts));
            let _ = std::fs::rename(p, target);
        }
        println!(
            "Moved {} corrupted blobs to {}",
            corrupted.len(),
            quarantine.display()
        );
    }

    // 5) Binary integrity check (non-fatal)
    println!("\nBinary integrity check:");
    let exe_path = std::env::current_exe().unwrap_or_else(|_| std::path::PathBuf::from("uhoh"));
    let local_hash = std::fs::read(&exe_path)
        .map(|b| blake3::hash(&b).to_hex().to_string())
        .unwrap_or_else(|_| String::from("unknown"));
    let version = env!("CARGO_PKG_VERSION");
    let asset_name = format!("uhoh-{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    // Perform async DNS query via update module
    let dns = uhoh::update::dns_verify_hash(version, &asset_name)
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

async fn run_verify_install() -> Result<()> {
    let exe_path = std::env::current_exe().context("Could not determine path to running binary")?;
    let exe_bytes = std::fs::read(&exe_path).context("Could not read running binary")?;

    let local_hash = blake3::hash(&exe_bytes).to_hex().to_string();
    let version = env!("CARGO_PKG_VERSION");
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let asset_name = format!("uhoh-{os}-{arch}");

    println!("Binary:  {}", exe_path.display());
    println!("Version: {version}");
    println!("Hash:    {local_hash}");
    println!("Asset:   {asset_name}");

    match uhoh::update::dns_verify_hash(version, &asset_name).await {
        Ok(expected) => {
            if expected.eq_ignore_ascii_case(&local_hash) {
                println!("\u{2713} Binary hash matches DNS record.");
                std::process::exit(0);
            } else {
                eprintln!("Binary hash does not match DNS record!");
                eprintln!("  Local:    {local_hash}");
                eprintln!("  Expected: {expected}");
                std::process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("Could not verify hash via DNS: {e}");
            // Non-fatal: installer treats this as a warning
            std::process::exit(0);
        }
    }
}
