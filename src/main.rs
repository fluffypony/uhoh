use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
use tracing::warn;

use uhoh::cas;
use uhoh::cli::{Cli, Commands};
use uhoh::config;
use uhoh::daemon;
use uhoh::db;
use uhoh::diff_view;
use uhoh::gc;
use uhoh::git;
use uhoh::marker;
use uhoh::operations;
use uhoh::platform;
use uhoh::restore;
use uhoh::snapshot;
use uhoh::update;

fn uhoh_dir() -> PathBuf { uhoh::platform::uhoh_dir() }

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

fn is_daemon_running(uhoh: &std::path::Path) -> bool {
    let pid_path = uhoh.join("daemon.pid");
    match std::fs::read_to_string(&pid_path) {
        Ok(pid_str) => {
            if let Ok(pid) = pid_str.trim().parse::<u32>() {
                platform::is_uhoh_process_alive(pid)
            } else {
                false
            }
        }
        Err(_) => false,
    }
}

fn maybe_start_daemon(uhoh: &std::path::Path) -> Result<()> {
    if !is_daemon_running(uhoh) {
        tracing::info!("Daemon not running, starting automatically...");
        daemon::spawn_detached_daemon()?;
    }
    Ok(())
}

fn resolve_project_path(path: Option<String>) -> Result<PathBuf> {
    let p = match path {
        Some(s) => dunce::canonicalize(&s)
            .with_context(|| format!("Cannot resolve path: {}", s))?,
        None => dunce::canonicalize(std::env::current_dir()?)
            .context("Cannot resolve current directory")?,
    };
    Ok(p)
}

fn resolve_target_project(
    _uhoh: &std::path::Path,
    database: &db::Database,
    target: Option<&str>,
) -> Result<db::ProjectEntry> {
    match target {
        Some(t) => {
            let as_path = PathBuf::from(t);
            if as_path.exists() {
                let canonical = dunce::canonicalize(&as_path)?;
                return database
                    .find_project_by_path(&canonical)?
                    .context("Not a registered uhoh project");
            }
            database
                .find_project_by_hash_prefix(t)?
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

fn dir_size(path: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            if let Ok(meta) = entry.metadata() {
                if meta.is_dir() {
                    total += dir_size(&entry.path());
                } else {
                    total += meta.len();
                }
            }
        }
    }
    total
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
        return run_zero_verb().await;
    }

    let cli = Cli::parse();
    #[cfg(windows)]
    if let Some(old_pid) = cli.takeover {
        // Wait briefly for previous daemon to exit during self-update restart
        for _ in 0..50 {
            if !platform::is_uhoh_process_alive(old_pid) { break; }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
    let uhoh = ensure_uhoh_dir()?;
    let database = db::Database::open(&uhoh.join("uhoh.db"))?;

    match cli.command {
        Commands::Add { path } => {
            maybe_start_daemon(&uhoh)?;
            let project_path = resolve_project_path(path)?;

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
                warn!("Not a git repo. Marker at {0}/.uhoh — add to your ignore file.", project_path.display());
                eprintln!("⚠ Warning: Not a git repo. Add `.uhoh` to your ignore file.");
            }

            let project_hash = marker::create_marker(&project_path)?;
            let canonical = dunce::canonicalize(&project_path)?;
            database.add_project(&project_hash, &canonical.to_string_lossy())?;
            println!("Registered: {}", canonical.display());

            let cfg = config::Config::load(&uhoh.join("config.toml"))?;
            snapshot::create_snapshot(&uhoh, &database, &project_hash, &canonical, "manual", Some("Initial snapshot"), &cfg)?;
            println!("Initial snapshot created.");
        }

        Commands::Remove { target } => {
            let project = match target {
                Some(ref t) => {
                    let canonical = dunce::canonicalize(t)?;
                    database.find_project_by_path(&canonical)?
                }
                None => {
                    let cwd = dunce::canonicalize(std::env::current_dir()?)?;
                    database.find_project_by_path(&cwd)?
                }
            }.context("Project not found")?;
            // Attempt to remove marker files
            let project_path = std::path::Path::new(&project.current_path);
            let marker_git = project_path.join(".git/.uhoh");
            let marker_root = project_path.join(".uhoh");
            if marker_git.exists() { std::fs::remove_file(&marker_git).ok(); }
            if marker_root.exists() { std::fs::remove_file(&marker_root).ok(); }
            database.remove_project(&project.hash)?;
            println!("Removed: {}", project.current_path);
        }

        Commands::List => {
            let projects = database.list_projects()?;
            if projects.is_empty() {
                println!("No registered projects. Use `uhoh add` to register one.");
            } else {
                for p in &projects {
                    let exists = std::path::Path::new(&p.current_path).exists();
                    let status = if exists { "✓" } else { "✗ MISSING" };
                    let count = database.snapshot_count(&p.hash)?;
                    println!("  {} {} ({} snapshots) [{}]", status, p.current_path, count, &p.hash[..p.hash.len().min(12)]);
                }
            }
        }

        Commands::Snapshots { target } => {
            let project = resolve_target_project(&uhoh, &database, target.as_deref())?;
            let snapshots = database.list_snapshots(&project.hash)?;
            if snapshots.is_empty() {
                println!("No snapshots.");
            } else {
                for s in &snapshots {
                    let id_str = cas::id_to_base58(s.snapshot_id);
                    let pin = if s.pinned { " 📌" } else { "" };
                    let msg = if s.message.is_empty() { String::new() } else { format!(" — {}", s.message) };
                    println!("  {} [{}] {}{}{}", s.timestamp, id_str, s.trigger, pin, msg);
                    // Show files with storage info
                    let files = database.get_snapshot_files(s.rowid)?;
                    for f in files.iter().take(10) {
                        let method = match f.storage_method { 0 => "none", 1 => "copy", 2 => "reflink", 3 => "hardlink", _ => "none" };
                        println!("       {:>8}  {:>7}  {}", f.size, method, f.path);
                    }
                    if files.len() > 10 { println!("       ... and {} more", files.len() - 10); }
                }
            }
        }

        Commands::Commit { message, trigger } => {
            maybe_start_daemon(&uhoh)?;
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database.find_project_by_path(&project_path)?.context("Not registered")?;
            let trigger_str = trigger.unwrap_or_else(|| "manual".to_string());
            let cfg = config::Config::load(&uhoh.join("config.toml"))?;
            snapshot::create_snapshot(&uhoh, &database, &project.hash, &project_path, &trigger_str, message.as_deref(), &cfg)?;
            println!("Snapshot created.");
        }

        Commands::Restore { id, target, dry_run, force } => {
            let project = resolve_target_project(&uhoh, &database, target.as_deref())?;
            restore::cmd_restore(&uhoh, &database, &project, &id, dry_run, force)?;
        }

        Commands::Gitstash { id, target } => {
            let project = resolve_target_project(&uhoh, &database, target.as_deref())?;
            git::cmd_gitstash(&uhoh, &database, &project, &id)?;
        }

        Commands::Diff { id1, id2 } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database.find_project_by_path(&project_path)?.context("Not registered")?;
            diff_view::cmd_diff(&uhoh, &database, &project, id1.as_deref(), id2.as_deref())?;
        }

        Commands::Cat { path, id } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database.find_project_by_path(&project_path)?.context("Not registered")?;
            diff_view::cmd_cat(&uhoh, &database, &project, &path, &id)?;
        }

        Commands::Log { path } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database.find_project_by_path(&project_path)?.context("Not registered")?;
            diff_view::cmd_log(&database, &project, &path)?;
        }

        Commands::Start { service } => {
            if service {
                daemon::run_foreground(&uhoh, &database).await?;
            } else {
                daemon::spawn_detached_daemon()?;
            }
        }
        Commands::Stop => { daemon::stop_daemon(&uhoh)?; }
        Commands::Restart => {
            daemon::stop_daemon(&uhoh).ok();
            std::thread::sleep(std::time::Duration::from_secs(1));
            daemon::spawn_detached_daemon()?;
        }

        Commands::Hook { action } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            match action.as_str() {
                "install" => git::install_hook(&project_path)?,
                "remove" => git::remove_hook(&project_path)?,
                other => anyhow::bail!("Unknown hook action: '{}'. Use 'install' or 'remove'.", other),
            }
        }

        Commands::Config { subcommand } => {
            let config_path = uhoh.join("config.toml");
            match subcommand.as_deref() {
                Some("edit") => {
                    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
                    std::process::Command::new(&editor).arg(&config_path).status()?;
                }
                Some("set") => {
                    // Expect: uhoh config set <key> <value>
                    let args: Vec<String> = std::env::args().collect();
                    // Find the position of "set" and read two following tokens
                    let set_pos = args.iter().position(|a| a == "set");
                    if set_pos.is_none() || args.len() <= set_pos.unwrap() + 2 {
                        println!("Usage: uhoh config set <key> <value>");
                    } else {
                        let key = args[set_pos.unwrap() + 1].clone();
                        let value = args[set_pos.unwrap() + 2].clone();
                        let content = if config_path.exists() { std::fs::read_to_string(&config_path)? } else { String::new() };
                        let mut doc: toml_edit::DocumentMut = content.parse().unwrap_or_else(|_| toml_edit::DocumentMut::new());
                        let parts: Vec<&str> = key.split('.').collect();
                        match parts.as_slice() {
                            [k] => { doc[*k] = toml_edit::value(parse_toml_value(&value)); }
                            [a,b] => {
                                if !doc.contains_key(a) { doc[*a] = toml_edit::Item::Table(toml_edit::Table::new()); }
                                doc[*a][*b] = toml_edit::value(parse_toml_value(&value));
                            }
                            _ => anyhow::bail!("Key nesting deeper than 2 levels is not supported"),
                        }
                        std::fs::write(&config_path, doc.to_string())?;
                        println!("Set {} = {}", key, value);
                    }
                }
                _ => {
                    let cfg = config::Config::load(&config_path)?;
                    println!("{}", toml::to_string_pretty(&cfg)?);
                }
            }
        }

        Commands::Gc => { gc::run_gc(&uhoh, &database)?; }
        Commands::Update => { update::check_and_apply_update(&uhoh).await?; }
        Commands::Doctor { fix, restore_latest, verify_install } => {
            if verify_install {
                return run_verify_install().await;
            }
            run_doctor(&uhoh, &database, fix, restore_latest)?;
        }

        Commands::Status => {
            let running = is_daemon_running(&uhoh);
            println!("Daemon: {}", if running { "running" } else { "stopped" });
            let projects = database.list_projects()?;
            println!("Projects: {}", projects.len());
            let total: u64 = projects.iter().filter_map(|p| database.snapshot_count(&p.hash).ok()).sum();
            println!("Snapshots: {}", total);
            let blobs = uhoh.join("blobs");
            if blobs.exists() {
                let size = tokio::task::spawn_blocking({ let blobs = blobs.clone(); move || dir_size(&blobs) })
                    .await
                    .unwrap_or(0);
                println!("Blob storage: {:.1} MB", size as f64 / 1_048_576.0);
            }
            let cfg = config::Config::load(&uhoh.join("config.toml")).unwrap_or_default();
            println!("AI: {}", if cfg.ai.enabled { "enabled" } else { "disabled" });
        }

        Commands::Mark { label } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database.find_project_by_path(&project_path)?.context("Not registered")?;
            operations::cmd_mark(&database, &project, &label)?;
        }
        Commands::Undo { target } => {
            let project = resolve_target_project(&uhoh, &database, target.as_deref())?;
            operations::cmd_undo(&uhoh, &database, &project)?;
        }
        Commands::Operations { target } => {
            let project = resolve_target_project(&uhoh, &database, target.as_deref())?;
            operations::cmd_list_operations(&database, &project)?;
        }

        Commands::ServiceInstall => { platform::install_service()?; println!("Service installed."); }
        Commands::ServiceRemove => { platform::remove_service()?; println!("Service removed."); }
    }

    Ok(())
}

fn parse_toml_value(s: &str) -> toml_edit::Value {
    if s.eq_ignore_ascii_case("true") { return toml_edit::Value::from(true); }
    if s.eq_ignore_ascii_case("false") { return toml_edit::Value::from(false); }
    if let Ok(i) = s.parse::<i64>() { return toml_edit::Value::from(i); }
    if let Ok(f) = s.parse::<f64>() { return toml_edit::Value::from(f); }
    toml_edit::Value::from(s.to_string())
}

async fn run_zero_verb() -> Result<()> {
    let uhoh = ensure_uhoh_dir()?;
    let database = db::Database::open(&uhoh.join("uhoh.db"))?;
    let cwd = dunce::canonicalize(std::env::current_dir()?)?;

    // Already registered?
    if let Some(project) = database.find_project_by_path(&cwd)? {
        // Show status instead of performing a destructive restore
        let snaps = database.list_snapshots(&project.hash)?; // newest-first
        println!("uhoh is active in this directory.");
        if let Some(latest) = snaps.first() {
            println!("Latest snapshot: {} ({})",
                cas::id_to_base58(latest.snapshot_id),
                latest.timestamp);
            println!("Total snapshots: {}", snaps.len());
        } else {
            println!("No snapshots yet.");
        }
        println!("\nTo undo the last AI operation: uhoh undo");
        println!("To restore a snapshot:         uhoh restore <id>");
        println!("To see recent changes:         uhoh log");
        return Ok(());
    }

    // Not registered: behave like `uhoh add` for this directory
    maybe_start_daemon(&uhoh)?;
    let project_path = cwd;

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
        warn!("Not a git repo. Marker at {0}/.uhoh — add to your ignore file.", project_path.display());
        eprintln!("⚠ Warning: Not a git repo. Add `.uhoh` to your ignore file.");
    }

    let project_hash = marker::create_marker(&project_path)?;
    let canonical = dunce::canonicalize(&project_path)?;
    database.add_project(&project_hash, &canonical.to_string_lossy())?;
    println!("Registered: {}", canonical.display());

    let cfg = config::Config::load(&uhoh.join("config.toml"))?;
    snapshot::create_snapshot(&uhoh, &database, &project_hash, &canonical, "manual", Some("Initial snapshot"), &cfg)?;
    println!("Initial snapshot created.");

    Ok(())
}

fn run_doctor(uhoh_dir: &std::path::Path, database: &db::Database, fix: bool, restore_latest: bool) -> Result<()> {
    // 1) SQLite integrity check
    {
        let conn = rusqlite::Connection::open(uhoh_dir.join("uhoh.db"))?;
        let ok: String = conn
            .prepare("PRAGMA integrity_check;")?
            .query_row([], |row| row.get(0))?;
        if ok != "ok" {
            eprintln!("Database integrity check FAILED: {}", ok);
            if restore_latest {
                // Attempt restore from latest backup
                let backups = uhoh_dir.join("backups");
                if backups.exists() {
                    let mut files: Vec<_> = std::fs::read_dir(&backups)?.flatten().collect();
                    files.sort_by_key(|e| e.file_name());
                    if let Some(last) = files.last() {
                        let src = last.path();
                        let dst = uhoh_dir.join("uhoh.db");
                        std::fs::copy(&src, &dst)?;
                        println!("Restored database from {}", src.display());
                        // Remove WAL/SHM to avoid carrying corruption state
                        let _ = std::fs::remove_file(uhoh_dir.join("uhoh.db-wal"));
                        let _ = std::fs::remove_file(uhoh_dir.join("uhoh.db-shm"));
                    } else {
                        eprintln!("No backups found to restore.");
                    }
                }
            }
        } else {
            println!("Database integrity: ok");
        }
    }

    // 2) Blob store cross-check
    let blob_root = uhoh_dir.join("blobs");
    let referenced = database.all_referenced_blob_hashes()?;
    let mut missing = Vec::new();
    for h in &referenced {
        let p = blob_root.join(&h[..h.len().min(2)]).join(h);
        if !p.exists() { missing.push(h.clone()); }
    }
    println!("Referenced blobs: {}, missing: {}", referenced.len(), missing.len());
    if !missing.is_empty() {
        for m in missing.iter().take(10) {
            println!("  missing {}...", &m[..m.len().min(12)]);
        }
    }

    // 3) Orphan detection
    let mut orphans = Vec::new();
    if blob_root.exists() {
        for pref in std::fs::read_dir(&blob_root)? {
            let pref = match pref { Ok(p) => p, Err(_) => continue };
            if !pref.file_type()?.is_dir() { continue; }
            if pref.file_name() == "tmp" { continue; }
            for e in std::fs::read_dir(pref.path())? {
                let e = match e { Ok(v) => v, Err(_) => continue };
                let name = e.file_name().to_string_lossy().to_string();
                if !referenced.contains(&name) { orphans.push(e.path()); }
            }
        }
    }
    println!("Orphaned blobs: {}", orphans.len());
    if fix && !orphans.is_empty() {
        for o in &orphans { let _ = std::fs::remove_file(o); }
        println!("Removed {} orphaned blobs", orphans.len());
    }

    // 4) Hash verification for referenced blobs (detect on-disk corruption)
    let mut corrupted = Vec::new();
    for h in &referenced {
        let p = blob_root.join(&h[..h.len().min(2)]).join(h);
        if !p.exists() { continue; }
        match std::fs::read(&p) {
            Ok(bytes) => {
                let actual = blake3::hash(&bytes).to_hex().to_string();
                if actual != *h {
                    corrupted.push((h.clone(), p.clone()));
                }
            }
            Err(e) => {
                tracing::warn!("Failed to read blob {}: {}", &h[..h.len().min(12)], e);
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
        println!("Moved {} corrupted blobs to {}", corrupted.len(), quarantine.display());
    }

    Ok(())
}

async fn run_verify_install() -> Result<()> {
    let exe_path = std::env::current_exe()
        .context("Could not determine path to running binary")?;
    let exe_bytes = std::fs::read(&exe_path)
        .context("Could not read running binary")?;

    let local_hash = blake3::hash(&exe_bytes).to_hex().to_string();
    let version = env!("CARGO_PKG_VERSION");
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    let asset_name = format!("uhoh-{}-{}", os, arch);

    println!("Binary:  {}", exe_path.display());
    println!("Version: {}", version);
    println!("Hash:    {}", local_hash);
    println!("Asset:   {}", asset_name);

    match uhoh::update::dns_verify_hash(version, &asset_name).await {
        Ok(expected) => {
            if expected.eq_ignore_ascii_case(&local_hash) {
                println!("\u{2713} Binary hash matches DNS record.");
                std::process::exit(0);
            } else {
                eprintln!("Binary hash does not match DNS record!");
                eprintln!("  Local:    {}", local_hash);
                eprintln!("  Expected: {}", expected);
                std::process::exit(2);
            }
        }
        Err(e) => {
            eprintln!("Could not verify hash via DNS: {}", e);
            // Non-fatal: installer treats this as a warning
            std::process::exit(0);
        }
    }
}
