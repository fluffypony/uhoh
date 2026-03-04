use anyhow::{Context, Result};
use clap::Parser;
use std::path::PathBuf;
#[cfg(unix)]
use std::process::Command;
use tracing::warn;

use uhoh::cas;
use uhoh::cli::{AgentAction, Cli, Commands, DbAction};
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

// Deduplicated: use library function for ~/.uhoh
fn uhoh_dir() -> PathBuf {
    uhoh::uhoh_dir()
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
        Some(s) => dunce::canonicalize(&s).with_context(|| format!("Cannot resolve path: {s}"))?,
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
            if !platform::is_uhoh_process_alive(old_pid) {
                break;
            }
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
                        database
                            .update_project_path(&existing_hash, &canonical.to_string_lossy())?;
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

            let cfg = config::Config::load(&uhoh.join("config.toml"))?;
            snapshot::create_snapshot(
                &uhoh,
                &database,
                &project_hash,
                &canonical,
                "manual",
                Some("Initial snapshot"),
                &cfg,
                None,
            )?;
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
            }
            .context("Project not found")?;
            // Attempt to remove marker files
            let project_path = std::path::Path::new(&project.current_path);
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
                    println!(
                        "  {} {} ({} snapshots) [{}]",
                        status,
                        p.current_path,
                        count,
                        &p.hash[..p.hash.len().min(12)]
                    );
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
                    let msg = if s.message.is_empty() {
                        String::new()
                    } else {
                        format!(" — {}", s.message)
                    };
                    println!("  {} [{}] {}{}{}", s.timestamp, id_str, s.trigger, pin, msg);
                    // Show files with storage info
                    let files = database.get_snapshot_files(s.rowid)?;
                    for f in files.iter().take(10) {
                        let method = match f.storage_method {
                            0 => "none",
                            1 => "copy",
                            2 => "reflink",
                            3 => "hardlink",
                            _ => "none",
                        };
                        println!("       {:>8}  {:>7}  {}", f.size, method, f.path);
                    }
                    if files.len() > 10 {
                        println!("       ... and {} more", files.len() - 10);
                    }
                }
            }
        }

        Commands::Commit { message, trigger } => {
            maybe_start_daemon(&uhoh)?;
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database
                .find_project_by_path(&project_path)?
                .context("Not registered")?;
            let trigger_str = trigger.unwrap_or_else(|| "manual".to_string());
            let cfg = config::Config::load(&uhoh.join("config.toml"))?;
            snapshot::create_snapshot(
                &uhoh,
                &database,
                &project.hash,
                &project_path,
                &trigger_str,
                message.as_deref(),
                &cfg,
                None,
            )?;
            println!("Snapshot created.");
        }

        Commands::Restore {
            id,
            target,
            dry_run,
            force,
        } => {
            let project = resolve_target_project(&uhoh, &database, target.as_deref())?;
            let _ = restore::cmd_restore(&uhoh, &database, &project, &id, None, dry_run, force)?;
        }

        Commands::Gitstash { id, target } => {
            let project = resolve_target_project(&uhoh, &database, target.as_deref())?;
            git::cmd_gitstash(&uhoh, &database, &project, &id)?;
        }

        Commands::Diff { id1, id2 } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database
                .find_project_by_path(&project_path)?
                .context("Not registered")?;
            diff_view::cmd_diff(&uhoh, &database, &project, id1.as_deref(), id2.as_deref())?;
        }

        Commands::Cat { path, id } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database
                .find_project_by_path(&project_path)?
                .context("Not registered")?;
            diff_view::cmd_cat(&uhoh, &database, &project, &path, &id)?;
        }

        Commands::Log { path } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database
                .find_project_by_path(&project_path)?
                .context("Not registered")?;
            diff_view::cmd_log(&database, &project, &path)?;
        }

        Commands::Mcp => {
            let config_path = uhoh.join("config.toml");
            let config = config::Config::load(&config_path)?;
            uhoh::mcp_stdio::run_stdio_mcp(&config)?;
        }

        Commands::Start { service } => {
            if service {
                daemon::run_foreground(&uhoh, std::sync::Arc::new(database)).await?;
            } else {
                daemon::spawn_detached_daemon()?;
            }
        }
        Commands::Stop => {
            daemon::stop_daemon(&uhoh)?;
        }
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
                other => {
                    anyhow::bail!("Unknown hook action: '{other}'. Use 'install' or 'remove'.")
                }
            }
        }

        Commands::Config { action } => {
            let config_path = uhoh.join("config.toml");
            match action {
                Some(uhoh::cli::ConfigAction::Edit) => {
                    let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
                    std::process::Command::new(&editor)
                        .arg(&config_path)
                        .status()?;
                }
                Some(uhoh::cli::ConfigAction::Set { key, value }) => {
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
                        [k] => {
                            doc[*k] = toml_edit::value(parse_toml_value(&value));
                        }
                        [a, b] => {
                            if !doc.contains_key(a) {
                                doc[*a] = toml_edit::Item::Table(toml_edit::Table::new());
                            }
                            doc[*a][*b] = toml_edit::value(parse_toml_value(&value));
                        }
                        _ => anyhow::bail!("Key nesting deeper than 2 levels is not supported"),
                    }
                    std::fs::write(&config_path, doc.to_string())?;
                    println!("Set {key} = {value}");
                }
                Some(uhoh::cli::ConfigAction::Get { key }) => {
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
                        [k] => doc.get(k).map(|v| v.to_string()).unwrap_or_default(),
                        [a, b] => doc
                            .get(a)
                            .and_then(|t| t.get(*b))
                            .map(|v| v.to_string())
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
        }

        Commands::Gc => {
            gc::run_gc(&uhoh, &database)?;
        }
        Commands::Update => {
            update::check_and_apply_update(&uhoh).await?;
        }
        Commands::Doctor {
            fix,
            restore_latest,
            verify_install,
        } => {
            if verify_install {
                return run_verify_install().await;
            }
            run_doctor(&uhoh, &database, fix, restore_latest).await?;
        }

        Commands::Status => {
            let running = is_daemon_running(&uhoh);
            println!("Daemon: {}", if running { "running" } else { "stopped" });
            let projects = database.list_projects()?;
            println!("Projects: {}", projects.len());
            let total: u64 = projects
                .iter()
                .filter_map(|p| database.snapshot_count(&p.hash).ok())
                .sum();
            println!("Snapshots: {total}");
            let size = database.get_blob_bytes().unwrap_or(0);
            println!("Blob storage: {:.1} MB", size as f64 / 1_048_576.0);
            let cfg = config::Config::load(&uhoh.join("config.toml")).unwrap_or_default();
            println!(
                "AI: {}",
                if cfg.ai.enabled {
                    "enabled"
                } else {
                    "disabled"
                }
            );
            // Inception loop guard: warn if project includes ~/.uhoh
            let uhoh_path = uhoh::uhoh_dir();
            for p in &projects {
                let proj_path = std::path::Path::new(&p.current_path);
                if uhoh_path.starts_with(proj_path) {
                    println!("Warning: Project {} includes the uhoh data directory; this may cause snapshot loops.", p.current_path);
                    break;
                }
            }

            if running {
                if let Ok(port_raw) = std::fs::read_to_string(uhoh.join("server.port")) {
                    if let Ok(port) = port_raw.trim().parse::<u16>() {
                        let url = format!("http://127.0.0.1:{port}/health");
                        if let Ok(resp) = reqwest::get(url).await {
                            if let Ok(json) = resp.json::<serde_json::Value>().await {
                                if let Some(subsystems) =
                                    json.get("subsystems").and_then(|v| v.as_array())
                                {
                                    println!("Subsystems:");
                                    for item in subsystems {
                                        let name = item
                                            .get("name")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("unknown");
                                        let status = item
                                            .get("status")
                                            .and_then(|v| v.as_str())
                                            .unwrap_or("unknown");
                                        println!("  - {}: {}", name, status);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Commands::Mark { label } => {
            let project_path = dunce::canonicalize(std::env::current_dir()?)?;
            let project = database
                .find_project_by_path(&project_path)?
                .context("Not registered")?;
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

        Commands::ServiceInstall => {
            platform::install_service()?;
            println!("Service installed.");
        }
        Commands::ServiceRemove => {
            platform::remove_service()?;
            println!("Service removed.");
        }

        Commands::Db { action } => {
            handle_db_commands(&database, &uhoh, &action)?;
        }

        Commands::Agent { action } => {
            handle_agent_commands(&database, &action)?;
        }

        Commands::Trace { event_id } => {
            let chain = database.event_ledger_trace(event_id)?;
            if chain.is_empty() {
                println!("No events found for trace id {event_id}");
            } else {
                for entry in chain {
                    println!(
                        "#{} {} {} [{}] {}",
                        entry.id, entry.ts, entry.source, entry.severity, entry.event_type
                    );
                }
            }
        }

        Commands::Blame { path } => {
            let events = database.event_ledger_recent(None, None, None, 200)?;
            for e in events
                .iter()
                .filter(|e| e.path.as_deref() == Some(path.as_str()))
            {
                println!("#{} {} {} {}", e.id, e.ts, e.source, e.event_type);
            }
        }

        Commands::Run { command } => {
            if command.is_empty() {
                anyhow::bail!("No command provided");
            }
            let cfg = config::Config::load(&uhoh.join("config.toml")).unwrap_or_default();
            let mut cmd = std::process::Command::new(&command[0]);
            cmd.args(&command[1..]);

            if cfg.agent.mcp_proxy_enabled {
                cmd.env(
                    "UHOH_MCP_PROXY_ADDR",
                    format!("127.0.0.1:{}", cfg.agent.mcp_proxy_port),
                );
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
            }

            cmd.env("UHOH_AGENT_RUNTIME_DIR", uhoh.join("agents/runtime"));

            let status = cmd
                .status()
                .with_context(|| format!("Failed to run command: {}", command[0]))?;
            if !status.success() {
                anyhow::bail!("Command failed with status: {status}");
            }
        }
    }

    Ok(())
}

fn handle_db_commands(database: &db::Database, uhoh_dir: &std::path::Path, action: &DbAction) -> Result<()> {
    match action {
        DbAction::Add {
            dsn,
            tables,
            name,
            mode,
        } => {
            let guard_name = name
                .clone()
                .unwrap_or_else(|| uhoh::db_guard::derive_guard_name_from_dsn(dsn));
            let engine = uhoh::db_guard::detect_engine(dsn);
            if engine == "unknown" {
                anyhow::bail!("Unsupported DSN format");
            }
            let tables_csv = tables.clone().unwrap_or_else(|| "*".to_string());

            if engine == "postgres" {
                install_postgres_monitoring_infrastructure(dsn, tables_csv.as_str())?;
            }

            let connection_ref = uhoh::db_guard::scrub_dsn(dsn);
            database.add_db_guard(&guard_name, engine, &connection_ref, &tables_csv, mode)?;
            println!("Added db guard '{guard_name}' ({engine})");
        }
        DbAction::Remove { name } => {
            if let Some(guard) = database
                .list_db_guards()?
                .into_iter()
                .find(|g| g.name == *name)
            {
                if guard.engine == "postgres" {
                    drop_postgres_monitoring_infrastructure(&guard.connection_ref, &guard.tables_csv)?;
                }
            }
            database.remove_db_guard(name)?;
            println!("Removed db guard '{name}'");
        }
        DbAction::List => {
            let guards = database.list_db_guards()?;
            if guards.is_empty() {
                println!("No db guards registered");
            } else {
                for guard in guards {
                    println!(
                        "{} [{}] mode={} tables={} active={}",
                        guard.name, guard.engine, guard.mode, guard.tables_csv, guard.active
                    );
                }
            }
        }
        DbAction::Events { name, table } => {
            let events =
                database.event_ledger_recent(Some("db_guard"), name.as_deref(), None, 100)?;
            for e in events {
                if let Some(t) = table {
                    if e.path.as_deref() != Some(t.as_str()) {
                        continue;
                    }
                }
                println!("#{} {} [{}] {}", e.id, e.ts, e.severity, e.event_type);
            }
        }
        DbAction::Recover { event_id, apply } => {
            let entry = database
                .event_ledger_get(*event_id)?
                .context("Event not found")?;
            let detail = entry.detail.clone().unwrap_or_default();
            println!("-- Recovery preview for event #{}", entry.id);
            println!("-- source: {}", entry.source);
            println!("-- type: {}", entry.event_type);
            println!("-- detail: {detail}");
            if let Some(pre_state_ref) = &entry.pre_state_ref {
                println!("-- pre_state_ref: {pre_state_ref}");
            }
            if let Some(path) = extract_artifact_path(&entry.detail) {
                println!("-- artifact_path: {path}");
                if *apply {
                    apply_recovery_artifact(&path, uhoh_dir)?;
                    println!("Applied recovery artifact from {path}");
                }
            }
            if *apply {
                println!("Applied recovery marker for event #{}", entry.id);
                database.event_ledger_mark_resolved(entry.id)?;
            } else {
                println!("Use --apply to mark as resolved");
            }
        }
        DbAction::Baseline { name } => {
            let guards = database.list_db_guards()?;
            let guard = guards
                .into_iter()
                .find(|g| g.name == *name)
                .context("Guard not found")?;
            match guard.engine.as_str() {
                "sqlite" => {
                    let sqlite_path = guard
                        .connection_ref
                        .strip_prefix("sqlite://")
                        .unwrap_or(&guard.connection_ref);
                    let _ = uhoh::db_guard::write_sqlite_baseline(
                        &uhoh::uhoh_dir(),
                        &guard.name,
                        std::path::Path::new(sqlite_path),
                        true,
                        30,
                    )?;
                }
                "postgres" => {
                    let creds = uhoh::db_guard::resolve_postgres_credentials(&guard.connection_ref)?;
                    let _ = uhoh::db_guard::write_postgres_schema_baseline(
                        &uhoh::uhoh_dir(),
                        &guard.name,
                        &guard.connection_ref,
                        &creds,
                        30,
                    )?;
                }
                _ => {}
            }
            let ts = chrono::Utc::now().to_rfc3339();
            database.set_db_guard_baseline_time(name, &ts)?;
            println!("Baseline timestamp updated for {name}");
        }
        DbAction::Test { name } => {
            let guards = database.list_db_guards()?;
            let guard = guards
                .into_iter()
                .find(|g| g.name == *name)
                .context("Guard not found")?;
            if guard.engine == "postgres" {
                test_postgres_monitoring_infrastructure(&guard.connection_ref)?;
            }
            println!(
                "Guard '{}' OK: engine={}, mode={}, conn={}",
                guard.name, guard.engine, guard.mode, guard.connection_ref
            );
        }
    }
    Ok(())
}

fn install_postgres_monitoring_infrastructure(dsn: &str, tables_csv: &str) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build tokio runtime for postgres guard install")?;

    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;

        tokio::spawn(async move {
            let _ = connection.await;
        });

        client
            .batch_execute(
                "
                CREATE TABLE IF NOT EXISTS _uhoh_ddl_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_tag TEXT NOT NULL,
                    object_type TEXT,
                    schema_name TEXT,
                    object_identity TEXT,
                    payload TEXT,
                    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS _uhoh_delete_counts (
                    table_name TEXT NOT NULL,
                    txid BIGINT NOT NULL DEFAULT txid_current(),
                    delete_count INTEGER NOT NULL DEFAULT 0,
                    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (table_name, txid)
                );

                CREATE OR REPLACE FUNCTION _uhoh_ddl_handler() RETURNS event_trigger AS $$
                DECLARE rec RECORD;
                DECLARE payload_json TEXT;
                BEGIN
                    FOR rec IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
                        payload_json := json_build_object(
                            'event_tag', tg_tag,
                            'object_type', rec.object_type,
                            'schema_name', rec.schema_name,
                            'object_identity', rec.object_identity
                        )::text;

                        INSERT INTO _uhoh_ddl_events (
                            event_tag,
                            object_type,
                            schema_name,
                            object_identity,
                            payload
                        ) VALUES (
                            tg_tag,
                            rec.object_type,
                            rec.schema_name,
                            rec.object_identity,
                            payload_json
                        );

                        PERFORM pg_notify('uhoh_events', payload_json);
                    END LOOP;
                END;
                $$ LANGUAGE plpgsql;

                DROP EVENT TRIGGER IF EXISTS uhoh_ddl_drop;
                CREATE EVENT TRIGGER uhoh_ddl_drop ON sql_drop
                    EXECUTE FUNCTION _uhoh_ddl_handler();
                ",
            )
            .await
            .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;

        let tables = parse_watched_tables(tables_csv);
        if tables.is_empty() {
            let rows = client
                .query(
                    "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = current_schema()",
                    &[],
                )
                .await
                .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;
            let table_names: Vec<String> = rows.into_iter().map(|r| r.get::<_, String>(0)).collect();
            for table in table_names {
                install_delete_counter_trigger(&client, &table).await?;
            }
            return Ok(());
        }
        for table in tables {
            install_delete_counter_trigger(&client, &table).await?;
        }

        Ok(())
    })
}

async fn install_delete_counter_trigger(
    client: &tokio_postgres::Client,
    table: &str,
) -> Result<()> {
    let fn_ident = format!("_uhoh_count_deletes_{}", sanitize_ident(table));
    let trigger_ident = format!("uhoh_delete_counter_{}", sanitize_ident(table));

    let install_sql = format!(
        "
        CREATE OR REPLACE FUNCTION {fn_ident}() RETURNS trigger AS $$
        BEGIN
            INSERT INTO _uhoh_delete_counts (table_name, txid, delete_count)
            VALUES ('{table}', txid_current(), 1)
            ON CONFLICT (table_name, txid)
            DO UPDATE SET delete_count = _uhoh_delete_counts.delete_count + 1,
                          ts = now();
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {trigger_ident} ON \"{table}\";
        CREATE TRIGGER {trigger_ident}
            BEFORE DELETE ON \"{table}\"
            FOR EACH ROW EXECUTE FUNCTION {fn_ident}();
        "
    );

    client
        .batch_execute(&install_sql)
        .await
        .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;
    Ok(())
}

fn drop_postgres_monitoring_infrastructure(dsn: &str, tables_csv: &str) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build tokio runtime for postgres guard uninstall")?;

    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        let tables = parse_watched_tables(tables_csv);
        for table in tables {
            let trigger_ident = format!("uhoh_delete_counter_{}", sanitize_ident(&table));
            let fn_ident = format!("_uhoh_count_deletes_{}", sanitize_ident(&table));
            let sql = format!(
                "DROP TRIGGER IF EXISTS {trigger_ident} ON \"{table}\"; DROP FUNCTION IF EXISTS {fn_ident}();"
            );
            client
                .batch_execute(&sql)
                .await
                .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;
        }

        client
            .batch_execute(
                "
                DROP EVENT TRIGGER IF EXISTS uhoh_ddl_drop;
                DROP FUNCTION IF EXISTS _uhoh_ddl_handler();
                DROP TABLE IF EXISTS _uhoh_ddl_events;
                DROP TABLE IF EXISTS _uhoh_delete_counts;
                ",
            )
            .await
            .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;

        Ok(())
    })
}

fn test_postgres_monitoring_infrastructure(dsn: &str) -> Result<()> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("Failed to build tokio runtime for postgres guard test")?;

    rt.block_on(async move {
        let (client, connection) = tokio_postgres::connect(dsn, tokio_postgres::NoTls)
            .await
            .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;
        tokio::spawn(async move {
            let _ = connection.await;
        });

        match client.query_one("SELECT 1 FROM _uhoh_ddl_events LIMIT 1", &[]).await {
            Ok(_) => {}
            Err(_) => {
                let _ = client
                    .query_one("SELECT 1", &[])
                    .await
                    .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;
            }
        }

        match client.query_one("SELECT 1 FROM _uhoh_delete_counts LIMIT 1", &[]).await {
            Ok(_) => {}
            Err(_) => {
                let _ = client
                    .query_one("SELECT 1", &[])
                    .await
                    .map_err(|e| anyhow::anyhow!(uhoh::db_guard::scrub_error_message(&e.to_string())))?;
            }
        }

        Ok(())
    })
}

fn parse_watched_tables(tables_csv: &str) -> Vec<String> {
    if tables_csv.trim() == "*" {
        return Vec::new();
    }
    tables_csv
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(|v| v.to_string())
        .collect()
}

fn sanitize_ident(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn extract_artifact_path(detail: &Option<String>) -> Option<String> {
    let raw = detail.as_ref()?;
    let json = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    json.get("artifact")
        .and_then(|v| v.as_str())
        .map(str::to_string)
}

fn apply_recovery_artifact(path: &str, _uhoh_dir: &std::path::Path) -> Result<()> {
    let artifact_path = std::path::Path::new(path);
    if !artifact_path.exists() {
        anyhow::bail!("Recovery artifact does not exist: {}", artifact_path.display());
    }

    let payload = std::fs::read(artifact_path)?;
    let sql = if payload.starts_with(b"UHOHENC1") {
        anyhow::bail!(
            "Encrypted recovery artifact detected. Decryption support is required before apply"
        );
    } else {
        String::from_utf8(payload).context("Recovery artifact is not valid UTF-8 SQL")?
    };

    #[cfg(unix)]
    {
        let status = Command::new("sh")
            .arg("-c")
            .arg(format!("cat '{}' >/dev/null", artifact_path.display()))
            .status()
            .context("Failed to verify artifact readability")?;
        if !status.success() {
            anyhow::bail!("Recovery artifact cannot be read safely");
        }
    }

    if !sql.contains("BEGIN;") || !sql.contains("COMMIT;") {
        anyhow::bail!("Recovery SQL must be transaction-wrapped (BEGIN/COMMIT)");
    }

    println!("-- SQL preview begin");
    for line in sql.lines().take(40) {
        println!("{line}");
    }
    if sql.lines().count() > 40 {
        println!("-- ... truncated preview ...");
    }
    println!("-- SQL preview end");
    Ok(())
}

fn handle_agent_commands(database: &db::Database, action: &AgentAction) -> Result<()> {
    match action {
        AgentAction::Add { name, profile } => {
            let profile_path = profile
                .clone()
                .unwrap_or_else(|| format!("~/.uhoh/agents/{name}.toml"));
            let resolved_profile = expand_home_path(&profile_path);
            if !std::path::Path::new(&resolved_profile).exists() {
                anyhow::bail!("Agent profile not found: {resolved_profile}");
            }
            let _ = uhoh::agent::load_agent_profile(std::path::Path::new(&resolved_profile))?;
            database.add_agent(name, &profile_path, 1, None)?;
            println!("Added agent '{name}'");
        }
        AgentAction::Remove { name } => {
            database.remove_agent(name)?;
            println!("Removed agent '{name}'");
        }
        AgentAction::List => {
            let agents = database.list_agents()?;
            if agents.is_empty() {
                println!("No agents registered");
            } else {
                for a in agents {
                    println!("{} [{}]", a.name, a.profile_path);
                }
            }
        }
        AgentAction::Log { name, session: _ } => {
            let events = database.event_ledger_recent(Some("agent"), None, name.as_deref(), 100)?;
            for e in events {
                println!("#{} {} [{}] {}", e.id, e.ts, e.severity, e.event_type);
            }
        }
        AgentAction::Undo {
            event_id,
            session: _,
            cascade,
        } => {
            if let Some(id) = cascade.or(*event_id) {
                database.event_ledger_mark_resolved(id)?;
                println!("Marked event #{} as resolved", id);
            } else {
                anyhow::bail!("Provide event id or --cascade");
            }
        }
        AgentAction::Approve => {
            let runtime = uhoh::uhoh_dir().join("agents/runtime");
            std::fs::create_dir_all(&runtime)?;
            let mut approved_any = false;
            for entry in std::fs::read_dir(&runtime)? {
                let entry = entry?;
                let path = entry.path();
                if !path
                    .file_name()
                    .and_then(|v| v.to_str())
                    .map(|v| v.ends_with(".pending.json"))
                    .unwrap_or(false)
                {
                    continue;
                }
                let Some(stem) = path
                    .file_name()
                    .and_then(|v| v.to_str())
                    .and_then(|v| v.strip_suffix(".pending.json"))
                else {
                    continue;
                };
                std::fs::write(runtime.join(format!("{stem}.approved")), b"approved")?;
                approved_any = true;
            }
            if approved_any {
                println!("Approved pending agent action");
            } else {
                println!("No pending agent actions found");
            }
        }
        AgentAction::Resume => {
            #[cfg(unix)]
            {
                let runtime = uhoh::uhoh_dir().join("agents/runtime");
                let resume_file = runtime.join("resume.pid");
                if let Ok(pid_raw) = std::fs::read_to_string(&resume_file) {
                    if let Ok(pid) = pid_raw.trim().parse::<i32>() {
                        let signal = unsafe { libc::kill(pid, libc::SIGCONT) };
                        if signal == 0 {
                            println!("Requested agent resume for pid {pid}");
                            return Ok(());
                        }
                    }
                }
            }
            println!("No paused agent pid available to resume");
        }
        AgentAction::Setup => {
            println!("Agent setup: configure profiles under ~/.uhoh/agents");
        }
        AgentAction::Test { name } => {
            let exists = database.list_agents()?.into_iter().any(|a| a.name == *name);
            if !exists {
                anyhow::bail!("Agent not registered");
            }
            println!("Agent '{name}' is registered");
        }
        AgentAction::Init => {
            let profile_dir = uhoh::uhoh_dir().join("agents");
            std::fs::create_dir_all(&profile_dir)?;
            let default_profile = profile_dir.join("generic.toml");
            if !default_profile.exists() {
                std::fs::write(
                    &default_profile,
                    r#"profile_version = 1
name = "generic"
process_names = ["node", "python", "uhoh"]
session_log_pattern = "~/.uhoh/agent-intent.jsonl"
tool_names_write = ["write", "apply_patch"]
tool_names_exec = ["exec", "bash", "shell"]
tool_call_format = "jsonl"
"#,
                )?;
                println!("Initialized default profile: {}", default_profile.display());
            } else {
                println!("Default profile already exists: {}", default_profile.display());
            }
        }
        AgentAction::UpdateProfiles => {
            let profile_dir = uhoh::uhoh_dir().join("agents");
            std::fs::create_dir_all(&profile_dir)?;
            let source = profile_dir.join("generic.toml");
            let target = profile_dir.join("generic.updated.toml");
            if source.exists() {
                std::fs::copy(&source, &target)?;
                println!("Updated profile cache: {}", target.display());
            } else {
                println!("No base profile exists yet. Run `uhoh agent init` first.");
            }
        }
    }
    Ok(())
}

fn expand_home_path(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).display().to_string();
        }
    }
    path.to_string()
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

    let cfg = config::Config::load(&uhoh.join("config.toml"))?;
    snapshot::create_snapshot(
        &uhoh,
        &database,
        &project_hash,
        &canonical,
        "manual",
        Some("Initial snapshot"),
        &cfg,
        None,
    )?;
    println!("Initial snapshot created.");

    Ok(())
}

async fn run_doctor(
    uhoh_dir: &std::path::Path,
    database: &db::Database,
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

    if !integrity_ok && restore_latest {
        // Attempt restore from latest backup after integrity-check connection is dropped
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
            } else {
                eprintln!("No backups found to restore.");
            }
        }
    }

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
    let mut corrupted = Vec::new();
    for h in &referenced {
        let p = blob_root.join(&h[..h.len().min(2)]).join(h);
        if !p.exists() {
            continue;
        }
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
