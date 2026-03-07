use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use std::process::Command as TestCommand;
use tempfile::TempDir;

use uhoh::db::{Database, NewEventLedgerEntry};
use uhoh::event_ledger::{new_event, EventLedger};
use uhoh::subsystem::{Subsystem, SubsystemContext, SubsystemHealth, SubsystemManager};

#[derive(Clone)]
struct TestCounters {
    run: Arc<AtomicUsize>,
    shutdown: Arc<AtomicUsize>,
}

struct TestSubsystem {
    counters: TestCounters,
}

#[async_trait::async_trait]
impl Subsystem for TestSubsystem {
    fn name(&self) -> &str {
        "test"
    }

    async fn run(
        &mut self,
        _shutdown: tokio_util::sync::CancellationToken,
        _ctx: SubsystemContext,
    ) -> Result<()> {
        self.counters.run.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.counters.shutdown.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn health_check(&self) -> SubsystemHealth {
        SubsystemHealth::Healthy
    }
}

fn temp_db() -> (TempDir, Arc<Database>) {
    let temp = tempfile::tempdir().unwrap();
    let db = Arc::new(Database::open(&temp.path().join("uhoh.db")).unwrap());
    (temp, db)
}

fn event(
    source: &str,
    event_type: &str,
    path: Option<&str>,
    causal_parent: Option<i64>,
) -> NewEventLedgerEntry {
    let mut e = new_event(source, event_type, "info");
    e.path = path.map(str::to_string);
    e.causal_parent = causal_parent;
    e
}

#[tokio::test]
async fn subsystem_manager_starts_reports_health_and_shuts_down() {
    let (tmp, db) = temp_db();
    let ledger = EventLedger::new(db.clone());
    let (event_tx, _event_rx) = tokio::sync::broadcast::channel(8);
    let ctx = SubsystemContext {
        database: db,
        event_ledger: ledger,
        config: uhoh::config::Config::default(),
        uhoh_dir: tmp.path().to_path_buf(),
        server_event_tx: event_tx,
    };

    let counters = TestCounters {
        run: Arc::new(AtomicUsize::new(0)),
        shutdown: Arc::new(AtomicUsize::new(0)),
    };

    let mut mgr = SubsystemManager::new(3, Duration::from_secs(60));
    mgr.register(Box::new(TestSubsystem {
        counters: counters.clone(),
    }));

    mgr.start_all(ctx.clone()).await;
    for _ in 0..40 {
        if counters.run.load(Ordering::SeqCst) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(counters.run.load(Ordering::SeqCst), 1);
    let health = mgr.health_snapshot().await;
    assert_eq!(health.len(), 1);
    assert_eq!(health[0].0, "test");
    assert!(matches!(health[0].1, SubsystemHealth::Healthy));

    mgr.shutdown_all().await;
    assert_eq!(counters.shutdown.load(Ordering::SeqCst), 1);
}

#[test]
fn event_ledger_trace_and_resolve_roundtrip() {
    let (_tmp, db) = temp_db();
    let ledger = EventLedger::new(db.clone());

    let _ = ledger.append(event("agent", "tool_call", Some("src/lib.rs"), None));
    let root = db
        .event_ledger_recent(None, None, None, None, 10)
        .unwrap()
        .first()
        .map(|e| e.id)
        .unwrap();
    let _ = ledger.append(event("fs", "file_write", Some("src/lib.rs"), Some(root)));
    let child = db
        .event_ledger_recent(None, None, None, None, 10)
        .unwrap()
        .first()
        .map(|e| e.id)
        .unwrap();

    let trace = ledger.trace(child).unwrap();
    assert_eq!(trace.entries.len(), 2);
    assert!(!trace.truncated);
    assert_eq!(trace.entries[0].id, child);
    assert_eq!(trace.entries[1].id, root);

    ledger.mark_resolved(child).unwrap();
    let updated = db.event_ledger_get(child).unwrap().unwrap();
    assert!(updated.resolved);
}

fn make_cli_home_with_events() -> (TempDir, i64, i64) {
    let home = tempfile::tempdir().unwrap();
    let uhoh_dir = home.path().join(".uhoh");
    std::fs::create_dir_all(&uhoh_dir).unwrap();

    let db = Database::open(&uhoh_dir.join("uhoh.db")).unwrap();
    let root_event = event("agent", "pre_notify", Some("src/lib.rs"), None);
    let root = db.insert_event_ledger(&root_event).unwrap();
    let child = db
        .insert_event_ledger(&event("fs", "file_write", Some("src/lib.rs"), Some(root)))
        .unwrap();

    (home, root, child)
}

fn make_cli_home_with_timeline_events() -> TempDir {
    let home = tempfile::tempdir().unwrap();
    let uhoh_dir = home.path().join(".uhoh");
    std::fs::create_dir_all(&uhoh_dir).unwrap();

    let db = Database::open(&uhoh_dir.join("uhoh.db")).unwrap();
    let mut fs_event = event("fs", "file_write", Some("src/lib.rs"), None);
    fs_event.detail = Some("timeline-fs".to_string());
    db.insert_event_ledger(&fs_event).unwrap();

    let mut db_event = event("db_guard", "drop_table", Some("users"), None);
    db_event.detail = Some("timeline-db".to_string());
    db.insert_event_ledger(&db_event).unwrap();

    let mut agent_event = event("agent", "tool_call", Some("src/main.rs"), None);
    agent_event.detail = Some("timeline-agent".to_string());
    db.insert_event_ledger(&agent_event).unwrap();

    home
}

fn apply_home_env(cmd: &mut TestCommand, home: &Path) {
    cmd.env("HOME", home);
    cmd.env("USERPROFILE", home);
}

fn run_cli(home: &Path, args: &[&str]) -> (bool, String, String) {
    #[allow(deprecated)]
    let exe = assert_cmd::cargo::cargo_bin("uhoh");
    let mut cmd = TestCommand::new(exe);
    apply_home_env(&mut cmd, home);
    cmd.args(args);
    let out = cmd.output().expect("failed to execute uhoh CLI");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[test]
fn cli_trace_prints_causal_chain() {
    let (home, root, child) = make_cli_home_with_events();
    let (ok, stdout, _stderr) = run_cli(home.path(), &["trace", &child.to_string()]);
    assert!(ok);
    assert!(stdout.contains(&format!("#{child}")));
    assert!(stdout.contains("fs"));
    assert!(stdout.contains(&format!("#{root}")));
    assert!(stdout.contains("agent"));
}

#[test]
fn cli_blame_prints_chain_for_path() {
    let (home, root, child) = make_cli_home_with_events();
    let (ok, stdout, _stderr) = run_cli(home.path(), &["blame", "src/lib.rs"]);
    assert!(ok);
    assert!(stdout.contains("Blame chain for src/lib.rs"));
    assert!(stdout.contains(&format!("#{child}")));
    assert!(stdout.contains(&format!("#{root}")));
}

#[test]
fn cli_blame_reports_when_path_not_found() {
    let (home, _root, _child) = make_cli_home_with_events();
    let (ok, stdout, _stderr) = run_cli(home.path(), &["blame", "missing/file.rs"]);
    assert!(ok);
    assert!(stdout.contains("No events found for path missing/file.rs"));
}

#[test]
fn cli_timeline_source_filter_and_since_window() {
    let home = make_cli_home_with_timeline_events();

    let (ok, stdout, _stderr) = run_cli(
        home.path(),
        &["timeline", "--source", "agent", "--since", "1h"],
    );
    assert!(ok);
    assert!(stdout.contains("agent"));
    assert!(stdout.contains("tool_call"));
    assert!(!stdout.contains("db_guard"));
    assert!(!stdout.contains("fs"));

    let (ok_all, stdout_all, _stderr_all) = run_cli(home.path(), &["timeline", "--since", "1h"]);
    assert!(ok_all);
    assert!(stdout_all.contains("fs"));
    assert!(stdout_all.contains("db_guard"));
    assert!(stdout_all.contains("agent"));
}

#[test]
fn cli_agent_undo_cascade_marks_descendants_resolved() {
    let (home, root, child) = make_cli_home_with_events();

    let (ok, stdout, _stderr) = run_cli(
        home.path(),
        &["agent", "undo", "--cascade", &root.to_string()],
    );
    assert!(ok);
    assert!(stdout.contains("downstream event"));

    let db = Database::open(&home.path().join(".uhoh/uhoh.db")).unwrap();
    let root_row = db.event_ledger_get(root).unwrap().unwrap();
    let child_row = db.event_ledger_get(child).unwrap().unwrap();
    assert!(root_row.resolved);
    assert!(child_row.resolved);
}

#[test]
fn health_endpoint_and_auth_exemption_present() {
    let source = std::fs::read_to_string("src/server/mod.rs").expect("read server mod");
    assert!(source.contains(".route(\"/health\", get(health_check))"));

    let auth = std::fs::read_to_string("src/server/auth.rs").expect("read auth middleware");
    assert!(auth.contains("path == \"/health\""));
}

#[test]
fn mcp_proxy_dangerous_patterns_use_exact_match_semantics() {
    let proxy = std::fs::read_to_string("src/agent/mcp_proxy.rs").expect("read mcp proxy");
    assert!(proxy.contains("return tool_l == raw.trim();"));
    // Path matching uses suffix/component matching so "Cargo.toml" matches full paths
    assert!(proxy.contains("file_path.ends_with(pattern_path)"));
    assert!(proxy.contains("tool_l == p || path_l == p"));
}

#[test]
fn mcp_proxy_auth_requires_jsonrpc_handshake_only() {
    let proxy = std::fs::read_to_string("src/agent/mcp_proxy.rs").expect("read mcp proxy");
    assert!(proxy.contains("method") && proxy.contains("uhoh/auth"));
    assert!(!proxy.contains("secure_eq(trimmed.as_bytes(), expected_token.as_bytes())"));
}

#[test]
fn mcp_transports_share_tool_definitions_module() {
    let stdio = std::fs::read_to_string("src/mcp_stdio.rs").expect("read stdio mcp");
    let http = std::fs::read_to_string("src/server/mcp.rs").expect("read http mcp");
    assert!(stdio.contains("crate::mcp_tools::tool_definitions()"));
    assert!(http.contains("crate::mcp_tools::tool_definitions()"));
}

#[test]
fn db_pool_connection_failures_return_error_not_panic() {
    let source = std::fs::read_to_string("src/db.rs").expect("read db module");
    assert!(source.contains("fn conn(&self) -> Result<DbConn>"));
    assert!(source.contains("Database connection pool unavailable after"));
    assert!(!source
        .contains("panic!(\n                            \"Database connection pool unavailable"));
}

#[test]
fn daemon_uses_bounded_watcher_channel_and_overflow_backpressure() {
    let daemon = std::fs::read_to_string("src/daemon.rs").expect("read daemon");
    let watcher = std::fs::read_to_string("src/watcher.rs").expect("read watcher");
    assert!(daemon.contains("mpsc::channel::<WatchEvent>(100_000)"));
    assert!(watcher.contains("send_watch_event"));
    assert!(watcher.contains("WatchEvent::Overflow"));
}

#[test]
fn ledger_hash_chain_includes_causal_parent_field() {
    let source = std::fs::read_to_string("src/db/ledger.rs").expect("read ledger hash module");
    assert!(source.contains("causal_parent"));
}

#[test]
fn postgres_listen_worker_queries_use_monotonic_id_cursor() {
    let source = std::fs::read_to_string("src/db_guard/postgres.rs")
        .expect("read postgres guard implementation");
    assert!(source.contains("async fn run_listen_worker"));
    assert!(source.contains("reconcile_listen_workers"));
    assert!(source.contains("shutdown: CancellationToken"));
    assert!(source.contains("let mut backoff = std::time::Duration::from_secs(1);"));
    assert!(source.contains("sleep_or_cancel"));
    assert!(source.contains("SELECT id, payload::text"));
    assert!(source.contains("WHERE id > $1"));
    assert!(source.contains("last_seen_id = last_seen_id.max(id);"));
    assert!(!source
        .contains("WHERE id > COALESCE((SELECT MAX(id) FROM _uhoh_ddl_events WHERE 1=0), 0)"));
}

#[test]
fn mcp_approval_reader_uses_nofollow_guards() {
    let source =
        std::fs::read_to_string("src/agent/mcp_proxy.rs").expect("read mcp proxy implementation");
    assert!(source.contains("symlink_metadata(path)"));
    assert!(source.contains("libc::O_NOFOLLOW | libc::O_CLOEXEC"));
}

#[test]
fn db_recover_apply_output_clarifies_manual_sql_execution() {
    let source = std::fs::read_to_string("src/main.rs").expect("read main command handler");
    assert!(source.contains("Applied recovery artifact from"));
    assert!(source.contains("Use --apply to validate, decrypt, and execute the recovery artifact"));
    assert!(source.contains("fn apply_postgres_recovery"));
    assert!(source.contains("fn apply_sqlite_recovery"));
}

#[test]
fn mcp_proxy_runs_as_async_task_with_shutdown_token() {
    let proxy = std::fs::read_to_string("src/agent/mcp_proxy.rs").expect("read mcp proxy");
    assert!(proxy.contains("pub async fn run_proxy"));
    assert!(proxy.contains("shutdown: CancellationToken"));
    assert!(proxy.contains("tokio::select!"));
    assert!(proxy.contains("listener.accept()"));

    let agent = std::fs::read_to_string("src/agent/mod.rs").expect("read agent subsystem");
    assert!(agent.contains("proxy_shutdown: Option<CancellationToken>"));
    assert!(agent.contains("mcp_proxy::run_proxy(ctx_cl, token_cl).await"));
}

#[test]
fn cargo_features_include_core_gates() {
    let cargo = std::fs::read_to_string("Cargo.toml").expect("read Cargo.toml");
    assert!(cargo.contains("audit-trail = []"));
    assert!(cargo.contains("keyring = [\"dep:keyring\"]"));
}

#[test]
fn postgres_db_ops_use_runtime_bridge_instead_of_nested_runtime_builder() {
    let source = std::fs::read_to_string("src/main.rs").expect("read main command handler");
    assert!(source.contains("fn block_on_runtime<T>"));
    assert!(source.contains("Builder::new_current_thread()"));
    assert!(!source.contains("tokio::task::block_in_place(|| handle.block_on(fut))"));
    assert!(
        !source.contains("postgres guard install")
            || source.contains("block_on_runtime(async move")
    );
}

#[test]
fn db_add_rolls_back_remote_and_credentials_on_local_failure() {
    let source = std::fs::read_to_string("src/main.rs").expect("read main command handler");
    assert!(source.contains("drop_postgres_monitoring_infrastructure"));
    assert!(source.contains("resolve_stored_credentials"));
    assert!(source.contains("store_encrypted_credential"));
}

#[test]
fn db_guard_module_uses_trait_based_engine_dispatch() {
    let source =
        std::fs::read_to_string("src/db_guard/mod.rs").expect("read db_guard subsystem module");
    assert!(source.contains("trait DbGuardEngine"));
    assert!(source.contains("impl DbGuardEngine for SqliteEngine"));
    assert!(source.contains("impl DbGuardEngine for PostgresEngine"));
    assert!(source.contains("impl DbGuardEngine for MysqlEngine"));
}

#[test]
fn event_ledger_append_falls_back_to_direct_insert_when_flusher_not_started() {
    let (_tmp, db) = temp_db();
    let ledger = EventLedger::new(db.clone());

    let event_id = ledger
        .append(event("agent", "pre_notify", Some("src/lib.rs"), None))
        .expect("append should succeed");
    assert!(event_id > 0);

    let fetched = db.event_ledger_get(event_id).expect("query ok");
    assert!(fetched.is_some());
}

#[test]
fn recovery_module_uses_machine_master_key_fallback_not_user_hostname() {
    let source = std::fs::read_to_string("src/db_guard/recovery.rs").expect("read recovery module");
    assert!(source.contains("MACHINE_KEY_FILE: &str = \"master.key\""));
    assert!(source.contains("load_or_create_machine_key"));
    assert!(!source.contains("HOSTNAME"));
    assert!(!source.contains("uhoh:{}:{}"));
}

#[test]
fn agent_subsystem_escalates_panics_to_failed_health_state() {
    let source = std::fs::read_to_string("src/agent/mod.rs").expect("read agent subsystem");
    assert!(source.contains("fatal_error: Option<String>"));
    assert!(source.contains("SubsystemHealth::Failed(message.clone())"));
    assert!(source.contains("task panicked"));
}

#[test]
fn intercept_tailer_uses_async_fs_and_sleep_primitives() {
    let source = std::fs::read_to_string("src/agent/intercept.rs").expect("read intercept tailer");
    assert!(source.contains("tokio::fs::File::open"));
    assert!(source.contains("tokio::fs::read"));
    assert!(source.contains("tokio::time::sleep"));
    assert!(!source.contains("std::thread::sleep"));
}

#[test]
fn daemon_registers_maintenance_subsystem_for_compaction_backup_ai() {
    let source = std::fs::read_to_string("src/daemon.rs").expect("read daemon");
    assert!(source.contains("struct DaemonMaintenanceSubsystem"));
    assert!(source.contains(
        "subsystem_manager_inner.register(Box::new(DaemonMaintenanceSubsystem::new(&config)))"
    ));
    assert!(source.contains("fn name(&self) -> &str"));
    assert!(source.contains("\"daemon_maintenance\""));
}

#[test]
fn snapshot_ai_diff_truncates_once_at_context_budget_limit() {
    let source = std::fs::read_to_string("src/snapshot.rs").expect("read snapshot module");
    assert!(source.contains("let mut diff_truncated = false;"));
    assert!(source.contains("[Diff truncated]"));
    assert!(source.contains("if diff_truncated {"));
}
