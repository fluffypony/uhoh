use std::path::Path;

use anyhow::{Context, Result};
use url::Url;
use zeroize::Zeroize;

use crate::cli::DbAction;
use crate::config;
use crate::db::{self, Database};

pub fn handle_db_guard_action(uhoh_dir: &Path, database: &Database, action: &DbAction) -> Result<()> {
    match action {
        DbAction::Add {
            dsn,
            tables,
            name,
            mode,
        } => {
            add_db_guard(database, dsn, tables.as_deref(), name.as_deref(), *mode)?;
        }
        DbAction::Remove { name } => {
            if let Some(guard) = database
                .list_db_guards()?
                .into_iter()
                .find(|guard| guard.name == *name)
            {
                if guard.engine == db::DbGuardEngine::Postgres {
                    let postgres_connection =
                        super::postgres_connection::ResolvedPostgresConnection::resolve(
                            &guard.connection_ref,
                        )?;
                    super::postgres_monitoring::drop_monitoring_infrastructure(
                        &postgres_connection,
                        &guard.tables_csv,
                    )?;
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
            let events = database.event_ledger_recent(
                db::LedgerRecentFilters {
                    source: Some(db::LedgerSource::DbGuard),
                    guard_name: name.as_deref(),
                    ..Default::default()
                },
                100,
            )?;
            for event in events {
                if let Some(table_name) = table {
                    if !event_matches_table_filter(&event, table_name) {
                        continue;
                    }
                }
                println!(
                    "#{} {} [{}] {}",
                    event.id, event.ts, event.severity, event.event_type
                );
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
            let artifact_path = extract_artifact_path(&entry.detail)
                .context("Recovery event detail missing artifact path")?;
            println!("-- artifact_path: {artifact_path}");
            if *apply {
                apply_recovery_artifact(database, &entry, &artifact_path, uhoh_dir)?;
                database.event_ledger_mark_resolved(entry.id)?;
                println!("Applied recovery artifact from {artifact_path}");
            }
            if !*apply {
                println!("Use --apply to validate, decrypt, and execute the recovery artifact");
            }
        }
        DbAction::Baseline { name } => {
            let guards = database.list_db_guards()?;
            let guard = guards
                .into_iter()
                .find(|guard| guard.name == *name)
                .context("Guard not found")?;
            match guard.engine {
                db::DbGuardEngine::Sqlite => {
                    let sqlite_path = guard
                        .connection_ref
                        .strip_prefix("sqlite://")
                        .unwrap_or(&guard.connection_ref);
                    let _ = super::recovery::write_sqlite_baseline(
                        uhoh_dir,
                        &guard.name,
                        Path::new(sqlite_path),
                        true,
                        30,
                        config::Config::load(&uhoh_dir.join("config.toml"))?
                            .db_guard
                            .max_baseline_size_mb,
                    )?;
                }
                db::DbGuardEngine::Postgres => {
                    let creds = super::credentials::resolve_postgres_credentials_with_keyring(
                        &guard.connection_ref,
                    )?;
                    if creds.keyring_status.is_degraded() {
                        eprintln!(
                            "Warning: using {} credentials for '{}'; keyring status is {}",
                            creds.source.label(),
                            guard.name,
                            creds.keyring_status.describe()
                        );
                    }
                    let postgres_connection =
                        super::postgres_connection::ResolvedPostgresConnection::resolve(
                            &guard.connection_ref,
                        )?;
                    let cfg = config::Config::load(&uhoh_dir.join("config.toml"))?;
                    let _ = super::recovery::write_postgres_schema_baseline(
                        uhoh_dir,
                        &guard.name,
                        &postgres_connection,
                        &creds.material,
                        30,
                        cfg.db_guard.max_baseline_size_mb,
                    )?;
                }
                db::DbGuardEngine::Mysql => {}
            }
            let ts = chrono::Utc::now().to_rfc3339();
            database.set_db_guard_baseline_time(name, &ts)?;
            println!("Baseline timestamp updated for {name}");
        }
        DbAction::Test { name } => {
            let guards = database.list_db_guards()?;
            let guard = guards
                .into_iter()
                .find(|guard| guard.name == *name)
                .context("Guard not found")?;
            if guard.engine == db::DbGuardEngine::Postgres {
                let postgres_connection =
                    super::postgres_connection::ResolvedPostgresConnection::resolve(
                        &guard.connection_ref,
                    )?;
                super::postgres_monitoring::test_monitoring_infrastructure(&postgres_connection)?;
            }
            println!(
                "Guard '{}' OK: engine={}, mode={}, conn={}",
                guard.name, guard.engine, guard.mode, guard.connection_ref
            );
        }
    }
    Ok(())
}

/// Provision a new db guard: validate prerequisites, store credentials,
/// install monitoring infrastructure (Postgres), and register in the local DB.
/// On failure at any step, rolls back all previously provisioned state.
fn add_db_guard(
    database: &Database,
    dsn: &str,
    tables: Option<&str>,
    name: Option<&str>,
    mode: db::DbGuardMode,
) -> Result<()> {
    let guard_name = name
        .map(str::to_string)
        .unwrap_or_else(|| super::derive_guard_name_from_dsn(dsn));
    let engine = super::detect_engine(dsn).context("Unsupported DSN format")?;
    validate_engine_prerequisites(engine)?;

    let tables_csv = tables.unwrap_or("*").to_string();
    let mode_kind = normalize_mode(engine, mode);
    let connection_ref = super::credentials::scrub_dsn(dsn);

    // Track provisioned state for rollback on failure
    let embedded_creds = extract_dsn_credentials(dsn);
    let previous_cred = store_credentials_if_needed(
        &connection_ref,
        &guard_name,
        engine,
        embedded_creds.as_ref(),
    )?;

    let install_result = install_and_register(
        database,
        dsn,
        engine,
        &guard_name,
        &connection_ref,
        &tables_csv,
        mode_kind,
    );

    if let Err(err) = install_result {
        // Consolidated rollback: restore credentials and drop monitoring infra
        rollback_credentials(&connection_ref, &guard_name, previous_cred.as_ref());
        // Monitoring infra may have been partially installed; best-effort cleanup
        if engine == db::DbGuardEngine::Postgres {
            rollback_postgres_infra(dsn, &guard_name, &tables_csv);
        }
        return Err(err);
    }

    println!("Added db guard '{guard_name}' ({engine})");
    Ok(())
}

fn validate_engine_prerequisites(engine: db::DbGuardEngine) -> Result<()> {
    match engine {
        db::DbGuardEngine::Postgres if which::which("pg_dump").is_err() => {
            anyhow::bail!(
                "pg_dump not found in PATH. Postgres guard requires pg_dump for baseline snapshots. \
                 Install postgresql-client or equivalent package."
            );
        }
        db::DbGuardEngine::Mysql if which::which("mysql").is_err() => {
            anyhow::bail!(
                "mysql CLI not found in PATH. MySQL guard requires the mysql client. \
                 Install mysql-client or equivalent package."
            );
        }
        _ => Ok(()),
    }
}

fn normalize_mode(engine: db::DbGuardEngine, mode: db::DbGuardMode) -> db::DbGuardMode {
    if engine == db::DbGuardEngine::Mysql && mode != db::DbGuardMode::SchemaPolling {
        tracing::warn!(
            "MySQL guard only supports schema_polling mode; normalizing requested mode '{}'",
            mode
        );
        db::DbGuardMode::SchemaPolling
    } else {
        mode
    }
}

/// Store embedded DSN credentials, returning the previous credential (if any) for rollback.
fn store_credentials_if_needed(
    connection_ref: &str,
    guard_name: &str,
    engine: db::DbGuardEngine,
    creds: Option<&super::credentials::CredentialMaterial>,
) -> Result<Option<Option<super::credentials::CredentialMaterial>>> {
    let Some(creds) = creds else {
        return Ok(None);
    };

    let previous = super::credentials::load_encrypted_credentials(connection_ref).unwrap_or(None);
    super::credentials::store_encrypted_credential(connection_ref, creds).with_context(|| {
        format!(
            "Failed to persist credentials for guard '{}'. \
             Ensure UHOH_MASTER_KEY is set and valid before adding a DSN with embedded credentials",
            guard_name
        )
    })?;

    if engine == db::DbGuardEngine::Postgres {
        let outcome =
            super::credentials::store_postgres_credentials_with_keyring(connection_ref, creds)?;
        if outcome.keyring_status.is_degraded() {
            eprintln!(
                "Warning: stored credentials for '{}' in the encrypted file backend, \
                 but the keyring mirror is unavailable ({})",
                guard_name,
                outcome.keyring_status.describe()
            );
        }
    }

    Ok(Some(previous))
}

/// Install postgres monitoring infrastructure and register the guard in the local DB.
fn install_and_register(
    database: &Database,
    dsn: &str,
    engine: db::DbGuardEngine,
    guard_name: &str,
    connection_ref: &str,
    tables_csv: &str,
    mode_kind: db::DbGuardMode,
) -> Result<()> {
    let watched_tables_cache = if engine == db::DbGuardEngine::Postgres {
        let pg_conn = super::postgres_connection::ResolvedPostgresConnection::resolve(dsn)?;
        super::postgres_monitoring::install_monitoring_infrastructure(&pg_conn, tables_csv)?
    } else {
        None
    };

    database.add_db_guard(
        guard_name,
        engine,
        connection_ref,
        tables_csv,
        watched_tables_cache.as_deref(),
        mode_kind,
    )?;
    Ok(())
}

fn rollback_credentials(
    connection_ref: &str,
    guard_name: &str,
    previous: Option<&Option<super::credentials::CredentialMaterial>>,
) {
    let Some(previous) = previous else { return };
    let restore = previous
        .clone()
        .unwrap_or(super::credentials::CredentialMaterial {
            username: None,
            password: None,
        });
    if let Err(err) = super::credentials::store_encrypted_credential(connection_ref, &restore) {
        tracing::warn!(
            "Failed to restore stored credentials after add failure for '{}': {}",
            guard_name,
            err
        );
    }
}

fn rollback_postgres_infra(dsn: &str, guard_name: &str, tables_csv: &str) {
    let Ok(pg_conn) = super::postgres_connection::ResolvedPostgresConnection::resolve(dsn) else {
        return;
    };
    if let Err(err) =
        super::postgres_monitoring::drop_monitoring_infrastructure(&pg_conn, tables_csv)
    {
        tracing::warn!(
            "Failed to roll back postgres monitoring infrastructure for '{}': {}",
            guard_name,
            err
        );
    }
}

fn extract_dsn_credentials(dsn: &str) -> Option<super::credentials::CredentialMaterial> {
    let parsed = Url::parse(dsn).ok()?;
    let username = if parsed.username().is_empty() {
        None
    } else {
        Some(parsed.username().to_string())
    };
    let password = parsed.password().map(str::to_string);
    if username.is_none() && password.is_none() {
        return None;
    }
    Some(super::credentials::CredentialMaterial { username, password })
}

fn extract_artifact_path(detail: &Option<String>) -> Option<String> {
    let raw = detail.as_ref()?;
    let json = serde_json::from_str::<serde_json::Value>(raw).ok()?;
    json.get("artifact")
        .and_then(|value| value.as_str())
        .map(str::to_string)
}

fn apply_recovery_artifact(
    database: &Database,
    entry: &db::EventLedgerEntry,
    path: &str,
    uhoh_dir: &Path,
) -> Result<()> {
    let artifact_path = Path::new(path);
    if !artifact_path.exists() {
        anyhow::bail!(
            "Recovery artifact does not exist: {}",
            artifact_path.display()
        );
    }

    let mut bytes = std::fs::read(artifact_path)?;
    let mut sql = if artifact_path.extension().and_then(|s| s.to_str()) == Some("enc") {
        let plaintext = super::recovery::decrypt_recovery_payload(&bytes, uhoh_dir)
            .context("Failed to decrypt recovery payload")?;
        bytes.zeroize();
        
        String::from_utf8(plaintext).context("Recovery payload is not valid UTF-8")?
    } else {
        String::from_utf8(bytes).context("Recovery artifact is not valid UTF-8")?
    };
    if !sql.ends_with('\n') {
        sql.push('\n');
    }

    let trimmed = sql.trim_start();
    if !trimmed.to_ascii_uppercase().starts_with("BEGIN") {
        anyhow::bail!("Recovery SQL must start with BEGIN");
    }
    if !sql.to_ascii_uppercase().contains("COMMIT") {
        anyhow::bail!("Recovery SQL must be transaction-wrapped (BEGIN/COMMIT)");
    }

    if entry.source != "db_guard" {
        anyhow::bail!("Recovery apply is only supported for db_guard events");
    }

    let guard_name = entry
        .guard_name
        .as_deref()
        .context("Recovery event missing guard_name")?;
    let guard = database
        .list_db_guards()?
        .into_iter()
        .find(|guard| guard.name == guard_name)
        .with_context(|| format!("Guard '{guard_name}' not found"))?;

    match guard.engine {
        db::DbGuardEngine::Sqlite => apply_sqlite_recovery(&guard.connection_ref, &sql),
        db::DbGuardEngine::Postgres => {
            let postgres_connection =
                super::postgres_connection::ResolvedPostgresConnection::resolve(
                    &guard.connection_ref,
                )?;
            super::postgres_monitoring::execute_sql(&postgres_connection, &sql)
        }
        db::DbGuardEngine::Mysql => {
            anyhow::bail!("Recovery apply is not supported for engine 'mysql'")
        }
    }?;

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

fn apply_sqlite_recovery(connection_ref: &str, sql: &str) -> Result<()> {
    let sqlite_path = connection_ref
        .strip_prefix("sqlite://")
        .unwrap_or(connection_ref);
    let conn = rusqlite::Connection::open(sqlite_path)
        .with_context(|| format!("Failed to open sqlite database at {sqlite_path}"))?;
    conn.execute_batch(sql)
        .with_context(|| format!("Failed to apply sqlite recovery SQL to {sqlite_path}"))?;
    Ok(())
}

fn event_matches_table_filter(entry: &db::EventLedgerEntry, table_filter: &str) -> bool {
    let table = table_filter.trim();
    if table.is_empty() {
        return true;
    }

    if let Some(path) = entry.path.as_deref() {
        if table_name_matches(path, table) {
            return true;
        }
    }

    let Some(detail) = entry.detail.as_deref() else {
        return false;
    };

    if let Ok(json) = serde_json::from_str::<serde_json::Value>(detail) {
        return json_contains_table_name(&json, table);
    }

    table_name_matches(detail, table)
}

fn json_contains_table_name(value: &serde_json::Value, table: &str) -> bool {
    match value {
        serde_json::Value::String(text) => table_name_matches(text, table),
        serde_json::Value::Array(items) => items
            .iter()
            .any(|item| json_contains_table_name(item, table)),
        serde_json::Value::Object(map) => {
            let key_hits = ["table", "table_name", "tableName", "relation", "relname"];
            for (key, value) in map {
                if key_hits.iter().any(|candidate| candidate == key)
                    && json_contains_table_name(value, table) {
                        return true;
                    }
                if matches!(
                    key.as_str(),
                    "added_tables" | "removed_tables" | "tables" | "details"
                ) && json_contains_table_name(value, table)
                {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

fn table_name_matches(candidate: &str, table: &str) -> bool {
    let normalized_table = table.trim().trim_matches('"').to_ascii_lowercase();
    if normalized_table.is_empty() {
        return false;
    }

    let normalized_candidate = candidate.trim().trim_matches('"').to_ascii_lowercase();
    if normalized_candidate == normalized_table {
        return true;
    }

    normalized_candidate
        .rsplit('.')
        .next()
        .map(|value| value.trim_matches('"') == normalized_table)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::{json_contains_table_name, table_name_matches};

    #[test]
    fn table_match_supports_schema_and_case() {
        assert!(table_name_matches("public.Users", "users"));
        assert!(table_name_matches("users", "users"));
        assert!(!table_name_matches("orders", "users"));
    }

    #[test]
    fn json_table_match_recurses_nested_objects() {
        let detail = serde_json::json!({
            "details": {
                "added_tables": ["public.orders"],
                "relname": "audit_log"
            }
        });
        assert!(json_contains_table_name(&detail, "orders"));
        assert!(!json_contains_table_name(&detail, "users"));
    }
}
