use anyhow::{Context, Result};
use std::process::Command;
use tempfile::NamedTempFile;
use url::Url;

use crate::db::DbGuardEntry;
use crate::db_guard::CredentialMaterial;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

#[derive(Debug, Clone, Default)]
pub struct MysqlGuardState {
    pub last_schema_hash: Option<String>,
    pub last_row_total: Option<i64>,
    pub last_table_count: Option<usize>,
}

pub fn tick_mysql_guard(
    ctx: &SubsystemContext,
    guard: &DbGuardEntry,
    state: &mut MysqlGuardState,
) -> Result<()> {
    // Phase 1 schema polling mode: detect abrupt table/index shape changes and estimate row
    // count drops to surface catastrophic operations before full binlog support lands.
    let stored_credentials = crate::db_guard::resolve_stored_credentials(&guard.connection_ref)?;
    let snapshot = match poll_schema_snapshot(&guard.connection_ref, stored_credentials.as_ref()) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let mut event = new_event("db_guard", "mysql_poll_failed", "warn");
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(format!("poll_error={e}"));
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append mysql_poll_failed event: {err}");
            }
            return Ok(());
        }
    };
    let schema_hash = blake3::hash(snapshot.schema_sql.as_bytes())
        .to_hex()
        .to_string();

    if let Some(prev_hash) = &state.last_schema_hash {
        if prev_hash != &schema_hash {
            let (event_type, severity) = if state
                .last_table_count
                .map(|previous| snapshot.table_count < previous)
                .unwrap_or(false)
            {
                ("drop_table", "critical")
            } else {
                ("schema_change", "warn")
            };
            let mut event = new_event("db_guard", event_type, severity);
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(
                serde_json::json!({
                    "mode": "schema_polling",
                    "previous_schema_hash": prev_hash,
                    "current_schema_hash": schema_hash,
                    "table_count_previous": state.last_table_count,
                    "table_count_current": snapshot.table_count,
                    "change_hint": event_type,
                    "detected_at": chrono::Utc::now().to_rfc3339(),
                })
                .to_string(),
            );
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append mysql schema change event: {err}");
            }
        }
    }

    if let Some(prev_rows) = state.last_row_total {
        let deleted = prev_rows.saturating_sub(snapshot.row_total);
        if deleted >= ctx.config.db_guard.mass_delete_row_threshold as i64 {
            let mut event = new_event("db_guard", "mass_delete", "critical");
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(
                serde_json::json!({
                    "mode": "schema_polling",
                    "estimated_deleted_rows": deleted,
                    "previous_row_total": prev_rows,
                    "current_row_total": snapshot.row_total,
                })
                .to_string(),
            );
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append mysql mass_delete event: {err}");
            }
        }
    }

    state.last_schema_hash = Some(schema_hash);
    state.last_row_total = Some(snapshot.row_total);
    state.last_table_count = Some(snapshot.table_count);

    let mut heartbeat = new_event("db_guard", "mysql_tick", "info");
    heartbeat.guard_name = Some(guard.name.clone());
    heartbeat.detail = Some(
        serde_json::json!({
            "mode": "schema_polling",
            "tables": snapshot.table_count,
            "row_total": snapshot.row_total,
        })
        .to_string(),
    );
    if let Err(err) = ctx.event_ledger.append(heartbeat) {
        tracing::error!("failed to append mysql_tick event: {err}");
    }
    Ok(())
}

struct MysqlSnapshot {
    schema_sql: String,
    row_total: i64,
    table_count: usize,
}

fn poll_schema_snapshot(
    connection_ref: &str,
    stored_credentials: Option<&CredentialMaterial>,
) -> Result<MysqlSnapshot> {
    let parsed = parse_mysql_ref(connection_ref)?;
    let effective_user = stored_credentials
        .and_then(|creds| creds.username.clone())
        .or(parsed.user.clone())
        .ok_or_else(|| anyhow::anyhow!("MySQL connection ref must include user@host"))?;
    let effective_password = stored_credentials
        .and_then(|creds| creds.password.clone())
        .or(parsed.password.clone());

    let mut defaults_guard: Option<NamedTempFile> = None;
    let mut defaults_path = None;
    let mut cmd = Command::new("mysql");

    if let Some(password) = &effective_password {
        let mut tmp = NamedTempFile::new().context("Failed creating MySQL defaults file")?;
        use std::io::Write as _;
        writeln!(tmp, "[client]")?;
        writeln!(tmp, "password={password}")?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(tmp.path(), std::fs::Permissions::from_mode(0o600)).ok();
        }
        cmd.arg(format!("--defaults-extra-file={}", tmp.path().display()));
        defaults_path = Some(tmp.path().to_path_buf());
        defaults_guard = Some(tmp);
    }

    cmd.arg("--batch")
        .arg("--skip-column-names")
        .arg("-e")
        .arg(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(TABLE_ROWS, 0) \
             FROM information_schema.tables \
             WHERE TABLE_TYPE='BASE TABLE' \
             ORDER BY TABLE_SCHEMA, TABLE_NAME",
        )
        .arg("-h")
        .arg(&parsed.host)
        .arg("-P")
        .arg(parsed.port.to_string())
        .arg("-u")
        .arg(&effective_user);
    if let Some(db) = &parsed.database {
        cmd.arg("-D").arg(db);
    }

    let output = cmd
        .output()
        .context("Failed to execute mysql client for schema polling")?;
    if let Some(path) = defaults_path.as_ref() {
        let _ = std::fs::remove_file(path);
    }
    drop(defaults_guard);
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!(
            "mysql schema polling failed: {}",
            crate::db_guard::scrub_error_message(stderr.trim())
        );
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut schema_sql = String::new();
    let mut row_total = 0i64;
    let mut table_count = 0usize;
    for line in stdout.lines() {
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 3 {
            continue;
        }
        table_count += 1;
        let schema = cols[0];
        let table = cols[1];
        let rows = cols[2].parse::<i64>().unwrap_or(0).max(0);
        row_total = row_total.saturating_add(rows);
        schema_sql.push_str(schema);
        schema_sql.push('.');
        schema_sql.push_str(table);
        schema_sql.push(';');
    }

    Ok(MysqlSnapshot {
        schema_sql,
        row_total,
        table_count,
    })
}

struct ParsedMysqlRef {
    host: String,
    port: u16,
    user: Option<String>,
    password: Option<String>,
    database: Option<String>,
}

fn parse_mysql_ref(connection_ref: &str) -> Result<ParsedMysqlRef> {
    let url = Url::parse(connection_ref).context("Invalid MySQL connection reference")?;
    if url.scheme() != "mysql" {
        anyhow::bail!("Expected mysql:// connection reference");
    }

    let user = if url.username().is_empty() {
        None
    } else {
        Some(url.username().to_string())
    };
    let password = url.password().map(str::to_string);
    let host = url
        .host_str()
        .map(str::to_string)
        .context("MySQL connection ref must include host")?;
    let port = url.port().unwrap_or(3306);
    let database = {
        let db = url.path().trim_start_matches('/').trim();
        if db.is_empty() {
            None
        } else {
            Some(db.to_string())
        }
    };

    Ok(ParsedMysqlRef {
        host,
        port,
        user,
        password,
        database,
    })
}
