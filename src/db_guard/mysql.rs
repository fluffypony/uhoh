use anyhow::{Context, Result};
use std::process::Command;
use tempfile::NamedTempFile;
use url::Url;

use super::credentials::{load_encrypted_credentials, scrub_error_message, CredentialMaterial};
use crate::db::{DbGuardEntry, LedgerEventType, LedgerSeverity, LedgerSource};
use crate::event_ledger::new_event;
use crate::subsystem::DbGuardContext;

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct MysqlGuardState {
    pub last_schema_hash: Option<String>,
    pub last_row_total: Option<i64>,
    pub last_table_count: Option<usize>,
}

pub fn tick_mysql_guard(
    ctx: &DbGuardContext,
    guard: &DbGuardEntry,
    state: &mut MysqlGuardState,
) -> Result<()> {
    // Phase 1 schema polling mode: detect abrupt table/index shape changes and estimate row
    // count drops to surface catastrophic operations before full binlog support lands.
    let stored_credentials = load_encrypted_credentials(&guard.connection_ref)?;
    let snapshot = match poll_schema_snapshot(
        &guard.connection_ref,
        stored_credentials.as_ref(),
        &guard.tables_csv,
    ) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let mut event = new_event(
                LedgerSource::DbGuard,
                LedgerEventType::MysqlPollFailed,
                LedgerSeverity::Warn,
            );
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(format!("poll_error={e}"));
            if let Err(err) = ctx.event_ledger.append(event) {
                tracing::error!("failed to append mysql_poll_failed event: {err}");
            }
            return Err(e);
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
                (LedgerEventType::DropTable, LedgerSeverity::Critical)
            } else {
                (LedgerEventType::SchemaChange, LedgerSeverity::Warn)
            };
            let change_hint = event_type.as_str().to_string();
            let mut event = new_event(LedgerSource::DbGuard, event_type, severity);
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(
                serde_json::json!({
                    "mode": "schema_polling",
                    "previous_schema_hash": prev_hash,
                    "current_schema_hash": schema_hash,
                    "table_count_previous": state.last_table_count,
                    "table_count_current": snapshot.table_count,
                    "change_hint": change_hint,
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
        let pct = if prev_rows > 0 {
            deleted as f64 / prev_rows as f64
        } else {
            0.0
        };
        if deleted >= ctx.config.db_guard.mass_delete_row_threshold as i64
            || pct >= ctx.config.db_guard.mass_delete_pct_threshold
        {
            let mut event = new_event(
                LedgerSource::DbGuard,
                LedgerEventType::MassDelete,
                LedgerSeverity::Critical,
            );
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

    let mut heartbeat = new_event(LedgerSource::DbGuard, LedgerEventType::MysqlTick, LedgerSeverity::Info);
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
    tables_csv: &str,
) -> Result<MysqlSnapshot> {
    let parsed = parse_mysql_ref(connection_ref)?;
    let env_credentials = resolve_mysql_env_credentials();
    let effective_user = env_credentials
        .as_ref()
        .and_then(|creds| creds.username.clone())
        .or_else(|| stored_credentials.and_then(|creds| creds.username.clone()))
        .or(parsed.user.clone())
        .ok_or_else(|| anyhow::anyhow!("MySQL connection ref must include user@host"))?;
    let effective_password = env_credentials
        .as_ref()
        .and_then(|creds| creds.password.clone())
        .or_else(|| stored_credentials.and_then(|creds| creds.password.clone()))
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

    // Build query with optional table filter from tables_csv
    let query = if tables_csv.trim().is_empty() || tables_csv.trim() == "*" {
        "SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(TABLE_ROWS, 0) \
         FROM information_schema.tables \
         WHERE TABLE_TYPE='BASE TABLE' \
         ORDER BY TABLE_SCHEMA, TABLE_NAME"
            .to_string()
    } else {
        let table_names: Vec<String> = tables_csv
            .split(',')
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty())
            .map(|t| format!("'{}'", t.replace('\'', "''")))
            .collect();
        format!(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COALESCE(TABLE_ROWS, 0) \
             FROM information_schema.tables \
             WHERE TABLE_TYPE='BASE TABLE' AND TABLE_NAME IN ({}) \
             ORDER BY TABLE_SCHEMA, TABLE_NAME",
            table_names.join(",")
        )
    };

    cmd.arg("--batch")
        .arg("--skip-column-names")
        .arg("-e")
        .arg(&query)
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
            scrub_error_message(stderr.trim())
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

fn resolve_mysql_env_credentials() -> Option<CredentialMaterial> {
    let user = std::env::var("UHOH_MYSQL_USER").ok();
    let password = std::env::var("UHOH_MYSQL_PASSWORD").ok();
    if user
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
        && password
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
    {
        return None;
    }
    Some(CredentialMaterial {
        username: user.filter(|v| !v.trim().is_empty()),
        password: password.filter(|v| !v.trim().is_empty()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_mysql_ref_full_url() {
        let parsed = parse_mysql_ref("mysql://admin:secret@db.example.com:3307/mydb").unwrap();
        assert_eq!(parsed.host, "db.example.com");
        assert_eq!(parsed.port, 3307);
        assert_eq!(parsed.user.as_deref(), Some("admin"));
        assert_eq!(parsed.password.as_deref(), Some("secret"));
        assert_eq!(parsed.database.as_deref(), Some("mydb"));
    }

    #[test]
    fn parse_mysql_ref_default_port() {
        let parsed = parse_mysql_ref("mysql://user@localhost/test").unwrap();
        assert_eq!(parsed.port, 3306);
        assert_eq!(parsed.host, "localhost");
    }

    #[test]
    fn parse_mysql_ref_no_user_password() {
        let parsed = parse_mysql_ref("mysql://localhost/db").unwrap();
        assert!(parsed.user.is_none());
        assert!(parsed.password.is_none());
    }

    #[test]
    fn parse_mysql_ref_no_database() {
        let parsed = parse_mysql_ref("mysql://localhost").unwrap();
        assert!(parsed.database.is_none());
    }

    #[test]
    fn parse_mysql_ref_wrong_scheme() {
        let result = parse_mysql_ref("postgres://localhost/db");
        assert!(result.is_err());
    }

    #[test]
    fn parse_mysql_ref_invalid_url() {
        let result = parse_mysql_ref("not a url at all");
        assert!(result.is_err());
    }

    #[test]
    fn mysql_guard_state_default() {
        let state = MysqlGuardState::default();
        assert!(state.last_schema_hash.is_none());
        assert!(state.last_row_total.is_none());
        assert!(state.last_table_count.is_none());
    }
}
