use anyhow::{Context, Result};
use std::process::Command;

use crate::db::DbGuardEntry;
use crate::event_ledger::new_event;
use crate::subsystem::SubsystemContext;

#[derive(Debug, Clone, Default)]
pub struct MysqlGuardState {
    pub last_schema_hash: Option<String>,
    pub last_row_total: Option<i64>,
}

pub fn tick_mysql_guard(
    ctx: &SubsystemContext,
    guard: &DbGuardEntry,
    state: &mut MysqlGuardState,
) -> Result<()> {
    // Phase 1 schema polling mode: detect abrupt table/index shape changes and estimate row
    // count drops to surface catastrophic operations before full binlog support lands.
    let snapshot = match poll_schema_snapshot(&guard.connection_ref) {
        Ok(snapshot) => snapshot,
        Err(e) => {
            let mut event = new_event("db_guard", "mysql_poll_failed", "warn");
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(format!("poll_error={e}"));
            let _ = ctx.event_ledger.append(event);
            return Ok(());
        }
    };
    let schema_hash = blake3::hash(snapshot.schema_sql.as_bytes()).to_hex().to_string();

    if let Some(prev_hash) = &state.last_schema_hash {
        if prev_hash != &schema_hash {
            let mut event = new_event("db_guard", "drop_table", "critical");
            event.guard_name = Some(guard.name.clone());
            event.detail = Some(
                serde_json::json!({
                    "mode": "schema_polling",
                    "previous_schema_hash": prev_hash,
                    "current_schema_hash": schema_hash,
                    "detected_at": chrono::Utc::now().to_rfc3339(),
                })
                .to_string(),
            );
            let _ = ctx.event_ledger.append(event);
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
            let _ = ctx.event_ledger.append(event);
        }
    }

    state.last_schema_hash = Some(schema_hash);
    state.last_row_total = Some(snapshot.row_total);

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
    let _ = ctx.event_ledger.append(heartbeat);
    Ok(())
}

struct MysqlSnapshot {
    schema_sql: String,
    row_total: i64,
    table_count: usize,
}

fn poll_schema_snapshot(connection_ref: &str) -> Result<MysqlSnapshot> {
    let parsed = parse_mysql_ref(connection_ref)?;
    let mut cmd = Command::new("mysql");
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
        .arg(&parsed.user);

    if let Some(db) = &parsed.database {
        cmd.arg("-D").arg(db);
    }
    if let Some(password) = &parsed.password {
        cmd.env("MYSQL_PWD", password);
    }

    let output = cmd
        .output()
        .context("Failed to execute mysql client for schema polling")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("mysql schema polling failed: {}", stderr.trim());
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
    user: String,
    password: Option<String>,
    database: Option<String>,
}

fn parse_mysql_ref(connection_ref: &str) -> Result<ParsedMysqlRef> {
    let uri = connection_ref
        .strip_prefix("mysql://")
        .ok_or_else(|| anyhow::anyhow!("Expected mysql:// connection reference"))?;
    let (userinfo_host, database) = uri
        .split_once('/')
        .map(|(lhs, rhs)| (lhs, Some(rhs.split('?').next().unwrap_or_default().to_string())))
        .unwrap_or((uri, None));

    let (userinfo, hostport) = userinfo_host
        .split_once('@')
        .ok_or_else(|| anyhow::anyhow!("MySQL connection ref must include user@host"))?;
    let (user, password) = userinfo
        .split_once(':')
        .map(|(u, p)| (u.to_string(), Some(p.to_string())))
        .unwrap_or((userinfo.to_string(), None));
    let (host, port) = hostport
        .split_once(':')
        .map(|(h, p)| (h.to_string(), p.parse::<u16>().unwrap_or(3306)))
        .unwrap_or((hostport.to_string(), 3306));

    Ok(ParsedMysqlRef {
        host,
        port,
        user,
        password,
        database,
    })
}
