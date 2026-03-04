use anyhow::{Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;

use super::credentials::CredentialMaterial;

#[derive(Debug, Clone)]
pub struct ArtifactInfo {
    pub path: String,
    pub blake3: String,
}

pub fn write_sqlite_baseline(
    uhoh_dir: &std::path::Path,
    guard_name: &str,
    sqlite_path: &std::path::Path,
    encrypt: bool,
    retention_days: u64,
) -> Result<ArtifactInfo> {
    let (base_dir, baseline_dir, _) = ensure_guard_dirs(uhoh_dir, guard_name)?;
    ensure_secure_dir(&base_dir)?;
    ensure_secure_dir(&baseline_dir)?;

    let ts = timestamp_tag();
    let target = baseline_dir.join(format!("{ts}.db"));
    let src = rusqlite::Connection::open(sqlite_path)
        .with_context(|| format!("Failed to open SQLite guard DB: {}", sqlite_path.display()))?;
    let mut dest = rusqlite::Connection::open(&target)
        .with_context(|| format!("Failed to open baseline target: {}", target.display()))?;
    let backup = rusqlite::backup::Backup::new(&src, &mut dest)?;
    backup.run_to_completion(8, std::time::Duration::from_millis(25), None)?;

    let raw = std::fs::read(&target)?;
    let payload = if encrypt { maybe_encrypt(&raw)? } else { raw };
    if encrypt {
        std::fs::write(&target, &payload)?;
    }
    set_secure_permissions(&target)?;

    cleanup_old_files(&baseline_dir, retention_days)?;

    Ok(ArtifactInfo {
        path: target.display().to_string(),
        blake3: blake3::hash(&payload).to_hex().to_string(),
    })
}

pub fn write_sqlite_schema_recovery(
    uhoh_dir: &std::path::Path,
    guard_name: &str,
    sqlite_path: &std::path::Path,
    label: &str,
    encrypt: bool,
    retention_days: u64,
) -> Result<ArtifactInfo> {
    let (_, _, recovery_dir) = ensure_guard_dirs(uhoh_dir, guard_name)?;
    ensure_secure_dir(&recovery_dir)?;

    let schema = sqlite_schema_dump(sqlite_path)?;
    let mut payload = schema.into_bytes();
    if encrypt {
        payload = maybe_encrypt(&payload)?;
    }

    let file = recovery_dir.join(format!("{}_{}_schema.sql", timestamp_tag(), label));
    std::fs::write(&file, &payload)?;
    set_secure_permissions(&file)?;

    cleanup_old_files(&recovery_dir, retention_days)?;

    Ok(ArtifactInfo {
        path: file.display().to_string(),
        blake3: blake3::hash(&payload).to_hex().to_string(),
    })
}

pub fn write_postgres_schema_recovery(
    uhoh_dir: &std::path::Path,
    guard_name: &str,
    connection_ref: &str,
    creds: &CredentialMaterial,
    label: &str,
    encrypt: bool,
    retention_days: u64,
) -> Result<ArtifactInfo> {
    let (_, baseline_dir, recovery_dir) = ensure_guard_dirs(uhoh_dir, guard_name)?;
    ensure_secure_dir(&baseline_dir)?;
    ensure_secure_dir(&recovery_dir)?;

    let schema = postgres_schema_dump(connection_ref, creds)?;
    let mut payload = schema.into_bytes();
    if encrypt {
        payload = maybe_encrypt(&payload)?;
    }

    let file = recovery_dir.join(format!("{}_{}_schema.sql", timestamp_tag(), label));
    std::fs::write(&file, &payload)?;
    set_secure_permissions(&file)?;

    cleanup_old_files(&recovery_dir, retention_days)?;
    cleanup_old_files(&baseline_dir, retention_days)?;

    Ok(ArtifactInfo {
        path: file.display().to_string(),
        blake3: blake3::hash(&payload).to_hex().to_string(),
    })
}

pub fn write_postgres_schema_baseline(
    uhoh_dir: &std::path::Path,
    guard_name: &str,
    connection_ref: &str,
    creds: &CredentialMaterial,
    retention_days: u64,
) -> Result<ArtifactInfo> {
    let (_, baseline_dir, _) = ensure_guard_dirs(uhoh_dir, guard_name)?;
    ensure_secure_dir(&baseline_dir)?;

    let schema = postgres_schema_dump(connection_ref, creds)?;
    let payload = schema.into_bytes();
    let file = baseline_dir.join(format!("{}_schema.sql", timestamp_tag()));
    std::fs::write(&file, &payload)?;
    set_secure_permissions(&file)?;
    cleanup_old_files(&baseline_dir, retention_days)?;

    Ok(ArtifactInfo {
        path: file.display().to_string(),
        blake3: blake3::hash(&payload).to_hex().to_string(),
    })
}

pub fn sqlite_schema_dump(sqlite_path: &std::path::Path) -> Result<String> {
    let conn = rusqlite::Connection::open(sqlite_path)
        .with_context(|| format!("Failed to open sqlite DB: {}", sqlite_path.display()))?;
    let mut stmt = conn.prepare(
        "SELECT sql FROM sqlite_master
         WHERE sql IS NOT NULL
         ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'trigger' THEN 2 ELSE 3 END,
                  name ASC",
    )?;
    let mut rows = stmt.query([])?;
    let mut sql = String::from("BEGIN;\n");
    while let Some(row) = rows.next()? {
        let line: String = row.get(0)?;
        sql.push_str(&line);
        if !line.trim_end().ends_with(';') {
            sql.push(';');
        }
        sql.push('\n');
    }
    sql.push_str("COMMIT;\n");
    Ok(sql)
}

fn postgres_schema_dump(connection_ref: &str, creds: &CredentialMaterial) -> Result<String> {
    let mut cmd = std::process::Command::new("pg_dump");
    cmd.arg("--schema-only")
        .arg("--no-owner")
        .arg("--no-privileges")
        .arg(connection_ref);
    if let Some(password) = &creds.password {
        cmd.env("PGPASSWORD", password);
    }
    let output = cmd.output().context("Failed to execute pg_dump")?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("pg_dump failed: {}", err.trim());
    }
    let dump = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(wrap_in_transaction(&dump))
}

fn wrap_in_transaction(sql: &str) -> String {
    let trimmed = sql.trim();
    let has_begin = trimmed.lines().any(|line| line.trim().eq_ignore_ascii_case("BEGIN;"));
    let has_commit = trimmed.lines().any(|line| line.trim().eq_ignore_ascii_case("COMMIT;"));
    if has_begin && has_commit {
        format!("{trimmed}\n")
    } else {
        format!("BEGIN;\n{trimmed}\nCOMMIT;\n")
    }
}

fn maybe_encrypt(plaintext: &[u8]) -> Result<Vec<u8>> {
    let key_material = derive_key_material();
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key_material));
    let mut nonce_buf = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce_buf);
    let nonce = Nonce::from_slice(&nonce_buf);
    let mut out = b"UHOHENC1".to_vec();
    out.extend_from_slice(&nonce_buf);
    let encrypted = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| anyhow::anyhow!("Failed to encrypt artifact"))?;
    out.extend_from_slice(&encrypted);
    Ok(out)
}

fn derive_key_material() -> [u8; 32] {
    if let Ok(master) = std::env::var("UHOH_MASTER_KEY") {
        if !master.trim().is_empty() {
            let hash = blake3::hash(master.as_bytes());
            let mut out = [0u8; 32];
            out.copy_from_slice(hash.as_bytes());
            return out;
        }
    }

    let user = std::env::var("USER").unwrap_or_default();
    let host = std::env::var("HOSTNAME").unwrap_or_default();
    let seed = format!("uhoh:{}:{}", user, host);
    let hash = blake3::hash(seed.as_bytes());
    let mut out = [0u8; 32];
    out.copy_from_slice(hash.as_bytes());
    out
}

fn ensure_guard_dirs(
    uhoh_dir: &std::path::Path,
    guard_name: &str,
) -> Result<(std::path::PathBuf, std::path::PathBuf, std::path::PathBuf)> {
    let base = uhoh_dir.join("db_guard").join(guard_name);
    let baseline = base.join("baselines");
    let recovery = base.join("recovery");
    std::fs::create_dir_all(&baseline)?;
    std::fs::create_dir_all(&recovery)?;
    Ok((base, baseline, recovery))
}

fn ensure_secure_dir(path: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_secure_permissions(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn cleanup_old_files(dir: &std::path::Path, retention_days: u64) -> Result<()> {
    let now = std::time::SystemTime::now();
    let keep_for = std::time::Duration::from_secs(retention_days.saturating_mul(86_400));
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let Ok(modified) = meta.modified() else {
            continue;
        };
        if now.duration_since(modified).unwrap_or_default() > keep_for {
            let _ = std::fs::remove_file(path);
        }
    }
    Ok(())
}

pub fn cleanup_retention(dir: &std::path::Path, retention_days: u64) -> Result<()> {
    cleanup_old_files(dir, retention_days)
}

fn timestamp_tag() -> String {
    chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string()
}
