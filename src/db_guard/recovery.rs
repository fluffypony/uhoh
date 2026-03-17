use anyhow::{Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::Rng;
use tempfile::NamedTempFile;
use zeroize::Zeroize;

use super::credentials::CredentialMaterial;
use super::postgres_connection::ResolvedPostgresConnection;

const ENC_MAGIC: &[u8; 8] = b"UHOHENC2";
const ENC_KDF_BLAKE3: u8 = 0;
const ENC_KDF_ARGON2ID: u8 = 1;
const ENC_V2_HEADER_LEN: usize = 8 + 1 + 16 + 12;
const MACHINE_KEY_FILE: &str = "master.key";
const RECOVERY_BLAKE3_CONTEXT: &str = "uhoh::recovery-artifacts::enc-v2";

struct EncryptionMaterial {
    key: [u8; 32],
    kdf_id: u8,
    salt: [u8; 16],
}

#[derive(Debug, Clone)]
#[non_exhaustive]
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
    max_baseline_size_mb: u64,
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
    let payload = if encrypt {
        maybe_encrypt(&raw, uhoh_dir)?
    } else {
        raw
    };
    enforce_max_payload_size(&payload, max_baseline_size_mb, "sqlite baseline")?;
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
    max_recovery_file_mb: u64,
) -> Result<ArtifactInfo> {
    let (_, _, recovery_dir) = ensure_guard_dirs(uhoh_dir, guard_name)?;
    ensure_secure_dir(&recovery_dir)?;

    let schema = sqlite_schema_dump(sqlite_path)?;
    let mut payload = schema.into_bytes();
    if encrypt {
        payload = maybe_encrypt(&payload, uhoh_dir)?;
    }
    enforce_max_payload_size(&payload, max_recovery_file_mb, "sqlite recovery")?;

    let file = recovery_dir.join(format!("{}_{}_schema.sql", timestamp_tag(), label));
    std::fs::write(&file, &payload)?;
    set_secure_permissions(&file)?;

    cleanup_old_files(&recovery_dir, retention_days)?;

    Ok(ArtifactInfo {
        path: file.display().to_string(),
        blake3: blake3::hash(&payload).to_hex().to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
pub fn write_postgres_schema_recovery(
    uhoh_dir: &std::path::Path,
    guard_name: &str,
    connection: &ResolvedPostgresConnection,
    creds: &CredentialMaterial,
    label: &str,
    encrypt: bool,
    retention_days: u64,
    max_recovery_file_mb: u64,
) -> Result<ArtifactInfo> {
    let (_, baseline_dir, recovery_dir) = ensure_guard_dirs(uhoh_dir, guard_name)?;
    ensure_secure_dir(&baseline_dir)?;
    ensure_secure_dir(&recovery_dir)?;

    let schema = postgres_schema_dump(connection, creds)?;
    let mut payload = schema.into_bytes();
    if encrypt {
        payload = maybe_encrypt(&payload, uhoh_dir)?;
    }
    enforce_max_payload_size(&payload, max_recovery_file_mb, "postgres recovery")?;

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
    connection: &ResolvedPostgresConnection,
    creds: &CredentialMaterial,
    retention_days: u64,
    max_baseline_size_mb: u64,
) -> Result<ArtifactInfo> {
    let (_, baseline_dir, _) = ensure_guard_dirs(uhoh_dir, guard_name)?;
    ensure_secure_dir(&baseline_dir)?;

    let schema = postgres_schema_dump(connection, creds)?;
    let payload = schema.into_bytes();
    enforce_max_payload_size(&payload, max_baseline_size_mb, "postgres baseline")?;
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

fn postgres_schema_dump(
    connection: &ResolvedPostgresConnection,
    creds: &CredentialMaterial,
) -> Result<String> {
    let mut passfile: Option<NamedTempFile> = None;
    let mut cmd = std::process::Command::new("pg_dump");
    cmd.arg("--schema-only")
        .arg("--no-owner")
        .arg("--no-privileges")
        .arg(connection.connect_dsn());
    if let Some(password) = &creds.password {
        use std::io::Write as _;
        let parsed = parse_postgres_connection_ref(connection.connect_dsn())?;
        let mut file = NamedTempFile::new().context("Failed creating temporary pgpass file")?;
        writeln!(
            file,
            "{}:{}:{}:{}:{}",
            parsed.host, parsed.port, parsed.database, parsed.user, password
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(file.path(), std::fs::Permissions::from_mode(0o600))?;
        }
        cmd.env("PGPASSFILE", file.path());
        passfile = Some(file);
    }
    let output = cmd.output().context("Failed to execute pg_dump")?;
    drop(passfile);
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("pg_dump failed: {}", err.trim());
    }
    let dump = String::from_utf8_lossy(&output.stdout).to_string();
    Ok(wrap_in_transaction(&dump))
}

struct ParsedPgRef {
    host: String,
    port: u16,
    user: String,
    database: String,
}

fn parse_postgres_connection_ref(connection_ref: &str) -> Result<ParsedPgRef> {
    let url = url::Url::parse(connection_ref).context("Invalid postgres connection reference")?;
    if !matches!(url.scheme(), "postgres" | "postgresql") {
        anyhow::bail!("Expected postgres:// or postgresql:// connection reference");
    }

    let host = url.host_str().unwrap_or("localhost").to_string();
    let port = url.port().unwrap_or(5432);
    let user = if url.username().is_empty() {
        "postgres".to_string()
    } else {
        url.username().to_string()
    };
    let database = {
        let name = url.path().trim_start_matches('/').trim();
        if name.is_empty() {
            user.clone()
        } else {
            name.to_string()
        }
    };

    Ok(ParsedPgRef {
        host,
        port,
        user,
        database,
    })
}

fn wrap_in_transaction(sql: &str) -> String {
    let trimmed = sql.trim();
    let has_begin = trimmed
        .lines()
        .any(|line| line.trim().eq_ignore_ascii_case("BEGIN;"));
    let has_commit = trimmed
        .lines()
        .any(|line| line.trim().eq_ignore_ascii_case("COMMIT;"));
    if has_begin && has_commit {
        format!("{trimmed}\n")
    } else {
        format!("BEGIN;\n{trimmed}\nCOMMIT;\n")
    }
}

fn enforce_max_payload_size(payload: &[u8], max_mb: u64, label: &str) -> Result<()> {
    let max_bytes = max_mb.saturating_mul(1024 * 1024);
    if max_bytes == 0 {
        anyhow::bail!("{label} maximum size must be greater than 0");
    }
    if payload.len() as u64 > max_bytes {
        #[allow(clippy::cast_precision_loss)] // precision loss acceptable for display-only MiB conversion
        let payload_mib = payload.len() as f64 / 1_048_576.0;
        #[allow(clippy::cast_precision_loss)] // precision loss acceptable for display-only MiB conversion
        let limit_mib = max_bytes as f64 / 1_048_576.0;
        anyhow::bail!(
            "{label} payload ({payload_mib:.2} MiB) exceeds configured limit ({limit_mib:.2} MiB)"
        );
    }
    Ok(())
}

pub fn decrypt_recovery_payload(payload: &[u8], uhoh_dir: &std::path::Path) -> Result<Vec<u8>> {
    if payload.starts_with(ENC_MAGIC) {
        return decrypt_v2(payload, uhoh_dir);
    }
    anyhow::bail!("Invalid or corrupted recovery payload: unrecognized encryption format")
}

fn maybe_encrypt(plaintext: &[u8], uhoh_dir: &std::path::Path) -> Result<Vec<u8>> {
    let mut material = derive_encryption_material(uhoh_dir)?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&material.key));
    let mut nonce_buf = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce_buf);
    let nonce = Nonce::from_slice(&nonce_buf);
    let mut out = ENC_MAGIC.to_vec();
    out.push(material.kdf_id);
    out.extend_from_slice(&material.salt);
    out.extend_from_slice(&nonce_buf);
    let encrypted = cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| anyhow::anyhow!("Failed to encrypt artifact"))?;
    out.extend_from_slice(&encrypted);
    material.key.zeroize();
    material.salt.zeroize();
    Ok(out)
}

fn derive_encryption_material(uhoh_dir: &std::path::Path) -> Result<EncryptionMaterial> {
    if let Some(mut master) = read_master_key_from_env() {
        let mut salt = [0u8; 16];
        if let Some(mut raw_key) = decode_hex_key(&master) {
            let key = blake3::derive_key(RECOVERY_BLAKE3_CONTEXT, &raw_key);
            raw_key.zeroize();
            master.zeroize();
            return Ok(EncryptionMaterial {
                key,
                kdf_id: ENC_KDF_BLAKE3,
                salt,
            });
        }

        rand::rng().fill_bytes(&mut salt);
        let key = match derive_argon2_key(&master, &salt) {
            Ok(k) => {
                master.zeroize();
                k
            }
            Err(e) => {
                master.zeroize();
                return Err(e);
            }
        };
        return Ok(EncryptionMaterial {
            key,
            kdf_id: ENC_KDF_ARGON2ID,
            salt,
        });
    }

    tracing::warn!(
        "UHOH_MASTER_KEY is not set; using machine-local fallback key at ~/.uhoh/{}",
        MACHINE_KEY_FILE
    );
    let mut machine_key = load_or_create_machine_key(uhoh_dir)?;
    let key = blake3::derive_key(RECOVERY_BLAKE3_CONTEXT, &machine_key);
    machine_key.zeroize();
    Ok(EncryptionMaterial {
        key,
        kdf_id: ENC_KDF_BLAKE3,
        salt: [0u8; 16],
    })
}

fn decrypt_v2(payload: &[u8], uhoh_dir: &std::path::Path) -> Result<Vec<u8>> {
    if payload.len() <= ENC_V2_HEADER_LEN {
        anyhow::bail!("Encrypted recovery artifact is malformed");
    }

    let kdf_id = payload[8];
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&payload[9..25]);
    let mut nonce_buf = [0u8; 12];
    nonce_buf.copy_from_slice(&payload[25..37]);
    let ciphertext = &payload[37..];

    let mut key = match kdf_id {
        ENC_KDF_ARGON2ID => {
            let mut master = read_master_key_from_env().ok_or_else(|| {
                anyhow::anyhow!(
                    "Encrypted artifact requires UHOH_MASTER_KEY for Argon2id decryption"
                )
            })?;
            match derive_argon2_key(&master, &salt) {
                Ok(v) => {
                    master.zeroize();
                    v
                }
                Err(e) => {
                    master.zeroize();
                    return Err(e);
                }
            }
        }
        ENC_KDF_BLAKE3 => {
            if let Some(mut master) = read_master_key_from_env() {
                if let Some(mut raw_key) = decode_hex_key(&master) {
                    let key = blake3::derive_key(RECOVERY_BLAKE3_CONTEXT, &raw_key);
                    raw_key.zeroize();
                    master.zeroize();
                    key
                } else {
                    master.zeroize();
                    anyhow::bail!(
                        "UHOH_MASTER_KEY must be 64-char hex for this encrypted recovery artifact"
                    );
                }
            } else {
                let mut machine_key = load_or_create_machine_key(uhoh_dir)?;
                let key = blake3::derive_key(RECOVERY_BLAKE3_CONTEXT, &machine_key);
                machine_key.zeroize();
                key
            }
        }
        _ => anyhow::bail!("Unsupported encrypted recovery artifact KDF id"),
    };

    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let nonce = Nonce::from_slice(&nonce_buf);
    let result = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|_| anyhow::anyhow!("Failed to decrypt recovery artifact"));
    key.zeroize();
    salt.zeroize();
    result
}

fn read_master_key_from_env() -> Option<String> {
    std::env::var("UHOH_MASTER_KEY")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn decode_hex_key(input: &str) -> Option<[u8; 32]> {
    if input.len() != 64 {
        return None;
    }
    let mut key = [0u8; 32];
    hex::decode_to_slice(input, &mut key).ok()?;
    Some(key)
}

fn derive_argon2_key(master: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    super::crypto_policy::derive_argon2id_key(master, salt).map_err(|err| {
        anyhow::anyhow!("Failed to derive encryption key from UHOH_MASTER_KEY: {err}")
    })
}

pub(crate) fn load_or_create_machine_key(uhoh_dir: &std::path::Path) -> Result<[u8; 32]> {
    let key_path = uhoh_dir.join(MACHINE_KEY_FILE);
    if key_path.exists() {
        let raw = std::fs::read_to_string(&key_path)
            .with_context(|| format!("Failed reading {}", key_path.display()))?;
        let trimmed = raw.trim();
        if let Some(key) = decode_hex_key(trimmed) {
            set_secure_permissions(&key_path)?;
            return Ok(key);
        }
        anyhow::bail!(
            "Invalid machine recovery key at {}. Delete it to regenerate.",
            key_path.display()
        );
    }

    let mut key = [0u8; 32];
    rand::rng().fill_bytes(&mut key);
    std::fs::write(&key_path, format!("{}\n", hex::encode(key)))
        .with_context(|| format!("Failed writing {}", key_path.display()))?;
    set_secure_permissions(&key_path)?;
    Ok(key)
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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn load_or_create_machine_key_persists_and_reuses_master_key_file() {
        let temp = tempfile::tempdir().expect("tempdir");
        let first = load_or_create_machine_key(temp.path()).expect("initial key");
        let second = load_or_create_machine_key(temp.path()).expect("reused key");

        assert_eq!(first, second);
        let raw = std::fs::read_to_string(temp.path().join("master.key")).expect("master.key");
        assert_eq!(raw.trim(), hex::encode(first));
    }

    // ── encrypt/decrypt roundtrip ──

    #[test]
    fn encrypt_decrypt_roundtrip_with_machine_key() {
        let temp = tempfile::tempdir().unwrap();
        // Ensure machine key exists
        load_or_create_machine_key(temp.path()).unwrap();

        let plaintext = b"hello, recovery artifacts";
        let encrypted = maybe_encrypt(plaintext, temp.path()).unwrap();

        // Encrypted payload should start with magic
        assert!(encrypted.starts_with(ENC_MAGIC));
        assert_ne!(&encrypted[ENC_V2_HEADER_LEN..], plaintext);

        let decrypted = decrypt_recovery_payload(&encrypted, temp.path()).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_rejects_invalid_magic() {
        let temp = tempfile::tempdir().unwrap();
        load_or_create_machine_key(temp.path()).unwrap();
        let bad = b"BADMAGIC\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00";
        assert!(decrypt_recovery_payload(bad, temp.path()).is_err());
    }

    // ── wrap_in_transaction ──

    #[test]
    fn wrap_in_transaction_adds_begin_commit() {
        let sql = "CREATE TABLE t (id INT);";
        let wrapped = wrap_in_transaction(sql);
        assert!(wrapped.starts_with("BEGIN;\n"));
        assert!(wrapped.ends_with("COMMIT;\n"));
    }

    #[test]
    fn wrap_in_transaction_preserves_existing_transaction() {
        let sql = "BEGIN;\nCREATE TABLE t (id INT);\nCOMMIT;";
        let wrapped = wrap_in_transaction(sql);
        assert_eq!(wrapped.matches("BEGIN;").count(), 1);
        assert_eq!(wrapped.matches("COMMIT;").count(), 1);
    }

    // ── enforce_max_payload_size ──

    #[test]
    fn enforce_max_payload_size_allows_within_limit() {
        let payload = vec![0u8; 100];
        assert!(enforce_max_payload_size(&payload, 1, "test").is_ok());
    }

    #[test]
    fn enforce_max_payload_size_rejects_oversized() {
        let payload = vec![0u8; 2 * 1024 * 1024]; // 2 MiB
        assert!(enforce_max_payload_size(&payload, 1, "test").is_err());
    }

    #[test]
    fn enforce_max_payload_size_rejects_zero_limit() {
        let payload = vec![0u8; 1];
        assert!(enforce_max_payload_size(&payload, 0, "test").is_err());
    }

    // ── parse_postgres_connection_ref ──

    #[test]
    fn parse_postgres_ref_standard() {
        let parsed = parse_postgres_connection_ref("postgres://admin@db.example.com:5433/mydb").unwrap();
        assert_eq!(parsed.host, "db.example.com");
        assert_eq!(parsed.port, 5433);
        assert_eq!(parsed.user, "admin");
        assert_eq!(parsed.database, "mydb");
    }

    #[test]
    fn parse_postgres_ref_defaults() {
        let parsed = parse_postgres_connection_ref("postgres:///").unwrap();
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 5432);
        assert_eq!(parsed.user, "postgres");
    }

    #[test]
    fn parse_postgres_ref_rejects_mysql_scheme() {
        assert!(parse_postgres_connection_ref("mysql://localhost/db").is_err());
    }

    // ── sqlite_schema_dump ──

    #[test]
    fn sqlite_schema_dump_returns_schema() {
        let temp = tempfile::tempdir().unwrap();
        let db_path = temp.path().join("test.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").unwrap();
        drop(conn);

        let schema = sqlite_schema_dump(&db_path).unwrap();
        assert!(schema.contains("CREATE TABLE users"));
        assert!(schema.starts_with("BEGIN;\n"));
        assert!(schema.ends_with("COMMIT;\n"));
    }

    // ── cleanup_retention ──

    #[test]
    fn cleanup_retention_keeps_recent_files() {
        let temp = tempfile::tempdir().unwrap();
        let recent = temp.path().join("recent.dat");
        std::fs::write(&recent, "fresh").unwrap();

        cleanup_retention(temp.path(), 30).unwrap();
        assert!(recent.exists(), "recent files should survive cleanup");
    }

    #[test]
    fn cleanup_retention_handles_empty_dir() {
        let temp = tempfile::tempdir().unwrap();
        assert!(cleanup_retention(temp.path(), 30).is_ok());
    }

    // ── write_sqlite_baseline (public API) ──

    #[test]
    fn write_sqlite_baseline_creates_artifact_unencrypted() {
        let uhoh_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE t (id INT); INSERT INTO t VALUES (42);").unwrap();
        drop(conn);

        let info = write_sqlite_baseline(
            uhoh_dir.path(), "my_guard", &db_path, false, 30, 100,
        ).unwrap();

        assert!(!info.path.is_empty());
        assert!(!info.blake3.is_empty());
        assert!(std::path::Path::new(&info.path).exists());
    }

    #[test]
    fn write_sqlite_baseline_creates_artifact_encrypted() {
        let uhoh_dir = tempfile::tempdir().unwrap();
        load_or_create_machine_key(uhoh_dir.path()).unwrap();

        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE t (id INT);").unwrap();
        drop(conn);

        let info = write_sqlite_baseline(
            uhoh_dir.path(), "enc_guard", &db_path, true, 30, 100,
        ).unwrap();

        // Encrypted artifact should start with magic
        let content = std::fs::read(&info.path).unwrap();
        assert!(content.starts_with(ENC_MAGIC));
    }

    // ── write_sqlite_schema_recovery (public API) ──

    #[test]
    fn write_sqlite_schema_recovery_creates_artifact() {
        let uhoh_dir = tempfile::tempdir().unwrap();
        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);").unwrap();
        drop(conn);

        let info = write_sqlite_schema_recovery(
            uhoh_dir.path(), "schema_guard", &db_path, "data_change", false, 30, 100,
        ).unwrap();

        assert!(std::path::Path::new(&info.path).exists());
        let content = std::fs::read_to_string(&info.path).unwrap();
        assert!(content.contains("CREATE TABLE users"));
    }

    #[test]
    fn write_sqlite_schema_recovery_encrypted() {
        let uhoh_dir = tempfile::tempdir().unwrap();
        load_or_create_machine_key(uhoh_dir.path()).unwrap();

        let db_dir = tempfile::tempdir().unwrap();
        let db_path = db_dir.path().join("test.db");
        let conn = rusqlite::Connection::open(&db_path).unwrap();
        conn.execute_batch("CREATE TABLE t (id INT);").unwrap();
        drop(conn);

        let info = write_sqlite_schema_recovery(
            uhoh_dir.path(), "enc_schema", &db_path, "change", true, 30, 100,
        ).unwrap();

        let content = std::fs::read(&info.path).unwrap();
        assert!(content.starts_with(ENC_MAGIC));

        // Verify roundtrip decryption
        let decrypted = decrypt_recovery_payload(&content, uhoh_dir.path()).unwrap();
        let schema = String::from_utf8(decrypted).unwrap();
        assert!(schema.contains("CREATE TABLE t"));
    }
}
