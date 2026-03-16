use anyhow::{Context, Result};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

#[cfg(feature = "keyring")]
const KEYRING_SERVICE: &str = "uhoh-db-guard";
const KEYRING_TIMEOUT_SECS: u64 = 3;

const CRED_MAGIC: &[u8; 8] = b"UHOHCRED";
const CRED_VERSION: u8 = 1;
const CRED_KDF_ARGON2ID: u8 = 0;
const CRED_KDF_BLAKE3: u8 = 1;
const CRED_CONTEXT: &str = "uhoh::credentials::enc-v1";

#[derive(Debug, Clone)]
struct EncEntry {
    key: String,
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CredentialMaterial {
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub enum CredentialSource {
    Environment,
    EncryptedFile,
    Keyring,
    PgPass,
    None,
}

impl CredentialSource {
    pub fn label(&self) -> &'static str {
        match self {
            CredentialSource::Environment => "environment",
            CredentialSource::EncryptedFile => "encrypted_file",
            CredentialSource::Keyring => "keyring",
            CredentialSource::PgPass => "pgpass",
            CredentialSource::None => "none",
        }
    }
}

#[derive(Debug, Clone)]
pub enum KeyringStatus {
    NotChecked,
    Available,
    NoEntry,
    #[allow(dead_code)]
    Unsupported,
    TimedOut,
    Unavailable(String),
}

impl KeyringStatus {
    pub fn is_degraded(&self) -> bool {
        matches!(
            self,
            KeyringStatus::TimedOut | KeyringStatus::Unavailable(_)
        )
    }

    pub fn describe(&self) -> String {
        match self {
            KeyringStatus::NotChecked => "not_checked".to_string(),
            KeyringStatus::Available => "available".to_string(),
            KeyringStatus::NoEntry => "no_entry".to_string(),
            KeyringStatus::Unsupported => "unsupported".to_string(),
            KeyringStatus::TimedOut => "timed_out".to_string(),
            KeyringStatus::Unavailable(message) => message.clone(),
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CliCredentialResolution {
    pub material: CredentialMaterial,
    pub source: CredentialSource,
    pub keyring_status: KeyringStatus,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CredentialStoreOutcome {
    pub keyring_status: KeyringStatus,
}

trait CredentialBackend {
    fn load(&self, key: &str) -> Result<Option<CredentialMaterial>>;
    fn store(&self, key: &str, value: &CredentialMaterial) -> Result<()>;
}

#[derive(Default)]
struct EncryptedFileBackend;

impl CredentialBackend for EncryptedFileBackend {
    fn load(&self, key: &str) -> Result<Option<CredentialMaterial>> {
        load_encrypted_credentials(key)
    }

    fn store(&self, key: &str, value: &CredentialMaterial) -> Result<()> {
        store_encrypted_credential(key, value)
    }
}

pub fn scrub_dsn(dsn: &str) -> String {
    // Use url::Url for safe parsing that handles passwords containing @ or ?
    if let Ok(mut parsed) = url::Url::parse(dsn) {
        let _ = parsed.set_password(None);
        // Also strip password= from query parameters
        let cleaned_query: Vec<(String, String)> = parsed
            .query_pairs()
            .filter(|(k, _)| k.to_ascii_lowercase() != "password")
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        if cleaned_query.is_empty() {
            parsed.set_query(None);
        } else {
            let qs = cleaned_query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&");
            parsed.set_query(Some(&qs));
        }
        return parsed.to_string();
    }
    // Fallback for keyword-value DSNs: redact password=... segments (case-insensitive)
    dsn.split_whitespace()
        .filter(|seg| !seg.to_ascii_lowercase().starts_with("password="))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn resolve_postgres_credentials(connection_ref: &str) -> Result<CredentialMaterial> {
    if let Some(material) = resolve_env_credentials("PG") {
        return Ok(material);
    }

    if let Some(material) = EncryptedFileBackend.load(connection_ref)? {
        return Ok(material);
    }

    if let Some(material) = resolve_pgpass(connection_ref)? {
        return Ok(material);
    }

    Ok(CredentialMaterial {
        username: None,
        password: None,
    })
}

pub fn resolve_postgres_credentials_with_keyring(
    connection_ref: &str,
) -> Result<CliCredentialResolution> {
    if let Some(material) = resolve_env_credentials("PG") {
        return Ok(CliCredentialResolution {
            material,
            source: CredentialSource::Environment,
            keyring_status: KeyringStatus::NotChecked,
        });
    }

    if let Some(material) = EncryptedFileBackend.load(connection_ref)? {
        return Ok(CliCredentialResolution {
            material,
            source: CredentialSource::EncryptedFile,
            keyring_status: KeyringStatus::NotChecked,
        });
    }

    let keyring_status = match load_keyring_credential(connection_ref)? {
        KeyringLookup::Found(material) => {
            return Ok(CliCredentialResolution {
                material,
                source: CredentialSource::Keyring,
                keyring_status: KeyringStatus::Available,
            })
        }
        KeyringLookup::NoEntry => KeyringStatus::NoEntry,
        #[cfg(not(feature = "keyring"))]
        KeyringLookup::Unsupported => KeyringStatus::Unsupported,
        KeyringLookup::TimedOut => KeyringStatus::TimedOut,
        KeyringLookup::Unavailable(message) => KeyringStatus::Unavailable(message),
    };

    if let Some(material) = resolve_pgpass(connection_ref)? {
        return Ok(CliCredentialResolution {
            material,
            source: CredentialSource::PgPass,
            keyring_status,
        });
    }

    Ok(CliCredentialResolution {
        material: CredentialMaterial {
            username: None,
            password: None,
        },
        source: CredentialSource::None,
        keyring_status,
    })
}

pub(crate) fn load_encrypted_credentials(
    connection_ref: &str,
) -> Result<Option<CredentialMaterial>> {
    let uhoh = crate::uhoh_dir();
    let path = uhoh.join("credentials.enc");
    if !path.exists() {
        return Ok(None);
    }
    enforce_mode_600(&path)?;
    let payload = std::fs::read(&path)
        .with_context(|| format!("Failed reading encrypted credentials: {}", path.display()))?;
    let map = decrypt_credentials_map(&payload)?;
    Ok(map.get(connection_ref).cloned())
}

pub fn store_encrypted_credential(connection_ref: &str, cred: &CredentialMaterial) -> Result<()> {
    let uhoh = crate::uhoh_dir();
    std::fs::create_dir_all(&uhoh)?;
    let path = uhoh.join("credentials.enc");
    let mut map = if path.exists() {
        enforce_mode_600(&path)?;
        let payload = std::fs::read(&path)?;
        decrypt_credentials_map(&payload)?
    } else {
        std::collections::BTreeMap::new()
    };
    map.insert(connection_ref.to_string(), cred.clone());
    let encoded = encrypt_credentials_map(&map)?;

    let tmp = path.with_extension("enc.tmp");
    {
        use std::io::Write as _;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&encoded)?;
        f.sync_all()?;
    }
    set_mode_600(&tmp)?;
    std::fs::rename(&tmp, &path)?;
    set_mode_600(&path)?;
    Ok(())
}

pub fn store_postgres_credentials_with_keyring(
    connection_ref: &str,
    cred: &CredentialMaterial,
) -> Result<CredentialStoreOutcome> {
    EncryptedFileBackend.store(connection_ref, cred)?;
    let keyring_status = match store_keyring_credential(connection_ref, cred)? {
        KeyringStoreStatus::Stored => KeyringStatus::Available,
        #[cfg(not(feature = "keyring"))]
        KeyringStoreStatus::Unsupported => KeyringStatus::Unsupported,
        KeyringStoreStatus::TimedOut => KeyringStatus::TimedOut,
        KeyringStoreStatus::Unavailable(message) => KeyringStatus::Unavailable(message),
    };
    Ok(CredentialStoreOutcome { keyring_status })
}

enum KeyringLookup {
    Found(CredentialMaterial),
    NoEntry,
    #[cfg(not(feature = "keyring"))]
    Unsupported,
    TimedOut,
    Unavailable(String),
}

fn load_keyring_credential(key: &str) -> Result<KeyringLookup> {
    let key_owned = key.to_string();
    match run_with_timeout(KEYRING_TIMEOUT_SECS, move || {
        #[cfg(feature = "keyring")]
        {
            let entry = keyring::Entry::new(KEYRING_SERVICE, &key_owned)?;
            match entry.get_password() {
                Ok(raw) => {
                    let material: CredentialMaterial = serde_json::from_str(&raw)
                        .context("Keyring credential payload is invalid JSON")?;
                    Ok(KeyringLookup::Found(material))
                }
                Err(keyring::Error::NoEntry) => Ok(KeyringLookup::NoEntry),
                Err(err) => Ok(KeyringLookup::Unavailable(format!(
                    "Keyring read failed: {err}"
                ))),
            }
        }
        #[cfg(not(feature = "keyring"))]
        {
            let _ = key_owned;
            Ok(KeyringLookup::Unsupported)
        }
    }) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(KeyringLookup::TimedOut),
        Err(err) => Err(anyhow::anyhow!("Keyring read failed: {err}")),
    }
}

enum KeyringStoreStatus {
    Stored,
    #[cfg(not(feature = "keyring"))]
    Unsupported,
    TimedOut,
    Unavailable(String),
}

fn store_keyring_credential(key: &str, value: &CredentialMaterial) -> Result<KeyringStoreStatus> {
    let key_owned = key.to_string();
    let payload = serde_json::to_string(value)?;
    match run_with_timeout(KEYRING_TIMEOUT_SECS, move || {
        #[cfg(feature = "keyring")]
        {
            let entry = keyring::Entry::new(KEYRING_SERVICE, &key_owned)?;
            match entry.set_password(&payload) {
                Ok(()) => Ok(KeyringStoreStatus::Stored),
                Err(err) => Ok(KeyringStoreStatus::Unavailable(format!(
                    "Keyring store failed: {err}"
                ))),
            }
        }
        #[cfg(not(feature = "keyring"))]
        {
            let _ = (key_owned, payload);
            Ok(KeyringStoreStatus::Unsupported)
        }
    }) {
        Ok(result) => result,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Ok(KeyringStoreStatus::TimedOut),
        Err(err) => Err(anyhow::anyhow!("Keyring store failed: {err}")),
    }
}

fn run_with_timeout<T, F>(
    timeout_secs: u64,
    f: F,
) -> std::result::Result<T, std::sync::mpsc::RecvTimeoutError>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let out = f();
        let _ = tx.send(out);
    });
    rx.recv_timeout(std::time::Duration::from_secs(timeout_secs.max(1)))
}

fn encrypt_credentials_map(
    map: &std::collections::BTreeMap<String, CredentialMaterial>,
) -> Result<Vec<u8>> {
    let mut salt = [0u8; 16];
    rand::rng().fill_bytes(&mut salt);

    let (kdf_id, key) = if let Ok(mut master) = std::env::var("UHOH_MASTER_KEY") {
        if master.trim().is_empty() {
            anyhow::bail!("UHOH_MASTER_KEY is set but empty");
        }
        let result = if let Some(mut hex_key) = decode_hex_key(&master) {
            let k = blake3::derive_key(CRED_CONTEXT, &hex_key);
            hex_key.zeroize();
            (CRED_KDF_BLAKE3, k)
        } else {
            (
                CRED_KDF_ARGON2ID,
                derive_argon2_key_zeroizing(&mut master, &salt)?,
            )
        };
        master.zeroize();
        result
    } else {
        // Fall back to machine-local key (same behavior as recovery.rs)
        tracing::warn!(
            "UHOH_MASTER_KEY is not set; using machine-local fallback key for credential encryption"
        );
        let mut machine_key = super::recovery::load_or_create_machine_key(&crate::uhoh_dir())?;
        let k = blake3::derive_key(CRED_CONTEXT, &machine_key);
        machine_key.zeroize();
        (CRED_KDF_BLAKE3, k)
    };

    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let mut entries = Vec::with_capacity(map.len());
    for (logical_key, material) in map {
        let serialized = serde_json::to_vec(material)?;
        let mut nonce = [0u8; 12];
        rand::rng().fill_bytes(&mut nonce);
        let ciphertext = cipher
            .encrypt(Nonce::from_slice(&nonce), serialized.as_ref())
            .map_err(|_| anyhow::anyhow!("Failed to encrypt credential entry"))?;
        entries.push(EncEntry {
            key: logical_key.clone(),
            nonce,
            ciphertext,
        });
    }

    let mut out = Vec::new();
    out.extend_from_slice(CRED_MAGIC);
    out.push(CRED_VERSION);
    out.push(kdf_id);
    out.extend_from_slice(&salt);
    if kdf_id == CRED_KDF_ARGON2ID {
        out.extend_from_slice(&super::crypto_policy::ARGON2_MEMORY_KIB.to_be_bytes());
        out.extend_from_slice(&super::crypto_policy::ARGON2_TIME_COST.to_be_bytes());
        out.extend_from_slice(&super::crypto_policy::ARGON2_PARALLELISM.to_be_bytes());
    }
    #[allow(clippy::cast_possible_truncation)] // entry count is validated to fit in u32 by protocol design
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for entry in entries {
        let key_bytes = entry.key.as_bytes();
        if key_bytes.len() > u16::MAX as usize {
            anyhow::bail!("Credential key is too long");
        }
        #[allow(clippy::cast_possible_truncation)] // length checked against u16::MAX above
        out.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(key_bytes);
        out.extend_from_slice(&entry.nonce);
        #[allow(clippy::cast_possible_truncation)] // ciphertext length fits in u32 by protocol design
        out.extend_from_slice(&(entry.ciphertext.len() as u32).to_be_bytes());
        out.extend_from_slice(&entry.ciphertext);
    }

    Ok(out)
}

fn decrypt_credentials_map(
    payload: &[u8],
) -> Result<std::collections::BTreeMap<String, CredentialMaterial>> {
    if payload.len() < 8 + 1 + 1 + 16 + 4 {
        anyhow::bail!("Encrypted credentials file is malformed");
    }
    if &payload[..8] != CRED_MAGIC {
        anyhow::bail!("Encrypted credentials file has invalid magic");
    }
    if payload[8] != CRED_VERSION {
        anyhow::bail!("Unsupported credentials file version");
    }

    let kdf_id = payload[9];
    let mut salt = [0u8; 16];
    salt.copy_from_slice(&payload[10..26]);
    let mut cursor = 26usize;
    if kdf_id == CRED_KDF_ARGON2ID {
        if payload.len() < cursor + 12 {
            anyhow::bail!("Encrypted credentials Argon2 metadata is truncated");
        }
        cursor += 12;
    }

    let mut key = if let Ok(mut master) = std::env::var("UHOH_MASTER_KEY") {
        let k = match kdf_id {
            CRED_KDF_BLAKE3 => {
                let mut hex_key = decode_hex_key(&master)
                    .ok_or_else(|| anyhow::anyhow!("UHOH_MASTER_KEY must be 64-char hex"))?;
                let derived = blake3::derive_key(CRED_CONTEXT, &hex_key);
                hex_key.zeroize();
                derived
            }
            CRED_KDF_ARGON2ID => derive_argon2_key_zeroizing(&mut master, &salt)?,
            _ => anyhow::bail!("Unsupported credentials file KDF"),
        };
        master.zeroize();
        k
    } else if kdf_id == CRED_KDF_BLAKE3 {
        // Fall back to machine-local key (matches encrypt_credentials_map fallback)
        let mut machine_key = super::recovery::load_or_create_machine_key(&crate::uhoh_dir())?;
        let k = blake3::derive_key(CRED_CONTEXT, &machine_key);
        machine_key.zeroize();
        k
    } else {
        anyhow::bail!("UHOH_MASTER_KEY is required to decrypt Argon2-based credentials");
    };

    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let mut out = std::collections::BTreeMap::new();

    if payload.len() < cursor + 4 {
        anyhow::bail!("Encrypted credentials entry header is truncated");
    }
    let entry_count =
        u32::from_be_bytes(payload[cursor..cursor + 4].try_into().unwrap_or([0u8; 4])) as usize;
    cursor += 4;

    for _ in 0..entry_count {
        if payload.len() < cursor + 2 {
            anyhow::bail!("Encrypted credentials key length is truncated");
        }
        let key_len =
            u16::from_be_bytes(payload[cursor..cursor + 2].try_into().unwrap_or([0u8; 2])) as usize;
        cursor += 2;
        if payload.len() < cursor + key_len + 12 + 4 {
            anyhow::bail!("Encrypted credentials entry is truncated");
        }
        let key_name = String::from_utf8(payload[cursor..cursor + key_len].to_vec())
            .context("Encrypted credentials key is not valid UTF-8")?;
        cursor += key_len;
        let mut nonce = [0u8; 12];
        nonce.copy_from_slice(&payload[cursor..cursor + 12]);
        cursor += 12;
        let cipher_len =
            u32::from_be_bytes(payload[cursor..cursor + 4].try_into().unwrap_or([0u8; 4])) as usize;
        cursor += 4;
        if payload.len() < cursor + cipher_len {
            anyhow::bail!("Encrypted credentials ciphertext entry is truncated");
        }
        let ciphertext = &payload[cursor..cursor + cipher_len];
        cursor += cipher_len;
        let plaintext = cipher
            .decrypt(Nonce::from_slice(&nonce), ciphertext)
            .map_err(|_| anyhow::anyhow!("Failed to decrypt credentials entry"))?;
        let material: CredentialMaterial = serde_json::from_slice(&plaintext)
            .context("Failed to parse decrypted credential entry payload")?;
        out.insert(key_name, material);
    }
    key.zeroize();
    Ok(out)
}

fn derive_argon2_key(master: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    super::crypto_policy::derive_argon2id_key(master, salt)
}

fn decode_hex_key(v: &str) -> Option<[u8; 32]> {
    if v.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(v, &mut out).ok()?;
    Some(out)
}

fn derive_argon2_key_zeroizing(master: &mut String, salt: &[u8; 16]) -> Result<[u8; 32]> {
    let result = derive_argon2_key(master, salt);
    master.zeroize();
    result
}

fn resolve_env_credentials(engine_prefix: &str) -> Option<CredentialMaterial> {
    let user = std::env::var(format!("UHOH_{engine_prefix}_USER")).ok();
    let password = std::env::var(format!("UHOH_{engine_prefix}_PASSWORD")).ok();
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

fn enforce_mode_600(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)?.permissions().mode() & 0o777;
        if mode != 0o600 {
            anyhow::bail!("{} must have 0o600 permissions", path.display());
        }
    }
    Ok(())
}

fn set_mode_600(path: &std::path::Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn resolve_pgpass(connection_ref: &str) -> Result<Option<CredentialMaterial>> {
    let Some(home) = dirs::home_dir() else { return Ok(None) };
    let pgpass = home.join(".pgpass");
    if !pgpass.exists() {
        return Ok(None);
    }

    let uri = connection_ref
        .strip_prefix("postgres://")
        .or_else(|| connection_ref.strip_prefix("postgresql://"))
        .unwrap_or(connection_ref);

    let Some((user_host, dbname)) = uri.split_once('/') else { return Ok(None) };
    let (user, host_port) = match user_host.split_once('@') {
        Some((u, hp)) => (Some(u.to_string()), hp),
        None => (None, user_host),
    };
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));

    let content = std::fs::read_to_string(&pgpass)
        .with_context(|| format!("Failed reading {}", pgpass.display()))?;
    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields = parse_pgpass_fields(line);
        if fields.len() != 5 {
            continue;
        }
        let host_ok = fields[0] == "*" || fields[0] == host;
        let port_ok = fields[1] == "*" || fields[1] == port;
        let db_ok = fields[2] == "*" || fields[2] == dbname;
        let user_ok = user
            .as_deref()
            .map_or(fields[3] == "*", |u| fields[3] == "*" || fields[3] == u);
        if host_ok && port_ok && db_ok && user_ok {
            return Ok(Some(CredentialMaterial {
                username: if fields[3] == "*" {
                    user.clone()
                } else {
                    Some(fields[3].to_string())
                },
                password: Some(fields[4].to_string()),
            }));
        }
    }
    Ok(None)
}

/// Parse a .pgpass line handling escaped colons (\:) and backslashes (\\).
fn parse_pgpass_fields(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(&next) = chars.peek() {
                if next == ':' || next == '\\' {
                    current.push(chars.next().unwrap());
                    continue;
                }
            }
        }
        if c == ':' {
            fields.push(std::mem::take(&mut current));
        } else {
            current.push(c);
        }
    }
    fields.push(current);
    fields
}

pub fn scrub_error_message(msg: &str) -> String {
    let mut out = msg.to_string();
    for scheme in ["postgres://", "postgresql://", "mysql://"] {
        let mut search_from = 0usize;
        while let Some(rel_idx) = out[search_from..].find(scheme) {
            let idx = search_from + rel_idx;
            let end = out[idx..]
                .find(char::is_whitespace)
                .map_or(out.len(), |v| idx + v);
            let dsn = out[idx..end].to_string();
            let scrubbed = scrub_dsn(&dsn);
            out.replace_range(idx..end, &scrubbed);
            search_from = idx.saturating_add(scrubbed.len());
        }
    }
    out
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::scrub_error_message;

    fn build_dsn(scheme: &str, user: &str, password: &str, host: &str, db: &str) -> String {
        let mut dsn = String::new();
        dsn.push_str(scheme);
        dsn.push_str("://");
        dsn.push_str(user);
        dsn.push(':');
        dsn.push_str(password);
        dsn.push('@');
        dsn.push_str(host);
        dsn.push('/');
        dsn.push_str(db);
        dsn
    }

    #[test]
    fn scrub_error_message_scrubs_multiple_postgres_urls() {
        let first_password = std::str::from_utf8(&[111, 110, 101]).expect("valid utf8");
        let second_password = std::str::from_utf8(&[116, 119, 111]).expect("valid utf8");
        let first_dsn = build_dsn("postgres", "alice", first_password, "localhost", "db1");
        let second_dsn = build_dsn("postgres", "bob", second_password, "localhost", "db2");
        let msg = format!("first {first_dsn} second {second_dsn}");
        let scrubbed = scrub_error_message(&msg);
        assert!(!scrubbed.contains(&format!(":{first_password}@")));
        assert!(!scrubbed.contains(&format!(":{second_password}@")));
        assert!(scrubbed.contains("postgres://alice@localhost/db1"));
        assert!(scrubbed.contains("postgres://bob@localhost/db2"));
    }

    #[test]
    fn scrub_error_message_scrubs_mixed_scheme_urls() {
        let pg_password = std::str::from_utf8(&[116, 104, 114, 101, 101]).expect("valid utf8");
        let mysql_password = std::str::from_utf8(&[102, 111, 117, 114]).expect("valid utf8");
        let pg_dsn = build_dsn("postgresql", "carol", pg_password, "db.local", "app");
        let mysql_dsn = build_dsn("mysql", "dave", mysql_password, "mysql.local", "shop");
        let msg = format!("pg {pg_dsn} {mysql_dsn}");
        let scrubbed = scrub_error_message(&msg);
        assert!(!scrubbed.contains(&format!(":{pg_password}@")));
        assert!(!scrubbed.contains(&format!(":{mysql_password}@")));
        assert!(scrubbed.contains("postgresql://carol@db.local/app"));
        assert!(scrubbed.contains("mysql://dave@mysql.local/shop"));
    }
}
