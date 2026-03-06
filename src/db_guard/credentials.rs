use anyhow::{Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
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
const ARGON2_MEMORY_KIB: u32 = 19 * 1024;
const ARGON2_TIME_COST: u32 = 2;
const ARGON2_PARALLELISM: u32 = 1;

#[derive(Debug, Clone)]
struct EncEntry {
    key: String,
    nonce: [u8; 12],
    ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialMaterial {
    pub username: Option<String>,
    pub password: Option<String>,
}

trait CredentialBackend {
    fn load(&self, key: &str) -> Result<Option<CredentialMaterial>>;
    fn store(&self, key: &str, value: &CredentialMaterial) -> Result<()>;
}

#[derive(Default)]
struct EncryptedFileBackend;

impl CredentialBackend for EncryptedFileBackend {
    fn load(&self, key: &str) -> Result<Option<CredentialMaterial>> {
        resolve_encrypted_credentials(key)
    }

    fn store(&self, key: &str, value: &CredentialMaterial) -> Result<()> {
        store_encrypted_credential(key, value)
    }
}

#[derive(Default)]
struct KeyringBackend;

impl CredentialBackend for KeyringBackend {
    fn load(&self, key: &str) -> Result<Option<CredentialMaterial>> {
        load_keyring_credential(key)
    }

    fn store(&self, key: &str, value: &CredentialMaterial) -> Result<()> {
        store_keyring_credential(key, value)
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
    // Fallback for keyword-value DSNs: redact password=... segments
    dsn.split_whitespace()
        .filter(|seg| !seg.starts_with("password="))
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn ensure_guard_dir(uhoh_dir: &std::path::Path) -> Result<std::path::PathBuf> {
    let dir = uhoh_dir.join("db_guard");
    std::fs::create_dir_all(&dir).context("Failed to create db_guard directory")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
    }
    Ok(dir)
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

pub fn resolve_postgres_credentials_cli(connection_ref: &str) -> Result<CredentialMaterial> {
    if let Some(material) = resolve_env_credentials("PG") {
        return Ok(material);
    }

    if let Some(material) = EncryptedFileBackend.load(connection_ref)? {
        return Ok(material);
    }

    if let Some(material) = KeyringBackend.load(connection_ref)? {
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

fn resolve_encrypted_credentials(connection_ref: &str) -> Result<Option<CredentialMaterial>> {
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

pub fn resolve_stored_credentials(connection_ref: &str) -> Result<Option<CredentialMaterial>> {
    resolve_encrypted_credentials(connection_ref)
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
        let mut f = std::fs::File::create(&tmp)?;
        use std::io::Write as _;
        f.write_all(&encoded)?;
        f.sync_all()?;
    }
    set_mode_600(&tmp)?;
    std::fs::rename(&tmp, &path)?;
    set_mode_600(&path)?;
    Ok(())
}

pub fn store_postgres_credentials_cli(
    connection_ref: &str,
    cred: &CredentialMaterial,
) -> Result<()> {
    EncryptedFileBackend.store(connection_ref, cred)?;
    if let Err(err) = KeyringBackend.store(connection_ref, cred) {
        tracing::warn!(
            "Keyring store failed for '{}': {}",
            scrub_dsn(connection_ref),
            err
        );
    }
    Ok(())
}

fn load_keyring_credential(key: &str) -> Result<Option<CredentialMaterial>> {
    let key_owned = key.to_string();
    run_with_timeout(KEYRING_TIMEOUT_SECS, move || {
        #[cfg(feature = "keyring")]
        {
            let entry = keyring::Entry::new(KEYRING_SERVICE, &key_owned)?;
            match entry.get_password() {
                Ok(raw) => {
                    let material: CredentialMaterial = serde_json::from_str(&raw)
                        .context("Keyring credential payload is invalid JSON")?;
                    Ok(Some(material))
                }
                Err(keyring::Error::NoEntry) => Ok(None),
                Err(err) => Err(anyhow::anyhow!("Keyring read failed: {err}")),
            }
        }
        #[cfg(not(feature = "keyring"))]
        {
            let _ = key_owned;
            Ok(None)
        }
    })
    .unwrap_or_else(|_| Ok(None))
}

fn store_keyring_credential(key: &str, value: &CredentialMaterial) -> Result<()> {
    let key_owned = key.to_string();
    let payload = serde_json::to_string(value)?;
    run_with_timeout(KEYRING_TIMEOUT_SECS, move || {
        #[cfg(feature = "keyring")]
        {
            let entry = keyring::Entry::new(KEYRING_SERVICE, &key_owned)?;
            entry.set_password(&payload)?;
            Ok(())
        }
        #[cfg(not(feature = "keyring"))]
        {
            let _ = (key_owned, payload);
            Ok(())
        }
    })
    .unwrap_or(Ok(()))
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
    let mut master = std::env::var("UHOH_MASTER_KEY")
        .context("UHOH_MASTER_KEY is required to write encrypted credentials")?;
    if master.trim().is_empty() {
        anyhow::bail!("UHOH_MASTER_KEY is required to write encrypted credentials");
    }

    let mut salt = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut salt);
    let (kdf_id, key) = if let Some(mut hex_key) = decode_hex_key(&master) {
        let k = blake3::derive_key(CRED_CONTEXT, &hex_key);
        hex_key.zeroize();
        (CRED_KDF_BLAKE3, k)
    } else {
        (CRED_KDF_ARGON2ID, derive_argon2_key(&master, &salt)?)
    };

    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let mut entries = Vec::with_capacity(map.len());
    for (logical_key, material) in map {
        let serialized = serde_json::to_vec(material)?;
        let mut nonce = [0u8; 12];
        rand::thread_rng().fill_bytes(&mut nonce);
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
        out.extend_from_slice(&ARGON2_MEMORY_KIB.to_be_bytes());
        out.extend_from_slice(&ARGON2_TIME_COST.to_be_bytes());
        out.extend_from_slice(&ARGON2_PARALLELISM.to_be_bytes());
    }
    out.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for entry in entries {
        let key_bytes = entry.key.as_bytes();
        if key_bytes.len() > u16::MAX as usize {
            anyhow::bail!("Credential key is too long");
        }
        out.extend_from_slice(&(key_bytes.len() as u16).to_be_bytes());
        out.extend_from_slice(key_bytes);
        out.extend_from_slice(&entry.nonce);
        out.extend_from_slice(&(entry.ciphertext.len() as u32).to_be_bytes());
        out.extend_from_slice(&entry.ciphertext);
    }

    master.zeroize();
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

    let mut master = std::env::var("UHOH_MASTER_KEY")
        .context("UHOH_MASTER_KEY is required to read encrypted credentials")?;
    let mut key = match kdf_id {
        CRED_KDF_BLAKE3 => {
            let mut hex_key = decode_hex_key(&master)
                .ok_or_else(|| anyhow::anyhow!("UHOH_MASTER_KEY must be 64-char hex"))?;
            let derived = blake3::derive_key(CRED_CONTEXT, &hex_key);
            hex_key.zeroize();
            derived
        }
        CRED_KDF_ARGON2ID => derive_argon2_key(&master, &salt)?,
        _ => anyhow::bail!("Unsupported credentials file KDF"),
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
    master.zeroize();
    key.zeroize();
    Ok(out)
}

fn derive_argon2_key(master: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    let params = Params::new(
        ARGON2_MEMORY_KIB,
        ARGON2_TIME_COST,
        ARGON2_PARALLELISM,
        Some(32),
    )
    .map_err(|e| anyhow::anyhow!("Invalid Argon2 params: {e}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut out = [0u8; 32];
    argon2
        .hash_password_into(master.as_bytes(), salt, &mut out)
        .map_err(|e| anyhow::anyhow!("Argon2 key derivation failed: {e}"))?;
    Ok(out)
}

fn decode_hex_key(v: &str) -> Option<[u8; 32]> {
    if v.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(v, &mut out).ok()?;
    Some(out)
}

fn resolve_env_credentials(engine_prefix: &str) -> Option<CredentialMaterial> {
    let user = std::env::var(format!("UHOH_{}_USER", engine_prefix)).ok();
    let password = std::env::var(format!("UHOH_{}_PASSWORD", engine_prefix)).ok();
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
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return Ok(None),
    };
    let pgpass = home.join(".pgpass");
    if !pgpass.exists() {
        return Ok(None);
    }

    let uri = connection_ref
        .strip_prefix("postgres://")
        .or_else(|| connection_ref.strip_prefix("postgresql://"))
        .unwrap_or(connection_ref);

    let (user_host, dbname) = match uri.split_once('/') {
        Some(parts) => parts,
        None => return Ok(None),
    };
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
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() != 5 {
            continue;
        }
        let host_ok = fields[0] == "*" || fields[0] == host;
        let port_ok = fields[1] == "*" || fields[1] == port;
        let db_ok = fields[2] == "*" || fields[2] == dbname;
        let user_ok = user
            .as_deref()
            .map(|u| fields[3] == "*" || fields[3] == u)
            .unwrap_or(fields[3] == "*");
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

pub fn scrub_error_message(msg: &str) -> String {
    let mut out = msg.to_string();
    if let Some(idx) = out.find("postgres://") {
        let end = out[idx..]
            .find(char::is_whitespace)
            .map(|v| idx + v)
            .unwrap_or(out.len());
        let dsn = &out[idx..end];
        out.replace_range(idx..end, &scrub_dsn(dsn));
    }
    if let Some(idx) = out.find("postgresql://") {
        let end = out[idx..]
            .find(char::is_whitespace)
            .map(|v| idx + v)
            .unwrap_or(out.len());
        let dsn = &out[idx..end];
        out.replace_range(idx..end, &scrub_dsn(dsn));
    }
    if let Some(idx) = out.find("mysql://") {
        let end = out[idx..]
            .find(char::is_whitespace)
            .map(|v| idx + v)
            .unwrap_or(out.len());
        let dsn = &out[idx..end];
        out.replace_range(idx..end, &scrub_dsn(dsn));
    }
    out
}
