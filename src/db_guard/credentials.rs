use anyhow::{Context, Result};
use argon2::{Algorithm, Argon2, Params, Version};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use zeroize::Zeroize;

const CRED_MAGIC: &[u8; 8] = b"UHOHCRED";
const CRED_VERSION: u8 = 1;
const CRED_KDF_ARGON2ID: u8 = 0;
const CRED_KDF_BLAKE3: u8 = 1;
const CRED_CONTEXT: &str = "uhoh::credentials::enc-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CredentialMaterial {
    pub username: Option<String>,
    pub password: Option<String>,
}

pub fn scrub_dsn(dsn: &str) -> String {
    if let Some((scheme, rest)) = dsn.split_once("://") {
        let mut tail = rest.to_string();
        if let Some((userinfo, host_and_path)) = rest.split_once('@') {
            if let Some((user, _password)) = userinfo.split_once(':') {
                tail = format!("{user}@{host_and_path}");
            }
        }
        if let Some((base, _)) = tail.split_once("?password=") {
            return format!("{scheme}://{base}");
        }
        return format!("{scheme}://{tail}");
    }
    dsn.to_string()
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
    if let Some(material) = resolve_encrypted_credentials(connection_ref)? {
        return Ok(material);
    }

    if let Some(material) = resolve_pgpass(connection_ref)? {
        return Ok(material);
    }

    if let Ok(master) = std::env::var("UHOH_MASTER_KEY") {
        if !master.trim().is_empty() {
            return Ok(CredentialMaterial {
                username: None,
                password: Some(master),
            });
        }
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

    let serialized = serde_json::to_vec(map)?;
    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let mut nonce = [0u8; 12];
    rand::thread_rng().fill_bytes(&mut nonce);
    let ciphertext = cipher
        .encrypt(Nonce::from_slice(&nonce), serialized.as_ref())
        .map_err(|_| anyhow::anyhow!("Failed to encrypt credentials"))?;

    let mut out = Vec::new();
    out.extend_from_slice(CRED_MAGIC);
    out.push(CRED_VERSION);
    out.push(kdf_id);
    out.extend_from_slice(&salt);
    out.extend_from_slice(&nonce);
    out.extend_from_slice(&(ciphertext.len() as u32).to_be_bytes());
    out.extend_from_slice(&ciphertext);

    master.zeroize();
    Ok(out)
}

fn decrypt_credentials_map(
    payload: &[u8],
) -> Result<std::collections::BTreeMap<String, CredentialMaterial>> {
    if payload.len() < 8 + 1 + 1 + 16 + 12 + 4 {
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
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&payload[26..38]);
    let len = u32::from_be_bytes(payload[38..42].try_into().unwrap_or([0u8; 4])) as usize;
    if payload.len() < 42 + len {
        anyhow::bail!("Encrypted credentials ciphertext is truncated");
    }
    let ciphertext = &payload[42..42 + len];

    let mut master = std::env::var("UHOH_MASTER_KEY")
        .context("UHOH_MASTER_KEY is required to read encrypted credentials")?;
    let key = match kdf_id {
        CRED_KDF_BLAKE3 => {
            let hex_key = decode_hex_key(&master)
                .ok_or_else(|| anyhow::anyhow!("UHOH_MASTER_KEY must be 64-char hex"))?;
            blake3::derive_key(CRED_CONTEXT, &hex_key)
        }
        CRED_KDF_ARGON2ID => derive_argon2_key(&master, &salt)?,
        _ => anyhow::bail!("Unsupported credentials file KDF"),
    };

    let cipher = ChaCha20Poly1305::new(Key::from_slice(&key));
    let plaintext = cipher
        .decrypt(Nonce::from_slice(&nonce), ciphertext)
        .map_err(|_| anyhow::anyhow!("Failed to decrypt credentials"))?;
    master.zeroize();

    let out = serde_json::from_slice(&plaintext)
        .context("Failed to parse decrypted credentials payload")?;
    Ok(out)
}

fn derive_argon2_key(master: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    let params = Params::new(19 * 1024, 2, 1, Some(32))
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
