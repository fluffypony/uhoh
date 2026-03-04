use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::Path;

/// How a blob was stored in the CAS.
/// Numeric values can be persisted in the DB if needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum StorageMethod {
    None = 0,
    Copy = 1,
    Reflink = 2,
    Hardlink = 3,
}

impl StorageMethod {
    pub fn is_recoverable(self) -> bool {
        !matches!(self, StorageMethod::None)
    }
    pub fn to_db(self) -> i64 { self as i64 }
    pub fn from_db(v: i64) -> Self {
        match v {
            1 => StorageMethod::Copy,
            2 => StorageMethod::Reflink,
            3 => StorageMethod::Hardlink,
            _ => StorageMethod::None,
        }
    }
    pub fn display_name(self) -> &'static str {
        match self {
            StorageMethod::None => "none",
            StorageMethod::Copy => "copy",
            StorageMethod::Reflink => "reflink",
            StorageMethod::Hardlink => "hardlink",
        }
    }
}

/// Store a blob in the CAS. Uses atomic write (write-to-temp, fsync, rename).
/// Returns the BLAKE3 hex hash.
pub fn store_blob(blob_root: &Path, content: &[u8]) -> Result<String> {
    let hash = blake3::hash(content).to_hex().to_string();
    let dir = blob_root.join(&hash[..hash.len().min(2)]);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create blob dir: {}", dir.display()))?;
    let blob_path = dir.join(&hash);
    if blob_path.exists() {
        return Ok(hash); // Deduplication: already stored
    }

    // Atomic write: temp file → fsync → rename
    let tmp_path = dir.join(format!(
        ".tmp.{}.{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    let data = maybe_compress(content);
    {
        let mut f = create_restricted_file(&tmp_path)
            .with_context(|| format!("Failed to write temp blob: {}", tmp_path.display()))?;
        f.write_all(&data)?;
        f.sync_all()?;
    }

    // Atomic rename
    std::fs::rename(&tmp_path, &blob_path).with_context(|| {
        format!(
            "Failed to rename blob {} -> {}",
            tmp_path.display(),
            blob_path.display()
        )
    })?;

    // Restrict permissions
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Standardize on read-only
        std::fs::set_permissions(&blob_path, std::fs::Permissions::from_mode(0o400)).ok();
    }

    Ok(hash)
}

/// Store a blob from a file path using streaming BLAKE3 and a tiered strategy.
/// Returns (hash, size, storage_method).
pub fn store_blob_from_file(
    blob_root: &Path,
    file_path: &Path,
    max_copy_blob_bytes: u64,
) -> Result<(String, u64, StorageMethod)> {
    let metadata = std::fs::metadata(file_path)
        .with_context(|| format!("Cannot stat: {}", file_path.display()))?;
    let file_size = metadata.len();

    let file = std::fs::File::open(file_path)
        .with_context(|| format!("Cannot open: {}", file_path.display()))?;
    let mut reader = std::io::BufReader::new(file);
    let mut hasher = blake3::Hasher::new();

    let mut first_chunk = vec![0u8; 8192.min(file_size as usize)];
    let first_n = reader.read(&mut first_chunk)?;
    first_chunk.truncate(first_n);
    hasher.update(&first_chunk);

    // We'll compute the hash regardless of storage method
    // Decide storage after hashing by trying reflink/hardlink/copy

    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
        // Just hashing here
    }

    let hash = hasher.finalize().to_hex().to_string();
    let dir = blob_root.join(&hash[..hash.len().min(2)]);
    std::fs::create_dir_all(&dir)?;
    let blob_path = dir.join(&hash);

    // If already exists, treat as stored via Copy for compatibility
    if blob_path.exists() {
        return Ok((hash, file_size, StorageMethod::Copy));
    }

    // Enforce size limits: decide if we should store or hash-only based on type
    let is_binary = {
        let head = &first_chunk[..first_chunk.len().min(8192)];
        content_inspector::inspect(head).is_binary()
    };
    let cfg_limit = if is_binary { crate::config::default_max_binary_blob_bytes() } else { crate::config::default_max_text_blob_bytes() } as u64;
    let effective_limit = std::cmp::min(cfg_limit, max_copy_blob_bytes);

    // Try reflink
    if reflink_copy::reflink(file_path, &blob_path).is_ok() {
        set_blob_readonly(&blob_path);
        return Ok((hash, file_size, StorageMethod::Reflink));
    }

    // Try full copy if within size limit
    if file_size <= effective_limit {
        // Write to temp then rename for atomicity
        let tmp_dir = blob_root.join("tmp");
        std::fs::create_dir_all(&tmp_dir)?;
        let tmp_path = tmp_dir.join(format!(
            ".tmp.{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::copy(file_path, &tmp_path)?;
        // TOCTOU guard: re-hash the stored bytes to ensure match
        let stored_hash = {
            let mut hasher = blake3::Hasher::new();
            let mut f = std::fs::File::open(&tmp_path)?;
            let _ = std::io::copy(&mut f, &mut hasher)?;
            hasher.finalize().to_hex().to_string()
        };
        if stored_hash != hash {
            let _ = std::fs::remove_file(&tmp_path);
            tracing::warn!("File changed during snapshot: {}", file_path.display());
            return Ok((hash, file_size, StorageMethod::None));
        }
        set_blob_readonly(&tmp_path);
        match std::fs::rename(&tmp_path, &blob_path) {
            Ok(()) => return Ok((hash, file_size, StorageMethod::Copy)),
            Err(_) => {
                // Race: someone else wrote it
                let _ = std::fs::remove_file(&tmp_path);
                if blob_path.exists() {
                    return Ok((hash, file_size, StorageMethod::Copy));
                }
            }
        }
    }

    // Could not store, return hash only
    tracing::debug!(
        "Not storing {} ({} bytes): reflink/copy failed or size exceeds limit ({} bytes)",
        file_path.display(), file_size, effective_limit
    );
    Ok((hash, file_size, StorageMethod::None))
}

/// Read a blob from the CAS with integrity verification.
pub fn read_blob(blob_root: &Path, hash: &str) -> Result<Option<Vec<u8>>> {
    if hash.len() < 2 {
        return Ok(None);
    }
    let path = blob_root.join(&hash[..hash.len().min(2)]).join(hash);
    let data = match std::fs::read(&path) {
        Ok(d) => d,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e).context("Failed to read blob"),
    };

    let content = maybe_decompress(&data)?;

    // Verify integrity (catch disk corruption / tampering)
    let actual_hash = blake3::hash(&content).to_hex().to_string();
    if actual_hash != hash {
        tracing::error!(
            "Blob corruption detected! Expected {}, got {}",
            &hash[..hash.len().min(16)],
            &actual_hash[..actual_hash.len().min(16)]
        );
        return Ok(None);
    }

    Ok(Some(content))
}

pub fn blob_exists(blob_root: &Path, hash: &str) -> bool {
    if hash.len() < 2 {
        return false;
    }
    blob_root.join(&hash[..hash.len().min(2)]).join(hash).exists()
}

fn set_blob_readonly(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o400));
    }
    #[cfg(not(unix))]
    {
        if let Ok(meta) = std::fs::metadata(path) {
            let mut perms = meta.permissions();
            perms.set_readonly(true);
            let _ = std::fs::set_permissions(path, perms);
        }
    }
}

/// Compress if feature enabled
fn maybe_compress(data: &[u8]) -> Vec<u8> {
    #[cfg(feature = "compression")]
    {
        // Prefix with a robust magic header to avoid collisions with raw data
        const COMPRESSION_MAGIC: &[u8; 4] = b"UHZS"; // "UH" + "ZS" (zstd)
        // Read config compress level if available; otherwise default to 3
        let level = {
            if let Some(home) = dirs::home_dir() {
                let cfg_path = home.join(".uhoh").join("config.toml");
                if let Ok(cfg_str) = std::fs::read_to_string(&cfg_path) {
                    if let Ok(conf) = toml::from_str::<crate::config::Config>(&cfg_str) {
                        conf.storage.compress_level
                    } else { 3 }
                } else { 3 }
            } else { 3 }
        };
        let compressed = zstd::encode_all(std::io::Cursor::new(data), level)
            .unwrap_or_else(|_| data.to_vec());
        if compressed.len() < data.len() {
            let mut out = Vec::with_capacity(compressed.len() + 4);
            out.extend_from_slice(COMPRESSION_MAGIC);
            out.extend_from_slice(&compressed);
            return out;
        }
    }
    #[cfg(not(feature = "compression"))]
    {
        data.to_vec()
    }
}

fn maybe_decompress(data: &[u8]) -> Result<Vec<u8>> {
    #[cfg(feature = "compression")]
    {
        const COMPRESSION_MAGIC: &[u8; 4] = b"UHZS";
        if data.len() > 4 && &data[..4] == COMPRESSION_MAGIC {
            return zstd::decode_all(std::io::Cursor::new(&data[4..]))
                .context("Failed to decompress blob");
        }
    }
    Ok(data.to_vec())
}

// === Base58 Snapshot ID encoding ===

pub fn id_to_base58(id: u64) -> String {
    // Note: base58 for zero is a single '1'. We never assign snapshot id 0.
    // Guard: if id==0 is somehow passed, return empty to avoid ambiguity with ID 1.
    if id == 0 { return String::new(); }
    let bytes = id.to_be_bytes();
    // Strip leading zero bytes for shorter IDs
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bs58::encode(&bytes[start..])
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_string()
}

pub fn base58_to_id(s: &str) -> Option<u64> {
    if s.is_empty() { return None; }
    let bytes = bs58::decode(s)
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_vec()
        .ok()?;
    if bytes.len() > 8 {
        return None; // Prevent panic on oversized input
    }
    let mut buf = [0u8; 8];
    let start = 8usize.saturating_sub(bytes.len());
    buf[start..].copy_from_slice(&bytes);
    let id = u64::from_be_bytes(buf);
    if id == 0 { return None; }
    Some(id)
}

/// Normalize a path to use forward slashes for cross-platform manifest storage.
pub fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Check if file is executable (Unix)
pub fn is_executable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(meta) = std::fs::metadata(path) {
            return meta.permissions().mode() & 0o111 != 0;
        }
    }
    false
}

#[cfg(unix)]
fn create_restricted_file(path: &Path) -> std::io::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)
}

#[cfg(not(unix))]
fn create_restricted_file(path: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
}

/// Estimate disk usage of a blob, accounting for hardlinks on Unix.
pub fn blob_disk_usage(blob_path: &Path) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(meta) = std::fs::metadata(blob_path) {
            return if meta.nlink() > 1 { meta.len() / meta.nlink() } else { meta.len() };
        }
        0
    }
    #[cfg(not(unix))]
    {
        std::fs::metadata(blob_path).map(|m| m.len()).unwrap_or(0)
    }
}

/// Clean up stale temp files in the blob store (from crashed processes).
/// Removes files under blobs/tmp older than the specified max_age.
pub fn cleanup_stale_temp_files(blob_root: &Path, max_age: std::time::Duration) {
    let tmp_dir = blob_root.join("tmp");
    if !tmp_dir.exists() {
        return;
    }
    let now = std::time::SystemTime::now();
    if let Ok(entries) = std::fs::read_dir(&tmp_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with(".tmp.") {
                    if let Ok(meta) = std::fs::metadata(&path) {
                        if let Ok(mtime) = meta.modified() {
                            if let Ok(age) = now.duration_since(mtime) {
                                if age > max_age {
                                    let _ = std::fs::remove_file(&path);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base58_roundtrip() {
        for id in [1, 42, 255, 1000, u64::MAX] {
            let encoded = id_to_base58(id);
            let decoded = base58_to_id(&encoded);
            assert_eq!(decoded, Some(id), "Failed roundtrip for id={}", id);
        }
    }

    #[test]
    fn test_base58_short_ids() {
        // ID 1 should produce a short string, not an 8+ char padded string
        let encoded = id_to_base58(1);
        assert!(encoded.len() <= 2, "ID 1 encoded as '{}' (too long)", encoded);
    }

    #[test]
    fn test_base58_oversized_input() {
        assert_eq!(base58_to_id("1111111111111111111111111"), None);
    }

    #[test]
    fn test_blob_store_and_read() {
        let tmp = tempfile::tempdir().unwrap();
        let blob_root = tmp.path().join("blobs");
        std::fs::create_dir_all(&blob_root).unwrap();

        let content = b"hello, uhoh!";
        let hash = store_blob(&blob_root, content).unwrap();

        let read_back = read_blob(&blob_root, &hash).unwrap().unwrap();
        assert_eq!(read_back, content);
    }

    #[test]
    fn test_blob_dedup() {
        let tmp = tempfile::tempdir().unwrap();
        let blob_root = tmp.path().join("blobs");
        std::fs::create_dir_all(&blob_root).unwrap();

        let content = b"duplicate content";
        let h1 = store_blob(&blob_root, content).unwrap();
        let h2 = store_blob(&blob_root, content).unwrap();
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_normalize_path() {
        let p = Path::new("src/main.rs");
        assert_eq!(normalize_path(p), "src/main.rs");
    }
}
