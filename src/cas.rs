use anyhow::{bail, Context, Result};
use std::io::Read;
use std::path::{Path, PathBuf};

/// Store a blob in the CAS. Uses atomic write (write-to-temp, fsync, rename).
/// Returns the BLAKE3 hex hash.
pub fn store_blob(blob_root: &Path, content: &[u8]) -> Result<String> {
    let hash = blake3::hash(content).to_hex().to_string();
    let dir = blob_root.join(&hash[..2]);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create blob dir: {}", dir.display()))?;
    let blob_path = dir.join(&hash);
    if blob_path.exists() {
        return Ok(hash); // Deduplication: already stored
    }

    // Atomic write: temp file → fsync → rename
    let tmp_path = dir.join(format!("{}.tmp.{}", &hash, std::process::id()));

    let data = maybe_compress(content);
    std::fs::write(&tmp_path, &data)
        .with_context(|| format!("Failed to write temp blob: {}", tmp_path.display()))?;

    // fsync the file
    let f = std::fs::File::open(&tmp_path)?;
    f.sync_all()?;
    drop(f);

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
        std::fs::set_permissions(&blob_path, std::fs::Permissions::from_mode(0o600)).ok();
    }

    Ok(hash)
}

/// Store a blob from a file path using streaming BLAKE3.
/// Returns (hash, size, is_stored).
pub fn store_blob_from_file(
    blob_root: &Path,
    file_path: &Path,
    max_binary_bytes: u64,
    max_text_bytes: u64,
) -> Result<(String, u64, bool)> {
    let mut file = std::fs::File::open(file_path)
        .with_context(|| format!("Cannot open: {}", file_path.display()))?;
    let meta = file
        .metadata()
        .with_context(|| format!("Cannot stat: {}", file_path.display()))?;
    let size = meta.len();

    // Stream the file for hashing
    let mut hasher = blake3::Hasher::new();
    let mut buf = vec![0u8; 64 * 1024]; // 64KB chunks
    let mut first_chunk = Vec::new();
    let mut full_content = Vec::new();
    let mut total_read = 0u64;

    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        if first_chunk.len() < 8192 {
            let take = std::cmp::min(n, 8192 - first_chunk.len());
            first_chunk.extend_from_slice(&buf[..take]);
        }
        full_content.extend_from_slice(&buf[..n]);
        total_read += n as u64;
    }

    let hash = hasher.finalize().to_hex().to_string();

    // Determine if binary
    let is_binary = content_inspector::inspect(&first_chunk).is_binary();
    let max_size = if is_binary {
        max_binary_bytes
    } else {
        max_text_bytes
    };

    let should_store = total_read <= max_size;

    if should_store {
        let dir = blob_root.join(&hash[..2]);
        std::fs::create_dir_all(&dir)?;
        let blob_path = dir.join(&hash);
        if !blob_path.exists() {
            let tmp_path = dir.join(format!("{}.tmp.{}", &hash, std::process::id()));
            let data = maybe_compress(&full_content);
            std::fs::write(&tmp_path, &data)?;
            let f = std::fs::File::open(&tmp_path)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp_path, &blob_path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&blob_path, std::fs::Permissions::from_mode(0o600)).ok();
            }
        }
    }

    Ok((hash, size, should_store))
}

/// Read a blob from the CAS with integrity verification.
pub fn read_blob(blob_root: &Path, hash: &str) -> Result<Option<Vec<u8>>> {
    if hash.len() < 2 {
        return Ok(None);
    }
    let path = blob_root.join(&hash[..2]).join(hash);
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
            &hash[..16],
            &actual_hash[..16]
        );
        return Ok(None);
    }

    Ok(Some(content))
}

pub fn blob_exists(blob_root: &Path, hash: &str) -> bool {
    if hash.len() < 2 {
        return false;
    }
    blob_root.join(&hash[..2]).join(hash).exists()
}

/// Compress if feature enabled
fn maybe_compress(data: &[u8]) -> Vec<u8> {
    #[cfg(feature = "compression")]
    {
        // Prefix with a magic byte to identify compressed blobs
        let mut out = vec![0x01]; // 0x01 = zstd compressed
        let compressed = zstd::encode_all(std::io::Cursor::new(data), 3)
            .unwrap_or_else(|_| data.to_vec());
        out.extend_from_slice(&compressed);
        return out;
    }
    #[cfg(not(feature = "compression"))]
    {
        data.to_vec()
    }
}

fn maybe_decompress(data: &[u8]) -> Result<Vec<u8>> {
    #[cfg(feature = "compression")]
    {
        if data.first() == Some(&0x01) {
            return zstd::decode_all(std::io::Cursor::new(&data[1..]))
                .context("Failed to decompress blob");
        }
    }
    Ok(data.to_vec())
}

// === Base58 Snapshot ID encoding ===

pub fn id_to_base58(id: u64) -> String {
    if id == 0 {
        return bs58::encode(&[0u8])
            .with_alphabet(bs58::Alphabet::BITCOIN)
            .into_string();
    }
    let bytes = id.to_be_bytes();
    // Strip leading zero bytes for shorter IDs
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bs58::encode(&bytes[start..])
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_string()
}

pub fn base58_to_id(s: &str) -> Option<u64> {
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
    Some(u64::from_be_bytes(buf))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_base58_roundtrip() {
        for id in [0, 1, 42, 255, 1000, u64::MAX] {
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
