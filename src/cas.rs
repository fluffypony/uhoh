use anyhow::{Context, Result};
use base64::Engine;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::Path;

#[cfg(feature = "compression")]
const COMPRESSION_MAGIC: &[u8; 12] = b"UHZS\x00ZSTD\x00v1";

/// How a blob was stored in the CAS.
/// Numeric values can be persisted in the DB if needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum StorageMethod {
    None = 0,
    Copy = 1,
    Reflink = 2,
}

impl StorageMethod {
    pub fn is_recoverable(self) -> bool {
        !matches!(self, StorageMethod::None)
    }
    pub fn to_db(self) -> i64 {
        self as i64
    }
    pub fn from_db(v: i64) -> Self {
        match v {
            1 => StorageMethod::Copy,
            2 => StorageMethod::Reflink,
            _ => StorageMethod::None,
        }
    }
    pub fn display_name(self) -> &'static str {
        match self {
            StorageMethod::None => "none",
            StorageMethod::Copy => "copy",
            StorageMethod::Reflink => "reflink",
        }
    }
}

/// Store a blob in the CAS. Uses atomic write (write-to-temp, fsync, rename).
/// Returns (BLAKE3 hex hash, bytes actually written to disk).
/// `bytes_written` is 0 when deduplication hits an existing blob.
pub fn store_blob(blob_root: &Path, content: &[u8]) -> Result<(String, u64)> {
    store_blob_with_level(blob_root, content, 3)
}

/// Same as `store_blob`, but lets callers override zstd compression level.
pub fn store_blob_with_level(
    blob_root: &Path,
    content: &[u8],
    #[allow(unused_variables)] compress_level: i32,
) -> Result<(String, u64)> {
    let hash = blake3::hash(content).to_hex().to_string();
    let dir = blob_root.join(&hash[..hash.len().min(2)]);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create blob dir: {}", dir.display()))?;
    let blob_path = dir.join(&hash);
    if blob_path.exists() {
        return Ok((hash, 0));
    }

    let tmp_path = dir.join(format!(
        ".tmp.{}.{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));

    #[cfg(feature = "compression")]
    let data = maybe_compress_with_level(content, compress_level);
    #[cfg(not(feature = "compression"))]
    let data = maybe_compress(content);
    let bytes_len = data.len() as u64;

    {
        let mut f = create_restricted_file(&tmp_path)
            .with_context(|| format!("Failed to write temp blob: {}", tmp_path.display()))?;
        f.write_all(&data)?;
        f.sync_all()?;
    }

    match std::fs::rename(&tmp_path, &blob_path) {
        Ok(()) => {}
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            if blob_path.exists() {
                return Ok((hash, 0));
            }
            return Err(e).with_context(|| {
                format!(
                    "Failed to rename blob {} -> {}",
                    tmp_path.display(),
                    blob_path.display()
                )
            });
        }
    }

    set_blob_readonly(&blob_path);
    fsync_parent_dir(&blob_path);

    Ok((hash, bytes_len))
}

/// Reads a symlink target at `abs_path`, stores the target bytes in CAS,
/// and returns (hash, target_byte_count, bytes_written_to_disk).
pub fn store_symlink_target(blob_root: &Path, abs_path: &Path) -> Result<(String, u64, u64)> {
    let target = std::fs::read_link(abs_path)
        .with_context(|| format!("Failed to read symlink: {}", abs_path.display()))?;

    #[cfg(unix)]
    let bytes = {
        use std::os::unix::ffi::OsStrExt;
        target.as_os_str().as_bytes().to_vec()
    };

    #[cfg(not(unix))]
    let bytes = target.to_string_lossy().into_owned().into_bytes();

    let size = bytes.len() as u64;
    let (hash, bytes_written) = store_blob(blob_root, &bytes)?;
    Ok((hash, size, bytes_written))
}

/// Store a blob from a file path using single-pass streaming hash+write.
/// Returns (hash, size, storage_method, bytes_on_disk).
pub fn store_blob_from_file(
    blob_root: &Path,
    file_path: &Path,
    max_copy_blob_bytes: u64,
    max_binary_blob_bytes: u64,
    max_text_blob_bytes: u64,
    #[allow(unused_variables)] compress_enabled: bool,
    #[allow(unused_variables)] compress_level: i32,
) -> Result<(String, u64, StorageMethod, u64)> {
    let metadata = std::fs::symlink_metadata(file_path)
        .with_context(|| format!("Cannot stat: {}", file_path.display()))?;
    let file_size = metadata.len();

    let file = std::fs::File::open(file_path)
        .with_context(|| format!("Cannot open: {}", file_path.display()))?;
    let mut reader = std::io::BufReader::with_capacity(64 * 1024, file);
    let mut hasher = blake3::Hasher::new();
    let mut buf = [0u8; 64 * 1024];
    let mut first_chunk = Vec::new();

    let first_n = reader.read(&mut buf)?;
    if first_n > 0 {
        first_chunk.extend_from_slice(&buf[..first_n.min(8192)]);
        hasher.update(&buf[..first_n]);
    }
    let is_binary = !first_chunk.is_empty() && content_inspector::inspect(&first_chunk).is_binary();
    let cfg_limit = if is_binary {
        max_binary_blob_bytes
    } else {
        max_text_blob_bytes
    };
    let effective_limit = std::cmp::min(cfg_limit, max_copy_blob_bytes);

    if file_size > effective_limit {
        loop {
            let n = reader.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let hash = hasher.finalize().to_hex().to_string();
        tracing::debug!(
            "Not storing {} ({} bytes): size exceeds limit ({} bytes)",
            file_path.display(),
            file_size,
            effective_limit
        );
        return Ok((hash, file_size, StorageMethod::None, 0));
    }

    #[cfg(feature = "compression")]
    let do_compress = compress_enabled;
    #[cfg(not(feature = "compression"))]
    let do_compress = false;

    // Single-pass: hash and write to temp file simultaneously to avoid TOCTOU race
    let tmp_dir = blob_root.join("tmp");
    std::fs::create_dir_all(&tmp_dir)?;
    let tmp_path = tmp_dir.join(format!(
        ".blob.{}.{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));

    // Write what we already read (first_n bytes) plus the rest, hashing as we go.
    // Track total bytes read to abort if the file grows past the size limit
    // (prevents unbounded disk consumption from actively-appended files).
    let mut tmp_file = create_restricted_file(&tmp_path)?;
    let mut total_read: u64 = first_n as u64;
    if first_n > 0 {
        tmp_file.write_all(&buf[..first_n])?;
    }
    loop {
        let n = reader.read(&mut buf)?;
        if n == 0 {
            break;
        }
        total_read += n as u64;
        if total_read > effective_limit {
            // File grew past limit during read — abort storage, keep hash-only
            drop(tmp_file);
            let _ = std::fs::remove_file(&tmp_path);
            // Finish hashing the rest for a correct hash
            hasher.update(&buf[..n]);
            loop {
                let m = reader.read(&mut buf)?;
                if m == 0 {
                    break;
                }
                hasher.update(&buf[..m]);
            }
            let hash = hasher.finalize().to_hex().to_string();
            tracing::debug!(
                "Aborting blob storage for {} — file grew past limit during read ({} bytes > {} limit)",
                file_path.display(),
                total_read,
                effective_limit
            );
            return Ok((hash, total_read, StorageMethod::None, 0));
        }
        hasher.update(&buf[..n]);
        tmp_file.write_all(&buf[..n])?;
    }
    tmp_file.sync_all()?;
    drop(tmp_file);

    let actual_size = std::fs::metadata(&tmp_path)?.len();
    let hash = hasher.finalize().to_hex().to_string();

    let dir = blob_root.join(&hash[..hash.len().min(2)]);
    std::fs::create_dir_all(&dir)?;
    let blob_path = dir.join(&hash);

    if blob_path.exists() {
        let _ = std::fs::remove_file(&tmp_path);
        return Ok((hash, actual_size, StorageMethod::Copy, 0));
    }

    // Try reflink from the temp file (not the original) to avoid TOCTOU race:
    // the original file may have changed since we hashed and wrote the temp copy.
    if !do_compress && reflink_copy::reflink(&tmp_path, &blob_path).is_ok() {
        let _ = std::fs::remove_file(&tmp_path);
        set_blob_readonly(&blob_path);
        fsync_parent_dir(&blob_path);
        return Ok((hash, actual_size, StorageMethod::Reflink, 0));
    }

    let bytes_on_disk: u64 = if do_compress {
        #[cfg(feature = "compression")]
        {
            let level = if (1..=22).contains(&compress_level) {
                compress_level
            } else {
                3
            };
            // Read from temp file (already consistent) and compress
            let src_file = std::fs::File::open(&tmp_path)?;
            let mut src_reader = std::io::BufReader::with_capacity(64 * 1024, src_file);

            let compressed_tmp = tmp_dir.join(format!(
                ".cblob.{}.{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_nanos()
            ));
            // Ensure compressed_tmp is cleaned up if compression fails
            let compress_result: anyhow::Result<_> = (|| {
                let cfile = create_restricted_file(&compressed_tmp)?;
                let mut cwriter = std::io::BufWriter::new(cfile);
                cwriter.write_all(COMPRESSION_MAGIC)?;
                let mut encoder = zstd::stream::write::Encoder::new(cwriter, level)?;
                loop {
                    let n = src_reader.read(&mut buf)?;
                    if n == 0 {
                        break;
                    }
                    encoder.write_all(&buf[..n])?;
                }
                let mut writer = encoder.finish()?;
                writer.flush()?;
                Ok(writer)
            })();
            let writer = match compress_result {
                Ok(w) => w,
                Err(e) => {
                    let _ = std::fs::remove_file(&compressed_tmp);
                    return Err(e);
                }
            };
            let finalize_result: anyhow::Result<u64> = (|| {
                let file_handle = writer
                    .into_inner()
                    .map_err(|e| anyhow::anyhow!("Failed to get temp file handle: {e}"))?;
                file_handle.sync_all()?;
                Ok(file_handle.metadata()?.len())
            })();
            let compressed_size = match finalize_result {
                Ok(sz) => sz,
                Err(e) => {
                    let _ = std::fs::remove_file(&compressed_tmp);
                    return Err(e);
                }
            };

            if compressed_size < actual_size + COMPRESSION_MAGIC.len() as u64 {
                // Compressed is smaller: use it
                let _ = std::fs::remove_file(&tmp_path);
                match std::fs::rename(&compressed_tmp, &blob_path) {
                    Ok(()) => {
                        set_blob_readonly(&blob_path);
                        fsync_parent_dir(&blob_path);
                        return Ok((hash, actual_size, StorageMethod::Copy, compressed_size));
                    }
                    Err(e) => {
                        let _ = std::fs::remove_file(&compressed_tmp);
                        if blob_path.exists() {
                            return Ok((hash, actual_size, StorageMethod::Copy, 0));
                        }
                        return Err(e).context("Failed to rename compressed blob");
                    }
                }
            } else {
                // Uncompressed is smaller or equal
                let _ = std::fs::remove_file(&compressed_tmp);
                actual_size
            }
        }
        #[cfg(not(feature = "compression"))]
        {
            actual_size
        }
    } else {
        actual_size
    };

    // Rename uncompressed temp file to final location
    match std::fs::rename(&tmp_path, &blob_path) {
        Ok(()) => {
            set_blob_readonly(&blob_path);
            fsync_parent_dir(&blob_path);
            Ok((hash, actual_size, StorageMethod::Copy, bytes_on_disk))
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            if blob_path.exists() {
                Ok((hash, actual_size, StorageMethod::Copy, 0))
            } else {
                Err(e).with_context(|| {
                    format!(
                        "Failed to rename blob {} -> {}",
                        tmp_path.display(),
                        blob_path.display()
                    )
                })
            }
        }
    }
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
        anyhow::bail!(
            "Blob corruption detected: expected {}, got {}",
            &hash[..hash.len().min(16)],
            &actual_hash[..actual_hash.len().min(16)]
        );
    }

    Ok(Some(content))
}

pub fn blob_exists(blob_root: &Path, hash: &str) -> bool {
    if hash.len() < 2 {
        return false;
    }
    blob_root
        .join(&hash[..hash.len().min(2)])
        .join(hash)
        .exists()
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
pub fn maybe_compress(data: &[u8]) -> Vec<u8> {
    #[cfg(feature = "compression")]
    {
        maybe_compress_with_level(data, 3)
    }
    #[cfg(not(feature = "compression"))]
    {
        data.to_vec()
    }
}

#[cfg(feature = "compression")]
pub fn maybe_compress_with_level(data: &[u8], level: i32) -> Vec<u8> {
    let lvl = if (1..=22).contains(&level) { level } else { 3 };
    let compressed =
        zstd::encode_all(std::io::Cursor::new(data), lvl).unwrap_or_else(|_| data.to_vec());
    maybe_wrap_compressed(data, &compressed)
}

#[cfg(feature = "compression")]
fn maybe_wrap_compressed(original: &[u8], compressed: &[u8]) -> Vec<u8> {
    if compressed.len() < original.len() {
        let mut out = Vec::with_capacity(compressed.len() + COMPRESSION_MAGIC.len());
        out.extend_from_slice(COMPRESSION_MAGIC);
        out.extend_from_slice(compressed);
        return out;
    }
    original.to_vec()
}

fn maybe_decompress(data: &[u8]) -> Result<Vec<u8>> {
    #[cfg(feature = "compression")]
    {
        if data.len() > 12 && &data[..12] == COMPRESSION_MAGIC {
            return zstd::decode_all(std::io::Cursor::new(&data[12..]))
                .context("Failed to decompress blob");
        }
    }
    #[cfg(not(feature = "compression"))]
    {
        // Detect compressed blobs when the compression feature is not compiled in.
        // Without this check, the raw compressed data would fail the BLAKE3
        // integrity check with a misleading "Blob missing" error.
        if data.len() > 12 && data.starts_with(b"UHZS\x00ZSTD\x00v1") {
            anyhow::bail!(
                "Blob is compressed (zstd) but uhoh was built without the 'compression' feature. \
                 Rebuild with --features compression to access this data."
            );
        }
    }
    Ok(data.to_vec())
}

// === Base58 Snapshot ID encoding ===

pub fn id_to_base58(id: u64) -> String {
    // Note: base58 for zero is a single '1'. We never assign snapshot id 0.
    // Guard: if id==0 is somehow passed, return empty to avoid ambiguity with ID 1.
    // This behavior is documented in README under Tips.
    if id == 0 {
        return String::new();
    }
    let bytes = id.to_be_bytes();
    // Strip leading zero bytes for shorter IDs
    let start = bytes.iter().position(|&b| b != 0).unwrap_or(7);
    bs58::encode(&bytes[start..])
        .with_alphabet(bs58::Alphabet::BITCOIN)
        .into_string()
}

pub fn base58_to_id(s: &str) -> Option<u64> {
    if s.is_empty() {
        return None;
    }
    if s.len() > 11 {
        return None;
    }
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
    if id == 0 {
        return None;
    }
    Some(id)
}

/// Normalize a path to use forward slashes for cross-platform manifest storage.
pub fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Encode a relative path for manifest storage. If the path is valid UTF-8, normalize slashes.
/// Otherwise, base64-encode the raw platform bytes with a "b64:" prefix.
pub fn encode_relpath(rel: &Path) -> String {
    if let Some(s) = rel.to_str() {
        if !s.starts_with("b64:") {
            return normalize_path(Path::new(s));
        }
    }
    encode_relpath_bytes(rel)
}

fn encode_relpath_bytes(rel: &Path) -> String {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let bytes = rel.as_os_str().as_bytes();
        format!(
            "b64:{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
        )
    }
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let wide: Vec<u16> = rel.as_os_str().encode_wide().collect();
        // Use explicit little-endian conversion instead of unsafe pointer cast
        // to guarantee consistent encoding regardless of host CPU endianness.
        let bytes: Vec<u8> = wide.iter().flat_map(|w| w.to_le_bytes()).collect();
        format!(
            "b64:{}",
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes)
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        normalize_path(Path::new(&rel.to_string_lossy().to_string()))
    }
}

/// Decode a manifest relative path back to a platform OsString.
pub fn decode_relpath_to_os(s: &str) -> OsString {
    if let Some(rest) = s.strip_prefix("b64:") {
        if let Ok(bytes) = base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(rest) {
            #[cfg(unix)]
            {
                use std::os::unix::ffi::OsStringExt;
                return OsString::from_vec(bytes);
            }
            #[cfg(windows)]
            {
                use std::os::windows::ffi::OsStringExt;
                let mut u16s = Vec::with_capacity(bytes.len() / 2);
                let mut i = 0;
                while i + 1 < bytes.len() {
                    u16s.push(u16::from_le_bytes([bytes[i], bytes[i + 1]]));
                    i += 2;
                }
                return OsString::from_wide(&u16s);
            }
        }
    }
    OsString::from(s)
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
        if let Ok(meta) = std::fs::metadata(blob_path) {
            return meta.len();
        }
        0
    }
    #[cfg(not(unix))]
    {
        std::fs::metadata(blob_path).map(|m| m.len()).unwrap_or(0)
    }
}

/// Clean up stale temp files in the blob store (from crashed processes).
/// Scans both blobs/tmp/ and hash-prefix directories (blobs/ab/, etc.)
/// for temp files older than the specified max_age.
pub fn cleanup_stale_temp_files(blob_root: &Path, max_age: std::time::Duration) {
    let now = std::time::SystemTime::now();

    let is_temp_file = |name: &str| -> bool {
        name.starts_with(".tmp.") || name.starts_with(".blob.") || name.starts_with(".cblob.")
    };

    let cleanup_dir = |dir: &Path| {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if is_temp_file(name) {
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
    };

    // Clean blobs/tmp/
    let tmp_dir = blob_root.join("tmp");
    if tmp_dir.exists() {
        cleanup_dir(&tmp_dir);
    }

    // Also clean hash-prefix directories (blobs/ab/, blobs/cd/, etc.)
    if let Ok(entries) = std::fs::read_dir(blob_root) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // Hash-prefix dirs are 2 hex chars
                    if name.len() == 2 && name.chars().all(|c| c.is_ascii_hexdigit()) {
                        cleanup_dir(&path);
                    }
                }
            }
        }
    }
}
fn fsync_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = std::fs::OpenOptions::new()
            .read(true)
            .open(parent)
            .and_then(|f| f.sync_all());
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
            assert_eq!(decoded, Some(id), "Failed roundtrip for id={id}");
        }
    }

    #[test]
    fn test_base58_short_ids() {
        // ID 1 should produce a short string, not an 8+ char padded string
        let encoded = id_to_base58(1);
        assert!(encoded.len() <= 2, "ID 1 encoded as '{encoded}' (too long)");
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
        let (hash, _) = store_blob(&blob_root, content).unwrap();

        let read_back = read_blob(&blob_root, &hash).unwrap().unwrap();
        assert_eq!(read_back, content);
    }

    #[test]
    fn test_blob_dedup() {
        let tmp = tempfile::tempdir().unwrap();
        let blob_root = tmp.path().join("blobs");
        std::fs::create_dir_all(&blob_root).unwrap();

        let content = b"duplicate content";
        let (h1, w1) = store_blob(&blob_root, content).unwrap();
        let (h2, w2) = store_blob(&blob_root, content).unwrap();
        assert_eq!(h1, h2);
        assert!(w1 > 0);
        assert_eq!(w2, 0);
    }

    #[test]
    fn test_normalize_path() {
        let p = Path::new("src/main.rs");
        assert_eq!(normalize_path(p), "src/main.rs");
    }
}
