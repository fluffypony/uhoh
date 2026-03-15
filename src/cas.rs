use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
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

/// Configuration for blob storage size limits and compression.
#[non_exhaustive]
pub struct BlobStorageParams {
    pub max_copy_blob_bytes: u64,
    pub max_binary_blob_bytes: u64,
    pub max_text_blob_bytes: u64,
    pub compress_enabled: bool,
    pub compress_level: i32,
}

impl BlobStorageParams {
    pub fn new(
        max_copy_blob_bytes: u64,
        max_binary_blob_bytes: u64,
        max_text_blob_bytes: u64,
        compress_enabled: bool,
        compress_level: i32,
    ) -> Self {
        Self {
            max_copy_blob_bytes,
            max_binary_blob_bytes,
            max_text_blob_bytes,
            compress_enabled,
            compress_level,
        }
    }
}

/// Store a blob from a file path using single-pass streaming hash+write.
/// Returns (hash, size, storage_method, bytes_on_disk).
pub fn store_blob_from_file(
    blob_root: &Path,
    file_path: &Path,
    params: &BlobStorageParams,
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
        params.max_binary_blob_bytes
    } else {
        params.max_text_blob_bytes
    };
    let effective_limit = std::cmp::min(cfg_limit, params.max_copy_blob_bytes);

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
    let do_compress = params.compress_enabled;
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

    // Attempt compression if enabled; returns Some((method, compressed_size)) if the
    // compressed blob was placed at blob_path, None if we should use uncompressed.
    if do_compress {
        match try_compress_and_place(
            &tmp_path,
            &tmp_dir,
            &blob_path,
            actual_size,
            params.compress_level,
            &mut buf,
        ) {
            Ok(Some(result)) => {
                let _ = std::fs::remove_file(&tmp_path);
                return Ok((hash, actual_size, result.0, result.1));
            }
            Ok(None) => {} // Compression didn't help; fall through to uncompressed
            Err(e) => {
                let _ = std::fs::remove_file(&tmp_path);
                return Err(e);
            }
        }
    }

    // Place uncompressed temp file at final location
    place_blob(&tmp_path, &blob_path, &hash, actual_size)
}

/// Rename a temp blob file to its final location, handling race conditions.
fn place_blob(
    tmp_path: &Path,
    blob_path: &Path,
    hash: &str,
    actual_size: u64,
) -> Result<(String, u64, StorageMethod, u64)> {
    match std::fs::rename(tmp_path, blob_path) {
        Ok(()) => {
            set_blob_readonly(blob_path);
            fsync_parent_dir(blob_path);
            Ok((hash.to_string(), actual_size, StorageMethod::Copy, actual_size))
        }
        Err(e) => {
            let _ = std::fs::remove_file(tmp_path);
            if blob_path.exists() {
                Ok((hash.to_string(), actual_size, StorageMethod::Copy, 0))
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

/// Try to compress a temp blob and place it at blob_path.
/// Returns Ok(Some((method, compressed_size))) if compressed blob was placed,
/// Ok(None) if compression didn't help (caller should place uncompressed),
/// Err on failure.
#[allow(unused_variables)]
fn try_compress_and_place(
    tmp_path: &Path,
    tmp_dir: &Path,
    blob_path: &Path,
    actual_size: u64,
    compress_level: i32,
    buf: &mut [u8],
) -> Result<Option<(StorageMethod, u64)>> {
    #[cfg(feature = "compression")]
    {
        let level = if (1..=22).contains(&compress_level) {
            compress_level
        } else {
            3
        };
        let src_file = std::fs::File::open(tmp_path)?;
        let mut src_reader = std::io::BufReader::with_capacity(64 * 1024, src_file);

        let compressed_tmp = tmp_dir.join(format!(
            ".cblob.{}.{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let compress_result: anyhow::Result<_> = (|| {
            let cfile = create_restricted_file(&compressed_tmp)?;
            let mut cwriter = std::io::BufWriter::new(cfile);
            cwriter.write_all(COMPRESSION_MAGIC)?;
            let mut encoder = zstd::stream::write::Encoder::new(cwriter, level)?;
            loop {
                let n = src_reader.read(buf)?;
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
            match std::fs::rename(&compressed_tmp, blob_path) {
                Ok(()) => {
                    set_blob_readonly(blob_path);
                    fsync_parent_dir(blob_path);
                    return Ok(Some((StorageMethod::Copy, compressed_size)));
                }
                Err(e) => {
                    let _ = std::fs::remove_file(&compressed_tmp);
                    if blob_path.exists() {
                        return Ok(Some((StorageMethod::Copy, 0)));
                    }
                    return Err(e).context("Failed to rename compressed blob");
                }
            }
        }
        let _ = std::fs::remove_file(&compressed_tmp);
    }
    Ok(None)
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
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !is_temp_file(name) {
                continue;
            }
            let is_stale = std::fs::metadata(&path)
                .ok()
                .and_then(|m| m.modified().ok())
                .and_then(|mtime| now.duration_since(mtime).ok())
                .is_some_and(|age| age > max_age);
            if is_stale {
                let _ = std::fs::remove_file(&path);
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
}
