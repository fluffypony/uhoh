//! Base58 snapshot ID encoding and cross-platform path encoding for manifest storage.
//!
//! These utilities are used throughout the codebase for converting between
//! internal integer IDs and user-facing base58 strings, and for safely
//! encoding filesystem paths (including non-UTF-8) into manifest entries.

use base64::Engine;
use std::ffi::OsString;
use std::path::Path;

// === Base58 Snapshot ID encoding ===

#[must_use] 
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

#[must_use] 
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

// === Path encoding ===

/// Normalize a path to use forward slashes for cross-platform manifest storage.
#[must_use] 
pub fn normalize_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

/// Encode a relative path for manifest storage. If the path is valid UTF-8, normalize slashes.
/// Otherwise, base64-encode the raw platform bytes with a "b64:" prefix.
#[must_use] 
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

/// Decode a manifest relative path back to a platform `OsString`.
#[must_use] 
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
#[must_use] 
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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    // === Base58 tests ===

    #[test]
    fn test_base58_roundtrip() {
        for id in [1, 42, 255, 1000, u64::MAX] {
            let encoded = id_to_base58(id);
            let decoded = base58_to_id(&encoded);
            assert_eq!(decoded, Some(id), "Failed roundtrip for id={id}");
        }
    }

    #[test]
    fn test_base58_roundtrip_powers_of_two() {
        for exp in 0..64 {
            let id: u64 = 1 << exp;
            let encoded = id_to_base58(id);
            assert_eq!(base58_to_id(&encoded), Some(id), "Failed at 2^{exp}");
        }
    }

    #[test]
    fn test_base58_short_ids() {
        let encoded = id_to_base58(1);
        assert!(encoded.len() <= 2, "ID 1 encoded as '{encoded}' (too long)");
    }

    #[test]
    fn test_base58_oversized_input() {
        assert_eq!(base58_to_id("1111111111111111111111111"), None);
    }

    #[test]
    fn test_base58_zero() {
        assert_eq!(id_to_base58(0), "");
        assert_eq!(base58_to_id(""), None);
    }

    #[test]
    fn test_base58_invalid_characters() {
        // '0', 'O', 'I', 'l' are not in bitcoin base58 alphabet
        assert_eq!(base58_to_id("0OIl"), None);
    }

    #[test]
    fn test_base58_sequential_ids_differ() {
        let a = id_to_base58(100);
        let b = id_to_base58(101);
        assert_ne!(a, b);
    }

    #[test]
    fn test_base58_u64_max() {
        let encoded = id_to_base58(u64::MAX);
        assert!(!encoded.is_empty());
        assert!(encoded.len() <= 11, "u64::MAX should encode to <= 11 chars");
        assert_eq!(base58_to_id(&encoded), Some(u64::MAX));
    }

    #[test]
    fn test_base58_boundary_values() {
        // Byte boundary values
        for id in [255u64, 256, 65_535, 65_536, 16_777_215, 16_777_216] {
            let encoded = id_to_base58(id);
            assert_eq!(base58_to_id(&encoded), Some(id), "Failed for id={id}");
        }
    }

    // === Path normalization tests ===

    #[test]
    fn test_normalize_path_forward_slashes() {
        let p = Path::new("src/main.rs");
        assert_eq!(normalize_path(p), "src/main.rs");
    }

    #[test]
    fn test_normalize_path_empty() {
        let p = Path::new("");
        assert_eq!(normalize_path(p), "");
    }

    #[test]
    fn test_normalize_path_single_file() {
        let p = Path::new("file.txt");
        assert_eq!(normalize_path(p), "file.txt");
    }

    #[test]
    fn test_normalize_path_deeply_nested() {
        let p = Path::new("a/b/c/d/e/f.txt");
        assert_eq!(normalize_path(p), "a/b/c/d/e/f.txt");
    }

    #[test]
    fn test_normalize_path_unicode() {
        let p = Path::new("src/日本語/файл.rs");
        let result = normalize_path(p);
        assert!(result.contains("日本語"));
        assert!(result.contains("файл.rs"));
    }

    #[test]
    fn test_normalize_path_spaces() {
        let p = Path::new("my project/src/main file.rs");
        assert_eq!(normalize_path(p), "my project/src/main file.rs");
    }

    // === Encode/decode relpath tests ===

    #[test]
    fn test_encode_decode_relpath_utf8() {
        let p = Path::new("src/main.rs");
        let encoded = encode_relpath(p);
        assert_eq!(encoded, "src/main.rs");
        let decoded = decode_relpath_to_os(&encoded);
        assert_eq!(decoded, "src/main.rs");
    }

    #[test]
    fn test_encode_relpath_preserves_normal_paths() {
        for path_str in ["a.txt", "dir/file.rs", "a/b/c/d.txt"] {
            let p = Path::new(path_str);
            let encoded = encode_relpath(p);
            assert_eq!(encoded, path_str, "Path should be preserved as-is");
        }
    }

    #[test]
    fn test_encode_relpath_b64_prefix_collision() {
        // A path that starts with "b64:" must be base64-encoded to avoid ambiguity
        let p = Path::new("b64:something");
        let encoded = encode_relpath(p);
        assert!(
            encoded.starts_with("b64:"),
            "Should be base64-encoded for safety"
        );
        let decoded = decode_relpath_to_os(&encoded);
        assert_eq!(decoded, "b64:something");
    }

    #[test]
    fn test_decode_relpath_plain_string() {
        let decoded = decode_relpath_to_os("just/a/path.txt");
        assert_eq!(decoded, "just/a/path.txt");
    }

    #[test]
    fn test_decode_relpath_invalid_b64() {
        // Invalid base64 after "b64:" prefix should fall through to plain string
        let decoded = decode_relpath_to_os("b64:!!!invalid!!!");
        assert_eq!(decoded, "b64:!!!invalid!!!");
    }

    #[test]
    fn test_encode_decode_roundtrip_unicode() {
        let p = Path::new("données/café.txt");
        let encoded = encode_relpath(p);
        let decoded = decode_relpath_to_os(&encoded);
        assert_eq!(decoded, "données/café.txt");
    }

    // === is_executable tests ===

    #[cfg(unix)]
    #[test]
    fn test_is_executable_on_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("notexec.txt");
        std::fs::write(&file, "hello").unwrap();
        assert!(!is_executable(&file));
    }

    #[cfg(unix)]
    #[test]
    fn test_is_executable_on_executable_file() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("exec.sh");
        std::fs::write(&file, "#!/bin/sh\necho hi").unwrap();
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_executable(&file));
    }

    #[test]
    fn test_is_executable_nonexistent() {
        assert!(!is_executable(Path::new("/nonexistent/path/to/file")));
    }
}
