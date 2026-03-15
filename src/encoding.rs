//! Base58 snapshot ID encoding and cross-platform path encoding for manifest storage.
//!
//! These utilities are used throughout the codebase for converting between
//! internal integer IDs and user-facing base58 strings, and for safely
//! encoding filesystem paths (including non-UTF-8) into manifest entries.

use base64::Engine;
use std::ffi::OsString;
use std::path::Path;

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

// === Path encoding ===

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
    fn test_base58_zero() {
        assert_eq!(id_to_base58(0), "");
        assert_eq!(base58_to_id(""), None);
    }

    #[test]
    fn test_normalize_path() {
        let p = Path::new("src/main.rs");
        assert_eq!(normalize_path(p), "src/main.rs");
    }

    #[test]
    fn test_encode_decode_relpath_utf8() {
        let p = Path::new("src/main.rs");
        let encoded = encode_relpath(p);
        assert_eq!(encoded, "src/main.rs");
        let decoded = decode_relpath_to_os(&encoded);
        assert_eq!(decoded, "src/main.rs");
    }
}
