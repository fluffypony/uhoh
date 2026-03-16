use anyhow::Result;
use argon2::{Algorithm, Argon2, Params, Version};

pub(crate) const ARGON2_MEMORY_KIB: u32 = 19 * 1024;
pub(crate) const ARGON2_TIME_COST: u32 = 2;
pub(crate) const ARGON2_PARALLELISM: u32 = 1;
const KEY_LEN: usize = 32;

pub(crate) fn argon2id() -> Result<Argon2<'static>> {
    let params = Params::new(
        ARGON2_MEMORY_KIB,
        ARGON2_TIME_COST,
        ARGON2_PARALLELISM,
        Some(KEY_LEN),
    )
    .map_err(|err| anyhow::anyhow!("Invalid Argon2 parameters: {err}"))?;
    Ok(Argon2::new(Algorithm::Argon2id, Version::V0x13, params))
}

pub(crate) fn derive_argon2id_key(master: &str, salt: &[u8; 16]) -> Result<[u8; 32]> {
    let argon2 = argon2id()?;
    let mut out = [0u8; 32];
    argon2
        .hash_password_into(master.as_bytes(), salt, &mut out)
        .map_err(|err| anyhow::anyhow!("Argon2 key derivation failed: {err}"))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argon2id_creates_valid_instance() {
        let result = argon2id();
        assert!(result.is_ok(), "argon2id() should succeed with default params");
    }

    #[test]
    fn derive_key_produces_32_bytes() {
        let salt = [0u8; 16];
        let key = derive_argon2id_key("test-password", &salt).unwrap();
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn derive_key_deterministic() {
        let salt = [1u8; 16];
        let key1 = derive_argon2id_key("password", &salt).unwrap();
        let key2 = derive_argon2id_key("password", &salt).unwrap();
        assert_eq!(key1, key2, "Same input should produce same key");
    }

    #[test]
    fn derive_key_different_passwords_differ() {
        let salt = [2u8; 16];
        let key1 = derive_argon2id_key("password1", &salt).unwrap();
        let key2 = derive_argon2id_key("password2", &salt).unwrap();
        assert_ne!(key1, key2, "Different passwords should produce different keys");
    }

    #[test]
    fn derive_key_different_salts_differ() {
        let salt1 = [3u8; 16];
        let salt2 = [4u8; 16];
        let key1 = derive_argon2id_key("password", &salt1).unwrap();
        let key2 = derive_argon2id_key("password", &salt2).unwrap();
        assert_ne!(key1, key2, "Different salts should produce different keys");
    }

    #[test]
    fn derive_key_empty_password() {
        let salt = [5u8; 16];
        let key = derive_argon2id_key("", &salt).unwrap();
        assert_eq!(key.len(), 32);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn constants_are_reasonable() {
        assert!(ARGON2_MEMORY_KIB >= 1024, "Memory should be at least 1 MiB");
        assert!(ARGON2_TIME_COST >= 1);
        assert!(ARGON2_PARALLELISM >= 1);
    }
}
