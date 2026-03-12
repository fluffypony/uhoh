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
