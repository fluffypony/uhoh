use anyhow::Result;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};

pub fn encrypt_recovery_blob(plaintext: &[u8], key_material: &[u8; 32]) -> Result<Vec<u8>> {
    let cipher = ChaCha20Poly1305::new(Key::from_slice(key_material));
    let nonce = Nonce::from_slice(b"uhoh-recovery");
    cipher
        .encrypt(nonce, plaintext)
        .map_err(|_| anyhow::anyhow!("Failed to encrypt recovery blob"))
}

pub fn write_recovery_file(path: &std::path::Path, payload: &[u8]) -> Result<()> {
    std::fs::write(path, payload)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}
