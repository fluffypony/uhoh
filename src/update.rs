use anyhow::{bail, Context, Result};
use std::path::Path;

/// Ed25519 public key for release signature verification.
///
/// RELEASE CHECKLIST: Confirm this key matches the actual release-signing pipeline.
/// If this is still a placeholder, Ed25519 verification will silently fail and the
/// updater falls back to DNS-only verification.
///
/// NOTE: The verify_ed25519_signature function pre-hashes the binary with BLAKE3
/// before passing the digest to Ed25519 verify. This means standard tools (openssl,
/// ssh-keygen) cannot generate compatible signatures — use the project's custom
/// signing script. This coupling is intentional for performance (avoids loading
/// large binaries into Ed25519's internal SHA-512).
const UPDATE_PUBLIC_KEY: &[u8; 32] = &[
    0xe2, 0xb0, 0x6e, 0x9b, 0x57, 0x1c, 0x96, 0x80, 0x74, 0xc3, 0xcb, 0xde, 0x70, 0xf2, 0xe5, 0xb8,
    0x3e, 0x33, 0x5d, 0xab, 0xbb, 0x75, 0xb7, 0x63, 0x49, 0x52, 0x94, 0xc8, 0x55, 0x21, 0xb1, 0xd2,
];

#[derive(serde::Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(serde::Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
}

pub async fn check_and_apply_update(uhoh_dir: &Path) -> Result<()> {
    let key_is_placeholder = UPDATE_PUBLIC_KEY.iter().all(|&b| b == 0);
    if key_is_placeholder {
        tracing::error!(
            "Auto-update disabled: embedded UPDATE_PUBLIC_KEY is unset. Configure signing key before release."
        );
        return Ok(());
    }
    let current_version = env!("CARGO_PKG_VERSION");
    println!("Current version: {current_version}");

    let current_semver =
        semver::Version::parse(current_version).context("Cannot parse current version")?;

    // Fetch latest release from GitHub
    let client = reqwest::Client::new();
    let releases: Vec<GithubRelease> = client
        .get("https://api.github.com/repos/fluffypony/uhoh/releases")
        .header("User-Agent", "uhoh")
        .send()
        .await?
        .json()
        .await?;

    let latest = releases.first().context("No releases found")?;

    let latest_version_str = latest.tag_name.trim_start_matches('v');
    let latest_semver =
        semver::Version::parse(latest_version_str).context("Cannot parse latest version")?;

    if latest_semver <= current_semver {
        println!("Already up to date.");
        return Ok(());
    }

    println!("New version available: {}", latest.tag_name);

    // Find asset for current platform
    let asset_name = format!("uhoh-{}-{}", std::env::consts::OS, std::env::consts::ARCH);
    let asset = latest
        .assets
        .iter()
        .find(|a| a.name.contains(&asset_name))
        .context("No binary available for this platform")?;

    // Download binary
    println!("Downloading {}...", asset.name);
    let binary = client
        .get(&asset.browser_download_url)
        .send()
        .await?
        .bytes()
        .await?;

    // Download signature file
    let sig_url = format!("{}.sig", asset.browser_download_url);
    let sig_result = client.get(&sig_url).send().await;

    // Primary verification: Ed25519 signature
    let mut verified = false;
    if let Ok(sig_resp) = sig_result {
        if sig_resp.status().is_success() {
            let sig_bytes = sig_resp.bytes().await?;
            match verify_ed25519_signature(&binary, &sig_bytes) {
                Ok(true) => {
                    println!("✓ Ed25519 signature verified.");
                    verified = true;
                }
                Ok(false) => {
                    bail!(
                        "Ed25519 signature verification FAILED! \
                         Possible supply chain attack — aborting update."
                    );
                }
                Err(e) => {
                    tracing::warn!("Signature verification error: {}", e);
                }
            }
        }
    }

    // Supplementary verification: DNS TXT record (not primary trust anchor)
    if !verified {
        println!("No signature file found. Attempting DNS verification...");
        match dns_verify_hash(latest_version_str, &asset_name).await {
            Ok(expected_hash) => {
                let actual_hash = blake3::hash(&binary).to_hex().to_string();
                if actual_hash == expected_hash {
                    println!("✓ DNS hash verification passed.");
                } else {
                    bail!(
                        "Hash mismatch! DNS expected {}, got {}. Aborting.",
                        &expected_hash[..expected_hash.len().min(16)],
                        &actual_hash[..actual_hash.len().min(16)]
                    );
                }
            }
            Err(e) => {
                bail!("Cannot verify update (no signature, DNS failed: {e}). Aborting.");
            }
        }
    }

    // Apply update
    apply_update(uhoh_dir, &binary)?;
    println!(
        "Updated to {}. Restart the daemon with `uhoh restart`.",
        latest.tag_name
    );

    Ok(())
}

fn verify_ed25519_signature(data: &[u8], signature_bytes: &[u8]) -> Result<bool> {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};

    let pubkey =
        VerifyingKey::from_bytes(UPDATE_PUBLIC_KEY).context("Invalid embedded public key")?;

    // Hash the binary with BLAKE3, then verify signature of the hash
    let hash = blake3::hash(data);
    let hash_bytes = hash.as_bytes();

    if signature_bytes.len() != 64 {
        return Ok(false);
    }

    let sig = Signature::from_slice(signature_bytes)
        .map_err(|_| anyhow::anyhow!("Invalid signature format"))?;

    Ok(pubkey.verify(hash_bytes, &sig).is_ok())
}

#[cfg(any(test, debug_assertions))]
pub async fn dns_verify_hash(version: &str, asset: &str) -> Result<String> {
    if let Ok(v) = std::env::var("UHOH_TEST_DNS_TXT") {
        return Ok(v);
    }
    dns_verify_hash_inner(version, asset).await
}

#[cfg(not(any(test, debug_assertions)))]
pub async fn dns_verify_hash(version: &str, asset: &str) -> Result<String> {
    dns_verify_hash_inner(version, asset).await
}

async fn dns_verify_hash_inner(version: &str, asset: &str) -> Result<String> {
    use hickory_resolver::config::ResolverConfig;
    use hickory_resolver::{name_server::TokioConnectionProvider, Resolver};
    let query = format!("release-{asset}.{version}.releases.uhoh.it.");
    let resolver: Resolver<_> = Resolver::builder_with_config(
        ResolverConfig::default(),
        TokioConnectionProvider::default(),
    )
    .build();
    let response = resolver
        .txt_lookup(query.clone())
        .await
        .with_context(|| format!("DNS TXT lookup failed for {query}"))?;
    let txt = response
        .iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("Empty TXT record for {query}"))?;
    Ok(txt.to_string())
}

fn apply_update(uhoh_dir: &Path, binary: &[u8]) -> Result<()> {
    let exe_path = std::env::current_exe()?;

    // Keep backup for rollback
    let backup_path = exe_path.with_extension("bak");
    if backup_path.exists() {
        std::fs::remove_file(&backup_path).ok();
    }

    // Write new binary to temp file (verify integrity from memory, not re-read)
    let tmp_path = exe_path.with_extension("new");
    std::fs::write(&tmp_path, binary)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o755))?;
    }

    // Use self-replace for atomic binary swap
    self_replace::self_replace(&tmp_path).context("Failed to replace binary")?;
    std::fs::remove_file(&tmp_path).ok();

    // Signal daemon to restart via trigger file
    let trigger = uhoh_dir.join(".update-ready");
    std::fs::write(&trigger, "").ok();

    Ok(())
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_signature_rejects_short_signature() {
        let data = b"test binary content";
        let short_sig = vec![0u8; 32]; // 32 bytes, need 64
        let result = verify_ed25519_signature(data, &short_sig).unwrap();
        assert!(!result, "Short signature should be rejected");
    }

    #[test]
    fn verify_signature_rejects_empty_signature() {
        let data = b"test";
        let result = verify_ed25519_signature(data, &[]).unwrap();
        assert!(!result);
    }

    #[test]
    fn verify_signature_rejects_random_64_bytes() {
        let data = b"test binary";
        let fake_sig = [0xABu8; 64];
        // This should either return Ok(false) or Err — either way it doesn't verify
        if let Ok(valid) = verify_ed25519_signature(data, &fake_sig) {
            // If it parses at all, it must not validate
            assert!(!valid);
        }
        // Err is also acceptable — invalid signature format
    }

    #[test]
    fn update_public_key_is_set() {
        // The key should not be all zeros (placeholder)
        assert!(
            !UPDATE_PUBLIC_KEY.iter().all(|&b| b == 0),
            "UPDATE_PUBLIC_KEY should not be a zeroed placeholder"
        );
    }

    #[test]
    fn update_public_key_is_32_bytes() {
        assert_eq!(UPDATE_PUBLIC_KEY.len(), 32);
    }
}
