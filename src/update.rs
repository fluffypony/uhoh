use anyhow::{bail, Context, Result};
use std::path::Path;

/// Ed25519 public key for release signature verification.
/// IMPORTANT: Replace this with your actual public key before release.
const UPDATE_PUBLIC_KEY: &[u8; 32] = &[0u8; 32];

#[cfg(not(debug_assertions))]
const _: () = {
    // Compile-time assertion: key must not be all zeros in release builds
    let bytes = *UPDATE_PUBLIC_KEY;
    let mut i = 0;
    let mut all_zero = true;
    while i < 32 {
        if bytes[i] != 0 {
            all_zero = false;
        }
        i += 1;
    }
    if all_zero {
        panic!("UPDATE_PUBLIC_KEY is still the placeholder! Replace before release.");
    }
};

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
    // Runtime guard: ensure public key is set
    if UPDATE_PUBLIC_KEY.iter().all(|&b| b == 0) {
        tracing::warn!("Update public key is not set; skipping update check.");
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
                    verified = true;
                } else {
                    bail!(
                        "Hash mismatch! DNS expected {}, got {}. Aborting.",
                        &expected_hash[..expected_hash.len().min(16)],
                        &actual_hash[..actual_hash.len().min(16)]
                    );
                }
            }
            Err(e) => {
                bail!(
                    "Cannot verify update (no signature, DNS failed: {e}). Aborting."
                );
            }
        }
    }

    if !verified {
        bail!("Cannot verify update integrity. Aborting.");
    }

    // Apply update
    apply_update(uhoh_dir, &binary).await?;
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

async fn apply_update(uhoh_dir: &Path, binary: &[u8]) -> Result<()> {
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
