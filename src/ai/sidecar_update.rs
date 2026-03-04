use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SidecarManifest {
    pub version: String,
    pub platform: String,
    pub sha256: String,
    pub source_url: String,
    pub installed_at: String,
    pub binary_size: u64,
}

#[derive(Debug, Deserialize)]
struct GithubRelease {
    tag_name: String,
    assets: Vec<GithubAsset>,
}

#[derive(Debug, Deserialize)]
struct GithubAsset {
    name: String,
    browser_download_url: String,
    size: u64,
}

fn detect_platform_asset_substring() -> Result<&'static str> {
    let os = std::env::consts::OS;
    let arch = std::env::consts::ARCH;
    match (os, arch) {
        ("macos", "aarch64") => Ok("macos-arm64"),
        ("macos", "x86_64") => Ok("macos-x64"),
        ("linux", "x86_64") => {
            if has_nvidia_gpu() {
                Ok("linux-cuda")
            } else {
                Ok("ubuntu-x64")
            }
        }
        ("linux", "aarch64") => Ok("linux-arm64"),
        ("windows", "x86_64") => {
            if has_nvidia_gpu() {
                Ok("win-cuda")
            } else {
                Ok("win-vulkan-x64")
            }
        }
        ("windows", "aarch64") => Ok("win-arm64"),
        _ => bail!("Unsupported platform: {os}-{arch}"),
    }
}

fn has_nvidia_gpu() -> bool {
    if let Ok(status) = std::process::Command::new("nvidia-smi")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
    {
        if status.success() {
            return true;
        }
    }
    #[cfg(target_os = "linux")]
    if Path::new("/proc/driver/nvidia").exists() {
        return true;
    }
    false
}

pub fn read_manifest(sidecar_dir: &Path) -> Option<SidecarManifest> {
    let content = fs::read_to_string(sidecar_dir.join("manifest.json")).ok()?;
    serde_json::from_str(&content).ok()
}

fn write_manifest(sidecar_dir: &Path, manifest: &SidecarManifest) -> Result<()> {
    fs::write(
        sidecar_dir.join("manifest.json"),
        serde_json::to_string_pretty(manifest)?,
    )?;
    Ok(())
}

pub fn check_for_update(
    sidecar_dir: &Path,
    repo: &str,
    pin_version: Option<&str>,
) -> Result<Option<(String, String, u64)>> {
    let platform = detect_platform_asset_substring()?;
    let current = read_manifest(sidecar_dir);

    let url = if let Some(version) = pin_version {
        format!("https://api.github.com/repos/{repo}/releases/tags/{version}")
    } else {
        format!("https://api.github.com/repos/{repo}/releases/latest")
    };

    let client = reqwest::blocking::Client::builder()
        .user_agent("uhoh-sidecar-updater")
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let response = client.get(&url).send()?;
    if response.status() == reqwest::StatusCode::FORBIDDEN
        || response.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
    {
        tracing::warn!("GitHub API rate limited; skipping sidecar update check");
        return Ok(None);
    }
    if !response.status().is_success() {
        tracing::warn!(
            "GitHub API returned {}; skipping sidecar update check",
            response.status()
        );
        return Ok(None);
    }

    let release: GithubRelease = response.json()?;
    if let Some(current) = current {
        if current.version == release.tag_name {
            return Ok(None);
        }
    }

    let asset = release
        .assets
        .iter()
        .find(|asset| {
            asset.name.contains(platform)
                && (asset.name.ends_with(".zip") || asset.name.ends_with(".tar.gz"))
        })
        .or_else(|| {
            let fallback = match platform {
                p if p.starts_with("linux-cuda") => "ubuntu-x64",
                p if p.starts_with("win-cuda") => "win-vulkan-x64",
                _ => return None,
            };
            release.assets.iter().find(|asset| {
                asset.name.contains(fallback)
                    && (asset.name.ends_with(".zip") || asset.name.ends_with(".tar.gz"))
            })
        });

    if let Some(asset) = asset {
        Ok(Some((
            release.tag_name,
            asset.browser_download_url.clone(),
            asset.size,
        )))
    } else {
        tracing::warn!("No matching sidecar asset for platform {}", platform);
        Ok(None)
    }
}

pub fn download_and_install(
    sidecar_dir: &Path,
    version: &str,
    download_url: &str,
    _expected_size: u64,
) -> Result<()> {
    let platform = detect_platform_asset_substring()?;
    fs::create_dir_all(sidecar_dir)?;

    let tmp_archive = sidecar_dir.join(".download.tmp");
    let tmp_binary = sidecar_dir.join(if cfg!(windows) {
        ".llama-server.new.exe"
    } else {
        ".llama-server.new"
    });
    let final_binary = sidecar_dir.join(if cfg!(windows) {
        "llama-server.exe"
    } else {
        "llama-server"
    });
    let backup_binary = sidecar_dir.join(if cfg!(windows) {
        "llama-server.exe.bak"
    } else {
        "llama-server.bak"
    });
    let binary_name = if cfg!(windows) {
        "llama-server.exe"
    } else {
        "llama-server"
    };

    let client = reqwest::blocking::Client::builder()
        .user_agent("uhoh-sidecar-updater")
        .timeout(std::time::Duration::from_secs(600))
        .build()?;

    let mut response = client.get(download_url).send()?;
    if !response.status().is_success() {
        bail!("Download failed with status {}", response.status());
    }

    let mut file = fs::File::create(&tmp_archive)?;
    std::io::copy(&mut response, &mut file)?;
    drop(file);

    let archive_file = fs::File::open(&tmp_archive)?;
    if download_url.ends_with(".zip") {
        let mut archive = zip::ZipArchive::new(archive_file)?;
        let mut found = false;
        for i in 0..archive.len() {
            let mut entry = archive.by_index(i)?;
            if entry.name().ends_with(binary_name) && !entry.name().contains("__MACOSX") {
                let mut out = fs::File::create(&tmp_binary)?;
                std::io::copy(&mut entry, &mut out)?;
                found = true;
                break;
            }
        }
        if !found {
            let _ = fs::remove_file(&tmp_archive);
            bail!("Could not find {binary_name} in downloaded archive");
        }
    } else {
        let gz = flate2::read::GzDecoder::new(archive_file);
        let mut tar = tar::Archive::new(gz);
        let mut found = false;
        for entry in tar.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.to_path_buf();
            if path.file_name().and_then(|n| n.to_str()) == Some(binary_name) {
                let mut out = fs::File::create(&tmp_binary)?;
                std::io::copy(&mut entry, &mut out)?;
                found = true;
                break;
            }
        }
        if !found {
            let _ = fs::remove_file(&tmp_archive);
            bail!("Could not find {binary_name} in downloaded archive");
        }
    }

    let _ = fs::remove_file(&tmp_archive);

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&tmp_binary)?.permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&tmp_binary, perms)?;
    }

    let binary_bytes = fs::read(&tmp_binary)?;
    let sha256 = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(&binary_bytes);
        format!("{:x}", hasher.finalize())
    };
    let binary_size = binary_bytes.len() as u64;

    match std::process::Command::new(&tmp_binary)
        .arg("--version")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
    {
        Ok(output) => {
            if !output.status.success() && output.stdout.is_empty() && output.stderr.is_empty() {
                bail!("Downloaded sidecar binary did not execute correctly");
            }
        }
        Err(e) => {
            let _ = fs::remove_file(&tmp_binary);
            bail!("Downloaded sidecar binary failed to execute: {e}");
        }
    }

    if final_binary.exists() {
        let _ = fs::remove_file(&backup_binary);
        fs::rename(&final_binary, &backup_binary).context("Failed to backup old sidecar binary")?;
    }
    fs::rename(&tmp_binary, &final_binary).context("Failed to install new sidecar binary")?;

    let manifest = SidecarManifest {
        version: version.to_string(),
        platform: platform.to_string(),
        sha256,
        source_url: download_url.to_string(),
        installed_at: chrono::Utc::now().to_rfc3339(),
        binary_size,
    };
    write_manifest(sidecar_dir, &manifest)?;

    Ok(())
}

pub fn check_mlx_lm_version() -> Option<String> {
    let output = std::process::Command::new("python3")
        .args(["-c", "import mlx_lm; print(mlx_lm.__version__)"])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .output()
        .ok()?;
    if output.status.success() {
        Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
    } else {
        None
    }
}

pub fn run_update_check(
    sidecar_dir: &Path,
    repo: &str,
    pin_version: Option<&str>,
    shutdown_fn: impl FnOnce(),
) -> Result<bool> {
    match check_for_update(sidecar_dir, repo, pin_version)? {
        Some((version, url, size)) => {
            shutdown_fn();
            std::thread::sleep(std::time::Duration::from_millis(500));
            download_and_install(sidecar_dir, &version, &url, size)?;
            Ok(true)
        }
        None => Ok(false),
    }
}
