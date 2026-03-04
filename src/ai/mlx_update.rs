use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result};
use once_cell::sync::Lazy;

use crate::config::{AiConfig, MlxConfig};

static LAST_CHECK: Lazy<Mutex<Option<Instant>>> = Lazy::new(|| Mutex::new(None));

pub async fn maybe_run_mlx_auto_update(
    config: &AiConfig,
    uhoh_dir: &std::path::Path,
) -> Result<()> {
    if !config.mlx.auto_update {
        return Ok(());
    }

    if config.skip_on_battery && !crate::ai::should_run_ai(config) {
        return Ok(());
    }

    {
        let mut guard = LAST_CHECK.lock().unwrap();
        if let Some(last) = *guard {
            if last.elapsed().as_secs() < config.mlx.check_interval_hours.saturating_mul(3600) {
                return Ok(());
            }
        }
        *guard = Some(Instant::now());
    }

    let venv_dir = resolve_venv_path(&config.mlx, uhoh_dir);
    ensure_venv(&venv_dir, &config.mlx).await?;
    run_upgrade(&venv_dir, &config.mlx).await?;
    Ok(())
}

fn resolve_venv_path(cfg: &MlxConfig, uhoh_dir: &std::path::Path) -> PathBuf {
    if let Some(rest) = cfg.mlx_path_from_home() {
        return rest;
    }
    uhoh_dir.join("venv").join("mlx")
}

async fn ensure_venv(venv_dir: &std::path::Path, cfg: &MlxConfig) -> Result<()> {
    if !venv_dir.exists() {
        let python = if cfg.python_path.trim().is_empty() {
            "python3".to_string()
        } else {
            cfg.python_path.clone()
        };
        let status = tokio::process::Command::new(python)
            .arg("-m")
            .arg("venv")
            .arg(venv_dir)
            .status()
            .await
            .context("Failed to create MLX virtual environment")?;
        if !status.success() {
            anyhow::bail!("python venv creation failed");
        }
    }
    Ok(())
}

async fn run_upgrade(venv_dir: &std::path::Path, cfg: &MlxConfig) -> Result<()> {
    let pip = venv_dir.join(if cfg!(windows) {
        "Scripts/pip.exe"
    } else {
        "bin/pip"
    });
    let pkg = if let Some(max) = &cfg.max_version {
        format!("mlx-lm<={max}")
    } else {
        "mlx-lm".to_string()
    };

    let status = tokio::time::timeout(
        std::time::Duration::from_secs(300),
        tokio::process::Command::new(&pip)
            .arg("install")
            .arg("--upgrade")
            .arg("--no-input")
            .arg("--disable-pip-version-check")
            .arg(pkg)
            .status(),
    )
    .await
    .context("Timed out while upgrading mlx-lm")?
    .context("Failed to execute pip upgrade for mlx-lm")?;

    if !status.success() {
        anyhow::bail!("pip upgrade failed");
    }
    Ok(())
}

trait MlxConfigExt {
    fn mlx_path_from_home(&self) -> Option<PathBuf>;
}

impl MlxConfigExt for MlxConfig {
    fn mlx_path_from_home(&self) -> Option<PathBuf> {
        let raw = self.venv_path.trim();
        if raw.is_empty() {
            return None;
        }
        if let Some(stripped) = raw.strip_prefix("~/") {
            return dirs::home_dir().map(|h| h.join(stripped));
        }
        Some(PathBuf::from(raw))
    }
}
