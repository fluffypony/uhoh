use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Instant;

use anyhow::{Context, Result};
use once_cell::sync::Lazy;

use crate::config::{AiConfig, MlxConfig};
use crate::server::events::ServerEvent;

static LAST_CHECK: Lazy<Mutex<Option<Instant>>> = Lazy::new(|| Mutex::new(None));

#[derive(Debug, serde::Deserialize)]
struct PypiInfoResponse {
    info: PypiPackageInfo,
}

#[derive(Debug, serde::Deserialize)]
struct PypiPackageInfo {
    version: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct MlxState {
    previous_version: Option<String>,
    current_version: Option<String>,
}

pub async fn maybe_run_mlx_auto_update(
    config: &AiConfig,
    uhoh_dir: &std::path::Path,
    event_tx: Option<&tokio::sync::broadcast::Sender<ServerEvent>>,
) -> Result<()> {
    // MLX only works on Apple Silicon macOS
    if cfg!(not(all(target_os = "macos", target_arch = "aarch64"))) {
        let _ = (config, uhoh_dir, event_tx);
        return Ok(());
    }

    if !config.mlx.auto_update {
        return Ok(());
    }

    if config.skip_on_battery && !crate::ai::should_run_ai(config) {
        return Ok(());
    }

    {
        let mut guard = match LAST_CHECK.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("MLX update check mutex poisoned, recovering state");
                poisoned.into_inner()
            }
        };
        if let Some(last) = *guard {
            if last.elapsed().as_secs() < config.mlx.check_interval_hours.saturating_mul(3600) {
                return Ok(());
            }
        }
        *guard = Some(Instant::now());
    }

    let venv_dir = resolve_venv_path(&config.mlx, uhoh_dir);
    ensure_venv(&venv_dir, &config.mlx).await?;

    if crate::ai::sidecar::sidecar_running() {
        emit_mlx_failed(
            event_tx,
            "mlx_update_skipped",
            "sidecar is active; deferring mlx-lm upgrade",
        );
        return Ok(());
    }

    let python = venv_dir.join(if cfg!(windows) {
        "Scripts/python.exe"
    } else {
        "bin/python3"
    });
    let current = read_current_version(&python).await.unwrap_or_default();
    let latest = fetch_latest_version()
        .await
        .unwrap_or_else(|_| current.clone());
    if !current.is_empty() && !latest.is_empty() && current == latest {
        return Ok(());
    }

    run_upgrade_with_rollback(&venv_dir, &config.mlx, uhoh_dir, event_tx).await?;
    Ok(())
}

fn resolve_venv_path(cfg: &MlxConfig, uhoh_dir: &std::path::Path) -> PathBuf {
    if let Some(rest) = cfg.mlx_path_from_home() {
        return if rest.is_absolute() {
            rest
        } else {
            uhoh_dir.join(rest)
        };
    }
    uhoh_dir.join("venv").join("mlx")
}

async fn ensure_venv(venv_dir: &std::path::Path, cfg: &MlxConfig) -> Result<()> {
    if !venv_dir.exists() {
        let python = if cfg.python_path.trim().is_empty() {
            if cfg!(windows) {
                "python".to_string()
            } else {
                "python3".to_string()
            }
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

async fn run_upgrade_with_rollback(
    venv_dir: &std::path::Path,
    cfg: &MlxConfig,
    uhoh_dir: &std::path::Path,
    event_tx: Option<&tokio::sync::broadcast::Sender<ServerEvent>>,
) -> Result<()> {
    let pip = venv_dir.join(if cfg!(windows) {
        "Scripts/pip.exe"
    } else {
        "bin/pip"
    });
    let python = venv_dir.join(if cfg!(windows) {
        "Scripts/python.exe"
    } else {
        "bin/python3"
    });

    let state_path = uhoh_dir.join("mlx_state.json");
    let previous_version = read_current_version(&python).await.ok();
    let state = MlxState {
        previous_version: previous_version.clone(),
        current_version: previous_version.clone(),
    };
    let _ = std::fs::write(&state_path, serde_json::to_string_pretty(&state)?);

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
        emit_mlx_failed(
            event_tx,
            "mlx_update_failed",
            "pip install --upgrade mlx-lm failed",
        );
        anyhow::bail!("pip upgrade failed");
    }

    if let Ok(new_version) = read_current_version(&python).await {
        // Inference smoke test to catch dependency breakage beyond import-level checks.
        if !run_inference_smoke_test(&python).await {
            emit_mlx_failed(
                event_tx,
                "mlx_update_failed",
                "mlx-lm inference smoke test failed after upgrade",
            );
            if let Some(prev) = previous_version {
                rollback_to_version(&pip, &prev, event_tx).await?;
            }
            return Ok(());
        }

        let state = MlxState {
            previous_version,
            current_version: Some(new_version),
        };
        let _ = std::fs::write(&state_path, serde_json::to_string_pretty(&state)?);
        return Ok(());
    }

    emit_mlx_failed(
        event_tx,
        "mlx_update_failed",
        "mlx-lm smoke test failed after upgrade",
    );

    if let Some(prev) = read_state_previous_version(&state_path)? {
        rollback_to_version(&pip, &prev, event_tx).await?;
        emit_mlx_failed(
            event_tx,
            "mlx_update_failed",
            "mlx-lm smoke test failed and rollback was executed",
        );
    }

    Ok(())
}

async fn read_current_version(python: &std::path::Path) -> Result<String> {
    let out = tokio::process::Command::new(python)
        .arg("-c")
        .arg("import mlx_lm; print(mlx_lm.__version__)")
        .output()
        .await
        .context("Failed to run mlx-lm version smoke test")?;
    if !out.status.success() {
        anyhow::bail!("mlx-lm version command failed");
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn read_state_previous_version(state_path: &std::path::Path) -> Result<Option<String>> {
    if !state_path.exists() {
        return Ok(None);
    }
    let raw = std::fs::read_to_string(state_path)?;
    let state: MlxState = serde_json::from_str(&raw)?;
    Ok(state.previous_version)
}

fn emit_mlx_failed(
    event_tx: Option<&tokio::sync::broadcast::Sender<ServerEvent>>,
    status: &str,
    detail: &str,
) {
    if let Some(tx) = event_tx {
        let _ = tx.send(ServerEvent::MlxUpdateFailed {
            status: status.to_string(),
            detail: detail.to_string(),
        });
    }
}

async fn fetch_latest_version() -> Result<String> {
    let url = "https://pypi.org/pypi/mlx-lm/json";
    let resp = reqwest::Client::new().get(url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("PyPI API returned {}", resp.status());
    }
    let body: PypiInfoResponse = resp.json().await?;
    Ok(body.info.version)
}

async fn run_inference_smoke_test(python: &std::path::Path) -> bool {
    // Actually test inference with a minimal generation call, not just import.
    // This catches corrupted models, incompatible architectures, and broken Metal/GPU drivers.
    let script = r#"
from mlx_lm import load, generate
try:
    model, tokenizer = load("mlx-community/Qwen3.5-0.5B-Instruct-4bit")
    result = generate(model, tokenizer, prompt="Hi", max_tokens=1)
    print("OK" if result else "EMPTY")
except Exception as e:
    print(f"FAIL: {e}")
    exit(1)
"#;
    let out = tokio::process::Command::new(python)
        .arg("-c")
        .arg(script)
        .output()
        .await;
    match out {
        Ok(output) => output.status.success(),
        Err(_) => false,
    }
}

async fn rollback_to_version(
    pip: &std::path::Path,
    prev: &str,
    event_tx: Option<&tokio::sync::broadcast::Sender<ServerEvent>>,
) -> Result<()> {
    let rollback_status = tokio::time::timeout(
        std::time::Duration::from_secs(300),
        tokio::process::Command::new(pip)
            .arg("install")
            .arg("--no-input")
            .arg("--disable-pip-version-check")
            .arg(format!("mlx-lm=={prev}"))
            .status(),
    )
    .await
    .context("Timed out during mlx-lm rollback")?
    .context("Failed to execute mlx-lm rollback")?;

    if !rollback_status.success() {
        emit_mlx_failed(event_tx, "mlx_update_failed", "mlx-lm rollback failed");
        anyhow::bail!("mlx-lm rollback failed");
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
