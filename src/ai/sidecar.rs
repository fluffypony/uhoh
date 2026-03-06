use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[derive(Clone, Debug)]
enum Backend {
    LlamaServer { binary: PathBuf },
    MlxLm { python: PathBuf },
}

// Deprecated: we no longer pre-bind ephemeral ports to avoid TOCTOU.

fn find_sidecar_binary(uhoh_dir: &Path) -> Result<PathBuf> {
    let sidecar_dir = uhoh_dir.join("sidecar");
    let candidates = [
        sidecar_dir.join("llama-server"),
        sidecar_dir.join("llama-server.exe"),
    ];

    for path in &candidates {
        if path.exists() {
            return Ok(path.clone());
        }
    }

    anyhow::bail!(
        "llama-server not found. Place it in {sidecar_dir:?}. PATH is intentionally ignored for security."
    )
}

// ===== Persistent global sidecar with idle shutdown =====

pub(crate) struct GlobalSidecar {
    child: Child,
    port: u16,
    last_used: Instant,
}

pub(crate) static GLOBAL_SIDECAR: Lazy<Mutex<Option<GlobalSidecar>>> =
    Lazy::new(|| Mutex::new(None));
static SIDECAR_INSTANCE_ID: AtomicU64 = AtomicU64::new(0);

fn lock_global_sidecar() -> std::sync::MutexGuard<'static, Option<GlobalSidecar>> {
    match GLOBAL_SIDECAR.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            tracing::warn!("GLOBAL_SIDECAR mutex poisoned, recovering state");
            poisoned.into_inner()
        }
    }
}

pub fn sidecar_running() -> bool {
    lock_global_sidecar().is_some()
}

/// Get or spawn a persistent sidecar and return its port.
pub fn get_or_spawn_port_with_ctx(
    model_path: &Path,
    uhoh_dir: &Path,
    idle_shutdown_secs: u64,
    ctx_tokens: u64,
) -> Result<u16> {
    // Fast path: if child alive, update last_used and return
    {
        let mut guard = lock_global_sidecar();
        if let Some(ref mut gs) = *guard {
            // Try a non-blocking check by waiting with zero timeout
            if let Ok(opt) = gs.child.try_wait() {
                if opt.is_none() {
                    gs.last_used = Instant::now();
                    return Ok(gs.port);
                }
                // else: child exited, reset
                *guard = None;
            } else {
                // assume dead and reset
                *guard = None;
            }
        }
    }

    // Spawn new sidecar
    let backend = detect_backend(uhoh_dir)?;
    // Use OS-assigned ephemeral port to avoid TOCTOU race
    let (child, port) = {
        let mut last_err: Option<anyhow::Error> = None;
        let mut out: Option<(Child, u16)> = None;
        for _ in 0..5 {
            // Bind to port 0 to get an OS-assigned ephemeral port, then release
            let ephemeral_port = match std::net::TcpListener::bind("127.0.0.1:0") {
                Ok(listener) => {
                    let port = listener.local_addr().map(|a| a.port()).unwrap_or(0);
                    drop(listener);
                    port
                }
                Err(_) => {
                    last_err = Some(anyhow::anyhow!("Failed to bind ephemeral port"));
                    continue;
                }
            };
            if ephemeral_port == 0 {
                last_err = Some(anyhow::anyhow!("OS returned port 0"));
                continue;
            }
            match spawn_backend(&backend, model_path, uhoh_dir, ephemeral_port, ctx_tokens) {
                Ok((mut child, bound_port)) => {
                    // Scale timeout based on model file size
                    let model_size_gb = std::fs::metadata(model_path)
                        .map(|m| m.len() / (1024 * 1024 * 1024))
                        .unwrap_or(0);
                    let timeout = Duration::from_secs(30 + model_size_gb);
                    match wait_for_ready_blocking(bound_port, timeout, &mut child) {
                        Ok(()) => {
                            out = Some((child, bound_port));
                            break;
                        }
                        Err(e) => {
                            last_err = Some(e);
                            let _ = child.kill();
                            let _ = child.wait();
                        }
                    }
                }
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            }
        }
        match out {
            Some(v) => v,
            None => {
                return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Failed to spawn sidecar")))
            }
        }
    };

    // Store globally and start idle watcher thread
    {
        let mut guard = lock_global_sidecar();
        *guard = Some(GlobalSidecar {
            child,
            port,
            last_used: Instant::now(),
        });
    }

    // Bump instance id so any old monitor thread exits
    let my_instance_id = SIDECAR_INSTANCE_ID.fetch_add(1, Ordering::SeqCst) + 1;
    let idle = idle_shutdown_secs.max(60);
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(30));
        // If a newer sidecar instance was spawned, exit this monitor thread
        let current_id = SIDECAR_INSTANCE_ID.load(Ordering::SeqCst);
        if current_id != my_instance_id {
            return;
        }
        let mut kill = false;
        {
            let guard = lock_global_sidecar();
            if let Some(ref gs) = *guard {
                if gs.last_used.elapsed().as_secs() >= idle {
                    kill = true;
                }
            } else {
                return;
            }
        }
        if kill {
            let mut guard = lock_global_sidecar();
            if let Some(mut gs) = guard.take() {
                let _ = gs.child.kill();
                let _ = gs.child.wait();
            }
            return;
        }
    });

    Ok(port)
}

/// Shutdown and clean up the global sidecar if running.
pub fn shutdown_global_sidecar() {
    let mut guard = lock_global_sidecar();
    if let Some(mut gs) = guard.take() {
        let _ = gs.child.kill();
        let _ = gs.child.wait();
    }
}

fn wait_for_ready_blocking(port: u16, max_wait: Duration, child: &mut Child) -> Result<()> {
    let mut delay = Duration::from_millis(100);
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > max_wait {
            anyhow::bail!("Sidecar did not become ready within {max_wait:?}");
        }
        // Check if child process has already exited
        if let Ok(Some(exit_status)) = child.try_wait() {
            anyhow::bail!("Sidecar exited early with status: {exit_status}");
        }
        // Use /v1/models which both llama-server and mlx_lm.server support
        let url = format!("http://127.0.0.1:{port}/v1/models");
        if let Ok(resp) = reqwest::blocking::get(&url) {
            if resp.status().is_success() {
                return Ok(());
            }
        }
        std::thread::sleep(delay);
        delay = std::cmp::min(delay * 2, Duration::from_secs(2));
    }
}

fn detect_backend(uhoh_dir: &Path) -> Result<Backend> {
    // Prefer MLX on Apple Silicon macOS when available
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        // Check the managed venv first (installed by mlx_update.rs)
        let venv_python = uhoh_dir.join("venv/mlx/bin/python3");
        let mlx_python = if venv_python.exists() {
            venv_python
        } else {
            PathBuf::from("python3")
        };

        if Command::new(&mlx_python)
            .args(["-c", "import mlx_lm; print('ok')"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return Ok(Backend::MlxLm {
                python: mlx_python,
            });
        }
    }
    // Fallback to llama-server shipped in ~/.uhoh/sidecar (no PATH)
    Ok(Backend::LlamaServer {
        binary: find_sidecar_binary(uhoh_dir)?,
    })
}

fn spawn_backend(
    backend: &Backend,
    model_path: &Path,
    uhoh_dir: &Path,
    port: u16,
    ctx_tokens: u64,
) -> Result<(Child, u16)> {
    let log_file = std::fs::File::create(uhoh_dir.join("sidecar.log"))?;
    match backend {
        Backend::LlamaServer { binary } => {
            let ctx_size = ctx_tokens.to_string();
            let mut cmd = Command::new(binary);
            cmd.args([
                "--model",
                &model_path.to_string_lossy(),
                "--port",
                &port.to_string(),
                "--host",
                "127.0.0.1",
                "--ctx-size",
                &ctx_size,
                "--n-gpu-layers",
                "999",
            ]);
            let child = cmd
                .stdout(Stdio::null())
                .stderr(Stdio::from(log_file))
                .spawn()
                .context("Failed to spawn llama-server")?;
            Ok((child, port))
        }
        Backend::MlxLm { python } => {
            // MLX expects a HuggingFace repo ID; map Qwen3.5 tiers or fall back to filename stem
            let model_id = {
                let stem = model_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("");
                match stem {
                    s if s.contains("0.5b") => {
                        "mlx-community/Qwen3.5-0.5B-Instruct-4bit".to_string()
                    }
                    s if s.contains("3b") => "mlx-community/Qwen3.5-3B-Instruct-4bit".to_string(),
                    s if s.contains("7b") => {
                        "mlx-community/Qwen3.5-7B-Instruct-4bit".to_string()
                    }
                    s if s.contains("9b") || s.contains("8b") => {
                        "mlx-community/Qwen3.5-8B-Instruct-4bit".to_string()
                    }
                    s if s.contains("35b") || s.contains("a3b") => {
                        "mlx-community/Qwen3.5-35B-A3B-4bit".to_string()
                    }
                    s if s.contains("32b") => {
                        "mlx-community/Qwen3.5-32B-Instruct-4bit".to_string()
                    }
                    _ => stem.to_string(),
                }
            };
            let mut cmd = Command::new(python);
            cmd.args([
                "-m",
                "mlx_lm.server",
                "--model",
                &model_id,
                "--host",
                "127.0.0.1",
                "--port",
                &port.to_string(),
            ]);
            let child = cmd
                .stdout(Stdio::null())
                .stderr(Stdio::from(log_file))
                .spawn()
                .context("Failed to spawn mlx_lm server")?;
            Ok((child, port))
        }
    }
}
