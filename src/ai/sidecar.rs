use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

#[derive(Clone, Debug)]
enum Backend {
    LlamaServer { binary: PathBuf },
    MlxLm { python: PathBuf },
}


fn find_available_port_listener() -> Result<(std::net::TcpListener, u16)> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    Ok((listener, port))
}

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
        "llama-server not found. Place it in {:?}. PATH is intentionally ignored for security.",
        sidecar_dir
    )
}

// ===== Persistent global sidecar with idle shutdown =====

pub(crate) struct GlobalSidecar {
    child: Child,
    port: u16,
    last_used: Instant,
}

pub(crate) static GLOBAL_SIDECAR: Lazy<Mutex<Option<GlobalSidecar>>> = Lazy::new(|| Mutex::new(None));
static SIDECAR_INSTANCE_ID: AtomicU64 = AtomicU64::new(0);

/// Get or spawn a persistent sidecar and return its port.
pub fn get_or_spawn_port(model_path: &Path, uhoh_dir: &Path, idle_shutdown_secs: u64) -> Result<u16> {
    // Fast path: if child alive, update last_used and return
    {
        let mut guard = GLOBAL_SIDECAR.lock().unwrap();
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
    // Try up to 3 attempts with fresh ephemeral ports to avoid TOCTOU on bind
    let (child, port) = {
        let mut last_err: Option<anyhow::Error> = None;
        let mut out: Option<(Child, u16)> = None;
        for _ in 0..3 {
            let (listener, port) = find_available_port_listener()?;
            drop(listener);
            match spawn_backend(&backend, model_path, uhoh_dir, port) {
                Ok(child) => {
                    // Wait for readiness before returning
                    if wait_for_ready_blocking(port, Duration::from_secs(30)).is_ok() {
                        out = Some((child, port));
                        break;
                    } else {
                        last_err = Some(anyhow::anyhow!("sidecar not ready on port {}", port));
                        continue;
                    }
                }
                Err(e) => { last_err = Some(e); continue; }
            }
        }
        match out { Some(v) => v, None => return Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Failed to spawn sidecar"))) }
    };

    // Store globally and start idle watcher thread
    {
        let mut guard = GLOBAL_SIDECAR.lock().unwrap();
        *guard = Some(GlobalSidecar { child, port, last_used: Instant::now() });
    }

    // Bump instance id so any old monitor thread exits
    let my_instance_id = SIDECAR_INSTANCE_ID.fetch_add(1, Ordering::SeqCst) + 1;
    let idle = idle_shutdown_secs.max(60);
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(30));
        // If a newer sidecar instance was spawned, exit this monitor thread
        let current_id = SIDECAR_INSTANCE_ID.load(Ordering::SeqCst);
        if current_id != my_instance_id { return; }
        let mut kill = false;
        {
            let guard = GLOBAL_SIDECAR.lock().unwrap();
            if let Some(ref gs) = *guard {
                if gs.last_used.elapsed().as_secs() >= idle {
                    kill = true;
                }
            } else {
                break;
            }
        }
        if kill {
            let mut guard = GLOBAL_SIDECAR.lock().unwrap();
            if let Some(mut gs) = guard.take() {
                let _ = gs.child.kill();
                let _ = gs.child.wait();
            }
            break;
        }
    });

    Ok(port)
}

/// Shutdown and clean up the global sidecar if running.
pub fn shutdown_global_sidecar() {
    if let Ok(mut guard) = GLOBAL_SIDECAR.lock() {
        if let Some(mut gs) = guard.take() {
            let _ = gs.child.kill();
            let _ = gs.child.wait();
        }
    }
}

fn wait_for_ready_blocking(port: u16, max_wait: Duration) -> Result<()> {
    let mut delay = Duration::from_millis(100);
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > max_wait {
            anyhow::bail!("Sidecar did not become ready within {:?}", max_wait);
        }
        let url = format!("http://127.0.0.1:{}/health", port);
        if let Ok(resp) = reqwest::blocking::get(&url) {
            if resp.status().is_success() { return Ok(()); }
        }
        std::thread::sleep(delay);
        delay = std::cmp::min(delay * 2, Duration::from_secs(2));
    }
}

fn detect_backend(uhoh_dir: &Path) -> Result<Backend> {
    // Prefer MLX on Apple Silicon macOS when available
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        if Command::new("python3")
            .args(["-c", "import mlx_lm; print('ok')"])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
        {
            return Ok(Backend::MlxLm { python: PathBuf::from("python3") });
        }
    }
    // Fallback to llama-server shipped in ~/.uhoh/sidecar (no PATH)
    Ok(Backend::LlamaServer { binary: find_sidecar_binary(uhoh_dir)? })
}

fn spawn_backend(backend: &Backend, model_path: &Path, uhoh_dir: &Path, port: u16) -> Result<Child> {
    let log_file = std::fs::File::create(uhoh_dir.join("sidecar.log"))?;
    match backend {
        Backend::LlamaServer { binary } => {
            // Use configured ctx-size based on AI config if available
            let ctx_size = {
                // Try to load config to read max_context_tokens; fallback to 8192 if unavailable
                if let Some(home) = dirs::home_dir() {
                    let cfg_path = home.join(".uhoh").join("config.toml");
                    if let Ok(cfg) = std::fs::read_to_string(&cfg_path) {
                        if let Ok(conf) = toml::from_str::<crate::config::Config>(&cfg) {
                            std::cmp::min(conf.ai.max_context_tokens, 8192).to_string()
                        } else { "8192".to_string() }
                    } else { "8192".to_string() }
                } else { "8192".to_string() }
            };
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
            cmd.stdout(Stdio::null())
                .stderr(Stdio::from(log_file))
                .spawn()
                .context("Failed to spawn llama-server")
        }
        Backend::MlxLm { python } => {
            // MLX expects a HuggingFace repo ID; map common tiers or fall back to filename stem
            let model_id = {
                let stem = model_path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                match stem {
                    s if s.contains("0.5b") => "mlx-community/Qwen2.5-0.5B-Instruct-4bit".to_string(),
                    s if s.contains("3b") => "mlx-community/Qwen2.5-3B-Instruct-4bit".to_string(),
                    s if s.contains("7b") || s.contains("9b") => "mlx-community/Qwen2.5-7B-Instruct-4bit".to_string(),
                    _ => stem.to_string(),
                }
            };
            let mut cmd = Command::new(python);
            cmd.args([
                    "-m",
                    "mlx_lm.server",
                    "--model",
                    &model_id,
                    "--port",
                    &port.to_string(),
                ]);
            cmd.stdout(Stdio::null())
                .stderr(Stdio::from(log_file))
                .spawn()
                .context("Failed to spawn mlx_lm server")
        }
    }
}
