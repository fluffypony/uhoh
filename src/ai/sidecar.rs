use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;
use std::sync::Mutex;
use once_cell::sync::Lazy;
use std::time::Instant;

#[derive(Clone, Debug)]
enum Backend {
    LlamaServer { binary: PathBuf },
    MlxLm { python: PathBuf },
}

pub struct Sidecar {
    child: Option<Child>,
    port: u16,
}

impl Sidecar {
    /// Spawn the inference sidecar on an ephemeral port.
    pub fn spawn(model_path: &Path, uhoh_dir: &Path) -> Result<Self> {
        // Bind to port 0 = OS assigns an ephemeral port
        // We'll use a known range and find an available port
        let (listener, port) = find_available_port_listener()?;

        let backend = detect_backend(uhoh_dir)?;
        // Release listener just before spawn to minimize race
        drop(listener);
        let child = spawn_backend(&backend, model_path, uhoh_dir, port)?;

        let sidecar = Sidecar { child: Some(child), port };

        // Wait for sidecar to be ready with exponential backoff
        sidecar.wait_for_ready()?;

        Ok(sidecar)
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Wait for the sidecar's health endpoint to respond.
    fn wait_for_ready(&self) -> Result<()> {
        let mut delay = Duration::from_millis(100);
        let max_wait = Duration::from_secs(30);
        let start = std::time::Instant::now();

        loop {
            if start.elapsed() > max_wait {
                anyhow::bail!("Sidecar did not become ready within 30 seconds");
            }

            let url = format!("http://127.0.0.1:{}/health", self.port);
            match reqwest::blocking::get(&url) {
                Ok(resp) if resp.status().is_success() => return Ok(()),
                _ => {}
            }

            std::thread::sleep(delay);
            delay = std::cmp::min(delay * 2, Duration::from_secs(2));
        }
    }

    pub fn shutdown(&mut self) {
        if let Some(ref mut child) = self.child {
            child.kill().ok();
            child.wait().ok();
        }
        self.child = None;
    }
}

impl Drop for Sidecar {
    fn drop(&mut self) {
        self.shutdown();
    }
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

struct GlobalSidecar {
    child: Child,
    port: u16,
    last_used: Instant,
}

static GLOBAL_SIDECAR: Lazy<Mutex<Option<GlobalSidecar>>> = Lazy::new(|| Mutex::new(None));

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
    let (listener, port) = find_available_port_listener()?;
    let backend = detect_backend(uhoh_dir)?;
    drop(listener);
    let child = spawn_backend(&backend, model_path, uhoh_dir, port)?;

    // Store globally and start idle watcher thread
    {
        let mut guard = GLOBAL_SIDECAR.lock().unwrap();
        *guard = Some(GlobalSidecar { child, port, last_used: Instant::now() });
    }

    let idle = idle_shutdown_secs.max(60);
    std::thread::spawn(move || loop {
        std::thread::sleep(Duration::from_secs(30));
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
                            conf.ai.max_context_tokens.to_string()
                        } else { "8192".to_string() }
                    } else { "8192".to_string() }
                } else { "8192".to_string() }
            };
            Command::new(binary)
                .args([
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
                ])
                .stdout(Stdio::null())
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
            Command::new(python)
                .args([
                    "-m",
                    "mlx_lm.server",
                    "--model",
                    &model_id,
                    "--port",
                    &port.to_string(),
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::from(log_file))
                .spawn()
                .context("Failed to spawn mlx_lm server")
        }
    }
}
