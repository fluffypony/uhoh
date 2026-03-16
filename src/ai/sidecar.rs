use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use std::time::Instant;

#[derive(Clone, Debug)]
enum Backend {
    LlamaServer { binary: PathBuf },
    MlxLm { python: PathBuf },
}

impl Backend {
    fn identity_key(&self) -> String {
        match self {
            Backend::LlamaServer { binary } => {
                format!("llama:{}", binary.to_string_lossy())
            }
            Backend::MlxLm { python } => format!("mlx:{}", python.to_string_lossy()),
        }
    }
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
        "llama-server not found. Place it in {sidecar_dir:?}. PATH is intentionally ignored for security."
    )
}

struct ManagedSidecar {
    child: Child,
    port: u16,
    last_used: Instant,
    model_path: PathBuf,
    backend_key: String,
    ctx_tokens: u64,
}

struct SidecarManagerInner {
    state: Mutex<Option<ManagedSidecar>>,
    idle_secs: AtomicU64,
    monitor_started: AtomicBool,
}

impl Drop for SidecarManagerInner {
    fn drop(&mut self) {
        match self.state.get_mut() {
            Ok(state) => shutdown_sidecar(state),
            Err(poisoned) => shutdown_sidecar(poisoned.into_inner()),
        }
    }
}

#[derive(Clone)]
pub struct SidecarManager(Arc<SidecarManagerInner>);

impl Default for SidecarManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SidecarManager {
    pub fn new() -> Self {
        Self(Arc::new(SidecarManagerInner {
            state: Mutex::new(None),
            idle_secs: AtomicU64::new(300),
            monitor_started: AtomicBool::new(false),
        }))
    }

    pub fn sidecar_running(&self) -> bool {
        self.lock_state().is_some()
    }

    pub fn start_or_restart_port_with_ctx(
        &self,
        model_path: &Path,
        uhoh_dir: &Path,
        idle_shutdown_secs: u64,
        ctx_tokens: u64,
    ) -> Result<u16> {
        let requested_model_path = model_path.to_path_buf();
        let requested_backend = detect_backend(uhoh_dir)?;
        let requested_backend_key = requested_backend.identity_key();

        {
            let mut guard = self.lock_state();
            if let Some(ref mut sidecar) = *guard {
                let config_matches = sidecar.model_path == requested_model_path
                    && sidecar.backend_key == requested_backend_key
                    && sidecar.ctx_tokens == ctx_tokens;
                if !config_matches {
                    shutdown_sidecar(&mut guard);
                }
            }
        }

        {
            let mut guard = self.lock_state();
            if let Some(ref mut sidecar) = *guard {
                if let Ok(status) = sidecar.child.try_wait() {
                    if status.is_none() {
                        sidecar.last_used = Instant::now();
                        return Ok(sidecar.port);
                    }
                    *guard = None;
                } else {
                    *guard = None;
                }
            }
        }

        let backend = requested_backend;
        let (child, port) = spawn_sidecar_process(&backend, model_path, uhoh_dir, ctx_tokens)?;

        {
            let mut guard = self.lock_state();
            *guard = Some(ManagedSidecar {
                child,
                port,
                last_used: Instant::now(),
                model_path: requested_model_path,
                backend_key: requested_backend_key,
                ctx_tokens,
            });
        }

        self.0
            .idle_secs
            .store(idle_shutdown_secs.max(60), Ordering::SeqCst);
        self.ensure_idle_monitor_thread();
        Ok(port)
    }

    pub fn shutdown(&self) {
        let mut guard = self.lock_state();
        shutdown_sidecar(&mut guard);
    }

    fn lock_state(&self) -> MutexGuard<'_, Option<ManagedSidecar>> {
        match self.0.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                tracing::warn!("SidecarManager mutex poisoned, recovering state");
                poisoned.into_inner()
            }
        }
    }

    fn ensure_idle_monitor_thread(&self) {
        if self
            .0
            .monitor_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        let manager = self.clone();
        std::thread::spawn(move || loop {
            std::thread::sleep(Duration::from_secs(1));
            let idle_secs = manager.0.idle_secs.load(Ordering::SeqCst).max(60);
            let should_shutdown = {
                let guard = manager.lock_state();
                guard
                    .as_ref()
                    .is_some_and(|sidecar| sidecar.last_used.elapsed().as_secs() >= idle_secs)
            };
            if should_shutdown {
                manager.shutdown();
            }
        });
    }
}

fn spawn_sidecar_process(
    backend: &Backend,
    model_path: &Path,
    uhoh_dir: &Path,
    ctx_tokens: u64,
) -> Result<(Child, u16)> {
    let mut last_err: Option<anyhow::Error> = None;
    let mut out: Option<(Child, u16)> = None;
    for _ in 0..5 {
        let ephemeral_port = if let Ok(listener) = std::net::TcpListener::bind("127.0.0.1:0") {
            let port = listener.local_addr().map(|addr| addr.port()).unwrap_or(0);
            drop(listener);
            port
        } else {
            last_err = Some(anyhow::anyhow!("Failed to bind ephemeral port"));
            continue;
        };
        if ephemeral_port == 0 {
            last_err = Some(anyhow::anyhow!("OS returned port 0"));
            continue;
        }

        match spawn_backend(backend, model_path, uhoh_dir, ephemeral_port, ctx_tokens) {
            Ok((mut child, bound_port)) => {
                let model_size_gb = std::fs::metadata(model_path)
                    .map(|meta| meta.len() / (1024 * 1024 * 1024))
                    .unwrap_or(0);
                let timeout = Duration::from_secs(30 + model_size_gb);
                match wait_for_ready_blocking(bound_port, timeout, &mut child) {
                    Ok(()) => {
                        out = Some((child, bound_port));
                        break;
                    }
                    Err(err) => {
                        last_err = Some(err);
                        let _ = child.kill();
                        let _ = child.wait();
                    }
                }
            }
            Err(err) => last_err = Some(err),
        }
    }

    out.ok_or_else(|| last_err.unwrap_or_else(|| anyhow::anyhow!("Failed to spawn sidecar")))
}

fn shutdown_sidecar(sidecar: &mut Option<ManagedSidecar>) {
    if let Some(mut sidecar) = sidecar.take() {
        let _ = sidecar.child.kill();
        let _ = sidecar.child.wait();
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

/// Check if MLX backend is available on this platform without full detection.
pub fn is_mlx_available(uhoh_dir: &Path) -> bool {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        let candidates = [
            uhoh_dir.join("venv/mlx/bin/python3"),
            uhoh_dir.join("venv/mlx/bin/python"),
            PathBuf::from("python3"),
            PathBuf::from("python"),
        ];
        for python in candidates {
            if Command::new(&python)
                .args(["-c", "import mlx_lm"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
            {
                return true;
            }
        }
    }
    let _ = uhoh_dir; // suppress unused warning on non-macOS
    false
}

fn detect_backend(uhoh_dir: &Path) -> Result<Backend> {
    // Prefer MLX on Apple Silicon macOS when available
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        // Prefer managed venv python (installed by mlx_update.rs), then fall back.
        let candidates = [
            uhoh_dir.join("venv/mlx/bin/python3"),
            uhoh_dir.join("venv/mlx/bin/python"),
            PathBuf::from("python3"),
            PathBuf::from("python"),
        ];

        for mlx_python in candidates {
            if Command::new(&mlx_python)
                .args(["-c", "import mlx_lm; print('ok')"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|s| s.success())
                .unwrap_or(false)
            {
                return Ok(Backend::MlxLm { python: mlx_python });
            }
        }
    }
    // Fallback to llama-server shipped in ~/.uhoh/sidecar (no PATH)
    Ok(Backend::LlamaServer {
        binary: find_sidecar_binary(uhoh_dir)?,
    })
}

/// Map a model file path to a `HuggingFace` MLX model repo ID.
/// Longer/more-specific patterns come first to avoid "3b" matching "35b" filenames.
fn mlx_model_id_from_path(model_path: &Path) -> String {
    let stem = model_path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("");
    match stem {
        s if s.contains("0.5b") => "mlx-community/Qwen3.5-0.5B-Instruct-4bit".to_string(),
        s if s.contains("35b") || s.contains("a3b") => {
            "mlx-community/Qwen3.5-35B-A3B-4bit".to_string()
        }
        s if s.contains("32b") => "mlx-community/Qwen3.5-32B-Instruct-4bit".to_string(),
        s if s.contains("9b") => "mlx-community/Qwen3.5-9B-Instruct-4bit".to_string(),
        s if s.contains("8b") => "mlx-community/Qwen3.5-8B-Instruct-4bit".to_string(),
        s if s.contains("7b") => "mlx-community/Qwen3.5-7B-Instruct-4bit".to_string(),
        s if s.contains("3b") => "mlx-community/Qwen3.5-3B-Instruct-4bit".to_string(),
        _ => stem.to_string(),
    }
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
                // Constrain to 1 parallel decoding slot to avoid over-allocating memory
                "-np",
                "1",
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
            let model_id = mlx_model_id_from_path(model_path);
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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_identity_key_llama() {
        let backend = Backend::LlamaServer {
            binary: PathBuf::from("/usr/local/bin/llama-server"),
        };
        let key = backend.identity_key();
        assert!(key.starts_with("llama:"));
        assert!(key.contains("llama-server"));
    }

    #[test]
    fn backend_identity_key_mlx() {
        let backend = Backend::MlxLm {
            python: PathBuf::from("/usr/bin/python3"),
        };
        let key = backend.identity_key();
        assert!(key.starts_with("mlx:"));
        assert!(key.contains("python3"));
    }

    #[test]
    fn backend_identity_keys_differ() {
        let llama = Backend::LlamaServer {
            binary: PathBuf::from("/bin/llama"),
        };
        let mlx = Backend::MlxLm {
            python: PathBuf::from("/bin/llama"),
        };
        assert_ne!(llama.identity_key(), mlx.identity_key());
    }

    #[test]
    fn sidecar_manager_default_not_running() {
        let mgr = SidecarManager::new();
        assert!(!mgr.sidecar_running());
    }

    #[test]
    fn sidecar_manager_default_eq_new() {
        let a = SidecarManager::default();
        let b = SidecarManager::new();
        // Both should start not running
        assert!(!a.sidecar_running());
        assert!(!b.sidecar_running());
    }

    #[test]
    fn mlx_model_id_mapping() {
        // Test the actual production function with various model filenames
        let test_cases = [
            ("qwen3.5-0.5b-instruct.gguf", "mlx-community/Qwen3.5-0.5B-Instruct-4bit"),
            ("qwen3.5-35b-a3b-q4_k_m.gguf", "mlx-community/Qwen3.5-35B-A3B-4bit"),
            ("some-model-a3b-variant.gguf", "mlx-community/Qwen3.5-35B-A3B-4bit"),
            ("qwen3.5-32b-instruct.gguf", "mlx-community/Qwen3.5-32B-Instruct-4bit"),
            ("qwen3.5-9b-q4_k_m.gguf", "mlx-community/Qwen3.5-9B-Instruct-4bit"),
            ("qwen3.5-8b-instruct.gguf", "mlx-community/Qwen3.5-8B-Instruct-4bit"),
            ("qwen3.5-7b-instruct.gguf", "mlx-community/Qwen3.5-7B-Instruct-4bit"),
            ("qwen3.5-3b-instruct.gguf", "mlx-community/Qwen3.5-3B-Instruct-4bit"),
            ("unknown-model.gguf", "unknown-model"),
        ];

        for (filename, expected) in test_cases {
            let path = PathBuf::from(filename);
            let model_id = mlx_model_id_from_path(&path);
            assert_eq!(model_id, expected, "Failed for filename '{filename}'");
        }
    }

    #[test]
    fn mlx_model_id_35b_before_3b() {
        // "35b" must match before "3b" to avoid misclassification
        // Test the actual production function
        let path = PathBuf::from("qwen3.5-35b-a3b-q4_k_m.gguf");
        let model_id = mlx_model_id_from_path(&path);
        assert_eq!(model_id, "mlx-community/Qwen3.5-35B-A3B-4bit",
            "35b/a3b pattern should take priority over 3b");
    }

    #[test]
    fn mlx_model_id_no_extension() {
        // Without .gguf extension, file_stem returns "qwen3" (up to first dot)
        // and the rest is considered extension — so use a name with no dots
        let path = PathBuf::from("qwen35-9b-instruct.gguf");
        let model_id = mlx_model_id_from_path(&path);
        assert_eq!(model_id, "mlx-community/Qwen3.5-9B-Instruct-4bit");
    }

    #[test]
    fn mlx_model_id_empty_path() {
        let path = PathBuf::from("");
        let model_id = mlx_model_id_from_path(&path);
        assert_eq!(model_id, "");
    }
}
