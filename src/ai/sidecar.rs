use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

pub struct Sidecar {
    child: Option<Child>,
    port: u16,
}

impl Sidecar {
    /// Spawn the inference sidecar on an ephemeral port.
    pub fn spawn(model_path: &Path, uhoh_dir: &Path) -> Result<Self> {
        // Bind to port 0 = OS assigns an ephemeral port
        // We'll use a known range and find an available port
        let port = find_available_port()?;

        let sidecar_binary = find_sidecar_binary(uhoh_dir)?;
        let log_file = std::fs::File::create(uhoh_dir.join("sidecar.log"))?;

        let child = Command::new(&sidecar_binary)
            .args([
                "--model",
                &model_path.to_string_lossy(),
                "--port",
                &port.to_string(),
                "--host",
                "127.0.0.1",
                "--ctx-size",
                "8192", // Conservative context, not 131072
                "--n-gpu-layers",
                "999",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::from(log_file))
            .spawn()
            .context("Failed to spawn sidecar")?;

        let mut sidecar = Sidecar {
            child: Some(child),
            port,
        };

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

fn find_available_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
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

    // Check if llama-server is in PATH
    if let Ok(output) = Command::new("which").arg("llama-server").output() {
        if output.status.success() {
            let path = String::from_utf8(output.stdout)?.trim().to_string();
            return Ok(PathBuf::from(path));
        }
    }

    anyhow::bail!(
        "llama-server not found. Install it in {:?} or add to PATH.",
        sidecar_dir
    )
}
