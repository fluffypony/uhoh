use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

/// Create a marker file with a unique random project identity.
/// Returns the hex-encoded project hash (64 chars).
pub fn create_marker(project_path: &Path) -> Result<String> {
    let mut id_bytes = [0u8; 32];
    getrandom::getrandom(&mut id_bytes)
        .map_err(|e| anyhow::anyhow!("Failed to generate random ID: {}", e))?;
    let id_hex = hex::encode(id_bytes);

    let marker_path = marker_path_for(project_path);
    if let Some(parent) = marker_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&marker_path, &id_bytes)
        .with_context(|| format!("Failed to write marker: {}", marker_path.display()))?;

    Ok(id_hex)
}

/// Read the marker file and return the project hash (hex string), or None.
pub fn read_marker(project_path: &Path) -> Result<Option<String>> {
    let marker_path = marker_path_for(project_path);
    match std::fs::read(&marker_path) {
        Ok(bytes) => {
            if bytes.len() == 32 {
                Ok(Some(hex::encode(&bytes)))
            } else {
                tracing::warn!(
                    "Marker file has unexpected size ({} bytes): {}",
                    bytes.len(),
                    marker_path.display()
                );
                Ok(None)
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).context("Failed to read marker file"),
    }
}

/// Determine the marker file path.
/// Uses `.git/.uhoh` for git repos (handling worktrees where .git is a file),
/// or `.uhoh` for non-git directories.
fn marker_path_for(project_path: &Path) -> PathBuf {
    let git_path = project_path.join(".git");
    if git_path.is_dir() {
        // Standard git repo
        git_path.join(".uhoh")
    } else if git_path.is_file() {
        // Git worktree: .git is a file containing "gitdir: <path>"
        if let Ok(content) = std::fs::read_to_string(&git_path) {
            if let Some(gitdir) = content.strip_prefix("gitdir: ") {
                let gitdir = gitdir.trim();
                let resolved = if Path::new(gitdir).is_absolute() {
                    PathBuf::from(gitdir)
                } else {
                    project_path.join(gitdir)
                };
                if resolved.is_dir() {
                    return resolved.join(".uhoh");
                }
            }
        }
        // Fallback: place in project root
        project_path.join(".uhoh")
    } else {
        // Not a git repo
        project_path.join(".uhoh")
    }
}

/// Scan for marker files to detect moved projects.
/// Returns Vec<(project_hash, discovered_path)>.
pub fn scan_for_markers(search_paths: &[PathBuf]) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    for base in search_paths {
        if let Some(hash) = read_marker(base).ok().flatten() {
            found.push((hash, base.clone()));
        }
    }
    found
}
