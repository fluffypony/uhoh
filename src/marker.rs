use anyhow::{Context, Result};
use rand::Rng;
use std::path::{Path, PathBuf};

const MARKER_MAGIC: &[u8; 4] = b"UHOH";
const MARKER_VERSION: u8 = 1;

/// Create a marker file with a unique random project identity.
/// Returns the hex-encoded project hash (64 chars).
pub fn create_marker(project_path: &Path) -> Result<String> {
    let mut id_bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut id_bytes);
    let id_hex = hex::encode(id_bytes);

    let marker_path = marker_path_for(project_path);
    if let Some(parent) = marker_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    // Write with magic + version + id
    let mut buf = Vec::with_capacity(37);
    buf.extend_from_slice(MARKER_MAGIC);
    buf.push(MARKER_VERSION);
    buf.extend_from_slice(&id_bytes);
    std::fs::write(&marker_path, &buf)
        .with_context(|| format!("Failed to write marker: {}", marker_path.display()))?;

    Ok(id_hex)
}

/// Read the marker file and return the project hash (hex string), or None.
pub fn read_marker(project_path: &Path) -> Result<Option<String>> {
    let marker_path = marker_path_for(project_path);
    match std::fs::read(&marker_path) {
        Ok(bytes) => {
            if bytes.len() == 37 && &bytes[..4] == MARKER_MAGIC && bytes[4] == MARKER_VERSION {
                Ok(Some(hex::encode(&bytes[5..37])))
            } else {
                anyhow::bail!(
                    "Marker file corrupted ({} bytes, expected 37): {}",
                    bytes.len(),
                    marker_path.display()
                );
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
/// Returns Vec<(`project_hash`, `discovered_path`)>.
pub fn scan_for_markers(search_paths: &[PathBuf]) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    for base in search_paths {
        match read_marker(base) {
            Ok(Some(hash)) => found.push((hash, base.clone())),
            Ok(None) => {} // no marker present
            Err(e) => {
                tracing::warn!("Corrupt or unreadable marker at {}: {e}", base.display());
            }
        }
    }
    found
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn create_and_read_marker_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("myproject");
        fs::create_dir_all(&project).unwrap();

        let hash = create_marker(&project).expect("create_marker");

        // Hash should be 64 hex chars (32 bytes)
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

        let read_back = read_marker(&project).expect("read_marker");
        assert_eq!(read_back, Some(hash));
    }

    #[test]
    fn create_marker_in_git_repo() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("gitrepo");
        fs::create_dir_all(project.join(".git")).unwrap();

        let hash = create_marker(&project).expect("create_marker");

        // Marker should be inside .git/.uhoh
        assert!(project.join(".git/.uhoh").exists());
        assert!(!project.join(".uhoh").exists());

        let read_back = read_marker(&project).expect("read_marker");
        assert_eq!(read_back, Some(hash));
    }

    #[test]
    fn create_marker_in_non_git_directory() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("plain");
        fs::create_dir_all(&project).unwrap();

        let hash = create_marker(&project).expect("create_marker");

        // Marker should be at .uhoh in project root
        assert!(project.join(".uhoh").exists());

        let read_back = read_marker(&project).expect("read_marker");
        assert_eq!(read_back, Some(hash));
    }

    #[test]
    fn read_marker_returns_none_for_missing() {
        let dir = tempfile::tempdir().unwrap();
        let result = read_marker(dir.path()).expect("read_marker");
        assert_eq!(result, None);
    }

    #[test]
    fn read_marker_rejects_corrupt_file() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("corrupt");
        fs::create_dir_all(&project).unwrap();
        // Write garbage data
        fs::write(project.join(".uhoh"), b"garbage data here").unwrap();

        let err = read_marker(&project).expect_err("should reject corrupt marker");
        assert!(err.to_string().contains("corrupted"));
    }

    #[test]
    fn read_marker_rejects_wrong_magic() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("badmagic");
        fs::create_dir_all(&project).unwrap();
        let mut buf = vec![0u8; 37];
        buf[..4].copy_from_slice(b"NOPE");
        buf[4] = MARKER_VERSION;
        fs::write(project.join(".uhoh"), &buf).unwrap();

        let err = read_marker(&project).expect_err("should reject wrong magic");
        assert!(err.to_string().contains("corrupted"));
    }

    #[test]
    fn read_marker_rejects_wrong_version() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("badver");
        fs::create_dir_all(&project).unwrap();
        let mut buf = vec![0u8; 37];
        buf[..4].copy_from_slice(MARKER_MAGIC);
        buf[4] = 99; // wrong version
        fs::write(project.join(".uhoh"), &buf).unwrap();

        let err = read_marker(&project).expect_err("should reject wrong version");
        assert!(err.to_string().contains("corrupted"));
    }

    #[test]
    fn read_marker_rejects_truncated_file() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("truncated");
        fs::create_dir_all(&project).unwrap();
        // Correct magic and version, but too short
        let mut buf = Vec::new();
        buf.extend_from_slice(MARKER_MAGIC);
        buf.push(MARKER_VERSION);
        buf.extend_from_slice(&[0u8; 10]); // only 15 bytes total, need 37
        fs::write(project.join(".uhoh"), &buf).unwrap();

        let err = read_marker(&project).expect_err("should reject truncated file");
        assert!(err.to_string().contains("corrupted"));
    }

    #[test]
    fn create_marker_produces_unique_hashes() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("proj1");
        let p2 = dir.path().join("proj2");
        fs::create_dir_all(&p1).unwrap();
        fs::create_dir_all(&p2).unwrap();

        let h1 = create_marker(&p1).expect("create_marker p1");
        let h2 = create_marker(&p2).expect("create_marker p2");
        assert_ne!(h1, h2, "Two markers should have different hashes");
    }

    #[test]
    fn marker_file_is_exactly_37_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("sizecheck");
        fs::create_dir_all(&project).unwrap();

        create_marker(&project).expect("create_marker");

        let data = fs::read(project.join(".uhoh")).expect("read marker file");
        assert_eq!(data.len(), 37, "Marker file must be exactly 37 bytes");
        assert_eq!(&data[..4], MARKER_MAGIC);
        assert_eq!(data[4], MARKER_VERSION);
    }

    #[test]
    fn scan_for_markers_finds_existing() {
        let dir = tempfile::tempdir().unwrap();
        let p1 = dir.path().join("a");
        let p2 = dir.path().join("b");
        let p3 = dir.path().join("c");
        fs::create_dir_all(&p1).unwrap();
        fs::create_dir_all(&p2).unwrap();
        fs::create_dir_all(&p3).unwrap();

        let h1 = create_marker(&p1).expect("marker p1");
        let _h2 = create_marker(&p2).expect("marker p2");
        // p3 has no marker

        let found = scan_for_markers(&[p1.clone(), p2.clone(), p3.clone()]);
        assert_eq!(found.len(), 2);
        assert!(found.iter().any(|(h, p)| h == &h1 && p == &p1));
    }

    #[test]
    fn scan_for_markers_empty_input() {
        let found = scan_for_markers(&[]);
        assert!(found.is_empty());
    }

    #[test]
    fn scan_for_markers_handles_nonexistent_paths() {
        let found = scan_for_markers(&[PathBuf::from("/nonexistent/path/abc123")]);
        assert!(found.is_empty());
    }

    #[test]
    fn marker_path_for_git_worktree() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("worktree");
        let gitdir = dir.path().join("real_gitdir");
        fs::create_dir_all(&project).unwrap();
        fs::create_dir_all(&gitdir).unwrap();

        // Simulate a git worktree: .git is a file pointing to the real gitdir
        fs::write(
            project.join(".git"),
            format!("gitdir: {}", gitdir.display()),
        )
        .unwrap();

        let marker = marker_path_for(&project);
        assert_eq!(marker, gitdir.join(".uhoh"));
    }

    #[test]
    fn marker_path_for_git_worktree_relative() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("worktree_rel");
        let gitdir = dir.path().join("worktree_rel/actual_git");
        fs::create_dir_all(&project).unwrap();
        fs::create_dir_all(&gitdir).unwrap();

        // Relative gitdir path
        fs::write(project.join(".git"), "gitdir: actual_git\n").unwrap();

        let marker = marker_path_for(&project);
        assert_eq!(marker, project.join("actual_git/.uhoh"));
    }

    #[test]
    fn marker_path_fallback_when_git_file_has_bad_content() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().join("badgit");
        fs::create_dir_all(&project).unwrap();
        // .git is a file but doesn't have "gitdir:" prefix
        fs::write(project.join(".git"), "random nonsense\n").unwrap();

        let marker = marker_path_for(&project);
        assert_eq!(marker, project.join(".uhoh"));
    }
}
