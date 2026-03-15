use anyhow::{bail, Context, Result};
use std::path::Path;

use crate::db::{Database, ProjectEntry};

/// Resolve a project from an explicit path/hash target.
///
/// This variant avoids implicit current-directory behavior so server handlers can
/// supply their own default path context.
pub fn resolve_project(
    database: &Database,
    target: Option<&str>,
    default_path: Option<&Path>,
) -> Result<ProjectEntry> {
    let projects = database.list_projects()?;
    if projects.is_empty() {
        bail!("No tracked projects. Run 'uhoh add' to register one.");
    }

    if let Some(target) = target {
        let target_path = Path::new(target);
        if target_path.exists() || target_path.is_absolute() {
            let canonical = dunce::canonicalize(target_path)
                .with_context(|| format!("Cannot resolve path: {target}"))?;
            let canonical_s = canonical.to_string_lossy();
            if let Some(project) = projects
                .iter()
                .find(|p| p.current_path == canonical_s.as_ref())
            {
                return Ok(project.clone());
            }
            // Return the most specific (deepest path) match for nested projects
            let best = projects
                .iter()
                .filter(|p| canonical.starts_with(Path::new(&p.current_path)))
                .max_by_key(|p| p.current_path.len());
            if let Some(project) = best {
                return Ok(project.clone());
            }
            bail!("Path '{canonical_s}' is not within a tracked project");
        }

        let matching: Vec<_> = projects
            .iter()
            .filter(|p| p.hash.starts_with(target))
            .collect();
        match matching.len() {
            0 => bail!("No project found matching '{target}'"),
            1 => return Ok(matching[0].clone()),
            _ => bail!(
                "Ambiguous hash prefix '{}': matches {} projects",
                target,
                matching.len()
            ),
        }
    }

    if let Some(default_path) = default_path {
        let canonical = dunce::canonicalize(default_path)
            .with_context(|| format!("Cannot resolve default path: {}", default_path.display()))?;
        // Return the most specific (deepest path) match for nested projects
        let best = projects
            .iter()
            .filter(|p| {
                let pp = Path::new(&p.current_path);
                canonical.starts_with(pp) || pp.starts_with(&canonical)
            })
            .max_by_key(|p| p.current_path.len());
        if let Some(project) = best {
            return Ok(project.clone());
        }
    }

    if projects.len() == 1 {
        return Ok(projects[0].clone());
    }

    bail!(
        "Multiple projects tracked, specify a path or hash prefix:\n{}",
        projects
            .iter()
            .map(|p| format!("  {} -> {}", &p.hash[..8.min(p.hash.len())], p.current_path))
            .collect::<Vec<_>>()
            .join("\n")
    )
}

/// Reject absolute and parent-traversal paths.
pub fn validate_path_within_project(project_path: &Path, relative_path: &str) -> Result<()> {
    let rel = Path::new(relative_path);
    if rel.is_absolute() {
        bail!("Absolute paths are not allowed: {relative_path}");
    }
    for component in rel.components() {
        if matches!(component, std::path::Component::ParentDir) {
            bail!("Path traversal is not allowed: {relative_path}");
        }
    }

    let full_path = project_path.join(rel);
    if full_path.exists() {
        let canonical = dunce::canonicalize(&full_path)?;
        let project_canonical = dunce::canonicalize(project_path)?;
        if !canonical.starts_with(project_canonical) {
            bail!("Path '{relative_path}' resolves outside the project");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    // === validate_path_within_project tests ===

    #[test]
    fn validate_rejects_absolute_path() {
        let dir = tempfile::tempdir().unwrap();
        let err = validate_path_within_project(dir.path(), "/etc/passwd").unwrap_err();
        assert!(err.to_string().contains("Absolute paths are not allowed"));
    }

    #[test]
    fn validate_rejects_parent_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let err = validate_path_within_project(dir.path(), "../../../etc/passwd").unwrap_err();
        assert!(err.to_string().contains("Path traversal is not allowed"));
    }

    #[test]
    fn validate_rejects_hidden_parent_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let err = validate_path_within_project(dir.path(), "src/../../secret.txt").unwrap_err();
        assert!(err.to_string().contains("Path traversal is not allowed"));
    }

    #[test]
    fn validate_accepts_simple_relative_path() {
        let dir = tempfile::tempdir().unwrap();
        validate_path_within_project(dir.path(), "src/main.rs")
            .expect("simple relative should be valid");
    }

    #[test]
    fn validate_accepts_deeply_nested_path() {
        let dir = tempfile::tempdir().unwrap();
        validate_path_within_project(dir.path(), "a/b/c/d/e/f.txt")
            .expect("deeply nested should be valid");
    }

    #[test]
    fn validate_accepts_single_file() {
        let dir = tempfile::tempdir().unwrap();
        validate_path_within_project(dir.path(), "file.txt")
            .expect("single file should be valid");
    }

    #[test]
    fn validate_accepts_dot_path_component() {
        let dir = tempfile::tempdir().unwrap();
        // "." as a component is fine (current dir reference)
        validate_path_within_project(dir.path(), "src/./main.rs")
            .expect("current dir component should be valid");
    }

    #[test]
    fn validate_rejects_symlink_escape() {
        let dir = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        let secret = outside.path().join("secret.txt");
        fs::write(&secret, "top secret").unwrap();

        // Create a symlink inside project pointing outside
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), dir.path().join("escape")).unwrap();
            let err = validate_path_within_project(dir.path(), "escape/secret.txt").unwrap_err();
            assert!(err.to_string().contains("resolves outside the project"));
        }
    }

    #[test]
    fn validate_accepts_existing_file_inside_project() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("real.txt");
        fs::write(&file, "hello").unwrap();

        validate_path_within_project(dir.path(), "real.txt")
            .expect("existing file inside project should be valid");
    }

    #[test]
    fn validate_accepts_nonexistent_relative_path() {
        let dir = tempfile::tempdir().unwrap();
        // Non-existent paths skip the canonicalize check
        validate_path_within_project(dir.path(), "does/not/exist.txt")
            .expect("nonexistent path should be accepted");
    }

    #[test]
    fn validate_rejects_absolute_windows_style() {
        let dir = tempfile::tempdir().unwrap();
        // On Unix this won't be detected as absolute by Path, but test the check anyway
        let result = validate_path_within_project(dir.path(), "C:\\Users\\foo\\secret.txt");
        // On Unix this is a relative path (not absolute), so it should pass.
        // On Windows this would be absolute and rejected.
        #[cfg(windows)]
        assert!(result.is_err());
        #[cfg(unix)]
        assert!(result.is_ok());
    }

    #[test]
    fn validate_rejects_only_parent_dir() {
        let dir = tempfile::tempdir().unwrap();
        let err = validate_path_within_project(dir.path(), "..").unwrap_err();
        assert!(err.to_string().contains("Path traversal is not allowed"));
    }
}
