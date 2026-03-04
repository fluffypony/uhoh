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
            for project in &projects {
                let project_path = Path::new(&project.current_path);
                if canonical.starts_with(project_path) {
                    return Ok(project.clone());
                }
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
        for project in &projects {
            let project_path = Path::new(&project.current_path);
            if canonical.starts_with(project_path) || project_path.starts_with(&canonical) {
                return Ok(project.clone());
            }
        }
    }

    if projects.len() == 1 {
        if let Some(project) = projects.first() {
            let project = project.clone();
            return Ok(project);
        }
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
