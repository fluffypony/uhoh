use anyhow::{Context, Result};
use std::path::Path;

use crate::db::Database;
use crate::db::EventLedgerEntry;
use crate::event_ledger::EventLedger;

fn check_no_symlink_parents(project_root: &Path, target: &Path) -> Result<()> {
    let relative = target.strip_prefix(project_root).with_context(|| {
        format!(
            "Target {} is outside project root {}",
            target.display(),
            project_root.display()
        )
    })?;
    let mut current = project_root.to_path_buf();
    for component in relative.parent().unwrap_or(Path::new("")).components() {
        current.push(component);
        if let Ok(meta) = std::fs::symlink_metadata(&current) {
            if meta.file_type().is_symlink() {
                anyhow::bail!(
                    "Refusing to revert through symlinked directory: {}",
                    current.display()
                );
            }
        }
    }
    Ok(())
}

fn revert_event(uhoh_dir: &Path, database: &Database, event: &EventLedgerEntry) -> Result<()> {
    let Some(path) = event.path.as_ref() else {
        return Ok(());
    };

    let target = Path::new(path);
    if !target.is_absolute() {
        anyhow::bail!(
            "Refusing to revert event #{}: path {} is not absolute",
            event.id,
            target.display()
        );
    }

    // Validate target is within a known project root to prevent writes outside projects.
    // Fail-closed: if we can't verify the path is within a project, reject the revert.
    let projects = database.list_projects().unwrap_or_default();
    let mut matched_project_root: Option<std::path::PathBuf> = None;
    for project in projects.iter().filter(|p| {
        event
            .project_hash
            .as_ref()
            .map(|ph| &p.hash == ph)
            .unwrap_or(true)
    }) {
        let project_root = Path::new(&project.current_path);
        let check_path = target.parent().unwrap_or(target);
        let within = if let (Ok(canon_check), Ok(canon_root)) = (
            dunce::canonicalize(check_path),
            dunce::canonicalize(project_root),
        ) {
            canon_check.starts_with(&canon_root)
        } else {
            target.starts_with(project_root)
        };
        if within {
            matched_project_root = Some(project_root.to_path_buf());
            break;
        }
    }
    let project_root = matched_project_root.context(format!(
        "Refusing to revert event #{}: path {} is not within any tracked project",
        event.id,
        target.display()
    ))?;

    // Reject symlinked parent directories to prevent writes escaping project root
    // when intermediate paths are symlinks and deeper components do not exist yet.
    check_no_symlink_parents(&project_root, target)?;

    // Reject symlink targets to prevent symlink-based escapes
    if target.exists() {
        let meta = std::fs::symlink_metadata(target)?;
        if meta.file_type().is_symlink() {
            anyhow::bail!(
                "Refusing to revert event #{}: target {} is a symlink",
                event.id,
                target.display()
            );
        }
        if meta.is_dir() {
            anyhow::bail!(
                "Refusing to revert event #{}: target {} is a directory",
                event.id,
                target.display()
            );
        }
    }

    if let Some(pre) = event.pre_state_ref.as_ref() {
        let blob_root = uhoh_dir.join("blobs");
        let content = crate::cas::read_blob(&blob_root, pre)?
            .with_context(|| format!("Missing pre-state blob for event #{}", event.id))?;
        if let Some(parent) = target.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(target, content)?;
    } else if target.exists() {
        let _ = std::fs::remove_file(target);
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn check_no_symlink_parents_normal_path() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let subdir = root.join("src");
        std::fs::create_dir_all(&subdir).unwrap();
        let target = subdir.join("main.rs");
        std::fs::write(&target, "fn main() {}").unwrap();

        assert!(check_no_symlink_parents(root, &target).is_ok());
    }

    #[test]
    fn check_no_symlink_parents_outside_root() {
        let tmp1 = tempfile::tempdir().unwrap();
        let tmp2 = tempfile::tempdir().unwrap();
        let target = tmp2.path().join("outside.txt");
        std::fs::write(&target, "outside").unwrap();

        let result = check_no_symlink_parents(tmp1.path(), &target);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("outside project root"));
    }

    #[cfg(unix)]
    #[test]
    fn check_no_symlink_parents_rejects_symlink_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let outside = tempfile::tempdir().unwrap();
        let outside_file = outside.path().join("secret.txt");
        std::fs::write(&outside_file, "secret").unwrap();

        // Create a symlink inside root pointing outside
        std::os::unix::fs::symlink(outside.path(), root.join("linked")).unwrap();
        let target = root.join("linked").join("secret.txt");

        let result = check_no_symlink_parents(root, &target);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("symlinked directory"));
    }

    #[test]
    fn check_no_symlink_parents_deeply_nested() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let deep = root.join("a").join("b").join("c");
        std::fs::create_dir_all(&deep).unwrap();
        let target = deep.join("file.txt");
        std::fs::write(&target, "content").unwrap();

        assert!(check_no_symlink_parents(root, &target).is_ok());
    }

    #[test]
    fn check_no_symlink_parents_file_at_root() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("root_file.txt");
        std::fs::write(&target, "content").unwrap();

        assert!(check_no_symlink_parents(tmp.path(), &target).is_ok());
    }
}

pub fn resolve_event(
    database: &Database,
    _event_ledger: &EventLedger,
    uhoh_dir: &Path,
    event_id: i64,
) -> Result<()> {
    if database.event_ledger_get(event_id)?.is_some() {
        let descendant_ids = database.event_ledger_descendant_ids(event_id)?;
        let mut ordered = Vec::with_capacity(descendant_ids.len());
        for id in descendant_ids {
            if let Some(entry) = database.event_ledger_get(id)? {
                ordered.push(entry);
            } else {
                tracing::warn!("Event ledger entry {id} not found during undo resolve");
            }
        }
        ordered.sort_by(|a, b| b.id.cmp(&a.id));
        for ev in ordered {
            revert_event(uhoh_dir, database, &ev)?;
        }
    }

    // Keep ledger status consistent with the revert set: root + causal descendants.
    database.event_ledger_mark_resolved_cascade(event_id)?;
    Ok(())
}
