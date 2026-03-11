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

pub fn resolve_event(
    database: &Database,
    _event_ledger: &EventLedger,
    uhoh_dir: &Path,
    event_id: i64,
) -> Result<()> {
    if database.event_ledger_get(event_id)?.is_some() {
        let mut ordered = database
            .event_ledger_descendant_ids(event_id)?
            .into_iter()
            .filter_map(|id| database.event_ledger_get(id).ok().flatten())
            .collect::<Vec<_>>();
        ordered.sort_by(|a, b| b.id.cmp(&a.id));
        for ev in ordered {
            revert_event(uhoh_dir, database, &ev)?;
        }
    }

    // Keep ledger status consistent with the revert set: root + causal descendants.
    database.event_ledger_mark_resolved_cascade(event_id)?;
    Ok(())
}
