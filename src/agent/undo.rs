use anyhow::{Context, Result};
use std::path::Path;

use crate::db::Database;
use crate::db::EventLedgerEntry;
use crate::event_ledger::EventLedger;

fn revert_event(uhoh_dir: &Path, database: &Database, event: &EventLedgerEntry) -> Result<()> {
    let Some(path) = event.path.as_ref() else {
        return Ok(());
    };

    let target = Path::new(path);

    // Validate target is within a known project root to prevent writes outside projects
    if let Some(ref project_hash) = event.project_hash {
        if let Ok(projects) = database.list_projects() {
            let project = projects.iter().find(|p| &p.hash == project_hash);
            if let Some(proj) = project {
                let project_root = Path::new(&proj.current_path);
                if let (Ok(canon_target), Ok(canon_root)) =
                    (dunce::canonicalize(target.parent().unwrap_or(target)), dunce::canonicalize(project_root))
                {
                    if !canon_target.starts_with(&canon_root) {
                        anyhow::bail!(
                            "Refusing to revert event #{}: path {} is outside project root {}",
                            event.id,
                            target.display(),
                            project_root.display()
                        );
                    }
                }
            }
        }
    }

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
    event_ledger: &EventLedger,
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

    event_ledger.mark_resolved(event_id)
}
