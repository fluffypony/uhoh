use std::collections::{HashMap, HashSet};
use std::path::Path;

use tokio::sync::broadcast;

use crate::ai::sidecar::SidecarManager;
use crate::config::AiConfig;
use crate::db::Database;
use crate::events::{publish_event, ServerEvent};

const MAX_AI_SUMMARIES_PER_TICK: u32 = 2;
const MAX_AI_SUMMARY_ATTEMPTS: i64 = 5;

/// Process a small batch of deferred AI summary jobs when conditions allow.
///
/// Skipped summaries and generation failures share the same retry budget so
/// callers can distinguish intentional skips from operational errors without
/// changing the queue policy.
pub async fn process_summary_queue(
    uhoh_dir: &Path,
    database: &Database,
    ai_config: &AiConfig,
    sidecar_manager: &SidecarManager,
    event_tx: &broadcast::Sender<ServerEvent>,
) {
    let jobs = match database.dequeue_pending_ai(MAX_AI_SUMMARIES_PER_TICK) {
        Ok(jobs) => jobs,
        Err(e) => {
            tracing::warn!("Failed to dequeue pending AI summaries: {e}");
            return;
        }
    };

    for job in jobs {
        if job.attempts >= MAX_AI_SUMMARY_ATTEMPTS {
            let _ = database.delete_pending_ai(job.snapshot_rowid);
            continue;
        }

        let Ok(Some(this)) = database.get_snapshot_by_rowid(job.snapshot_rowid) else {
            let _ = database.delete_pending_ai(job.snapshot_rowid);
            continue;
        };
        let prev = database
            .snapshot_before(&job.project_hash, this.snapshot_id)
            .unwrap_or_default();
        let Ok(this_files) = database.get_snapshot_files(this.rowid) else {
            let _ = database.increment_ai_attempts(job.snapshot_rowid);
            continue;
        };
        let prev_files_vec = prev
            .as_ref()
            .and_then(|snapshot| {
                match database.get_snapshot_files(snapshot.rowid) {
                    Ok(files) => Some(files),
                    Err(err) => {
                        tracing::warn!(
                            "Failed to fetch previous snapshot files for AI diff: {err}"
                        );
                        None
                    }
                }
            })
            .unwrap_or_default();

        let mut prev_map: HashMap<String, (String, bool, u64)> = HashMap::new();
        for file in prev_files_vec {
            prev_map.insert(file.path.clone(), (file.hash, file.stored, file.size));
        }

        let blob_root = uhoh_dir.join("blobs");
        let prepared = build_summary_inputs(&blob_root, ai_config, &this_files, &prev_map);
        let uhoh_dir_for_ai = uhoh_dir.to_path_buf();
        let ai_config = ai_config.clone();
        let sidecar_manager = sidecar_manager.clone();
        let diff_for_ai = prepared.diff_text.clone();
        let files_for_ai = prepared.files.clone();
        let ai_result = tokio::task::spawn_blocking(move || {
            crate::ai::summary::generate_summary_blocking(
                &uhoh_dir_for_ai,
                &ai_config,
                &sidecar_manager,
                &diff_for_ai,
                &files_for_ai,
            )
        })
        .await
        .unwrap_or_else(|err| Err(anyhow::anyhow!("AI summary task join error: {err}")));

        match ai_result {
            Ok(Some(text)) => {
                if let Err(err) = database.set_ai_summary(job.snapshot_rowid, &text) {
                    tracing::warn!("Failed to set AI summary: {}", err);
                } else {
                    let _ = database.delete_pending_ai(job.snapshot_rowid);
                    if let Ok(Some(snapshot)) = database.get_snapshot_by_rowid(job.snapshot_rowid) {
                        publish_event(
                            event_tx,
                            ServerEvent::AiSummaryCompleted {
                                project_hash: job.project_hash.clone(),
                                snapshot_id: crate::encoding::id_to_base58(snapshot.snapshot_id),
                                summary: text,
                            },
                        );
                    }
                }
            }
            Ok(None) => {
                tracing::debug!(
                    "Skipping deferred AI summary for rowid={} because no model is currently eligible",
                    job.snapshot_rowid
                );
                delete_after_retry_budget(database, job.snapshot_rowid);
            }
            Err(err) => {
                tracing::warn!(
                    "Deferred AI summary failed for rowid={}: {}",
                    job.snapshot_rowid,
                    err
                );
                delete_after_retry_budget(database, job.snapshot_rowid);
            }
        }
    }
}

fn delete_after_retry_budget(database: &Database, snapshot_rowid: i64) {
    if let Ok(attempts) = database.increment_ai_attempts(snapshot_rowid) {
        if attempts >= MAX_AI_SUMMARY_ATTEMPTS {
            let _ = database.delete_pending_ai(snapshot_rowid);
        }
    }
}

fn build_summary_inputs(
    blob_root: &Path,
    ai_config: &AiConfig,
    this_files: &[crate::db::FileEntryRow],
    prev_map: &HashMap<String, (String, bool, u64)>,
) -> crate::ai::summary::PreparedSummaryInput {
    let current_paths: HashSet<&str> = this_files.iter().map(|f| f.path.as_str()).collect();

    let changes = crate::ai::summary::build_diff_entries(
        this_files.iter().map(|f| {
            (
                f.path.as_str(),
                crate::ai::summary::SummaryBlobRef {
                    hash: &f.hash,
                    stored: f.stored,
                    size: f.size,
                },
            )
        }),
        prev_map
            .iter()
            .filter(|(path, _)| !current_paths.contains(path.as_str()))
            .map(|(path, (hash, stored, size))| {
                (
                    path.as_str(),
                    crate::ai::summary::SummaryBlobRef {
                        hash,
                        stored: *stored,
                        size: *size,
                    },
                )
            }),
        |path| {
            prev_map
                .get(path)
                .map(|(hash, stored, size)| crate::ai::summary::SummaryBlobRef {
                    hash,
                    stored: *stored,
                    size: *size,
                })
        },
    );

    crate::ai::summary::prepare_summary_inputs(blob_root, ai_config, &changes)
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::StorageMethod;
    use crate::db::FileEntryRow;
    use std::collections::HashMap;
    use std::path::Path;

    fn make_file(path: &str, hash: &str, size: u64) -> FileEntryRow {
        FileEntryRow {
            path: path.to_string(),
            hash: hash.to_string(),
            size,
            stored: true,
            executable: false,
            mtime: None,
            storage_method: StorageMethod::None,
            is_symlink: false,
        }
    }

    #[test]
    fn build_summary_inputs_empty_files() {
        let blob_root = Path::new("/tmp/nonexistent-blobs");
        let ai_config = crate::config::AiConfig::default();
        let this_files: Vec<FileEntryRow> = vec![];
        let prev_map: HashMap<String, (String, bool, u64)> = HashMap::new();

        let result = build_summary_inputs(blob_root, &ai_config, &this_files, &prev_map);

        assert!(result.files.added.is_empty());
        assert!(result.files.deleted.is_empty());
        assert!(result.files.modified.is_empty());
        assert!(result.diff_text.is_empty());
    }

    #[test]
    fn build_summary_inputs_all_new_files() {
        let blob_root = Path::new("/tmp/nonexistent-blobs");
        let ai_config = crate::config::AiConfig::default();
        let this_files = vec![
            make_file("src/main.rs", "hash_a", 100),
            make_file("src/lib.rs", "hash_b", 200),
        ];
        let prev_map: HashMap<String, (String, bool, u64)> = HashMap::new();

        let result = build_summary_inputs(blob_root, &ai_config, &this_files, &prev_map);

        assert_eq!(result.files.added.len(), 2);
        assert!(result.files.added.contains(&"src/main.rs".to_string()));
        assert!(result.files.added.contains(&"src/lib.rs".to_string()));
        assert!(result.files.deleted.is_empty());
        assert!(result.files.modified.is_empty());
    }

    #[test]
    fn build_summary_inputs_mixed_changes() {
        let blob_root = Path::new("/tmp/nonexistent-blobs");
        let ai_config = crate::config::AiConfig::default();

        // Current snapshot has: main.rs (modified), new.rs (added)
        // Previous had: main.rs (different hash), deleted.rs (not in current)
        let this_files = vec![
            make_file("src/main.rs", "hash_new", 150),
            make_file("src/new.rs", "hash_c", 300),
        ];
        let mut prev_map: HashMap<String, (String, bool, u64)> = HashMap::new();
        prev_map.insert("src/main.rs".to_string(), ("hash_old".to_string(), true, 100));
        prev_map.insert(
            "src/deleted.rs".to_string(),
            ("hash_d".to_string(), true, 50),
        );

        let result = build_summary_inputs(blob_root, &ai_config, &this_files, &prev_map);

        assert_eq!(result.files.added, vec!["src/new.rs".to_string()]);
        assert_eq!(result.files.deleted, vec!["src/deleted.rs".to_string()]);
        assert_eq!(result.files.modified, vec!["src/main.rs".to_string()]);
    }

    #[test]
    fn build_summary_inputs_unchanged_files_excluded() {
        let blob_root = Path::new("/tmp/nonexistent-blobs");
        let ai_config = crate::config::AiConfig::default();

        // File has same hash in both snapshots -- should not appear
        let this_files = vec![make_file("src/stable.rs", "same_hash", 100)];
        let mut prev_map: HashMap<String, (String, bool, u64)> = HashMap::new();
        prev_map.insert(
            "src/stable.rs".to_string(),
            ("same_hash".to_string(), true, 100),
        );

        let result = build_summary_inputs(blob_root, &ai_config, &this_files, &prev_map);

        assert!(result.files.added.is_empty());
        assert!(result.files.deleted.is_empty());
        assert!(result.files.modified.is_empty());
    }
}
