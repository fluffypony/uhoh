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
    let Ok(jobs) = database.dequeue_pending_ai(MAX_AI_SUMMARIES_PER_TICK) else {
        return;
    };

    for job in jobs {
        if job.attempts >= MAX_AI_SUMMARY_ATTEMPTS {
            let _ = database.delete_pending_ai(job.snapshot_rowid);
            continue;
        }

        let this = match database.get_snapshot_by_rowid(job.snapshot_rowid) {
            Ok(Some(snapshot)) => snapshot,
            _ => {
                let _ = database.delete_pending_ai(job.snapshot_rowid);
                continue;
            }
        };
        let prev = database
            .snapshot_before(&job.project_hash, this.snapshot_id)
            .unwrap_or_default();
        let this_files = match database.get_snapshot_files(this.rowid) {
            Ok(files) => files,
            Err(_) => {
                let _ = database.increment_ai_attempts(job.snapshot_rowid);
                continue;
            }
        };
        let prev_files_vec = prev
            .as_ref()
            .and_then(|snapshot| database.get_snapshot_files(snapshot.rowid).ok())
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
                                snapshot_id: crate::cas::id_to_base58(snapshot.snapshot_id),
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
    let mut changes = Vec::with_capacity(this_files.len() + prev_map.len());
    let mut current_paths = HashSet::with_capacity(this_files.len());

    for file in this_files {
        current_paths.insert(file.path.as_str());

        let previous = prev_map.get(&file.path).map(|(hash, stored, size)| {
            crate::ai::summary::SummaryBlobRef {
                hash,
                stored: *stored,
                size: *size,
            }
        });
        let current = Some(crate::ai::summary::SummaryBlobRef {
            hash: &file.hash,
            stored: file.stored,
            size: file.size,
        });

        if previous.is_none() || previous.is_some_and(|previous| previous.hash != file.hash) {
            changes.push(crate::ai::summary::SummaryDiffEntry {
                path: &file.path,
                previous,
                current,
            });
        }
    }

    for (path, (hash, stored, size)) in prev_map {
        if !current_paths.contains(path.as_str()) {
            changes.push(crate::ai::summary::SummaryDiffEntry {
                path,
                previous: Some(crate::ai::summary::SummaryBlobRef {
                    hash,
                    stored: *stored,
                    size: *size,
                }),
                current: None,
            });
        }
    }

    crate::ai::summary::prepare_summary_inputs(blob_root, ai_config, &changes)
}
