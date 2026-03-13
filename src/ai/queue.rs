use std::collections::HashMap;
use std::path::Path;

use tokio::sync::broadcast;

use crate::ai::sidecar::SidecarManager;
use crate::config::AiConfig;
use crate::db::Database;
use crate::events::{publish_event, ServerEvent};

const MAX_AI_SUMMARIES_PER_TICK: u32 = 2;
const MAX_AI_SUMMARY_ATTEMPTS: i64 = 5;
const MAX_AI_DIFF_FILE_SIZE: u64 = 512 * 1024;

/// Process a small batch of deferred AI summary jobs when conditions allow.
///
/// Empty summaries and generation errors share the same retry budget so the
/// runtime behavior matches the documented queue policy.
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
        let (added, modified, deleted, diff_chunks) =
            build_summary_inputs(&blob_root, ai_config, &this_files, &prev_map);

        let files = crate::ai::summary::FileChangeSummary {
            added: added.clone(),
            deleted: deleted.clone(),
            modified: modified.clone(),
        };
        let uhoh_dir_for_ai = uhoh_dir.to_path_buf();
        let ai_config = ai_config.clone();
        let sidecar_manager = sidecar_manager.clone();
        let diff_for_ai = diff_chunks.clone();
        let files_for_ai = files.clone();
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
            Ok(text) if !text.is_empty() => {
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
            Ok(_) => {
                tracing::warn!(
                    "AI summary returned empty for snapshot rowid={} (model may be unavailable or still downloading)",
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
) -> (Vec<String>, Vec<String>, Vec<String>, String) {
    let mut added = Vec::new();
    let mut modified = Vec::new();
    let mut deleted = Vec::new();
    let mut diff_chunks = String::new();
    let max_diff_chars = ai_config.max_context_tokens.saturating_mul(4);
    let mut diff_chars = 0usize;
    let mut diff_truncated = false;

    for file in this_files {
        if let Some((prev_hash, prev_stored, prev_size)) = prev_map.get(&file.path) {
            if &file.hash != prev_hash {
                modified.push(file.path.clone());
                if file.stored
                    && *prev_stored
                    && *prev_size <= MAX_AI_DIFF_FILE_SIZE
                    && file.size <= MAX_AI_DIFF_FILE_SIZE
                {
                    let old = crate::cas::read_blob(blob_root, prev_hash).ok().flatten();
                    let new = crate::cas::read_blob(blob_root, &file.hash).ok().flatten();
                    if let (Some(old), Some(new)) = (old, new) {
                        let head_old = &old[..old.len().min(8192)];
                        let head_new = &new[..new.len().min(8192)];
                        if !(content_inspector::inspect(head_old).is_binary()
                            || content_inspector::inspect(head_new).is_binary())
                        {
                            if let (Ok(old_s), Ok(new_s)) =
                                (String::from_utf8(old), String::from_utf8(new))
                            {
                                let diff = similar::TextDiff::from_lines(&old_s, &new_s);
                                crate::ai::summary::append_diff_chunk(
                                    &mut diff_chunks,
                                    &mut diff_chars,
                                    max_diff_chars,
                                    &mut diff_truncated,
                                    &format!("--- a/{}\n+++ b/{}\n", file.path, file.path),
                                );
                                if diff_truncated {
                                    break;
                                }
                                for hunk in diff.unified_diff().context_radius(2).iter_hunks() {
                                    crate::ai::summary::append_diff_chunk(
                                        &mut diff_chunks,
                                        &mut diff_chars,
                                        max_diff_chars,
                                        &mut diff_truncated,
                                        &format!("{}\n", hunk.header()),
                                    );
                                    if diff_truncated {
                                        break;
                                    }
                                    for change in hunk.iter_changes() {
                                        let sign = match change.tag() {
                                            similar::ChangeTag::Delete => '-',
                                            similar::ChangeTag::Insert => '+',
                                            similar::ChangeTag::Equal => ' ',
                                        };
                                        crate::ai::summary::append_diff_chunk(
                                            &mut diff_chunks,
                                            &mut diff_chars,
                                            max_diff_chars,
                                            &mut diff_truncated,
                                            &format!("{}{}", sign, change),
                                        );
                                        if diff_truncated {
                                            break;
                                        }
                                    }
                                    if diff_truncated {
                                        break;
                                    }
                                }
                            }
                        } else {
                            crate::ai::summary::append_diff_chunk(
                                &mut diff_chunks,
                                &mut diff_chars,
                                max_diff_chars,
                                &mut diff_truncated,
                                &format!("--- {}\n[Binary file]\n", file.path),
                            );
                        }
                    }
                }
            }
        } else {
            added.push(file.path.clone());
        }

        if diff_truncated {
            break;
        }
    }

    let this_map: HashMap<String, ()> = this_files
        .iter()
        .map(|file| (file.path.clone(), ()))
        .collect();
    for path in prev_map.keys() {
        if !this_map.contains_key(path) {
            deleted.push(path.clone());
        }
    }

    (added, modified, deleted, diff_chunks)
}
