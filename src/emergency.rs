//! Emergency delete detection logic.
//!
//! Provides pure functions for threshold evaluation and manifest-diff
//! verification, keeping the daemon event loop focused on orchestration.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::{Duration, Instant};

/// Result of emergency evaluation.
#[derive(Debug, Clone)]
pub enum EmergencyEvaluation {
    /// No emergency detected.
    NoEmergency,
    /// Emergency thresholds exceeded.
    Triggered {
        verified_deleted_count: usize,
        baseline_count: u64,
        ratio: f64,
        deleted_paths_sample: Vec<String>,
    },
    /// Threshold exceeded but cooldown is active.
    CooldownSuppressed {
        verified_deleted_count: usize,
        baseline_count: u64,
        ratio: f64,
        cooldown_remaining_secs: u64,
    },
    /// Skipped: no prior snapshot / restore in progress / overflow.
    Skipped { reason: &'static str },
}

/// Pure threshold math. Returns true if both conditions are met:
/// 1. `deleted_count >= min_files`
/// 2. `deleted_count / baseline_count >= threshold`
///
/// Returns false when `baseline_count == 0` (no prior snapshot).
pub fn exceeds_threshold(
    deleted_count: usize,
    baseline_count: u64,
    threshold: f64,
    min_files: usize,
) -> bool {
    if baseline_count == 0 || deleted_count == 0 {
        return false;
    }
    if deleted_count < min_files {
        return false;
    }
    let ratio = deleted_count as f64 / baseline_count as f64;
    ratio >= threshold
}

/// Compute the deletion ratio. Returns 0.0 if baseline is 0.
pub fn deletion_ratio(deleted_count: usize, baseline_count: u64) -> f64 {
    if baseline_count == 0 {
        return 0.0;
    }
    deleted_count as f64 / baseline_count as f64
}

/// Expand directory deletions against the cached manifest.
///
/// When a `FileDeleted` event arrives for a path that is NOT directly in
/// the manifest but IS a prefix of manifest entries (i.e., a directory
/// was deleted), return all manifest paths under that directory.
///
/// Uses BTreeSet range queries for O(log n + k) performance.
pub fn expand_directory_deletion(
    deleted_path_rel: &str,
    manifest: &BTreeSet<String>,
) -> Vec<String> {
    // Normalize backslashes to forward slashes for cross-platform matching
    let normalized = deleted_path_rel.replace('\\', "/");
    let prefix = if normalized.ends_with('/') {
        normalized
    } else {
        format!("{}/", normalized)
    };

    manifest
        .range(prefix.clone()..)
        .take_while(|p| p.starts_with(&prefix))
        .cloned()
        .collect()
}

/// Verify which manifest paths are truly deleted by stat-checking the filesystem.
///
/// Only counts a file as deleted if `symlink_metadata(abs_path)` returns NotFound.
/// This eliminates false positives from ephemeral files and platform-specific
/// watcher quirks. Note: renames are counted as deletions of the old path
/// (which is correct — the file at that path no longer exists).
pub fn verify_deletions_against_manifest(
    project_root: &Path,
    manifest_paths: &BTreeSet<String>,
) -> Vec<String> {
    manifest_paths
        .iter()
        .filter(|rel_path| {
            let abs_path = project_root.join(rel_path);
            matches!(
                std::fs::symlink_metadata(&abs_path),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound
            )
        })
        .cloned()
        .collect()
}

/// Full emergency evaluation combining all checks.
///
/// This is the main entry point called from the daemon's debounce processor.
pub fn evaluate_emergency(
    deleted_paths_hint_count: usize,
    cached_baseline_count: Option<u64>,
    last_emergency_at: Option<Instant>,
    cooldown_secs: u64,
    threshold: f64,
    min_files: usize,
    restore_in_progress: bool,
    overflow_occurred: bool,
    project_root: &Path,
    cached_manifest: Option<&BTreeSet<String>>,
) -> EmergencyEvaluation {
    if restore_in_progress {
        return EmergencyEvaluation::Skipped {
            reason: "restore_in_progress",
        };
    }
    if overflow_occurred {
        return EmergencyEvaluation::Skipped {
            reason: "watcher_overflow_unreliable_counts",
        };
    }

    let baseline_count = match cached_baseline_count {
        Some(0) | None => {
            return EmergencyEvaluation::Skipped {
                reason: "no_prior_snapshot_or_empty_baseline",
            };
        }
        Some(c) => c,
    };

    if deleted_paths_hint_count < min_files {
        return EmergencyEvaluation::NoEmergency;
    }

    let manifest = match cached_manifest {
        Some(m) => m,
        None => {
            return EmergencyEvaluation::Skipped {
                reason: "no_cached_manifest_for_verification",
            };
        }
    };

    let verified_deleted = verify_deletions_against_manifest(project_root, manifest);
    let verified_count = verified_deleted.len();

    if !exceeds_threshold(verified_count, baseline_count, threshold, min_files) {
        return EmergencyEvaluation::NoEmergency;
    }

    let ratio = deletion_ratio(verified_count, baseline_count);

    let deleted_paths_sample: Vec<String> = verified_deleted.iter().take(20).cloned().collect();

    // Check cooldown
    if let Some(last) = last_emergency_at {
        let elapsed = last.elapsed();
        let cooldown = Duration::from_secs(cooldown_secs);
        if elapsed < cooldown {
            let remaining = (cooldown - elapsed).as_secs();
            return EmergencyEvaluation::CooldownSuppressed {
                verified_deleted_count: verified_count,
                baseline_count,
                ratio,
                cooldown_remaining_secs: remaining,
            };
        }
    }

    EmergencyEvaluation::Triggered {
        verified_deleted_count: verified_count,
        baseline_count,
        ratio,
        deleted_paths_sample,
    }
}

/// Determine severity based on deletion ratio.
pub fn severity_for_ratio(ratio: f64) -> &'static str {
    if ratio >= 0.5 {
        "critical"
    } else {
        "warn"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // ---- Threshold math tests ----

    #[test]
    fn test_threshold_no_baseline() {
        assert!(!exceeds_threshold(10, 0, 0.30, 5));
    }

    #[test]
    fn test_threshold_no_deletes() {
        assert!(!exceeds_threshold(0, 100, 0.30, 5));
    }

    #[test]
    fn test_threshold_below_min_files() {
        assert!(!exceeds_threshold(4, 10, 0.30, 5));
    }

    #[test]
    fn test_threshold_below_ratio() {
        assert!(!exceeds_threshold(5, 100, 0.30, 5));
    }

    #[test]
    fn test_threshold_exactly_at_ratio() {
        assert!(exceeds_threshold(30, 100, 0.30, 5));
    }

    #[test]
    fn test_threshold_above_ratio() {
        assert!(exceeds_threshold(31, 100, 0.30, 5));
    }

    #[test]
    fn test_threshold_meets_ratio_but_not_min_files() {
        assert!(!exceeds_threshold(3, 5, 0.30, 5));
    }

    #[test]
    fn test_threshold_100_percent() {
        assert!(exceeds_threshold(100, 100, 1.0, 5));
        assert!(!exceeds_threshold(99, 100, 1.0, 5));
    }

    #[test]
    fn test_threshold_small_project() {
        assert!(exceeds_threshold(5, 10, 0.30, 5));
    }

    // ---- Directory expansion tests ----

    #[test]
    fn test_expand_directory_deletion() {
        let mut manifest = BTreeSet::new();
        manifest.insert("src/main.rs".to_string());
        manifest.insert("src/lib.rs".to_string());
        manifest.insert("src/utils/helper.rs".to_string());
        manifest.insert("tests/test1.rs".to_string());
        manifest.insert("Cargo.toml".to_string());

        let expanded = expand_directory_deletion("src", &manifest);
        assert_eq!(expanded.len(), 3);
        assert!(expanded.contains(&"src/main.rs".to_string()));
        assert!(expanded.contains(&"src/lib.rs".to_string()));
        assert!(expanded.contains(&"src/utils/helper.rs".to_string()));
    }

    #[test]
    fn test_expand_nonexistent_directory() {
        let mut manifest = BTreeSet::new();
        manifest.insert("src/main.rs".to_string());

        let expanded = expand_directory_deletion("build", &manifest);
        assert!(expanded.is_empty());
    }

    #[test]
    fn test_expand_with_trailing_slash() {
        let mut manifest = BTreeSet::new();
        manifest.insert("src/main.rs".to_string());

        let expanded = expand_directory_deletion("src/", &manifest);
        assert_eq!(expanded.len(), 1);
    }

    // ---- Ratio / severity tests ----

    #[test]
    fn test_severity_critical() {
        assert_eq!(severity_for_ratio(0.5), "critical");
        assert_eq!(severity_for_ratio(0.9), "critical");
    }

    #[test]
    fn test_severity_warn() {
        assert_eq!(severity_for_ratio(0.3), "warn");
        assert_eq!(severity_for_ratio(0.49), "warn");
    }

    #[test]
    fn test_deletion_ratio_zero_baseline() {
        assert_eq!(deletion_ratio(10, 0), 0.0);
    }

    // ---- Cooldown gating tests ----

    #[test]
    fn test_cooldown_suppresses_immediate_retrigger() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = BTreeSet::new();
        for i in 0..10 {
            let name = format!("file{}.rs", i);
            manifest.insert(name);
            // Don't create the files -> they appear "deleted"
        }

        let result = evaluate_emergency(
            10,
            Some(10),
            None,
            120,
            0.30,
            5,
            false,
            false,
            tmp.path(),
            Some(&manifest),
        );
        assert!(matches!(result, EmergencyEvaluation::Triggered { .. }));

        let result = evaluate_emergency(
            10,
            Some(10),
            Some(Instant::now()),
            120,
            0.30,
            5,
            false,
            false,
            tmp.path(),
            Some(&manifest),
        );
        assert!(matches!(
            result,
            EmergencyEvaluation::CooldownSuppressed { .. }
        ));
    }

    #[test]
    fn test_cooldown_allows_after_expiry() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = BTreeSet::new();
        for i in 0..10 {
            manifest.insert(format!("file{}.rs", i));
        }

        let long_ago = Instant::now() - Duration::from_secs(300);
        let result = evaluate_emergency(
            10,
            Some(10),
            Some(long_ago),
            120,
            0.30,
            5,
            false,
            false,
            tmp.path(),
            Some(&manifest),
        );
        assert!(matches!(result, EmergencyEvaluation::Triggered { .. }));
    }

    // ---- Restore suppression test ----

    #[test]
    fn test_skipped_during_restore() {
        let tmp = TempDir::new().unwrap();
        let manifest = BTreeSet::new();

        let result = evaluate_emergency(
            100,
            Some(100),
            None,
            120,
            0.30,
            5,
            true,
            false,
            tmp.path(),
            Some(&manifest),
        );
        assert!(matches!(
            result,
            EmergencyEvaluation::Skipped {
                reason: "restore_in_progress"
            }
        ));
    }

    // ---- Overflow suppression test ----

    #[test]
    fn test_skipped_on_overflow() {
        let tmp = TempDir::new().unwrap();
        let manifest = BTreeSet::new();

        let result = evaluate_emergency(
            100,
            Some(100),
            None,
            120,
            0.30,
            5,
            false,
            true,
            tmp.path(),
            Some(&manifest),
        );
        assert!(matches!(
            result,
            EmergencyEvaluation::Skipped {
                reason: "watcher_overflow_unreliable_counts"
            }
        ));
    }

    // ---- Manifest verification tests ----

    #[test]
    fn test_verify_deletions_only_counts_missing_files() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("exists.rs"), "fn main() {}").unwrap();

        let mut manifest = BTreeSet::new();
        manifest.insert("exists.rs".to_string());
        manifest.insert("deleted.rs".to_string());

        let verified = verify_deletions_against_manifest(tmp.path(), &manifest);
        assert_eq!(verified.len(), 1);
        assert!(verified.contains(&"deleted.rs".to_string()));
    }

    #[test]
    fn test_rename_old_path_counted_as_delete() {
        // A rename removes the old path — it IS correctly counted as deleted.
        // The new path isn't in the manifest, so it doesn't affect the count.
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("new_name.rs"), "content").unwrap();
        fs::write(tmp.path().join("still_here.rs"), "content").unwrap();

        let mut manifest = BTreeSet::new();
        manifest.insert("old_name.rs".to_string());
        manifest.insert("still_here.rs".to_string());

        let verified = verify_deletions_against_manifest(tmp.path(), &manifest);
        assert_eq!(verified.len(), 1);
        assert!(verified.contains(&"old_name.rs".to_string()));
    }

    // ---- No prior snapshot test ----

    #[test]
    fn test_skipped_no_baseline() {
        let tmp = TempDir::new().unwrap();
        let manifest = BTreeSet::new();

        let result = evaluate_emergency(
            100,
            None,
            None,
            120,
            0.30,
            5,
            false,
            false,
            tmp.path(),
            Some(&manifest),
        );
        assert!(matches!(
            result,
            EmergencyEvaluation::Skipped {
                reason: "no_prior_snapshot_or_empty_baseline"
            }
        ));
    }

    #[test]
    fn test_below_min_files_no_emergency() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = BTreeSet::new();
        for i in 0..3 {
            manifest.insert(format!("file{}.rs", i));
        }

        let result = evaluate_emergency(
            3,
            Some(10),
            None,
            120,
            0.30,
            5,
            false,
            false,
            tmp.path(),
            Some(&manifest),
        );
        assert!(matches!(result, EmergencyEvaluation::NoEmergency));
    }
}
