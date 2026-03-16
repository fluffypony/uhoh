//! Emergency delete detection logic.
//!
//! Provides pure functions for threshold evaluation and manifest-diff
//! verification, keeping the daemon event loop focused on orchestration.

use std::collections::BTreeSet;
use std::path::Path;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub enum EmergencyEvaluation {
    NoEmergency,
    Triggered {
        verified_deleted_count: usize,
        baseline_count: u64,
        ratio: f64,
        deleted_paths_sample: Vec<String>,
    },
    CooldownSuppressed {
        verified_deleted_count: usize,
        baseline_count: u64,
        ratio: f64,
        cooldown_remaining_secs: u64,
    },
    Skipped {
        reason: &'static str,
    },
}

#[non_exhaustive]
pub struct EmergencyEvalInput<'a> {
    pub deleted_paths_hint_count: usize,
    pub cached_baseline_count: Option<u64>,
    pub last_emergency_at: Option<Instant>,
    pub cooldown_secs: u64,
    pub threshold: f64,
    pub min_files: usize,
    pub restore_in_progress: bool,
    pub overflow_occurred: bool,
    pub project_root: &'a Path,
    pub cached_manifest: Option<&'a BTreeSet<String>>,
}

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

pub fn deletion_ratio(deleted_count: usize, baseline_count: u64) -> f64 {
    if baseline_count == 0 {
        return 0.0;
    }
    deleted_count as f64 / baseline_count as f64
}

pub fn expand_directory_deletion(
    deleted_path_rel: &str,
    manifest: &BTreeSet<String>,
) -> Vec<String> {
    // Normalize backslashes to forward slashes for cross-platform matching
    let normalized = deleted_path_rel.replace('\\', "/");
    let prefix = if normalized.ends_with('/') {
        normalized
    } else {
        format!("{normalized}/")
    };

    manifest
        .range(prefix.clone()..)
        .take_while(|p| p.starts_with(&prefix))
        .cloned()
        .collect()
}

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

pub fn evaluate_emergency(input: EmergencyEvalInput<'_>) -> EmergencyEvaluation {
    let EmergencyEvalInput {
        deleted_paths_hint_count,
        cached_baseline_count,
        last_emergency_at,
        cooldown_secs,
        threshold,
        min_files,
        restore_in_progress,
        overflow_occurred,
        project_root,
        cached_manifest,
    } = input;

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

    let Some(manifest) = cached_manifest else {
        return EmergencyEvaluation::Skipped {
            reason: "no_cached_manifest_for_verification",
        };
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

pub fn severity_for_ratio(ratio: f64) -> crate::db::LedgerSeverity {
    if ratio >= 0.5 {
        crate::db::LedgerSeverity::Critical
    } else {
        crate::db::LedgerSeverity::Warn
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
        assert_eq!(severity_for_ratio(0.5), crate::db::LedgerSeverity::Critical);
        assert_eq!(severity_for_ratio(0.9), crate::db::LedgerSeverity::Critical);
    }

    #[test]
    fn test_severity_warn() {
        assert_eq!(severity_for_ratio(0.3), crate::db::LedgerSeverity::Warn);
        assert_eq!(severity_for_ratio(0.49), crate::db::LedgerSeverity::Warn);
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
            let name = format!("file{i}.rs");
            manifest.insert(name);
            // Don't create the files -> they appear "deleted"
        }

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 10,
            cached_baseline_count: Some(10),
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
        assert!(matches!(result, EmergencyEvaluation::Triggered { .. }));

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 10,
            cached_baseline_count: Some(10),
            last_emergency_at: Some(Instant::now()),
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
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
            manifest.insert(format!("file{i}.rs"));
        }

        let long_ago = Instant::now() - Duration::from_secs(300);
        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 10,
            cached_baseline_count: Some(10),
            last_emergency_at: Some(long_ago),
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
        assert!(matches!(result, EmergencyEvaluation::Triggered { .. }));
    }

    // ---- Restore suppression test ----

    #[test]
    fn test_skipped_during_restore() {
        let tmp = TempDir::new().unwrap();
        let manifest = BTreeSet::new();

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 100,
            cached_baseline_count: Some(100),
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: true,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
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

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 100,
            cached_baseline_count: Some(100),
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: true,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
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

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 100,
            cached_baseline_count: None,
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
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
            manifest.insert(format!("file{i}.rs"));
        }

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 3,
            cached_baseline_count: Some(10),
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
        assert!(matches!(result, EmergencyEvaluation::NoEmergency));
    }

    // ---- Additional edge-case tests ----

    #[test]
    fn test_deletion_ratio_normal() {
        let r = deletion_ratio(30, 100);
        assert!((r - 0.3).abs() < f64::EPSILON);
    }

    #[test]
    fn test_deletion_ratio_zero_deletes() {
        assert_eq!(deletion_ratio(0, 100), 0.0);
    }

    #[test]
    fn test_deletion_ratio_all_deleted() {
        assert_eq!(deletion_ratio(100, 100), 1.0);
    }

    #[test]
    fn test_deletion_ratio_more_deleted_than_baseline() {
        // Can happen when baseline is stale
        let r = deletion_ratio(150, 100);
        assert!((r - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_severity_boundary_just_below_critical() {
        assert_eq!(
            severity_for_ratio(0.4999),
            crate::db::LedgerSeverity::Warn
        );
    }

    #[test]
    fn test_severity_exactly_at_critical() {
        assert_eq!(
            severity_for_ratio(0.5),
            crate::db::LedgerSeverity::Critical
        );
    }

    #[test]
    fn test_severity_at_zero() {
        assert_eq!(severity_for_ratio(0.0), crate::db::LedgerSeverity::Warn);
    }

    #[test]
    fn test_severity_at_one() {
        assert_eq!(
            severity_for_ratio(1.0),
            crate::db::LedgerSeverity::Critical
        );
    }

    #[test]
    fn test_expand_backslash_normalized() {
        let mut manifest = BTreeSet::new();
        manifest.insert("src/main.rs".to_string());
        manifest.insert("src/lib.rs".to_string());

        // Windows-style backslash should be normalized
        let expanded = expand_directory_deletion("src\\", &manifest);
        assert_eq!(expanded.len(), 2);
    }

    #[test]
    fn test_expand_empty_manifest() {
        let manifest = BTreeSet::new();
        let expanded = expand_directory_deletion("src", &manifest);
        assert!(expanded.is_empty());
    }

    #[test]
    fn test_expand_no_prefix_collision() {
        // "src" should not match "srclib/foo.rs"
        let mut manifest = BTreeSet::new();
        manifest.insert("srclib/foo.rs".to_string());
        manifest.insert("src/bar.rs".to_string());

        let expanded = expand_directory_deletion("src", &manifest);
        assert_eq!(expanded.len(), 1);
        assert!(expanded.contains(&"src/bar.rs".to_string()));
    }

    #[test]
    fn test_verify_deletions_empty_manifest() {
        let tmp = TempDir::new().unwrap();
        let manifest = BTreeSet::new();
        let verified = verify_deletions_against_manifest(tmp.path(), &manifest);
        assert!(verified.is_empty());
    }

    #[test]
    fn test_verify_deletions_all_present() {
        let tmp = TempDir::new().unwrap();
        fs::write(tmp.path().join("a.rs"), "a").unwrap();
        fs::write(tmp.path().join("b.rs"), "b").unwrap();

        let mut manifest = BTreeSet::new();
        manifest.insert("a.rs".to_string());
        manifest.insert("b.rs".to_string());

        let verified = verify_deletions_against_manifest(tmp.path(), &manifest);
        assert!(verified.is_empty());
    }

    #[test]
    fn test_verify_deletions_all_missing() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = BTreeSet::new();
        manifest.insert("gone1.rs".to_string());
        manifest.insert("gone2.rs".to_string());

        let verified = verify_deletions_against_manifest(tmp.path(), &manifest);
        assert_eq!(verified.len(), 2);
    }

    #[test]
    fn test_skipped_on_empty_baseline() {
        let tmp = TempDir::new().unwrap();
        let manifest = BTreeSet::new();

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 100,
            cached_baseline_count: Some(0),
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
        assert!(matches!(
            result,
            EmergencyEvaluation::Skipped {
                reason: "no_prior_snapshot_or_empty_baseline"
            }
        ));
    }

    #[test]
    fn test_skipped_no_manifest() {
        let tmp = TempDir::new().unwrap();

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 100,
            cached_baseline_count: Some(100),
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: None,
        });
        assert!(matches!(
            result,
            EmergencyEvaluation::Skipped {
                reason: "no_cached_manifest_for_verification"
            }
        ));
    }

    #[test]
    fn test_triggered_sample_capped_at_20() {
        let tmp = TempDir::new().unwrap();
        let mut manifest = BTreeSet::new();
        for i in 0..50 {
            manifest.insert(format!("file{i:03}.rs"));
        }

        let result = evaluate_emergency(EmergencyEvalInput {
            deleted_paths_hint_count: 50,
            cached_baseline_count: Some(50),
            last_emergency_at: None,
            cooldown_secs: 120,
            threshold: 0.30,
            min_files: 5,
            restore_in_progress: false,
            overflow_occurred: false,
            project_root: tmp.path(),
            cached_manifest: Some(&manifest),
        });
        match result {
            EmergencyEvaluation::Triggered {
                deleted_paths_sample,
                ..
            } => {
                assert_eq!(deleted_paths_sample.len(), 20);
            }
            other => panic!("Expected Triggered, got {:?}", other),
        }
    }
}
