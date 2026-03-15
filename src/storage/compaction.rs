use anyhow::Result;
use chrono::{DateTime, Duration, TimeZone, Utc};

use crate::config::CompactionConfig;
use crate::db::{Database, SnapshotRow, SnapshotTrigger};

/// Run compaction on a project's snapshots.
/// Uses bucket-based deduplication with O(1) bucket membership check via HashSet.
pub fn compact_project(
    database: &Database,
    project_hash: &str,
    config: &CompactionConfig,
) -> Result<u64> {
    let mut snapshots = database.list_snapshots(project_hash)?;
    // Guard against timestamp clock skew by making retention winner selection
    // deterministic on monotonic snapshot_id.
    snapshots.sort_by(|a, b| b.snapshot_id.cmp(&a.snapshot_id));
    let now = Utc::now();
    let mut freed_bytes = 0u64;
    let emergency_retention = Duration::hours(config.emergency_expire_hours as i64);

    // Track occupied buckets for O(1) dominance checking
    let mut buckets = CompactionBuckets::default();

    // Predecessor protection: protect the snapshot immediately preceding any
    // retained emergency snapshot. This ensures the pre-deletion state survives
    // as long as the emergency snapshot does.
    let mut protected_predecessors: std::collections::HashSet<i64> =
        std::collections::HashSet::new();

    // First pass: identify protected predecessors.
    for snapshot in &snapshots {
        if snapshot.trigger == SnapshotTrigger::Emergency {
            let ts = parse_timestamp(&snapshot.timestamp)
                .unwrap_or_else(|| Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap());
            let age = now.signed_duration_since(ts);
            if age < emergency_retention {
                // Emergency still within retention: protect its predecessor
                if let Ok(Some(pred)) = database.snapshot_before(project_hash, snapshot.snapshot_id)
                {
                    protected_predecessors.insert(pred.rowid);
                }
            }
        }
    }

    // Snapshots are returned newest-first; process in that order so
    // newer snapshots take precedence in each bucket. If this order ever changes,
    // bucket retention would need to be revisited to ensure newer snapshots are kept.
    for snapshot in &snapshots {
        // Pinned snapshots: always keep
        if snapshot.pinned {
            buckets.register(snapshot);
            continue;
        }

        // Protected predecessors of retained emergency snapshots: always keep
        if protected_predecessors.contains(&snapshot.rowid) {
            buckets.register(snapshot);
            continue;
        }

        let ts = parse_timestamp(&snapshot.timestamp)
            .unwrap_or_else(|| Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap());
        let age = now.signed_duration_since(ts);

        // Emergency snapshots are immune to bucket-deduplication pruning within
        // the configured retention window.
        if snapshot.trigger == SnapshotTrigger::Emergency && age < emergency_retention {
            buckets.register(snapshot);
            continue;
            // After retention window expires, fall through to normal bucket logic
        }

        // Keep everything within keep_all_minutes
        if age < Duration::minutes(config.keep_all_minutes as i64) {
            buckets.register(snapshot);
            continue;
        }

        // Manual commits with messages are always retained (like pinned snapshots).
        // They represent explicit user saves and should never be pruned by bucket dedup.
        if snapshot.trigger == SnapshotTrigger::Manual && !snapshot.message.is_empty() {
            buckets.register(snapshot);
            continue;
        }

        let dominated = if age < Duration::days(config.keep_5min_days as i64) {
            let bucket = ts.timestamp().div_euclid(300);
            !buckets.five_min.insert(bucket)
        } else if age < Duration::days(config.keep_hourly_days as i64) {
            let bucket = ts.timestamp().div_euclid(3600);
            !buckets.hourly.insert(bucket)
        } else if age < Duration::days(config.keep_daily_days as i64) {
            let bucket = ts.timestamp().div_euclid(86400);
            !buckets.daily.insert(bucket)
        } else if config.keep_weekly_beyond {
            let bucket = ts.timestamp().div_euclid(604800);
            !buckets.weekly.insert(bucket)
        } else {
            true // No weekly retention: drop everything older
        };

        if dominated {
            let est = database
                .estimate_snapshot_blob_size(snapshot.rowid)
                .unwrap_or(0);
            freed_bytes = freed_bytes.saturating_add(est);
            database.delete_snapshot(snapshot.rowid)?;
        }
    }

    Ok(freed_bytes)
}
#[derive(Default)]
struct CompactionBuckets {
    five_min: std::collections::HashSet<i64>,
    hourly: std::collections::HashSet<i64>,
    daily: std::collections::HashSet<i64>,
    weekly: std::collections::HashSet<i64>,
}

impl CompactionBuckets {
    fn register(&mut self, snapshot: &SnapshotRow) {
        let ts = parse_timestamp(&snapshot.timestamp)
            .unwrap_or_else(|| Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap());
        self.five_min.insert(ts.timestamp().div_euclid(300));
        self.hourly.insert(ts.timestamp().div_euclid(3600));
        self.daily.insert(ts.timestamp().div_euclid(86400));
        self.weekly.insert(ts.timestamp().div_euclid(604800));
    }
}

fn parse_timestamp(s: &str) -> Option<DateTime<Utc>> {
    match DateTime::parse_from_rfc3339(s) {
        Ok(dt) => Some(dt.with_timezone(&Utc)),
        Err(e) => {
            tracing::warn!(
                "Failed to parse timestamp '{}': {} — treating as fallback epoch for pruning safety",
                s,
                e
            );
            None
        }
    }
}
