use anyhow::Result;
use chrono::{DateTime, Duration, Utc};

use crate::config::CompactionConfig;
use crate::db::{Database, SnapshotRow};

/// Run compaction on a project's snapshots.
/// Uses bucket-based deduplication with O(1) bucket membership check via HashSet.
pub fn compact_project(
    database: &Database,
    project_hash: &str,
    config: &CompactionConfig,
) -> Result<u64> {
    let snapshots = database.list_snapshots(project_hash)?;
    let now = Utc::now();
    let mut deleted_count = 0u64;

    // Track occupied buckets for O(1) dominance checking
    let mut buckets_5min: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut buckets_hourly: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut buckets_daily: std::collections::HashSet<i64> = std::collections::HashSet::new();
    let mut buckets_weekly: std::collections::HashSet<i64> = std::collections::HashSet::new();

    // Snapshots are returned newest-first; process in that order so
    // newer snapshots take precedence in each bucket.
    for snapshot in &snapshots {
        // Pinned snapshots: always keep
        if snapshot.pinned {
            register_in_buckets(
                &snapshot,
                &now,
                config,
                &mut buckets_5min,
                &mut buckets_hourly,
                &mut buckets_daily,
                &mut buckets_weekly,
            );
            continue;
        }

        let ts = parse_timestamp(&snapshot.timestamp);
        let age = now.signed_duration_since(ts);

        // Emergency-delete snapshots expire after configured hours
        if snapshot.trigger == "emergency-delete"
            && age > Duration::hours(config.emergency_expire_hours as i64)
        {
            database.delete_snapshot(snapshot.rowid)?;
            deleted_count += 1;
            continue;
        }

        // Keep everything within keep_all_minutes
        if age < Duration::minutes(config.keep_all_minutes as i64) {
            register_in_buckets(
                &snapshot,
                &now,
                config,
                &mut buckets_5min,
                &mut buckets_hourly,
                &mut buckets_daily,
                &mut buckets_weekly,
            );
            continue;
        }

        // Manual commits with messages get minimum daily retention
        let is_manual_with_msg =
            snapshot.trigger == "manual" && !snapshot.message.is_empty();

        let dominated = if age < Duration::days(config.keep_5min_days as i64) {
            let bucket_secs = if is_manual_with_msg { 86400i64 } else { 300 };
            let bucket = ts.timestamp() / bucket_secs;
            if is_manual_with_msg {
                !buckets_daily.insert(bucket)
            } else {
                !buckets_5min.insert(bucket)
            }
        } else if age < Duration::days(config.keep_hourly_days as i64) {
            let bucket_secs = if is_manual_with_msg { 86400i64 } else { 3600 };
            let bucket = ts.timestamp() / bucket_secs;
            if is_manual_with_msg {
                !buckets_daily.insert(bucket)
            } else {
                !buckets_hourly.insert(bucket)
            }
        } else if age < Duration::days(config.keep_daily_days as i64) {
            let bucket = ts.timestamp() / 86400;
            !buckets_daily.insert(bucket)
        } else if config.keep_weekly_beyond {
            let bucket = ts.timestamp() / 604800;
            !buckets_weekly.insert(bucket)
        } else {
            true // No weekly retention: drop everything older
        };

        if dominated {
            database.delete_snapshot(snapshot.rowid)?;
            deleted_count += 1;
        }
    }

    Ok(deleted_count)
}

fn register_in_buckets(
    snapshot: &SnapshotRow,
    _now: &DateTime<Utc>,
    _config: &CompactionConfig,
    b5: &mut std::collections::HashSet<i64>,
    bh: &mut std::collections::HashSet<i64>,
    bd: &mut std::collections::HashSet<i64>,
    bw: &mut std::collections::HashSet<i64>,
) {
    let ts = parse_timestamp(&snapshot.timestamp);
    b5.insert(ts.timestamp() / 300);
    bh.insert(ts.timestamp() / 3600);
    bd.insert(ts.timestamp() / 86400);
    bw.insert(ts.timestamp() / 604800);
}

fn parse_timestamp(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}
