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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    // ---- parse_timestamp tests ----

    #[test]
    fn parse_valid_rfc3339() {
        let ts = "2024-06-15T12:30:00Z";
        let parsed = parse_timestamp(ts).unwrap();
        assert_eq!(parsed, Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 0).unwrap());
    }

    #[test]
    fn parse_rfc3339_with_offset() {
        let ts = "2024-06-15T14:30:00+02:00";
        let parsed = parse_timestamp(ts).unwrap();
        // +02:00 means UTC is 12:30
        assert_eq!(parsed, Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 0).unwrap());
    }

    #[test]
    fn parse_rfc3339_with_fractional_seconds() {
        let ts = "2024-01-01T00:00:00.123456789Z";
        let parsed = parse_timestamp(ts);
        assert!(parsed.is_some());
        assert_eq!(parsed.unwrap().date_naive(), chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap());
    }

    #[test]
    fn parse_invalid_timestamp_returns_none() {
        assert!(parse_timestamp("not-a-date").is_none());
        assert!(parse_timestamp("").is_none());
        assert!(parse_timestamp("2024-13-01T00:00:00Z").is_none());
    }

    #[test]
    fn parse_epoch_timestamp() {
        let ts = "1970-01-01T00:00:00Z";
        let parsed = parse_timestamp(ts).unwrap();
        assert_eq!(parsed.timestamp(), 0);
    }

    // ---- CompactionBuckets tests ----

    fn make_snapshot_row(timestamp: &str, trigger: SnapshotTrigger, pinned: bool) -> SnapshotRow {
        SnapshotRow {
            rowid: 1,
            snapshot_id: 1,
            timestamp: timestamp.to_string(),
            trigger,
            message: String::new(),
            pinned,
            ai_summary: None,
            file_count: 0,
        }
    }

    #[test]
    fn buckets_register_populates_all_tiers() {
        let mut buckets = CompactionBuckets::default();
        let snap = make_snapshot_row("2024-06-15T12:30:00Z", SnapshotTrigger::Auto, false);
        buckets.register(&snap);

        let ts = Utc.with_ymd_and_hms(2024, 6, 15, 12, 30, 0).unwrap().timestamp();
        assert!(buckets.five_min.contains(&ts.div_euclid(300)));
        assert!(buckets.hourly.contains(&ts.div_euclid(3600)));
        assert!(buckets.daily.contains(&ts.div_euclid(86400)));
        assert!(buckets.weekly.contains(&ts.div_euclid(604800)));
    }

    #[test]
    fn buckets_same_5min_window_detected_as_dominated() {
        let mut buckets = CompactionBuckets::default();
        // Two timestamps 2 minutes apart — same 5-min bucket
        let snap1 = make_snapshot_row("2024-06-15T12:30:00Z", SnapshotTrigger::Auto, false);
        buckets.register(&snap1);

        let ts2 = Utc.with_ymd_and_hms(2024, 6, 15, 12, 32, 0).unwrap().timestamp();
        let bucket = ts2.div_euclid(300);
        // Inserting into an already-occupied bucket returns false (dominated)
        assert!(!buckets.five_min.insert(bucket));
    }

    #[test]
    fn buckets_different_5min_window_not_dominated() {
        let mut buckets = CompactionBuckets::default();
        let snap1 = make_snapshot_row("2024-06-15T12:30:00Z", SnapshotTrigger::Auto, false);
        buckets.register(&snap1);

        // 6 minutes later — different 5-min bucket
        let ts2 = Utc.with_ymd_and_hms(2024, 6, 15, 12, 36, 0).unwrap().timestamp();
        let bucket = ts2.div_euclid(300);
        assert!(buckets.five_min.insert(bucket));
    }

    #[test]
    fn buckets_fallback_for_invalid_timestamp() {
        let mut buckets = CompactionBuckets::default();
        let snap = make_snapshot_row("garbage", SnapshotTrigger::Auto, false);
        // Should not panic — falls back to 2000-01-01 epoch
        buckets.register(&snap);

        let fallback = Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap().timestamp();
        assert!(buckets.five_min.contains(&fallback.div_euclid(300)));
    }

    #[test]
    fn buckets_default_is_empty() {
        let buckets = CompactionBuckets::default();
        assert!(buckets.five_min.is_empty());
        assert!(buckets.hourly.is_empty());
        assert!(buckets.daily.is_empty());
        assert!(buckets.weekly.is_empty());
    }

    // ---- compact_project integration tests with temp DB ----

    fn make_test_db() -> (tempfile::TempDir, Database) {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Database::open(&tmp.path().join("test.db")).unwrap();
        db.add_project("proj1", "/fake/path").unwrap();
        (tmp, db)
    }

    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT_SNAPSHOT_ID: AtomicU64 = AtomicU64::new(1);

    fn insert_snapshot(
        db: &Database,
        timestamp: &str,
        trigger: SnapshotTrigger,
        pinned: bool,
        message: &str,
    ) {
        let id = NEXT_SNAPSHOT_ID.fetch_add(1, Ordering::Relaxed);
        db.create_snapshot(crate::db::CreateSnapshotRow::new(
            "proj1",
            id,
            timestamp,
            trigger,
            message,
            pinned,
            &[],
            &[],
        ))
        .unwrap();
    }

    #[test]
    fn compact_empty_project_frees_nothing() {
        let (_tmp, db) = make_test_db();
        let config = CompactionConfig::default();
        let freed = compact_project(&db, "proj1", &config).unwrap();
        assert_eq!(freed, 0);
    }

    #[test]
    fn compact_keeps_recent_snapshots() {
        let (_tmp, db) = make_test_db();
        let config = CompactionConfig::default();

        // Insert snapshots within keep_all_minutes (default 60 min)
        let now = Utc::now();
        for i in 0..5 {
            let ts = (now - Duration::minutes(i * 5)).to_rfc3339();
            insert_snapshot(&db, &ts, SnapshotTrigger::Auto, false, "");
        }

        let freed = compact_project(&db, "proj1", &config).unwrap();
        assert_eq!(freed, 0, "All recent snapshots should be kept");

        let remaining = db.list_snapshots("proj1").unwrap();
        assert_eq!(remaining.len(), 5);
    }

    #[test]
    fn compact_prunes_dominated_5min_buckets() {
        let (_tmp, db) = make_test_db();
        // Set keep_all_minutes=0 so all snapshots are subject to bucket logic
        let config = CompactionConfig::new(0, 14, 30, 180, true, 48);

        let now = Utc::now();
        // Insert two snapshots 1 minute apart (same 5-min bucket)
        // Both within keep_5min_days range (14 days)
        // Use explicit snapshot IDs to ensure deterministic sort order:
        // older timestamp gets lower ID, newer timestamp gets higher ID.
        // compact_project sorts by snapshot_id DESC, so the newer one (higher ID)
        // claims the bucket first, and the older one is dominated.
        let ts_older = (now - Duration::hours(2) - Duration::minutes(1)).to_rfc3339();
        let ts_newer = (now - Duration::hours(2)).to_rfc3339();
        db.create_snapshot(crate::db::CreateSnapshotRow::new(
            "proj1", 900_001, &ts_older, SnapshotTrigger::Auto, "", false, &[], &[],
        )).unwrap();
        db.create_snapshot(crate::db::CreateSnapshotRow::new(
            "proj1", 900_002, &ts_newer, SnapshotTrigger::Auto, "", false, &[], &[],
        )).unwrap();

        let before = db.list_snapshots("proj1").unwrap().len();
        assert_eq!(before, 2, "Should have 2 snapshots before compaction");
        compact_project(&db, "proj1", &config).unwrap();
        let after = db.list_snapshots("proj1").unwrap().len();

        assert!(
            after < before,
            "Dominated snapshot in same 5-min bucket should be pruned (before={before}, after={after})"
        );
    }

    #[test]
    fn compact_keeps_pinned_snapshots() {
        let (_tmp, db) = make_test_db();
        let config = CompactionConfig::new(0, 0, 0, 0, false, 48);

        let now = Utc::now();
        let ts = (now - Duration::days(365)).to_rfc3339();
        insert_snapshot(&db, &ts, SnapshotTrigger::Auto, true, "");

        compact_project(&db, "proj1", &config).unwrap();
        let remaining = db.list_snapshots("proj1").unwrap();
        assert_eq!(remaining.len(), 1, "Pinned snapshot should survive compaction");
    }

    #[test]
    fn compact_keeps_manual_with_message() {
        let (_tmp, db) = make_test_db();
        let config = CompactionConfig::new(0, 0, 0, 0, false, 48);

        let now = Utc::now();
        let ts = (now - Duration::days(365)).to_rfc3339();
        insert_snapshot(&db, &ts, SnapshotTrigger::Manual, false, "important commit");

        compact_project(&db, "proj1", &config).unwrap();
        let remaining = db.list_snapshots("proj1").unwrap();
        assert_eq!(remaining.len(), 1, "Manual commit with message should survive");
    }

    #[test]
    fn compact_drops_old_with_no_weekly_retention() {
        let (_tmp, db) = make_test_db();
        // Everything older than daily window is dropped
        let config = CompactionConfig::new(0, 0, 0, 0, false, 48);

        let now = Utc::now();
        let ts = (now - Duration::days(365)).to_rfc3339();
        insert_snapshot(&db, &ts, SnapshotTrigger::Auto, false, "");

        compact_project(&db, "proj1", &config).unwrap();
        let remaining = db.list_snapshots("proj1").unwrap();
        assert_eq!(remaining.len(), 0, "Ancient snapshot with no weekly retention should be pruned");
    }

    #[test]
    fn compact_keeps_emergency_within_retention() {
        let (_tmp, db) = make_test_db();
        let config = CompactionConfig::new(0, 0, 0, 0, false, 48);

        let now = Utc::now();
        // Emergency snapshot 1 hour ago — within 48h retention
        let ts = (now - Duration::hours(1)).to_rfc3339();
        insert_snapshot(&db, &ts, SnapshotTrigger::Emergency, false, "");

        compact_project(&db, "proj1", &config).unwrap();
        let remaining = db.list_snapshots("proj1").unwrap();
        assert_eq!(remaining.len(), 1, "Emergency within retention should survive");
    }
}
