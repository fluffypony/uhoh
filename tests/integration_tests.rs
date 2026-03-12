use tempfile::TempDir;

#[cfg(unix)]
#[test]
fn test_non_utf8_path_roundtrip() {
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let dir = TempDir::new().unwrap();
    let project = dir.path().join("project");
    std::fs::create_dir_all(&project).unwrap();

    let bad_name = OsStr::from_bytes(&[0x66, 0x6f, 0x6f, 0xff, 0xfe, 0x62, 0x61, 0x72]);
    // Some filesystems (notably macOS) reject non-UTF-8 filenames at create time,
    // so validate round-trip encoding without requiring on-disk creation.
    let bad_rel = std::path::Path::new(bad_name);

    let b64_collision = project.join("b64:notactuallybase64");
    std::fs::write(&b64_collision, b"collision test content").unwrap();

    let encoded = uhoh::cas::encode_relpath(bad_rel);
    assert!(encoded.starts_with("b64:"));

    let decoded = uhoh::cas::decode_relpath_to_os(&encoded);
    assert_eq!(decoded.as_os_str().as_bytes(), bad_name.as_bytes());

    let rel2 = b64_collision.strip_prefix(&project).unwrap();
    let encoded2 = uhoh::cas::encode_relpath(rel2);
    assert!(encoded2.starts_with("b64:"));
    let decoded2 = uhoh::cas::decode_relpath_to_os(&encoded2);
    assert_eq!(decoded2.to_string_lossy(), "b64:notactuallybase64");
}

#[test]
fn test_pre_1970_mtime_roundtrip() {
    use std::time::{Duration, UNIX_EPOCH};

    // mtime_to_millis returns milliseconds; negative values round outward from epoch
    let test_cases: Vec<(std::time::SystemTime, i64)> = vec![
        (UNIX_EPOCH - Duration::from_secs(86400), -(86400 * 1000 + 1)),
        (
            UNIX_EPOCH - Duration::from_secs(365 * 86400),
            -(365i64 * 86400 * 1000 + 1),
        ),
        (UNIX_EPOCH - Duration::from_millis(500), -501),
        (UNIX_EPOCH, 0),
        (UNIX_EPOCH + Duration::from_secs(1_000_000), 1_000_000_000),
    ];

    for (time, expected_i64) in &test_cases {
        let converted = uhoh::snapshot::mtime_to_millis(*time);
        assert_eq!(converted, *expected_i64);

        let back = uhoh::snapshot::millis_to_mtime(converted);
        if *expected_i64 < 0 {
            assert!(back < UNIX_EPOCH);
        }
    }

    let _ = uhoh::snapshot::millis_to_mtime(i64::MIN + 1);
    let _ = uhoh::snapshot::millis_to_mtime(0);
    let _ = uhoh::snapshot::millis_to_mtime(i64::MAX);
}

#[cfg(unix)]
#[test]
fn test_symlink_capture_stats_and_restore() {
    let dir = TempDir::new().unwrap();
    let project = dir.path().join("project");
    let blob_root = dir.path().join("blobs");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::create_dir_all(&blob_root).unwrap();

    let real_file = project.join("real.txt");
    std::fs::write(&real_file, b"hello world").unwrap();

    let link_path = project.join("link.txt");
    std::os::unix::fs::symlink("real.txt", &link_path).unwrap();

    let dangling = project.join("dangling");
    std::os::unix::fs::symlink("nonexistent_target", &dangling).unwrap();

    let subdir = project.join("subdir");
    std::fs::create_dir_all(&subdir).unwrap();
    let dir_link = project.join("dir_link");
    std::os::unix::fs::symlink("subdir", &dir_link).unwrap();

    let (hash, size, bytes_written) =
        uhoh::cas::store_symlink_target(&blob_root, &link_path).unwrap();
    assert_eq!(size, "real.txt".len() as u64);
    assert!(bytes_written > 0);

    let (hash2, _, bytes_written2) =
        uhoh::cas::store_symlink_target(&blob_root, &link_path).unwrap();
    assert_eq!(hash, hash2);
    assert_eq!(bytes_written2, 0);

    let (_, size_dangling, _) = uhoh::cas::store_symlink_target(&blob_root, &dangling).unwrap();
    assert_eq!(size_dangling, "nonexistent_target".len() as u64);

    let (_, size_dirlink, _) = uhoh::cas::store_symlink_target(&blob_root, &dir_link).unwrap();
    assert_eq!(size_dirlink, "subdir".len() as u64);

    let blob_content = uhoh::cas::read_blob(&blob_root, &hash).unwrap().unwrap();
    assert_eq!(blob_content, b"real.txt");

    let restore_dir = dir.path().join("restored");
    std::fs::create_dir_all(&restore_dir).unwrap();
    let restored_link = restore_dir.join("link.txt");
    uhoh::restore::restore_symlink_target(&blob_content, &restored_link).unwrap();

    let meta = std::fs::symlink_metadata(&restored_link).unwrap();
    assert!(meta.file_type().is_symlink());
    let target = std::fs::read_link(&restored_link).unwrap();
    assert_eq!(target.to_string_lossy(), "real.txt");
}

#[tokio::test]
async fn test_http_range_resume() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let data = vec![0xABu8; 10_000];
    let data_clone = data.clone();
    let drop_first = Arc::new(AtomicBool::new(true));
    let drop_clone = drop_first.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let data = data_clone.clone();
            let should_drop = drop_clone.clone();

            tokio::spawn(async move {
                use tokio::io::{AsyncReadExt, AsyncWriteExt};

                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap();
                let request = String::from_utf8_lossy(&buf[..n]);

                let range_start =
                    if let Some(range_line) = request.lines().find(|l| l.starts_with("Range:")) {
                        let bytes_eq = range_line.find("bytes=").unwrap() + 6;
                        let dash = range_line.find('-').unwrap();
                        range_line[bytes_eq..dash].parse::<usize>().unwrap_or(0)
                    } else {
                        0
                    };

                let slice = &data[range_start..];

                if should_drop.load(Ordering::Relaxed) && range_start == 0 {
                    should_drop.store(false, Ordering::Relaxed);
                    let half = slice.len() / 2;
                    let response = format!(
                        "HTTP/1.1 206 Partial Content\r\n\
                         Content-Length: {}\r\n\
                         Content-Range: bytes {}-{}/{}\r\n\r\n",
                        half,
                        range_start,
                        range_start + half - 1,
                        data.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.write_all(&slice[..half]).await;
                    return;
                }

                let status = if range_start > 0 {
                    "206 Partial Content"
                } else {
                    "200 OK"
                };
                let response = format!(
                    "HTTP/1.1 {}\r\n\
                     Content-Length: {}\r\n\
                     Content-Range: bytes {}-{}/{}\r\n\r\n",
                    status,
                    slice.len(),
                    range_start,
                    data.len() - 1,
                    data.len()
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.write_all(slice).await;
            });
        }
    });

    let dir = TempDir::new().unwrap();
    let out_path = dir.path().join("test_download");

    let url = format!("http://{addr}/model.bin");
    let client = reqwest::Client::new();

    let mut pos: u64 = 0;
    let mut out = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&out_path)
        .unwrap();

    for _ in 0..3 {
        let mut req = client.get(&url);
        if pos > 0 {
            req = req.header("Range", format!("bytes={pos}-"));
        }

        let resp = req.send().await.unwrap();
        if pos > 0 && resp.status() == reqwest::StatusCode::OK {
            pos = 0;
            out.set_len(0).unwrap();
            use std::io::Seek;
            out.seek(std::io::SeekFrom::Start(0)).unwrap();
        }

        let bytes = resp.bytes().await.unwrap();
        if bytes.is_empty() {
            break;
        }
        use std::io::Write;
        out.write_all(&bytes).unwrap();
        pos += bytes.len() as u64;

        if pos >= 10_000 {
            break;
        }
    }

    let result = std::fs::read(&out_path).unwrap();
    assert_eq!(result.len(), 10_000);
    assert_eq!(result, data);

    server.abort();
}

#[test]
fn test_base58_id_roundtrip() {
    assert_eq!(uhoh::cas::id_to_base58(0), "");
    assert_eq!(uhoh::cas::base58_to_id(""), None);

    let encoded = uhoh::cas::id_to_base58(1);
    assert!(!encoded.is_empty());
    assert_eq!(uhoh::cas::base58_to_id(&encoded), Some(1));

    let encoded_large = uhoh::cas::id_to_base58(u64::MAX);
    assert_eq!(uhoh::cas::base58_to_id(&encoded_large), Some(u64::MAX));

    for id in [1u64, 2, 57, 58, 59, 100, 1000, 1_000_000, u64::MAX - 1] {
        let enc = uhoh::cas::id_to_base58(id);
        let dec = uhoh::cas::base58_to_id(&enc);
        assert_eq!(dec, Some(id));
    }
}

#[cfg(feature = "compression")]
#[test]
fn test_compression_blob_roundtrip() {
    let dir = TempDir::new().unwrap();
    let blob_root = dir.path().join("blobs");
    std::fs::create_dir_all(&blob_root).unwrap();

    let content = b"Hello, this is test content that should compress well. Repeated text repeated text repeated text repeated text.";

    let (hash, bytes_written) = uhoh::cas::store_blob(&blob_root, content).unwrap();
    assert!(bytes_written > 0);

    let read_back = uhoh::cas::read_blob(&blob_root, &hash).unwrap().unwrap();
    assert_eq!(read_back.as_slice(), content);

    let test_file = dir.path().join("test_input.txt");
    std::fs::write(&test_file, content).unwrap();

    let (hash2, _size, _method, _disk_bytes) = uhoh::cas::store_blob_from_file(
        &blob_root,
        &test_file,
        u64::MAX,
        u64::MAX,
        u64::MAX,
        true,
        3,
    )
    .unwrap();

    let read_back2 = uhoh::cas::read_blob(&blob_root, &hash2).unwrap().unwrap();
    assert_eq!(read_back2.as_slice(), content);
    assert_eq!(hash, hash2);
}

#[test]
fn test_compaction_preserves_pinned() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("test.db");
    let database = uhoh::db::Database::open(&db_path).unwrap();

    database.add_project("proj", "/tmp/proj").unwrap();
    let files: Vec<uhoh::db::SnapFileEntry> = vec![];

    let mut pin_rowid = None;
    for i in 0..8u64 {
        let ts = chrono::Utc::now() - chrono::Duration::days((60 - (i as i64) * 5).max(1));
        let trigger = if i % 2 == 0 { "manual" } else { "auto" };
        let message = if i == 0 { "keep" } else { "" };
        let pinned = i == 0;
        let (rowid, _) = database
            .create_snapshot(uhoh::db::CreateSnapshotRow {
                project_hash: "proj",
                snapshot_id: i + 1,
                timestamp: &ts.to_rfc3339(),
                trigger,
                message,
                pinned,
                files: &files,
                deleted: &[],
            })
            .unwrap();
        if pinned {
            pin_rowid = Some(rowid);
        }
    }

    let cfg = uhoh::config::CompactionConfig::default();
    let _ = uhoh::compaction::compact_project(&database, "proj", &cfg).unwrap();

    let snaps = database.list_snapshots("proj").unwrap();
    let pinned_rowid = pin_rowid.unwrap();
    assert!(snaps.iter().any(|s| s.rowid == pinned_rowid));
}

// ---- Emergency delete detection integration tests ----

/// Mass deletion in create_snapshot triggers dynamic auto→emergency upgrade.
#[test]
fn test_dynamic_trigger_upgrade_on_mass_delete() {
    let tmp = TempDir::new().unwrap();
    let uhoh_dir = tmp.path().join(".uhoh");
    std::fs::create_dir_all(uhoh_dir.join("blobs")).unwrap();
    let db = uhoh::db::Database::open(&uhoh_dir.join("test.db")).unwrap();
    let project_dir = tmp.path().join("project");
    std::fs::create_dir_all(&project_dir).unwrap();

    // Create 20 files
    for i in 0..20 {
        std::fs::write(project_dir.join(format!("file{}.rs", i)), "content").unwrap();
    }

    db.add_project("emrg1", project_dir.to_str().unwrap())
        .unwrap();
    let cfg = uhoh::config::Config::default();

    // First snapshot captures all 20 files
    let snap1 = uhoh::snapshot::create_snapshot(
        &uhoh_dir,
        &db,
        &cfg,
        uhoh::snapshot::CreateSnapshotRequest {
            project_hash: "emrg1",
            project_path: &project_dir,
            trigger: "auto",
            message: None,
            changed_paths: None,
        },
    )
    .unwrap();
    assert!(snap1.is_some());

    // Delete 15 of 20 files (75% > 30% threshold, 15 > 5 min_files)
    for i in 0..15 {
        std::fs::remove_file(project_dir.join(format!("file{}.rs", i))).unwrap();
    }

    // Second snapshot should auto-upgrade to "emergency"
    let snap2 = uhoh::snapshot::create_snapshot(
        &uhoh_dir,
        &db,
        &cfg,
        uhoh::snapshot::CreateSnapshotRequest {
            project_hash: "emrg1",
            project_path: &project_dir,
            trigger: "auto",
            message: None,
            changed_paths: None,
        },
    )
    .unwrap();
    assert!(snap2.is_some());

    let snaps = db.list_snapshots("emrg1").unwrap();
    let latest = &snaps[0]; // newest first
    assert_eq!(
        latest.trigger, "emergency",
        "Expected trigger upgrade to 'emergency'"
    );
    assert!(
        latest.message.contains("Mass delete detected"),
        "Expected auto-generated emergency message, got: {}",
        latest.message
    );
}

/// Sub-threshold deletions produce normal "auto" trigger, not "emergency".
#[test]
fn test_sub_threshold_no_emergency_upgrade() {
    let tmp = TempDir::new().unwrap();
    let uhoh_dir = tmp.path().join(".uhoh");
    std::fs::create_dir_all(uhoh_dir.join("blobs")).unwrap();
    let db = uhoh::db::Database::open(&uhoh_dir.join("test.db")).unwrap();
    let project_dir = tmp.path().join("project");
    std::fs::create_dir_all(&project_dir).unwrap();

    // Create 100 files
    for i in 0..100 {
        std::fs::write(project_dir.join(format!("file{}.rs", i)), "content").unwrap();
    }

    db.add_project("emrg2", project_dir.to_str().unwrap())
        .unwrap();
    let cfg = uhoh::config::Config::default();

    let _ = uhoh::snapshot::create_snapshot(
        &uhoh_dir,
        &db,
        &cfg,
        uhoh::snapshot::CreateSnapshotRequest {
            project_hash: "emrg2",
            project_path: &project_dir,
            trigger: "auto",
            message: None,
            changed_paths: None,
        },
    )
    .unwrap();

    // Delete 2 files (2% < 30% threshold)
    std::fs::remove_file(project_dir.join("file0.rs")).unwrap();
    std::fs::remove_file(project_dir.join("file1.rs")).unwrap();

    let snap2 = uhoh::snapshot::create_snapshot(
        &uhoh_dir,
        &db,
        &cfg,
        uhoh::snapshot::CreateSnapshotRequest {
            project_hash: "emrg2",
            project_path: &project_dir,
            trigger: "auto",
            message: None,
            changed_paths: None,
        },
    )
    .unwrap();
    assert!(snap2.is_some());

    let snaps = db.list_snapshots("emrg2").unwrap();
    assert_eq!(
        snaps[0].trigger, "auto",
        "Should remain 'auto' for sub-threshold delete"
    );
}

/// Emergency snapshots within retention window survive compaction.
#[test]
fn test_emergency_snapshot_retained_within_window() {
    let tmp = TempDir::new().unwrap();
    let db = uhoh::db::Database::open(&tmp.path().join("test.db")).unwrap();
    db.add_project("emrg3", "/fake/path").unwrap();

    // Create snapshots with emergency trigger
    let ts_recent = chrono::Utc::now().to_rfc3339();
    let files: Vec<uhoh::db::SnapFileEntry> = Vec::new();
    let deleted: Vec<(String, String, u64, bool, uhoh::cas::StorageMethod)> = Vec::new();

    let (rowid, _) = db
        .create_snapshot(uhoh::db::CreateSnapshotRow {
            project_hash: "emrg3",
            snapshot_id: 0,
            timestamp: &ts_recent,
            trigger: "emergency",
            message: "mass delete",
            pinned: false,
            files: &files,
            deleted: &deleted,
        })
        .unwrap();

    let cfg = uhoh::config::CompactionConfig::default();
    let _ = uhoh::compaction::compact_project(&db, "emrg3", &cfg).unwrap();

    let snaps = db.list_snapshots("emrg3").unwrap();
    assert!(
        snaps.iter().any(|s| s.rowid == rowid),
        "Emergency snapshot within retention window should survive compaction"
    );
}

/// Emergency snapshot outside retention window is eligible for pruning.
#[test]
fn test_emergency_snapshot_pruned_after_window() {
    let tmp = TempDir::new().unwrap();
    let db = uhoh::db::Database::open(&tmp.path().join("test.db")).unwrap();
    db.add_project("emrg4", "/fake/path").unwrap();

    // Create emergency snapshot with old timestamp (72h ago, beyond 48h default)
    let old_ts = (chrono::Utc::now() - chrono::Duration::hours(72)).to_rfc3339();
    let files: Vec<uhoh::db::SnapFileEntry> = Vec::new();
    let deleted: Vec<(String, String, u64, bool, uhoh::cas::StorageMethod)> = Vec::new();

    let (old_rowid, _) = db
        .create_snapshot(uhoh::db::CreateSnapshotRow {
            project_hash: "emrg4",
            snapshot_id: 0,
            timestamp: &old_ts,
            trigger: "emergency",
            message: "old mass delete",
            pinned: false,
            files: &files,
            deleted: &deleted,
        })
        .unwrap();

    // Create a recent snapshot so the old one can be dominated in buckets
    let recent_ts = chrono::Utc::now().to_rfc3339();
    let (_, _) = db
        .create_snapshot(uhoh::db::CreateSnapshotRow {
            project_hash: "emrg4",
            snapshot_id: 0,
            timestamp: &recent_ts,
            trigger: "auto",
            message: "",
            pinned: false,
            files: &files,
            deleted: &deleted,
        })
        .unwrap();

    let cfg = uhoh::config::CompactionConfig {
        emergency_expire_hours: 48,
        keep_all_minutes: 0,
        keep_5min_days: 0,
        keep_hourly_days: 0,
        keep_daily_days: 0,
        keep_weekly_beyond: false,
    };
    let _ = uhoh::compaction::compact_project(&db, "emrg4", &cfg).unwrap();

    let snaps = db.list_snapshots("emrg4").unwrap();
    assert!(
        !snaps.iter().any(|s| s.rowid == old_rowid),
        "Emergency snapshot beyond retention window should be prunable"
    );
}

/// Predecessor of a retained emergency snapshot is protected from compaction.
#[test]
fn test_predecessor_protection_in_compaction() {
    let tmp = TempDir::new().unwrap();
    let db = uhoh::db::Database::open(&tmp.path().join("test.db")).unwrap();
    db.add_project("emrg5", "/fake/path").unwrap();

    let files: Vec<uhoh::db::SnapFileEntry> = Vec::new();
    let deleted: Vec<(String, String, u64, bool, uhoh::cas::StorageMethod)> = Vec::new();

    // Create predecessor snapshot (old but not emergency)
    let pred_ts = (chrono::Utc::now() - chrono::Duration::hours(2)).to_rfc3339();
    let (pred_rowid, _) = db
        .create_snapshot(uhoh::db::CreateSnapshotRow {
            project_hash: "emrg5",
            snapshot_id: 0,
            timestamp: &pred_ts,
            trigger: "auto",
            message: "",
            pinned: false,
            files: &files,
            deleted: &deleted,
        })
        .unwrap();

    // Create emergency snapshot (recent, within retention)
    let emrg_ts = (chrono::Utc::now() - chrono::Duration::hours(1)).to_rfc3339();
    let (emrg_rowid, _) = db
        .create_snapshot(uhoh::db::CreateSnapshotRow {
            project_hash: "emrg5",
            snapshot_id: 0,
            timestamp: &emrg_ts,
            trigger: "emergency",
            message: "mass delete",
            pinned: false,
            files: &files,
            deleted: &deleted,
        })
        .unwrap();

    // Aggressive compaction config that would normally prune old snapshots
    let cfg = uhoh::config::CompactionConfig {
        emergency_expire_hours: 48,
        keep_all_minutes: 0,
        keep_5min_days: 0,
        keep_hourly_days: 0,
        keep_daily_days: 0,
        keep_weekly_beyond: false,
    };
    let _ = uhoh::compaction::compact_project(&db, "emrg5", &cfg).unwrap();

    let snaps = db.list_snapshots("emrg5").unwrap();
    assert!(
        snaps.iter().any(|s| s.rowid == emrg_rowid),
        "Emergency snapshot should survive compaction"
    );
    assert!(
        snaps.iter().any(|s| s.rowid == pred_rowid),
        "Predecessor of retained emergency snapshot should be protected"
    );
}

/// Fast-path directory deletion falls back to full scan.
#[test]
fn test_fast_path_directory_deletion_fallback() {
    let tmp = TempDir::new().unwrap();
    let uhoh_dir = tmp.path().join(".uhoh");
    std::fs::create_dir_all(uhoh_dir.join("blobs")).unwrap();
    let db = uhoh::db::Database::open(&uhoh_dir.join("test.db")).unwrap();
    let project_dir = tmp.path().join("project");
    std::fs::create_dir_all(project_dir.join("src")).unwrap();

    // Create files in a subdirectory
    std::fs::write(project_dir.join("src/main.rs"), "fn main() {}").unwrap();
    std::fs::write(project_dir.join("src/lib.rs"), "pub fn hello() {}").unwrap();
    std::fs::write(project_dir.join("Cargo.toml"), "[package]").unwrap();

    db.add_project("dirtest", project_dir.to_str().unwrap())
        .unwrap();
    let cfg = uhoh::config::Config::default();

    // Take initial snapshot
    let _ = uhoh::snapshot::create_snapshot(
        &uhoh_dir,
        &db,
        &cfg,
        uhoh::snapshot::CreateSnapshotRequest {
            project_hash: "dirtest",
            project_path: &project_dir,
            trigger: "auto",
            message: None,
            changed_paths: None,
        },
    )
    .unwrap();

    // Delete the src directory entirely
    std::fs::remove_dir_all(project_dir.join("src")).unwrap();

    // Use incremental path with changed_paths pointing to the deleted directory
    let changed = vec![project_dir.join("src")];
    let snap2 = uhoh::snapshot::create_snapshot(
        &uhoh_dir,
        &db,
        &cfg,
        uhoh::snapshot::CreateSnapshotRequest {
            project_hash: "dirtest",
            project_path: &project_dir,
            trigger: "auto",
            message: None,
            changed_paths: Some(&changed),
        },
    )
    .unwrap();
    assert!(
        snap2.is_some(),
        "Should detect directory deletion as a change"
    );

    // Verify the resulting snapshot only has Cargo.toml (src/* deleted)
    let snaps = db.list_snapshots("dirtest").unwrap();
    let latest_rowid = snaps[0].rowid;
    let files = db.get_snapshot_files(latest_rowid).unwrap();
    let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
    assert!(paths.contains(&"Cargo.toml"), "Cargo.toml should survive");
    assert!(
        !paths.iter().any(|p| p.starts_with("src/")),
        "src/* should be deleted"
    );
}

/// Non-daemon restore should create and remove marker file used by daemon
/// emergency suppression fallback.
#[test]
fn test_restore_sets_and_clears_restore_marker_file() {
    let tmp = TempDir::new().unwrap();
    let uhoh_dir = tmp.path().join(".uhoh");
    std::fs::create_dir_all(uhoh_dir.join("blobs")).unwrap();
    let db = uhoh::db::Database::open(&uhoh_dir.join("test.db")).unwrap();
    let project_dir = tmp.path().join("project");
    std::fs::create_dir_all(&project_dir).unwrap();

    std::fs::write(project_dir.join("keep.txt"), "v1").unwrap();
    db.add_project("restmark", project_dir.to_str().unwrap())
        .unwrap();
    let cfg = uhoh::config::Config::default();

    // Initial snapshot
    let snap = uhoh::snapshot::create_snapshot(
        &uhoh_dir,
        &db,
        &cfg,
        uhoh::snapshot::CreateSnapshotRequest {
            project_hash: "restmark",
            project_path: &project_dir,
            trigger: "manual",
            message: Some("baseline"),
            changed_paths: None,
        },
    )
    .unwrap()
    .unwrap();
    let snap_id = uhoh::cas::id_to_base58(snap);

    // Modify file then restore
    std::fs::write(project_dir.join("keep.txt"), "v2").unwrap();
    let project = uhoh::resolve::resolve_project(&db, Some("restmark"), None).unwrap();
    let outcome = uhoh::restore::restore_project(
        &uhoh_dir,
        &db,
        &project,
        uhoh::restore::RestoreRequest {
            snapshot_id: &snap_id,
            target_path: None,
            dry_run: false,
            force: true,
            pre_restore_snapshot: Some(uhoh::restore::PreRestoreSnapshot {
                trigger: "pre-restore",
                message: Some(format!("Before restore to {snap_id}")),
                config: &cfg,
            }),
            confirm_large_delete: None,
        },
    )
    .unwrap();
    assert!(outcome.applied);

    let marker = uhoh_dir.join(".restore-in-progress");
    assert!(
        !marker.exists(),
        "restore marker file should be cleaned up after restore"
    );
}

#[test]
fn test_ledger_verify_tolerates_unknown_source_severity_strings() {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("test.db");
    let db = uhoh::db::Database::open(&db_path).unwrap();
    let conn = rusqlite::Connection::open(&db_path).unwrap();

    let ts = chrono::Utc::now().to_rfc3339();
    let detail = Some("seed".to_string());
    conn.execute(
        "INSERT INTO event_ledger (
            ts, source, event_type, severity, project_hash, agent_name, guard_name,
            path, detail, pre_state_ref, post_state_ref, prev_hash, causal_parent, resolved
        ) VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, NULL, ?5, NULL, NULL, '', NULL, 0)",
        rusqlite::params![
            ts,
            "legacy_source",
            "manual_injected",
            "legacy_severity",
            detail
        ],
    )
    .unwrap();
    let id = conn.last_insert_rowid();

    let mut hasher = blake3::Hasher::new();
    hasher.update(b"");
    hasher.update(&[0u8]);
    hasher.update(id.to_string().as_bytes());
    hasher.update(&[0u8]);
    hasher.update(ts.as_bytes());
    hasher.update(&[0u8]);
    hasher.update(b"legacy_source");
    hasher.update(&[0u8]);
    hasher.update(b"manual_injected");
    hasher.update(&[0u8]);
    hasher.update(b"legacy_severity");
    hasher.update(&[0u8]);
    hasher.update(b"");
    hasher.update(&[0u8]);
    hasher.update(b"");
    hasher.update(&[0u8]);
    hasher.update(b"");
    hasher.update(&[0u8]);
    hasher.update(b"");
    hasher.update(&[0u8]);
    hasher.update(b"seed");
    hasher.update(&[0u8]);
    hasher.update(b"");
    hasher.update(&[0u8]);
    hasher.update(b"");
    hasher.update(&[0u8]);
    hasher.update(b"");
    let chain_hash = hasher.finalize().to_hex().to_string();
    conn.execute(
        "UPDATE event_ledger SET prev_hash = ?1 WHERE id = ?2",
        rusqlite::params![chain_hash, id],
    )
    .unwrap();

    let (count, broken) = db.verify_event_ledger_chain().unwrap();
    assert_eq!(count, 1);
    assert!(!broken.contains(&id));
}

#[test]
fn test_list_db_guards_rejects_invalid_engine_or_mode() {
    let tmp = TempDir::new().unwrap();
    let db = uhoh::db::Database::open(&tmp.path().join("test.db")).unwrap();

    let conn = rusqlite::Connection::open(tmp.path().join("test.db")).unwrap();
    conn.execute(
        "INSERT INTO db_guards (name, engine, connection_ref, tables_csv, watched_tables_cache, mode, created_at, active)
         VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, 1)",
        rusqlite::params![
            "bad_guard",
            "oracle",
            "sqlite:///tmp/demo.db",
            "*",
            "triggers",
            chrono::Utc::now().to_rfc3339()
        ],
    )
    .unwrap();

    let err = db
        .list_db_guards()
        .expect_err("invalid db_guard engine must error");
    assert!(err.to_string().contains("invalid db_guard engine"));
}
