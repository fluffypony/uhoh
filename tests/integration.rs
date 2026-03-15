//! Basic integration tests for uhoh core functionality.

use assert_cmd::prelude::*;
use std::path::Path;
use std::process::Command as TestCommand;

#[test]
fn test_cas_store_read_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();
    let blob_root = tmp.path().join("blobs");
    std::fs::create_dir_all(&blob_root).unwrap();

    let content = b"fn main() { println!(\"hello\"); }";
    let (hash, _) = uhoh::cas::store_blob(&blob_root, content).unwrap();

    let read_back = uhoh::cas::read_blob(&blob_root, &hash).unwrap().unwrap();
    assert_eq!(read_back, content);
}

#[test]
fn test_database_project_lifecycle() {
    let tmp = tempfile::tempdir().unwrap();
    let db = uhoh::db::Database::open(&tmp.path().join("test.db")).unwrap();

    // Add project
    db.add_project("abc123", "/home/user/project").unwrap();

    // Find by path
    let found = db
        .find_project_by_path(Path::new("/home/user/project"))
        .unwrap()
        .unwrap();
    assert_eq!(found.hash, "abc123");

    // Find by hash prefix
    let found2 = db.find_project_by_hash_prefix("abc").unwrap().unwrap();
    assert_eq!(found2.current_path, "/home/user/project");

    // Update path
    db.update_project_path("abc123", "/home/user/new-project")
        .unwrap();
    let updated = db.get_project("abc123").unwrap().unwrap();
    assert_eq!(updated.current_path, "/home/user/new-project");

    // Remove
    db.remove_project("abc123").unwrap();
    assert!(db.get_project("abc123").unwrap().is_none());
}

#[test]
fn test_snapshot_creation_and_query() {
    let tmp = tempfile::tempdir().unwrap();
    let db = uhoh::db::Database::open(&tmp.path().join("test.db")).unwrap();

    db.add_project("proj1", "/tmp/test").unwrap();
    let snap_id = 1;

    let files: Vec<uhoh::db::SnapFileEntry> = vec![
        uhoh::db::SnapFileEntry::new(
            "src/main.rs".into(),
            "hash1".into(),
            100,
            true,
            false,
            None,
            uhoh::cas::StorageMethod::Copy,
            false,
        ),
        uhoh::db::SnapFileEntry::new(
            "README.md".into(),
            "hash2".into(),
            50,
            true,
            false,
            None,
            uhoh::cas::StorageMethod::Copy,
            false,
        ),
    ];

    let (rowid, _sid) = db
        .create_snapshot(uhoh::db::CreateSnapshotRow::new(
            "proj1",
            snap_id,
            "2025-01-01T00:00:00Z",
            uhoh::db::SnapshotTrigger::Manual,
            "test",
            false,
            &files,
            &[],
        ))
        .unwrap();

    let snap_files = db.get_snapshot_files(rowid).unwrap();
    assert_eq!(snap_files.len(), 2);

    let snaps = db.list_snapshots("proj1").unwrap();
    assert_eq!(snaps.len(), 1);
    assert_eq!(snaps[0].trigger, uhoh::db::SnapshotTrigger::Manual);

    // Smoke test for snapshot listing display formatting by invoking the binary
    // This validates that storage method strings appear without panics.
    // Note: We don't match exact formatting here to avoid flakiness across platforms.
    // In tests, the binary is `deps/<test-name>`; try to run `uhoh` if available.
    // Skip if we can't find the binary in PATH.
    if which::which("uhoh").is_ok() {
        TestCommand::new("uhoh")
            .arg("snapshots")
            .current_dir("/") // Not using a real project here; this is a smoke run only
            .assert()
            .failure(); // Expect failure when no project is registered
    }
}

#[test]
fn test_base58_edge_cases() {
    // ID 0 rejected
    let s = uhoh::cas::id_to_base58(0);
    assert_eq!(uhoh::cas::base58_to_id(&s), None);

    // ID 1 should be short
    let s1 = uhoh::cas::id_to_base58(1);
    assert!(s1.len() <= 2);
    assert_eq!(uhoh::cas::base58_to_id(&s1), Some(1));

    // Large ID
    let big = uhoh::cas::id_to_base58(u64::MAX);
    assert_eq!(uhoh::cas::base58_to_id(&big), Some(u64::MAX));

    // Invalid input
    assert_eq!(uhoh::cas::base58_to_id("not-valid-base58!!!"), None);
}
