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

    let test_cases = vec![
        (UNIX_EPOCH - Duration::from_secs(86400), -86400),
        (
            UNIX_EPOCH - Duration::from_secs(365 * 86400),
            -(365 * 86400),
        ),
        (UNIX_EPOCH - Duration::from_millis(500), -1),
        (UNIX_EPOCH, 0),
        (UNIX_EPOCH + Duration::from_secs(1_000_000), 1_000_000),
    ];

    for (time, expected_i64) in &test_cases {
        let converted = uhoh::snapshot::mtime_to_i64(*time);
        assert_eq!(converted, *expected_i64);

        let back = uhoh::snapshot::i64_to_mtime(converted);
        if *expected_i64 < 0 {
            assert!(back < UNIX_EPOCH);
        }
    }

    let _ = uhoh::snapshot::i64_to_mtime(i64::MIN + 1);
    let _ = uhoh::snapshot::i64_to_mtime(0);
    let _ = uhoh::snapshot::i64_to_mtime(i64::MAX);
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
            .create_snapshot(
                "proj",
                i + 1,
                &ts.to_rfc3339(),
                trigger,
                message,
                pinned,
                &files,
                &[],
                &[],
            )
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
