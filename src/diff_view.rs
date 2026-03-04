use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use similar::{ChangeTag, TextDiff};
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use crate::cas;
use chrono::TimeZone;
use crate::db::{Database, ProjectEntry};
use syntect::easy::HighlightLines;
// Style imported implicitly via ranges; suppress unused warnings by not importing it explicitly
use syntect::util::as_24_bit_terminal_escaped;

// Lazy-load syntect assets (avoid ~100ms hit per invocation)
static SYNTAX_SET: Lazy<syntect::parsing::SyntaxSet> =
    Lazy::new(|| syntect::parsing::SyntaxSet::load_defaults_newlines());
static THEME_SET: Lazy<syntect::highlighting::ThemeSet> =
    Lazy::new(|| syntect::highlighting::ThemeSet::load_defaults());

pub fn cmd_diff(
    uhoh_dir: &Path,
    database: &Database,
    project: &ProjectEntry,
    id1: Option<&str>,
    id2: Option<&str>,
) -> Result<()> {
    let blob_root = uhoh_dir.join("blobs");

    let (files1, files2, label1, label2) = match (id1, id2) {
        (Some(a), Some(b)) => {
            let s1 = database
                .find_snapshot_by_base58(&project.hash, a)?
                .context("First snapshot not found")?;
            let s2 = database
                .find_snapshot_by_base58(&project.hash, b)?
                .context("Second snapshot not found")?;
            let f1 = database.get_snapshot_files(s1.rowid)?;
            let f2 = database.get_snapshot_files(s2.rowid)?;
            (f1, f2, a.to_string(), b.to_string())
        }
        (Some(a), None) => {
            let s1 = database
                .find_snapshot_by_base58(&project.hash, a)?
                .context("Snapshot not found")?;
            let f1 = database.get_snapshot_files(s1.rowid)?;
            let f2 = build_current_file_list_readonly(Path::new(&project.current_path))?;
            (f1, f2, a.to_string(), "current".to_string())
        }
        (None, None) => {
            // Diff latest snapshot vs current
            if let Some(rowid) = database.latest_snapshot_rowid(&project.hash)? {
                let f1 = database.get_snapshot_files(rowid)?;
                let f2 = build_current_file_list_readonly(Path::new(&project.current_path))?;
                (f1, f2, "latest".to_string(), "current".to_string())
            } else {
                println!("No snapshots to diff against.");
                return Ok(());
            }
        }
        _ => anyhow::bail!("Invalid diff arguments"),
    };

    let map1: std::collections::HashMap<&str, &str> = files1
        .iter()
        .map(|f| (f.path.as_str(), f.hash.as_str()))
        .collect();
    let map2: std::collections::HashMap<&str, &str> = files2
        .iter()
        .map(|f| (f.path.as_str(), f.hash.as_str()))
        .collect();

    let all_paths: HashSet<&str> = map1.keys().chain(map2.keys()).copied().collect();
    let mut sorted_paths: Vec<&str> = all_paths.into_iter().collect();
    sorted_paths.sort();

    let mut stdout = std::io::stdout().lock();

    // Cap for very large files to avoid excessive memory in diffing
    const MAX_DIFF_BYTES: usize = 2 * 1024 * 1024; // 2 MiB
    for path in sorted_paths {
        let old_hash = map1.get(path).copied();
        let new_hash = map2.get(path).copied();

        if old_hash == new_hash {
            continue;
        }

        let old_content = old_hash
            .and_then(|h| cas::read_blob(&blob_root, h).ok().flatten())
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let new_content = new_hash
            .and_then(|h| cas::read_blob(&blob_root, h).ok().flatten())
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();

        if old_content.len() > MAX_DIFF_BYTES || new_content.len() > MAX_DIFF_BYTES {
            writeln!(stdout, "\n--- {}/{}", label1, path)?;
            writeln!(stdout, "+++ {}/{}", label2, path)?;
            writeln!(stdout, "[Diff skipped due to large file size > {} bytes]", MAX_DIFF_BYTES)?;
            continue;
        }

        writeln!(stdout, "\n--- {}/{}", label1, path)?;
        writeln!(stdout, "+++ {}/{}", label2, path)?;

        let diff = TextDiff::from_lines(&old_content, &new_content);
        // Try to detect syntax
        let syntax = SYNTAX_SET
            .find_syntax_for_file(path)
            .ok()
            .flatten()
            .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text());
        let theme = &THEME_SET.themes["base16-eighties.dark"];
        let mut highlighter = HighlightLines::new(syntax, theme);
        for hunk in diff.unified_diff().context_radius(3).iter_hunks() {
            writeln!(stdout, "{}", hunk.header())?;
            for change in hunk.iter_changes() {
                let sign = match change.tag() {
                    ChangeTag::Delete => "-",
                    ChangeTag::Insert => "+",
                    ChangeTag::Equal => " ",
                };
                // Apply highlighting per line
                if let Ok(ranges) = highlighter.highlight_line(change.to_string().as_str(), &SYNTAX_SET) {
                    let escaped = as_24_bit_terminal_escaped(&ranges, false);
                    write!(stdout, "{}{}", sign, escaped)?;
                } else {
                    write!(stdout, "{}{}", sign, change)?;
                }
            }
        }
    }

    Ok(())
}

pub fn cmd_cat(
    uhoh_dir: &Path,
    database: &Database,
    project: &ProjectEntry,
    file_path: &str,
    id_str: &str,
) -> Result<()> {
    // Try RFC3339/timestamp formats first (less ambiguous), then base58 ID
    let snap = if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(id_str) {
        let target = ts.with_timezone(&chrono::Utc);
        database
            .list_snapshots(&project.hash)?
            .into_iter()
            .find(|s| chrono::DateTime::parse_from_rfc3339(&s.timestamp).map(|d| d.with_timezone(&chrono::Utc) <= target).unwrap_or(false))
    } else if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(id_str, "%Y-%m-%dT%H:%M:%S") {
        let target = chrono::Utc.from_utc_datetime(&ts);
        database
            .list_snapshots(&project.hash)?
            .into_iter()
            .find(|s| chrono::DateTime::parse_from_rfc3339(&s.timestamp).map(|d| d.with_timezone(&chrono::Utc) <= target).unwrap_or(false))
    } else if let Some(_) = crate::cas::base58_to_id(id_str) {
        database.find_snapshot_by_base58(&project.hash, id_str)?
    } else {
        None
    }
    .context("Snapshot not found")?;

    let files = database.get_snapshot_files(snap.rowid)?;
    let entry = files
        .iter()
        .find(|f| f.path == file_path)
        .ok_or_else(|| anyhow::anyhow!("File '{}' not in snapshot {}", file_path, id_str))?;

    if !entry.stored {
        anyhow::bail!("File content was not stored (binary file too large)");
    }

    let blob_root = uhoh_dir.join("blobs");
    let content = cas::read_blob(&blob_root, &entry.hash)?
        .ok_or_else(|| anyhow::anyhow!("Blob missing for {}", file_path))?;

    std::io::stdout().write_all(&content)?;
    Ok(())
}

pub fn cmd_log(database: &Database, project: &ProjectEntry, file_path: &str) -> Result<()> {
    let history = database.file_history(&project.hash, file_path)?;
    if history.is_empty() {
        println!("No history found for '{}'", file_path);
        return Ok(());
    }

    println!("History of '{}':", file_path);
    let mut prev_hash = String::new();
    for (snapshot_id, timestamp, hash, trigger) in &history {
        let id_str = cas::id_to_base58(*snapshot_id);
        let changed = if hash != &prev_hash { "changed" } else { "same" };
        println!("  {} [{}] {} ({})", timestamp, id_str, changed, trigger);
        prev_hash = hash.clone();
    }
    Ok(())
}

/// Build a file list from the current working directory (for diffing against current state).
fn build_current_file_list_readonly(
    project_path: &Path,
) -> Result<Vec<crate::db::FileEntryRow>> {
    let walker = crate::ignore_rules::build_walker(project_path);
    let mut entries = Vec::new();
    for entry in walker {
        let entry = match entry { Ok(e) => e, Err(_) => continue };
        let path = entry.path();
        if !path.is_file() { continue; }
        if path.file_name().map_or(false, |n| n == ".uhoh") { continue; }
        let rel_path = match path.strip_prefix(project_path) { Ok(r) => cas::normalize_path(r), Err(_) => continue };
        // Hash only, do not store in CAS
        let (hash, size) = {
            let mut hasher = blake3::Hasher::new();
            let mut f = std::fs::File::open(path)?;
            let mut buf = [0u8; 64 * 1024];
            let mut total = 0u64;
            loop {
                let n = std::io::Read::read(&mut f, &mut buf)?;
                if n == 0 { break; }
                hasher.update(&buf[..n]);
                total += n as u64;
            }
            (hasher.finalize().to_hex().to_string(), total)
        };
        entries.push(crate::db::FileEntryRow {
            path: rel_path,
            hash,
            size,
            stored: false,
            executable: cas::is_executable(path),
            mtime: None,
            storage_method: 0,
        });
    }
    Ok(entries)
}
// safe_truncate: removed unused function (dead code)
