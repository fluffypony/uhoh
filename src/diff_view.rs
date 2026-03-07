use anyhow::{Context, Result};
use once_cell::sync::Lazy;
use serde::Serialize;
use similar::{ChangeTag, TextDiff};
use std::collections::HashSet;
use std::io::Write;
use std::path::Path;

use crate::cas;
use crate::db::{Database, ProjectEntry};
use chrono::TimeZone;
use syntect::easy::HighlightLines;
// Style imported implicitly via ranges; suppress unused warnings by not importing it explicitly
use syntect::util::as_24_bit_terminal_escaped;

// Lazy-load syntect assets (avoid ~100ms hit per invocation)
static SYNTAX_SET: Lazy<syntect::parsing::SyntaxSet> =
    Lazy::new(syntect::parsing::SyntaxSet::load_defaults_newlines);
static THEME_SET: Lazy<syntect::highlighting::ThemeSet> =
    Lazy::new(syntect::highlighting::ThemeSet::load_defaults);

#[derive(Debug, Clone, Serialize)]
pub struct DiffLine {
    pub change_type: String,
    pub old_line: Option<usize>,
    pub new_line: Option<usize>,
    pub content: String,
    pub highlighted_html: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiffHunk {
    pub old_start: usize,
    pub old_count: usize,
    pub new_start: usize,
    pub new_count: usize,
    pub lines: Vec<DiffLine>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FileDiff {
    pub path: String,
    pub status: String,
    pub hunks: Vec<DiffHunk>,
    pub too_large: bool,
    pub binary: bool,
}

pub const MAX_STRUCTURED_DIFF_BYTES: usize = 2 * 1024 * 1024;

pub fn compute_structured_diff(
    old_content: &[u8],
    new_content: &[u8],
    file_path: &str,
    with_highlighting: bool,
) -> FileDiff {
    let is_binary = content_inspector::inspect(&old_content[..old_content.len().min(8192)])
        .is_binary()
        || content_inspector::inspect(&new_content[..new_content.len().min(8192)]).is_binary();
    if is_binary {
        return FileDiff {
            path: file_path.to_string(),
            status: "modified".to_string(),
            hunks: Vec::new(),
            too_large: false,
            binary: true,
        };
    }

    if old_content.len() > MAX_STRUCTURED_DIFF_BYTES
        || new_content.len() > MAX_STRUCTURED_DIFF_BYTES
    {
        return FileDiff {
            path: file_path.to_string(),
            status: "modified".to_string(),
            hunks: Vec::new(),
            too_large: true,
            binary: false,
        };
    }

    let old_str = String::from_utf8_lossy(old_content);
    let new_str = String::from_utf8_lossy(new_content);
    let status = if old_str.is_empty() && !new_str.is_empty() {
        "added"
    } else if !old_str.is_empty() && new_str.is_empty() {
        "deleted"
    } else {
        "modified"
    };

    let diff = TextDiff::from_lines(old_str.as_ref(), new_str.as_ref());
    let syntax = if with_highlighting {
        let ext = std::path::Path::new(file_path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("");
        Some(
            SYNTAX_SET
                .find_syntax_by_extension(ext)
                .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text()),
        )
    } else {
        None
    };

    let mut lines = Vec::new();
    let mut old_line: usize = 0;
    let mut new_line: usize = 0;

    for change in diff.iter_all_changes() {
        let (change_type, o_line, n_line) = match change.tag() {
            ChangeTag::Equal => {
                old_line += 1;
                new_line += 1;
                ("context", Some(old_line), Some(new_line))
            }
            ChangeTag::Delete => {
                old_line += 1;
                ("remove", Some(old_line), None)
            }
            ChangeTag::Insert => {
                new_line += 1;
                ("add", None, Some(new_line))
            }
        };

        let content = change.value().trim_end_matches('\n').to_string();
        let highlighted_html = if with_highlighting {
            if let Some(syntax_ref) = syntax {
                let mut html_gen = syntect::html::ClassedHTMLGenerator::new_with_class_style(
                    syntax_ref,
                    &SYNTAX_SET,
                    syntect::html::ClassStyle::Spaced,
                );
                let line_with_newline = format!("{}\n", &content);
                let _ = html_gen.parse_html_for_line_which_includes_newline(&line_with_newline);
                Some(html_gen.finalize())
            } else {
                None
            }
        } else {
            None
        };

        lines.push(DiffLine {
            change_type: change_type.to_string(),
            old_line: o_line,
            new_line: n_line,
            content,
            highlighted_html,
        });
    }

    let (old_start, new_start) = lines
        .iter()
        .map(|line| (line.old_line.unwrap_or(1), line.new_line.unwrap_or(1)))
        .next()
        .unwrap_or((1, 1));

    let hunk = DiffHunk {
        old_start,
        old_count: lines.iter().filter(|l| l.old_line.is_some()).count(),
        new_start,
        new_count: lines.iter().filter(|l| l.new_line.is_some()).count(),
        lines,
    };

    FileDiff {
        path: file_path.to_string(),
        status: status.to_string(),
        hunks: vec![hunk],
        too_large: false,
        binary: false,
    }
}

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

    // Track whether files2 represents current working tree (not stored in CAS)
    let is_current_tree = label2 == "current";
    let project_path = Path::new(&project.current_path);

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

        let old_bytes = old_hash.and_then(|h| cas::read_blob(&blob_root, h).ok().flatten());
        let new_bytes = if is_current_tree {
            let file_on_disk = project_path.join(cas::decode_relpath_to_os(path));
            // Use symlink_metadata to avoid following symlinks; read symlink target as content
            if let Ok(meta) = std::fs::symlink_metadata(&file_on_disk) {
                if meta.file_type().is_symlink() {
                    std::fs::read_link(&file_on_disk)
                        .ok()
                        .map(|t| t.to_string_lossy().into_owned().into_bytes())
                } else {
                    std::fs::read(&file_on_disk).ok()
                }
            } else {
                None
            }
        } else {
            new_hash.and_then(|h| cas::read_blob(&blob_root, h).ok().flatten())
        };

        // Detect binary content before converting to string
        let old_is_binary = old_bytes
            .as_ref()
            .map(|b| content_inspector::inspect(&b[..b.len().min(8192)]).is_binary())
            .unwrap_or(false);
        let new_is_binary = new_bytes
            .as_ref()
            .map(|b| content_inspector::inspect(&b[..b.len().min(8192)]).is_binary())
            .unwrap_or(false);

        if old_is_binary || new_is_binary {
            let display_path = if path.strip_prefix("b64:").is_some() {
                crate::cas::decode_relpath_to_os(path)
                    .to_string_lossy()
                    .into_owned()
            } else {
                path.to_string()
            };
            writeln!(stdout, "\n--- {label1}/{display_path}")?;
            writeln!(stdout, "+++ {label2}/{display_path}")?;
            writeln!(stdout, "[Binary files differ]")?;
            continue;
        }

        let old_content = old_bytes
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();
        let new_content = new_bytes
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default();

        if old_content.len() > MAX_DIFF_BYTES || new_content.len() > MAX_DIFF_BYTES {
            writeln!(stdout, "\n--- {label1}/{path}")?;
            writeln!(stdout, "+++ {label2}/{path}")?;
            writeln!(
                stdout,
                "[Diff skipped due to large file size > {MAX_DIFF_BYTES} bytes]"
            )?;
            continue;
        }

        let display_path = if path.strip_prefix("b64:").is_some() {
            let os = crate::cas::decode_relpath_to_os(path);
            os.to_string_lossy().into_owned()
        } else {
            path.to_string()
        };
        writeln!(stdout, "\n--- {label1}/{display_path}")?;
        writeln!(stdout, "+++ {label2}/{display_path}")?;

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
                if let Ok(ranges) =
                    highlighter.highlight_line(change.to_string().as_str(), &SYNTAX_SET)
                {
                    let escaped = as_24_bit_terminal_escaped(&ranges, false);
                    write!(stdout, "{sign}{escaped}")?;
                } else {
                    write!(stdout, "{sign}{change}")?;
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
            .find(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.timestamp)
                    .map(|d| d.with_timezone(&chrono::Utc) <= target)
                    .unwrap_or(false)
            })
    } else if let Ok(ts) = chrono::NaiveDateTime::parse_from_str(id_str, "%Y-%m-%dT%H:%M:%S") {
        let target = chrono::Utc.from_utc_datetime(&ts);
        database
            .list_snapshots(&project.hash)?
            .into_iter()
            .find(|s| {
                chrono::DateTime::parse_from_rfc3339(&s.timestamp)
                    .map(|d| d.with_timezone(&chrono::Utc) <= target)
                    .unwrap_or(false)
            })
    } else if crate::cas::base58_to_id(id_str).is_some() {
        database.find_snapshot_by_base58(&project.hash, id_str)?
    } else {
        None
    }
    .context("Snapshot not found")?;

    let files = database.get_snapshot_files(snap.rowid)?;
    let entry = files
        .iter()
        .find(|f| {
            if f.path == file_path {
                return true;
            }
            if file_path.strip_prefix("b64:").is_some() {
                return f.path == file_path;
            }
            // Try decoding stored path for non-UTF8 encoding
            let stored_os = crate::cas::decode_relpath_to_os(&f.path);
            stored_os.to_string_lossy() == file_path
        })
        .ok_or_else(|| anyhow::anyhow!("File '{file_path}' not in snapshot {id_str}"))?;

    if !entry.stored {
        anyhow::bail!("File content was not stored (binary file too large)");
    }

    let blob_root = uhoh_dir.join("blobs");
    let content = cas::read_blob(&blob_root, &entry.hash)?
        .ok_or_else(|| anyhow::anyhow!("Blob missing for {file_path}"))?;

    std::io::stdout().write_all(&content)?;
    Ok(())
}

pub fn cmd_log(database: &Database, project: &ProjectEntry, file_path: &str) -> Result<()> {
    let history = database.file_history(&project.hash, file_path)?;
    if history.is_empty() {
        println!("No history found for '{file_path}'");
        return Ok(());
    }

    println!("History of '{file_path}':");
    let mut prev_hash = String::new();
    for (snapshot_id, timestamp, hash, trigger) in &history {
        let id_str = cas::id_to_base58(*snapshot_id);
        let changed = if hash != &prev_hash {
            "changed"
        } else {
            "same"
        };
        println!("  {timestamp} [{id_str}] {changed} ({trigger})");
        prev_hash = hash.clone();
    }
    Ok(())
}

/// Build a file list from the current working directory (for diffing against current state).
fn build_current_file_list_readonly(project_path: &Path) -> Result<Vec<crate::db::FileEntryRow>> {
    let walker = crate::ignore_rules::build_walker(project_path);
    let mut entries = Vec::new();
    for entry in walker {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let path = entry.path();
        if path.file_name().is_some_and(|n| n == ".uhoh") {
            continue;
        }
        let meta = match std::fs::symlink_metadata(path) {
            Ok(m) => m,
            Err(_) => continue,
        };
        let ft = meta.file_type();
        if !ft.is_file() && !ft.is_symlink() {
            continue;
        }
        let rel_path = match path.strip_prefix(project_path) {
            Ok(r) => cas::encode_relpath(r),
            Err(_) => continue,
        };
        let (hash, size, is_symlink, executable) = if ft.is_symlink() {
            let target = match std::fs::read_link(path) {
                Ok(t) => t,
                Err(_) => continue, // Skip unreadable symlinks
            };
            #[cfg(unix)]
            let target_bytes = {
                use std::os::unix::ffi::OsStrExt;
                target.as_os_str().as_bytes().to_vec()
            };
            #[cfg(not(unix))]
            let target_bytes = target.to_string_lossy().into_owned().into_bytes();

            (
                blake3::hash(&target_bytes).to_hex().to_string(),
                target_bytes.len() as u64,
                true,
                false,
            )
        } else {
            let hash_result: std::io::Result<(String, u64)> = (|| {
                let mut hasher = blake3::Hasher::new();
                let mut f = std::fs::File::open(path)?;
                let mut buf = [0u8; 64 * 1024];
                let mut total = 0u64;
                loop {
                    let n = std::io::Read::read(&mut f, &mut buf)?;
                    if n == 0 {
                        break;
                    }
                    hasher.update(&buf[..n]);
                    total += n as u64;
                }
                Ok((hasher.finalize().to_hex().to_string(), total))
            })();
            let (hash, size) = match hash_result {
                Ok(v) => v,
                Err(_) => continue, // Skip unreadable files
            };
            (hash, size, false, cas::is_executable(path))
        };
        entries.push(crate::db::FileEntryRow {
            path: rel_path,
            hash,
            size,
            stored: false,
            executable,
            mtime: None,
            storage_method: 0,
            is_symlink,
        });
    }
    Ok(entries)
}
// safe_truncate: removed unused function (dead code)
