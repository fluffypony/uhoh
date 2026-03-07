use anyhow::{bail, Context, Result};
use std::path::Path;
use std::process::Command;

use crate::cas;
use crate::db::{Database, ProjectEntry};
use which::which;

/// Create a git stash entry from a snapshot using plumbing commands.
/// This does NOT modify the working tree — it constructs the stash directly
/// in the git object database.
pub fn cmd_gitstash(
    uhoh_dir: &Path,
    database: &Database,
    project: &ProjectEntry,
    id_str: &str,
) -> Result<()> {
    let project_path = Path::new(&project.current_path);
    let git_dir = resolve_git_dir(project_path)?;

    // Verify this is a git repo
    if !git_dir.exists() {
        bail!("Not a git repository. Git stash requires a git repo.");
    }

    let snap = database
        .find_snapshot_by_base58(&project.hash, id_str)?
        .context("Snapshot not found")?;

    let files = database.get_snapshot_files(snap.rowid)?;
    let blob_root = uhoh_dir.join("blobs");

    // Warn about unstored files before proceeding
    let unstored: Vec<&str> = files
        .iter()
        .filter(|f| !f.stored)
        .map(|f| f.path.as_str())
        .collect();
    if !unstored.is_empty() {
        tracing::warn!(
            "WARNING: {} file(s) are not stored and will be omitted from the stash.",
            unstored.len()
        );
        for p in unstored.iter().take(10) {
            tracing::warn!("  - {} (not recoverable)", p);
        }
        if unstored.len() > 10 {
            tracing::warn!("  ... and {} more", unstored.len() - 10);
        }
        eprintln!(
            "⚠ {} file(s) are not recoverable and will be omitted from the stash. \
             Applying this stash may effectively delete those files from your working tree.",
            unstored.len()
        );
    }

    // Step 1: Create git blobs for each file in the snapshot
    let mut tree_entries: Vec<(String, String)> = Vec::new(); // (path, git_blob_hash)

    for file in &files {
        if !file.stored {
            continue;
        }
        let content = cas::read_blob(&blob_root, &file.hash)?
            .ok_or_else(|| anyhow::anyhow!("Blob missing for {}", file.path))?;

        // Create git blob object
        let output = Command::new("git")
            .current_dir(project_path)
            .args(["hash-object", "-w", "--stdin"])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child.stdin.take().unwrap().write_all(&content)?;
                child.wait_with_output()
            })
            .context("Failed to create git blob")?;

        if !output.status.success() {
            bail!(
                "git hash-object failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        let git_hash = String::from_utf8(output.stdout)?.trim().to_string();
        tree_entries.push((file.path.clone(), git_hash));
    }

    // Step 2: Build tree object using temporary index to avoid corrupting user's index
    let tmp_index = git_dir.join("index.uhoh-tmp");
    // Use per-command environment to avoid global side effects
    run_git_with_index(project_path, &tmp_index, &["read-tree", "--empty"])?;
    // Build a lookup for executable flag from DB
    let mut exec_map = std::collections::HashSet::new();
    let snap_files = database.get_snapshot_files(snap.rowid)?;
    for f in &snap_files {
        if f.executable {
            exec_map.insert(f.path.as_str());
        }
    }
    let mut symlink_map = std::collections::HashSet::new();
    for f in &snap_files {
        if f.is_symlink {
            symlink_map.insert(f.path.as_str());
        }
    }

    // Use a single git update-index --index-info process and stream entries to stdin
    let mut upd = Command::new("git")
        .current_dir(project_path)
        .env("GIT_INDEX_FILE", &tmp_index)
        .args(["update-index", "-z", "--index-info"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .context("Failed to spawn git update-index --index-info")?;
    if let Some(mut sin) = upd.stdin.take() {
        use std::io::Write as _;
        for (path, blob_hash) in &tree_entries {
            // Decode b64:-encoded paths back to filesystem paths for git,
            // normalizing to forward slashes (git index requires forward slashes).
            let git_path = if path.starts_with("b64:") {
                cas::decode_relpath_to_os(path)
                    .to_string_lossy()
                    .replace('\\', "/")
            } else {
                path.replace('\\', "/")
            };
            // Path traversal guard
            let p = std::path::Path::new(&git_path);
            if p.is_absolute()
                || p.components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                tracing::warn!("Skipping suspicious path in git stash: {}", git_path);
                continue;
            }
            // Symlink mode handling (120000) if snapshot recorded it as non-executable and content looks like link
            let mode = if symlink_map.contains(path.as_str()) {
                "120000"
            } else if exec_map.contains(path.as_str()) {
                "100755"
            } else {
                "100644"
            };
            // Write NUL-terminated entries: "<mode> <hash>\t<path>\0"
            write!(sin, "{mode} {blob_hash}\t{git_path}\0")?;
        }
    }
    let status = upd.wait()?;
    if !status.success() {
        bail!("git update-index --index-info failed with status {status}");
    }
    //  - write-tree
    let tree_hash = run_git_output_with_index(project_path, &tmp_index, &["write-tree"])?
        .trim()
        .to_string();

    // Step 3: Get current HEAD commit
    let head_output = run_git_output(project_path, &["rev-parse", "HEAD"])?;
    let head_commit = head_output.trim().to_string();

    // Step 4: Create a proper two-parent stash structure (HEAD + index commit)
    let msg = format!("uhoh: snapshot {} ({})", id_str, snap.timestamp);
    // Create index commit first (with fallback identity for unconfigured git)
    let index_commit_out = Command::new("git")
        .current_dir(project_path)
        .env("GIT_AUTHOR_NAME", "uhoh")
        .env("GIT_AUTHOR_EMAIL", "uhoh@localhost")
        .env("GIT_COMMITTER_NAME", "uhoh")
        .env("GIT_COMMITTER_EMAIL", "uhoh@localhost")
        .args([
            "commit-tree",
            &tree_hash,
            "-p",
            &head_commit,
            "-m",
            &format!("index on {id_str}"),
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .context("Failed to create index commit for stash")?;
    if !index_commit_out.status.success() {
        bail!(
            "git commit-tree (index) failed: {}",
            String::from_utf8_lossy(&index_commit_out.stderr)
        );
    }
    let index_commit = String::from_utf8(index_commit_out.stdout)?
        .trim()
        .to_string();

    // Create the stash commit with two parents
    let stash_commit_out = Command::new("git")
        .current_dir(project_path)
        .env("GIT_AUTHOR_NAME", "uhoh")
        .env("GIT_AUTHOR_EMAIL", "uhoh@localhost")
        .env("GIT_COMMITTER_NAME", "uhoh")
        .env("GIT_COMMITTER_EMAIL", "uhoh@localhost")
        .args([
            "commit-tree",
            &tree_hash,
            "-p",
            &head_commit,
            "-p",
            &index_commit,
            "-m",
            &msg,
        ])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .context("Failed to create two-parent stash commit")?;
    if !stash_commit_out.status.success() {
        bail!(
            "git commit-tree (stash) failed: {}",
            String::from_utf8_lossy(&stash_commit_out.stderr)
        );
    }
    let stash_commit = String::from_utf8(stash_commit_out.stdout)?
        .trim()
        .to_string();

    // Step 5: Store as a stash entry
    run_git(project_path, &["stash", "store", "-m", &msg, &stash_commit])?;

    // Clean up temp index
    let _ = std::fs::remove_file(&tmp_index);

    println!("Snapshot {id_str} stashed as git stash entry. Use `git stash pop` to apply.");
    Ok(())
}

/// Resolve the actual git directory, handling worktrees where `.git` is a file
/// containing `gitdir: <path>` pointing to the real gitdir.
fn resolve_git_dir(project_path: &Path) -> Result<std::path::PathBuf> {
    let git_path = project_path.join(".git");
    if !git_path.exists() {
        bail!("Not a git repository.");
    }
    if git_path.is_file() {
        // Worktree: .git is a file with "gitdir: <actual-gitdir-path>"
        let content = std::fs::read_to_string(&git_path)
            .with_context(|| format!("Failed to read .git file: {}", git_path.display()))?;
        if let Some(gitdir) = content.trim().strip_prefix("gitdir: ") {
            let resolved = if Path::new(gitdir).is_absolute() {
                std::path::PathBuf::from(gitdir)
            } else {
                project_path.join(gitdir)
            };
            return dunce::canonicalize(&resolved)
                .with_context(|| format!("Failed to resolve gitdir: {gitdir}"));
        }
        bail!("Invalid .git file format in {}", git_path.display());
    }
    Ok(git_path)
}

/// Install a pre-commit hook (checks for existing hooks first).
pub fn install_hook(project_path: &Path) -> Result<()> {
    let git_dir = resolve_git_dir(project_path)?;

    let hooks_dir = git_dir.join("hooks");
    std::fs::create_dir_all(&hooks_dir)?;

    let hook_path = hooks_dir.join("pre-commit");

    // Always resolve to an absolute path for hooks. GUI Git clients often have
    // stripped PATH, so bare "uhoh" would fail even if it was in PATH at install time.
    let exe_str = if let Ok(path) = which("uhoh") {
        path.to_string_lossy().to_string()
    } else if let Ok(exe) = std::env::current_exe() {
        exe.to_string_lossy().to_string()
    } else {
        let bin_name = format!("uhoh{}", std::env::consts::EXE_SUFFIX);
        // Try ~/.uhoh/bin, /usr/local/bin, ~/.local/bin in order
        let candidates = [
            crate::platform::uhoh_dir().join("bin").join(&bin_name),
            std::path::PathBuf::from("/usr/local/bin").join(&bin_name),
            dirs::home_dir()
                .unwrap_or_default()
                .join(".local/bin")
                .join(&bin_name),
        ];
        candidates
            .iter()
            .find(|p| p.exists())
            .map(|p| p.to_string_lossy().to_string())
            .unwrap_or_else(|| {
                crate::platform::uhoh_dir()
                    .join("bin")
                    .join(&bin_name)
                    .to_string_lossy()
                    .to_string()
            })
    };

    let uhoh_hook_content = format!(
        r#"
# BEGIN uhoh pre-commit hook
"{exe_str}" commit --trigger pre-commit "Pre-commit snapshot" 2>/dev/null || true
# END uhoh pre-commit hook
"#
    );

    if hook_path.exists() {
        // Check if our hook is already in there
        let existing = std::fs::read_to_string(&hook_path)?;
        if existing.contains("uhoh pre-commit hook") {
            println!("Hook already installed.");
            return Ok(());
        }
        // Append to existing hook
        let mut content = existing;
        content.push_str(&uhoh_hook_content);
        std::fs::write(&hook_path, content)?;
        println!("Appended uhoh hook to existing pre-commit hook.");
    } else {
        let content = format!("#!/bin/sh\n{uhoh_hook_content}");
        std::fs::write(&hook_path, content)?;
        println!("Pre-commit hook installed.");
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hook_path, std::fs::Permissions::from_mode(0o755))?;
    }

    Ok(())
}

pub fn remove_hook(project_path: &Path) -> Result<()> {
    let git_dir = resolve_git_dir(project_path)?;
    let hook_path = git_dir.join("hooks/pre-commit");
    if !hook_path.exists() {
        println!("No pre-commit hook found.");
        return Ok(());
    }

    let content = std::fs::read_to_string(&hook_path)?;
    if content.contains("# BEGIN uhoh pre-commit hook") {
        let mut new_content = String::new();
        let mut in_block = false;
        for line in content.lines() {
            if line.contains("# BEGIN uhoh pre-commit hook") {
                in_block = true;
                continue;
            }
            if line.contains("# END uhoh pre-commit hook") {
                in_block = false;
                continue;
            }
            if !in_block {
                new_content.push_str(line);
                new_content.push('\n');
            }
        }
        let trimmed = new_content.trim();
        if trimmed == "#!/bin/sh" || trimmed.is_empty() {
            std::fs::remove_file(&hook_path)?;
            println!("Pre-commit hook removed.");
        } else {
            std::fs::write(&hook_path, new_content)?;
            println!("Removed uhoh hook block.");
        }
    } else {
        println!("No uhoh hook found in pre-commit.");
    }
    Ok(())
}

fn run_git(cwd: &Path, args: &[&str]) -> Result<()> {
    let status = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .status()
        .with_context(|| format!("Failed to run git {}", args.join(" ")))?;
    if !status.success() {
        bail!("git {} failed with status {}", args.join(" "), status);
    }
    Ok(())
}

fn run_git_output(cwd: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .current_dir(cwd)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .with_context(|| format!("Failed to run git {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?)
}

fn run_git_with_index(cwd: &Path, index_file: &Path, args: &[&str]) -> Result<()> {
    let status = Command::new("git")
        .current_dir(cwd)
        .env("GIT_INDEX_FILE", index_file)
        .args(args)
        .status()
        .with_context(|| format!("Failed to run git {}", args.join(" ")))?;
    if !status.success() {
        bail!("git {} failed with status {}", args.join(" "), status);
    }
    Ok(())
}

fn run_git_output_with_index(cwd: &Path, index_file: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .current_dir(cwd)
        .env("GIT_INDEX_FILE", index_file)
        .args(args)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .with_context(|| format!("Failed to run git {}", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?)
}
