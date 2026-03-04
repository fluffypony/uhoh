use anyhow::{bail, Context, Result};
use std::path::Path;
use std::process::Command;

use crate::cas;
use crate::db::{Database, ProjectEntry};

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

    // Verify this is a git repo
    if !project_path.join(".git").exists() {
        bail!("Not a git repository. Git stash requires a git repo.");
    }

    let snap = database
        .find_snapshot_by_base58(&project.hash, id_str)?
        .context("Snapshot not found")?;

    let files = database.get_snapshot_files(snap.rowid)?;
    let blob_root = uhoh_dir.join("blobs");

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
            bail!("git hash-object failed: {}", String::from_utf8_lossy(&output.stderr));
        }

        let git_hash = String::from_utf8(output.stdout)?.trim().to_string();
        tree_entries.push((file.path.clone(), git_hash));
    }

    // Step 2: Build tree object using temporary index to avoid corrupting user's index
    let git_dir = project_path.join(".git");
    let tmp_index = git_dir.join("index.uhoh-tmp");
    // Use per-command environment to avoid global side effects
    run_git_with_index(project_path, &tmp_index, &["read-tree", "--empty"]) ?;
    // Build a lookup for executable flag from DB
    let mut exec_map = std::collections::HashSet::new();
    let snap_files = database.get_snapshot_files(snap.rowid)?;
    for f in &snap_files { if f.executable { exec_map.insert(f.path.as_str()); } }
    for (path, blob_hash) in &tree_entries {
        let mode = if exec_map.contains(path.as_str()) { "100755" } else { "100644" };
        let args = ["update-index", "--add", "--cacheinfo", mode, blob_hash.as_str(), path.as_str()];
        run_git_with_index(project_path, &tmp_index, &args)?;
    }
    //  - write-tree
    let tree_hash = run_git_output_with_index(project_path, &tmp_index, &["write-tree"])?.trim().to_string();

    // Step 3: Get current HEAD commit
    let head_output = run_git_output(project_path, &["rev-parse", "HEAD"])?;
    let head_commit = head_output.trim().to_string();

    // Step 4: Create a commit object for the stash
    let msg = format!("uhoh: snapshot {} ({})", id_str, snap.timestamp);
    let commit_output = Command::new("git")
        .current_dir(project_path)
        .args(["commit-tree", &tree_hash, "-p", &head_commit, "-m", &msg])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .context("Failed to create stash commit")?;

    if !commit_output.status.success() {
        bail!(
            "git commit-tree failed: {}",
            String::from_utf8_lossy(&commit_output.stderr)
        );
    }
    let stash_commit = String::from_utf8(commit_output.stdout)?
        .trim()
        .to_string();

    // Step 5: Store as a stash entry
    run_git(project_path, &["stash", "store", "-m", &msg, &stash_commit])?;

    // Clean up temp index
    let _ = std::fs::remove_file(&tmp_index);

    println!(
        "Snapshot {} stashed as git stash entry. Use `git stash pop` to apply.",
        id_str
    );
    Ok(())
}

/// Install a pre-commit hook (checks for existing hooks first).
pub fn install_hook(project_path: &Path) -> Result<()> {
    let git_dir = project_path.join(".git");
    if !git_dir.exists() {
        bail!("Not a git repository.");
    }

    let hooks_dir = git_dir.join("hooks");
    std::fs::create_dir_all(&hooks_dir)?;

    let hook_path = hooks_dir.join("pre-commit");

    // Use absolute path to uhoh binary (prevents PATH hijacking)
    let exe_path = std::env::current_exe()
        .context("Cannot determine uhoh binary path")?;
    let exe_str = exe_path.to_string_lossy();

    let uhoh_hook_content = format!(
        r#"
# BEGIN uhoh pre-commit hook
"{}" commit --trigger pre-commit "Pre-commit snapshot" 2>/dev/null || true
# END uhoh pre-commit hook
"#,
        exe_str
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
        let content = format!("#!/bin/sh\n{}", uhoh_hook_content);
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
    let hook_path = project_path.join(".git/hooks/pre-commit");
    if !hook_path.exists() {
        println!("No pre-commit hook found.");
        return Ok(());
    }

    let content = std::fs::read_to_string(&hook_path)?;
    if content.contains("# BEGIN uhoh pre-commit hook") {
        let mut new_content = String::new();
        let mut in_block = false;
        for line in content.lines() {
            if line.contains("# BEGIN uhoh pre-commit hook") { in_block = true; continue; }
            if line.contains("# END uhoh pre-commit hook") { in_block = false; continue; }
            if !in_block { new_content.push_str(line); new_content.push('\n'); }
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
