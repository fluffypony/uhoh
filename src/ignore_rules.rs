use ignore::WalkBuilder;
use std::path::Path;

/// Build a directory walker that respects the full .gitignore chain
/// (nested .gitignore files, .git/info/exclude, global gitignore)
/// and additionally .uhohignore files.
///
/// .uhohignore uses standard gitignore syntax. Positive patterns add
/// additional ignores; negation patterns (`!pattern`) re-include files
/// that were gitignored.
pub fn build_walker(project_path: &Path) -> ignore::Walk {
    let mut builder = WalkBuilder::new(project_path);
    let project_path_buf = project_path.to_path_buf();

    builder
        .hidden(false) // Don't skip hidden files by default (gitignore handles that)
        .git_ignore(true) // Respect .gitignore chain
        .git_global(true) // Respect global gitignore
        .git_exclude(true) // Respect .git/info/exclude
        .follow_links(false) // Don't follow symlinks (security + loop prevention)
        .max_depth(None);

    // Add .uhohignore if it exists
    let uhohignore_paths = [
        project_path.join(".git/.uhohignore"),
        project_path.join(".uhohignore"),
    ];
    for path in &uhohignore_paths {
        if path.exists() {
            builder.add_ignore(path);
        }
    }

    // Always ignore common large/ephemeral directories that might slip through
    let base = project_path_buf.clone();
    builder.filter_entry(move |entry| {
        let name = entry.file_name().to_string_lossy();
        // Skip .git internals (we handle .git/.uhoh separately)
        if name == ".git" {
            return false;
        }
        // Always ignore the uhoh data directory to avoid inception loops
        if name == ".uhoh" && entry.path().starts_with(&base) {
            return false;
        }
        true
    });

    builder.build()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_project(dir: &Path, files: &[&str]) {
        for file in files {
            let p = dir.join(file);
            if let Some(parent) = p.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(&p, "content").unwrap();
        }
    }

    fn walk_files(dir: &Path) -> Vec<String> {
        let walker = build_walker(dir);
        let mut files: Vec<String> = walker
            .filter_map(|entry| entry.ok())
            .filter(|e| e.file_type().is_some_and(|ft| ft.is_file()))
            .map(|e| {
                e.path()
                    .strip_prefix(dir)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();
        files.sort();
        files
    }

    #[test]
    fn walks_regular_files() {
        let dir = tempfile::tempdir().unwrap();
        setup_project(dir.path(), &["a.txt", "src/b.rs", "src/c.rs"]);

        let files = walk_files(dir.path());
        assert!(files.contains(&"a.txt".to_string()));
        assert!(files.contains(&"src/b.rs".to_string()));
        assert!(files.contains(&"src/c.rs".to_string()));
    }

    #[test]
    fn excludes_dot_git_directory() {
        let dir = tempfile::tempdir().unwrap();
        setup_project(
            dir.path(),
            &["a.txt", ".git/config", ".git/HEAD", ".git/objects/abc"],
        );

        let files = walk_files(dir.path());
        assert!(files.contains(&"a.txt".to_string()));
        for f in &files {
            assert!(!f.starts_with(".git/"), "Should exclude .git contents: {f}");
        }
    }

    #[test]
    fn excludes_dot_uhoh_directory() {
        let dir = tempfile::tempdir().unwrap();
        setup_project(dir.path(), &["a.txt", ".uhoh/db.sqlite", ".uhoh/blobs/xyz"]);

        let files = walk_files(dir.path());
        assert!(files.contains(&"a.txt".to_string()));
        for f in &files {
            assert!(
                !f.starts_with(".uhoh/"),
                "Should exclude .uhoh contents: {f}"
            );
        }
    }

    #[test]
    fn includes_hidden_files() {
        let dir = tempfile::tempdir().unwrap();
        setup_project(dir.path(), &[".hidden_file", ".dotdir/inner.txt", "visible.txt"]);

        let files = walk_files(dir.path());
        assert!(
            files.contains(&".hidden_file".to_string()),
            "Should include hidden files"
        );
        assert!(
            files.contains(&".dotdir/inner.txt".to_string()),
            "Should include files in hidden dirs"
        );
    }

    #[test]
    fn respects_gitignore() {
        let dir = tempfile::tempdir().unwrap();
        // Initialize a git repo so .gitignore is respected
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join(".gitignore"), "*.log\nbuild/\n").unwrap();
        setup_project(
            dir.path(),
            &["a.txt", "debug.log", "build/output.bin", "src/main.rs"],
        );

        let files = walk_files(dir.path());
        assert!(files.contains(&"a.txt".to_string()));
        assert!(files.contains(&"src/main.rs".to_string()));
        assert!(
            !files.contains(&"debug.log".to_string()),
            "Should respect .gitignore for *.log"
        );
        assert!(
            !files.contains(&"build/output.bin".to_string()),
            "Should respect .gitignore for build/"
        );
    }

    #[test]
    fn respects_uhohignore_in_project_root() {
        let dir = tempfile::tempdir().unwrap();
        // Need .git dir for gitignore chain to activate
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join(".uhohignore"), "*.tmp\nsecrets/\n").unwrap();
        setup_project(
            dir.path(),
            &["a.txt", "scratch.tmp", "secrets/key.pem", "src/main.rs"],
        );

        let files = walk_files(dir.path());
        assert!(files.contains(&"a.txt".to_string()));
        assert!(files.contains(&"src/main.rs".to_string()));
        assert!(
            !files.contains(&"scratch.tmp".to_string()),
            "Should respect .uhohignore for *.tmp"
        );
        assert!(
            !files.contains(&"secrets/key.pem".to_string()),
            "Should respect .uhohignore for secrets/"
        );
    }

    #[test]
    fn respects_uhohignore_in_dot_git() {
        let dir = tempfile::tempdir().unwrap();
        fs::create_dir_all(dir.path().join(".git")).unwrap();
        fs::write(dir.path().join(".git/.uhohignore"), "*.bak\n").unwrap();
        setup_project(dir.path(), &["a.txt", "old.bak"]);

        let files = walk_files(dir.path());
        assert!(files.contains(&"a.txt".to_string()));
        assert!(
            !files.contains(&"old.bak".to_string()),
            "Should respect .git/.uhohignore"
        );
    }

    #[test]
    fn empty_project_yields_no_files() {
        let dir = tempfile::tempdir().unwrap();
        let files = walk_files(dir.path());
        assert!(files.is_empty());
    }

    #[test]
    fn deeply_nested_files_are_walked() {
        let dir = tempfile::tempdir().unwrap();
        setup_project(dir.path(), &["a/b/c/d/e/f/g.txt"]);

        let files = walk_files(dir.path());
        assert!(files.contains(&"a/b/c/d/e/f/g.txt".to_string()));
    }
}
