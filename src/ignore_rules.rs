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
    builder.filter_entry(|entry| {
        let name = entry.file_name().to_string_lossy();
        // Skip .git internals (we handle .git/.uhoh separately)
        if name == ".git" {
            return false;
        }
        true
    });

    builder.build()
}
