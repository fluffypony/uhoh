use std::path::PathBuf;

#[derive(Debug)]
pub(super) enum WatchEvent {
    FileChanged(PathBuf),
    FileDeleted(PathBuf),
    Overflow,
    WatcherDied,
}
