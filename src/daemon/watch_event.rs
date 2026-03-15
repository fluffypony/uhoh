use std::path::PathBuf;

#[derive(Debug)]
pub(super) enum WatchEvent {
    FileChanged(PathBuf),
    FileDeleted(PathBuf),
    Overflow,
    WatcherDied,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_event_debug_format() {
        let changed = WatchEvent::FileChanged(PathBuf::from("/tmp/file.rs"));
        let deleted = WatchEvent::FileDeleted(PathBuf::from("/tmp/old.rs"));
        let overflow = WatchEvent::Overflow;
        let died = WatchEvent::WatcherDied;

        assert!(format!("{changed:?}").contains("FileChanged"));
        assert!(format!("{deleted:?}").contains("FileDeleted"));
        assert!(format!("{overflow:?}").contains("Overflow"));
        assert!(format!("{died:?}").contains("WatcherDied"));
    }

    #[test]
    fn watch_event_file_changed_contains_path() {
        let event = WatchEvent::FileChanged(PathBuf::from("src/main.rs"));
        if let WatchEvent::FileChanged(path) = event {
            assert_eq!(path, PathBuf::from("src/main.rs"));
        } else {
            panic!("Expected FileChanged");
        }
    }
}
