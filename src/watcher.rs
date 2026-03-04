use anyhow::{Context, Result};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;
use tokio::sync::mpsc;

use crate::daemon::WatchEvent;

/// Start watching the given paths. Returns the watcher handle (must be kept alive).
pub fn start_watching(
    paths: &[PathBuf],
    tx: mpsc::Sender<WatchEvent>,
) -> Result<RecommendedWatcher> {
    let (file_tx, file_rx) = std::sync::mpsc::channel();

    let mut watcher = RecommendedWatcher::new(
        move |res: std::result::Result<Event, notify::Error>| {
            file_tx.send(res).ok();
        },
        Config::default(),
    )
    .context("Failed to create file watcher")?;

    for path in paths {
        if path.exists() {
            match watcher.watch(path, RecursiveMode::Recursive) {
                Ok(_) => tracing::debug!("Watching: {}", path.display()),
                Err(e) => tracing::warn!("Cannot watch {}: {}", path.display(), e),
            }
        }
    }

    // Spawn thread to forward events to async channel
    let tx_clone = tx;
    std::thread::spawn(move || {
        for result in file_rx {
            match result {
                Ok(event) => {
                    let watch_event = match event.kind {
                        EventKind::Remove(_) => {
                            event.paths.first().map(|p| WatchEvent::FileDeleted(p.clone()))
                        }
                        EventKind::Create(_)
                        | EventKind::Modify(_) => {
                            event.paths.first().map(|p| WatchEvent::FileChanged(p.clone()))
                        }
                        EventKind::Other => {
                            // Treat as overflow; rescan all
                            Some(WatchEvent::Overflow)
                        }
                        _ => None,
                    };
                    if let Some(we) = watch_event {
                        if tx_clone.blocking_send(we).is_err() {
                            break; // Receiver dropped
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("Watch error: {}", e);
                    // On error, treat as overflow (global rescan)
                    let _ = tx_clone.blocking_send(WatchEvent::Overflow);
                }
            }
        }
        // Notify daemon that watcher died
        let _ = tx_clone.blocking_send(WatchEvent::WatcherDied);
    });

    Ok(watcher)
}
