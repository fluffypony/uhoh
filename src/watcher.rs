use anyhow::{Context, Result};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::path::PathBuf;

use crate::daemon::WatchEvent;

/// Start watching the given paths. Returns the watcher handle (must be kept alive).
pub fn start_watching(
    paths: &[PathBuf],
    tx: tokio::sync::mpsc::Sender<WatchEvent>,
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
        let sender = tx_clone; // move into thread scope
        let res = std::panic::catch_unwind(|| {
            for result in file_rx {
                match result {
                    Ok(event) => match event.kind {
                        EventKind::Remove(_) => {
                            for p in event.paths {
                                if crate::daemon::send_watch_event(
                                    &sender,
                                    WatchEvent::FileDeleted(p),
                                )
                                .is_err()
                                {
                                    let _ = crate::daemon::send_watch_event(
                                        &sender,
                                        WatchEvent::Overflow,
                                    );
                                }
                            }
                        }
                        EventKind::Modify(notify::event::ModifyKind::Name(
                            notify::event::RenameMode::Both,
                        )) => {
                            // Rename: old path is a deletion, new path is a change
                            if event.paths.len() >= 2 {
                                if crate::daemon::send_watch_event(
                                    &sender,
                                    WatchEvent::FileDeleted(event.paths[0].clone()),
                                )
                                .is_err()
                                {
                                    let _ = crate::daemon::send_watch_event(
                                        &sender,
                                        WatchEvent::Overflow,
                                    );
                                }
                                if crate::daemon::send_watch_event(
                                    &sender,
                                    WatchEvent::FileChanged(event.paths[1].clone()),
                                )
                                .is_err()
                                {
                                    let _ = crate::daemon::send_watch_event(
                                        &sender,
                                        WatchEvent::Overflow,
                                    );
                                }
                            } else {
                                for p in event.paths {
                                    if crate::daemon::send_watch_event(
                                        &sender,
                                        WatchEvent::FileChanged(p),
                                    )
                                    .is_err()
                                    {
                                        let _ = crate::daemon::send_watch_event(
                                            &sender,
                                            WatchEvent::Overflow,
                                        );
                                    }
                                }
                            }
                        }
                        EventKind::Create(_) | EventKind::Modify(_) => {
                            for p in event.paths {
                                if crate::daemon::send_watch_event(
                                    &sender,
                                    WatchEvent::FileChanged(p),
                                )
                                .is_err()
                                {
                                    let _ = crate::daemon::send_watch_event(
                                        &sender,
                                        WatchEvent::Overflow,
                                    );
                                }
                            }
                        }
                        EventKind::Other => {
                            tracing::trace!("Ignoring EventKind::Other");
                        }
                        _ => {}
                    },
                    Err(e) => {
                        tracing::warn!("Watch error: {}", e);
                        // On error, treat as overflow (global rescan)
                        let _ = crate::daemon::send_watch_event(&sender, WatchEvent::Overflow);
                    }
                }
            }
        });
        if res.is_err() {
            tracing::error!("Watcher bridge thread panicked; signaling WatcherDied");
        }
        // Notify daemon that watcher died
        let _ = crate::daemon::send_watch_event(&sender, WatchEvent::WatcherDied);
    });

    Ok(watcher)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_channel_is_bounded_sender_type() {
        fn assert_sender_type(_: tokio::sync::mpsc::Sender<WatchEvent>) {}
        // Compile-time type check only.
        let _ = assert_sender_type;
    }
}
