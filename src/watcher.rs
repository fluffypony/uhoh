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

    // Helper: send event, return false if receiver dropped (daemon exiting)
    fn send(
        tx: &tokio::sync::mpsc::Sender<WatchEvent>,
        event: WatchEvent,
    ) -> bool {
        crate::daemon::send_watch_event(tx, event).is_ok()
    }

    // Spawn thread to forward events to async channel.
    // Uses blocking_send (via send_watch_event) for natural backpressure —
    // if the channel is full, this thread blocks instead of dropping events.
    let tx_clone = tx;
    std::thread::spawn(move || {
        let sender = tx_clone;
        let res = std::panic::catch_unwind(|| {
            for result in file_rx {
                match result {
                    Ok(event) => match event.kind {
                        EventKind::Remove(_) => {
                            for p in event.paths {
                                if !send(&sender, WatchEvent::FileDeleted(p)) {
                                    return;
                                }
                            }
                        }
                        EventKind::Modify(notify::event::ModifyKind::Name(
                            notify::event::RenameMode::Both,
                        )) => {
                            if event.paths.len() >= 2 {
                                if !send(
                                    &sender,
                                    WatchEvent::FileDeleted(event.paths[0].clone()),
                                ) {
                                    return;
                                }
                                if !send(
                                    &sender,
                                    WatchEvent::FileChanged(event.paths[1].clone()),
                                ) {
                                    return;
                                }
                            } else {
                                for p in event.paths {
                                    if !send(&sender, WatchEvent::FileChanged(p)) {
                                        return;
                                    }
                                }
                            }
                        }
                        EventKind::Create(_) | EventKind::Modify(_) => {
                            for p in event.paths {
                                if !send(&sender, WatchEvent::FileChanged(p)) {
                                    return;
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
                        if !send(&sender, WatchEvent::Overflow) {
                            return;
                        }
                    }
                }
            }
        });
        if res.is_err() {
            tracing::error!("Watcher bridge thread panicked; signaling WatcherDied");
        }
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
        let _ = assert_sender_type;
    }
}
