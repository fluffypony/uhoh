use anyhow::{Context, Result};
use notify::{Config, Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::db::Database;
use crate::restore::RestoreCoordinator;

use super::snapshots::{self, ProjectDaemonState};
use super::watch_event::WatchEvent;

pub(super) struct WatcherRuntime {
    event_tx: mpsc::Sender<WatchEvent>,
    watcher_handle: RecommendedWatcher,
    recover_attempts: u32,
    next_recover_at: Option<Instant>,
    was_restoring: bool,
}

pub(super) struct BinaryChangeWatcher {
    pub(super) event_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    _event_tx: tokio::sync::mpsc::UnboundedSender<()>,
    _watcher: Option<RecommendedWatcher>,
}

impl BinaryChangeWatcher {
    pub(super) fn start(uhoh_dir: &Path) -> Self {
        let (event_tx, event_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let exe_path = std::env::current_exe().ok();
        let watcher = if let Some(ref exe) = exe_path {
            let (notify_tx, notify_rx) = std::sync::mpsc::channel();
            let mut watcher = notify::RecommendedWatcher::new(
                move |res| {
                    let _ = notify_tx.send(res);
                },
                notify::Config::default(),
            )
            .ok();
            if let Some(ref mut handle) = watcher {
                if let Some(parent) = exe.parent() {
                    let _ = handle.watch(parent, RecursiveMode::NonRecursive);
                }
                let _ = handle.watch(uhoh_dir, RecursiveMode::NonRecursive);
            }

            let exe_path = exe.clone();
            let event_tx = event_tx.clone();
            std::thread::Builder::new()
                .name("bin-watcher-bridge".into())
                .spawn(move || {
                    for event in notify_rx.into_iter().flatten() {
                        let involves_binary = event.paths.iter().any(|path| path == &exe_path)
                            || event.paths.iter().any(|path| {
                                path.file_name().is_some_and(|name| name == ".update-ready")
                            });
                        if involves_binary {
                            let _ = event_tx.send(());
                        }
                    }
                })
                .ok();
            watcher
        } else {
            None
        };

        Self {
            event_rx,
            _event_tx: event_tx,
            _watcher: watcher,
        }
    }
}

impl WatcherRuntime {
    pub(super) fn start(paths: &[PathBuf], event_tx: mpsc::Sender<WatchEvent>) -> Result<Self> {
        Ok(Self {
            watcher_handle: start_watching(paths, event_tx.clone())?,
            event_tx,
            recover_attempts: 0,
            next_recover_at: None,
            was_restoring: false,
        })
    }

    pub(super) fn watcher_handle_mut(&mut self) -> &mut RecommendedWatcher {
        &mut self.watcher_handle
    }

    pub(super) fn was_restoring_flag_mut(&mut self) -> &mut bool {
        &mut self.was_restoring
    }

    pub(super) fn handle_event(
        &mut self,
        event: WatchEvent,
        project_states: &mut HashMap<String, ProjectDaemonState>,
        database: &Database,
        uhoh_dir: &Path,
        restore_coordinator: &RestoreCoordinator,
    ) {
        let currently_restoring = crate::restore::is_restore_active(restore_coordinator, uhoh_dir);
        if self.was_restoring && !currently_restoring {
            let now = Instant::now();
            snapshots::mark_restore_completed(project_states, database, now);
            tracing::debug!("Restore completed, grace period started for all projects");
        }
        self.was_restoring = currently_restoring;

        if currently_restoring && self.should_skip_for_restore(project_states, &event, uhoh_dir) {
            tracing::debug!("Skipping watcher event during restore operation");
            return;
        }

        match event {
            WatchEvent::WatcherDied => self.recover(project_states),
            other => snapshots::handle_watch_event(project_states, &other, uhoh_dir),
        }
    }

    fn should_skip_for_restore(
        &self,
        project_states: &HashMap<String, ProjectDaemonState>,
        event: &WatchEvent,
        uhoh_dir: &Path,
    ) -> bool {
        let is_control = matches!(event, WatchEvent::WatcherDied | WatchEvent::Overflow);
        if is_control {
            return false;
        }

        let restoring_hash = crate::restore::read_restoring_project_hash(uhoh_dir);
        snapshots::should_skip_event_during_restore(
            project_states,
            event,
            restoring_hash.as_deref(),
        )
    }

    fn recover(&mut self, project_states: &HashMap<String, ProjectDaemonState>) {
        let now = Instant::now();
        if let Some(ready_at) = self.next_recover_at {
            if now < ready_at {
                self.requeue_recovery_signal(ready_at - now);
                return;
            }
        }

        let delay_secs = (1u64 << self.recover_attempts.min(6)).min(60);
        tracing::error!(
            "File watcher died — attempting recovery (attempt #{})...",
            self.recover_attempts + 1
        );
        let paths: Vec<PathBuf> = project_states.keys().map(PathBuf::from).collect();
        match start_watching(&paths, self.event_tx.clone()) {
            Ok(new_watcher) => {
                self.watcher_handle = new_watcher;
                self.recover_attempts = 0;
                self.next_recover_at = None;
                tracing::info!("File watcher recovered successfully");
            }
            Err(err) => {
                self.recover_attempts = self.recover_attempts.saturating_add(1);
                self.next_recover_at = Some(now + Duration::from_secs(delay_secs));
                tracing::error!(
                    "Failed to recover file watcher: {}. Backing off {}s.",
                    err,
                    delay_secs
                );
            }
        }
    }

    fn requeue_recovery_signal(&self, remaining: Duration) {
        let event_tx = self.event_tx.clone();
        tokio::spawn(async move {
            tokio::time::sleep(remaining).await;
            if send_watch_event(&event_tx, WatchEvent::WatcherDied).is_err() {
                tracing::warn!("Watcher recovery signal dropped due to full event queue");
            }
        });
    }
}

fn start_watching(paths: &[PathBuf], tx: mpsc::Sender<WatchEvent>) -> Result<RecommendedWatcher> {
    // Helper: send event, return false if receiver dropped (daemon exiting)
    fn send(tx: &mpsc::Sender<WatchEvent>, event: WatchEvent) -> bool {
        send_watch_event(tx, event).is_ok()
    }

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
                Ok(()) => tracing::debug!("Watching: {}", path.display()),
                Err(e) => tracing::warn!("Cannot watch {}: {}", path.display(), e),
            }
        }
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
                                if !send(&sender, WatchEvent::FileDeleted(event.paths[0].clone())) {
                                    return;
                                }
                                if !send(&sender, WatchEvent::FileChanged(event.paths[1].clone())) {
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
        let _ = send_watch_event(&sender, WatchEvent::WatcherDied);
    });

    Ok(watcher)
}

fn send_watch_event(
    tx: &mpsc::Sender<WatchEvent>,
    event: WatchEvent,
) -> std::result::Result<(), mpsc::error::SendError<WatchEvent>> {
    tx.blocking_send(event)
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watcher_channel_is_bounded_sender_type() {
        fn assert_sender_type(_: tokio::sync::mpsc::Sender<WatchEvent>) {}
        let _ = assert_sender_type;
    }

    #[test]
    fn watcher_runtime_fields_initialize_correctly() {
        // WatcherRuntime initializes with zero recover attempts and no pending recovery
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let paths = vec![temp.path().to_path_buf()];
        let (tx, _rx) = mpsc::channel(16);

        let result = rt.block_on(async {
            WatcherRuntime::start(&paths, tx)
        });
        let runtime = result.unwrap();
        assert_eq!(runtime.recover_attempts, 0);
        assert!(runtime.next_recover_at.is_none());
        assert!(!runtime.was_restoring);
    }

    #[test]
    fn watcher_runtime_start_watches_existing_paths() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let paths = vec![temp.path().to_path_buf()];
        let (tx, _rx) = mpsc::channel(16);

        let result = rt.block_on(async {
            WatcherRuntime::start(&paths, tx)
        });
        assert!(result.is_ok());
    }

    #[test]
    fn watcher_runtime_start_handles_nonexistent_paths() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let paths = vec![PathBuf::from("/nonexistent/path/that/does/not/exist")];
        let (tx, _rx) = mpsc::channel(16);

        // Should succeed (nonexistent paths are skipped with a warning, not failed)
        let result = rt.block_on(async {
            WatcherRuntime::start(&paths, tx)
        });
        assert!(result.is_ok());
    }

    #[test]
    fn binary_change_watcher_creates_receiver() {
        let temp = tempfile::tempdir().unwrap();
        let watcher = BinaryChangeWatcher::start(temp.path());
        // Receiver should be available and not immediately closed
        assert!(!watcher.event_rx.is_closed());
    }
}
