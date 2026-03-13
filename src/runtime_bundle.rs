use std::path::{Path, PathBuf};
use std::sync::Arc;

use tokio::sync::broadcast;

use crate::ai::sidecar::SidecarManager;
use crate::config::Config;
use crate::db::Database;
use crate::events::ServerEvent;
use crate::restore_runtime::{RestoreCoordinator, RestoreRuntime};
use crate::snapshot::{SnapshotRuntime, SnapshotSettings};

#[derive(Clone)]
pub struct RuntimeBundle(Arc<RuntimeBundleInner>);

struct RuntimeBundleInner {
    database: Arc<Database>,
    uhoh_dir: PathBuf,
    config: Config,
    event_tx: Option<broadcast::Sender<ServerEvent>>,
    restore_runtime: RestoreRuntime,
    sidecar_manager: SidecarManager,
}

pub struct RuntimeBundleConfig {
    pub database: Arc<Database>,
    pub uhoh_dir: PathBuf,
    pub config: Config,
    pub event_tx: Option<broadcast::Sender<ServerEvent>>,
    pub restore_coordinator: Option<RestoreCoordinator>,
    pub sidecar_manager: Option<SidecarManager>,
}

impl RuntimeBundle {
    pub fn new(config: RuntimeBundleConfig) -> Self {
        let RuntimeBundleConfig {
            database,
            uhoh_dir,
            config,
            event_tx,
            restore_coordinator,
            sidecar_manager,
        } = config;

        let mut restore_runtime = RestoreRuntime::new(database.clone(), uhoh_dir.clone());
        if let Some(tx) = event_tx.clone() {
            restore_runtime = restore_runtime.with_event_tx(tx);
        }
        if let Some(coordinator) = restore_coordinator {
            restore_runtime = restore_runtime.with_coordinator(coordinator);
        }

        Self(Arc::new(RuntimeBundleInner {
            database,
            uhoh_dir,
            config,
            event_tx,
            restore_runtime,
            sidecar_manager: sidecar_manager.unwrap_or_default(),
        }))
    }

    pub fn database(&self) -> Arc<Database> {
        self.0.database.clone()
    }

    pub fn uhoh_dir(&self) -> &Path {
        &self.0.uhoh_dir
    }

    pub fn uhoh_dir_buf(&self) -> PathBuf {
        self.0.uhoh_dir.clone()
    }

    pub fn config(&self) -> &Config {
        &self.0.config
    }

    pub fn config_owned(&self) -> Config {
        self.0.config.clone()
    }

    pub fn event_tx(&self) -> Option<broadcast::Sender<ServerEvent>> {
        self.0.event_tx.clone()
    }

    pub fn restore_runtime(&self) -> RestoreRuntime {
        self.0.restore_runtime.clone()
    }

    pub fn sidecar_manager(&self) -> SidecarManager {
        self.0.sidecar_manager.clone()
    }

    pub fn snapshot_runtime(&self) -> SnapshotRuntime {
        SnapshotRuntime::new(
            SnapshotSettings::from_config(&self.0.config),
            self.0.sidecar_manager.clone(),
        )
    }
}
