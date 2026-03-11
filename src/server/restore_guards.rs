use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Result};

pub(crate) struct RestoreLockGuard {
    locks: Arc<Mutex<HashSet<String>>>,
    key: String,
}

impl RestoreLockGuard {
    pub(crate) fn acquire(locks: Arc<Mutex<HashSet<String>>>, key: String) -> Result<Self> {
        let mut guard = match locks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.contains(&key) {
            bail!("Restore already in progress for this project");
        }
        guard.insert(key.clone());
        drop(guard);
        Ok(Self { locks, key })
    }
}

impl Drop for RestoreLockGuard {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.locks.lock() {
            guard.remove(&self.key);
        }
    }
}

pub(crate) struct RestoreFlagGuard {
    flag: Arc<AtomicBool>,
}

impl RestoreFlagGuard {
    pub(crate) fn acquire(flag: Arc<AtomicBool>) -> Result<Self> {
        if flag.swap(true, Ordering::SeqCst) {
            bail!("Another restore is already in progress");
        }
        Ok(Self { flag })
    }
}

impl Drop for RestoreFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}

pub(crate) struct StaticRestoreFlagGuard {
    flag: &'static AtomicBool,
}

impl StaticRestoreFlagGuard {
    pub(crate) fn acquire(flag: &'static AtomicBool) -> Result<Self> {
        if flag.swap(true, Ordering::SeqCst) {
            bail!("Another restore is already in progress");
        }
        Ok(Self { flag })
    }
}

impl Drop for StaticRestoreFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::SeqCst);
    }
}
