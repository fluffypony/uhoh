use std::collections::HashSet;
use std::fmt;

#[derive(Debug)]
pub struct RestoreBusyError {
    message: String,
}

impl RestoreBusyError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for RestoreBusyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for RestoreBusyError {}

pub struct RestoreLockGuard {
    locks: std::sync::Arc<std::sync::Mutex<HashSet<String>>>,
    key: String,
}

impl RestoreLockGuard {
    pub fn acquire(
        locks: std::sync::Arc<std::sync::Mutex<HashSet<String>>>,
        key: String,
    ) -> anyhow::Result<Self> {
        let mut guard = match locks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.contains(&key) {
            return Err(
                RestoreBusyError::new("Restore already in progress for this project").into(),
            );
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

pub struct RestoreFlagGuard {
    flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl RestoreFlagGuard {
    pub fn acquire(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) -> anyhow::Result<Self> {
        if flag.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(RestoreBusyError::new("Another restore is already in progress").into());
        }
        Ok(Self { flag })
    }
}

impl Drop for RestoreFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
