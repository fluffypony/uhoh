use std::collections::HashSet;

pub(crate) struct RestoreLockGuard {
    locks: std::sync::Arc<std::sync::Mutex<HashSet<String>>>,
    key: String,
}

impl RestoreLockGuard {
    pub(crate) fn acquire(
        locks: std::sync::Arc<std::sync::Mutex<HashSet<String>>>,
        key: String,
    ) -> anyhow::Result<Self> {
        let mut guard = match locks.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if guard.contains(&key) {
            anyhow::bail!("Restore already in progress for this project");
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
    flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl RestoreFlagGuard {
    pub(crate) fn acquire(
        flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
    ) -> anyhow::Result<Self> {
        if flag.swap(true, std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("Another restore is already in progress");
        }
        Ok(Self { flag })
    }
}

impl Drop for RestoreFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

pub(crate) struct StaticRestoreFlagGuard {
    flag: &'static std::sync::atomic::AtomicBool,
}

impl StaticRestoreFlagGuard {
    pub(crate) fn acquire(flag: &'static std::sync::atomic::AtomicBool) -> anyhow::Result<Self> {
        if flag.swap(true, std::sync::atomic::Ordering::SeqCst) {
            anyhow::bail!("Another restore is already in progress");
        }
        Ok(Self { flag })
    }
}

impl Drop for StaticRestoreFlagGuard {
    fn drop(&mut self) {
        self.flag.store(false, std::sync::atomic::Ordering::SeqCst);
    }
}
