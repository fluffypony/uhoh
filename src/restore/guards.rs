use std::collections::HashSet;
use std::fmt;

#[derive(Debug)]
pub(crate) struct RestoreBusyError {
    message: String,
}

impl RestoreBusyError {
    pub(crate) fn new(message: impl Into<String>) -> Self {
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

#[derive(Debug)]
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

#[derive(Debug)]
pub(crate) struct RestoreFlagGuard {
    flag: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl RestoreFlagGuard {
    pub(crate) fn acquire(flag: std::sync::Arc<std::sync::atomic::AtomicBool>) -> anyhow::Result<Self> {
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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::sync::atomic::AtomicBool;

    // ── RestoreBusyError ──

    #[test]
    fn restore_busy_error_display() {
        let err = RestoreBusyError::new("test message");
        assert_eq!(format!("{err}"), "test message");
    }

    #[test]
    fn restore_busy_error_is_error() {
        let err = RestoreBusyError::new("msg");
        let _: &dyn std::error::Error = &err;
    }

    // ── RestoreLockGuard ──

    #[test]
    fn lock_guard_acquires_successfully() {
        let locks = Arc::new(Mutex::new(HashSet::new()));
        let guard = RestoreLockGuard::acquire(locks.clone(), "proj1".to_string());
        assert!(guard.is_ok());
    }

    #[test]
    fn lock_guard_rejects_duplicate() {
        let locks = Arc::new(Mutex::new(HashSet::new()));
        let _g1 = RestoreLockGuard::acquire(locks.clone(), "proj1".to_string()).unwrap();
        let g2 = RestoreLockGuard::acquire(locks.clone(), "proj1".to_string());
        assert!(g2.is_err());
        assert!(g2.unwrap_err().to_string().contains("already in progress"));
    }

    #[test]
    fn lock_guard_allows_different_keys() {
        let locks = Arc::new(Mutex::new(HashSet::new()));
        let _g1 = RestoreLockGuard::acquire(locks.clone(), "proj1".to_string()).unwrap();
        let g2 = RestoreLockGuard::acquire(locks.clone(), "proj2".to_string());
        assert!(g2.is_ok());
    }

    #[test]
    fn lock_guard_releases_on_drop() {
        let locks = Arc::new(Mutex::new(HashSet::new()));
        {
            let _guard = RestoreLockGuard::acquire(locks.clone(), "proj1".to_string()).unwrap();
        } // guard dropped here
        // Should be able to acquire again
        let guard = RestoreLockGuard::acquire(locks.clone(), "proj1".to_string());
        assert!(guard.is_ok());
    }

    // ── RestoreFlagGuard ──

    #[test]
    fn flag_guard_acquires_successfully() {
        let flag = Arc::new(AtomicBool::new(false));
        let guard = RestoreFlagGuard::acquire(flag.clone());
        assert!(guard.is_ok());
        assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn flag_guard_rejects_when_already_set() {
        let flag = Arc::new(AtomicBool::new(false));
        let _g1 = RestoreFlagGuard::acquire(flag.clone()).unwrap();
        let g2 = RestoreFlagGuard::acquire(flag.clone());
        assert!(g2.is_err());
        assert!(g2.unwrap_err().to_string().contains("already in progress"));
    }

    #[test]
    fn flag_guard_resets_on_drop() {
        let flag = Arc::new(AtomicBool::new(false));
        {
            let _guard = RestoreFlagGuard::acquire(flag.clone()).unwrap();
            assert!(flag.load(std::sync::atomic::Ordering::SeqCst));
        }
        assert!(!flag.load(std::sync::atomic::Ordering::SeqCst));
    }
}
