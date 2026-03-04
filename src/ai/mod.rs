pub mod models;
pub mod sidecar;
pub mod summary;

use crate::config::AiConfig;
use sysinfo as _; // ensure sysinfo available in this module

/// Check if AI features should run right now.
pub fn should_run_ai(config: &AiConfig) -> bool {
    if !config.enabled {
        return false;
    }

    if config.skip_on_battery && !on_ac_power() {
        tracing::debug!("Skipping AI: on battery power");
        return false;
    }

    if !memory_available(config.min_available_memory_gb) {
        tracing::debug!("Skipping AI: insufficient available memory");
        return false;
    }

    true
}

/// Like should_run_ai, but reuse a provided sysinfo::System snapshot.
pub fn should_run_ai_with(config: &AiConfig, sys: &sysinfo::System) -> bool {
    if !config.enabled { return false; }
    if config.skip_on_battery && !on_ac_power() { return false; }
    let available_gb = sys.available_memory() / (1024 * 1024 * 1024);
    available_gb >= config.min_available_memory_gb
}

/// Check if on AC power. Defaults to true (assume AC) on any error.
fn on_ac_power() -> bool {
    let manager = match battery::Manager::new() {
        Ok(m) => m,
        Err(_) => return true,
    };
    let batteries = match manager.batteries() {
        Ok(b) => b,
        Err(_) => return true,
    };
    for bat in batteries {
        if let Ok(b) = bat {
            if b.state() == battery::State::Discharging {
                return false;
            }
        }
    }
    true
}

/// Check if enough RAM is available (not total, not used — available).
fn memory_available(min_gb: u64) -> bool {
    use sysinfo::System;
    let mut sys = System::new();
    sys.refresh_memory();
    let available_bytes = sys.available_memory();
    let available_gb = available_bytes / (1024 * 1024 * 1024);
    available_gb >= min_gb
}
