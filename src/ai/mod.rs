pub mod models;
pub mod sidecar;
pub mod summary;

use anyhow::Result;
use crate::config::AiConfig;

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

/// Check if on AC power. Defaults to true (assume AC) on any error.
fn on_ac_power() -> bool {
    // Wrap in catch_all since battery crate can be unreliable on some platforms
    std::panic::catch_unwind(|| {
        let manager = match battery::Manager::new() {
            Ok(m) => m,
            Err(_) => return true, // No battery manager = probably a desktop
        };
        let batteries = match manager.batteries() {
            Ok(b) => b,
            Err(_) => return true,
        };
        for bat in batteries {
            match bat {
                Ok(b) => {
                    use battery::State;
                    match b.state() {
                        State::Discharging => return false,
                        State::Full | State::Charging | State::Unknown | _ => {}
                    }
                }
                Err(_) => continue,
            }
        }
        true
    })
    .unwrap_or(true)
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
