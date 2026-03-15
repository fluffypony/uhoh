pub mod mlx_update;
pub mod models;
pub mod queue;
pub mod sidecar;
pub mod sidecar_update;
pub mod summary;

// Facade re-exports: external modules should use these instead of reaching
// into sub-modules directly.
pub use mlx_update::MlxAutoUpdateState;
pub use mlx_update::maybe_run_mlx_auto_update;
pub use queue::process_summary_queue;
pub use sidecar::SidecarManager;
pub use sidecar_update::read_manifest;
pub use sidecar_update::run_update_check;
pub use summary::SummaryBlobRef;
pub use summary::build_diff_entries;
pub use summary::generate_summary_blocking;
pub use summary::prepare_summary_inputs;

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
    if !config.enabled {
        return false;
    }
    if config.skip_on_battery && !on_ac_power() {
        return false;
    }
    let available_mb = sys.available_memory() / (1024 * 1024);
    available_mb >= config.min_available_memory_gb * 1024
}

/// Check if on AC power.
/// - No battery hardware (e.g., desktop, VM) → true (AC)
/// - Battery present and discharging → false
/// - API error → false (conservative: skip AI rather than drain battery)
fn on_ac_power() -> bool {
    let Ok(manager) = battery::Manager::new() else {
        // Manager creation failed — likely no battery subsystem (desktop/VM). Assume AC.
        return true;
    };
    let Ok(mut batteries) = manager.batteries() else {
        // API error enumerating batteries. On Linux, this can happen on battery-less
        // desktops/VMs where Manager::new() succeeds but batteries() fails.
        // Default to AC (true) since Manager creation would have failed on a
        // truly unsupported platform.
        return true;
    };
    // If no batteries found at all, this is a desktop/VM — assume AC
    let mut found_any = false;
    for b in batteries.by_ref().flatten() {
        found_any = true;
        if b.state() == battery::State::Discharging {
            return false;
        }
    }
    if !found_any {
        return true; // No battery hardware → AC
    }
    true
}

/// Check if enough RAM is available (not total, not used — available).
fn memory_available(min_gb: u64) -> bool {
    use sysinfo::System;
    let mut sys = System::new();
    sys.refresh_memory();
    let available_mb = sys.available_memory() / (1024 * 1024);
    available_mb >= min_gb * 1024
}
