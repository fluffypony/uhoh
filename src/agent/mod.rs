mod audit;
pub mod commands;
#[cfg(target_os = "linux")]
mod fanotify;
mod intercept;
pub mod profiles;
pub mod proxy;
mod runtime;
pub mod sandbox;
pub mod undo;

pub use commands::handle_cli_action;

pub(crate) fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).display().to_string();
        }
    }
    path.to_string()
}
pub use profiles::load_agent_profile;
pub use proxy::{auth_handshake_line, ensure_proxy_token};
pub use runtime::AgentSubsystem;
pub use sandbox::{apply_landlock, sandbox_supported};
