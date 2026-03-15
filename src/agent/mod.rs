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

pub use commands::handle_agent_action;

pub(crate) fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).display().to_string();
        }
    }
    path.to_string()
}
pub use proxy::{auth_handshake_line, ensure_proxy_token};
pub use runtime::AgentSubsystem;
pub use sandbox::sandbox_supported;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_home_with_tilde() {
        let result = expand_home("~/Documents/file.txt");
        // Should expand to home dir path if home dir is available
        if dirs::home_dir().is_some() {
            assert!(!result.starts_with("~/"));
            assert!(result.ends_with("Documents/file.txt"));
        }
    }

    #[test]
    fn expand_home_without_tilde() {
        assert_eq!(expand_home("/absolute/path"), "/absolute/path");
        assert_eq!(expand_home("relative/path"), "relative/path");
        assert_eq!(expand_home(""), "");
    }

    #[test]
    fn expand_home_tilde_without_slash() {
        // "~foo" should NOT be expanded (only "~/..." is)
        assert_eq!(expand_home("~foo"), "~foo");
        assert_eq!(expand_home("~"), "~");
    }

    #[test]
    fn expand_home_tilde_slash_only() {
        let result = expand_home("~/");
        if let Some(home) = dirs::home_dir() {
            // "~/" expands to home dir + "/"
            assert!(result.starts_with(&home.display().to_string()));
        }
    }
}
