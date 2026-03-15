use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct AgentProfile {
    pub name: String,
    #[serde(default)]
    pub session_log_pattern: String,
    #[serde(default)]
    #[allow(dead_code)] // deserialized from TOML, reserved for future use
    pub process_names: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)] // used by sandbox.rs behind feature gate
    pub data_dirs: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)] // used by sandbox.rs behind feature gate
    pub config_files: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)] // used by sandbox.rs behind feature gate
    pub personality_files: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)] // used by sandbox.rs behind feature gate
    pub memory_dirs: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)] // deserialized from TOML, reserved for future use
    pub tool_names_write: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)] // deserialized from TOML, reserved for future use
    pub tool_names_exec: Vec<String>,
    #[serde(default)]
    #[allow(dead_code)] // deserialized from TOML, reserved for future use
    pub tool_call_format: Option<String>,
}

pub fn load_agent_profile(path: &std::path::Path) -> Result<AgentProfile> {
    validate_profile_path(path)?;
    let raw = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read profile: {}", path.display()))?;
    let profile: AgentProfile = toml::from_str(&raw)
        .with_context(|| format!("Failed to parse profile TOML: {}", path.display()))?;
    if profile.name.trim().is_empty() {
        anyhow::bail!("Agent profile name cannot be empty");
    }
    if profile.session_log_pattern.trim().is_empty() {
        anyhow::bail!("Agent profile must define session_log_pattern");
    }
    Ok(profile)
}

pub fn resolve_session_log_path(pattern: &str) -> Result<Option<std::path::PathBuf>> {
    let expanded = super::expand_home(pattern);
    let mut matches = Vec::new();
    for entry in glob::glob(&expanded)? {
        let Ok(path) = entry else { continue };
        if path.is_file() {
            matches.push(path);
        }
    }
    // Sort by modification time (newest last) to pick the most recent session log,
    // not just the lexicographically last one.
    matches.sort_by_key(|p| {
        p.metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::SystemTime::UNIX_EPOCH)
    });
    Ok(matches.pop())
}

pub fn validate_profile_path(path: &std::path::Path) -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Unable to resolve home directory"))?;
    let canonical = dunce::canonicalize(path)?;
    if !canonical.starts_with(&home) {
        anyhow::bail!("Profile path must be inside the user home directory");
    }

    let mut forbidden = vec![home.join(".ssh"), home.join(".gnupg"), home.join(".aws")];
    #[cfg(target_os = "macos")]
    forbidden.push(home.join("Library/Application Support"));
    for forbidden_path in forbidden {
        if canonical.starts_with(&forbidden_path) {
            anyhow::bail!(
                "Profile path points to a sensitive directory: {}",
                forbidden_path.display()
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: write a TOML profile to a temp file within the home directory
    /// and return the path.
    fn write_profile_in_home(content: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        // Create a temp dir inside the home directory so validate_profile_path succeeds.
        let home = dirs::home_dir().expect("home dir");
        let tmp = tempfile::tempdir_in(&home).expect("tempdir in home");
        let path = tmp.path().join("profile.toml");
        std::fs::write(&path, content).expect("write profile");
        (tmp, path)
    }

    #[test]
    fn parse_valid_profile() {
        let toml = r#"
name = "claude-code"
session_log_pattern = "~/.claude/sessions/*.jsonl"
process_names = ["claude"]
data_dirs = ["~/.claude"]
"#;
        let (_tmp, path) = write_profile_in_home(toml);
        let profile = load_agent_profile(&path).expect("should parse");
        assert_eq!(profile.name, "claude-code");
        assert_eq!(profile.session_log_pattern, "~/.claude/sessions/*.jsonl");
        assert_eq!(profile.process_names, vec!["claude".to_string()]);
        assert_eq!(profile.data_dirs, vec!["~/.claude".to_string()]);
    }

    #[test]
    fn parse_minimal_profile() {
        let toml = r#"
name = "minimal"
session_log_pattern = "/tmp/*.log"
"#;
        let (_tmp, path) = write_profile_in_home(toml);
        let profile = load_agent_profile(&path).expect("should parse minimal profile");
        assert_eq!(profile.name, "minimal");
        assert!(profile.process_names.is_empty());
        assert!(profile.data_dirs.is_empty());
        assert!(profile.config_files.is_empty());
        assert!(profile.personality_files.is_empty());
        assert!(profile.memory_dirs.is_empty());
        assert!(profile.tool_names_write.is_empty());
        assert!(profile.tool_names_exec.is_empty());
        assert!(profile.tool_call_format.is_none());
    }

    #[test]
    fn reject_empty_name() {
        let toml = r#"
name = ""
session_log_pattern = "/tmp/*.log"
"#;
        let (_tmp, path) = write_profile_in_home(toml);
        let err = load_agent_profile(&path).expect_err("empty name should fail");
        assert!(
            err.to_string().contains("name cannot be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reject_whitespace_only_name() {
        let toml = r#"
name = "   "
session_log_pattern = "/tmp/*.log"
"#;
        let (_tmp, path) = write_profile_in_home(toml);
        let err = load_agent_profile(&path).expect_err("whitespace-only name should fail");
        assert!(
            err.to_string().contains("name cannot be empty"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reject_empty_session_log_pattern() {
        let toml = r#"
name = "test-agent"
session_log_pattern = ""
"#;
        let (_tmp, path) = write_profile_in_home(toml);
        let err = load_agent_profile(&path).expect_err("empty pattern should fail");
        assert!(
            err.to_string().contains("session_log_pattern"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reject_missing_session_log_pattern() {
        let toml = r#"
name = "test-agent"
"#;
        let (_tmp, path) = write_profile_in_home(toml);
        let err = load_agent_profile(&path).expect_err("missing pattern should fail");
        assert!(
            err.to_string().contains("session_log_pattern"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn reject_invalid_toml() {
        let content = "this is not valid [[[toml";
        let (_tmp, path) = write_profile_in_home(content);
        let err = load_agent_profile(&path).expect_err("invalid TOML should fail");
        assert!(
            err.to_string().contains("Failed to parse"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_session_log_path_no_matches() {
        // Use a glob pattern that won't match anything
        let result =
            resolve_session_log_path("/tmp/uhoh-test-nonexistent-dir-xyz/*.nonexistent")
                .expect("should not error");
        assert!(result.is_none());
    }

    #[test]
    fn resolve_session_log_path_finds_files() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let file_a = tmp.path().join("session_a.log");
        let file_b = tmp.path().join("session_b.log");
        std::fs::write(&file_a, "log a").expect("write a");
        // Small delay so mtime differs
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::fs::write(&file_b, "log b").expect("write b");

        let pattern = tmp.path().join("*.log").display().to_string();
        let result = resolve_session_log_path(&pattern).expect("should succeed");
        assert!(result.is_some());
        // Should pick the most recently modified file
        let resolved = result.unwrap();
        assert_eq!(
            resolved.file_name().unwrap().to_str().unwrap(),
            "session_b.log"
        );
    }

    #[test]
    fn resolve_session_log_path_ignores_directories() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let subdir = tmp.path().join("not-a-file.log");
        std::fs::create_dir_all(&subdir).expect("create dir");

        let pattern = tmp.path().join("*.log").display().to_string();
        let result = resolve_session_log_path(&pattern).expect("should succeed");
        assert!(result.is_none(), "directories should not be matched");
    }

    #[test]
    fn validate_profile_path_rejects_sensitive_dir() {
        // Create a real file inside a sensitive directory pattern to test the check
        let home = dirs::home_dir().expect("home dir");
        let ssh_dir = home.join(".ssh");
        if ssh_dir.exists() {
            // Create a temp file inside .ssh to ensure canonicalize succeeds
            let test_file = ssh_dir.join(".uhoh_test_profile.toml");
            std::fs::write(&test_file, "name = \"test\"\nsession_log_pattern = \"*.log\"")
                .expect("write test file");
            let result = validate_profile_path(&test_file);
            let _ = std::fs::remove_file(&test_file);
            let err = result.expect_err("paths in .ssh should be rejected");
            assert!(
                err.to_string().contains("sensitive directory"),
                "expected sensitive directory error, got: {err}"
            );
        }
    }
}
