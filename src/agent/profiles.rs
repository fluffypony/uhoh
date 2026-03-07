use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AgentProfile {
    #[serde(default)]
    pub profile_version: Option<u32>,
    pub name: String,
    #[serde(default)]
    pub session_log_pattern: String,
    #[serde(default)]
    pub process_names: Vec<String>,
    #[serde(default)]
    pub data_dirs: Vec<String>,
    #[serde(default)]
    pub config_files: Vec<String>,
    #[serde(default)]
    pub personality_files: Vec<String>,
    #[serde(default)]
    pub memory_dirs: Vec<String>,
    #[serde(default)]
    pub tool_names_write: Vec<String>,
    #[serde(default)]
    pub tool_names_exec: Vec<String>,
    #[serde(default)]
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
    let expanded = crate::util::expand_home(pattern);
    let mut matches = Vec::new();
    for entry in glob::glob(&expanded)? {
        let path = match entry {
            Ok(p) => p,
            Err(_) => continue,
        };
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

    let forbidden = [
        home.join(".ssh"),
        home.join(".gnupg"),
        home.join(".aws"),
        home.join("Library/Application Support"),
    ];
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
