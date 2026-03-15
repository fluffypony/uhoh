#[cfg(all(target_os = "linux", feature = "landlock-sandbox"))]
use landlock::{
    path_beneath_rules, AccessFs, LandlockStatus, RestrictionStatus, Ruleset, RulesetStatus, ABI,
};

pub fn sandbox_supported() -> bool {
    #[cfg(all(target_os = "linux", feature = "landlock-sandbox"))]
    {
        runtime_probe().is_ok()
    }
    #[cfg(all(target_os = "linux", not(feature = "landlock-sandbox")))]
    {
        false
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(all(target_os = "linux", feature = "landlock-sandbox"))]
fn runtime_probe() -> anyhow::Result<()> {
    let abi = match LandlockStatus::default() {
        LandlockStatus::Available { effective_abi, .. } => effective_abi,
        _ => ABI::Unsupported,
    };
    if abi == ABI::Unsupported {
        anyhow::bail!("Landlock unsupported or disabled");
    }
    let _ = Ruleset::default().handle_access(AccessFs::from_all(abi))?;
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "landlock-sandbox"))]
pub(crate) fn apply_landlock(profile: &crate::agent::profiles::AgentProfile) -> anyhow::Result<()> {
    // SAFETY: prctl(PR_SET_NO_NEW_PRIVS, 1) is safe to call — it restricts the
    // current thread from gaining new privileges, which is a prerequisite for
    // Landlock enforcement. Failure is checked and surfaced as an error.
    unsafe {
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            return Err(anyhow::anyhow!("Failed to set PR_SET_NO_NEW_PRIVS"));
        }
    }

    let abi = match LandlockStatus::default() {
        LandlockStatus::Available { effective_abi, .. } => effective_abi,
        _ => ABI::Unsupported,
    };
    if abi == ABI::Unsupported {
        anyhow::bail!("Landlock unsupported or disabled");
    }
    let mut ruleset = Ruleset::default()
        .handle_access(AccessFs::from_all(abi))?
        .create()?;
    ruleset.set_no_new_privs(true);

    // System paths: read-only access (prevents escalation even with root)
    let system_paths = vec![
        "/usr".to_string(),
        "/bin".to_string(),
        "/sbin".to_string(),
        "/lib".to_string(),
        "/lib64".to_string(),
        "/etc".to_string(),
        "/etc/ld.so.cache".to_string(),
        "/dev/null".to_string(),
        "/dev/urandom".to_string(),
        "/proc/self".to_string(),
    ];

    // Workspace paths: full read/write access
    let mut workspace_paths = vec!["/tmp".to_string()];
    if let Ok(cwd) = std::env::current_dir() {
        workspace_paths.push(cwd.display().to_string());
    }
    workspace_paths.push(crate::uhoh_dir().display().to_string());
    workspace_paths.extend(profile.data_dirs.clone());
    workspace_paths.extend(profile.memory_dirs.clone());
    workspace_paths.extend(profile.config_files.clone());
    workspace_paths.extend(profile.personality_files.clone());

    let canonicalize_paths = |paths: &[String]| -> Vec<String> {
        paths
            .iter()
            .map(|p| super::expand_home(p))
            .filter_map(|p| {
                let candidate = std::path::PathBuf::from(&p);
                if candidate.exists() {
                    dunce::canonicalize(&candidate).ok().or(Some(candidate))
                } else {
                    None
                }
            })
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .map(|p| p.display().to_string())
            .collect()
    };

    let sys_refs = canonicalize_paths(&system_paths);
    let sys_strs: Vec<&str> = sys_refs.iter().map(String::as_str).collect();
    ruleset = ruleset.add_rules(path_beneath_rules(&sys_strs, AccessFs::from_read(abi)))?;

    let ws_refs = canonicalize_paths(&workspace_paths);
    let ws_strs: Vec<&str> = ws_refs.iter().map(String::as_str).collect();
    ruleset = ruleset.add_rules(path_beneath_rules(&ws_strs, AccessFs::from_all(abi)))?;
    let status = ruleset.restrict_self()?;
    log_restriction_status(&status.restriction);
    match status.ruleset {
        RulesetStatus::FullyEnforced | RulesetStatus::PartiallyEnforced => Ok(()),
        RulesetStatus::NotEnforced => anyhow::bail!("Landlock not enforced on this kernel"),
    }
}

#[cfg(all(target_os = "linux", not(feature = "landlock-sandbox")))]
pub(crate) fn apply_landlock(_profile: &crate::agent::profiles::AgentProfile) -> anyhow::Result<()> {
    anyhow::bail!("Landlock sandboxing requires building uhoh with --features landlock-sandbox")
}

#[cfg(not(target_os = "linux"))]
#[allow(dead_code)]
pub(crate) fn apply_landlock(_profile: &crate::agent::profiles::AgentProfile) -> anyhow::Result<()> {
    tracing::warn!("Sandboxing is only available on Linux. Running without sandbox.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sandbox_supported_returns_bool() {
        let supported = sandbox_supported();
        // On macOS and non-Linux, this should be false
        #[cfg(not(target_os = "linux"))]
        assert!(!supported, "sandbox_supported() should be false on non-Linux");
        // On Linux without the feature, also false
        #[cfg(all(target_os = "linux", not(feature = "landlock-sandbox")))]
        assert!(!supported, "sandbox_supported() should be false without landlock feature");
        // Just verify it returns without panicking on any platform
        let _ = supported;
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn apply_landlock_non_linux_succeeds() {
        // Parse a minimal TOML profile to get a valid AgentProfile
        let profile: crate::agent::profiles::AgentProfile =
            toml::from_str("name = \"test\"").unwrap();
        // On non-Linux, apply_landlock logs a warning but returns Ok
        assert!(apply_landlock(&profile).is_ok());
    }
}

#[cfg(all(target_os = "linux", feature = "landlock-sandbox"))]
fn log_restriction_status(status: &RestrictionStatus) {
    match status {
        RestrictionStatus::FullyEnforced => {
            tracing::info!("Landlock restrictions fully enforced")
        }
        RestrictionStatus::PartiallyEnforced => {
            tracing::warn!("Landlock restrictions partially enforced on this kernel")
        }
        RestrictionStatus::NotEnforced => tracing::warn!("Landlock restrictions not enforced"),
    }
}
