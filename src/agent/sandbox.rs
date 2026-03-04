#[cfg(target_os = "linux")]
use landlock::{
    path_beneath_rules, AccessFs, LandlockStatus, RestrictionStatus, Ruleset, RulesetStatus, ABI,
};

pub fn sandbox_supported() -> bool {
    #[cfg(target_os = "linux")]
    {
        runtime_probe().is_ok()
    }
    #[cfg(not(target_os = "linux"))]
    {
        false
    }
}

#[cfg(target_os = "linux")]
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

#[cfg(target_os = "linux")]
pub fn apply_landlock(profile: &crate::agent::profiles::AgentProfile) -> anyhow::Result<()> {
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

    let mut allow_paths = vec![
        "/usr".to_string(),
        "/lib".to_string(),
        "/etc".to_string(),
        "/etc/ld.so.cache".to_string(),
        "/tmp".to_string(),
        "/dev/null".to_string(),
        "/dev/urandom".to_string(),
        "/proc/self".to_string(),
    ];
    if let Ok(cwd) = std::env::current_dir() {
        allow_paths.push(cwd.display().to_string());
    }
    allow_paths.push(crate::uhoh_dir().display().to_string());
    allow_paths.extend(profile.data_dirs.clone());
    allow_paths.extend(profile.memory_dirs.clone());
    allow_paths.extend(profile.config_files.clone());
    allow_paths.extend(profile.personality_files.clone());

    let refs = allow_paths
        .iter()
        .map(|path| expand_home(path))
        .filter_map(|path| {
            let candidate = std::path::PathBuf::from(&path);
            if candidate.exists() {
                dunce::canonicalize(&candidate).ok().or(Some(candidate))
            } else {
                None
            }
        })
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    ruleset = ruleset.add_rules(path_beneath_rules(&refs, AccessFs::from_all(abi)))?;
    let status = ruleset.restrict_self()?;
    log_restriction_status(&status.restriction);
    match status.ruleset {
        RulesetStatus::FullyEnforced | RulesetStatus::PartiallyEnforced => Ok(()),
        RulesetStatus::NotEnforced => anyhow::bail!("Landlock not enforced on this kernel"),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn apply_landlock(_profile: &crate::agent::profiles::AgentProfile) -> anyhow::Result<()> {
    anyhow::bail!("Landlock is only supported on Linux")
}

#[cfg(target_os = "linux")]
fn expand_home(path: &str) -> String {
    if let Some(rest) = path.strip_prefix("~/") {
        if let Some(home) = dirs::home_dir() {
            return home.join(rest).display().to_string();
        }
    }
    path.to_string()
}

#[cfg(target_os = "linux")]
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
