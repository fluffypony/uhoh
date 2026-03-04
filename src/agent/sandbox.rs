#[cfg(target_os = "linux")]
use landlock::{path_beneath_rules, AccessFs, Ruleset};

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
    let abi = landlock::ABI::V1;
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

    let abi = landlock::ABI::V1;
    let mut ruleset = Ruleset::default()
        .handle_access(AccessFs::from_all(abi))?
        .create()?;
    ruleset.set_no_new_privs(true);

    let mut allow_paths = vec![
        "/usr".to_string(),
        "/lib".to_string(),
        "/etc".to_string(),
        "/tmp".to_string(),
        "/dev/null".to_string(),
        "/dev/urandom".to_string(),
        "/proc/self".to_string(),
    ];
    allow_paths.extend(profile.data_dirs.clone());
    allow_paths.extend(profile.memory_dirs.clone());
    allow_paths.extend(profile.config_files.clone());
    allow_paths.extend(profile.personality_files.clone());

    let refs = allow_paths
        .iter()
        .map(std::string::String::as_str)
        .collect::<Vec<_>>();
    ruleset = ruleset.add_rules(path_beneath_rules(&refs, AccessFs::from_all(abi)))?;
    let status = ruleset.restrict_self()?;
    match status.ruleset {
        landlock::RulesetStatus::NotEnforced => {
            anyhow::bail!("Landlock not enforced on this kernel")
        }
        _ => Ok(()),
    }
}

#[cfg(not(target_os = "linux"))]
pub fn apply_landlock(_profile: &crate::agent::profiles::AgentProfile) -> anyhow::Result<()> {
    anyhow::bail!("Landlock is only supported on Linux")
}
