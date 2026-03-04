use anyhow::Result;

pub fn validate_profile_path(path: &std::path::Path) -> Result<()> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("Unable to resolve home directory"))?;
    let canonical = dunce::canonicalize(path)?;
    if !canonical.starts_with(&home) {
        anyhow::bail!("Profile path must be inside the user home directory");
    }
    Ok(())
}
