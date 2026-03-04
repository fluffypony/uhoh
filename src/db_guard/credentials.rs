use anyhow::{Context, Result};

pub fn scrub_dsn(dsn: &str) -> String {
    if let Some((scheme, rest)) = dsn.split_once("://") {
        let rest = rest.split('@').last().unwrap_or(rest);
        format!("{scheme}://{rest}")
    } else {
        dsn.to_string()
    }
}

pub fn ensure_guard_dir(uhoh_dir: &std::path::Path) -> Result<std::path::PathBuf> {
    let dir = uhoh_dir.join("db_guard");
    std::fs::create_dir_all(&dir).context("Failed to create db_guard directory")?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700));
    }
    Ok(dir)
}
