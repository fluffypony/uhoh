use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct CredentialMaterial {
    pub username: Option<String>,
    pub password: Option<String>,
}

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

pub fn resolve_postgres_credentials(connection_ref: &str) -> Result<CredentialMaterial> {
    if let Some(material) = resolve_pgpass(connection_ref)? {
        return Ok(material);
    }

    if let Ok(master) = std::env::var("UHOH_MASTER_KEY") {
        if !master.trim().is_empty() {
            return Ok(CredentialMaterial {
                username: None,
                password: Some(master),
            });
        }
    }

    Ok(CredentialMaterial {
        username: None,
        password: None,
    })
}

fn resolve_pgpass(connection_ref: &str) -> Result<Option<CredentialMaterial>> {
    let home = match dirs::home_dir() {
        Some(h) => h,
        None => return Ok(None),
    };
    let pgpass = home.join(".pgpass");
    if !pgpass.exists() {
        return Ok(None);
    }

    let uri = connection_ref
        .strip_prefix("postgres://")
        .or_else(|| connection_ref.strip_prefix("postgresql://"))
        .unwrap_or(connection_ref);

    let (user_host, dbname) = match uri.split_once('/') {
        Some(parts) => parts,
        None => return Ok(None),
    };
    let (user, host_port) = match user_host.split_once('@') {
        Some((u, hp)) => (Some(u.to_string()), hp),
        None => (None, user_host),
    };
    let (host, port) = host_port.split_once(':').unwrap_or((host_port, "5432"));

    let content = std::fs::read_to_string(&pgpass)
        .with_context(|| format!("Failed reading {}", pgpass.display()))?;
    for raw in content.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let fields: Vec<&str> = line.split(':').collect();
        if fields.len() != 5 {
            continue;
        }
        let host_ok = fields[0] == "*" || fields[0] == host;
        let port_ok = fields[1] == "*" || fields[1] == port;
        let db_ok = fields[2] == "*" || fields[2] == dbname;
        let user_ok = user
            .as_deref()
            .map(|u| fields[3] == "*" || fields[3] == u)
            .unwrap_or(fields[3] == "*");
        if host_ok && port_ok && db_ok && user_ok {
            return Ok(Some(CredentialMaterial {
                username: if fields[3] == "*" {
                    user.clone()
                } else {
                    Some(fields[3].to_string())
                },
                password: Some(fields[4].to_string()),
            }));
        }
    }
    Ok(None)
}

pub fn scrub_error_message(msg: &str) -> String {
    let mut out = msg.to_string();
    if let Some(idx) = out.find("postgres://") {
        let end = out[idx..]
            .find(char::is_whitespace)
            .map(|v| idx + v)
            .unwrap_or(out.len());
        let dsn = &out[idx..end];
        out.replace_range(idx..end, &scrub_dsn(dsn));
    }
    out
}
