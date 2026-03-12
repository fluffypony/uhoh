use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

use crate::cli::ConfigAction;
use crate::config;
use crate::daemon;
use crate::db;
use crate::gc;
use crate::git;
use crate::platform;
use crate::update;

use super::shared::is_daemon_running;

pub async fn start(uhoh: &Path, database: &db::Database, service: bool) -> Result<()> {
    if service {
        daemon::run_foreground(uhoh, Arc::new(database.clone_handle())).await?;
    } else {
        daemon::spawn_detached_daemon(uhoh)?;
    }
    Ok(())
}

pub fn stop(uhoh: &Path) -> Result<()> {
    daemon::stop_daemon(uhoh)
}

pub fn restart(uhoh: &Path) -> Result<()> {
    daemon::stop_daemon(uhoh).ok();
    std::thread::sleep(std::time::Duration::from_secs(1));
    daemon::spawn_detached_daemon(uhoh)
}

pub fn hook(action: &str) -> Result<()> {
    let project_path = dunce::canonicalize(std::env::current_dir()?)?;
    match action {
        "install" => git::install_hook(&project_path),
        "remove" => git::remove_hook(&project_path),
        other => anyhow::bail!("Unknown hook action: '{other}'. Use 'install' or 'remove'."),
    }
}

pub fn config_action(uhoh: &Path, action: Option<ConfigAction>) -> Result<()> {
    let config_path = uhoh.join("config.toml");
    match action {
        Some(ConfigAction::Edit) => {
            let editor = std::env::var("EDITOR").unwrap_or_else(|_| "vi".to_string());
            std::process::Command::new(&editor)
                .arg(&config_path)
                .status()?;
        }
        Some(ConfigAction::Set { key, value }) => {
            let content = if config_path.exists() {
                std::fs::read_to_string(&config_path)?
            } else {
                String::new()
            };
            let mut doc: toml_edit::DocumentMut = content
                .parse()
                .unwrap_or_else(|_| toml_edit::DocumentMut::new());
            let parts: Vec<&str> = key.split('.').collect();
            match parts.as_slice() {
                [key] => {
                    doc[*key] = toml_edit::value(parse_toml_value(&value));
                }
                [section, key] => {
                    if !doc.contains_key(section) {
                        doc[*section] = toml_edit::Item::Table(toml_edit::Table::new());
                    }
                    doc[*section][*key] = toml_edit::value(parse_toml_value(&value));
                }
                _ => anyhow::bail!("Key nesting deeper than 2 levels is not supported"),
            }
            std::fs::write(&config_path, doc.to_string())?;
            println!("Set {key} = {value}");
        }
        Some(ConfigAction::Get { key }) => {
            let content = if config_path.exists() {
                std::fs::read_to_string(&config_path)?
            } else {
                String::new()
            };
            let doc: toml_edit::DocumentMut = content
                .parse()
                .unwrap_or_else(|_| toml_edit::DocumentMut::new());
            let parts: Vec<&str> = key.split('.').collect();
            let out = match parts.as_slice() {
                [key] => doc
                    .get(key)
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                [section, key] => doc
                    .get(section)
                    .and_then(|table| table.get(*key))
                    .map(|value| value.to_string())
                    .unwrap_or_default(),
                _ => String::new(),
            };
            println!("{out}");
        }
        None => {
            let cfg = config::Config::load(&config_path)?;
            println!("{}", toml::to_string_pretty(&cfg)?);
        }
    }
    Ok(())
}

pub fn run_gc(uhoh: &Path, database: &db::Database) -> Result<()> {
    gc::run_gc(uhoh, database)
}

pub async fn update(uhoh: &Path) -> Result<()> {
    update::check_and_apply_update(uhoh).await
}

pub async fn status(uhoh: &Path, database: &db::Database) -> Result<()> {
    let running = is_daemon_running(uhoh);
    println!("Daemon: {}", if running { "running" } else { "stopped" });
    let projects = database.list_projects()?;
    println!("Projects: {}", projects.len());
    let total: u64 = projects
        .iter()
        .filter_map(|project| database.snapshot_count(&project.hash).ok())
        .sum();
    println!("Snapshots: {total}");
    let size = database.get_blob_bytes().unwrap_or(0);
    println!("Blob storage: {:.1} MB", size as f64 / 1_048_576.0);
    let cfg = config::Config::load(&uhoh.join("config.toml")).unwrap_or_default();
    println!(
        "AI: {}",
        if cfg.ai.enabled {
            "enabled"
        } else {
            "disabled"
        }
    );
    for project in &projects {
        let project_path = Path::new(&project.current_path);
        if uhoh.starts_with(project_path) {
            println!(
                "Warning: Project {} includes the uhoh data directory; this may cause snapshot loops.",
                project.current_path
            );
            break;
        }
    }

    if running {
        if let Ok(port_raw) = std::fs::read_to_string(uhoh.join("server.port")) {
            if let Ok(port) = port_raw.trim().parse::<u16>() {
                let url = format!("http://127.0.0.1:{port}/health");
                if let Ok(resp) = reqwest::get(url).await {
                    if let Ok(json) = resp.json::<serde_json::Value>().await {
                        if let Some(subsystems) =
                            json.get("subsystems").and_then(|value| value.as_array())
                        {
                            println!("Subsystems:");
                            for item in subsystems {
                                let name = item
                                    .get("name")
                                    .and_then(|value| value.as_str())
                                    .unwrap_or("unknown");
                                let status = item
                                    .get("status")
                                    .and_then(|value| value.as_str())
                                    .unwrap_or("unknown");
                                println!("  - {}: {}", name, status);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

pub fn install_service() -> Result<()> {
    platform::install_service()?;
    println!("Service installed.");
    Ok(())
}

pub fn remove_service() -> Result<()> {
    platform::remove_service()?;
    println!("Service removed.");
    Ok(())
}

fn parse_toml_value(s: &str) -> toml_edit::Value {
    if s.eq_ignore_ascii_case("true") {
        return toml_edit::Value::from(true);
    }
    if s.eq_ignore_ascii_case("false") {
        return toml_edit::Value::from(false);
    }
    if let Ok(i) = s.parse::<i64>() {
        return toml_edit::Value::from(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return toml_edit::Value::from(f);
    }
    toml_edit::Value::from(s.to_string())
}
