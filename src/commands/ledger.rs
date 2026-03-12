use anyhow::{Context, Result};

use crate::db::{self, LedgerSource};

pub fn trace(database: &db::Database, event_id: i64) -> Result<()> {
    let chain = database.event_ledger_trace(event_id)?;
    if chain.entries.is_empty() {
        println!("No events found for trace id {event_id}");
        return Ok(());
    }

    for entry in chain.entries {
        println!(
            "#{} {} {} [{}] {}",
            entry.id, entry.ts, entry.source, entry.severity, entry.event_type
        );
    }
    if chain.truncated {
        println!("Trace truncated after 1024 links; graph depth exceeded safe traversal limit.");
    }
    Ok(())
}

pub fn blame(database: &db::Database, path: &str) -> Result<()> {
    let events = database.event_ledger_recent(None, None, None, None, 500)?;
    if let Some(seed) = events
        .into_iter()
        .find(|entry| entry.path.as_deref() == Some(path))
    {
        let chain = database.event_ledger_trace(seed.id)?;
        println!("Blame chain for {}", path);
        for entry in chain.entries {
            println!(
                "#{} {} {} [{}] {}",
                entry.id, entry.ts, entry.source, entry.severity, entry.event_type
            );
        }
        if chain.truncated {
            println!(
                "Trace truncated after 1024 links; graph depth exceeded safe traversal limit."
            );
        }
    } else {
        println!("No events found for path {}", path);
    }
    Ok(())
}

pub fn timeline(
    database: &db::Database,
    source: Option<String>,
    since: Option<String>,
) -> Result<()> {
    let normalized_source = source
        .as_deref()
        .map(normalize_timeline_source)
        .transpose()?;
    let since_cutoff = since.as_deref().map(parse_since_cutoff).transpose()?;
    let since_rfc3339 = since_cutoff.map(|cutoff| cutoff.to_rfc3339());

    let mut events = database.event_ledger_recent_since(
        normalized_source,
        None,
        None,
        None,
        since_rfc3339.as_deref(),
        1000,
    )?;
    events.reverse();

    if events.is_empty() {
        println!("No timeline events matched filters");
        return Ok(());
    }

    for entry in events {
        println!(
            "#{} {} {} [{}] {}{}",
            entry.id,
            entry.ts,
            entry.source,
            entry.severity,
            entry.event_type,
            entry
                .path
                .as_deref()
                .map(|path| format!(" path={path}"))
                .unwrap_or_default()
        );
    }
    Ok(())
}

pub fn verify(database: &db::Database) -> Result<()> {
    let (count, broken) = database.verify_event_ledger_chain()?;
    if broken.is_empty() {
        println!("Ledger verified: {} event(s), chain intact", count);
        return Ok(());
    }

    println!(
        "Ledger verification failed: {} broken event(s): {}",
        broken.len(),
        broken
            .iter()
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    );
    std::process::exit(2);
}

fn normalize_timeline_source(source: &str) -> Result<LedgerSource> {
    let source = source.trim().to_ascii_lowercase();
    let normalized = match source.as_str() {
        "fs" | "filesystem" => LedgerSource::Fs,
        "db" | "db_guard" | "database" => LedgerSource::DbGuard,
        "agent" => LedgerSource::Agent,
        "daemon" => LedgerSource::Daemon,
        "mlx" => LedgerSource::Mlx,
        _ => {
            anyhow::bail!(
                "Invalid --source '{}'. Expected one of: fs, db_guard, agent, daemon, mlx",
                source
            )
        }
    };
    Ok(normalized)
}

fn parse_since_cutoff(raw: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    let since = raw.trim();
    if since.is_empty() {
        anyhow::bail!("--since cannot be empty");
    }
    if since.len() < 2 {
        anyhow::bail!(
            "Invalid --since format '{}'; use forms like 30m, 1h, 2d",
            raw
        );
    }

    let split_pos = since
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(since.len());
    let num_part = &since[..split_pos];
    let unit_part = &since[split_pos..];
    let qty: i64 = num_part
        .parse()
        .with_context(|| format!("Invalid --since quantity '{}'", num_part))?;
    if qty <= 0 {
        anyhow::bail!("--since quantity must be positive");
    }

    let delta = match unit_part.to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" => chrono::Duration::seconds(qty),
        "m" | "min" | "mins" => chrono::Duration::minutes(qty),
        "h" | "hr" | "hrs" | "hour" | "hours" => chrono::Duration::hours(qty),
        "d" | "day" | "days" => chrono::Duration::days(qty),
        _ => anyhow::bail!(
            "Invalid --since unit '{}'. Use one of s, m, h, d",
            unit_part
        ),
    };

    Ok(chrono::Utc::now() - delta)
}
