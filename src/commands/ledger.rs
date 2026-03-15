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
    let events = database.event_ledger_recent(db::LedgerRecentFilters::default(), 500)?;
    if let Some(seed) = events
        .into_iter()
        .find(|entry| entry.path.as_deref() == Some(path))
    {
        let chain = database.event_ledger_trace(seed.id)?;
        println!("Blame chain for {path}");
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
        println!("No events found for path {path}");
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

    let mut events = database.event_ledger_recent(
        db::LedgerRecentFilters {
            source: normalized_source,
            since: since_rfc3339.as_deref(),
            ..Default::default()
        },
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
        println!("Ledger verified: {count} event(s), chain intact");
        return Ok(());
    }

    let broken_ids = broken
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    anyhow::bail!(
        "Ledger verification failed: {} broken event(s): {}",
        broken.len(),
        broken_ids
    );
}

fn normalize_timeline_source(source: &str) -> Result<LedgerSource> {
    let source = source.trim().to_ascii_lowercase();
    LedgerSource::parse(&source).ok_or_else(|| {
        anyhow::anyhow!(
            "Invalid --source '{source}'. Expected one of: fs, db_guard, agent, daemon, mlx"
        )
    })
}

fn parse_since_cutoff(raw: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    let since = raw.trim();
    if since.is_empty() {
        anyhow::bail!("--since cannot be empty");
    }
    if since.len() < 2 {
        anyhow::bail!("Invalid --since format '{raw}'; use forms like 30m, 1h, 2d");
    }

    let split_pos = since
        .find(|c: char| c.is_ascii_alphabetic())
        .unwrap_or(since.len());
    let num_part = &since[..split_pos];
    let unit_part = &since[split_pos..];
    let qty: i64 = num_part
        .parse()
        .with_context(|| format!("Invalid --since quantity '{num_part}'"))?;
    if qty <= 0 {
        anyhow::bail!("--since quantity must be positive");
    }

    let delta = match unit_part.to_ascii_lowercase().as_str() {
        "s" | "sec" | "secs" => chrono::Duration::seconds(qty),
        "m" | "min" | "mins" => chrono::Duration::minutes(qty),
        "h" | "hr" | "hrs" | "hour" | "hours" => chrono::Duration::hours(qty),
        "d" | "day" | "days" => chrono::Duration::days(qty),
        _ => anyhow::bail!("Invalid --since unit '{unit_part}'. Use one of s, m, h, d"),
    };

    Ok(chrono::Utc::now() - delta)
}

#[cfg(test)]
mod tests {
    use super::normalize_timeline_source;
    use crate::db::LedgerSource;

    #[test]
    fn timeline_source_requires_canonical_names() {
        assert_eq!(
            normalize_timeline_source("db_guard").unwrap(),
            LedgerSource::DbGuard
        );
        assert_eq!(normalize_timeline_source("fs").unwrap(), LedgerSource::Fs);
        assert!(normalize_timeline_source("db").is_err());
        assert!(normalize_timeline_source("filesystem").is_err());
    }
}
