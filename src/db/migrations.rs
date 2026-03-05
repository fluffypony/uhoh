use anyhow::Result;
use rusqlite::Connection;

/// Apply the consolidated schema. For fresh databases, executes the full schema.
/// For existing databases (any user_version > 0), this is a no-op since all tables
/// already exist via prior incremental migrations.
pub fn apply_migrations(conn: &Connection, version: i32) -> Result<()> {
    if version < 8 {
        conn.execute_batch(include_str!("schema.sql"))?;
    }
    Ok(())
}
