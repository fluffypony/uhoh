use anyhow::Result;
use rusqlite::params;

use super::{AgentEntry, Database};

impl Database {
    /// Register a new agent with its profile path and optional data directory.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn add_agent(&self, name: &str, profile_path: &str, data_dir: Option<&str>) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO agents (name, profile_path, data_dir, registered_at, active)
             VALUES (?1, ?2, ?3, ?4, 1)",
            params![name, profile_path, data_dir, now],
        )?;
        Ok(())
    }

    /// List all registered agents ordered by ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn list_agents(&self) -> Result<Vec<AgentEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, name, profile_path, data_dir, registered_at, active
             FROM agents ORDER BY id ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(AgentEntry {
                id: row.get(0)?,
                name: row.get(1)?,
                profile_path: row.get(2)?,
                data_dir: row.get(3)?,
                registered_at: row.get(4)?,
                active: row.get::<_, i32>(5)? != 0,
            })
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        Ok(out)
    }

    /// Delete an agent by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the database operation fails.
    pub fn remove_agent(&self, name: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM agents WHERE name = ?1", params![name])?;
        Ok(())
    }
}
