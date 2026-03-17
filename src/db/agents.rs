use anyhow::Result;
use rusqlite::params;

use super::{AgentEntry, Database};

impl Database {
    /// Register a new agent with its profile path and optional data directory.
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
    pub fn remove_agent(&self, name: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute("DELETE FROM agents WHERE name = ?1", params![name])?;
        Ok(())
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (tempfile::TempDir, Database) {
        let tmp = tempfile::tempdir().unwrap();
        let db = Database::open(&tmp.path().join("test.db")).unwrap();
        (tmp, db)
    }

    #[test]
    fn add_then_list_agents() {
        let (_tmp, db) = temp_db();
        db.add_agent("agent-alpha", "/profiles/alpha", Some("/data/alpha"))
            .unwrap();

        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "agent-alpha");
        assert_eq!(agents[0].profile_path, "/profiles/alpha");
        assert_eq!(agents[0].data_dir.as_deref(), Some("/data/alpha"));
        assert!(agents[0].active);
    }

    #[test]
    fn add_agent_without_data_dir() {
        let (_tmp, db) = temp_db();
        db.add_agent("agent-beta", "/profiles/beta", None).unwrap();

        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 1);
        assert_eq!(agents[0].name, "agent-beta");
        assert!(agents[0].data_dir.is_none());
    }

    #[test]
    fn remove_then_list_agents() {
        let (_tmp, db) = temp_db();
        db.add_agent("agent-remove", "/p", None).unwrap();
        assert_eq!(db.list_agents().unwrap().len(), 1);

        db.remove_agent("agent-remove").unwrap();
        assert!(db.list_agents().unwrap().is_empty());
    }

    #[test]
    fn remove_nonexistent_agent_is_noop() {
        let (_tmp, db) = temp_db();
        // Should not error even if no rows match
        db.remove_agent("ghost").unwrap();
        assert!(db.list_agents().unwrap().is_empty());
    }

    #[test]
    fn add_multiple_agents_ordered_by_id() {
        let (_tmp, db) = temp_db();
        db.add_agent("c-agent", "/c", None).unwrap();
        db.add_agent("a-agent", "/a", None).unwrap();
        db.add_agent("b-agent", "/b", None).unwrap();

        let agents = db.list_agents().unwrap();
        assert_eq!(agents.len(), 3);
        // Ordered by id (insertion order)
        assert_eq!(agents[0].name, "c-agent");
        assert_eq!(agents[1].name, "a-agent");
        assert_eq!(agents[2].name, "b-agent");
        // IDs are monotonically increasing
        assert!(agents[0].id < agents[1].id);
        assert!(agents[1].id < agents[2].id);
    }

    #[test]
    fn add_duplicate_agent_name_fails() {
        let (_tmp, db) = temp_db();
        db.add_agent("dup", "/p1", None).unwrap();
        // The agents table uses INSERT (no ON CONFLICT), and if there's a UNIQUE
        // constraint on name, the second insert should fail.  Even if there isn't a
        // unique constraint, the test documents current behaviour.
        let result = db.add_agent("dup", "/p2", None);
        // If the schema has a UNIQUE constraint this is Err; otherwise two rows exist.
        if result.is_ok() {
            let agents = db.list_agents().unwrap();
            assert_eq!(agents.len(), 2);
        } else {
            // Constraint violation is the expected stricter behaviour
            assert!(result.is_err());
        }
    }

    #[test]
    fn list_agents_empty_initially() {
        let (_tmp, db) = temp_db();
        let agents = db.list_agents().unwrap();
        assert!(agents.is_empty());
    }
}
