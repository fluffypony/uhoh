use anyhow::{Context, Result};
use rusqlite::{params, OptionalExtension};
use std::path::Path;

use super::{Database, ProjectEntry};

impl Database {
    pub fn add_project(&self, hash: &str, path: &str) -> Result<()> {
        let conn = self.conn()?;
        let now = chrono::Utc::now().to_rfc3339();
        conn.execute(
            "INSERT INTO projects (hash, current_path, created_at) VALUES (?1, ?2, ?3)",
            params![hash, path, now],
        )?;
        Ok(())
    }

    pub fn get_project(&self, hash: &str) -> Result<Option<ProjectEntry>> {
        let conn = self.conn()?;
        conn.query_row(
            "SELECT hash, current_path, created_at FROM projects WHERE hash = ?1",
            params![hash],
            |row| {
                Ok(ProjectEntry {
                    hash: row.get(0)?,
                    current_path: row.get(1)?,
                    created_at: row.get(2)?,
                })
            },
        )
        .optional()
        .context("Failed to query project")
    }

    pub fn find_project_by_path(&self, path: &Path) -> Result<Option<ProjectEntry>> {
        let conn = self.conn()?;
        let path_str = path.to_string_lossy();
        conn.query_row(
            "SELECT hash, current_path, created_at FROM projects WHERE current_path = ?1",
            params![path_str.as_ref()],
            |row| {
                Ok(ProjectEntry {
                    hash: row.get(0)?,
                    current_path: row.get(1)?,
                    created_at: row.get(2)?,
                })
            },
        )
        .optional()
        .context("Failed to query project by path")
    }

    pub fn find_project_by_hash_prefix(&self, prefix: &str) -> Result<Option<ProjectEntry>> {
        let conn = self.conn()?;
        // Escape SQL wildcards in user-provided prefix using backslash + ESCAPE clause
        let esc = prefix
            .replace('\\', "\\\\")
            .replace('%', "\\%")
            .replace('_', "\\_");
        let pattern = format!("{esc}%");

        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM projects WHERE hash LIKE ?1 ESCAPE '\\'",
            params![pattern.clone()],
            |row| row.get(0),
        )?;

        if count > 1 {
            anyhow::bail!(
                "Ambiguous hash prefix '{prefix}' matches {count} projects. Use a longer prefix."
            );
        }

        conn.query_row(
            "SELECT hash, current_path, created_at FROM projects WHERE hash LIKE ?1 ESCAPE '\\' LIMIT 1",
            params![pattern],
            |row| {
                Ok(ProjectEntry {
                    hash: row.get(0)?,
                    current_path: row.get(1)?,
                    created_at: row.get(2)?,
                })
            },
        )
        .optional()
        .context("Failed to query project by hash prefix")
    }

    pub fn update_project_path(&self, hash: &str, new_path: &str) -> Result<()> {
        let mut conn = self.conn()?;
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE projects SET current_path = ?1 WHERE hash = ?2",
            params![new_path, hash],
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn remove_project(&self, hash: &str) -> Result<()> {
        let conn = self.conn()?;
        conn.execute(
            "DELETE FROM search_index WHERE project_hash = ?1",
            params![hash],
        )?;
        conn.execute("DELETE FROM projects WHERE hash = ?1", params![hash])?;
        Ok(())
    }

    pub fn list_projects(&self) -> Result<Vec<ProjectEntry>> {
        let conn = self.conn()?;
        let mut stmt = conn
            .prepare("SELECT hash, current_path, created_at FROM projects ORDER BY created_at")?;
        let rows = stmt.query_map([], |row| {
            Ok(ProjectEntry {
                hash: row.get(0)?,
                current_path: row.get(1)?,
                created_at: row.get(2)?,
            })
        })?;
        let mut projects = Vec::new();
        for row in rows {
            projects.push(row?);
        }
        Ok(projects)
    }
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db() -> (tempfile::TempDir, Database) {
        let tmp = tempfile::TempDir::new().unwrap();
        let db = Database::open(&tmp.path().join("test.db")).unwrap();
        (tmp, db)
    }

    #[test]
    fn add_and_get_project() {
        let (_tmp, db) = temp_db();
        db.add_project("abc123", "/home/user/project").unwrap();
        let project = db.get_project("abc123").unwrap().unwrap();
        assert_eq!(project.hash, "abc123");
        assert_eq!(project.current_path, "/home/user/project");
    }

    #[test]
    fn get_missing_project_returns_none() {
        let (_tmp, db) = temp_db();
        let result = db.get_project("nonexistent").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn find_project_by_path() {
        let (_tmp, db) = temp_db();
        db.add_project("hash1", "/path/to/project").unwrap();
        let found = db
            .find_project_by_path(Path::new("/path/to/project"))
            .unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().hash, "hash1");
    }

    #[test]
    fn find_project_by_path_missing() {
        let (_tmp, db) = temp_db();
        let found = db.find_project_by_path(Path::new("/nowhere")).unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn find_project_by_hash_prefix() {
        let (_tmp, db) = temp_db();
        db.add_project("abcdef123456", "/project1").unwrap();
        let found = db.find_project_by_hash_prefix("abcdef").unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().hash, "abcdef123456");
    }

    #[test]
    fn find_project_by_hash_prefix_ambiguous() {
        let (_tmp, db) = temp_db();
        db.add_project("abc111", "/p1").unwrap();
        db.add_project("abc222", "/p2").unwrap();
        let result = db.find_project_by_hash_prefix("abc");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Ambiguous"));
    }

    #[test]
    fn find_project_by_hash_prefix_no_match() {
        let (_tmp, db) = temp_db();
        db.add_project("abc123", "/p1").unwrap();
        let found = db.find_project_by_hash_prefix("xyz").unwrap();
        assert!(found.is_none());
    }

    #[test]
    fn update_project_path() {
        let (_tmp, db) = temp_db();
        db.add_project("hash1", "/old/path").unwrap();
        db.update_project_path("hash1", "/new/path").unwrap();
        let project = db.get_project("hash1").unwrap().unwrap();
        assert_eq!(project.current_path, "/new/path");
    }

    #[test]
    fn remove_project() {
        let (_tmp, db) = temp_db();
        db.add_project("hash1", "/path").unwrap();
        db.remove_project("hash1").unwrap();
        let result = db.get_project("hash1").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn list_projects_order() {
        let (_tmp, db) = temp_db();
        db.add_project("z_hash", "/z").unwrap();
        db.add_project("a_hash", "/a").unwrap();
        let projects = db.list_projects().unwrap();
        assert_eq!(projects.len(), 2);
        // Ordered by created_at, which is insertion order
        assert_eq!(projects[0].hash, "z_hash");
        assert_eq!(projects[1].hash, "a_hash");
    }

    #[test]
    fn find_by_hash_prefix_escapes_wildcards() {
        let (_tmp, db) = temp_db();
        db.add_project("abc%def", "/p1").unwrap();
        // "abc%" should not match "abc%def" via wildcard expansion
        let found = db.find_project_by_hash_prefix("abc%").unwrap();
        // It should match literally because % is escaped
        assert!(found.is_some());
    }
}
