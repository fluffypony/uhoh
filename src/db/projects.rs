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
