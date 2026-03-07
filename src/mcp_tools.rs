use serde_json::json;

/// Shared MCP tool definitions used by both HTTP and STDIO transports.
pub fn tool_definitions() -> serde_json::Value {
    json!({
        "tools": [
            {
                "name": "create_snapshot",
                "description": "Create a manual snapshot of a project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "project_hash": { "type": "string" },
                        "message": { "type": "string" }
                    }
                }
            },
            {
                "name": "list_snapshots",
                "description": "List snapshots for a project.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "project_hash": { "type": "string" },
                        "limit": { "type": "integer", "default": 20 },
                        "offset": { "type": "integer", "default": 0 }
                    }
                }
            },
            {
                "name": "restore_snapshot",
                "description": "Restore to a previous snapshot. Defaults to dry run.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "snapshot_id": { "type": "string" },
                        "path": { "type": "string" },
                        "project_hash": { "type": "string" },
                        "dry_run": { "type": "boolean", "default": true },
                        "confirm": { "type": "boolean", "default": false },
                        "target_path": {
                            "type": "string",
                            "description": "Optional single file path to restore within the snapshot"
                        }
                    },
                    "required": ["snapshot_id"]
                }
            },
            {
                "name": "uhoh_pre_notify",
                "description": "Cooperative pre-action notification for agent actions.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "agent": { "type": "string" },
                        "action": { "type": "string" },
                        "path": { "type": "string" }
                    },
                    "required": ["agent", "action"]
                }
            }
        ]
    })
}
