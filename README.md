# uhoh

> For when your AI coding agent develops a mind of its own

`uhoh` is a local-only, cross-platform (Windows/Linux/macOS) filesystem snapshot
tool. It monitors registered directories, creates content-addressable snapshots
on file changes, supports time-travel recovery, optional AI-powered change
descriptions, and self-updates from GitHub with cryptographic verification.

It's basically **ctrl-z for AI agents**.

## Quick Start

```bash
# Register the current directory for watching
uhoh add     # or: uhoh +

# The daemon starts automatically. Manual control:
uhoh start
uhoh stop

# List registered projects
uhoh list    # or: uhoh l

# View snapshots
uhoh snapshots  # or: uhoh s

# Restore to a snapshot
uhoh restore <id>  # or: uhoh r <id>

# Create a manual snapshot
uhoh commit "before big refactor"  # or: uhoh c "message"

# Diff snapshots
uhoh diff        # or: uhoh d
uhoh diff <id1> <id2>

# View a file at a point in time
uhoh cat src/main.rs <id>  # or: uhoh p src/main.rs <id>

# File history
uhoh log src/main.rs  # or: uhoh o src/main.rs

# Mark an AI-agent operation for grouped undo
uhoh mark "implementing auth module"
# ... AI agent makes changes ...
uhoh operations   # list operations
uhoh undo         # revert last operation
```

## Configuration

Global config lives at `~/.uhoh/config.toml`. Run `uhoh config` to view or
`uhoh config edit` to open in your editor.

## Architecture

- **Content-Addressable Store**: BLAKE3-hashed blobs under `~/.uhoh/blobs/`
- **SQLite Metadata**: All manifests, registry, and operation data in `~/.uhoh/uhoh.db`
- **Daemon**: Single process watching all registered directories via OS-native file events
- **AI Summaries**: Optional local Qwen models via llama.cpp / mlx-lm sidecar
- **Updates**: Ed25519 signature verification with DNS TXT supplementary check
