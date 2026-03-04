# uhoh

*When your coding agent develops a mind of its own, just **uhoh** it*

Local snapshots for messy work. When your AI agent gets overconfident, `uhoh` gives you a clean way back.

uhoh watches your project folders, takes small content‑addressable snapshots as files change, and lets you time‑travel without leaving your editor. It runs locally on macOS, Linux, and Windows. No cloud, no telemetry.

## Highlights

- Fast, local snapshots with BLAKE3 and a deduplicated blob store
- SQLite metadata with transactional snapshot creation and schema migrations
- Tiered storage (reflink → hardlink → copy → none) to keep space under control
- Restore individual files or whole trees; see diffs and file history
- Symlink-aware: stores and restores symlink targets (with Windows fallback)
- Optional zstd compression for blobs (behind `compression` feature flag)
- `.uhohignore` files for project-specific exclusions beyond `.gitignore`
- Optional AI summaries via a local sidecar (Qwen 3.5 tiers, MLX on Apple Silicon); skips on battery/low‑RAM
- Built-in localhost server on `127.0.0.1:22822` with REST API, Time Machine UI, WebSocket events, and MCP HTTP endpoint
- MCP over STDIO with `uhoh mcp` for zero-config agent integration
- Unified event ledger across filesystem, database guard, and agent monitor events
- Event forensics commands: `uhoh trace <event-id>`, `uhoh blame <path>`, and `uhoh timeline [--source ...] [--since ...]`
- Database guardian for PostgreSQL and SQLite with baseline/recovery artifact generation, plus MySQL phase-1 schema polling
- Agent monitoring with MCP proxy interception, session-tail fallback, dangerous-action pause/approve flow, and profile-based registration
- Bearer token auth for mutating server operations (token stored in `~/.uhoh/server.token`)
- Git integration: pre-commit hooks, snapshot-to-stash, worktree support
- Safe auto‑updates: Ed25519 signatures, DNS TXT fallback, atomically applied
- Guardrails: emergency‑delete detection, GC, compaction, and a `doctor` command

## Installation

### Quick Install (Recommended)

macOS / Linux:

```bash
curl -fsSL https://uhoh.it/install.sh | bash
```

Windows (PowerShell):

```powershell
irm https://uhoh.it/install.ps1 | iex
```

### What the Install Script Does

1. Checks for existing installation and reports the current version
2. Detects your OS and CPU architecture and selects the correct binary asset
3. Downloads the latest release from GitHub
4. Installs the binary to a directory on your PATH
5. Verifies binary integrity via DNS TXT records using `uhoh doctor --verify-install`
6. Prints success or a warning if verification could not complete

You can re-verify at any time:

```bash
uhoh doctor --verify-install
```

Manual installation remains supported: download from the latest release and place the binary on your PATH (`/usr/local/bin`, `~/.local/bin`, or `%LOCALAPPDATA%\uhoh\bin`).

## Quick start

### Just type `uhoh`

In a project folder:
- If the folder is not registered yet, `uhoh` will register it and take an initial snapshot.
- If it is already registered, `uhoh` will display status and tips (undo, restore, log) without modifying your files.

### Important `uhoh` commands

```bash
# Register the current directory
uhoh +         # alias: uhoh add

# The daemon starts automatically; you can control it:
uhoh start     # run in background
uhoh stop

# List projects and snapshots
uhoh l         # alias: uhoh list
uhoh s         # alias: uhoh snapshots

# Create and restore
uhoh c "before refactor"   # alias: uhoh commit "before refactor"
uhoh r <id>                # alias: uhoh restore <snapshot-id>

# Diff and time travel
uhoh d                       # alias: uhoh diff (latest vs working tree)
uhoh d <id1> <id2>           # alias: uhoh diff <id1> <id2>
uhoh p src/main.rs <id>      # alias: uhoh cat src/main.rs <id>
uhoh o src/main.rs           # alias: uhoh log src/main.rs

# MCP server over STDIO (Claude Desktop, Cursor, etc.)
uhoh mcp

# Grouped undo for agent runs
uhoh mark "implement search"
uhoh operations
uhoh undo                    # restores to just-before the marked operation

# Unified event-ledger tooling
uhoh timeline --since 1h
uhoh timeline --source agent --since 30m
uhoh trace <event-id>
uhoh blame src/main.rs

# Database guardian
uhoh db add postgres://user@localhost/mydb --tables users,orders --name appdb
uhoh db list
uhoh db events appdb

# Agent monitor (OpenClaw and other MCP/log-based agents)
uhoh agent init
uhoh agent add openclaw --profile ~/.uhoh/agents/openclaw.toml
uhoh agent log openclaw
uhoh agent undo --cascade <event-id>
uhoh agent approve
uhoh run -- openclaw start

# Git integration
uhoh hook install            # add pre-commit snapshot hook
uhoh hook remove             # remove it
uhoh gitstash <id>           # push a snapshot into git stash
```

## Ignore rules

uhoh respects the full `.gitignore` chain (nested `.gitignore` files, `.git/info/exclude`, global gitignore). On top of that, you can create `.uhohignore` files for project-specific exclusions.

Place a `.uhohignore` in the project root or in `.git/.uhohignore`. It uses standard gitignore syntax. Positive patterns add additional ignores; negation patterns (`!pattern`) re-include files that were gitignored.

uhoh always skips `.git` internals and its own `.uhoh` marker file automatically. Symlinks are not followed during directory walking to prevent loops and escaping the project root.

## How it works

uhoh keeps two things in `~/.uhoh`:
1. A blob store `~/.uhoh/blobs/` where file contents live by BLAKE3 hash.
2. A SQLite database `~/.uhoh/uhoh.db` with projects, snapshots, and file lists.

When a file changes, uhoh computes its BLAKE3 hash and tries to store it using a tiered strategy:
1. Reflink (copy‑on‑write clone) if the filesystem supports it
2. Hardlink as a fallback
3. Full copy if under the configured size limit
4. Otherwise record the hash only (not recoverable)

Symlinks are handled separately: uhoh stores the raw symlink target bytes in the blob store (not the file the symlink points to) and restores it as a proper symlink. On Windows, if symlink creation fails (common without Developer Mode or elevated privileges), the target path is written as a regular file instead.

If optional zstd compression is enabled (requires building with the `compression` feature flag), blobs are compressed before storage. If the compressed output ends up larger than the original, the raw bytes are kept instead. Compression level is configurable from 1 to 22, defaulting to 3.

Snapshots are created transactionally inside a single SQLite transaction. Each snapshot contains the file list with size, hash, storage method, mtime, executable bit, and symlink flag. Old snapshots are compacted using time buckets (5‑minute, hourly, daily, weekly), with pinned and message‑bearing snapshots preferentially kept. Manual commits that have a message get minimum daily retention even when they'd otherwise fall into a shorter bucket. Garbage collection prunes unreferenced blobs with a 15-minute grace period to avoid racing with in-progress snapshots.

The daemon uses a notify bridge thread, with a retry/backoff if the watcher dies. It batches changes with a configurable debounce window (quiet period elapsed, or a max ceiling since the first change) and enforces a minimum interval between snapshots per project. Multiple projects are snapshotted concurrently with a parallelism cap based on available CPU cores. Compaction is staggered: one project per tick rather than all at once, reducing lock contention.

The daemon also watches its own binary file and a `~/.uhoh/.update-ready` trigger file. When either changes (after `uhoh update`, for example), the daemon automatically re-execs itself on Unix or spawns a replacement process on Windows.

When enabled, the daemon also starts a unified localhost server (default `127.0.0.1:22822`) that serves:
1. `GET /` Time Machine UI
2. `GET/POST /api/v1/*` snapshot APIs
3. `GET /ws` live events (`snapshot_created`, `snapshot_restored`, `ai_summary_completed`, `sidecar_updated`)
4. `POST /mcp` MCP Streamable HTTP JSON-RPC endpoint
5. `GET /health` and `GET /api/v1/health` health endpoints

Mutating requests require a bearer token by default. The daemon writes the token to `~/.uhoh/server.token` and the bound port to `~/.uhoh/server.port` for local tooling discovery.

## Database guardian and agent monitor

uhoh now includes two subsystem-style safety layers that feed a shared `event_ledger` table.

Database guardian focuses on high-risk events, not full auditing. PostgreSQL guard mode installs trigger-based monitoring and periodic baseline snapshots. SQLite guard mode tracks `PRAGMA data_version` changes and emits recovery references when state shifts. MySQL support is currently phase-1 schema polling.

Agent monitor combines MCP proxy interception with fallback session-log tailing. If your agent talks MCP through uhoh, calls can be classified before they execute. When a call matches dangerous patterns and pause mode is enabled, uhoh records a pending approval and waits for `uhoh agent approve` or timeout.

MCP proxy clients must authenticate on connection by sending a first-line JSON-RPC message:

```json
{"jsonrpc":"2.0","id":"uhoh-auth","method":"uhoh/auth","params":{"token":"<token-from-~/.uhoh/agents/runtime/agent_proxy.token>"}}
```

When you launch tools through `uhoh run`, `UHOH_MCP_PROXY_TOKEN` and `UHOH_MCP_PROXY_AUTH_LINE` are exported automatically for clients that can send a startup auth line.

All of this is tied together in the unified ledger so you can inspect one timeline instead of three separate logs.

## Commands you'll use most

1. `uhoh add [path]` registers a project and creates the first snapshot. A small binary marker file (magic header + 32 random bytes) is written to the project so folder moves can be detected. In git repos it goes into `.git/.uhoh`; in git worktrees (where `.git` is a file pointing to the real git dir) it follows the gitdir path. Non-git projects get `.uhoh` in the project root.
2. `uhoh snapshots` shows the timeline. For each snapshot, you'll see per-file size and storage method: `reflink`, `hardlink`, `copy`, or `none`.
3. `uhoh diff` shows changes between snapshots (or snapshot vs working tree). Output is unified diff with syntax highlighting via syntect. Files larger than 2 MiB are skipped to avoid excessive memory use.
4. `uhoh restore <id>` resets your working tree to a snapshot. Before any destructive changes, uhoh takes a pre‑restore snapshot. Files are first written to a temporary staging directory, then moved into place. On Unix, executable bits are preserved and symlinks are restored. Use `--dry-run` to preview changes without touching files, or `--force` to skip the confirmation prompt when deleting more than 10 files.
5. `uhoh mark / uhoh undo` gives you grouped undo for larger agent runs. Starting a new mark automatically closes any previously active operation. `uhoh undo` closes the current operation (if still active), finds the most recent completed operation, and restores to the snapshot just before it started.
6. `uhoh hook install` adds a git pre-commit hook that takes a snapshot before each commit. If a pre-commit hook already exists, uhoh appends a clearly marked block rather than overwriting. `uhoh hook remove` strips just the uhoh block, leaving any other hooks intact. The hook tries `uhoh` on PATH first; if not found (common in GUI git clients), it falls back to `~/.uhoh/bin/uhoh`.

## Safety nets

- Emergency delete detection: if a large fraction of files disappear (configurable, default 30% and at least 5 files), uhoh creates an emergency snapshot so you can recover.
- Read‑only blobs: stored blobs are set to mode 0400 (Unix) or read-only (Windows) to reduce accidental mutation.
- Integrity checks: reading a blob rehashes the bytes; a mismatch returns no data and logs a clear error.
- Path traversal protection: restore refuses to write files with absolute paths or `..` components, and refuses to write through symlinked parent directories.
- Doctor: `uhoh doctor` runs a database integrity check (SQLite `PRAGMA integrity_check`), compares referenced hashes against what's on disk, finds orphans, and verifies every blob's BLAKE3 hash to detect corruption. With `--fix`, it removes orphans and moves corrupted blobs to `~/.uhoh/quarantine/` with a timestamp.
- Periodic backups: the daemon keeps timestamped backups of `uhoh.db` in `~/.uhoh/backups` and rotates to the most recent 14. `uhoh doctor --restore-latest` can restore the latest one if the integrity check fails.
- Inception guard: `uhoh status` warns if a registered project's path contains the `~/.uhoh` data directory, which would cause snapshot loops.
- Stale temp cleanup: GC and the blob store remove leftover `.tmp.*` and `.blob.*` files from crashed or interrupted snapshot processes (anything older than 10 minutes in prefix dirs, 1 hour in the tmp dir).

## Configuration

Edit `~/.uhoh/config.toml` or use the `uhoh config` subcommands. Running `uhoh config` with no arguments prints the full current config as TOML.

- `uhoh config edit` opens the file in `$EDITOR` (falls back to `vi`)
- `uhoh config set <key> <value>` writes a value (supports dotted keys up to two levels, e.g. `watch.debounce_quiet_secs 5`)
- `uhoh config get <key>` reads a value

Some settings are hot-reloaded by the daemon on its periodic tick without a restart. Others require `uhoh restart`.

### Watch settings

- `watch.debounce_quiet_secs` (default 2): seconds of quiet after the last change before creating a snapshot. **Hot-reloaded.**
- `watch.min_snapshot_interval_secs` (default 5): minimum seconds between snapshots for the same project. Restart required.
- `watch.max_debounce_secs` (default 30): if changes keep arriving, force a snapshot after this many seconds from the first change. Restart required.
- `watch.emergency_delete_threshold` (default 0.30): fraction of tracked files whose deletion triggers an emergency snapshot. Restart required.
- `watch.emergency_delete_min_files` (default 5): minimum file count for the emergency threshold to apply (avoids false positives in small projects). Restart required.

### Storage settings

- `storage.max_copy_blob_bytes` (default 50 MB): maximum file size for a full copy into the blob store when reflink/hardlink aren't available. Restart required.
- `storage.max_binary_blob_bytes` (default 1 MB): size cap for binary files specifically. Larger binaries get their hash recorded but content is not stored. Restart required.
- `storage.max_text_blob_bytes` (default 50 MB): size cap for text files. Restart required.
- `storage.storage_limit_fraction` (default 0.15): per-project blob storage limit as a fraction of the watched folder's total file size. When exceeded, the oldest unpinned snapshots are pruned automatically. Restart required.
- `storage.storage_min_bytes` (default 500 MB): absolute storage floor so small projects aren't starved. Restart required.
- `storage.compress` (default false): enable zstd compression for blobs. Requires the `compression` Cargo feature. Restart required.
- `storage.compress_level` (default 3): zstd level, 1 to 22. Restart required.

### Compaction settings

- `compaction.keep_all_minutes` (default 60): keep every snapshot within this window.
- `compaction.keep_5min_days` (default 14): keep one snapshot per 5-minute bucket for this many days.
- `compaction.keep_hourly_days` (default 30): one per hour for this many days.
- `compaction.keep_daily_days` (default 180): one per day for this many days.
- `compaction.keep_weekly_beyond` (default true): one per week for everything older.
- `compaction.emergency_expire_hours` (default 48): retention window for emergency snapshots before normal bucket rules apply.

All compaction settings require daemon restart.

### AI settings

- `ai.enabled` (default false): turn on AI summaries. Restart required to start/stop the sidecar.
- `ai.skip_on_battery` (default true): skip AI when running on battery power. Restart recommended.
- `ai.max_context_tokens` (default 8192): max tokens of diff context sent to the local model. Restart recommended.
- `ai.idle_shutdown_secs` (default 300): shut down the model server after this many idle seconds. Restart recommended.
- `ai.min_available_memory_gb` (default 4): don't start AI if available RAM is below this. Restart recommended.
- `ai.models` (default empty, uses built-in tiers): override the model tier list. Each entry needs `name`, `filename`, `url`, and `min_ram_gb`. Restart required.
- `ai.mlx.auto_update` (default true): enable periodic `mlx-lm` upgrades in a dedicated virtualenv.
- `ai.mlx.check_interval_hours` (default 12): how often MLX upgrade checks run.
- `ai.mlx.python_path` (default empty): optional Python executable for creating the MLX virtualenv.
- `ai.mlx.venv_path` (default `~/.uhoh/venv/mlx`): dedicated MLX virtualenv path.
- `ai.mlx.max_version` (default unset): optional upper version pin, e.g. `0.25`.

### Notifications Settings

- `notifications.desktop` (default true): enable desktop notifications.
- `notifications.webhook_url` (default empty): webhook destination for high-signal alerts.
- `notifications.webhook_events` (default critical db/agent/mlx events): event names forwarded to webhook.
- `notifications.cooldown_seconds` (default 60): dedupe window per event type.

### Database Guard Settings

- `db_guard.enabled` (default false): enable database guardian subsystem.
- `db_guard.mass_delete_row_threshold` (default 100): row-count threshold for alerting.
- `db_guard.mass_delete_pct_threshold` (default 0.05): table percentage threshold for alerting.
- `db_guard.baseline_interval_hours` (default 6): baseline snapshot cadence.
- `db_guard.recovery_retention_days` (default 30): recovery artifact retention.
- `db_guard.max_baseline_size_mb` (default 500): table baseline cap.
- `db_guard.max_recovery_file_mb` (default 500): single recovery artifact cap.
- `db_guard.encrypt_recovery` (default true): encrypt recovery artifacts at rest.

Encrypted recovery artifacts now support decryption in `uhoh db recover --apply`. Key selection follows:
1. `UHOH_MASTER_KEY` set to a 64-char hex key: BLAKE3 KDF mode (domain-separated).
2. `UHOH_MASTER_KEY` set to passphrase: Argon2id key derivation.
3. If `UHOH_MASTER_KEY` is unset: machine-local key fallback in `~/.uhoh/master.key` (0600) is used for recovery artifacts.

Database guard is designed for emergency detection and recovery prep. It is not a full SQL audit stream.

### Agent Monitor Settings

- `agent.enabled` (default false): enable agent monitoring subsystem.
- `agent.mcp_proxy_enabled` (default true): enable MCP proxy tick processing.
- `agent.mcp_proxy_port` (default 22823): MCP proxy listen port.
- `agent.intercept_enabled` (default true): enable session log tailing fallback.
- `agent.audit_enabled` (default false): enable OS-level audit loop.
- `agent.audit_scope` (default `project`): audit scope (`project` or `home`).
- `agent.sandbox_enabled` (default false): enable sandbox integrations when available.
- `agent.on_dangerous_change` (default `none`): dangerous-action policy.
- `agent.pause_timeout_seconds` (default 300): auto-resume timeout.
- `agent.dangerous_patterns`: pattern set used for classification.

`agent` settings are intentionally layered: MCP proxy first, session-log fallback second, and OS-level audit as opt-in only.

### Update settings

- `update.auto_check` (default true): enable periodic update checks by the daemon. Restart required.
- `update.check_interval_hours` (default 24): hours between checks. **Hot-reloaded.**

### Server Settings

- `server.enabled` (default true): enable the unified localhost server. Restart required.
- `server.port` (default 22822): server port. Restart required.
- `server.bind_address` (default `127.0.0.1`): bind address. Keep loopback-only for security.
- `server.ui_enabled` (default true): serve Time Machine UI at `/`.
- `server.mcp_enabled` (default true): serve MCP HTTP endpoint at `/mcp`.
- `server.require_auth` (default true): require bearer auth for mutating requests.

### Sidecar Update Settings

- `sidecar_update.auto_update` (default true): enable periodic llama.cpp sidecar checks.
- `sidecar_update.check_interval_hours` (default 24): sidecar update check cadence.
- `sidecar_update.pin_version` (default unset): optional release tag pin (e.g. `b5200`).
- `sidecar_update.llama_repo` (default `ggml-org/llama.cpp`): GitHub release source.

## Deep dive: database guardian

Database guardian is built for high-signal mistakes: dropped objects and large destructive changes. It is not trying to be a full SQL audit platform.

### What it watches

For PostgreSQL, `uhoh db add postgres://...` installs `_uhoh_ddl_events` and `_uhoh_delete_counts` objects plus trigger plumbing so the daemon can detect dangerous operations quickly. In trigger mode, this is the default path.

For SQLite, the guard tracks `PRAGMA data_version`, records change events, and rotates baseline/recovery artifacts under `~/.uhoh/db_guard/<guard-name>/`.

For MySQL, current support is phase-1 schema polling. It checks table metadata and row estimates for abrupt changes and logs those into the unified ledger.

### Recovery model

On high-risk events, uhoh writes recovery artifacts (and baseline snapshots on cadence), hashes them with BLAKE3, and stores references in the event ledger. `uhoh db recover <event-id>` prints the artifact context and supports apply-mode safety checks.

### Practical workflow

1. Register a guard with `uhoh db add ...`.
2. Keep `db_guard.enabled = true` in config.
3. Check recent events with `uhoh db events` or `uhoh timeline --source db --since 1h`.
4. Use `uhoh db recover <event-id>` when you need to inspect or apply recovery SQL.

## Deep dive: agent monitoring (OpenClaw and others)

Agent monitoring is layered. MCP proxy interception is the primary path, session-log tailing is the fallback path, and OS-level audit is optional.

### OpenClaw example

OpenClaw is a good fit because it can be pointed at uhoh's MCP proxy and can also be profiled with a session log pattern in `~/.uhoh/agents/openclaw.toml`.

Typical setup:

1. `uhoh agent init`
2. Create or tune `~/.uhoh/agents/openclaw.toml`
3. `uhoh agent add openclaw --profile ~/.uhoh/agents/openclaw.toml`
4. Run through uhoh: `uhoh run -- openclaw start`

### Dangerous action flow

When `agent.on_dangerous_change = "pause"`, uhoh writes a pending approval marker and blocks the dangerous tool call path until `uhoh agent approve` arrives or timeout expires. This is the guardrail that keeps high-risk actions from silently slipping through.

### Event forensics and undo

Everything lands in the same event ledger:

- `uhoh agent log [name]`
- `uhoh blame <path>`
- `uhoh trace <event-id>`
- `uhoh timeline --source agent --since 30m`

For rollback workflows, `uhoh agent undo --cascade <event-id>` resolves the selected event and its causal descendants in one shot.

## Deep dive: storage methods

Every file in a snapshot records a `storage_method`:
- `reflink`: same bytes, no extra space until modified; best case
- `hardlink`: shares disk blocks; safe when the original isn't modified in place
- `copy`: a full copy; always available, but costs space
- `none`: hash only; content wasn't stored (too big for the relevant size limit, or an error)

Binary and text files have separate size caps: `storage.max_binary_blob_bytes` (1 MB by default) and `storage.max_text_blob_bytes` (50 MB). Binary detection uses the first 8 KB of the file. The effective limit for any given file is the minimum of its type-specific cap and `storage.max_copy_blob_bytes`.

You'll see the method in `uhoh snapshots`. `uhoh restore` only restores files with recoverable storage (`reflink`, `hardlink`, `copy`). Unstored files are listed with a warning.

When blob storage for a project exceeds its limit (computed from `storage_limit_fraction` × project size, floored at `storage_min_bytes`), uhoh prunes the oldest unpinned snapshots until it's back under the cap. Actual blob deletion happens during the next GC pass.

## Deep dive: updates (safely)

`uhoh update` fetches the latest release, verifies it, and atomically swaps the binary.
1. Primary check: Ed25519 signature over the BLAKE3 hash of the binary
2. Secondary: DNS TXT record `release-<asset>.<version>.releases.uhoh.it` with the expected hash
3. Apply: write to a temp file, set executable, use `self_replace` for atomic swap, then write a `.update-ready` trigger file

The daemon watches both its own binary and the trigger file. On change, it re-execs (Unix) or spawns a replacement with `--takeover <old-pid>` (Windows, which waits for the old process to exit before proceeding).

For CI/testing, set `UHOH_TEST_DNS_TXT` to a hash string to stub the DNS lookup.

## Deep dive: AI summaries

If enabled, uhoh builds a compact diff (up to `max_context_tokens` × 4 characters, truncated at a valid UTF-8 boundary) and asks a local sidecar for a one-to-two-sentence summary. Binary files and files over 512 KB are skipped in the diff.

### Backends

uhoh supports two inference backends:

- **llama.cpp** (`llama-server`): place the binary in `~/.uhoh/sidecar/llama-server`. PATH is intentionally not searched, for security.
- **MLX** (`mlx_lm`): preferred automatically on Apple Silicon macOS when the `mlx_lm` Python package is importable. uhoh checks by running `python3 -c "import mlx_lm"`.

The sidecar process is kept alive as a persistent global instance, bound to a random high port on 127.0.0.1, and shut down after the configured idle timeout. Startup retries up to 5 times with different ports, and waits up to 30 seconds for the health endpoint to respond.

### Model tiers

uhoh selects the largest model your available RAM can handle. Defaults (overridable via `ai.models` in config):

| Model | File | Min RAM |
|---|---|---|
| Qwen3.5-9B-Q4_K_M | qwen3.5-9b-q4_k_m.gguf | 8 GB |
| Qwen3.5-9B-Q8_0 | qwen3.5-9b-q8_0.gguf | 16 GB |
| Qwen3.5-35B-A3B-Q4_K_M | qwen3.5-35b-a3b-q4_k_m.gguf | 24 GB |
| Qwen3.5-35B-A3B-Q6_K | qwen3.5-35b-a3b-q6_k.gguf | 32 GB |
| Qwen3.5-35B-A3B-Q8_0 | qwen3.5-35b-a3b-q8_0.gguf | 48 GB |

Models are downloaded on first use to `~/.uhoh/models/` with HTTP range-request resume support and a progress bar. The 35B-A3B variants are mixture-of-experts models with GatedDeltaNet attention.

### Deferred queue

If conditions aren't met at snapshot time (on battery, low RAM, sidecar not started), the snapshot's rowid is queued in a `pending_ai_summaries` table. The daemon processes up to 2 queued jobs per tick when conditions improve. Each job gets up to 5 attempts. Queue entries older than 7 days are pruned.

## Git integration

### Pre-commit hook

`uhoh hook install` adds a block to `.git/hooks/pre-commit` that runs `uhoh commit --trigger pre-commit "Pre-commit snapshot"` before each commit. If a pre-commit hook already exists, uhoh appends a clearly marked block (`# BEGIN uhoh pre-commit hook` / `# END uhoh pre-commit hook`) rather than overwriting. `uhoh hook remove` strips just the uhoh block, leaving other hooks intact. If nothing else remains, the hook file is deleted.

The hook tries `uhoh` on PATH first. If not found (common in GUI git clients with stripped environments), it falls back to `~/.uhoh/bin/uhoh`.

### Snapshot to git stash

`uhoh gitstash <id>` constructs a proper two-parent git stash entry from a snapshot without touching your working tree or index. It uses git plumbing commands (`hash-object -w`, `update-index --index-info`, `write-tree`, `commit-tree`, `stash store`) and a temporary index file (`.git/index.uhoh-tmp`, cleaned up afterward). Executable bits (mode 100755) and symlink modes (mode 120000) are preserved. Files that weren't stored in the snapshot are omitted with a warning.

### Worktrees

uhoh detects git worktrees where `.git` is a file containing `gitdir: <path>` rather than a directory. The marker file is placed inside the resolved git directory, so worktrees of the same repo get independent uhoh identities.

## System service

You can set uhoh to start automatically on login:

```bash
uhoh service-install    # set up auto-start
uhoh service-remove     # remove it
```

On macOS this creates a launchd agent (`~/Library/LaunchAgents/com.uhoh.daemon.plist`) with `KeepAlive` on failure. On Linux it creates a systemd user unit (`~/.config/systemd/user/uhoh.service`) with `Restart=on-failure`. On Windows it creates a scheduled task (`uhoh-daemon`) that runs at logon. All three run `uhoh start --service` and log to `~/.uhoh/daemon.log`.

## CLI reference

- `uhoh` — no-subcommand shortcut: if unregistered, registers and creates initial snapshot; if registered, shows status and tips
- `uhoh + [path]` — alias: `uhoh add [path]`
- `uhoh - [path-or-hash]` — alias: `uhoh remove [path-or-hash]`
- `uhoh l` — alias: `uhoh list`
- `uhoh s [target]` — alias: `uhoh snapshots [target]`
- `uhoh c [message]` — alias: `uhoh commit [message]`
- `uhoh d [id1] [id2]` — alias: `uhoh diff [id1] [id2]`
- `uhoh p <file> <id>` — alias: `uhoh cat <file> <id>` (id can be base58, RFC 3339 timestamp, or `YYYY-MM-DDTHH:MM:SS`)
- `uhoh o <file>` — alias: `uhoh log <file>`
- `uhoh r <id> [--dry-run] [--force]` — alias: `uhoh restore <id>`
- `uhoh gitstash <id>` — restore snapshot into a git stash entry
- `uhoh mark <label>` / `uhoh undo` / `uhoh operations`
- `uhoh hook install` / `uhoh hook remove`
- `uhoh config` — print full config
- `uhoh config edit` — open in `$EDITOR`
- `uhoh config set <key> <value>` — supports dotted keys up to two levels (e.g. `ai.enabled true`)
- `uhoh config get <key>`
- `uhoh doctor [--fix] [--restore-latest] [--verify-install]`
- `uhoh gc` — manual garbage collection of orphaned blobs
- `uhoh update` — check for and apply updates
- `uhoh status` — show daemon state, project count, snapshots, blob storage, AI status
- `uhoh start [--service]` / `uhoh stop` / `uhoh restart`
- `uhoh service-install` / `uhoh service-remove`
- `uhoh db add <dsn> [--tables ...] [--name ...] [--mode triggers|replication]`
- `uhoh db remove <name>` / `uhoh db list`
- `uhoh db events [name] [--table ...]`
- `uhoh db recover <event-id> [--apply]`
- `uhoh db baseline <name>` / `uhoh db test <name>`
- `uhoh agent add <name> [--profile <path>]`
- `uhoh agent remove <name>` / `uhoh agent list`
- `uhoh agent log [name] [--session <id>]`
- `uhoh agent undo [event-id] [--cascade <event-id>] [--session <id>]`
- `uhoh agent approve` / `uhoh agent resume` / `uhoh agent setup`
- `uhoh agent test <name>` / `uhoh agent init` / `uhoh agent update-profiles`
- `uhoh trace <event-id>` / `uhoh blame <path>`
- `uhoh timeline [--source fs|db|agent] [--since 30m|1h|2d]`
- `uhoh run -- <command ...>`

## Tips

- Snapshot IDs are base58. ID 0 is reserved and rejected; valid IDs start at 1.
- `uhoh cat` accepts RFC 3339 timestamps (`2025-01-15T10:30:00+00:00`), bare datetimes (`2025-01-15T10:30:00`), and base58 snapshot IDs. Timestamps find the most recent snapshot at or before that time.
- If the watcher dies repeatedly (system limits, network shares), uhoh backs off with exponential delay (1s, 2s, 4s, ... up to 60s) and retries. On Linux, low inotify limits are a common cause.
- Large repos: compaction and GC keep storage under control. You can tune the storage limit per project via `storage.storage_limit_fraction`.
- Non-UTF8 filenames are supported. They're stored with a `b64:` prefix (base64-encoded raw bytes) in the database and decoded back to platform-native paths on restore.
- The daemon hot-reloads `watch.debounce_quiet_secs` and `update.check_interval_hours` without a restart. Other config changes need `uhoh restart`.
- Compaction runs one project per daemon tick to reduce contention. Pinned snapshots are always kept. Manual commits with messages get at least daily-bucket retention even when they'd otherwise be pruned at the 5-minute level.
- `uhoh status` reports daemon state, project count, total snapshots, blob storage size, and AI status. It also warns about inception loops if a project directory contains `~/.uhoh`.
- When blob storage for a project exceeds its limit, the oldest unpinned snapshots are automatically deleted. Run `uhoh gc` afterward to reclaim the disk space immediately, or wait for the daemon's periodic GC.

## Why SQLite and a blob store

We want atomic snapshots, fast lookups, and safe recovery. SQLite gives us transactional inserts and an easy way to answer "what changed" without parsing files on disk. Blobs live in the filesystem so we can use reflink/hardlink and avoid copying bytes when we don't have to.

The database runs in WAL mode for concurrent readers, uses a 5-second busy timeout, and has foreign keys enabled with cascading deletes (removing a project cleans up all its snapshots and file entries). The daemon periodically backs up the database and can VACUUM after large compaction runs to reclaim free pages.

## Contributing

Issues and PRs are welcome. If you're changing snapshot logic, include a test and run `uhoh doctor` locally to sanity‑check the blob store.

## Troubleshooting

- **The watcher keeps dying, then recovering**
  uhoh backs off with exponential delay and retries automatically. On Linux, low inotify limits are a common cause. Check `/proc/sys/fs/inotify/max_user_watches` and raise it, e.g. `sudo sysctl fs.inotify.max_user_watches=524288`.

- **`uhoh snapshots` shows many `none` storage methods**
  Files were too large to copy given the active size limits. Binary files have a separate, lower cap (`storage.max_binary_blob_bytes`, default 1 MB) than text files (`storage.max_text_blob_bytes`, default 50 MB). Raise the relevant limit and re-snapshot.

- **`uhoh restore` complains about missing blobs**
  Run `uhoh doctor` to list missing and orphaned blobs. If blobs are corrupted, doctor can quarantine them with `--fix`. If the DB looks damaged, try `uhoh doctor --restore-latest` to restore from the most recent backup.

- **Updates fail with "public key not set"**
  Release builds require a non‑zero Ed25519 update key baked into the binary at compile time. For local development builds, skip updates. For production, set the real key in `src/update.rs` before publishing.

- **AI summaries don't appear**
  AI is off by default. Set `ai.enabled = true` in `~/.uhoh/config.toml`. Summaries are skipped on battery or when available memory is below `ai.min_available_memory_gb`. Large and binary files are intentionally excluded from the diff context. If conditions aren't met at snapshot time, summaries are queued and retried later (up to 5 attempts over 7 days). Check `~/.uhoh/sidecar.log` for backend errors.

- **"Not a registered uhoh project"**
  Run `uhoh +` in the project root. uhoh stores the canonical path in the DB, so make sure you're not in a symlinked directory when running commands.

- **Snapshot ID is ambiguous or invalid**
  IDs are base58 and must be >= 1. Use a longer prefix if the short prefix matches multiple snapshots.

- **Pre-commit hook doesn't fire**
  Make sure the hook file is executable (`chmod +x .git/hooks/pre-commit`). `uhoh hook install` sets this on Unix. If using a GUI git client, the hook falls back to `~/.uhoh/bin/uhoh`; make sure that path exists.

- **Symlinks not restored on Windows**
  Creating symlinks on Windows requires Developer Mode or elevated privileges. When neither is available, uhoh writes the symlink target path as a regular file and logs a warning.

- **`uhoh` shows "uhoh is active in this directory" but I expected it to restore**
  The zero-argument shortcut only shows status for already-registered projects. Use `uhoh restore <id>` or `uhoh undo` to actually change files.
