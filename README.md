# uhoh

*When your coding agent develops a mind of its own, just **uhoh** it*

Local snapshots for messy work. When your AI agent gets overconfident, `uhoh` gives you a clean way back.

uhoh watches your project folders, takes small content‑addressable snapshots as files change, and lets you time‑travel without leaving your editor. It runs locally on macOS, Linux, and Windows. No cloud, no telemetry.

Highlights
- Fast, local snapshots with BLAKE3 and a deduplicated blob store
- SQLite metadata with transactional snapshot creation and schema migrations
- Tiered storage (reflink → hardlink → copy → none) to keep space under control
- Restore individual files or whole trees; see diffs and file history
- Optional AI summaries via a local sidecar (Qwen tiers); skips on battery/low‑RAM
- Safe auto‑updates: Ed25519 signatures, DNS TXT fallback, atomically applied
- Guardrails: emergency‑delete detection, GC, compaction, and a `doctor` command

## Install

Prebuilt binaries are available on the releases page. Place `uhoh` in your `PATH`.

## Quick start

```bash
# Register the current directory
uhoh add       # alias: uhoh +

# The daemon starts automatically; you can control it:
uhoh start     # run in background
uhoh stop

# List projects and snapshots
uhoh list      # alias: uhoh l
uhoh snapshots # alias: uhoh s

# Create and restore
uhoh commit "before refactor"   # alias: uhoh c
uhoh restore <snapshot-id>       # alias: uhoh r <id>

# Diff and time travel
uhoh diff                    # latest vs working tree
uhoh diff <id1> <id2>        # snapshot vs snapshot
uhoh cat src/main.rs <id>    # print a file at a snapshot (alias: uhoh p)
uhoh log src/main.rs         # history for a file (alias: uhoh o)

# Grouped undo for agent runs
uhoh mark "implement search"
uhoh operations
uhoh undo                    # restores to just-before the marked operation
```

### Shortcut: just type `uhoh`

In a project folder:
- If the folder is not registered yet, `uhoh` will register it and take an initial snapshot.
- If it is already registered, `uhoh` will take a quick snapshot and immediately revert your working tree to the previous snapshot. It’s a one‑keystroke safety net when an agent gets ahead of you.

## How it works

uhoh keeps two things in `~/.uhoh`:
1. A blob store `~/.uhoh/blobs/` where file contents live by BLAKE3 hash.
2. A SQLite database `~/.uhoh/uhoh.db` with projects, snapshots, and file lists.

When a file changes, uhoh computes its BLAKE3 hash and tries to store it using a tiered strategy:
1. Reflink (copy‑on‑write clone) if the filesystem supports it
2. Hardlink as a fallback
3. Full copy if under the configured size limit
4. Otherwise record the hash only (not recoverable)

Snapshots are created transactionally. Each contains the file list with size, hash, storage method, mtime, and executable bit. Old snapshots are compacted using time buckets (5‑minute, hourly, daily, weekly), with pinned and message‑bearing snapshots preferentially kept. Garbage collection prunes unreferenced blobs.

The daemon uses a notify bridge thread, with a retry/backoff if the watcher dies. It batches changes with a debounce window and enforces a minimum interval between snapshots per project.

## Commands you’ll use most

1. `uhoh add [path]` registers a project and creates the first snapshot. A small marker file is written to the project so folder moves can be detected.
2. `uhoh snapshots` shows the timeline. For each file, you’ll see its size and how it was stored: `reflink`, `hardlink`, `copy`, or `none`.
3. `uhoh diff` shows changes between snapshots (or snapshot vs working tree). Output is unified diff with basic syntax highlighting.
4. `uhoh restore <id>` resets your working tree to a snapshot. Before any destructive changes, uhoh takes a pre‑restore snapshot. On Unix, it preserves executable bits.
5. `uhoh mark / uhoh undo` gives you grouped undo for larger agent runs.

## Safety nets

- Emergency delete detection: if a large fraction of files disappear (e.g., branch switch, bad script), uhoh creates an emergency snapshot so you can recover.
- Read‑only blobs: reflink/hardlink targets are set read‑only to reduce accidental mutation.
- Integrity checks: reading a blob rehashes the bytes; mismatch returns no data and logs a clear error.
- Doctor: `uhoh doctor` runs a database integrity check, compares referenced hashes against disk, finds orphans, and can verify every blob’s hash on disk. With `--fix`, it removes orphans and quarantines corrupted blobs to `~/.uhoh/quarantine`.
- Periodic backups: the daemon keeps timestamped backups of `uhoh.db` in `~/.uhoh/backups` and rotates to the most recent 14. `uhoh doctor --restore-latest` can restore the latest one.

## Configuration

Edit `~/.uhoh/config.toml` or run `uhoh config`.

Notable settings
- `watch.debounce_quiet_secs` and `watch.max_debounce_secs`: tune snapshot batching
- `storage.max_copy_blob_bytes`: skip full copies for very large files
- `storage.storage_limit_fraction` and `storage.storage_min_bytes`: per‑project limits
- `ai.enabled`, `ai.max_context_tokens`, `ai.min_available_memory_gb`, `ai.skip_on_battery`: keep local AI friendly to your machine

## Deep dive: storage methods

Every file in a snapshot records a `storage_method`:
- `reflink`: same bytes, no extra space until modified; best case
- `hardlink`: shares disk blocks; safe when the original isn’t modified in place
- `copy`: a full copy; always available, but costs space
- `none`: hash only; content wasn’t stored (too big for copy limit, or an error)

You’ll see the method in `uhoh snapshots`. `uhoh restore` only restores files with recoverable storage (`reflink`, `hardlink`, `copy`). If you want to ensure large binaries are always recoverable, raise `storage.max_copy_blob_bytes` and re‑snapshot.

## Deep dive: updates (safely)

`uhoh update` fetches the latest release, verifies it, and atomically swaps the binary.
1. Primary check: Ed25519 signature over the BLAKE3 of the binary
2. Secondary: DNS TXT record `release-<asset>.<version>.releases.uhoh.it` with the expected hash
3. Apply: write to a temp file and use `self_replace` to swap in place, then signal the daemon to restart

For testing, you can set `UHOH_TEST_DNS_TXT` to stub the expected hash in CI.

## Deep dive: AI summaries

If enabled, uhoh builds a compact diff and asks a local sidecar (llama.cpp or mlx‑lm) for a one‑liner. It skips when on battery or memory is tight. Context size is configurable. Large or binary files are skipped automatically.

## CLI reference

- `uhoh add [path]`
- `uhoh remove [path-or-hash]` (alias: `uhoh -`)
- `uhoh list` (alias: `uhoh l`)
- `uhoh snapshots` (alias: `uhoh s`)
- `uhoh commit [message]` (alias: `uhoh c`)
- `uhoh diff [id1] [id2]` (alias: `uhoh d`)
- `uhoh cat <file> <id>` (alias: `uhoh p`)
- `uhoh log <file>` (alias: `uhoh o`)
- `uhoh gitstash <id>` (restore snapshot into a git stash)
- `uhoh mark <label>` / `uhoh undo` / `uhoh operations`
- `uhoh config [edit|set]`
- `uhoh doctor [--fix] [--restore-latest]`
- `uhoh gc`, `uhoh update`, `uhoh status`, `uhoh start`, `uhoh stop`, `uhoh restart`

## Tips

- Snapshot IDs are base58. ID 0 is reserved and rejected; valid IDs start at 1.
- If the watcher dies repeatedly (system limits, network shares), uhoh backs off and retries with exponential delay. You’ll see this in the log.
- Large repos: compaction and GC keep storage under control. You can tune the storage limit per project via `storage.storage_limit_fraction`.

## Why SQLite and a blob store

We want atomic snapshots, fast lookups, and safe recovery. SQLite gives us transactional inserts and an easy way to answer “what changed” without parsing files on disk. Blobs live in the filesystem so we can use reflink/hardlink and avoid copying bytes when we don’t have to. 

## Contributing

Issues and PRs are welcome. If you’re changing snapshot logic, include a test and run `uhoh doctor` locally to sanity‑check the blob store.
