use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "uhoh",
    version,
    about = "Local filesystem snapshots — ctrl-z for AI agents",
    long_about = "uhoh monitors registered directories, creates content-addressable snapshots \
                  on file changes, and supports time-travel recovery.\n\n\
                  Short aliases: + (add), - (remove), l (list), s (snapshots), \
                  r (restore), g (gitstash), c (commit), d (diff), p (cat), o (log)"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    /// Internal: PID of previous daemon to wait for during update takeover (Windows)
    #[arg(long, hide = true, global = true)]
    pub takeover: Option<u32>,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Register a folder for watching (default: current directory)
    #[command(alias = "+")]
    Add {
        path: Option<String>,
    },

    /// Unregister a folder
    #[command(alias = "-")]
    Remove {
        target: Option<String>,
    },

    /// List all registered folders with status
    #[command(alias = "l")]
    List,

    /// List snapshots for a project
    #[command(alias = "s")]
    Snapshots {
        target: Option<String>,
    },

    /// Restore working directory to a snapshot
    #[command(alias = "r")]
    Restore {
        /// Snapshot ID (base58)
        id: String,
        /// Project path or hash prefix
        target: Option<String>,
        /// Show what would change without modifying files
        #[arg(long)]
        dry_run: bool,
        /// Skip confirmation prompt
        #[arg(long, short)]
        force: bool,
    },

    /// Restore a snapshot into a git stash entry
    #[command(alias = "g")]
    Gitstash {
        id: String,
        target: Option<String>,
    },

    /// Create a manual snapshot with optional message
    #[command(alias = "c")]
    Commit {
        message: Option<String>,
        /// Trigger type (auto, manual, pre-commit, etc.)
        #[arg(long, hide = true)]
        trigger: Option<String>,
    },

    /// Diff snapshot(s) vs current or each other
    #[command(alias = "d")]
    Diff {
        id1: Option<String>,
        id2: Option<String>,
    },

    /// Print a file at a point in time
    #[command(alias = "p")]
    Cat {
        path: String,
        id: String,
    },

    /// History of a specific file across snapshots
    #[command(alias = "o")]
    Log {
        path: String,
    },

    /// Start the snapshot daemon
    Start {
        /// Run in foreground (for service managers)
        #[arg(long)]
        service: bool,
    },

    /// Stop the snapshot daemon
    Stop,

    /// Restart the snapshot daemon
    Restart,

    /// Install or remove git pre-commit hook
    Hook {
        /// "install" or "remove"
        action: String,
    },

    /// View or edit configuration
    Config {
        /// Optional: "edit" to open in $EDITOR
        subcommand: Option<String>,
    },

    /// Manual garbage collection of orphaned blobs
    Gc,

    /// Check for and apply updates
    Update,

    /// Show daemon and project status
    Status,

    /// Validate database and blob store; optionally repair or restore
    Doctor {
        /// Attempt to fix issues (delete orphaned blobs, etc.)
        #[arg(long)]
        fix: bool,
        /// Restore the DB from latest backup if integrity check fails
        #[arg(long)]
        restore_latest: bool,
        /// Verify installed binary hash against DNS records and exit
        #[arg(long)]
        verify_install: bool,
    },

    /// Mark the start of an AI-agent operation for grouped undo
    Mark {
        label: String,
    },

    /// Revert the last completed operation
    Undo {
        target: Option<String>,
    },

    /// List recorded operations
    Operations {
        target: Option<String>,
    },

    /// Install system service for auto-start
    #[command(name = "service-install", hide = true)]
    ServiceInstall,

    /// Remove system service
    #[command(name = "service-remove", hide = true)]
    ServiceRemove,
}
