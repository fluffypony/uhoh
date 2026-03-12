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
    #[command(alias = "+")]
    Add {
        path: Option<String>,
    },

    #[command(alias = "-")]
    Remove {
        target: Option<String>,
    },

    #[command(alias = "l")]
    List,

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

    Mcp,

    Start {
        #[arg(long)]
        service: bool,
    },

    Stop,

    Restart,

    /// Install or remove git pre-commit hook
    Hook {
        /// "install" or "remove"
        action: String,
    },

    Config {
        #[command(subcommand)]
        action: Option<ConfigAction>,
    },

    Gc,

    Update,

    Status,

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

    Mark {
        label: String,
    },

    Undo {
        target: Option<String>,
    },

    Operations {
        target: Option<String>,
    },

    #[command(name = "service-install", hide = true)]
    ServiceInstall,

    #[command(name = "service-remove", hide = true)]
    ServiceRemove,

    Db {
        #[command(subcommand)]
        action: DbAction,
    },

    Agent {
        #[command(subcommand)]
        action: AgentAction,
    },

    Trace {
        event_id: i64,
    },

    Blame {
        path: String,
    },

    Timeline {
        /// Filter by event source (`fs`, `db`, `agent`)
        #[arg(long)]
        source: Option<String>,
        /// Relative lookback window (examples: `30m`, `1h`, `2d`)
        #[arg(long)]
        since: Option<String>,
    },

    Ledger {
        #[command(subcommand)]
        action: LedgerAction,
    },

    Run {
        #[arg(trailing_var_arg = true)]
        command: Vec<String>,
    },
}

#[derive(Subcommand)]
pub enum LedgerAction {
    Verify,
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Open the config file in $EDITOR
    Edit,
    /// Set a key to a value (supports up to two-level nesting: section.key)
    Set { key: String, value: String },
    /// Get the current value for a key
    Get { key: String },
}

#[derive(Subcommand)]
pub enum DbAction {
    Add {
        dsn: String,
        #[arg(long)]
        tables: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long, default_value = "triggers")]
        mode: String,
    },
    Remove {
        name: String,
    },
    List,
    Events {
        name: Option<String>,
        #[arg(long)]
        table: Option<String>,
    },
    Recover {
        event_id: i64,
        #[arg(long)]
        apply: bool,
    },
    Baseline {
        name: String,
    },
    Test {
        name: String,
    },
}

#[derive(Subcommand)]
pub enum AgentAction {
    Add {
        name: String,
        #[arg(long)]
        profile: Option<String>,
    },
    Remove {
        name: String,
    },
    List,
    Log {
        name: Option<String>,
        #[arg(long)]
        session: Option<String>,
    },
    Undo {
        event_id: Option<i64>,
        #[arg(long)]
        session: Option<String>,
        #[arg(long)]
        cascade: Option<i64>,
    },
    Approve,
    /// Deny (reject) pending dangerous agent actions
    Deny,
    Resume,
    Setup,
    Test {
        name: String,
    },
    Init,
}
