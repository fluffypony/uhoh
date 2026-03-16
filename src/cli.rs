use clap::{Parser, Subcommand, ValueEnum};

use crate::db::DbGuardMode;

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
#[non_exhaustive]
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
        action: HookAction,
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
        /// Filter by event source (`fs`, `db_guard`, `agent`, `daemon`, `mlx`)
        #[arg(long)]
        source: Option<crate::db::LedgerSource>,
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

#[derive(Clone, Copy, ValueEnum)]
pub enum HookAction {
    Install,
    Remove,
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
        #[arg(long, value_enum, default_value_t = DbGuardMode::Triggers)]
        mode: DbGuardMode,
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

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn parse(args: &[&str]) -> Cli {
        Cli::try_parse_from(args).unwrap()
    }

    #[test]
    fn parse_add_with_path() {
        let cli = parse(&["uhoh", "add", "/some/path"]);
        match cli.command {
            Commands::Add { path } => assert_eq!(path.as_deref(), Some("/some/path")),
            _ => panic!("expected Add"),
        }
    }

    #[test]
    fn parse_add_alias() {
        let cli = parse(&["uhoh", "+", "/some/path"]);
        match cli.command {
            Commands::Add { path } => assert_eq!(path.as_deref(), Some("/some/path")),
            _ => panic!("expected Add via + alias"),
        }
    }

    #[test]
    fn parse_add_no_path() {
        let cli = parse(&["uhoh", "add"]);
        match cli.command {
            Commands::Add { path } => assert!(path.is_none()),
            _ => panic!("expected Add"),
        }
    }

    #[test]
    fn parse_remove_alias() {
        let cli = parse(&["uhoh", "-", "proj"]);
        match cli.command {
            Commands::Remove { target } => assert_eq!(target.as_deref(), Some("proj")),
            _ => panic!("expected Remove"),
        }
    }

    #[test]
    fn parse_list_and_alias() {
        let cli = parse(&["uhoh", "list"]);
        assert!(matches!(cli.command, Commands::List));

        let cli2 = parse(&["uhoh", "l"]);
        assert!(matches!(cli2.command, Commands::List));
    }

    #[test]
    fn parse_snapshots_alias() {
        let cli = parse(&["uhoh", "s"]);
        match cli.command {
            Commands::Snapshots { target } => assert!(target.is_none()),
            _ => panic!("expected Snapshots"),
        }
    }

    #[test]
    fn parse_restore_with_flags() {
        let cli = parse(&["uhoh", "restore", "abc123", "--dry-run", "--force"]);
        match cli.command {
            Commands::Restore { id, target, dry_run, force } => {
                assert_eq!(id, "abc123");
                assert!(target.is_none());
                assert!(dry_run);
                assert!(force);
            }
            _ => panic!("expected Restore"),
        }
    }

    #[test]
    fn parse_restore_alias() {
        let cli = parse(&["uhoh", "r", "xyz"]);
        match cli.command {
            Commands::Restore { id, dry_run, force, .. } => {
                assert_eq!(id, "xyz");
                assert!(!dry_run);
                assert!(!force);
            }
            _ => panic!("expected Restore via alias"),
        }
    }

    #[test]
    fn parse_commit_with_message() {
        let cli = parse(&["uhoh", "commit", "my message"]);
        match cli.command {
            Commands::Commit { message, trigger } => {
                assert_eq!(message.as_deref(), Some("my message"));
                assert!(trigger.is_none());
            }
            _ => panic!("expected Commit"),
        }
    }

    #[test]
    fn parse_commit_alias() {
        let cli = parse(&["uhoh", "c"]);
        match cli.command {
            Commands::Commit { message, .. } => assert!(message.is_none()),
            _ => panic!("expected Commit via alias"),
        }
    }

    #[test]
    fn parse_diff() {
        let cli = parse(&["uhoh", "diff", "id1", "id2"]);
        match cli.command {
            Commands::Diff { id1, id2 } => {
                assert_eq!(id1.as_deref(), Some("id1"));
                assert_eq!(id2.as_deref(), Some("id2"));
            }
            _ => panic!("expected Diff"),
        }
    }

    #[test]
    fn parse_diff_alias() {
        let cli = parse(&["uhoh", "d"]);
        match cli.command {
            Commands::Diff { id1, id2 } => {
                assert!(id1.is_none());
                assert!(id2.is_none());
            }
            _ => panic!("expected Diff via alias"),
        }
    }

    #[test]
    fn parse_cat() {
        let cli = parse(&["uhoh", "cat", "src/main.rs", "snap1"]);
        match cli.command {
            Commands::Cat { path, id } => {
                assert_eq!(path, "src/main.rs");
                assert_eq!(id, "snap1");
            }
            _ => panic!("expected Cat"),
        }
    }

    #[test]
    fn parse_cat_alias() {
        let cli = parse(&["uhoh", "p", "file.txt", "abc"]);
        match cli.command {
            Commands::Cat { path, id } => {
                assert_eq!(path, "file.txt");
                assert_eq!(id, "abc");
            }
            _ => panic!("expected Cat via alias"),
        }
    }

    #[test]
    fn parse_log() {
        let cli = parse(&["uhoh", "log", "src/lib.rs"]);
        match cli.command {
            Commands::Log { path } => assert_eq!(path, "src/lib.rs"),
            _ => panic!("expected Log"),
        }
    }

    #[test]
    fn parse_log_alias() {
        let cli = parse(&["uhoh", "o", "file.rs"]);
        match cli.command {
            Commands::Log { path } => assert_eq!(path, "file.rs"),
            _ => panic!("expected Log via alias"),
        }
    }

    #[test]
    fn parse_doctor_flags() {
        let cli = parse(&["uhoh", "doctor", "--fix", "--restore-latest", "--verify-install"]);
        match cli.command {
            Commands::Doctor { fix, restore_latest, verify_install } => {
                assert!(fix);
                assert!(restore_latest);
                assert!(verify_install);
            }
            _ => panic!("expected Doctor"),
        }
    }

    #[test]
    fn parse_doctor_defaults() {
        let cli = parse(&["uhoh", "doctor"]);
        match cli.command {
            Commands::Doctor { fix, restore_latest, verify_install } => {
                assert!(!fix);
                assert!(!restore_latest);
                assert!(!verify_install);
            }
            _ => panic!("expected Doctor"),
        }
    }

    #[test]
    fn parse_gitstash_alias() {
        let cli = parse(&["uhoh", "g", "snapid"]);
        match cli.command {
            Commands::Gitstash { id, target } => {
                assert_eq!(id, "snapid");
                assert!(target.is_none());
            }
            _ => panic!("expected Gitstash via alias"),
        }
    }

    #[test]
    fn parse_hook_install() {
        let cli = parse(&["uhoh", "hook", "install"]);
        match cli.command {
            Commands::Hook { action } => assert!(matches!(action, HookAction::Install)),
            _ => panic!("expected Hook"),
        }
    }

    #[test]
    fn parse_hook_remove() {
        let cli = parse(&["uhoh", "hook", "remove"]);
        match cli.command {
            Commands::Hook { action } => assert!(matches!(action, HookAction::Remove)),
            _ => panic!("expected Hook"),
        }
    }

    #[test]
    fn parse_config_set() {
        let cli = parse(&["uhoh", "config", "set", "key", "value"]);
        match cli.command {
            Commands::Config { action: Some(ConfigAction::Set { key, value }) } => {
                assert_eq!(key, "key");
                assert_eq!(value, "value");
            }
            _ => panic!("expected Config Set"),
        }
    }

    #[test]
    fn parse_config_get() {
        let cli = parse(&["uhoh", "config", "get", "mykey"]);
        match cli.command {
            Commands::Config { action: Some(ConfigAction::Get { key }) } => {
                assert_eq!(key, "mykey");
            }
            _ => panic!("expected Config Get"),
        }
    }

    #[test]
    fn parse_db_add() {
        let cli = parse(&["uhoh", "db", "add", "sqlite:///tmp/test.db"]);
        match cli.command {
            Commands::Db { action: DbAction::Add { dsn, tables, name, mode } } => {
                assert_eq!(dsn, "sqlite:///tmp/test.db");
                assert!(tables.is_none());
                assert!(name.is_none());
                assert!(matches!(mode, DbGuardMode::Triggers)); // default
            }
            _ => panic!("expected Db Add"),
        }
    }

    #[test]
    fn parse_agent_add() {
        let cli = parse(&["uhoh", "agent", "add", "claude"]);
        match cli.command {
            Commands::Agent { action: AgentAction::Add { name, profile } } => {
                assert_eq!(name, "claude");
                assert!(profile.is_none());
            }
            _ => panic!("expected Agent Add"),
        }
    }

    #[test]
    fn parse_trace() {
        let cli = parse(&["uhoh", "trace", "42"]);
        match cli.command {
            Commands::Trace { event_id } => assert_eq!(event_id, 42),
            _ => panic!("expected Trace"),
        }
    }

    #[test]
    fn parse_timeline_with_source() {
        let cli = parse(&["uhoh", "timeline", "--source", "agent"]);
        match cli.command {
            Commands::Timeline { source, since } => {
                assert_eq!(source, Some(crate::db::LedgerSource::Agent));
                assert!(since.is_none());
            }
            _ => panic!("expected Timeline"),
        }
    }

    #[test]
    fn parse_timeline_with_since() {
        let cli = parse(&["uhoh", "timeline", "--since", "30m"]);
        match cli.command {
            Commands::Timeline { source, since } => {
                assert!(source.is_none());
                assert_eq!(since.as_deref(), Some("30m"));
            }
            _ => panic!("expected Timeline"),
        }
    }

    #[test]
    fn parse_run_with_trailing_args() {
        let cli = parse(&["uhoh", "run", "echo", "hello", "world"]);
        match cli.command {
            Commands::Run { command } => {
                assert_eq!(command, vec!["echo", "hello", "world"]);
            }
            _ => panic!("expected Run"),
        }
    }

    #[test]
    fn parse_takeover_global_option() {
        let cli = parse(&["uhoh", "--takeover", "12345", "status"]);
        assert_eq!(cli.takeover, Some(12345));
        assert!(matches!(cli.command, Commands::Status));
    }

    #[test]
    fn parse_simple_commands() {
        // These commands take no arguments
        assert!(matches!(parse(&["uhoh", "mcp"]).command, Commands::Mcp));
        assert!(matches!(parse(&["uhoh", "stop"]).command, Commands::Stop));
        assert!(matches!(parse(&["uhoh", "restart"]).command, Commands::Restart));
        assert!(matches!(parse(&["uhoh", "gc"]).command, Commands::Gc));
        assert!(matches!(parse(&["uhoh", "update"]).command, Commands::Update));
        assert!(matches!(parse(&["uhoh", "status"]).command, Commands::Status));
    }

    #[test]
    fn parse_start_service_flag() {
        let cli = parse(&["uhoh", "start", "--service"]);
        match cli.command {
            Commands::Start { service } => assert!(service),
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn parse_start_no_service_flag() {
        let cli = parse(&["uhoh", "start"]);
        match cli.command {
            Commands::Start { service } => assert!(!service),
            _ => panic!("expected Start"),
        }
    }

    #[test]
    fn parse_ledger_verify() {
        let cli = parse(&["uhoh", "ledger", "verify"]);
        match cli.command {
            Commands::Ledger { action: LedgerAction::Verify } => {}
            _ => panic!("expected Ledger Verify"),
        }
    }

    #[test]
    fn invalid_command_fails() {
        let result = Cli::try_parse_from(["uhoh", "nonexistent"]);
        assert!(result.is_err());
    }

    #[test]
    fn missing_required_args_fails() {
        // cat requires path and id
        let result = Cli::try_parse_from(["uhoh", "cat"]);
        assert!(result.is_err());
    }
}
