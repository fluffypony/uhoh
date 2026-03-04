#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::collections::VecDeque;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::ffi::CString;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::io::Read;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::fd::AsRawFd;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::unix::ffi::OsStrExt;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::fd::RawFd;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::path::{Path, PathBuf};

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use anyhow::{Context, Result};

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::db::AgentEntry;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::event_ledger::EventLedger;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::event_ledger::new_event;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::subsystem::SubsystemContext;

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
pub fn run_permission_monitor(ctx: &SubsystemContext, _agents: &[AgentEntry]) -> Result<()> {
    let monitor_root = std::env::current_dir().context("Unable to resolve current directory")?;
    let fan_fd = unsafe {
        libc::fanotify_init(
            (libc::FAN_CLASS_CONTENT
                | libc::FAN_CLOEXEC
                | libc::FAN_REPORT_FID
                | libc::FAN_REPORT_DFID_NAME) as u32,
            libc::O_RDONLY | libc::O_LARGEFILE,
        )
    };
    if fan_fd < 0 {
        anyhow::bail!("fanotify_init failed (CAP_SYS_ADMIN likely required)");
    }

    let cpath = CString::new(monitor_root.as_os_str().as_bytes().to_vec())
        .context("Invalid monitor root path")?;
    let mark_fd = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC)
        .open(&monitor_root)
        .with_context(|| format!("Failed opening monitor root: {}", monitor_root.display()))?;
    let mark_ok = unsafe {
        libc::fanotify_mark(
            fan_fd,
            (libc::FAN_MARK_ADD | libc::FAN_MARK_MOUNT | libc::FAN_MARK_FILESYSTEM) as u32,
            (libc::FAN_OPEN_PERM | libc::FAN_EVENT_ON_CHILD) as u64,
            mark_fd.as_raw_fd(),
            cpath.as_ptr(),
        )
    };
    if mark_ok < 0 {
        unsafe {
            libc::close(fan_fd);
        }
        anyhow::bail!("fanotify_mark failed for {}", monitor_root.display());
    }

    let mut event = new_event("agent", "fanotify_monitor_started", "info");
    event.detail = Some(format!("root={}", monitor_root.display()));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append fanotify_monitor_started event: {err}");
    }

    let mut batch: VecDeque<PendingAudit> = VecDeque::with_capacity(8192);
    let mut dropped = 0u64;
    let max_per_sec = ctx.config.agent.audit_max_events_per_second.max(1);
    let mut sec_window_start = std::time::Instant::now();
    let mut sec_count = 0u64;
    let mut buf = vec![0u8; 8192];
    loop {
        let nread = unsafe { libc::read(fan_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if nread <= 0 {
            flush_batch(ctx.event_ledger.clone(), &mut batch)?;
            continue;
        }
        let mut offset = 0usize;
        while offset + std::mem::size_of::<libc::fanotify_event_metadata>() <= nread as usize {
            let metadata =
                unsafe { &*(buf[offset..].as_ptr() as *const libc::fanotify_event_metadata) };
            if metadata.vers as usize != libc::FANOTIFY_METADATA_VERSION {
                break;
            }
            if (metadata.mask & libc::FAN_Q_OVERFLOW as u64) != 0 {
                dropped = dropped.saturating_add(1);
            }
            if metadata.fd >= 0 && (metadata.mask & libc::FAN_OPEN_PERM as u64) != 0 {
                let target_path = fd_path(metadata.fd);
                if let Some(path) = target_path.as_ref() {
                    if path_within(&monitor_root, path) {
                        if sec_window_start.elapsed() >= std::time::Duration::from_secs(1) {
                            sec_window_start = std::time::Instant::now();
                            sec_count = 0;
                        }
                        if sec_count >= max_per_sec {
                            dropped = dropped.saturating_add(1);
                        } else if let Err(err) = capture_preimage(&ctx.uhoh_dir, path, &mut batch) {
                            tracing::warn!("fanotify pre-image capture failed: {}", err);
                        } else {
                            sec_count = sec_count.saturating_add(1);
                        }
                    }
                }
                respond_allow(fan_fd, metadata.fd);
            }
            if metadata.fd >= 0 {
                unsafe {
                    libc::close(metadata.fd);
                }
            }
            if metadata.event_len == 0 {
                break;
            }
            offset += metadata.event_len as usize;
            if batch.len() >= 6144 {
                flush_batch(ctx.event_ledger.clone(), &mut batch)?;
            }
        }
        if dropped > 0 {
            let mut overflow = new_event("agent", "fanotify_overflow", "warn");
            overflow.detail = Some(format!("dropped={dropped}"));
            if let Err(err) = ctx.event_ledger.append(overflow) {
                tracing::error!("failed to append fanotify overflow event: {err}");
            }
            dropped = 0;
        }
    }
}

#[cfg(not(all(target_os = "linux", feature = "audit-trail")))]
pub fn run_permission_monitor(
    _ctx: &SubsystemContext,
    _agents: &[AgentEntry],
) -> anyhow::Result<()> {
    anyhow::bail!("fanotify permission monitor requires Linux + audit-trail feature")
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
#[derive(Debug)]
struct PendingAudit {
    path: PathBuf,
    pre_state_ref: String,
    pid: i32,
    pid_start_time_ticks: u64,
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn fd_path(fd: RawFd) -> Option<PathBuf> {
    let link = format!("/proc/self/fd/{fd}");
    std::fs::read_link(link).ok()
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn path_within(root: &Path, path: &Path) -> bool {
    let root_c = dunce::canonicalize(root).ok();
    let path_c = dunce::canonicalize(path).ok();
    match (root_c, path_c) {
        (Some(r), Some(p)) => p.starts_with(r),
        _ => false,
    }
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn capture_preimage(
    uhoh_dir: &Path,
    path: &Path,
    batch: &mut VecDeque<PendingAudit>,
) -> Result<()> {
    if !path.exists() || !path.is_file() {
        return Ok(());
    }
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open pre-image source: {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("Failed reading pre-image source: {}", path.display()))?;
    let (hash, _) = crate::cas::store_blob(&uhoh_dir.join("blobs"), &bytes)?;
    let (pid, start_ticks) = process_pid_start_ticks()?;
    batch.push_back(PendingAudit {
        path: path.to_path_buf(),
        pre_state_ref: hash,
        pid,
        pid_start_time_ticks: start_ticks,
    });
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn flush_batch(ledger: EventLedger, batch: &mut VecDeque<PendingAudit>) -> Result<()> {
    while let Some(item) = batch.pop_front() {
        let mut event = new_event("agent", "fanotify_preimage", "info");
        event.path = Some(item.path.display().to_string());
        event.pre_state_ref = Some(item.pre_state_ref.clone());
        event.detail = Some(
            serde_json::json!({
                "source": "fanotify_open_perm",
                "pid": item.pid,
                "pid_start_time_ticks": item.pid_start_time_ticks,
            })
            .to_string(),
        );
        if let Err(err) = ledger.append(event) {
            tracing::error!("failed to append fanotify_preimage event: {err}");
        }
    }
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn process_pid_start_ticks() -> Result<(i32, u64)> {
    let pid = std::process::id() as i32;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .with_context(|| format!("Failed reading /proc/{pid}/stat"))?;
    let parts: Vec<&str> = stat.split_whitespace().collect();
    let start_ticks = parts
        .get(21)
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or_default();
    Ok((pid, start_ticks))
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn respond_allow(fan_fd: RawFd, fd: RawFd) {
    let response = libc::fanotify_response {
        fd,
        response: libc::FAN_ALLOW,
    };
    unsafe {
        let _ = libc::write(
            fan_fd,
            &response as *const libc::fanotify_response as *const libc::c_void,
            std::mem::size_of::<libc::fanotify_response>(),
        );
    }
}
