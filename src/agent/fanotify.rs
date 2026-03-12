#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::collections::VecDeque;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::ffi::CString;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::io::Read;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::fd::AsRawFd;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::fd::RawFd;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::unix::ffi::OsStrExt;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::unix::fs::OpenOptionsExt;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::os::unix::io::FromRawFd;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use std::path::{Path, PathBuf};

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use anyhow::{Context, Result};

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::db::AgentEntry;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::event_ledger::new_event;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::event_ledger::EventLedger;
#[cfg(all(target_os = "linux", feature = "audit-trail"))]
use crate::subsystem::AgentContext;

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
struct FanotifyFd(RawFd);

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
impl Drop for FanotifyFd {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.0);
        }
    }
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
pub fn run_permission_monitor_with_roots(
    ctx: &AgentContext,
    _agents: &[AgentEntry],
    monitor_roots: &[PathBuf],
) -> Result<()> {
    let monitor_roots: Vec<PathBuf> = if monitor_roots.is_empty() {
        vec![std::env::current_dir()
            .context("No monitor roots found and unable to resolve current directory")?]
    } else {
        monitor_roots.to_vec()
    };
    let fan_fd_raw = unsafe {
        libc::fanotify_init(
            (libc::FAN_CLASS_CONTENT
                | libc::FAN_CLOEXEC
                | libc::FAN_REPORT_FID
                | libc::FAN_REPORT_DFID_NAME) as u32,
            libc::O_RDONLY | libc::O_LARGEFILE,
        )
    };
    if fan_fd_raw < 0 {
        anyhow::bail!("fanotify_init failed (requires CAP_SYS_ADMIN and kernel >= 5.1)");
    }
    let fan_fd = FanotifyFd(fan_fd_raw);

    for monitor_root in &monitor_roots {
        let cpath = CString::new(monitor_root.as_os_str().as_bytes().to_vec())
            .context("Invalid monitor root path")?;
        let mark_fd = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(monitor_root)
            .with_context(|| format!("Failed opening monitor root: {}", monitor_root.display()))?;
        let mark_ok = unsafe {
            libc::fanotify_mark(
                fan_fd.0,
                (libc::FAN_MARK_ADD | libc::FAN_MARK_MOUNT | libc::FAN_MARK_FILESYSTEM) as u32,
                (libc::FAN_OPEN_PERM | libc::FAN_EVENT_ON_CHILD) as u64,
                mark_fd.as_raw_fd(),
                cpath.as_ptr(),
            )
        };
        if mark_ok < 0 {
            let err = std::io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EINVAL) {
                tracing::warn!(
                    "FAN_MARK_FILESYSTEM unsupported, retrying with FAN_MARK_MOUNT only"
                );
                let fallback = unsafe {
                    libc::fanotify_mark(
                        fan_fd.0,
                        (libc::FAN_MARK_ADD | libc::FAN_MARK_MOUNT) as u32,
                        (libc::FAN_OPEN_PERM | libc::FAN_EVENT_ON_CHILD) as u64,
                        mark_fd.as_raw_fd(),
                        cpath.as_ptr(),
                    )
                };
                if fallback < 0 {
                    anyhow::bail!("fanotify_mark failed for {}", monitor_root.display());
                }
            } else {
                anyhow::bail!("fanotify_mark failed for {}", monitor_root.display());
            }
        }
    }

    let mut event = new_event("agent", "fanotify_monitor_started", "info");
    event.detail = Some(format!(
        "roots={}",
        monitor_roots
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(",")
    ));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append fanotify_monitor_started event: {err}");
    }

    let mut batch: VecDeque<PendingAudit> = VecDeque::with_capacity(8192);
    let mut dropped = 0u64;
    let max_per_sec = ctx.config.agent.audit_max_events_per_second.max(1);
    let mut sec_window_start = std::time::Instant::now();
    let mut sec_count = 0u64;
    let mut buf = vec![0u8; 8192];
    let mut pollfd = libc::pollfd {
        fd: fan_fd.0,
        events: libc::POLLIN,
        revents: 0,
    };

    loop {
        let poll_ret = unsafe { libc::poll(&mut pollfd, 1, 1000) };
        if poll_ret == 0 {
            continue;
        }
        if poll_ret < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() != std::io::ErrorKind::Interrupted {
                tracing::error!("fanotify poll error: {err}");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            continue;
        }

        let nread =
            unsafe { libc::read(fan_fd.0, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if nread < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() != std::io::ErrorKind::Interrupted {
                tracing::error!("fanotify read error: {err}");
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            flush_batch(ctx.event_ledger.clone(), &mut batch)?;
            continue;
        }
        if nread == 0 {
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
                    if monitor_roots.iter().any(|root| path_within(root, path)) {
                        if sec_window_start.elapsed() >= std::time::Duration::from_secs(1) {
                            sec_window_start = std::time::Instant::now();
                            sec_count = 0;
                        }
                        if sec_count >= max_per_sec {
                            dropped = dropped.saturating_add(1);
                        } else if let Err(err) = capture_preimage_from_fd(
                            &ctx.uhoh_dir,
                            path,
                            metadata.fd,
                            metadata.pid,
                            &mut batch,
                        ) {
                            tracing::warn!("fanotify pre-image capture failed: {}", err);
                        } else {
                            sec_count = sec_count.saturating_add(1);
                        }
                    }
                }
                respond_allow(fan_fd.0, metadata.fd);
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
pub fn run_permission_monitor_with_roots(
    _ctx: &crate::subsystem::AgentContext,
    _agents: &[crate::db::AgentEntry],
    _monitor_roots: &[std::path::PathBuf],
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
const PREIMAGE_MAX_BYTES: u64 = 5 * 1024 * 1024; // 5 MiB cap for pre-image capture

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn capture_preimage_from_fd(
    uhoh_dir: &Path,
    path: &Path,
    event_fd: i32,
    pid: i32,
    batch: &mut VecDeque<PendingAudit>,
) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    // Use dup() on the fanotify-provided fd instead of File::open() to avoid
    // generating a second FAN_OPEN_PERM event that would deadlock the monitor.
    let dup_fd = unsafe { libc::dup(event_fd) };
    if dup_fd < 0 {
        anyhow::bail!("dup() failed for pre-image fd");
    }
    let mut file = unsafe { std::fs::File::from_raw_fd(dup_fd) };
    // Check file size before reading to prevent OOM
    let file_len = file.metadata().map(|m| m.len()).unwrap_or(0);
    if file_len > PREIMAGE_MAX_BYTES {
        tracing::debug!(
            "Skipping pre-image for {} ({} bytes > {} limit)",
            path.display(),
            file_len,
            PREIMAGE_MAX_BYTES,
        );
        return Ok(());
    }
    // Seek to start in case fd position is not at 0
    use std::io::Seek;
    let _ = file.seek(std::io::SeekFrom::Start(0));
    let mut bytes = Vec::with_capacity(file_len as usize);
    file.read_to_end(&mut bytes)
        .with_context(|| format!("Failed reading pre-image source: {}", path.display()))?;
    let (hash, _) = crate::cas::store_blob(&uhoh_dir.join("blobs"), &bytes)?;
    let start_ticks = process_start_ticks(pid)
        .with_context(|| format!("failed to resolve process start ticks for pid {}", pid))?;
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
        let session_id = format!("pid:{}:{}", item.pid, item.pid_start_time_ticks);
        event.detail = Some(
            serde_json::json!({
                "session_id": session_id,
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
fn process_start_ticks(pid: i32) -> Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .with_context(|| format!("Failed reading /proc/{pid}/stat"))?;
    let close_paren = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("Malformed /proc/{pid}/stat: missing ')'"))?;
    let tail = stat
        .get(close_paren + 2..)
        .ok_or_else(|| anyhow::anyhow!("Malformed /proc/{pid}/stat: missing tail"))?;
    let parts: Vec<&str> = tail.split_whitespace().collect();
    let start_ticks = parts
        .get(19)
        .ok_or_else(|| anyhow::anyhow!("Malformed /proc/{pid}/stat: missing starttime field"))?
        .parse::<u64>()
        .with_context(|| format!("Malformed /proc/{pid}/stat: invalid starttime"))?;
    Ok(start_ticks)
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn respond_allow(fan_fd: RawFd, fd: RawFd) {
    let response = libc::fanotify_response {
        fd,
        response: libc::FAN_ALLOW,
    };
    let ret = unsafe {
        libc::write(
            fan_fd,
            &response as *const libc::fanotify_response as *const libc::c_void,
            std::mem::size_of::<libc::fanotify_response>(),
        )
    };
    if ret < 0 {
        tracing::error!(
            "Failed to write FAN_ALLOW response: {}",
            std::io::Error::last_os_error()
        );
        // Do NOT close fd here — caller owns the fd and will close it
    }
}

#[cfg(all(test, target_os = "linux", feature = "audit-trail"))]
mod tests {
    use super::process_start_ticks;

    #[test]
    fn process_start_ticks_reads_current_process() {
        let pid = std::process::id() as i32;
        let ticks = process_start_ticks(pid).expect("current process stat should be readable");
        assert!(ticks > 0);
    }
}
