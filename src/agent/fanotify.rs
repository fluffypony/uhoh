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
use crate::db::{AgentEntry, LedgerSeverity, LedgerSource};
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
        // SAFETY: closing a valid fd obtained from fanotify_init; Drop guarantees
        // this runs exactly once and the fd is not used afterward.
        unsafe {
            libc::close(self.0);
        }
    }
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
struct EventRateLimiter {
    max_per_sec: u64,
    window_started_at: std::time::Instant,
    seen_in_window: u64,
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
impl EventRateLimiter {
    fn new(max_per_sec: u64) -> Self {
        Self {
            max_per_sec,
            window_started_at: std::time::Instant::now(),
            seen_in_window: 0,
        }
    }

    fn allow_next(&mut self) -> bool {
        if self.window_started_at.elapsed() >= std::time::Duration::from_secs(1) {
            self.window_started_at = std::time::Instant::now();
            self.seen_in_window = 0;
        }
        if self.seen_in_window >= self.max_per_sec {
            return false;
        }
        self.seen_in_window = self.seen_in_window.saturating_add(1);
        true
    }
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
struct PendingAuditBatch {
    items: VecDeque<PendingAudit>,
    dropped: u64,
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
impl PendingAuditBatch {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            items: VecDeque::with_capacity(capacity),
            dropped: 0,
        }
    }

    fn push(&mut self, item: PendingAudit) {
        self.items.push_back(item);
    }

    fn note_drop(&mut self) {
        self.dropped = self.dropped.saturating_add(1);
    }

    fn maybe_flush(&mut self, ledger: &EventLedger) -> Result<()> {
        if self.items.len() >= 6144 {
            self.flush(ledger)?;
        }
        Ok(())
    }

    fn flush(&mut self, ledger: &EventLedger) -> Result<()> {
        flush_batch(ledger, &mut self.items)
    }

    fn emit_overflow_if_needed(&mut self, ctx: &AgentContext) {
        if self.dropped == 0 {
            return;
        }
        let mut overflow = new_event(
            LedgerSource::Agent,
            "fanotify_overflow",
            LedgerSeverity::Warn,
        );
        overflow.detail = Some(format!("dropped={}", self.dropped));
        if let Err(err) = ctx.event_ledger.append(overflow) {
            tracing::error!("failed to append fanotify overflow event: {err}");
        }
        self.dropped = 0;
    }
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
pub(crate) fn run_permission_monitor_with_roots(
    ctx: &AgentContext,
    _agents: &[AgentEntry],
    monitor_roots: &[PathBuf],
) -> Result<()> {
    let monitor_roots = normalize_monitor_roots(monitor_roots)?;
    let fan_fd = init_fanotify()?;
    mark_monitor_roots(fan_fd.0, &monitor_roots)?;
    emit_monitor_started(ctx, &monitor_roots);

    let mut batch = PendingAuditBatch::with_capacity(8192);
    let mut rate_limiter =
        EventRateLimiter::new(ctx.config.agent.audit_max_events_per_second.max(1));
    let mut buf = vec![0u8; 8192];
    let mut pollfd = libc::pollfd {
        fd: fan_fd.0,
        events: libc::POLLIN,
        revents: 0,
    };

    loop {
        if !poll_for_events(&mut pollfd) {
            continue;
        }
        let Some(nread) = read_fanotify_chunk(fan_fd.0, &mut buf, &ctx.event_ledger, &mut batch)?
        else {
            continue;
        };
        process_event_metadata(
            &ctx.uhoh_dir,
            &ctx.event_ledger,
            &monitor_roots,
            fan_fd.0,
            &buf[..nread],
            &mut rate_limiter,
            &mut batch,
        )?;
        batch.emit_overflow_if_needed(ctx);
    }
}

#[cfg(not(all(target_os = "linux", feature = "audit-trail")))]
pub(crate) fn run_permission_monitor_with_roots(
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
fn normalize_monitor_roots(monitor_roots: &[PathBuf]) -> Result<Vec<PathBuf>> {
    if monitor_roots.is_empty() {
        return Ok(vec![std::env::current_dir().context(
            "No monitor roots found and unable to resolve current directory",
        )?]);
    }
    Ok(monitor_roots.to_vec())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn init_fanotify() -> Result<FanotifyFd> {
    // SAFETY: fanotify_init is called with valid flag constants; failure returns -1
    // which is checked immediately below.
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
    Ok(FanotifyFd(fan_fd_raw))
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn mark_monitor_roots(fan_fd: RawFd, monitor_roots: &[PathBuf]) -> Result<()> {
    for monitor_root in monitor_roots {
        let cpath = CString::new(monitor_root.as_os_str().as_bytes().to_vec())
            .context("Invalid monitor root path")?;
        let mark_fd = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_CLOEXEC)
            .open(monitor_root)
            .with_context(|| format!("Failed opening monitor root: {}", monitor_root.display()))?;
        // SAFETY: fanotify_mark is called with a valid fanotify fd, a valid directory fd
        // from OpenOptions, and a NUL-terminated CString path; return value is checked.
        let mark_ok = unsafe {
            libc::fanotify_mark(
                fan_fd,
                (libc::FAN_MARK_ADD | libc::FAN_MARK_MOUNT | libc::FAN_MARK_FILESYSTEM) as u32,
                (libc::FAN_OPEN_PERM | libc::FAN_EVENT_ON_CHILD) as u64,
                mark_fd.as_raw_fd(),
                cpath.as_ptr(),
            )
        };
        if mark_ok >= 0 {
            continue;
        }

        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINVAL) {
            tracing::warn!("FAN_MARK_FILESYSTEM unsupported, retrying with FAN_MARK_MOUNT only");
            // SAFETY: same as above but without FAN_MARK_FILESYSTEM; all fd and
            // path arguments remain valid from the enclosing scope.
            let fallback = unsafe {
                libc::fanotify_mark(
                    fan_fd,
                    (libc::FAN_MARK_ADD | libc::FAN_MARK_MOUNT) as u32,
                    (libc::FAN_OPEN_PERM | libc::FAN_EVENT_ON_CHILD) as u64,
                    mark_fd.as_raw_fd(),
                    cpath.as_ptr(),
                )
            };
            if fallback >= 0 {
                continue;
            }
        }

        anyhow::bail!("fanotify_mark failed for {}", monitor_root.display());
    }

    Ok(())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn emit_monitor_started(ctx: &AgentContext, monitor_roots: &[PathBuf]) {
    let mut event = new_event(
        LedgerSource::Agent,
        "fanotify_monitor_started",
        LedgerSeverity::Info,
    );
    event.detail = Some(format!(
        "roots={}",
        monitor_roots
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(",")
    ));
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append fanotify_monitor_started event: {err}");
    }
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn poll_for_events(pollfd: &mut libc::pollfd) -> bool {
    // SAFETY: pollfd points to a valid, initialized libc::pollfd struct with a valid
    // fanotify fd; nfds=1 matches the single-element pointer.
    let poll_ret = unsafe { libc::poll(pollfd, 1, 1000) };
    if poll_ret == 0 {
        return false;
    }
    if poll_ret < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            tracing::error!("fanotify poll error: {err}");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        return false;
    }
    true
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn read_fanotify_chunk(
    fan_fd: RawFd,
    buf: &mut [u8],
    ledger: &EventLedger,
    batch: &mut PendingAuditBatch,
) -> Result<Option<usize>> {
    // SAFETY: reading from a valid fanotify fd into a caller-owned buffer;
    // buf.as_mut_ptr() and buf.len() describe a valid writable region.
    let nread = unsafe { libc::read(fan_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
    if nread < 0 {
        let err = std::io::Error::last_os_error();
        if err.kind() != std::io::ErrorKind::Interrupted {
            tracing::error!("fanotify read error: {err}");
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        batch.flush(ledger)?;
        return Ok(None);
    }
    if nread == 0 {
        batch.flush(ledger)?;
        return Ok(None);
    }
    Ok(Some(nread as usize))
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn process_event_metadata(
    uhoh_dir: &Path,
    event_ledger: &EventLedger,
    monitor_roots: &[PathBuf],
    fan_fd: RawFd,
    bytes: &[u8],
    rate_limiter: &mut EventRateLimiter,
    batch: &mut PendingAuditBatch,
) -> Result<()> {
    let mut offset = 0usize;
    while let Some(metadata) = next_event_metadata(bytes, &mut offset) {
        handle_event_metadata(
            uhoh_dir,
            monitor_roots,
            fan_fd,
            metadata,
            rate_limiter,
            batch,
        )?;
        batch.maybe_flush(event_ledger)?;
    }
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn next_event_metadata<'a>(
    bytes: &'a [u8],
    offset: &mut usize,
) -> Option<&'a libc::fanotify_event_metadata> {
    if *offset + std::mem::size_of::<libc::fanotify_event_metadata>() > bytes.len() {
        return None;
    }
    // SAFETY: the size check above guarantees bytes[*offset..] has enough room for a
    // fanotify_event_metadata. The buffer was filled by read(2) from the fanotify fd, which
    // writes metadata at naturally aligned boundaries. The content is kernel-produced and valid.
    // Note: &[u8] from stack/heap allocation may not be naturally aligned for this struct;
    // this works because fanotify_event_metadata has no alignment requirement beyond 1 byte
    // on Linux (it is packed by the kernel ABI).
    let metadata = unsafe { &*(bytes[*offset..].as_ptr() as *const libc::fanotify_event_metadata) };
    if metadata.vers as usize != libc::FANOTIFY_METADATA_VERSION || metadata.event_len == 0 {
        return None;
    }
    *offset += metadata.event_len as usize;
    Some(metadata)
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn handle_event_metadata(
    uhoh_dir: &Path,
    monitor_roots: &[PathBuf],
    fan_fd: RawFd,
    metadata: &libc::fanotify_event_metadata,
    rate_limiter: &mut EventRateLimiter,
    batch: &mut PendingAuditBatch,
) -> Result<()> {
    if (metadata.mask & libc::FAN_Q_OVERFLOW as u64) != 0 {
        batch.note_drop();
    }
    if metadata.fd >= 0 && (metadata.mask & libc::FAN_OPEN_PERM as u64) != 0 {
        handle_permission_event(
            uhoh_dir,
            monitor_roots,
            fan_fd,
            metadata,
            rate_limiter,
            batch,
        )?;
    }
    close_metadata_fd(metadata.fd);
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn handle_permission_event(
    uhoh_dir: &Path,
    monitor_roots: &[PathBuf],
    fan_fd: RawFd,
    metadata: &libc::fanotify_event_metadata,
    rate_limiter: &mut EventRateLimiter,
    batch: &mut PendingAuditBatch,
) -> Result<()> {
    let target_path = fd_path(metadata.fd);
    if let Some(path) = target_path.as_ref() {
        if monitor_roots.iter().any(|root| path_within(root, path)) {
            if !rate_limiter.allow_next() {
                batch.note_drop();
            } else if let Err(err) =
                capture_preimage_from_fd(uhoh_dir, path, metadata.fd, metadata.pid, batch)
            {
                tracing::warn!("fanotify pre-image capture failed: {}", err);
            }
        }
    }
    respond_allow(fan_fd, metadata.fd);
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn close_metadata_fd(fd: RawFd) {
    if fd >= 0 {
        // SAFETY: fd is a non-negative file descriptor provided by fanotify event
        // metadata; each event fd is closed exactly once here after processing.
        unsafe {
            libc::close(fd);
        }
    }
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
    batch: &mut PendingAuditBatch,
) -> Result<()> {
    if !path.is_file() {
        return Ok(());
    }
    // Use dup() on the fanotify-provided fd instead of File::open() to avoid
    // generating a second FAN_OPEN_PERM event that would deadlock the monitor.
    // SAFETY: dup() on a valid fanotify event fd; failure returns -1 which is
    // checked immediately below. The original fd remains owned by the caller.
    let dup_fd = unsafe { libc::dup(event_fd) };
    if dup_fd < 0 {
        anyhow::bail!("dup() failed for pre-image fd");
    }
    // SAFETY: dup_fd is a valid, newly duplicated fd (checked non-negative above);
    // ownership transfers to File which will close it on drop.
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
    batch.push(PendingAudit {
        path: path.to_path_buf(),
        pre_state_ref: hash,
        pid,
        pid_start_time_ticks: start_ticks,
    });
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "audit-trail"))]
fn flush_batch(ledger: &EventLedger, batch: &mut VecDeque<PendingAudit>) -> Result<()> {
    while let Some(item) = batch.pop_front() {
        let mut event = new_event(
            LedgerSource::Agent,
            "fanotify_preimage",
            LedgerSeverity::Info,
        );
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
    // SAFETY: writing a properly initialized fanotify_response to a valid fanotify fd;
    // the response struct lives on the stack and the pointer/size are correct.
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
