#[cfg(target_os = "linux")]
use std::ffi::CString;
#[cfg(target_os = "linux")]
use std::io::Read;
#[cfg(target_os = "linux")]
use std::os::fd::RawFd;
#[cfg(target_os = "linux")]
use std::path::{Path, PathBuf};

#[cfg(target_os = "linux")]
use anyhow::{Context, Result};

#[cfg(target_os = "linux")]
use crate::db::AgentEntry;
#[cfg(target_os = "linux")]
use crate::event_ledger::new_event;
#[cfg(target_os = "linux")]
use crate::subsystem::SubsystemContext;

#[cfg(target_os = "linux")]
pub fn run_permission_monitor(ctx: &SubsystemContext, _agents: &[AgentEntry]) -> Result<()> {
    let monitor_root = std::env::current_dir().context("Unable to resolve current directory")?;
    let fan_fd = unsafe {
        libc::fanotify_init(
            (libc::FAN_CLASS_CONTENT | libc::FAN_CLOEXEC) as u32,
            libc::O_RDONLY | libc::O_LARGEFILE,
        )
    };
    if fan_fd < 0 {
        anyhow::bail!("fanotify_init failed (CAP_SYS_ADMIN likely required)");
    }

    let cpath = CString::new(monitor_root.to_string_lossy().as_bytes().to_vec())
        .context("Invalid monitor root path")?;
    let mark_ok = unsafe {
        libc::fanotify_mark(
            fan_fd,
            (libc::FAN_MARK_ADD | libc::FAN_MARK_MOUNT) as u32,
            (libc::FAN_OPEN_PERM | libc::FAN_EVENT_ON_CHILD) as u64,
            libc::AT_FDCWD,
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

    let mut buf = vec![0u8; 8192];
    loop {
        let nread = unsafe { libc::read(fan_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if nread <= 0 {
            continue;
        }
        let mut offset = 0usize;
        while offset + std::mem::size_of::<libc::fanotify_event_metadata>() <= nread as usize {
            let metadata =
                unsafe { &*(buf[offset..].as_ptr() as *const libc::fanotify_event_metadata) };
            if metadata.vers as usize != libc::FANOTIFY_METADATA_VERSION {
                break;
            }
            if metadata.fd >= 0 && (metadata.mask & libc::FAN_OPEN_PERM as u64) != 0 {
                let target_path = fd_path(metadata.fd);
                if let Some(path) = target_path.as_ref() {
                    if path_within(&monitor_root, path) {
                        if let Err(err) = capture_preimage(ctx, path) {
                            tracing::warn!("fanotify pre-image capture failed: {}", err);
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
        }
    }
}

#[cfg(target_os = "linux")]
fn fd_path(fd: RawFd) -> Option<PathBuf> {
    let link = format!("/proc/self/fd/{fd}");
    std::fs::read_link(link).ok()
}

#[cfg(target_os = "linux")]
fn path_within(root: &Path, path: &Path) -> bool {
    let root_c = dunce::canonicalize(root).ok();
    let path_c = dunce::canonicalize(path).ok();
    match (root_c, path_c) {
        (Some(r), Some(p)) => p.starts_with(r),
        _ => false,
    }
}

#[cfg(target_os = "linux")]
fn capture_preimage(ctx: &SubsystemContext, path: &Path) -> Result<()> {
    if !path.exists() || !path.is_file() {
        return Ok(());
    }
    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open pre-image source: {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("Failed reading pre-image source: {}", path.display()))?;
    let (hash, _) = crate::cas::store_blob(&ctx.uhoh_dir.join("blobs"), &bytes)?;
    let mut event = new_event("agent", "fanotify_preimage", "info");
    event.path = Some(path.display().to_string());
    event.pre_state_ref = Some(hash);
    event.detail = Some("source=fanotify_open_perm".to_string());
    if let Err(err) = ctx.event_ledger.append(event) {
        tracing::error!("failed to append fanotify_preimage event: {err}");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
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
