// SPDX-License-Identifier: LGPL-3.0-or-later
//! SIGINT cancels the run (plan section 5.4). The first interrupt sets the
//! process's cancellation token: the run stops at the next block boundary of
//! a pass or before its next phase, and publishes nothing. A system call the
//! interrupt lands in is restarted, so a read in flight completes rather than
//! failing with `EINTR`. The handler then resets itself, so a second
//! interrupt terminates the process.

use std::sync::OnceLock;

use casa_imaging_application::Cancel;

static TOKEN: OnceLock<Cancel> = OnceLock::new();

/// The process's cancellation token, which every run of this process
/// observes.
pub fn token() -> &'static Cancel {
    TOKEN.get_or_init(Cancel::new)
}

/// Install the SIGINT handler that sets [`token`].
///
/// # Errors
///
/// The handler could not be installed.
pub fn install() -> std::io::Result<()> {
    // The token exists before the handler can run, so the handler only
    // loads it and stores a flag: both async-signal-safe.
    token();
    // SAFETY: `action` is fully initialised before `sigaction` reads it, and
    // `interrupted` touches only an initialised atomic.
    let status = unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = interrupted as extern "C" fn(libc::c_int) as libc::sighandler_t;
        action.sa_flags = libc::SA_RESETHAND | libc::SA_RESTART;
        libc::sigemptyset(&mut action.sa_mask);
        libc::sigaction(libc::SIGINT, &action, std::ptr::null_mut())
    };
    if status == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

extern "C" fn interrupted(_: libc::c_int) {
    if let Some(token) = TOKEN.get() {
        token.cancel();
    }
}
