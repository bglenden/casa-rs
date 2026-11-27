// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;

/// Expand casacore path variables and the current user's home directory.
/// Returns `None` when repeated expansion exceeds casacore's limit.
pub(crate) fn expand_path(name: &str) -> Option<Cow<'_, str>> {
    expand(name, false)
}

/// Also resolve named home directories for ASDM index path display.
pub(crate) fn expand_asdm_path(name: &str) -> Option<Cow<'_, str>> {
    expand(name, true)
}

fn expand(name: &str, named_home: bool) -> Option<Cow<'_, str>> {
    if !name.starts_with('~') && !name.contains('$') {
        return Some(Cow::Borrowed(name));
    }
    let mut path = Cow::Borrowed(name);
    // casacore Path::expandName follows environment references recursively
    // and stops expanding if the substitutions form a cycle.
    for _ in 0..25 {
        if let Some(tail) = path.strip_prefix('~') {
            let user = tail.split('/').next().unwrap_or_default();
            if (user.is_empty() || named_home)
                && let Some(home) = home_directory(user).filter(|home| !home.is_empty())
            {
                path = Cow::Owned(format!("{home}{}", &tail[user.len()..]));
            }
        }
        let mut changed = false;
        let next = path
            .split('/')
            .map(|part| match expand_variable(part) {
                Some(expanded) => {
                    changed = true;
                    Cow::Owned(expanded)
                }
                None => Cow::Borrowed(part),
            })
            .collect::<Vec<_>>()
            .join("/");
        if !changed {
            return Some(path);
        }
        path = Cow::Owned(next);
    }
    None
}

fn home_directory(user: &str) -> Option<String> {
    if user.is_empty() {
        return std::env::var("HOME").ok();
    }
    #[cfg(unix)]
    {
        use std::ffi::{CStr, CString};
        use std::mem::MaybeUninit;
        use std::ptr::null_mut;

        let user = CString::new(user).ok()?;
        let mut scratch = vec![0; 16 * 1024];
        let mut account = MaybeUninit::<libc::passwd>::uninit();
        loop {
            let mut result = null_mut();
            // SAFETY: The buffers are live and writable throughout the lookup.
            let status = unsafe {
                libc::getpwnam_r(
                    user.as_ptr(),
                    account.as_mut_ptr(),
                    scratch.as_mut_ptr(),
                    scratch.len(),
                    &mut result,
                )
            };
            if status == libc::ERANGE {
                scratch.resize(scratch.len().checked_mul(2)?, 0);
                continue;
            }
            if status != 0 || result.is_null() {
                return None;
            }
            // SAFETY: A successful lookup with a non-null result initialized
            // the account and its NUL-terminated home path in the buffer.
            return unsafe { CStr::from_ptr((*result).pw_dir) }
                .to_str()
                .ok()
                .map(str::to_owned);
        }
    }
    #[cfg(not(unix))]
    None
}

fn expand_variable(part: &str) -> Option<String> {
    let (prefix, expression) = part.split_once('$')?;
    let (variable, suffix) = match expression.strip_prefix('{') {
        Some(braced) => braced.split_once('}')?,
        None => (expression, ""),
    };
    let value = std::env::var(variable)
        .ok()
        .filter(|value| !value.is_empty())?;
    Some(format!("{prefix}{value}{suffix}"))
}
