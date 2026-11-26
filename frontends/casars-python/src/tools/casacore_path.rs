// SPDX-License-Identifier: LGPL-3.0-or-later

use std::borrow::Cow;

/// Expand casacore path variables and the current user's home directory.
/// Returns `None` when repeated expansion exceeds casacore's limit.
pub(crate) fn expand_path(name: &str) -> Option<Cow<'_, str>> {
    if !name.starts_with('~') && !name.contains('$') {
        return Some(Cow::Borrowed(name));
    }
    let mut path = Cow::Borrowed(name);
    // casacore Path::expandName follows environment references recursively
    // and stops expanding if the substitutions form a cycle.
    for _ in 0..25 {
        if let Some(suffix) = path
            .strip_prefix('~')
            .filter(|s| s.is_empty() || s.starts_with('/'))
            && let Ok(home) = std::env::var("HOME")
            && !home.is_empty()
        {
            path = Cow::Owned(format!("{home}{suffix}"));
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
