// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve existing ancestors as well as paths of products not yet created.
use std::path::{Component, Path, PathBuf};

fn normalized(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(path)
    };
    let mut lexical = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::ParentDir => {
                lexical.pop();
            }
            Component::CurDir => {}
            other => lexical.push(other.as_os_str()),
        }
    }
    let mut ancestor = lexical.as_path();
    let mut tail = Vec::new();
    while !ancestor.exists() {
        let (Some(name), Some(parent)) = (ancestor.file_name(), ancestor.parent()) else {
            return lexical;
        };
        tail.push(name.to_os_string());
        ancestor = parent;
    }
    let mut result = ancestor
        .canonicalize()
        .unwrap_or_else(|_| ancestor.to_path_buf());
    for name in tail.into_iter().rev() {
        result.push(name);
    }
    result
}

pub(crate) fn overlap(left: &Path, right: &Path) -> bool {
    let left = normalized(left);
    let right = normalized(right);
    left.starts_with(&right) || right.starts_with(&left)
}
