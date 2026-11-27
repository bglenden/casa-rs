// SPDX-License-Identifier: LGPL-3.0-or-later
//! Scoped directory for resolving operands on an image task's worker.

use std::cell::RefCell;
use std::path::{Path, PathBuf};

use casa_images::ImageError;

thread_local! {
    static WORKSPACE: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
}

struct RestoreWorkspace(Option<PathBuf>);

impl Drop for RestoreWorkspace {
    fn drop(&mut self) {
        WORKSPACE.replace(self.0.take());
    }
}

/// Resolve relative image and mask operands from the supplied task workspace
/// for the duration of a synchronous image operation.
///
/// Other threads keep their own workspace. Without a scope, image operations
/// use the working directory as usual.
pub fn with_image_expression_workspace<R>(workspace: &Path, run: impl FnOnce() -> R) -> R {
    let _restore = RestoreWorkspace(WORKSPACE.replace(Some(workspace.to_path_buf())));
    run()
}

pub(super) fn directory() -> Result<PathBuf, ImageError> {
    let workspace = WORKSPACE.with_borrow(Clone::clone);
    workspace.map_or_else(
        || std::env::current_dir().map_err(|error| ImageError::Io(error.to_string())),
        Ok,
    )
}

pub(super) fn operand_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    WORKSPACE.with_borrow(|workspace| {
        if let Some(workspace) = workspace {
            workspace.join(path)
        } else {
            path.to_path_buf()
        }
    })
}
