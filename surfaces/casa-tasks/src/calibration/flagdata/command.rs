// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publish a saved flag command file.

use std::io::{ErrorKind, Write};
use std::path::Path;

use crate::TaskFailure;

pub(super) fn write_file(
    outfile: &str,
    content: &str,
    overwrite: bool,
    save_only: bool,
) -> Result<(), TaskFailure> {
    let path = Path::new(outfile);
    let parent = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let save = || -> std::io::Result<()> {
        let mut builder = tempfile::Builder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            builder.permissions(std::fs::Permissions::from_mode(0o666));
        }
        let mut pending = builder.tempfile_in(parent)?;
        writeln!(pending, "{content}")?;
        if overwrite {
            pending.persist(path)?;
        } else {
            pending.persist_noclobber(path)?;
        }
        Ok(())
    };
    save().map_err(|error| {
        if !overwrite && error.kind() == ErrorKind::AlreadyExists {
            let message = format!(
                "You have set overwrite to False. Remove {outfile} before saving the flag commands"
            );
            if save_only {
                return TaskFailure::validation(message).at("outfile");
            }
            return TaskFailure::execution(message).at("outfile");
        }
        let parameter = if error.kind() == ErrorKind::NotFound {
            "outfile.not_found"
        } else {
            "outfile"
        };
        TaskFailure::execution(format!("Error writing flag commands to {outfile}: {error}"))
            .at(parameter)
    })
}
