// SPDX-License-Identifier: LGPL-3.0-or-later
//! Move constituent tables into a new virtual concatenation (#1028).

use std::collections::HashSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use casa_tables::{Table, TableInfo, TableOptions};

use super::copy::{
    check_target, refuse_nested, same_directory, staging_path, used_by_another_process,
};
use super::state::{Fail, State};
use crate::convert::value::CasaValue;
use crate::tools::LogLine;

fn io_fail(error: std::io::Error) -> Fail {
    Fail::Runtime(error.to_string())
}

impl State {
    pub(super) fn move_multitable(
        &self,
        target: &Path,
        sources: &[PathBuf],
        subdirectory: &Path,
        log: &mut Vec<LogLine>,
    ) -> Result<CasaValue, Fail> {
        if !subdirectory
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
        {
            return Err(Fail::Runtime(
                "Invalid subdirectory for concatenation".into(),
            ));
        }
        let mut seen_paths = HashSet::new();
        let mut seen_names = HashSet::new();
        for (index, source) in sources.iter().enumerate() {
            if !fs::symlink_metadata(source).map_err(io_fail)?.is_dir()
                || !seen_paths.insert(fs::canonicalize(source).map_err(io_fail)?)
                || !seen_names.insert(
                    source
                        .file_name()
                        .ok_or_else(|| Fail::Runtime("Invalid input name".into()))?
                        .to_os_string(),
                )
            {
                return Err(Fail::Runtime(
                    "Unsafe or duplicate concatenation input".into(),
                ));
            }
            for other in &sources[..index] {
                refuse_nested(source, other)?;
            }
            if self
                .open
                .as_ref()
                .is_some_and(|open| same_directory(&open.name, source))
            {
                return Err(Fail::Runtime("Concatenation input is still open".into()));
            }
            self.refuse_open_target(source)?;
            if used_by_another_process(source) {
                return Err(Fail::Runtime(format!(
                    "Concatenation input {} is in use",
                    source.display()
                )));
            }
        }

        let staging = staging_path(target);
        fs::create_dir(&staging).map_err(io_fail)?;
        let mut moved = Vec::new();
        let result = make_moved_table(target, sources, subdirectory, &staging, &mut moved, log);
        if result.is_err() {
            let mut restored = true;
            for (original, temporary) in moved.iter().rev() {
                if fs::symlink_metadata(original).is_ok()
                    || fs::rename(temporary, original).is_err()
                {
                    restored = false;
                    log.push(LogLine::error(format!(
                        "Could not restore {}; data remains at {}",
                        original.display(),
                        temporary.display()
                    )));
                }
            }
            if restored {
                let _ = fs::remove_dir_all(&staging);
            }
        }
        result.map(|()| CasaValue::Bool(true))
    }
}

fn make_moved_table(
    target: &Path,
    sources: &[PathBuf],
    subdirectory: &Path,
    staging: &Path,
    moved: &mut Vec<(PathBuf, PathBuf)>,
    log: &mut Vec<LogLine>,
) -> Result<(), Fail> {
    let parts = staging.join(subdirectory);
    fs::create_dir_all(&parts).map_err(io_fail)?;
    for source in sources {
        let destination = parts.join(source.file_name().expect("input name checked"));
        fs::rename(source, &destination).map_err(io_fail)?;
        moved.push((source.clone(), destination));
    }
    let tables = moved
        .iter()
        .map(|(_, path)| Table::open(TableOptions::new(path)))
        .collect::<Result<Vec<_>, _>>()?;
    let concat = Table::concat(tables)?;
    concat.save(TableOptions::new(staging))?;
    let mut readme = vec!["Virtual concatenation of the following tables:".to_string()];
    readme.extend(sources.iter().map(|path| {
        format!(
            "  {}",
            subdirectory
                .join(path.file_name().expect("input name checked"))
                .display()
        )
    }));
    let info = TableInfo {
        sub_type: "CONCATENATED".into(),
        readme,
        ..TableInfo::default()
    };
    fs::write(staging.join("table.info"), info.to_string()).map_err(io_fail)?;

    check_target(target)?;
    let backup = staging_path(target);
    let had_target = fs::symlink_metadata(target).is_ok();
    if had_target {
        if fs::symlink_metadata(&backup).is_ok() {
            return Err(Fail::Runtime(format!(
                "Backup path {} already exists",
                backup.display()
            )));
        }
        fs::rename(target, &backup).map_err(io_fail)?;
    }
    if let Err(error) = fs::rename(staging, target) {
        if had_target && fs::rename(&backup, target).is_err() {
            log.push(LogLine::error(format!(
                "Previous output remains at {}",
                backup.display()
            )));
        }
        return Err(io_fail(error));
    }
    if had_target && let Err(error) = fs::remove_dir_all(&backup) {
        log.push(LogLine::warning(format!(
            "Previous output remains at {}: {error}",
            backup.display()
        )));
    }
    Ok(())
}
