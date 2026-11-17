// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plain table names referenced by a table's on-disk metadata.

use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};

use super::{Table, TableError};
use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};
use crate::table_path::add_directory_for_link;

/// Make a table cache name absolute and remove lexical `.` and `..` without
/// resolving symbolic links. casacore caches a link and its target under
/// distinct names. Shell variable and tilde expansion are not performed.
pub fn table_cache_name(path: &Path) -> Result<PathBuf, TableError> {
    let path = if path.as_os_str().is_empty() {
        Path::new(".")
    } else {
        path
    };
    let absolute =
        std::path::absolute(path).map_err(|error| TableError::Storage(error.to_string()))?;
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => (),
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

fn visit(
    path: &Path,
    active: &mut HashSet<PathBuf>,
    result: &mut BTreeSet<PathBuf>,
    depth: usize,
) -> Result<(), TableError> {
    if depth >= 128 {
        return Err(TableError::Storage(
            "table reference chain is too deep".into(),
        ));
    }
    let real =
        std::fs::canonicalize(path).map_err(|error| TableError::Storage(error.to_string()))?;
    if !active.insert(real.clone()) {
        return Err(TableError::Storage("table reference cycle".into()));
    }
    match read_table_dat_dispatch(&path.join(crate::storage::TABLE_CONTROL_FILE))? {
        TableDatResult::Plain(_) => {
            result.insert(table_cache_name(path)?);
        }
        TableDatResult::Ref(reference) => {
            visit(
                &add_directory_for_link(&reference.parent_relative_path, path),
                active,
                result,
                depth + 1,
            )?;
        }
        TableDatResult::Concat(concat) => {
            for part in &concat.table_paths {
                visit(
                    &add_directory_for_link(part, path),
                    active,
                    result,
                    depth + 1,
                )?;
            }
        }
    }
    active.remove(&real);
    Ok(())
}

impl Table {
    /// Return the absolute plain-table cache names held by this table. A
    /// reference contributes its root and a concatenation contributes its
    /// constituents; memory tables contribute none. Names are sorted and
    /// deduplicated without resolving symlinks. Missing, cyclic, or malformed
    /// references return an error. This reads metadata only, never cells.
    pub fn cache_plain_table_paths(&self) -> Result<Vec<PathBuf>, TableError> {
        if self.is_memory() {
            return Ok(Vec::new());
        }
        let Some(path) = self.source_path.as_ref() else {
            return Ok(Vec::new());
        };
        let mut paths = BTreeSet::new();
        visit(path, &mut HashSet::new(), &mut paths, 0)?;
        Ok(paths.into_iter().collect())
    }
}
