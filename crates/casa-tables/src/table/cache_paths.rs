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

fn immediate_names(path: &Path, parts: &[String]) -> Result<Vec<PathBuf>, TableError> {
    parts
        .iter()
        .map(|part| table_cache_name(&add_directory_for_link(part, path)))
        .collect()
}

/// A persisted table's immediate parts, as reported by casacore.
pub enum TablePartNames {
    /// A plain table reports its own name.
    Plain(PathBuf),
    /// A reference table reports its directly referenced parent.
    Reference(PathBuf),
    /// A concatenation reports its constituent tables in descriptor order.
    Concatenation(Vec<PathBuf>),
}

impl Table {
    /// Read the direct part names reported by casacore's `Table::getPartNames`.
    ///
    /// Read the descriptor at `path` and resolve relative links against the
    /// public `name`. This lets an already captured path remain authoritative
    /// for I/O while preserving symlinks in the reported names.
    ///
    /// # Errors
    ///
    /// The control file cannot be read or its descriptor is malformed.
    pub fn immediate_part_names(path: &Path, name: &Path) -> Result<TablePartNames, TableError> {
        let control = read_table_dat_dispatch(&path.join(crate::storage::TABLE_CONTROL_FILE))?;
        let name = table_cache_name(name)?;
        match control {
            TableDatResult::Plain(_) => Ok(TablePartNames::Plain(name)),
            TableDatResult::Ref(reference) => table_cache_name(&add_directory_for_link(
                &reference.parent_relative_path,
                &name,
            ))
            .map(TablePartNames::Reference),
            TableDatResult::Concat(concat) => {
                immediate_names(&name, &concat.table_paths).map(TablePartNames::Concatenation)
            }
        }
    }

    /// Read a concatenation's immediate table paths in its stored row order.
    /// Plain and reference tables return `None`. No table cells are opened.
    pub fn concatenated_source_paths(path: &Path) -> Result<Option<Vec<PathBuf>>, TableError> {
        let path = table_cache_name(path)?;
        let control = read_table_dat_dispatch(&path.join(crate::storage::TABLE_CONTROL_FILE))?;
        match control {
            TableDatResult::Concat(concat) => immediate_names(&path, &concat.table_paths).map(Some),
            _ => Ok(None),
        }
    }

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
