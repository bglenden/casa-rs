// SPDX-License-Identifier: LGPL-3.0-or-later
//! Table paths referenced by a table's on-disk metadata.

use std::collections::{BTreeSet, HashSet};
use std::path::{Component, Path, PathBuf};

use super::{Table, TableError, TableOptions};
use crate::storage::table_control::{RefTableDatContents, TableDatResult, read_table_dat_dispatch};
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
    depth: usize,
    include_descriptors: bool,
    record: &mut impl FnMut(&Path) -> Result<(), TableError>,
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
    if include_descriptors {
        record(path)?;
    }
    match read_table_dat_dispatch(&path.join(crate::storage::TABLE_CONTROL_FILE))? {
        TableDatResult::Plain(_) => {
            if !include_descriptors {
                record(path)?;
            }
        }
        TableDatResult::Ref(reference) => {
            visit(
                &add_directory_for_link(&reference.parent_relative_path, path),
                active,
                depth + 1,
                include_descriptors,
                record,
            )?;
        }
        TableDatResult::Concat(concat) => {
            for part in &concat.table_paths {
                visit(
                    &add_directory_for_link(part, path),
                    active,
                    depth + 1,
                    include_descriptors,
                    record,
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
    Concatenation {
        /// The immediate member tables.
        parts: Vec<PathBuf>,
        /// Subtable keywords whose values concatenate the same-named
        /// subtables of all members. Other keywords come from the first part.
        subtable_names: Vec<String>,
    },
}

/// The persisted row and column projection of a saved reference onto a plain
/// table. Keep it only while the reference and parent publications are current.
#[doc(hidden)]
pub struct PlainReferenceRead {
    parent: Table,
    reference: RefTableDatContents,
}

impl PlainReferenceRead {
    /// The open physical parent of this view.
    pub fn parent(&self) -> &Table {
        &self.parent
    }

    /// Translate logical rows to parent rows in order, preserving duplicates.
    pub fn parent_rows(&self, rows: &[usize]) -> Result<Vec<usize>, TableError> {
        self.reference.parent_rows(rows).map_err(TableError::from)
    }

    /// Find the physical column when the view exposes the logical column.
    pub fn parent_column<'a>(&'a self, column: &'a str) -> Result<&'a str, TableError> {
        if !self
            .reference
            .column_names
            .iter()
            .any(|name| name == column)
        {
            return Err(TableError::SchemaColumnUnknown {
                column: column.into(),
            });
        }
        Ok(self.reference.parent_column(column))
    }
}

impl Table {
    /// Drop cached cells on an unmodified lazy read handle while preserving
    /// the parsed descriptor. The caller must first check that the table
    /// layout and row count are current. When this returns false, reopen the
    /// table to read fresh cells.
    #[doc(hidden)]
    pub fn try_clear_read_cells(&mut self) -> bool {
        // These storage managers read payloads using the current data files
        // and do not retain a separate per-file index in the table handle.
        if !self.dm_info.iter().all(|manager| {
            matches!(
                manager.dm_type.as_str(),
                "StandardStMan" | "IncrementalStMan" | "StManAipsIO"
            )
        }) {
            return false;
        }
        self.inner.try_clear_read_cells()
    }

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
            TableDatResult::Concat(concat) => Ok(TablePartNames::Concatenation {
                parts: immediate_names(&name, &concat.table_paths)?,
                subtable_names: concat.sub_table_names,
            }),
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

    /// Open the physical parent and projection of a direct on-disk reference
    /// whose parent is plain. Other layouts return `None`.
    ///
    /// # Errors
    /// A required descriptor is missing or cannot be decoded.
    #[doc(hidden)]
    pub fn open_plain_reference(path: &Path) -> Result<Option<PlainReferenceRead>, TableError> {
        let control = crate::storage::TABLE_CONTROL_FILE;
        let TableDatResult::Ref(reference) = read_table_dat_dispatch(&path.join(control))? else {
            return Ok(None);
        };
        let parent_path = add_directory_for_link(&reference.parent_relative_path, path);
        if !matches!(
            read_table_dat_dispatch(&parent_path.join(control))?,
            TableDatResult::Plain(_)
        ) {
            return Ok(None);
        }
        let parent = Self::open(TableOptions::new(parent_path))?;
        Ok(Some(PlainReferenceRead { parent, reference }))
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
        visit(path, &mut HashSet::new(), 0, false, &mut |path| {
            paths.insert(table_cache_name(path)?);
            Ok(())
        })?;
        Ok(paths.into_iter().collect())
    }

    /// Open a table and refresh the tile cache across its persisted layout.
    ///
    /// The callback runs before reading each path's descriptor so a caller
    /// can capture publication stamps for the full metadata dependency chain.
    /// Relative links retain their on-disk interpretation and paths shared by
    /// multiple branches can be visited more than once. Cells remain lazy.
    ///
    /// # Errors
    /// A path is missing, cyclic, too deeply nested, or has invalid metadata.
    #[doc(hidden)]
    pub fn open_with_layout_paths(
        options: TableOptions,
        mut record: impl FnMut(&Path),
    ) -> Result<Self, TableError> {
        visit(options.path(), &mut HashSet::new(), 0, true, &mut |path| {
            record(path);
            if path != options.path() {
                crate::storage::tiled_stman::invalidate_shared_tile_cache_for_table(path);
            }
            Ok(())
        })?;
        Self::open(options)
    }
}
