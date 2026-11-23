// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row locations through reference and concatenation descriptors.

use std::path::PathBuf;

use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};
use crate::storage::{CompositeStorage, TABLE_CONTROL_FILE};
use crate::table_path::add_directory_for_link;

use super::{Table, TableError};

impl Table {
    /// Return each row's number in the root table, in view order. Repeated
    /// rows stay repeated, and mappings from nested references are composed.
    /// A plain, concatenated, or memory table is its own root.
    ///
    /// An unreadable parent, invalid mapping, or reference cycle is an error.
    /// This reads each reference's on-disk metadata; it does not read its cells.
    ///
    /// # C++ equivalent
    ///
    /// `Table::rowNumbers()`.
    pub fn root_row_numbers(&self) -> Result<Vec<u64>, TableError> {
        let mut rows: Vec<u64> = (0..self.row_count()).map(|row| row as u64).collect();
        let Some(mut path) = self.source_path.clone() else {
            return Ok(rows);
        };
        let mut seen = std::collections::HashSet::new();
        loop {
            let real = std::fs::canonicalize(&path)
                .map_err(|error| TableError::Storage(error.to_string()))?;
            if !seen.insert(real) {
                return Err(TableError::Storage("reference table cycle".into()));
            }
            let control = path.join(crate::storage::TABLE_CONTROL_FILE);
            match crate::storage::table_control::read_table_dat_dispatch(&control)? {
                crate::storage::table_control::TableDatResult::Ref(reference) => {
                    for row in &mut rows {
                        let index = usize::try_from(*row).map_err(|_| {
                            TableError::Storage("reference row is too large".into())
                        })?;
                        *row = *reference.row_map.get(index).ok_or_else(|| {
                            TableError::Storage(format!("reference row {index} is out of bounds"))
                        })?;
                    }
                    path = crate::table_path::add_directory_for_link(
                        &reference.parent_relative_path,
                        &path,
                    );
                }
                _ => return Ok(rows),
            }
        }
    }

    /// Find the physical table directory containing a logical row. This
    /// follows reference row maps and concatenated member boundaries without
    /// loading any cells. A memory table returns `None`.
    ///
    /// # Errors
    /// The view row is invalid or a source descriptor is unreadable or cyclic.
    pub fn physical_row_path(&self, row: usize) -> Result<Option<PathBuf>, TableError> {
        self.require_row_index_without_loading_rows(row)?;
        let Some(mut path) = self.source_path.clone() else {
            return Ok(None);
        };
        let mut row = row;
        for _ in 0..128 {
            match read_table_dat_dispatch(&path.join(TABLE_CONTROL_FILE))? {
                TableDatResult::Plain(_) => return Ok(Some(path)),
                TableDatResult::Ref(reference) => {
                    row = reference
                        .row_map
                        .get(row)
                        .and_then(|&index| usize::try_from(index).ok())
                        .ok_or_else(|| TableError::Storage("invalid reference row".into()))?;
                    path = add_directory_for_link(&reference.parent_relative_path, &path);
                }
                TableDatResult::Concat(concat) => {
                    let mut found = None;
                    for link in &concat.table_paths {
                        let member = add_directory_for_link(link, &path);
                        let count = CompositeStorage.load_metadata_only(&member)?.row_count;
                        if row < count {
                            found = Some(member);
                            break;
                        }
                        row -= count;
                    }
                    path = found.ok_or_else(|| {
                        TableError::Storage("invalid concatenated table row".into())
                    })?;
                }
            }
        }
        Err(TableError::Storage(
            "table row source chain is too deep".into(),
        ))
    }
}
