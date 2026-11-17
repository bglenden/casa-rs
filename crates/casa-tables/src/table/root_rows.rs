// SPDX-License-Identifier: LGPL-3.0-or-later
//! Row numbers in the root of a reference table chain.

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
}
