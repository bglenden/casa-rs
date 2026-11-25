// SPDX-License-Identifier: LGPL-3.0-or-later
//! Resolve a column's lazy ASDM index through a saved table projection.

use super::{Table, TableError, TableOptions};
use crate::AsdmBdfIndex;
use crate::storage::TABLE_CONTROL_FILE;
use crate::storage::asdm_stman::TYPE;
use crate::storage::table_control::{TableDatResult, read_table_dat_dispatch};
use crate::table_path::add_directory_for_link;

impl Table {
    /// Find the index of observed visibility data backed by ASDM BDFs.
    ///
    /// A reference follows its visible column to the physical table. A
    /// concatenation uses its first member, as casacore's findDataManager
    /// does. DATA is preferred, followed by FLOAT_DATA when DATA is absent.
    ///
    /// # Errors
    /// The table has neither observed column, or its projection cannot be read.
    pub fn asdm_bdf_index(&self) -> Result<Option<AsdmBdfIndex>, TableError> {
        let column = if self.schema().is_some_and(|schema| {
            !schema.contains_column("DATA") && schema.contains_column("FLOAT_DATA")
        }) {
            "FLOAT_DATA"
        } else {
            "DATA"
        };
        self.require_column(column)?;
        let Some(source) = self.path() else {
            return Ok(None);
        };
        if !self.data_manager_info().is_empty() && !self.is_reference_table()? {
            return Ok(self.asdm_index_for_column(column));
        }
        let mut path = source.to_owned();
        let mut column = column.to_owned();
        for _ in 0..32 {
            match read_table_dat_dispatch(&path.join(TABLE_CONTROL_FILE))? {
                TableDatResult::Plain(_) => {
                    let root = Table::open(TableOptions::new(path))?;
                    return Ok(root.asdm_index_for_column(&column));
                }
                TableDatResult::Ref(reference) => {
                    column = reference.parent_column(&column).to_owned();
                    path = add_directory_for_link(&reference.parent_relative_path, &path);
                }
                TableDatResult::Concat(concat) => {
                    let Some(first) = concat.table_paths.first() else {
                        return Ok(None);
                    };
                    path = add_directory_for_link(first, &path);
                }
            }
        }
        Err(TableError::Storage(
            "ASDM storage manager path is too deep".into(),
        ))
    }

    fn asdm_index_for_column(&self, column: &str) -> Option<AsdmBdfIndex> {
        let path = self.path()?;
        self.data_manager_info()
            .iter()
            .find(|manager| {
                // On reopen, casacore constructs this manager with the
                // persisted type as its name. The column's declared group
                // can still describe the materialized storage layout.
                manager.dm_type == TYPE
                    && manager.columns.iter().any(|name| name == column)
            })
            .map(|manager| AsdmBdfIndex::new(path.to_owned(), manager.seq_nr))
    }
}
