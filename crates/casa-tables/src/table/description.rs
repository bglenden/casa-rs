// SPDX-License-Identifier: LGPL-3.0-or-later
//! The declared metadata retained in an on-disk table description.

use casa_types::RecordValue;
use std::path::PathBuf;

use super::{Table, TableError};

/// The parts of a plain table's saved description not represented by its
/// column schema: each column's declared manager and the private keywords.
/// A manager entry is `(column name, manager type, manager group)`. These are
/// declarations and can differ from the manager actually bound to a column.
#[derive(Debug, Clone)]
pub struct StoredTableDescription {
    /// The declared manager type and group, in table-description order.
    pub declared_managers: Vec<(String, String, String)>,
    /// The table-description private keywords, including hypercolumns.
    pub private_keywords: RecordValue,
}

impl Table {
    /// Find the root of a saved reference that retains every column under its
    /// original name and has no private descriptor keywords. `None` means the
    /// table is not such a reference. Invalid or cyclic chains are errors.
    /// This bounded query lets a caller reuse the root's description without
    /// assuming how projected or renamed columns affect hypercolumns.
    pub fn simple_reference_root(&self) -> Result<Option<PathBuf>, TableError> {
        let Some(mut path) = self.source_path.clone() else {
            return Ok(None);
        };
        let mut seen = std::collections::HashSet::new();
        let mut columns: Option<Vec<String>> = None;
        loop {
            let real = std::fs::canonicalize(&path)
                .map_err(|error| TableError::Storage(error.to_string()))?;
            if !seen.insert(real) {
                return Err(TableError::Storage("reference table cycle".into()));
            }
            match crate::storage::table_control::read_table_dat_dispatch(
                &path.join(crate::storage::TABLE_CONTROL_FILE),
            )? {
                crate::storage::table_control::TableDatResult::Ref(reference) => {
                    if reference
                        .column_name_map
                        .iter()
                        .any(|(view, parent)| view != parent)
                    {
                        return Ok(None);
                    }
                    let mut names = reference.column_names;
                    names.sort();
                    if columns.as_ref().is_some_and(|previous| previous != &names) {
                        return Ok(None);
                    }
                    columns = Some(names);
                    path = crate::table_path::add_directory_for_link(
                        &reference.parent_relative_path,
                        &path,
                    );
                }
                crate::storage::table_control::TableDatResult::Plain(table) => {
                    let Some(columns) = columns else {
                        return Ok(None);
                    };
                    let mut names: Vec<_> = table
                        .table_desc
                        .columns
                        .into_iter()
                        .map(|column| column.col_name)
                        .collect();
                    names.sort();
                    return Ok((columns == names
                        && table.table_desc.private_keywords.fields().is_empty())
                    .then_some(path));
                }
                _ => return Ok(None),
            }
        }
    }

    /// Read the declared managers and private keywords from a saved plain
    /// table's descriptor. Memory, reference, and concatenated tables return
    /// `None`; an unreadable or invalid descriptor returns an error. No table
    /// cells are read. Manager declarations can differ from the runtime
    /// bindings reported by [`Table::data_manager_info`].
    pub fn stored_table_description(&self) -> Result<Option<StoredTableDescription>, TableError> {
        let Some(path) = self.source_path.as_ref() else {
            return Ok(None);
        };
        let control = path.join(crate::storage::TABLE_CONTROL_FILE);
        let crate::storage::table_control::TableDatResult::Plain(table) =
            crate::storage::table_control::read_table_dat_dispatch(&control)?
        else {
            return Ok(None);
        };
        Ok(Some(StoredTableDescription {
            declared_managers: table
                .table_desc
                .columns
                .into_iter()
                .map(|column| {
                    (
                        column.col_name,
                        column.data_manager_type,
                        column.data_manager_group,
                    )
                })
                .collect(),
            private_keywords: table.table_desc.private_keywords,
        }))
    }
}
