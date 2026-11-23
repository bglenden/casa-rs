// SPDX-License-Identifier: LGPL-3.0-or-later
//! A saved RefTable's lock and cell storage belong to its base table.

use super::*;
use crate::storage::{
    RefTableDatContents, TABLE_CONTROL_FILE, TABLE_INFO_FILE, TableDatResult,
    read_table_dat_dispatch,
};

/// Ordered RefTable descriptors from the view to its root. No cells are read
/// and the returned path is the one whose process lock covers the view.
fn reference_chain(
    mut path: PathBuf,
    lock_probe: bool,
) -> Result<(PathBuf, Vec<RefTableDatContents>), TableError> {
    let mut seen = std::collections::HashSet::new();
    let mut chain = Vec::new();
    loop {
        let control = path.join(TABLE_CONTROL_FILE);
        match crate::storage::table_control::is_reference_descriptor(&control) {
            // The descriptor of a plain base may be absent or incomplete
            // while its writer holds the lock. Wait at that directory and
            // defer validation to the locked open.
            Ok(false) if lock_probe || chain.is_empty() => return Ok((path, chain)),
            Err(_) if lock_probe => return Ok((path, chain)),
            Err(error) => return Err(error.into()),
            Ok(_) => {}
        }
        let real = std::fs::canonicalize(&path).map_err(crate::storage::StorageError::from)?;
        if !seen.insert(real) {
            return Err(TableError::Storage("reference table cycle".into()));
        }
        match read_table_dat_dispatch(&control)? {
            TableDatResult::Ref(reference) => {
                path = crate::table_path::add_directory_for_link(
                    &reference.parent_relative_path,
                    &path,
                );
                chain.push(reference);
            }
            TableDatResult::Plain(_) => return Ok((path, chain)),
            TableDatResult::Concat(_) if chain.is_empty() => return Ok((path, chain)),
            TableDatResult::Concat(_) => {
                return Err(TableError::Storage(
                    "reference base is a concatenated table".into(),
                ));
            }
        }
    }
}

pub(super) fn reference_lock_path(path: &Path) -> Result<PathBuf, TableError> {
    reference_chain(path.to_path_buf(), true).map(|(root, _)| root)
}

fn base_name<'a>(chain: &'a [RefTableDatContents], mut name: &'a str) -> &'a str {
    for view in chain {
        if let Some((_, column)) = view.column_name_map.iter().find(|(view, _)| view == name) {
            name = column;
        }
    }
    name
}

fn base_row(chain: &[RefTableDatContents], mut row: usize) -> Result<usize, TableError> {
    for view in chain {
        row = view
            .row_map
            .get(row)
            .and_then(|&row| usize::try_from(row).ok())
            .ok_or_else(|| TableError::Storage(format!("reference row {row} is out of bounds")))?;
    }
    Ok(row)
}

impl Table {
    pub(crate) fn reference_column_manager_type(
        &self,
        column: &str,
    ) -> Result<Option<String>, TableError> {
        if !self.is_reference_table()? {
            return Ok(None);
        }
        let source = self.source_path.as_ref().expect("saved reference");
        let (root, chain) = reference_chain(source.clone(), false)?;
        let root_table = Table::open_metadata_only(TableOptions::new(root))?;
        let name = base_name(&chain, column);
        Ok(root_table
            .data_manager_info()
            .iter()
            .find(|manager| manager.columns.iter().any(|column| column == name))
            .map(|manager| manager.dm_type.clone()))
    }

    /// Apply cell changes through the saved row/column projection under
    /// the view's already-held write lock. Return false for plain tables.
    pub(super) fn try_save_reference_cells(
        &mut self,
        columns: &[&str],
        changed_rows: &[usize],
    ) -> Result<bool, TableError> {
        if !self.is_reference_table()? {
            return Ok(false);
        }
        for column in columns {
            self.require_writable_column(column)?;
        }
        let source = self.source_path.as_ref().expect("saved reference");
        let (root, chain) = reference_chain(source.clone(), false)?;
        self.check_reference_base(&root)?;
        let mut base = Table::open(TableOptions::new(&root))?;
        let mut selected_columns: Vec<_> =
            columns.iter().map(|col| base_name(&chain, col)).collect();
        selected_columns.sort_unstable();
        selected_columns.dedup();
        let changed_rows = if changed_rows.is_empty() {
            (0..self.row_count()).collect::<Vec<_>>()
        } else {
            changed_rows.to_vec()
        };
        let mut selected_rows = Vec::new();
        for &row in &changed_rows {
            let index = base_row(&chain, row)?;
            for &column in columns {
                let name = base_name(&chain, column);
                let descriptor = self
                    .schema()
                    .and_then(|schema| schema.column(column))
                    .ok_or_else(|| TableError::SchemaColumnUnknown {
                        column: column.to_string(),
                    })?;
                // Mutated scalar/array cells are in the overlay. Read
                // those directly instead of loading the parent rows.
                match descriptor.column_type() {
                    crate::ColumnType::Scalar => match self.get_scalar_cell(row, column) {
                        Ok(value) => base.write_scalar_cell_bound(index, name, value.clone())?,
                        Err(TableError::ColumnNotFound { .. }) => {}
                        Err(error) => return Err(error),
                    },
                    crate::ColumnType::Array(_) => match self.get_array_cell(row, column) {
                        Ok(value) => base.write_array_cell_bound(index, name, value.clone())?,
                        Err(TableError::ColumnNotFound { .. }) => {}
                        Err(error) => return Err(error),
                    },
                    crate::ColumnType::Record => {
                        base.set_cell(index, name, Value::Record(self.record_cell(row, column)?))?;
                    }
                }
            }
            selected_rows.push(index);
        }
        selected_rows.sort_unstable();
        selected_rows.dedup();
        self.note_persisted_write(&root);
        let result = base.persist_selected_rows_in_place(&selected_columns, &selected_rows);
        if matches!(&result, Err(TableError::Storage(message)) if message.starts_with("partial in-place save does not support"))
            && let Some(options) = base.single_rewrite_options()?
        {
            base.save(options)?;
        } else {
            result?;
        }
        let mut changed_rows = changed_rows;
        changed_rows.sort_unstable();
        self.inner.note_selected_cells_saved(columns, &changed_rows);
        Ok(true)
    }

    fn check_reference_base(&self, base: &Path) -> Result<(), TableError> {
        #[cfg(unix)]
        if let Some(lock) = &self.lock_state
            && !locking::same_directory(&lock.path, base)
        {
            return Err(TableError::Storage(
                "the saved reference now points to a different base; reopen it".into(),
            ));
        }
        #[cfg(not(unix))]
        let _ = base;
        Ok(())
    }

    /// RefTable keywords (table and column) belong to the base table.
    /// TableInfo is stored beside the view's descriptor.
    pub(super) fn try_save_reference_metadata(
        &self,
        destination: &Path,
    ) -> Result<bool, TableError> {
        if !self.is_reference_table()?
            || !self
                .source_path
                .as_deref()
                .is_some_and(|path| locking::same_directory(path, destination))
        {
            return Ok(false);
        }
        let (root, chain) = reference_chain(destination.to_path_buf(), false)?;
        self.check_reference_base(&root)?;
        let mut table = Table::open(TableOptions::new(&root))?;
        *table.keywords_mut() = self.keywords().clone();
        for column in &chain[0].column_names {
            table.set_column_keywords(
                base_name(&chain, column),
                self.column_keywords(column).cloned().unwrap_or_default(),
            );
        }
        self.note_persisted_write(&root);
        table.save_metadata_only(TableOptions::new(&root))?;
        std::fs::write(destination.join(TABLE_INFO_FILE), self.info().to_string())
            .map_err(crate::storage::StorageError::from)?;
        self.inner.note_metadata_saved();
        Ok(true)
    }
}
