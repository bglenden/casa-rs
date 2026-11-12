// SPDX-License-Identifier: LGPL-3.0-or-later
//! Payload-free array-cell metadata through the ordinary manager bindings.
use super::table_control::{ColumnDescContents, DataManagerEntry};
use super::*;

struct Binding<'a> {
    manager: &'a DataManagerEntry,
    columns: Vec<&'a ColumnDescContents>,
    column: usize,
}

impl<'a> Binding<'a> {
    fn of(table: &'a TableDatContents, name: &str) -> Result<Self, StorageError> {
        let descriptor = table
            .table_desc
            .columns
            .iter()
            .position(|column| column.col_name == name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!("array column '{name}' not found"))
            })?;
        if !table.table_desc.columns[descriptor].is_array {
            return Err(StorageError::FormatMismatch(format!(
                "column '{name}' is not an array column"
            )));
        }
        let sequence = table
            .column_set
            .columns
            .iter()
            .find(|column| column.original_name == name)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{name}' missing ColumnSet binding"
                ))
            })?
            .dm_seq_nr;
        let manager = table
            .column_set
            .data_managers
            .iter()
            .find(|manager| manager.seq_nr == sequence)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{name}' missing data manager {sequence}"
                ))
            })?;
        let bindings = table
            .column_set
            .columns
            .iter()
            .enumerate()
            .filter(|(_, binding)| binding.dm_seq_nr == sequence)
            .collect::<Vec<_>>();
        let column = bindings
            .iter()
            .position(|(index, _)| *index == descriptor)
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "array column '{name}' missing data-manager column binding"
                ))
            })?;
        let columns = bindings
            .into_iter()
            .map(|(index, _)| &table.table_desc.columns[index])
            .collect();
        Ok(Self {
            manager,
            columns,
            column,
        })
    }
}

impl CompositeStorage {
    pub(crate) fn plain_array_cell_shapes_uncached(
        &self,
        table_path: &Path,
        metadata: &RetainedTableReadMetadata,
        column: &str,
        rows: &[usize],
    ) -> Result<Vec<Option<Vec<usize>>>, StorageError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        let binding = Binding::of(&metadata.table_dat, column)?;
        let manager = binding.manager;
        match manager.type_name.as_str() {
            "StandardStMan" => {
                let data = table_path.join(format!("{TABLE_DATA_FILE_PREFIX}{}", manager.seq_nr));
                standard_stman::read_ssm_array_column_shapes(
                    &data,
                    &manager.data,
                    &binding.columns,
                    binding.column,
                    rows,
                )?
                .ok_or_else(|| {
                    StorageError::FormatMismatch(format!(
                        "metadata-only shapes unavailable for SSM column '{column}'"
                    ))
                })
            }
            "TiledColumnStMan" | "TiledShapeStMan" | "TiledCellStMan" | "TiledDataStMan" => {
                metadata.tiled.array_shapes(
                    manager.seq_nr,
                    &binding.columns,
                    &metadata.table_dat.table_desc,
                    binding.column,
                    rows,
                )
            }
            other => Err(StorageError::UnsupportedDataManager(format!(
                "metadata-only array shapes for {other}"
            ))),
        }
    }

    pub(crate) fn plain_array_cell_is_defined_uncached(
        &self,
        table: &TableDatContents,
        metadata: &TiledReadMetadata,
        column: &str,
        row: usize,
    ) -> Result<bool, StorageError> {
        let binding = Binding::of(table, column)?;
        if binding.manager.type_name != "TiledShapeStMan" {
            return Err(StorageError::FormatMismatch(format!(
                "metadata-only array-cell definedness for column '{column}' requires TiledShapeStMan, found {}",
                binding.manager.type_name
            )));
        }
        metadata.shape_cell_is_defined(binding.manager.seq_nr, binding.column, row)
    }
}
