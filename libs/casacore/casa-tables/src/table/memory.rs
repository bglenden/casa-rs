// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::storage::table_control::{TableDatContents, TableDatResult, read_table_dat_dispatch};
use crate::storage::virtual_engine::is_virtual_engine;

impl Table {
    /// Create an independent in-memory copy of this table. Rows, schema and
    /// keywords are copied. The source path and lock state are discarded.
    ///
    /// C++ equivalent: `Table::copyToMemoryTable`.
    pub fn to_memory(&self) -> Result<Self, TableError> {
        Ok(self.memory_copy(
            self.inner.rows()?.to_vec(),
            self.inner.undefined_cells()?.to_vec(),
        ))
    }

    /// Copy the given rows and table metadata into an independent memory
    /// table. The result preserves selection order and repeated row indices.
    /// Where available, read projected columns through their storage managers
    /// without loading all the rows of the source table.
    ///
    /// # Errors
    /// A row lies outside the source or a selected value could not be read.
    pub fn to_memory_rows(&self, indices: &[usize]) -> Result<Self, TableError> {
        for &index in indices {
            self.require_row_index_without_loading_rows(index)?;
        }
        if indices.is_empty() {
            return Ok(self.memory_copy(Vec::new(), Vec::new()));
        }
        let Some(schema) = self.schema() else {
            return self.memory_copy_from_rows(indices);
        };
        if self.path().is_none()
            || self.inner.has_loaded_rows()
            || schema
                .columns()
                .iter()
                .any(|col| col.column_type() == &ColumnType::Record)
            || !self.can_project_copy()?
        {
            return self.memory_copy_from_rows(indices);
        }
        let mut rows = (0..indices.len())
            .map(|_| RecordValue::new(Vec::with_capacity(schema.columns().len())))
            .collect::<Vec<_>>();
        let mut undefined = vec![HashSet::new(); indices.len()];
        for column in schema.columns() {
            let name = column.name();
            let values: Vec<Option<Value>> = match column.column_type() {
                ColumnType::Scalar => self
                    .inner
                    .selected_scalar_cells(indices, name, column.options().undefined)?
                    .ok_or_else(|| TableError::Storage("table has no selected scalar rows".into()))?
                    .into_iter()
                    .map(|value| value.map(Value::Scalar))
                    .collect(),
                ColumnType::Array(_) => self
                    .get_array_cells_owned_uncached(name, indices)?
                    .into_iter()
                    .map(|value| value.map(Value::Array))
                    .collect(),
                ColumnType::Record => unreachable!("record columns use the stored rows"),
            };
            for ((row, undefined), value) in rows.iter_mut().zip(&mut undefined).zip(values) {
                if let Some(value) = value {
                    row.push(RecordField::new(name, value));
                } else if column.options().undefined {
                    undefined.insert(name.to_owned());
                }
            }
        }
        Ok(self.memory_copy(rows, undefined))
    }

    fn memory_copy_from_rows(&self, indices: &[usize]) -> Result<Self, TableError> {
        let source = self.inner.rows()?;
        let source_undefined = self.inner.undefined_cells()?;
        let mut rows = Vec::with_capacity(indices.len());
        let mut undefined = Vec::with_capacity(indices.len());
        for &row_index in indices {
            let row = source.get(row_index).ok_or(TableError::RowOutOfBounds {
                row_index,
                row_count: source.len(),
            })?;
            rows.push(row.clone());
            undefined.push(source_undefined[row_index].clone());
        }
        Ok(self.memory_copy(rows, undefined))
    }

    fn can_project_copy(&self) -> Result<bool, TableError> {
        if let Some(descriptor) = self.inner.retained_copy_descriptor() {
            return Ok(can_project_plain_copy(descriptor));
        }
        for path in self.cache_plain_table_paths()? {
            if let TableDatResult::Plain(descriptor) =
                read_table_dat_dispatch(&path.join("table.dat"))?
                && !can_project_plain_copy(&descriptor)
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn memory_copy(&self, rows: Vec<RecordValue>, undefined: Vec<HashSet<String>>) -> Self {
        Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                rows,
                undefined,
                self.inner.keywords().clone(),
                self.inner.all_column_keywords().clone(),
                self.inner.schema().cloned(),
            ),
            source_path: None,
            kind: TableKind::Memory,
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: self.table_info.clone(),
            dm_info: vec![],
            external_sync: None,
            measures: self.measures.clone(),
            marked_for_delete: false,
            #[cfg(unix)]
            lock_state: None,
        }
    }
}

fn can_project_plain_copy(descriptor: &TableDatContents) -> bool {
    // Most virtual engines need a full snapshot of their backing columns.
    // The compressed complex reader can decode only the selected rows.
    descriptor.column_set.data_managers.iter().all(|manager| {
        let name = manager.type_name.as_str();
        !is_virtual_engine(name) || matches!(name, "CompressComplex" | "CompressComplexSD")
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selected_memory_rows_keep_order_duplicates_and_undefined_cells() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::array_variable("samples", PrimitiveType::Int32, Some(1)),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        for index in 0..12 {
            let mut row = RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(index)),
            )]);
            if index != 2 {
                let samples = ndarray::array![index, index + 1].into_dyn();
                row.push(RecordField::new(
                    "samples",
                    Value::Array(ArrayValue::Int32(samples)),
                ));
            }
            table.add_row(row).unwrap();
        }
        let disk = tempfile::tempdir().unwrap();
        table.save(TableOptions::new(disk.path())).unwrap();
        let saved = Table::open(TableOptions::new(disk.path())).unwrap();
        let selection = [7, 2, 0, 7];
        let copied = saved.to_memory_rows(&selection).unwrap();
        assert!(!saved.inner.has_loaded_rows());
        assert!(copied.is_memory() && copied.path().is_none());
        for (destination, source) in selection.into_iter().enumerate() {
            assert_eq!(copied.row(destination).unwrap(), table.row(source).unwrap());
        }
        assert!(!copied.is_cell_defined(1, "samples").unwrap());
        assert!(saved.to_memory_rows(&[12]).is_err());
    }

    #[test]
    fn selected_memory_rows_report_missing_construction_rows() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let disk = tempfile::tempdir().unwrap();
        let options = TableOptions::new(disk.path());
        Table::with_schema(schema).save(options.clone()).unwrap();
        let construction = Table::open_metadata_for_construction(options, 1).unwrap();
        assert!(matches!(
            construction.to_memory_rows(&[0]),
            Err(TableError::RowOutOfBounds {
                row_index: 0,
                row_count: 0
            })
        ));
    }

    #[test]
    fn selected_memory_rows_share_one_materialization_across_virtual_columns() {
        let schema = TableSchema::new(
            ["id", "first", "second"]
                .into_iter()
                .map(|name| ColumnSchema::scalar(name, PrimitiveType::Int64))
                .collect(),
        )
        .unwrap();
        let mut table = Table::with_schema(schema);
        for id in [7, 14] {
            table
                .add_row(RecordValue::new(
                    [("id", id), ("first", 0), ("second", 0)]
                        .into_iter()
                        .map(|(name, value)| {
                            RecordField::new(name, Value::Scalar(ScalarValue::Int64(value)))
                        })
                        .collect(),
                ))
                .unwrap();
        }
        table.bind_taql_column("first", "id + 1").unwrap();
        table.bind_taql_column("second", "id * 3").unwrap();
        let disk = tempfile::tempdir().unwrap();
        let options = TableOptions::new(disk.path());
        table.save(options.clone()).unwrap();
        let saved = Table::open(options).unwrap();
        assert!(!saved.inner.has_loaded_rows());
        let copied = saved.to_memory_rows(&[1, 0, 1]).unwrap();
        assert!(saved.inner.has_loaded_rows());
        for (row, (first, second)) in [(15, 42), (8, 21), (15, 42)].into_iter().enumerate() {
            for (name, value) in [("first", first), ("second", second)] {
                assert_eq!(
                    copied.cell(row, name).unwrap(),
                    Some(&Value::Scalar(ScalarValue::Int64(value)))
                );
            }
        }
    }
}
