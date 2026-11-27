// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
        let Some(schema) = self.schema().filter(|_| self.path().is_some()) else {
            let rows = self.inner.rows()?;
            let undefined = self.inner.undefined_cells()?;
            return Ok(self.memory_copy(
                indices.iter().map(|&row| rows[row].clone()).collect(),
                indices.iter().map(|&row| undefined[row].clone()).collect(),
            ));
        };
        let mut rows = (0..indices.len())
            .map(|_| RecordValue::new(Vec::with_capacity(schema.columns().len())))
            .collect::<Vec<_>>();
        let mut undefined = vec![HashSet::new(); indices.len()];
        for column in schema.columns() {
            let name = column.name();
            let values = match column.column_type() {
                ColumnType::Scalar => self
                    .get_scalar_cells_owned_for_rows(name, indices)?
                    .into_iter()
                    .map(|value| value.map(Value::Scalar))
                    .collect(),
                ColumnType::Array(_) => self
                    .get_array_cells_owned_uncached(name, indices)?
                    .into_iter()
                    .map(|value| value.map(Value::Array))
                    .collect(),
                ColumnType::Record => indices
                    .iter()
                    .map(|&row| self.cell(row, name).map(|value| value.cloned()))
                    .collect::<Result<Vec<_>, _>>()?,
            };
            for ((row, undefined), value) in rows.iter_mut().zip(&mut undefined).zip(values) {
                if let Some(value) = value {
                    row.push(RecordField::new(name, value));
                } else if column.options().undefined
                    && !matches!(column.column_type(), ColumnType::Record)
                {
                    undefined.insert(name.to_owned());
                }
            }
        }
        Ok(self.memory_copy(rows, undefined))
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
}
