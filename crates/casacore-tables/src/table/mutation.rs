// SPDX-License-Identifier: LGPL-3.0-or-later
use super::columns::{validate_cell_against_schema_column, validate_row_against_schema};
use super::*;

impl Table {
    // -------------------------------------------------------------------
    // Row copy and fill utilities
    // -------------------------------------------------------------------

    /// Copies all rows from `source` into this table.
    ///
    /// Both tables must share the same schema (same column names and types).
    /// Rows are appended after any existing rows.
    ///
    /// # Errors
    ///
    /// - [`TableError::SchemaColumnUnknown`] if schemas are incompatible
    ///
    /// # C++ equivalent
    ///
    /// `TableCopy::copyRows(target, source)`.
    pub fn copy_rows(&mut self, source: &Table) -> Result<(), TableError> {
        self.validate_schema_compat(source)?;
        for row in source.rows()? {
            self.add_row(row.clone())?;
        }
        Ok(())
    }

    /// Copies selected rows from `source` into this table.
    ///
    /// `row_indices` specifies which source rows to copy, in order.
    /// Both tables must share the same schema.
    ///
    /// # Errors
    ///
    /// - [`TableError::SchemaColumnUnknown`] if schemas are incompatible
    /// - [`TableError::RowOutOfBounds`] if a row index exceeds source row count
    ///
    /// # C++ equivalent
    ///
    /// `TableCopy::copyRows(target, source, ..., rowMap)`.
    pub fn copy_rows_with_mapping(
        &mut self,
        source: &Table,
        row_indices: &[usize],
    ) -> Result<(), TableError> {
        self.validate_schema_compat(source)?;
        let src_rows = source.rows()?;
        let src_count = src_rows.len();
        for &idx in row_indices {
            let row = src_rows.get(idx).ok_or(TableError::RowOutOfBounds {
                row_index: idx,
                row_count: src_count,
            })?;
            self.add_row(row.clone())?;
        }
        Ok(())
    }

    /// Copies [`TableInfo`] metadata from `source` to this table.
    ///
    /// # C++ equivalent
    ///
    /// `TableCopy::copyInfo(target, source)`.
    pub fn copy_info(&mut self, source: &Table) {
        self.table_info = source.table_info.clone();
    }

    /// Sets every cell in `column` to `value`.
    ///
    /// # Errors
    ///
    /// - [`TableError::SchemaColumnUnknown`] if `column` does not exist
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::fillColumn(value)` / `ScalarColumn<T>::fillColumn(value)`.
    pub fn fill_column(&mut self, column: &str, value: Value) -> Result<(), TableError> {
        let n = self.row_count();
        for row_idx in 0..n {
            self.set_cell(row_idx, column, value.clone())?;
        }
        Ok(())
    }

    /// Sets cells in `column` within `row_range` to `value`.
    ///
    /// Rows outside the range are unchanged.
    ///
    /// # Errors
    ///
    /// - [`TableError::SchemaColumnUnknown`] if `column` does not exist
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::putColumnRange(rowRange, value)`.
    pub fn fill_column_range(
        &mut self,
        column: &str,
        row_range: RowRange,
        value: Value,
    ) -> Result<(), TableError> {
        for row_idx in row_range.iter() {
            if row_idx >= self.row_count() {
                break;
            }
            self.set_cell(row_idx, column, value.clone())?;
        }
        Ok(())
    }

    /// Validates that `source` has a compatible schema with `self`.
    fn validate_schema_compat(&self, source: &Table) -> Result<(), TableError> {
        let self_schema = self.schema();
        let src_schema = source.schema();
        match (self_schema, src_schema) {
            (Some(s), Some(o)) => {
                if s != o {
                    return Err(TableError::SchemaMismatch {
                        message: format!("expected {s:?}, found {o:?}"),
                    });
                }
            }
            (None, None) => {}
            _ => {
                return Err(TableError::SchemaMismatch {
                    message: format!("expected {self_schema:?}, found {src_schema:?}"),
                });
            }
        }
        Ok(())
    }

    /// Adds a column to the table schema.
    ///
    /// If `default` is `Some`, existing rows are populated with that value. If
    /// `None`, the column must allow absent values (e.g. `undefined: true` for
    /// scalars, variable-shape arrays, or record columns); otherwise an error
    /// is returned.
    ///
    /// A [`TableSchema`] must be attached to the table. Returns an error if the
    /// column name already exists or if the default value does not match the
    /// column type.
    ///
    /// C++ equivalent: `Table::addColumn` / `TableDesc::addColumn`.
    pub fn add_column(
        &mut self,
        col: ColumnSchema,
        default: Option<Value>,
    ) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("add_column")?;
        let result = (|| {
            let schema = self.inner.schema().ok_or_else(|| {
                TableError::Schema("schema required for column operations".into())
            })?;
            // Verify the column is not already present (add_column checks this too,
            // but we need the schema borrow released before mutating).
            if schema.contains_column(col.name()) {
                return Err(SchemaError::DuplicateColumn(col.name().to_string()).into());
            }

            // Validate the default value against the new column schema.
            validate_cell_against_schema_column(0, &col, default.as_ref())?;

            // Schema mutation (must re-borrow mutably via set_schema round-trip
            // because TableImpl stores Option<TableSchema>).
            let mut schema = self.inner.schema().cloned().unwrap();
            schema.add_column(col.clone())?;
            self.inner.set_schema(Some(schema));

            // Populate existing rows with the default value.
            if let Some(value) = default {
                for row in self.inner.rows_mut()? {
                    row.push(RecordField::new(col.name(), value.clone()));
                }
            }
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Removes a column from the table schema, all rows, and column keywords.
    ///
    /// Returns an error if no schema is attached or the column does not exist
    /// in the schema.
    ///
    /// C++ equivalent: `Table::removeColumn`.
    pub fn remove_column(&mut self, name: &str) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("remove_column")?;
        let result = (|| {
            self.inner.schema().ok_or_else(|| {
                TableError::Schema("schema required for column operations".into())
            })?;

            let mut schema = self.inner.schema().cloned().unwrap();
            schema.remove_column(name)?;
            self.inner.set_schema(Some(schema));

            for row in self.inner.rows_mut()? {
                row.remove(name);
            }
            for set in self.inner.undefined_cells_mut()? {
                set.remove(name);
            }
            self.inner.remove_column_keywords(name);
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Renames a column in the table schema, all rows, and column keywords.
    ///
    /// Returns an error if no schema is attached, `old` does not exist, or
    /// `new` already exists.
    ///
    /// C++ equivalent: `Table::renameColumn`.
    pub fn rename_column(&mut self, old: &str, new: &str) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("rename_column")?;
        let result = (|| {
            self.inner.schema().ok_or_else(|| {
                TableError::Schema("schema required for column operations".into())
            })?;

            let mut schema = self.inner.schema().cloned().unwrap();
            schema.rename_column(old, new)?;
            self.inner.set_schema(Some(schema));

            for row in self.inner.rows_mut()? {
                row.rename_field(old, new);
            }
            for set in self.inner.undefined_cells_mut()? {
                if set.remove(old) {
                    set.insert(new.to_string());
                }
            }
            self.inner.rename_column_keywords(old, new.to_string());
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Removes rows at the given indices.
    ///
    /// The `indices` slice must be sorted in ascending order with no
    /// duplicates; each index must be less than [`row_count`](Table::row_count).
    /// Returns an error if any index is out of bounds or the slice is
    /// not sorted/unique.
    ///
    /// C++ equivalent: `Table::removeRow`.
    pub fn remove_rows(&mut self, indices: &[usize]) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("remove_rows")?;
        let result = (|| {
            let row_count = self.row_count();
            // Validate sorted, unique, and in bounds.
            for (i, &idx) in indices.iter().enumerate() {
                if idx >= row_count {
                    return Err(TableError::RowOutOfBounds {
                        row_index: idx,
                        row_count,
                    });
                }
                if i > 0 && idx <= indices[i - 1] {
                    return Err(TableError::InvalidRowRange {
                        start: indices[i - 1],
                        end: idx,
                        row_count,
                    });
                }
            }
            // Remove in reverse order to preserve earlier indices.
            for &idx in indices.iter().rev() {
                let _ = self.inner.remove_row(idx)?;
            }
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Inserts a row at the given position.
    ///
    /// Index `0` inserts before the first row; [`row_count`](Table::row_count)
    /// appends at the end (equivalent to [`add_row`](Table::add_row)).
    /// If a schema is attached, the row is validated against it.
    ///
    /// C++ equivalent: constructing rows and adding them to a `Table`.
    pub fn insert_row(&mut self, index: usize, row: RecordValue) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("insert_row")?;
        let result = (|| {
            let row_count = self.row_count();
            if index > row_count {
                return Err(TableError::RowOutOfBounds {
                    row_index: index,
                    row_count,
                });
            }
            if let Some(schema) = self.inner.schema() {
                validate_row_against_schema(index, &row, schema)?;
            }
            self.inner.insert_row(index, row)?;
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }
}
