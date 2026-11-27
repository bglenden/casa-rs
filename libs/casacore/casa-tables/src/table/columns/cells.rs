// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

impl<'a> TableColumn<'a> {
    /// Returns the column name bound to this accessor.
    pub fn name(&self) -> &str {
        &self.column
    }

    /// Returns the number of rows in the underlying table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Returns a cell accessor for `row_index`.
    pub fn cell(&self, row_index: usize) -> Result<TableCell<'a>, TableError> {
        self.table.cell_accessor(row_index, &self.column)
    }

    /// Returns the cell value at `row_index`, or `None` if absent.
    pub fn get(&self, row_index: usize) -> Result<Option<&'a Value>, TableError> {
        self.table.cell(row_index, &self.column)
    }

    /// Returns an iterator over all rows in the column.
    pub fn iter(&self) -> Result<ColumnCellIter<'a>, TableError> {
        self.table.get_column(&self.column)
    }

    /// Returns an iterator over a row range in the column.
    pub fn iter_range(&self, row_range: RowRange) -> Result<ColumnCellIter<'a>, TableError> {
        self.table.get_column_range(&self.column, row_range)
    }

    /// Returns record cells over all rows in the column.
    pub fn record_iter(&self) -> Result<RecordColumnIter<'a>, TableError> {
        self.table.get_record_column(&self.column)
    }

    /// Returns record cells over `row_range`.
    pub fn record_iter_range(
        &self,
        row_range: RowRange,
    ) -> Result<RecordColumnIter<'a>, TableError> {
        self.table.get_record_column_range(&self.column, row_range)
    }

    /// Returns a chunked iterator over the column.
    pub fn chunks(
        &self,
        row_range: RowRange,
        chunk_size: usize,
    ) -> Result<ColumnChunkIter<'a>, TableError> {
        self.table
            .iter_column_chunks(&self.column, row_range, chunk_size)
    }

    /// Returns all cells for the column as an allocated vector.
    pub fn cells(&self) -> Result<Vec<Option<&'a Value>>, TableError> {
        self.table.column_cells(&self.column)
    }

    /// Returns the scalar cell at `row_index`.
    pub fn scalar_cell(&self, row_index: usize) -> Result<&'a ScalarValue, TableError> {
        self.table.get_scalar_cell(row_index, &self.column)
    }

    /// Returns the array cell at `row_index`.
    pub fn array_cell(&self, row_index: usize) -> Result<&'a ArrayValue, TableError> {
        self.table.get_array_cell(row_index, &self.column)
    }

    /// Returns the record cell at `row_index`.
    pub fn record_cell(&self, row_index: usize) -> Result<RecordValue, TableError> {
        self.table.record_cell(row_index, &self.column)
    }

    /// Returns owned scalar values for the column.
    pub fn scalar_cells_owned(&self) -> Result<Vec<Option<ScalarValue>>, TableError> {
        self.table.get_scalar_cells_owned(&self.column)
    }

    /// Returns owned scalar values for selected rows in this column.
    ///
    /// The output preserves the order of `row_indices`. Missing cells are
    /// returned as `None`.
    pub fn scalar_cells_owned_for_rows(
        &self,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ScalarValue>>, TableError> {
        self.table
            .get_scalar_cells_owned_for_rows(&self.column, row_indices)
    }

    /// Returns owned array values for the selected rows.
    pub fn array_cells_owned(
        &self,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        self.table.get_array_cells_owned(&self.column, row_indices)
    }

    /// Returns owned array values for selected rows without populating the
    /// table-level row cache.
    ///
    /// This is intended for bounded streaming scans where retaining each row
    /// defeats the caller's memory budget. The output preserves the order of
    /// `row_indices`. Missing cells are returned as `None`.
    pub fn array_cells_owned_uncached(
        &self,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        self.table
            .get_array_cells_owned_uncached(&self.column, row_indices)
    }

    /// Read a uniform two-dimensional column selection as one array with
    /// shape `[axis0, axis1, selected_rows]` in Fortran order.
    ///
    /// Row order and duplicates are preserved. Returns `None` when the
    /// storage manager cannot fill a packed buffer, the table has pending
    /// cell changes, or selected cells are empty, undefined, or differ in
    /// shape. The read bypasses the row cache.
    ///
    /// # Errors
    /// A selected row is out of bounds or the storage read fails.
    pub fn packed_2d_cells_uncached(
        &self,
        row_indices: &[usize],
    ) -> Result<Option<ArrayValue>, TableError> {
        self.read_packed_2d_cells(row_indices, false)
    }

    /// Read as in [`Self::packed_2d_cells_uncached`], but decode Complex32
    /// storage directly into a Complex64 result. Other supported element
    /// types retain their normal representation.
    ///
    /// # Errors
    /// A selected row is out of bounds or the storage read fails.
    pub fn packed_2d_cells_uncached_with_complex_promotion(
        &self,
        row_indices: &[usize],
    ) -> Result<Option<ArrayValue>, TableError> {
        self.read_packed_2d_cells(row_indices, true)
    }

    fn read_packed_2d_cells(
        &self,
        row_indices: &[usize],
        widen_complex: bool,
    ) -> Result<Option<ArrayValue>, TableError> {
        for &row in row_indices {
            self.table.require_row_index_without_loading_rows(row)?;
        }
        self.table
            .inner
            .packed_2d_cells_uncached(&self.column, row_indices, widen_complex)
    }

    /// Read whole 2-D cells of exactly `shape`, packed `[channel][row][axis0]`,
    /// without populating the table cache. This never truncates extra channels.
    ///
    /// StandardStMan indirect arrays grow a flat block after validating each
    /// stored shape; channel transposition or reordered rows require a second
    /// block. Other managers hold the selected whole cells while packing.
    /// Returns `None` for an unsupported layout, an
    /// undefined/differently-shaped cell, an empty selection/shape, or a table
    /// already materialized or holding pending writes. I/O errors propagate.
    pub fn array_cells_fixed_2d_typed_uncached(
        &self,
        row_indices: &[usize],
        shape: [usize; 2],
    ) -> Result<Option<SelectedArray2DCells>, TableError> {
        self.table.require_column(&self.column)?;
        for &row_index in row_indices {
            self.table
                .require_row_index_without_loading_rows(row_index)?;
        }
        self.table
            .inner
            .fixed_2d_cells_uncached(&self.column, row_indices, shape)
    }

    /// Returns typed 2-D array channel slices for selected rows without
    /// populating the table-level row cache.
    ///
    /// The returned values are packed as `[channel][row][axis0]`. Returns
    /// `None` when the column exists but all requested cells are undefined.
    pub fn array_cells_2d_channel_range_typed_uncached(
        &self,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Option<SelectedArray2DCells>, TableError> {
        self.table.get_array_cells_2d_channel_range_typed_uncached(
            &self.column,
            row_indices,
            channel_start,
            channel_count,
        )
    }

    /// Fill a caller-owned typed 2-D selected-channel vector without
    /// populating the table-level row cache.
    pub fn fill_array_cells_2d_channel_range_typed_uncached(
        &self,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
        destination: SelectedArray2DCellsMut<'_>,
    ) -> Result<Option<SelectedArray2DShape>, TableError> {
        self.table.fill_array_cells_2d_channel_range_typed_uncached(
            &self.column,
            row_indices,
            channel_start,
            channel_count,
            destination,
        )
    }

    /// Returns typed 1-D array cells for selected rows without populating the
    /// table-level row cache.
    ///
    /// The returned values are packed as `[row][axis0]`.
    pub fn array_cells_1d_typed_uncached(
        &self,
        row_indices: &[usize],
    ) -> Result<SelectedArray1DCells, TableError> {
        self.table
            .get_array_cells_1d_typed_uncached(&self.column, row_indices)
    }

    /// Fill a caller-owned typed 1-D selected-row vector without populating
    /// the table-level row cache.
    pub fn fill_array_cells_1d_typed_uncached(
        &self,
        row_indices: &[usize],
        destination: SelectedArray1DCellsMut<'_>,
    ) -> Result<SelectedArray1DShape, TableError> {
        self.table
            .fill_array_cells_1d_typed_uncached(&self.column, row_indices, destination)
    }
}

impl<'a> TableColumnMut<'a> {
    /// Returns the column name bound to this accessor.
    pub fn name(&self) -> &str {
        &self.column
    }

    /// Returns the number of rows in the underlying table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Returns a read-only cell accessor for `row_index`.
    pub fn cell(&self, row_index: usize) -> Result<TableCell<'_>, TableError> {
        self.table.cell_accessor(row_index, &self.column)
    }

    /// Returns a mutable cell accessor for `row_index`.
    pub fn cell_mut(&mut self, row_index: usize) -> Result<TableCellMut<'_>, TableError> {
        self.table.cell_accessor_mut(row_index, &self.column)
    }

    /// Returns an iterator over all rows in the column.
    pub fn iter(&self) -> Result<ColumnCellIter<'_>, TableError> {
        self.table.get_column(&self.column)
    }

    /// Returns an iterator over a row range in the column.
    pub fn iter_range(&self, row_range: RowRange) -> Result<ColumnCellIter<'_>, TableError> {
        self.table.get_column_range(&self.column, row_range)
    }

    /// Returns a chunked iterator over the column.
    pub fn chunks(
        &self,
        row_range: RowRange,
        chunk_size: usize,
    ) -> Result<ColumnChunkIter<'_>, TableError> {
        self.table
            .iter_column_chunks(&self.column, row_range, chunk_size)
    }

    /// Returns the scalar cell at `row_index`.
    pub fn scalar_cell(&self, row_index: usize) -> Result<&ScalarValue, TableError> {
        self.table.get_scalar_cell(row_index, &self.column)
    }

    /// Returns the array cell at `row_index`.
    pub fn array_cell(&self, row_index: usize) -> Result<&ArrayValue, TableError> {
        self.table.get_array_cell(row_index, &self.column)
    }

    /// Returns owned scalar values for the column.
    pub fn scalar_cells_owned(&self) -> Result<Vec<Option<ScalarValue>>, TableError> {
        self.table.get_scalar_cells_owned(&self.column)
    }

    /// Returns owned array values for the selected rows.
    pub fn array_cells_owned(
        &self,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        self.table.get_array_cells_owned(&self.column, row_indices)
    }

    /// Writes `value` to `row_index`.
    pub fn set(&mut self, row_index: usize, value: Value) -> Result<(), TableError> {
        self.table.set_cell(row_index, &self.column, value)
    }

    /// Writes a record value to `row_index`.
    pub fn set_record(&mut self, row_index: usize, value: RecordValue) -> Result<(), TableError> {
        self.table.set_record_cell(row_index, &self.column, value)
    }

    /// Writes a full column's values.
    pub fn put<I>(&mut self, values: I) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = Value>,
    {
        self.table.put_column(&self.column, values)
    }

    /// Writes values for `row_range`.
    pub fn put_range<I>(&mut self, row_range: RowRange, values: I) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = Value>,
    {
        self.table.put_column_range(&self.column, row_range, values)
    }
}

impl<'a> TableCell<'a> {
    /// Returns the row index bound to this accessor.
    pub fn row_index(&self) -> usize {
        self.row_index
    }

    /// Returns the column name bound to this accessor.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the cell value, or `None` if absent.
    pub fn value(&self) -> Result<Option<&'a Value>, TableError> {
        self.table.cell(self.row_index, &self.column)
    }

    /// Returns the cell as a scalar value.
    pub fn scalar(&self) -> Result<&'a ScalarValue, TableError> {
        self.table.get_scalar_cell(self.row_index, &self.column)
    }

    /// Returns the cell as an array value.
    pub fn array(&self) -> Result<&'a ArrayValue, TableError> {
        self.table.get_array_cell(self.row_index, &self.column)
    }

    /// Returns the cell as a record value.
    pub fn record(&self) -> Result<RecordValue, TableError> {
        self.table.record_cell(self.row_index, &self.column)
    }

    /// Returns whether the cell is defined.
    pub fn is_defined(&self) -> Result<bool, TableError> {
        self.table.is_cell_defined(self.row_index, &self.column)
    }

    /// Returns the array shape of the cell, if it is an array.
    pub fn array_shape(&self) -> Result<Option<Vec<usize>>, TableError> {
        self.table.array_shape(self.row_index, &self.column)
    }
}

impl<'a> TableCellMut<'a> {
    /// Returns the row index bound to this accessor.
    pub fn row_index(&self) -> usize {
        self.row_index
    }

    /// Returns the column name bound to this accessor.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the cell value, or `None` if absent.
    pub fn value(&self) -> Result<Option<&Value>, TableError> {
        self.table.cell(self.row_index, &self.column)
    }

    /// Returns the cell as a scalar value.
    pub fn scalar(&self) -> Result<&ScalarValue, TableError> {
        self.table.get_scalar_cell(self.row_index, &self.column)
    }

    /// Returns the cell as an array value.
    pub fn array(&self) -> Result<&ArrayValue, TableError> {
        self.table.get_array_cell(self.row_index, &self.column)
    }

    /// Returns the cell as a record value.
    pub fn record(&self) -> Result<RecordValue, TableError> {
        self.table.record_cell(self.row_index, &self.column)
    }

    /// Returns whether the cell is defined.
    pub fn is_defined(&self) -> Result<bool, TableError> {
        self.table.is_cell_defined(self.row_index, &self.column)
    }

    /// Returns the array shape of the cell, if it is an array.
    pub fn array_shape(&self) -> Result<Option<Vec<usize>>, TableError> {
        self.table.array_shape(self.row_index, &self.column)
    }

    /// Writes a new value to the cell.
    pub fn set(&mut self, value: Value) -> Result<(), TableError> {
        self.table.set_cell(self.row_index, &self.column, value)
    }

    /// Writes a record value to the cell.
    pub fn set_record(&mut self, value: RecordValue) -> Result<(), TableError> {
        self.table
            .set_record_cell(self.row_index, &self.column, value)
    }
}

// ── Schema validation helpers ─────────────────────────────────────────

pub(crate) fn validate_row_against_schema(
    row_index: usize,
    row: &RecordValue,
    schema: &TableSchema,
) -> Result<(), TableError> {
    for column in schema.columns() {
        validate_cell_against_schema_column(row_index, column, row.get(column.name()))?;
    }
    for field in row.fields() {
        if !schema.contains_column(&field.name) {
            return Err(TableError::RowContainsUnknownColumn {
                row_index,
                column: field.name.clone(),
            });
        }
    }
    Ok(())
}

pub(crate) fn collect_undefined_cells_for_schema(
    rows: &[RecordValue],
    schema: &TableSchema,
) -> Vec<HashSet<String>> {
    rows.iter()
        .map(|row| undefined_columns_for_row(row, schema))
        .collect()
}

/// The `Undefined` columns `row` leaves out. Record columns are never
/// among them: casacore's `ScalarRecordColumnData::isDefined` is always true,
/// and a missing record reads as an empty one, with or without the option.
pub(crate) fn undefined_columns_for_row(
    row: &RecordValue,
    schema: &TableSchema,
) -> HashSet<String> {
    schema
        .columns()
        .iter()
        .filter(|column| {
            column.options().undefined
                && !matches!(column.column_type(), ColumnType::Record)
                && row.get(column.name()).is_none()
        })
        .map(|column| column.name().to_string())
        .collect()
}

pub(crate) fn validate_cell_against_schema_column(
    row_index: usize,
    column: &ColumnSchema,
    value: Option<&Value>,
) -> Result<(), TableError> {
    match (column.column_type(), value) {
        (ColumnType::Scalar, Some(Value::Scalar(value))) => {
            validate_primitive_type(row_index, column, value.primitive_type())
        }
        (ColumnType::Scalar, Some(value)) => Err(TableError::ColumnTypeMismatch {
            row_index,
            column: column.name().to_string(),
            expected: "scalar",
            found: value.kind(),
        }),
        (ColumnType::Scalar, None) => {
            if column.options().undefined {
                Ok(())
            } else {
                Err(TableError::SchemaColumnMissing {
                    row_index,
                    column: column.name().to_string(),
                })
            }
        }
        (ColumnType::Record, Some(Value::Record(_))) => Ok(()),
        (ColumnType::Record, Some(value)) => Err(TableError::ColumnTypeMismatch {
            row_index,
            column: column.name().to_string(),
            expected: "record",
            found: value.kind(),
        }),
        (ColumnType::Record, None) => Ok(()),
        (ColumnType::Array(contract), Some(Value::Array(array))) => {
            validate_primitive_type(row_index, column, array.primitive_type())?;
            validate_array_contract(row_index, column.name(), contract, array)
        }
        (ColumnType::Array(_), Some(value)) => Err(TableError::ColumnTypeMismatch {
            row_index,
            column: column.name().to_string(),
            expected: "array",
            found: value.kind(),
        }),
        (ColumnType::Array(ArrayShapeContract::Fixed { .. }), None) => {
            Err(TableError::SchemaColumnMissing {
                row_index,
                column: column.name().to_string(),
            })
        }
        (ColumnType::Array(ArrayShapeContract::Variable { .. }), None) => Ok(()),
    }
}

pub(super) fn validate_primitive_type(
    row_index: usize,
    column: &ColumnSchema,
    found: PrimitiveType,
) -> Result<(), TableError> {
    let Some(expected) = column.data_type() else {
        return Ok(());
    };
    if found == expected {
        Ok(())
    } else {
        Err(TableError::PrimitiveTypeMismatch {
            row_index,
            column: column.name().to_string(),
            expected,
            found,
        })
    }
}

pub(super) fn validate_array_contract(
    row_index: usize,
    column_name: &str,
    contract: &ArrayShapeContract,
    array: &ArrayValue,
) -> Result<(), TableError> {
    match contract {
        ArrayShapeContract::Fixed { shape } => {
            let found = array.shape().to_vec();
            if found == *shape {
                Ok(())
            } else {
                Err(TableError::ArrayShapeMismatch {
                    row_index,
                    column: column_name.to_string(),
                    expected: shape.clone(),
                    found,
                })
            }
        }
        ArrayShapeContract::Variable {
            ndim: Some(expected),
        } => {
            let found = array.ndim();
            if found == *expected {
                Ok(())
            } else {
                Err(TableError::ArrayNdimMismatch {
                    row_index,
                    column: column_name.to_string(),
                    expected: *expected,
                    found,
                })
            }
        }
        ArrayShapeContract::Variable { ndim: None } => Ok(()),
    }
}
