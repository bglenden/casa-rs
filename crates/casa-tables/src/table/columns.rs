// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::HashSet;

use super::*;

fn compile_prepared_row_slots(
    table: &Table,
    columns: &[&str],
) -> Result<
    (
        Vec<PreparedRowSlot>,
        std::collections::HashMap<String, usize>,
    ),
    TableError,
> {
    let schema = table
        .schema()
        .ok_or(TableError::PreparedRowRequiresSchema)?;
    let mut slots = Vec::with_capacity(columns.len());
    let mut column_indices = std::collections::HashMap::with_capacity(columns.len());

    for &column in columns {
        table.require_column(column)?;
        let schema_column = schema
            .columns()
            .iter()
            .find(|candidate| candidate.name() == column)
            .ok_or_else(|| TableError::SchemaColumnUnknown {
                column: column.to_string(),
            })?;
        let kind = match schema_column.column_type() {
            ColumnType::Scalar => PreparedRowSlotKind::Scalar,
            ColumnType::Array(_) => PreparedRowSlotKind::Array,
            ColumnType::Record => {
                return Err(TableError::PreparedRowRecordColumnUnsupported {
                    column: column.to_string(),
                });
            }
        };
        let slot_index = slots.len();
        slots.push(PreparedRowSlot {
            column: column.to_string(),
            kind,
        });
        column_indices
            .entry(column.to_string())
            .or_insert(slot_index);
    }

    Ok((slots, column_indices))
}

fn placeholder_value(kind: PreparedRowSlotKind) -> Value {
    match kind {
        PreparedRowSlotKind::Scalar => Value::Scalar(ScalarValue::Bool(false)),
        PreparedRowSlotKind::Array => Value::Array(ArrayValue::from_bool_vec(Vec::new())),
    }
}

fn prepared_row_record(slots: &[PreparedRowSlot]) -> RecordValue {
    RecordValue::new(
        slots
            .iter()
            .map(|slot| RecordField::new(slot.column.clone(), placeholder_value(slot.kind)))
            .collect(),
    )
}

fn load_prepared_row_value(
    table: &Table,
    row_index: usize,
    slot: &PreparedRowSlot,
) -> Result<Value, TableError> {
    match slot.kind {
        PreparedRowSlotKind::Scalar => table
            .get_scalar_cell(row_index, &slot.column)
            .map(|value| Value::Scalar(value.clone())),
        PreparedRowSlotKind::Array => table
            .get_array_cell(row_index, &slot.column)
            .map(|value| Value::Array(value.clone())),
    }
}

fn fill_prepared_row_buffer(
    table: &Table,
    slots: &[PreparedRowSlot],
    row: &mut RecordValue,
    row_index: usize,
) -> Result<(), TableError> {
    let fields = row.fields_mut();
    for (slot, field) in slots.iter().zip(fields.iter_mut()) {
        field.value = load_prepared_row_value(table, row_index, slot)?;
    }
    Ok(())
}

fn current_prepared_row_index(row_index: Option<usize>) -> Result<usize, TableError> {
    row_index.ok_or(TableError::RowOutOfBounds {
        row_index: 0,
        row_count: 0,
    })
}

fn flush_prepared_row_buffer(
    table: &mut Table,
    slots: &[PreparedRowSlot],
    row: &RecordValue,
    row_index: usize,
) -> Result<(), TableError> {
    for (slot, field) in slots.iter().zip(row.fields().iter()) {
        match (slot.kind, &field.value) {
            (PreparedRowSlotKind::Scalar, Value::Scalar(value)) => {
                table.set_scalar_cell_assuming_valid(row_index, &slot.column, value.clone())?;
            }
            (PreparedRowSlotKind::Array, Value::Array(value)) => {
                table.set_array_cell_assuming_valid(row_index, &slot.column, value.clone())?;
            }
            (PreparedRowSlotKind::Scalar, value) => {
                return Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: slot.column.clone(),
                    expected: "scalar",
                    found: value.kind(),
                });
            }
            (PreparedRowSlotKind::Array, value) => {
                return Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: slot.column.clone(),
                    expected: "array",
                    found: value.kind(),
                });
            }
        }
    }
    Ok(())
}

impl Table {
    /// Returns the attached schema, if any.
    ///
    /// When a schema is present, all row and cell mutations are validated
    /// against it. Returns `None` for schema-free tables.
    pub fn schema(&self) -> Option<&TableSchema> {
        self.inner.schema()
    }

    /// Attaches or replaces the schema, validating all existing rows.
    ///
    /// If the new schema is incompatible with any existing row the schema is
    /// not updated and the original is restored. Returns the first
    /// [`TableError`] encountered during validation.
    pub fn set_schema(&mut self, schema: TableSchema) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("set_schema")?;
        let result = (|| {
            let previous = self.inner.schema().cloned();
            self.inner.set_schema(Some(schema));
            if let Err(err) = self.validate() {
                self.inner.set_schema(previous);
                return Err(err);
            }
            let undefined =
                collect_undefined_cells_for_schema(self.rows()?, self.schema().unwrap());
            self.inner
                .undefined_cells_mut()?
                .clone_from_slice(&undefined);
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Removes the attached schema, disabling per-mutation validation.
    ///
    /// Existing row data is preserved unchanged.
    pub fn clear_schema(&mut self) {
        self.inner.set_schema(None);
    }

    /// Validates all rows against the attached schema.
    ///
    /// Returns `Ok(())` immediately when no schema is attached. Otherwise,
    /// checks every cell in every row and returns the first [`TableError`]
    /// encountered. This is called automatically by [`save`][Table::save],
    /// [`open`][Table::open], and schema-setting methods.
    pub fn validate(&self) -> Result<(), TableError> {
        let Some(schema) = self.schema() else {
            return Ok(());
        };

        for (row_index, row) in self.rows()?.iter().enumerate() {
            validate_row_against_schema(row_index, row, schema)?;
        }
        Ok(())
    }

    /// Returns the number of rows in the table.
    pub fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    /// Returns the canonical read-only row accessor for this table.
    pub fn row_accessor(&self) -> TableRow<'_> {
        TableRow { table: self }
    }

    /// Returns the canonical mutable row accessor for this table.
    pub fn row_accessor_mut(&mut self) -> TableRowMut<'_> {
        TableRowMut { table: self }
    }

    /// Returns the canonical read-only column accessor for `column`.
    pub fn column_accessor(&self, column: &str) -> Result<TableColumn<'_>, TableError> {
        self.require_column(column)?;
        Ok(TableColumn {
            table: self,
            column: column.to_string(),
        })
    }

    /// Returns the canonical mutable column accessor for `column`.
    pub fn column_accessor_mut(&mut self, column: &str) -> Result<TableColumnMut<'_>, TableError> {
        self.require_column(column)?;
        Ok(TableColumnMut {
            table: self,
            column: column.to_string(),
        })
    }

    /// Returns the canonical read-only cell accessor for `(row_index, column)`.
    pub fn cell_accessor(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<TableCell<'_>, TableError> {
        self.require_row(row_index)?;
        self.require_column(column)?;
        Ok(TableCell {
            table: self,
            row_index,
            column: column.to_string(),
        })
    }

    /// Returns the canonical mutable cell accessor for `(row_index, column)`.
    pub fn cell_accessor_mut(
        &mut self,
        row_index: usize,
        column: &str,
    ) -> Result<TableCellMut<'_>, TableError> {
        self.require_row(row_index)?;
        self.require_column(column)?;
        Ok(TableCellMut {
            table: self,
            row_index,
            column: column.to_string(),
        })
    }

    /// Returns a slice over all rows in insertion order.
    pub fn rows(&self) -> Result<&[RecordValue], TableError> {
        self.inner.rows()
    }

    /// Returns per-row sets of column names that are explicitly undefined.
    pub fn undefined_cells(&self) -> Result<&[std::collections::HashSet<String>], TableError> {
        self.inner.undefined_cells()
    }

    /// Appends a row to the table.
    ///
    /// If a schema is attached, the row is validated before insertion.
    /// Returns [`TableError`] if the row violates the schema; the table is
    /// left unchanged in that case.
    pub fn add_row(&mut self, row: RecordValue) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("add_row")?;
        let result = (|| {
            let mut undefined = None;
            if let Some(schema) = self.schema() {
                validate_row_against_schema(self.row_count(), &row, schema)?;
                undefined = Some(undefined_columns_for_row(&row, schema));
            }
            self.inner.add_row(row)?;
            if let Some(undefined) = undefined {
                if let Some(set) = self.inner.undefined_for_row_mut(self.row_count() - 1)? {
                    *set = undefined;
                }
            }
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Appends a row without re-validating it against the attached schema.
    ///
    /// This is intended for advanced callers that already know `row` matches
    /// the current schema because it was synthesized directly from the schema
    /// or validated earlier in the same write path. Undefined scalar-column
    /// tracking is still updated when a schema is attached.
    ///
    /// Callers that are not certain the row is schema-valid should keep using
    /// [`add_row`](Table::add_row).
    pub fn add_row_assuming_valid(&mut self, row: RecordValue) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("add_row_assuming_valid")?;
        let result = (|| {
            let undefined = self
                .schema()
                .map(|schema| undefined_columns_for_row(&row, schema));
            self.inner.add_row(row)?;
            if let Some(undefined) = undefined
                && let Some(set) = self.inner.undefined_for_row_mut(self.row_count() - 1)?
            {
                *set = undefined;
            }
            Ok(())
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Appends rows without re-validating them against the attached schema.
    ///
    /// This is the bulk counterpart to [`add_row_assuming_valid`](Table::add_row_assuming_valid)
    /// for writers that already have schema-compatible records and need to
    /// avoid per-row write-operation overhead.
    pub fn add_rows_assuming_valid<I>(&mut self, rows: I) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = RecordValue>,
    {
        let auto_unlock = self.begin_write_operation("add_rows_assuming_valid")?;
        let result = (|| {
            let schema = self.schema().cloned();
            let mut count = 0usize;
            for row in rows {
                let undefined = schema
                    .as_ref()
                    .map(|schema| undefined_columns_for_row(&row, schema));
                self.inner.add_row(row)?;
                if let Some(undefined) = undefined
                    && let Some(set) = self.inner.undefined_for_row_mut(self.row_count() - 1)?
                {
                    *set = undefined;
                }
                count += 1;
            }
            Ok(count)
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Returns a shared reference to the row at `row_index`.
    ///
    /// Compatibility note: new row-oriented code should prefer
    /// [`row_accessor`](Table::row_accessor).
    pub(crate) fn row(&self, row_index: usize) -> Result<&RecordValue, TableError> {
        self.inner
            .row(row_index)?
            .ok_or(TableError::RowOutOfBounds {
                row_index,
                row_count: self.row_count(),
            })
    }

    /// Returns an exclusive reference to the row at `row_index`.
    ///
    /// Direct mutation through this reference bypasses schema validation.
    /// Use [`set_cell`][Table::set_cell] or [`add_row`][Table::add_row] for
    /// validated writes.
    ///
    /// Compatibility note: new row-oriented write paths should prefer
    /// [`row_accessor_mut`](Table::row_accessor_mut).
    pub(crate) fn row_mut(&mut self, row_index: usize) -> Result<&mut RecordValue, TableError> {
        let row_count = self.row_count();
        self.inner
            .row_mut(row_index)?
            .ok_or(TableError::RowOutOfBounds {
                row_index,
                row_count,
            })
    }

    /// Returns a reference to the value at `(row_index, column)`, or `None` if absent.
    ///
    /// Returns `None` both when `row_index` is out of bounds and when the
    /// column is simply absent from the row. Use [`get_scalar_cell`][Table::get_scalar_cell]
    /// or [`get_array_cell`][Table::get_array_cell] for type-checked access with
    /// descriptive errors.
    ///
    /// Compatibility note: new cell-oriented code should prefer
    /// [`cell_accessor`](Table::cell_accessor).
    pub(crate) fn cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<Option<&Value>, TableError> {
        Ok(self.row(row_index)?.get(column))
    }

    /// Returns an iterator over every cell in `column`, covering all rows.
    ///
    /// This is a shorthand for `get_column_range(column, RowRange::new(0, row_count()))`.
    /// Returns [`TableError::SchemaColumnUnknown`] if a schema is attached and
    /// `column` is not declared in it.
    ///
    /// Compatibility note: new column-oriented code should prefer
    /// [`column_accessor`](Table::column_accessor).
    pub(crate) fn get_column<'a>(&'a self, column: &str) -> Result<ColumnCellIter<'a>, TableError> {
        self.get_column_range(column, RowRange::new(0, self.row_count()))
    }

    /// Returns an iterator over the cells in `column` within `row_range`.
    ///
    /// The iterator borrows the table's row data and yields one
    /// [`ColumnCellRef`] per selected row. Returns [`TableError`] if the
    /// column is unknown or the range is invalid.
    pub(crate) fn get_column_range<'a>(
        &'a self,
        column: &str,
        row_range: RowRange,
    ) -> Result<ColumnCellIter<'a>, TableError> {
        self.require_column(column)?;
        row_range.validate(self.row_count())?;
        Ok(ColumnCellIter {
            row_data: self.rows()?,
            column: column.to_string(),
            rows: row_range.iter(),
        })
    }

    /// Returns the record value at `(row_index, column)`.
    ///
    /// When a schema is attached and the column is declared as a record column,
    /// a missing cell is treated as an empty [`RecordValue`]. Without a schema,
    /// an absent cell returns [`TableError::ColumnNotFound`].
    ///
    /// Returns [`TableError::SchemaColumnNotRecord`] if the schema declares the
    /// column with a non-record type.
    pub fn record_cell(&self, row_index: usize, column: &str) -> Result<RecordValue, TableError> {
        self.require_row(row_index)?;
        if let Some(schema) = self.schema() {
            let column_schema =
                schema
                    .column(column)
                    .ok_or_else(|| TableError::SchemaColumnUnknown {
                        column: column.to_string(),
                    })?;
            if !matches!(column_schema.column_type(), ColumnType::Record) {
                return Err(TableError::SchemaColumnNotRecord {
                    column: column.to_string(),
                });
            }
            return match self.cell(row_index, column)? {
                Some(Value::Record(record)) => Ok(record.clone()),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: column.to_string(),
                    expected: "record",
                    found: value.kind(),
                }),
                None => Ok(RecordValue::default()),
            };
        }

        match self.cell(row_index, column)? {
            Some(Value::Record(record)) => Ok(record.clone()),
            Some(value) => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: column.to_string(),
                expected: "record",
                found: value.kind(),
            }),
            None => Err(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }
    }

    /// Writes a record value to `(row_index, column)`.
    ///
    /// This is a convenience wrapper around [`set_cell`][Table::set_cell] that
    /// wraps `value` in [`Value::Record`].
    ///
    /// Compatibility note: new cell-oriented write paths should prefer
    /// [`cell_accessor_mut`](Table::cell_accessor_mut).
    pub(crate) fn set_record_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: RecordValue,
    ) -> Result<(), TableError> {
        self.set_cell(row_index, column, Value::Record(value))
    }

    /// Returns an iterator over every record cell in `column`, covering all rows.
    ///
    /// Shorthand for `get_record_column_range(column, RowRange::new(0, row_count()))`.
    pub(crate) fn get_record_column<'a>(
        &'a self,
        column: &str,
    ) -> Result<RecordColumnIter<'a>, TableError> {
        self.get_record_column_range(column, RowRange::new(0, self.row_count()))
    }

    /// Returns an iterator over the record cells in `column` within `row_range`.
    ///
    /// Each item is a [`RecordColumnCell`] whose `value` is always populated:
    /// absent cells are defaulted to an empty [`RecordValue`] when the schema
    /// permits it. Returns [`TableError`] if the column is unknown, not a
    /// record column, or a cell has the wrong type.
    pub(crate) fn get_record_column_range<'a>(
        &'a self,
        column: &str,
        row_range: RowRange,
    ) -> Result<RecordColumnIter<'a>, TableError> {
        self.require_column(column)?;
        row_range.validate(self.row_count())?;

        let default_missing = if let Some(schema) = self.schema() {
            let column_schema =
                schema
                    .column(column)
                    .ok_or_else(|| TableError::SchemaColumnUnknown {
                        column: column.to_string(),
                    })?;
            if !matches!(column_schema.column_type(), ColumnType::Record) {
                return Err(TableError::SchemaColumnNotRecord {
                    column: column.to_string(),
                });
            }
            true
        } else {
            false
        };

        let row_data = self.rows()?;
        for row_index in row_range.iter() {
            match row_data[row_index].get(column) {
                Some(Value::Record(_)) => {}
                Some(value) => {
                    return Err(TableError::ColumnTypeMismatch {
                        row_index,
                        column: column.to_string(),
                        expected: "record",
                        found: value.kind(),
                    });
                }
                None => {
                    if !default_missing {
                        return Err(TableError::ColumnNotFound {
                            row_index,
                            column: column.to_string(),
                        });
                    }
                }
            }
        }

        Ok(RecordColumnIter {
            row_data,
            column: column.to_string(),
            rows: row_range.iter(),
            default_missing,
        })
    }

    /// Writes values from an iterator into `column` for all rows.
    ///
    /// Shorthand for `put_column_range(column, RowRange::new(0, row_count()), values)`.
    /// Returns the number of cells written, or [`TableError`] if the value
    /// count does not match the row count.
    ///
    /// Compatibility note: new column-oriented write paths should prefer
    /// [`column_accessor_mut`](Table::column_accessor_mut).
    pub(crate) fn put_column<I>(&mut self, column: &str, values: I) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = Value>,
    {
        self.put_column_range(column, RowRange::new(0, self.row_count()), values)
    }

    /// Writes values from an iterator into `column` for the rows in `row_range`.
    ///
    /// The iterator must produce exactly as many values as there are rows in
    /// `row_range`; otherwise [`TableError::ColumnWriteTooFewValues`] or
    /// [`TableError::ColumnWriteTooManyValues`] is returned. Returns the
    /// number of cells written on success.
    pub(crate) fn put_column_range<I>(
        &mut self,
        column: &str,
        row_range: RowRange,
        values: I,
    ) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = Value>,
    {
        let auto_unlock = self.begin_write_operation("put_column_range")?;
        let result = (|| {
            self.require_column(column)?;
            row_range.validate(self.row_count())?;

            let expected = row_range.len();
            let row_iter = row_range.iter();
            let mut value_iter = values.into_iter();
            let mut provided = 0usize;
            for row_index in row_iter {
                let Some(value) = value_iter.next() else {
                    return Err(TableError::ColumnWriteTooFewValues { expected, provided });
                };
                self.set_cell_impl(row_index, column, value)?;
                provided += 1;
            }
            if value_iter.next().is_some() {
                return Err(TableError::ColumnWriteTooManyValues { expected });
            }
            Ok(provided)
        })();
        self.finish_write_operation(auto_unlock, result)
    }

    /// Returns `true` if the cell at `(row_index, column)` is considered defined.
    ///
    /// A cell is defined if a value is present in the row. For record columns
    /// with a schema, a missing cell is still considered defined because it
    /// defaults to an empty record. Returns [`TableError`] if `row_index` is
    /// out of bounds or the column is unknown per the schema.
    pub fn is_cell_defined(&self, row_index: usize, column: &str) -> Result<bool, TableError> {
        self.require_row(row_index)?;
        self.require_column(column)?;
        if let Some(undefined) = self
            .inner
            .undefined_cells()?
            .get(row_index)
            .map(|set| set.contains(column))
        {
            if undefined {
                return Ok(false);
            }
        }
        if self.cell(row_index, column)?.is_some() {
            return Ok(true);
        }
        if let Some(schema) = self.schema()
            && matches!(
                schema.column(column).map(ColumnSchema::column_type),
                Some(ColumnType::Record)
            )
        {
            return Ok(true);
        }
        Ok(false)
    }

    /// Returns the shape of the array at `(row_index, column)`, or `None` if absent.
    ///
    /// Returns [`TableError::ColumnTypeMismatch`] if the cell is present but
    /// is not an array value.
    pub fn array_shape(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<Option<Vec<usize>>, TableError> {
        self.require_row(row_index)?;
        self.require_column(column)?;
        match self.cell(row_index, column)? {
            None => Ok(None),
            Some(Value::Array(array)) => Ok(Some(array.shape().to_vec())),
            Some(value) => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: column.to_string(),
                expected: "array",
                found: value.kind(),
            }),
        }
    }

    /// Writes `value` to the cell at `(row_index, column)`.
    ///
    /// When a schema is attached the value is validated against the column
    /// schema before writing; new column names are allowed only if they are
    /// declared in the schema. Without a schema the column must already exist
    /// in the row (use [`add_row`][Table::add_row] to populate new rows first).
    ///
    /// Returns [`TableError`] if the row is out of bounds, the column is
    /// unknown per the schema, or the value violates the column type/shape.
    ///
    /// Compatibility note: new cell-oriented write paths should prefer
    /// [`cell_accessor_mut`](Table::cell_accessor_mut).
    pub(crate) fn set_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: Value,
    ) -> Result<(), TableError> {
        let auto_unlock = self.begin_write_operation("set_cell")?;
        let result = self.set_cell_impl(row_index, column, value);
        self.finish_write_operation(auto_unlock, result)
    }

    fn set_cell_impl(
        &mut self,
        row_index: usize,
        column: &str,
        value: Value,
    ) -> Result<(), TableError> {
        let schema_column = if let Some(schema) = self.schema() {
            Some(
                schema
                    .column(column)
                    .ok_or_else(|| TableError::SchemaColumnUnknown {
                        column: column.to_string(),
                    })?
                    .clone(),
            )
        } else {
            None
        };
        if let Some(column_schema) = &schema_column {
            validate_cell_against_schema_column(row_index, column_schema, Some(&value))?;
        }

        {
            if let Some(set) = self.inner.undefined_for_row_mut(row_index)? {
                set.remove(column);
            }
        }

        let row = self.row_mut(row_index)?;

        if schema_column.is_some() {
            row.upsert(column.to_string(), value);
            return Ok(());
        }

        let target = row
            .get_mut(column)
            .ok_or_else(|| TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            })?;
        *target = value;
        Ok(())
    }

    /// Returns all cell values for `column` as an allocated `Vec`.
    ///
    /// This materializes the entire column into memory. For large tables,
    /// prefer [`Table::column_accessor`](Table::column_accessor) with
    /// [`TableColumn::iter`] or [`TableColumn::chunks`] to stream lazily.
    ///
    /// Compatibility note: new column-oriented code should prefer
    /// [`column_accessor`](Table::column_accessor).
    pub(crate) fn column_cells(&self, column: &str) -> Result<Vec<Option<&Value>>, TableError> {
        Ok(self
            .rows()?
            .iter()
            .map(|record| record.get(column))
            .collect())
    }

    /// Returns a chunked iterator over a column's cells.
    ///
    /// Each iteration yields a `Vec<ColumnCellRef>` of up to `chunk_size` rows.
    /// This avoids materializing the entire column at once while still allowing
    /// batch processing.
    ///
    /// Compatibility note: new column-oriented code should prefer
    /// [`column_accessor`](Table::column_accessor).
    pub(crate) fn iter_column_chunks<'a>(
        &'a self,
        column: &str,
        row_range: RowRange,
        chunk_size: usize,
    ) -> Result<ColumnChunkIter<'a>, TableError> {
        let inner = self.get_column_range(column, row_range)?;
        Ok(ColumnChunkIter {
            inner,
            chunk_size: chunk_size.max(1),
        })
    }

    /// Returns a reference to the array value in a cell without cloning.
    ///
    /// Use ndarray's slicing on the returned `ArrayValue` for sub-array access.
    ///
    /// Compatibility note: new cell-oriented code should prefer
    /// [`cell_accessor`](Table::cell_accessor).
    pub(crate) fn get_array_cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<&ArrayValue, TableError> {
        self.require_column(column)?;
        if row_index >= self.row_count() {
            return Err(TableError::RowOutOfBounds {
                row_index,
                row_count: self.row_count(),
            });
        }
        match self.inner.array_cell(row_index, column)? {
            crate::table_impl::LazyArrayLookup::Hit(array) => return Ok(array),
            crate::table_impl::LazyArrayLookup::Missing => {
                return Err(TableError::ColumnNotFound {
                    row_index,
                    column: column.to_string(),
                });
            }
            crate::table_impl::LazyArrayLookup::Unknown => {}
        }
        match self.cell(row_index, column)? {
            Some(Value::Array(array)) => Ok(array),
            Some(value) => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: column.to_string(),
                expected: "array",
                found: value.kind(),
            }),
            None => Err(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }
    }

    /// Returns owned array values for the selected rows in `column`.
    ///
    /// The output preserves the order of `row_indices`. Missing cells are
    /// returned as `None`.
    ///
    /// Compatibility note: new column-oriented code should prefer
    /// [`column_accessor`](Table::column_accessor).
    pub(crate) fn get_array_cells_owned(
        &self,
        column: &str,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        if let Some(values) = self.inner.array_cells_owned(row_indices, column)? {
            return Ok(values);
        }
        row_indices
            .iter()
            .map(|&row_index| match self.cell(row_index, column)? {
                Some(Value::Array(array)) => Ok(Some(array.clone())),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: column.to_string(),
                    expected: "array",
                    found: value.kind(),
                }),
                None => Ok(None),
            })
            .collect()
    }

    pub(crate) fn get_array_cells_owned_uncached(
        &self,
        column: &str,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        if let Some(values) = self.inner.array_cells_owned_uncached(row_indices, column)? {
            return Ok(values);
        }
        row_indices
            .iter()
            .map(|&row_index| match self.cell(row_index, column)? {
                Some(Value::Array(array)) => Ok(Some(array.clone())),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: column.to_string(),
                    expected: "array",
                    found: value.kind(),
                }),
                None => Ok(None),
            })
            .collect()
    }

    pub(crate) fn get_array_cells_2d_channel_range_owned_uncached(
        &self,
        column: &str,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        if let Some(values) = self.inner.array_cells_2d_channel_range_owned_uncached(
            row_indices,
            column,
            channel_start,
            channel_count,
        )? {
            return Ok(values);
        }
        row_indices
            .iter()
            .map(|&row_index| match self.cell(row_index, column)? {
                Some(Value::Array(array)) => crate::storage::slice_array_value_2d_channel_range(
                    array.clone(),
                    channel_start,
                    channel_count,
                )
                .map(Some)
                .map_err(|error| TableError::Storage(error.to_string())),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: column.to_string(),
                    expected: "array",
                    found: value.kind(),
                }),
                None => Ok(None),
            })
            .collect()
    }

    /// Returns owned scalar values for every row in `column`.
    ///
    /// Missing cells are returned as `None`.
    ///
    /// Compatibility note: new column-oriented code should prefer
    /// [`column_accessor`](Table::column_accessor).
    pub(crate) fn get_scalar_cells_owned(
        &self,
        column: &str,
    ) -> Result<Vec<Option<ScalarValue>>, TableError> {
        self.require_column(column)?;
        if let Some(values) = self.inner.scalar_cells_owned(column)? {
            return Ok(values);
        }
        (0..self.row_count())
            .map(|row_index| match self.cell(row_index, column)? {
                Some(Value::Scalar(scalar)) => Ok(Some(scalar.clone())),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: column.to_string(),
                    expected: "scalar",
                    found: value.kind(),
                }),
                None => Ok(None),
            })
            .collect()
    }

    pub(crate) fn get_scalar_cells_owned_for_rows(
        &self,
        column: &str,
        row_indices: &[usize],
    ) -> Result<Vec<Option<ScalarValue>>, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        if let Some(values) = self
            .inner
            .scalar_cells_owned_for_rows(row_indices, column)?
        {
            return Ok(values);
        }
        row_indices
            .iter()
            .map(|&row_index| match self.cell(row_index, column)? {
                Some(Value::Scalar(scalar)) => Ok(Some(scalar.clone())),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: column.to_string(),
                    expected: "scalar",
                    found: value.kind(),
                }),
                None => Ok(None),
            })
            .collect()
    }

    /// Returns a reference to the scalar value in a cell without cloning.
    ///
    /// Compatibility note: new cell-oriented code should prefer
    /// [`cell_accessor`](Table::cell_accessor).
    pub(crate) fn get_scalar_cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<&ScalarValue, TableError> {
        self.require_column(column)?;
        if row_index >= self.row_count() {
            return Err(TableError::RowOutOfBounds {
                row_index,
                row_count: self.row_count(),
            });
        }
        match self.inner.scalar_cell(row_index, column)? {
            crate::table_impl::LazyScalarLookup::Hit(scalar) => return Ok(scalar),
            crate::table_impl::LazyScalarLookup::Missing => {
                return Err(TableError::ColumnNotFound {
                    row_index,
                    column: column.to_string(),
                });
            }
            crate::table_impl::LazyScalarLookup::Unknown => {}
        }
        match self.cell(row_index, column)? {
            Some(Value::Scalar(scalar)) => Ok(scalar),
            Some(value) => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: column.to_string(),
                expected: "scalar",
                found: value.kind(),
            }),
            None => Err(TableError::ColumnNotFound {
                row_index,
                column: column.to_string(),
            }),
        }
    }

    /// Updates a scalar cell while preserving lazy column-backed state when possible.
    ///
    /// This is an advanced path for callers that already know the replacement
    /// value satisfies the schema. When the table is still in lazy mode, it
    /// updates the cached scalar column rather than forcing full-row
    /// materialization. If rows are already loaded, it mutates the row in
    /// memory directly.
    ///
    /// Compatibility note: new write paths should prefer
    /// [`cell_accessor_mut`](Table::cell_accessor_mut) or
    /// [`column_accessor_mut`](Table::column_accessor_mut).
    pub(crate) fn set_scalar_cell_assuming_valid(
        &mut self,
        row_index: usize,
        column: &str,
        value: ScalarValue,
    ) -> Result<(), TableError> {
        self.require_column(column)?;
        self.require_row_index_without_loading_rows(row_index)?;
        let Some(value) = self
            .inner
            .set_cached_scalar_cell(row_index, column, value)?
        else {
            return Ok(());
        };
        self.set_cell_impl(row_index, column, Value::Scalar(value))
    }

    /// Updates an array cell while preserving lazy column-backed state when possible.
    ///
    /// This is an advanced path for callers that already know the replacement
    /// value satisfies the schema. When the table is still in lazy mode, it
    /// updates the cached array column rather than forcing full-row
    /// materialization. If rows are already loaded, it mutates the row in
    /// memory directly.
    ///
    /// Compatibility note: new write paths should prefer
    /// [`cell_accessor_mut`](Table::cell_accessor_mut) or
    /// [`column_accessor_mut`](Table::column_accessor_mut).
    pub(crate) fn set_array_cell_assuming_valid(
        &mut self,
        row_index: usize,
        column: &str,
        value: ArrayValue,
    ) -> Result<(), TableError> {
        self.require_column(column)?;
        self.require_row_index_without_loading_rows(row_index)?;
        let Some(value) = self.inner.set_cached_array_cell(row_index, column, value)? else {
            return Ok(());
        };
        self.set_cell_impl(row_index, column, Value::Array(value))
    }

    /// Reserves sparse lazy-update capacity for repeated array-cell writes.
    ///
    /// This is useful when a caller knows it will update many rows of the same
    /// array column while keeping the table in lazy disk-backed mode.
    pub fn reserve_array_cell_updates(&mut self, column: &str, additional: usize) {
        self.inner.reserve_pending_array_cells(column, additional);
    }

    /// Returns the table-level keyword record.
    ///
    /// Keywords are arbitrary key/value pairs attached to the table as a whole.
    /// They correspond to the `TableRecord` stored in C++ casacore's `Table`
    /// object and are persisted alongside the row data.
    pub fn keywords(&self) -> &RecordValue {
        self.inner.keywords()
    }

    /// Returns a mutable reference to the table-level keyword record.
    ///
    /// Use this to insert or update table-level keywords before saving.
    pub fn keywords_mut(&mut self) -> &mut RecordValue {
        self.inner.keywords_mut()
    }

    /// Returns the keyword record for `column`, or `None` if no keywords have been set.
    ///
    /// Per-column keywords correspond to the `TableRecord` stored in C++
    /// casacore's `ROTableColumn::keywordSet()`.
    pub fn column_keywords(&self, column: &str) -> Option<&RecordValue> {
        self.inner.column_keywords(column)
    }

    /// Sets the keyword record for `column`, replacing any existing keywords.
    pub fn set_column_keywords(&mut self, column: impl Into<String>, keywords: RecordValue) {
        self.inner.set_column_keywords(column.into(), keywords);
    }

    /// Returns `true` if `column` has quantum (unit) metadata keywords.
    ///
    /// Convenience wrapper around
    /// [`TableQuantumDesc::has_quanta`](crate::table_quantum::TableQuantumDesc::has_quanta).
    pub fn has_quantum_column(&self, column: &str) -> bool {
        crate::table_quantum::TableQuantumDesc::has_quanta(self, column)
    }

    /// Reconstructs the quantum descriptor for `column`, if present.
    ///
    /// Returns `None` if the column has no `QuantumUnits` or `VariableUnits`
    /// keyword. Convenience wrapper around
    /// [`TableQuantumDesc::reconstruct`](crate::table_quantum::TableQuantumDesc::reconstruct).
    pub fn quantum_desc(&self, column: &str) -> Option<crate::table_quantum::TableQuantumDesc> {
        crate::table_quantum::TableQuantumDesc::reconstruct(self, column)
    }

    fn require_column(&self, column: &str) -> Result<(), TableError> {
        if let Some(schema) = self.schema()
            && !schema.contains_column(column)
        {
            return Err(TableError::SchemaColumnUnknown {
                column: column.to_string(),
            });
        }
        Ok(())
    }

    fn require_row_index_without_loading_rows(&self, row_index: usize) -> Result<(), TableError> {
        if row_index >= self.row_count() {
            return Err(TableError::RowOutOfBounds {
                row_index,
                row_count: self.row_count(),
            });
        }
        Ok(())
    }

    fn require_row(&self, row_index: usize) -> Result<(), TableError> {
        self.row(row_index).map(|_| ())
    }
}

impl<'a> TableRow<'a> {
    /// Returns the number of rows in the underlying table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Prepares a reusable row buffer for `columns`.
    ///
    /// The returned accessor compiles a stable slot order once and reuses a
    /// single `RecordValue` buffer across repeated row loads.
    pub fn prepare(self, columns: &[&str]) -> Result<PreparedTableRow<'a>, TableError> {
        let (slots, column_indices) = compile_prepared_row_slots(self.table, columns)?;
        Ok(PreparedTableRow {
            table: self.table,
            row: prepared_row_record(&slots),
            slots,
            column_indices,
            cached_rows: self.table.inner.prepared_rows(columns)?,
            row_index: None,
            row_materialized: false,
        })
    }

    /// Returns the row at `row_index`.
    pub fn row(&self, row_index: usize) -> Result<&'a RecordValue, TableError> {
        self.table.row(row_index)
    }

    /// Returns a cell accessor for `(row_index, column)`.
    pub fn cell(&self, row_index: usize, column: &str) -> Result<TableCell<'a>, TableError> {
        self.table.cell_accessor(row_index, column)
    }

    /// Returns the scalar cell at `(row_index, column)`.
    pub fn scalar_cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<&'a ScalarValue, TableError> {
        self.table.get_scalar_cell(row_index, column)
    }

    /// Returns the array cell at `(row_index, column)`.
    pub fn array_cell(&self, row_index: usize, column: &str) -> Result<&'a ArrayValue, TableError> {
        self.table.get_array_cell(row_index, column)
    }

    /// Returns the record cell at `(row_index, column)`.
    pub fn record_cell(&self, row_index: usize, column: &str) -> Result<RecordValue, TableError> {
        self.table.record_cell(row_index, column)
    }
}

impl<'a> TableRowMut<'a> {
    /// Returns the number of rows in the underlying table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Prepares a reusable mutable row buffer for `columns`.
    ///
    /// Call [`PreparedTableRowMut::flush`] after the final mutation so the
    /// last loaded row is written back through the table accessor layer.
    pub fn prepare(self, columns: &[&str]) -> Result<PreparedTableRowMut<'a>, TableError> {
        let (slots, column_indices) = compile_prepared_row_slots(self.table, columns)?;
        Ok(PreparedTableRowMut {
            table: self.table,
            row: prepared_row_record(&slots),
            slots,
            column_indices,
            row_index: None,
            row_materialized: false,
            dirty: false,
        })
    }

    /// Returns the row at `row_index`.
    pub fn row(&self, row_index: usize) -> Result<&RecordValue, TableError> {
        self.table.row(row_index)
    }

    /// Returns the row at `row_index` for direct mutation.
    pub fn row_mut(&mut self, row_index: usize) -> Result<&mut RecordValue, TableError> {
        self.table.row_mut(row_index)
    }

    /// Returns a read-only cell accessor for `(row_index, column)`.
    pub fn cell(&self, row_index: usize, column: &str) -> Result<TableCell<'_>, TableError> {
        self.table.cell_accessor(row_index, column)
    }

    /// Returns a mutable cell accessor for `(row_index, column)`.
    pub fn cell_mut(
        &mut self,
        row_index: usize,
        column: &str,
    ) -> Result<TableCellMut<'_>, TableError> {
        self.table.cell_accessor_mut(row_index, column)
    }

    /// Writes `value` to `(row_index, column)`.
    pub fn set_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: Value,
    ) -> Result<(), TableError> {
        self.table.set_cell(row_index, column, value)
    }

    /// Writes a record value to `(row_index, column)`.
    pub fn set_record_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: RecordValue,
    ) -> Result<(), TableError> {
        self.table.set_record_cell(row_index, column, value)
    }

    /// Lazily updates a scalar cell while assuming schema validity.
    pub fn set_scalar_cell_assuming_valid(
        &mut self,
        row_index: usize,
        column: &str,
        value: ScalarValue,
    ) -> Result<(), TableError> {
        self.table
            .set_scalar_cell_assuming_valid(row_index, column, value)
    }

    /// Lazily updates an array cell while assuming schema validity.
    pub fn set_array_cell_assuming_valid(
        &mut self,
        row_index: usize,
        column: &str,
        value: ArrayValue,
    ) -> Result<(), TableError> {
        self.table
            .set_array_cell_assuming_valid(row_index, column, value)
    }
}

impl<'a> PreparedTableRow<'a> {
    /// Returns the number of rows in the underlying table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Returns the stable slot index for `column`.
    pub fn column_index(&self, column: &str) -> Option<usize> {
        self.column_indices.get(column).copied()
    }

    /// Returns the currently loaded row index, if any.
    pub fn current_row_index(&self) -> Option<usize> {
        self.row_index
    }

    /// Returns the current reusable row buffer, if one has been loaded.
    pub fn row(&mut self) -> Option<&RecordValue> {
        let row_index = self.row_index?;
        if let Some(rows) = self.cached_rows.as_ref() {
            if !self.row_materialized {
                let cached_row = rows.get(row_index)?;
                for (target, source) in self
                    .row
                    .fields_mut()
                    .iter_mut()
                    .zip(cached_row.fields().iter())
                {
                    target.value = source.value.clone();
                }
                self.row_materialized = true;
            }
            return Some(&self.row);
        }
        if !self.row_materialized {
            fill_prepared_row_buffer(self.table, &self.slots, &mut self.row, row_index).ok()?;
            self.row_materialized = true;
        }
        Some(&self.row)
    }

    /// Selects `row_index` as the current row for indexed access.
    pub fn load(&mut self, row_index: usize) -> Result<(), TableError> {
        self.table
            .require_row_index_without_loading_rows(row_index)?;
        self.row_index = Some(row_index);
        self.row_materialized = false;
        Ok(())
    }

    /// Returns the scalar value for `slot_index` in the current row without cloning.
    pub fn scalar_at(&self, slot_index: usize) -> Result<&ScalarValue, TableError> {
        let row_index = current_prepared_row_index(self.row_index)?;
        let slot = self
            .slots
            .get(slot_index)
            .ok_or_else(|| TableError::SchemaColumnUnknown {
                column: format!("#{slot_index}"),
            })?;
        if let Some(rows) = self.cached_rows.as_ref() {
            let row = rows.get(row_index).ok_or(TableError::RowOutOfBounds {
                row_index,
                row_count: rows.len(),
            })?;
            return match row.fields().get(slot_index).map(|field| &field.value) {
                Some(Value::Scalar(value)) => Ok(value),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: slot.column.clone(),
                    expected: "scalar",
                    found: value.kind(),
                }),
                None => Err(TableError::ColumnNotFound {
                    row_index,
                    column: slot.column.clone(),
                }),
            };
        }
        match slot.kind {
            PreparedRowSlotKind::Scalar => self.table.get_scalar_cell(row_index, &slot.column),
            PreparedRowSlotKind::Array => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: slot.column.clone(),
                expected: "scalar",
                found: ValueKind::Array,
            }),
        }
    }

    /// Returns the array value for `slot_index` in the current row without cloning.
    pub fn array_at(&self, slot_index: usize) -> Result<&ArrayValue, TableError> {
        let row_index = current_prepared_row_index(self.row_index)?;
        let slot = self
            .slots
            .get(slot_index)
            .ok_or_else(|| TableError::SchemaColumnUnknown {
                column: format!("#{slot_index}"),
            })?;
        if let Some(rows) = self.cached_rows.as_ref() {
            let row = rows.get(row_index).ok_or(TableError::RowOutOfBounds {
                row_index,
                row_count: rows.len(),
            })?;
            return match row.fields().get(slot_index).map(|field| &field.value) {
                Some(Value::Array(value)) => Ok(value),
                Some(value) => Err(TableError::ColumnTypeMismatch {
                    row_index,
                    column: slot.column.clone(),
                    expected: "array",
                    found: value.kind(),
                }),
                None => Err(TableError::ColumnNotFound {
                    row_index,
                    column: slot.column.clone(),
                }),
            };
        }
        match slot.kind {
            PreparedRowSlotKind::Array => self.table.get_array_cell(row_index, &slot.column),
            PreparedRowSlotKind::Scalar => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: slot.column.clone(),
                expected: "array",
                found: ValueKind::Scalar,
            }),
        }
    }
}

impl<'a> PreparedTableRowMut<'a> {
    /// Returns the number of rows in the underlying table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
    }

    /// Returns the stable slot index for `column`.
    pub fn column_index(&self, column: &str) -> Option<usize> {
        self.column_indices.get(column).copied()
    }

    /// Returns the currently loaded row index, if any.
    pub fn current_row_index(&self) -> Option<usize> {
        self.row_index
    }

    /// Returns the current reusable row buffer, if one has been loaded.
    pub fn row(&self) -> Option<&RecordValue> {
        self.row_materialized.then_some(&self.row)
    }

    /// Returns the current reusable row buffer for mutation, if one has been loaded.
    pub fn row_mut(&mut self) -> Option<&mut RecordValue> {
        if self.row_materialized {
            self.dirty = true;
            Some(&mut self.row)
        } else {
            None
        }
    }

    /// Loads `row_index` into the reusable row buffer.
    ///
    /// If the current row buffer is dirty and `row_index` differs from the
    /// loaded row, the current row is flushed first.
    pub fn load(&mut self, row_index: usize) -> Result<&RecordValue, TableError> {
        self.table
            .require_row_index_without_loading_rows(row_index)?;
        if self.row_index != Some(row_index) || !self.row_materialized {
            self.flush()?;
            fill_prepared_row_buffer(self.table, &self.slots, &mut self.row, row_index)?;
            self.row_index = Some(row_index);
            self.row_materialized = true;
        }
        Ok(&self.row)
    }

    /// Selects `row_index` as the current row for direct indexed writes.
    ///
    /// This avoids loading the reusable row buffer unless [`row_mut`](Self::row_mut)
    /// is used afterwards.
    pub fn seek(&mut self, row_index: usize) -> Result<(), TableError> {
        self.table
            .require_row_index_without_loading_rows(row_index)?;
        if self.row_index != Some(row_index) && self.dirty {
            self.flush()?;
        }
        if self.row_index != Some(row_index) {
            self.row_materialized = false;
        }
        self.row_index = Some(row_index);
        Ok(())
    }

    /// Flushes the current row buffer through the table accessor layer.
    pub fn flush(&mut self) -> Result<(), TableError> {
        let Some(row_index) = self.row_index else {
            self.dirty = false;
            return Ok(());
        };
        if self.dirty {
            flush_prepared_row_buffer(self.table, &self.slots, &self.row, row_index)?;
            self.dirty = false;
        }
        Ok(())
    }

    /// Writes `value` to `slot_index` in the currently selected row.
    ///
    /// This is the fast path for callers that already computed replacement
    /// values and do not need a materialized row buffer.
    pub fn set_value_at(&mut self, slot_index: usize, value: Value) -> Result<(), TableError> {
        let row_index = current_prepared_row_index(self.row_index)?;
        if self.dirty && self.row_materialized {
            if let Some(field) = self.row.fields_mut().get_mut(slot_index) {
                field.value = value;
                return Ok(());
            }
        }
        let slot = self
            .slots
            .get(slot_index)
            .ok_or_else(|| TableError::SchemaColumnUnknown {
                column: format!("#{slot_index}"),
            })?;
        if self.row_materialized
            && let Some(field) = self.row.fields_mut().get_mut(slot_index)
        {
            field.value = value.clone();
        }
        match (slot.kind, value) {
            (PreparedRowSlotKind::Scalar, Value::Scalar(value)) => self
                .table
                .set_scalar_cell_assuming_valid(row_index, &slot.column, value),
            (PreparedRowSlotKind::Array, Value::Array(value)) => self
                .table
                .set_array_cell_assuming_valid(row_index, &slot.column, value),
            (PreparedRowSlotKind::Scalar, value) => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: slot.column.clone(),
                expected: "scalar",
                found: value.kind(),
            }),
            (PreparedRowSlotKind::Array, value) => Err(TableError::ColumnTypeMismatch {
                row_index,
                column: slot.column.clone(),
                expected: "array",
                found: value.kind(),
            }),
        }
    }
}

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

    /// Returns 2-D array channel slices for selected rows without populating the
    /// table-level row cache.
    ///
    /// The returned arrays keep axis 0 intact and contain
    /// `channel_start..channel_start + channel_count` from axis 1. The output
    /// preserves the order of `row_indices`.
    pub fn array_cells_2d_channel_range_owned_uncached(
        &self,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Vec<Option<ArrayValue>>, TableError> {
        self.table.get_array_cells_2d_channel_range_owned_uncached(
            &self.column,
            row_indices,
            channel_start,
            channel_count,
        )
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

    /// Lazily updates a scalar cell while assuming schema validity.
    pub fn set_scalar_assuming_valid(
        &mut self,
        row_index: usize,
        value: ScalarValue,
    ) -> Result<(), TableError> {
        self.table
            .set_scalar_cell_assuming_valid(row_index, &self.column, value)
    }

    /// Lazily updates an array cell while assuming schema validity.
    pub fn set_array_assuming_valid(
        &mut self,
        row_index: usize,
        value: ArrayValue,
    ) -> Result<(), TableError> {
        self.table
            .set_array_cell_assuming_valid(row_index, &self.column, value)
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

    /// Lazily updates the cell as a scalar while assuming schema validity.
    pub fn set_scalar_assuming_valid(&mut self, value: ScalarValue) -> Result<(), TableError> {
        self.table
            .set_scalar_cell_assuming_valid(self.row_index, &self.column, value)
    }

    /// Lazily updates the cell as an array while assuming schema validity.
    pub fn set_array_assuming_valid(&mut self, value: ArrayValue) -> Result<(), TableError> {
        self.table
            .set_array_cell_assuming_valid(self.row_index, &self.column, value)
    }
}

// ── Schema validation helpers ─────────────────────────────────────────

pub(super) fn validate_row_against_schema(
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

pub(super) fn collect_undefined_cells_for_schema(
    rows: &[RecordValue],
    schema: &TableSchema,
) -> Vec<HashSet<String>> {
    rows.iter()
        .map(|row| undefined_columns_for_row(row, schema))
        .collect()
}

pub(super) fn undefined_columns_for_row(
    row: &RecordValue,
    schema: &TableSchema,
) -> HashSet<String> {
    schema
        .columns()
        .iter()
        .filter(|column| column.options().undefined && row.get(column.name()).is_none())
        .map(|column| column.name().to_string())
        .collect()
}

pub(super) fn validate_cell_against_schema_column(
    row_index: usize,
    column: &ColumnSchema,
    value: Option<&Value>,
) -> Result<(), TableError> {
    match (column.column_type(), value) {
        (ColumnType::Scalar, Some(Value::Scalar(_))) => Ok(()),
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

fn validate_array_contract(
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
