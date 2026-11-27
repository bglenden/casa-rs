// SPDX-License-Identifier: LGPL-3.0-or-later
mod array_metadata;

use std::collections::HashSet;

use super::*;
use crate::storage::RequiredScalarColumnData;
mod cells;
mod rows;

pub(super) use cells::collect_undefined_cells_for_schema;
pub(super) use cells::undefined_columns_for_row;
pub(super) use cells::validate_cell_against_schema_column;
pub(super) use cells::validate_row_against_schema;
use rows::*;

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

    /// Prepare a schema-bound appender for a sequence of rows.
    pub fn prepare_row_appender(&mut self) -> Result<PreparedRowAppender<'_>, TableError> {
        let schema = self
            .schema()
            .cloned()
            .ok_or(TableError::PreparedRowRequiresSchema)?;
        Ok(PreparedRowAppender {
            table: self,
            schema,
        })
    }

    pub(super) fn has_asdm_manager(&self) -> bool {
        self.dm_info
            .iter()
            .any(|manager| manager.dm_type == crate::storage::asdm_stman::TYPE)
    }

    fn asdm_manages(&self, column: &str) -> bool {
        self.dm_info.iter().any(|manager| {
            manager.dm_type == crate::storage::asdm_stman::TYPE
                && manager.columns.iter().any(|name| name == column)
        })
    }

    pub(super) fn require_writable_column(&self, column: &str) -> Result<(), TableError> {
        if self.asdm_manages(column) {
            return Err(TableError::Storage(format!(
                "AsdmStMan column '{column}' is read-only"
            )));
        }
        Ok(())
    }

    pub(super) fn require_mutable_row_registration(&self) -> Result<(), TableError> {
        if self.has_asdm_manager() {
            return Err(TableError::Storage(
                "AsdmStMan row registration is read-only".into(),
            ));
        }
        Ok(())
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
        self.require_writable_column(column)?;
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
        self.require_writable_column(column)?;
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
        self.require_mutable_row_registration()?;
        let auto_unlock = self.begin_write_operation("add_row")?;
        let result = (|| {
            let mut undefined = None;
            if let Some(schema) = self.schema() {
                validate_row_against_schema(self.row_count(), &row, schema)?;
                undefined = Some(undefined_columns_for_row(&row, schema));
            }
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

    /// Appends a row without re-validating it against the attached schema.
    ///
    /// This is intended for advanced callers that already know `row` matches
    /// the current schema because it was synthesized directly from the schema
    /// or validated earlier in the same write path. Undefined scalar-column
    /// tracking is still updated when a schema is attached.
    ///
    /// Callers that are not certain the row is schema-valid should keep using
    /// [`add_row`](Table::add_row).
    pub(crate) fn append_row_bound(&mut self, row: RecordValue) -> Result<(), TableError> {
        self.require_mutable_row_registration()?;
        let auto_unlock = self.begin_write_operation("append_row_bound")?;
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
        self.require_mutable_row_registration()?;
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
        if self.asdm_manages(column)
            && let Some(shape) = self.stored_array_shape(row_index, column)?
        {
            return Ok(shape.is_some());
        }
        // A table read lazily answers from the cell's own store, without
        // loading its rows (a record cell is always defined, so it is not
        // asked there).
        let record_column = matches!(
            self.schema()
                .and_then(|schema| schema.column(column))
                .map(ColumnSchema::column_type),
            Some(ColumnType::Record)
        );
        if !record_column {
            match self.inner.array_cell(row_index, column)? {
                crate::table_impl::LazyArrayLookup::Hit(_) => return Ok(true),
                crate::table_impl::LazyArrayLookup::Missing => return Ok(false),
                crate::table_impl::LazyArrayLookup::Unknown => {}
            }
            match self.inner.scalar_cell(row_index, column)? {
                crate::table_impl::LazyScalarLookup::Hit(_) => return Ok(true),
                crate::table_impl::LazyScalarLookup::Missing => return Ok(false),
                crate::table_impl::LazyScalarLookup::Unknown => {}
            }
        }
        if let Some(undefined) = self
            .inner
            .undefined_cells()?
            .get(row_index)
            .map(|set| set.contains(column))
            && undefined
        {
            return Ok(false);
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
        if let Some(shape) = self.stored_array_shape(row_index, column)? {
            return Ok(shape);
        }
        // Ask the lazy store first: `cell` loads every row of the table.
        match self.inner.array_cell(row_index, column)? {
            crate::table_impl::LazyArrayLookup::Hit(array) => {
                return Ok(Some(array.shape().to_vec()));
            }
            crate::table_impl::LazyArrayLookup::Missing => return Ok(None),
            crate::table_impl::LazyArrayLookup::Unknown => {}
        }
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

    /// Read one cell's dimensions without retaining an indirect array payload.
    pub fn array_shape_uncached(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<Option<Vec<usize>>, TableError> {
        self.require_row(row_index)?;
        self.require_column(column)?;
        if let Some(shape) = self.stored_array_shape(row_index, column)? {
            return Ok(shape);
        }
        let mut cells = self
            .column_accessor(column)?
            .array_cells_owned_uncached(&[row_index])?;
        Ok(cells.pop().flatten().map(|cell| cell.shape().to_vec()))
    }

    fn stored_array_shape(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<Option<Option<Vec<usize>>>, TableError> {
        if matches!(
            self.schema()
                .and_then(|schema| schema.column(column))
                .map(ColumnSchema::column_type),
            Some(ColumnType::Array(_))
        ) && self.dm_info.iter().any(|manager| {
            matches!(
                manager.dm_type.as_str(),
                "TiledShapeStMan" | crate::storage::asdm_stman::TYPE
            ) && manager.columns.iter().any(|name| name == column)
        }) {
            return self.inner.try_retained_array_shape(row_index, column);
        }
        Ok(None)
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
        self.require_writable_column(column)?;
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

        let row = self.inner.editable_cell_row(row_index, column)?;

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

    pub(crate) fn get_array_cells_2d_channel_range_typed_uncached(
        &self,
        column: &str,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Option<SelectedArray2DCells>, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        self.inner.array_cells_2d_channel_range_typed_uncached(
            row_indices,
            column,
            channel_start,
            channel_count,
        )
    }

    pub(crate) fn fill_array_cells_2d_channel_range_typed_uncached(
        &self,
        column: &str,
        row_indices: &[usize],
        channel_start: usize,
        channel_count: usize,
        destination: SelectedArray2DCellsMut<'_>,
    ) -> Result<Option<SelectedArray2DShape>, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        self.inner.array_cells_2d_channel_range_typed_uncached_into(
            row_indices,
            column,
            channel_start,
            channel_count,
            destination,
        )
    }

    pub(crate) fn get_array_cells_1d_typed_uncached(
        &self,
        column: &str,
        row_indices: &[usize],
    ) -> Result<SelectedArray1DCells, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        self.inner
            .array_cells_1d_typed_uncached(row_indices, column)
    }

    pub(crate) fn fill_array_cells_1d_typed_uncached(
        &self,
        column: &str,
        row_indices: &[usize],
        destination: SelectedArray1DCellsMut<'_>,
    ) -> Result<SelectedArray1DShape, TableError> {
        self.require_column(column)?;
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        self.inner
            .array_cells_1d_typed_uncached_into(row_indices, column, destination)
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

    /// Returns owned scalar values for every row in each requested column.
    ///
    /// Missing cells are returned as `None`. Disk-backed callers should prefer
    /// this over repeated single-column calls when several scalar columns from
    /// the same storage manager are needed together.
    pub fn scalar_columns_owned(
        &self,
        columns: &[&str],
    ) -> Result<HashMap<String, Vec<Option<ScalarValue>>>, TableError> {
        for &column in columns {
            self.require_column(column)?;
        }
        if let Some(values_by_column) = self.inner.scalar_columns_owned(columns)? {
            return Ok(values_by_column);
        }
        columns
            .iter()
            .map(|&column| {
                self.get_scalar_cells_owned(column)
                    .map(|values| (column.to_string(), values))
            })
            .collect()
    }

    /// Load persisted scalar columns together without opening a table handle.
    /// Missing cells return `None`. Callers with a handle and pending edits
    /// should use [`Self::scalar_columns_owned`] to include those edits.
    ///
    /// # Errors
    /// Returns an error if the table, a required manager or a requested
    /// scalar column is unavailable or unreadable.
    pub fn read_scalar_columns_from_disk(
        path: &Path,
        columns: &[&str],
    ) -> Result<HashMap<String, Vec<Option<ScalarValue>>>, TableError> {
        #[cfg(unix)]
        let row_hint = crate::lock::read_sync_data_from_table_dir(path)
            .ok()
            .flatten()
            .map(|sync| sync.nrrow);
        #[cfg(not(unix))]
        let row_hint = None;
        let names = columns.iter().copied().collect::<HashSet<_>>();
        let snapshot =
            CompositeStorage.load_named_scalar_columns_with_row_hint(path, &names, row_hint)?;
        for &column in columns {
            if !snapshot.columns.contains_key(column) {
                return Err(TableError::SchemaColumnUnknown {
                    column: column.into(),
                });
            }
        }
        Ok(snapshot.columns)
    }

    /// Returns required scalar values for every row in each requested column.
    ///
    /// This is a typed, column-oriented variant of
    /// [`scalar_columns_owned`](Self::scalar_columns_owned) for hot paths that
    /// know the selected scalar columns are fully populated.
    pub fn required_scalar_columns_owned(
        &self,
        columns: &[&str],
    ) -> Result<HashMap<String, RequiredScalarColumnValues>, TableError> {
        for &column in columns {
            self.require_column(column)?;
        }
        if let Some(values_by_column) = self.inner.required_scalar_columns_owned(columns)? {
            return Ok(values_by_column
                .into_iter()
                .map(|(name, values)| (name, required_scalar_column_values(values)))
                .collect());
        }
        columns
            .iter()
            .map(|&column| {
                let values = self.get_scalar_cells_owned(column)?;
                required_scalar_column_values_from_optional_scalars(&values, column)
                    .map(|values| (column.to_string(), values))
            })
            .collect()
    }

    /// Returns typed required scalar values for selected rows in each column.
    ///
    /// Storage managers may coalesce these reads by manager and bucket. This
    /// remains a column/cell operation; manager bindings persisted by CASA are
    /// honored rather than replaced by a separate read convention.
    pub fn required_scalar_columns_owned_for_rows(
        &self,
        columns: &[&str],
        row_indices: &[usize],
    ) -> Result<HashMap<String, RequiredScalarColumnValues>, TableError> {
        for &column in columns {
            self.require_column(column)?;
        }
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        if let Some(values_by_column) = self
            .inner
            .required_scalar_columns_owned_for_rows(row_indices, columns)?
        {
            return Ok(values_by_column
                .into_iter()
                .map(|(name, values)| (name, required_scalar_column_values(values)))
                .collect());
        }
        columns
            .iter()
            .map(|&column| {
                let values = self.get_scalar_cells_owned_for_rows(column, row_indices)?;
                required_scalar_column_values_from_optional_scalars(&values, column)
                    .map(|values| (column.to_string(), values))
            })
            .collect()
    }

    /// Fill caller-owned typed vectors for required scalar values at selected
    /// rows while preserving storage-manager grouped I/O.
    ///
    /// This disk read refuses pending scalar overrides and cached columns on
    /// edited handles. Use [`TableColumn::scalar_cells_owned_for_rows`] for
    /// those columns to observe changes made through the open handle.
    pub fn required_scalar_columns_for_rows_into(
        &self,
        row_indices: &[usize],
        destinations: &mut [RequiredScalarColumnDestination<'_>],
    ) -> Result<(), TableError> {
        let mut names = HashSet::with_capacity(destinations.len());
        for destination in destinations.iter() {
            self.require_column(destination.column())?;
            if !names.insert(destination.column()) {
                return Err(TableError::Storage(format!(
                    "required scalar reusable fill contains duplicate column '{}'",
                    destination.column()
                )));
            }
        }
        for &row_index in row_indices {
            if row_index >= self.row_count() {
                return Err(TableError::RowOutOfBounds {
                    row_index,
                    row_count: self.row_count(),
                });
            }
        }
        self.inner
            .required_scalar_columns_for_rows_into(row_indices, destinations)
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
    pub(crate) fn write_scalar_cell_bound(
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
    pub(crate) fn write_array_cell_bound(
        &mut self,
        row_index: usize,
        column: &str,
        value: ArrayValue,
    ) -> Result<(), TableError> {
        self.require_column(column)?;
        self.require_writable_column(column)?;
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

    pub(super) fn require_column(&self, column: &str) -> Result<(), TableError> {
        if let Some(schema) = self.schema()
            && !schema.contains_column(column)
        {
            return Err(TableError::SchemaColumnUnknown {
                column: column.to_string(),
            });
        }
        Ok(())
    }

    pub(super) fn require_row_index_without_loading_rows(
        &self,
        row_index: usize,
    ) -> Result<(), TableError> {
        if row_index >= self.row_count() {
            return Err(TableError::RowOutOfBounds {
                row_index,
                row_count: self.row_count(),
            });
        }
        Ok(())
    }

    /// Checks that `row_index` is a row, without loading the rows of a table
    /// read lazily (`row` does).
    fn require_row(&self, row_index: usize) -> Result<(), TableError> {
        self.require_row_index_without_loading_rows(row_index)
    }
}
