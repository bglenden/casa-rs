// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn compile_prepared_row_slots(
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
            schema_column: schema_column.clone(),
        });
        column_indices
            .entry(column.to_string())
            .or_insert(slot_index);
    }

    Ok((slots, column_indices))
}

pub(super) fn placeholder_value(kind: PreparedRowSlotKind) -> Value {
    match kind {
        PreparedRowSlotKind::Scalar => Value::Scalar(ScalarValue::Bool(false)),
        PreparedRowSlotKind::Array => Value::Array(ArrayValue::from_bool_vec(Vec::new())),
    }
}

pub(super) fn prepared_row_record(slots: &[PreparedRowSlot]) -> RecordValue {
    RecordValue::new(
        slots
            .iter()
            .map(|slot| RecordField::new(slot.column.clone(), placeholder_value(slot.kind)))
            .collect(),
    )
}

pub(super) fn load_prepared_row_value(
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

pub(super) fn fill_prepared_row_buffer(
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

/// The value of the slot `slot_index`, column `column`, in a cached row of
/// the prepared columns: at the slot's position, unless an undefined cell
/// before it, which a row leaves out, moved it; `None` for an undefined
/// cell.
pub(super) fn cached_slot_value<'r>(
    row: &'r RecordValue,
    slot_index: usize,
    column: &str,
) -> Option<&'r Value> {
    row.fields()
        .get(slot_index)
        .filter(|field| field.name == column)
        .or_else(|| row.fields().iter().find(|field| field.name == column))
        .map(|field| &field.value)
}

pub(super) fn current_prepared_row_index(row_index: Option<usize>) -> Result<usize, TableError> {
    row_index.ok_or(TableError::RowOutOfBounds {
        row_index: 0,
        row_count: 0,
    })
}

pub(super) fn flush_prepared_row_buffer(
    table: &mut Table,
    slots: &[PreparedRowSlot],
    row: &RecordValue,
    row_index: usize,
) -> Result<(), TableError> {
    for (slot, field) in slots.iter().zip(row.fields().iter()) {
        validate_cell_against_schema_column(row_index, &slot.schema_column, Some(&field.value))?;
        match (slot.kind, &field.value) {
            (PreparedRowSlotKind::Scalar, Value::Scalar(value)) => {
                table.write_scalar_cell_bound(row_index, &slot.column, value.clone())?;
            }
            (PreparedRowSlotKind::Array, Value::Array(value)) => {
                table.write_array_cell_bound(row_index, &slot.column, value.clone())?;
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
        for column in columns {
            self.table.require_writable_column(column)?;
        }
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
}

impl PreparedRowAppender<'_> {
    /// Append one row through this schema proof.
    pub fn append(&mut self, row: RecordValue) -> Result<(), TableError> {
        let row_index = self.table.row_count();
        validate_row_against_schema(row_index, &row, &self.schema)?;
        self.table.append_row_bound(row)
    }

    /// Append a sequence of rows, stopping before the first invalid row.
    ///
    /// Rows accepted before a later value-validation error remain appended;
    /// callers that require transactional construction should prepare and
    /// validate their values before opening the appender.
    pub fn extend<I>(&mut self, rows: I) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = RecordValue>,
    {
        let mut count = 0;
        for row in rows {
            self.append(row)?;
            count += 1;
        }
        Ok(count)
    }

    /// Return the number of rows currently in the table.
    pub fn row_count(&self) -> usize {
        self.table.row_count()
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
                // As without cached rows, a row with an undefined cell has
                // no buffer.
                for (slot_index, target) in self.row.fields_mut().iter_mut().enumerate() {
                    target.value = cached_slot_value(cached_row, slot_index, &target.name)?.clone();
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
            return match cached_slot_value(row, slot_index, &slot.column) {
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
            return match cached_slot_value(row, slot_index, &slot.column) {
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
        if self.dirty
            && self.row_materialized
            && let Some(field) = self.row.fields_mut().get_mut(slot_index)
        {
            field.value = value;
            return Ok(());
        }
        let slot = self
            .slots
            .get(slot_index)
            .ok_or_else(|| TableError::SchemaColumnUnknown {
                column: format!("#{slot_index}"),
            })?;
        validate_cell_against_schema_column(row_index, &slot.schema_column, Some(&value))?;
        if self.row_materialized
            && let Some(field) = self.row.fields_mut().get_mut(slot_index)
        {
            field.value = value.clone();
        }
        match (slot.kind, value) {
            (PreparedRowSlotKind::Scalar, Value::Scalar(value)) => self
                .table
                .write_scalar_cell_bound(row_index, &slot.column, value),
            (PreparedRowSlotKind::Array, Value::Array(value)) => {
                self.table
                    .write_array_cell_bound(row_index, &slot.column, value)
            }
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

pub(super) fn required_scalar_column_values(
    values: RequiredScalarColumnData,
) -> RequiredScalarColumnValues {
    match values {
        RequiredScalarColumnData::Bool(values) => RequiredScalarColumnValues::Bool(values),
        RequiredScalarColumnData::Int32(values) => RequiredScalarColumnValues::Int32(values),
        RequiredScalarColumnData::Float32(values) => RequiredScalarColumnValues::Float32(values),
        RequiredScalarColumnData::Float64(values) => RequiredScalarColumnValues::Float64(values),
    }
}

pub(super) fn required_scalar_column_values_from_optional_scalars(
    values: &[Option<ScalarValue>],
    column: &str,
) -> Result<RequiredScalarColumnValues, TableError> {
    let Some(first) = values.iter().find_map(|value| value.as_ref()) else {
        return Err(TableError::Storage(format!(
            "required scalar column {column} has no values"
        )));
    };
    match first {
        ScalarValue::Bool(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Bool(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Bool"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnValues::Bool),
        ScalarValue::Int32(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Int32(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Int32"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnValues::Int32),
        ScalarValue::Float32(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Float32(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Float32"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnValues::Float32),
        ScalarValue::Float64(_) => values
            .iter()
            .enumerate()
            .map(|(row, value)| match value {
                Some(ScalarValue::Float64(value)) => Ok(*value),
                _ => Err(TableError::Storage(format!(
                    "required scalar column {column} row {row} is not Float64"
                ))),
            })
            .collect::<Result<Vec<_>, _>>()
            .map(RequiredScalarColumnValues::Float64),
        other => Err(TableError::Storage(format!(
            "required scalar column {column} has unsupported type {:?}",
            other.primitive_type()
        ))),
    }
}
