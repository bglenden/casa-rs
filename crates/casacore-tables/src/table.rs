use std::path::{Path, PathBuf};

use casacore_types::{ArrayValue, RecordValue, ScalarValue, Value, ValueKind};
use thiserror::Error;

use crate::schema::{ArrayShapeContract, ColumnSchema, ColumnType, SchemaError, TableSchema};
use crate::storage::{CompositeStorage, StorageManager, StorageSnapshot};
use crate::table_impl::TableImpl;

/// Which data manager to use when writing table data.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataManagerKind {
    /// StManAipsIO: simple whole-column AipsIO streaming.
    #[default]
    StManAipsIO,
    /// StandardStMan: bucket-based storage (the C++ casacore default).
    StandardStMan,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    pub path: PathBuf,
    pub data_manager: DataManagerKind,
}

impl TableOptions {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            data_manager: DataManagerKind::default(),
        }
    }

    pub fn with_data_manager(mut self, kind: DataManagerKind) -> Self {
        self.data_manager = kind;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    start: usize,
    end: usize,
    stride: usize,
}

impl RowRange {
    pub const fn new(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            stride: 1,
        }
    }

    pub const fn with_stride(start: usize, end: usize, stride: usize) -> Self {
        Self { start, end, stride }
    }

    pub const fn start(&self) -> usize {
        self.start
    }

    pub const fn end(&self) -> usize {
        self.end
    }

    pub const fn stride(&self) -> usize {
        self.stride
    }

    fn validate(&self, row_count: usize) -> Result<(), TableError> {
        if self.stride == 0 {
            return Err(TableError::InvalidRowStride {
                stride: self.stride,
            });
        }
        if self.start > self.end || self.end > row_count {
            return Err(TableError::InvalidRowRange {
                start: self.start,
                end: self.end,
                row_count,
            });
        }
        Ok(())
    }

    fn len(&self) -> usize {
        if self.start >= self.end || self.stride == 0 {
            0
        } else {
            1 + ((self.end - self.start - 1) / self.stride)
        }
    }

    fn iter(&self) -> RowRangeIter {
        RowRangeIter {
            next: self.start,
            end: self.end,
            stride: self.stride,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnCellRef<'a> {
    pub row_index: usize,
    pub value: Option<&'a Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RecordColumnCell {
    pub row_index: usize,
    pub value: RecordValue,
}

pub struct ColumnCellIter<'a> {
    row_data: &'a [RecordValue],
    column: &'a str,
    rows: RowRangeIter,
}

impl<'a> Iterator for ColumnCellIter<'a> {
    type Item = ColumnCellRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|row_index| ColumnCellRef {
            row_index,
            value: self.row_data[row_index].get(self.column),
        })
    }
}

pub struct RecordColumnIter<'a> {
    row_data: &'a [RecordValue],
    column: &'a str,
    rows: RowRangeIter,
    default_missing: bool,
}

impl<'a> Iterator for RecordColumnIter<'a> {
    type Item = RecordColumnCell;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|row_index| {
            let value = match self.row_data[row_index].get(self.column) {
                Some(Value::Record(record)) => record.clone(),
                Some(_) => unreachable!("record iterator was prevalidated"),
                None => {
                    if self.default_missing {
                        RecordValue::default()
                    } else {
                        unreachable!("record iterator was prevalidated")
                    }
                }
            };
            RecordColumnCell { row_index, value }
        })
    }
}

pub struct ColumnChunkIter<'a> {
    inner: ColumnCellIter<'a>,
    chunk_size: usize,
}

impl<'a> Iterator for ColumnChunkIter<'a> {
    type Item = Vec<ColumnCellRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let chunk: Vec<_> = self.inner.by_ref().take(self.chunk_size).collect();
        if chunk.is_empty() { None } else { Some(chunk) }
    }
}

#[derive(Debug, Clone, Copy)]
struct RowRangeIter {
    next: usize,
    end: usize,
    stride: usize,
}

impl Iterator for RowRangeIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.end {
            return None;
        }
        let row_index = self.next;
        self.next = self.next.saturating_add(self.stride);
        Some(row_index)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TableError {
    #[error("row index {row_index} is out of bounds for table with {row_count} rows")]
    RowOutOfBounds { row_index: usize, row_count: usize },
    #[error("row range [{start}, {end}) is invalid for table with {row_count} rows")]
    InvalidRowRange {
        start: usize,
        end: usize,
        row_count: usize,
    },
    #[error("row stride must be >= 1, got {stride}")]
    InvalidRowStride { stride: usize },
    #[error("column \"{column}\" not found in row {row_index}")]
    ColumnNotFound { row_index: usize, column: String },
    #[error("column \"{column}\" does not exist in schema")]
    SchemaColumnUnknown { column: String },
    #[error("column \"{column}\" is not a record column in schema")]
    SchemaColumnNotRecord { column: String },
    #[error("schema column \"{column}\" is missing in row {row_index}")]
    SchemaColumnMissing { row_index: usize, column: String },
    #[error("row {row_index} contains unknown column \"{column}\" not present in schema")]
    RowContainsUnknownColumn { row_index: usize, column: String },
    #[error(
        "row {row_index} column \"{column}\" has unexpected type {found:?}; expected {expected}"
    )]
    ColumnTypeMismatch {
        row_index: usize,
        column: String,
        expected: &'static str,
        found: ValueKind,
    },
    #[error("row {row_index} column \"{column}\" has shape {found:?}; expected {expected:?}")]
    ArrayShapeMismatch {
        row_index: usize,
        column: String,
        expected: Vec<usize>,
        found: Vec<usize>,
    },
    #[error("row {row_index} column \"{column}\" has ndim {found}; expected {expected}")]
    ArrayNdimMismatch {
        row_index: usize,
        column: String,
        expected: usize,
        found: usize,
    },
    #[error("column write received too few values: expected {expected}, provided {provided}")]
    ColumnWriteTooFewValues { expected: usize, provided: usize },
    #[error("column write received too many values: expected {expected}")]
    ColumnWriteTooManyValues { expected: usize },
    #[error("schema error: {0}")]
    Schema(String),
    #[error("storage error: {0}")]
    Storage(String),
}

impl From<crate::storage::StorageError> for TableError {
    fn from(value: crate::storage::StorageError) -> Self {
        Self::Storage(value.to_string())
    }
}

impl From<SchemaError> for TableError {
    fn from(value: SchemaError) -> Self {
        Self::Schema(value.to_string())
    }
}

#[derive(Debug, Default)]
pub struct Table {
    inner: TableImpl,
}

impl Table {
    pub fn new() -> Self {
        Self {
            inner: TableImpl::new(),
        }
    }

    pub fn with_schema(schema: TableSchema) -> Self {
        let mut inner = TableImpl::new();
        inner.set_schema(Some(schema));
        Self { inner }
    }

    pub fn from_rows(rows: Vec<RecordValue>) -> Self {
        Self {
            inner: TableImpl::from_rows(rows),
        }
    }

    pub fn from_rows_with_schema(
        rows: Vec<RecordValue>,
        schema: TableSchema,
    ) -> Result<Self, TableError> {
        let table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                rows,
                RecordValue::default(),
                std::collections::HashMap::new(),
                Some(schema),
            ),
        };
        table.validate()?;
        Ok(table)
    }

    pub fn open(options: TableOptions) -> Result<Self, TableError> {
        let storage = CompositeStorage;
        let snapshot = storage.load(&options.path)?;
        let table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                snapshot.rows,
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
            ),
        };
        table.validate()?;
        Ok(table)
    }

    pub fn save(&self, options: TableOptions) -> Result<(), TableError> {
        self.validate()?;
        let snapshot = StorageSnapshot {
            rows: self.inner.rows().to_vec(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
        };
        let storage = CompositeStorage;
        storage.save(&options.path, &snapshot, options.data_manager)?;
        Ok(())
    }

    pub fn schema(&self) -> Option<&TableSchema> {
        self.inner.schema()
    }

    pub fn set_schema(&mut self, schema: TableSchema) -> Result<(), TableError> {
        let previous = self.inner.schema().cloned();
        self.inner.set_schema(Some(schema));
        if let Err(err) = self.validate() {
            self.inner.set_schema(previous);
            return Err(err);
        }
        Ok(())
    }

    pub fn clear_schema(&mut self) {
        self.inner.set_schema(None);
    }

    pub fn validate(&self) -> Result<(), TableError> {
        let Some(schema) = self.schema() else {
            return Ok(());
        };

        for (row_index, row) in self.rows().iter().enumerate() {
            validate_row_against_schema(row_index, row, schema)?;
        }
        Ok(())
    }

    pub fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    pub fn rows(&self) -> &[RecordValue] {
        self.inner.rows()
    }

    pub fn add_row(&mut self, row: RecordValue) -> Result<(), TableError> {
        if let Some(schema) = self.schema() {
            validate_row_against_schema(self.row_count(), &row, schema)?;
        }
        self.inner.add_row(row);
        Ok(())
    }

    pub fn row(&self, row_index: usize) -> Option<&RecordValue> {
        self.inner.row(row_index)
    }

    pub fn row_mut(&mut self, row_index: usize) -> Option<&mut RecordValue> {
        self.inner.row_mut(row_index)
    }

    pub fn cell(&self, row_index: usize, column: &str) -> Option<&Value> {
        self.row(row_index).and_then(|row| row.get(column))
    }

    pub fn get_column<'a>(&'a self, column: &'a str) -> Result<ColumnCellIter<'a>, TableError> {
        self.get_column_range(column, RowRange::new(0, self.row_count()))
    }

    pub fn get_column_range<'a>(
        &'a self,
        column: &'a str,
        row_range: RowRange,
    ) -> Result<ColumnCellIter<'a>, TableError> {
        self.require_column(column)?;
        row_range.validate(self.row_count())?;
        Ok(ColumnCellIter {
            row_data: self.rows(),
            column,
            rows: row_range.iter(),
        })
    }

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
            return match self.cell(row_index, column) {
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

        match self.cell(row_index, column) {
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

    pub fn set_record_cell(
        &mut self,
        row_index: usize,
        column: &str,
        value: RecordValue,
    ) -> Result<(), TableError> {
        self.set_cell(row_index, column, Value::Record(value))
    }

    pub fn get_record_column<'a>(
        &'a self,
        column: &'a str,
    ) -> Result<RecordColumnIter<'a>, TableError> {
        self.get_record_column_range(column, RowRange::new(0, self.row_count()))
    }

    pub fn get_record_column_range<'a>(
        &'a self,
        column: &'a str,
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

        for row_index in row_range.iter() {
            match self.rows()[row_index].get(column) {
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
            row_data: self.rows(),
            column,
            rows: row_range.iter(),
            default_missing,
        })
    }

    pub fn put_column<I>(&mut self, column: &str, values: I) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = Value>,
    {
        self.put_column_range(column, RowRange::new(0, self.row_count()), values)
    }

    pub fn put_column_range<I>(
        &mut self,
        column: &str,
        row_range: RowRange,
        values: I,
    ) -> Result<usize, TableError>
    where
        I: IntoIterator<Item = Value>,
    {
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
            self.set_cell(row_index, column, value)?;
            provided += 1;
        }
        if value_iter.next().is_some() {
            return Err(TableError::ColumnWriteTooManyValues { expected });
        }
        Ok(provided)
    }

    pub fn is_cell_defined(&self, row_index: usize, column: &str) -> Result<bool, TableError> {
        self.require_row(row_index)?;
        self.require_column(column)?;
        if self.cell(row_index, column).is_some() {
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

    pub fn array_shape(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<Option<Vec<usize>>, TableError> {
        self.require_row(row_index)?;
        self.require_column(column)?;
        match self.cell(row_index, column) {
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

    pub fn set_cell(
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

        let row_count = self.row_count();
        let row = self.row_mut(row_index).ok_or(TableError::RowOutOfBounds {
            row_index,
            row_count,
        })?;

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
    /// prefer [`get_column`] or [`iter_column_chunks`] which stream lazily.
    pub fn column_cells(&self, column: &str) -> Vec<Option<&Value>> {
        self.rows()
            .iter()
            .map(|record| record.get(column))
            .collect()
    }

    /// Returns a chunked iterator over a column's cells.
    ///
    /// Each iteration yields a `Vec<ColumnCellRef>` of up to `chunk_size` rows.
    /// This avoids materializing the entire column at once while still allowing
    /// batch processing.
    pub fn iter_column_chunks<'a>(
        &'a self,
        column: &'a str,
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
    pub fn get_array_cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<&ArrayValue, TableError> {
        self.require_row(row_index)?;
        match self.cell(row_index, column) {
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

    /// Returns a reference to the scalar value in a cell without cloning.
    pub fn get_scalar_cell(
        &self,
        row_index: usize,
        column: &str,
    ) -> Result<&ScalarValue, TableError> {
        self.require_row(row_index)?;
        match self.cell(row_index, column) {
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

    pub fn keywords(&self) -> &RecordValue {
        self.inner.keywords()
    }

    pub fn keywords_mut(&mut self) -> &mut RecordValue {
        self.inner.keywords_mut()
    }

    pub fn column_keywords(&self, column: &str) -> Option<&RecordValue> {
        self.inner.column_keywords(column)
    }

    pub fn set_column_keywords(&mut self, column: impl Into<String>, keywords: RecordValue) {
        self.inner.set_column_keywords(column.into(), keywords);
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

    fn require_row(&self, row_index: usize) -> Result<(), TableError> {
        if self.row(row_index).is_some() {
            Ok(())
        } else {
            Err(TableError::RowOutOfBounds {
                row_index,
                row_count: self.row_count(),
            })
        }
    }
}

fn validate_row_against_schema(
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

fn validate_cell_against_schema_column(
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use casacore_types::{
        Array2, ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
    };

    use crate::schema::{ColumnSchema, TableSchema};

    use super::{RowRange, Table, TableError, TableOptions};

    #[test]
    fn table_keeps_rows_in_order() {
        let first = RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )]);
        let second = RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(2)),
        )]);

        let table = Table::from_rows(vec![first.clone(), second.clone()]);
        assert_eq!(table.row_count(), 2);
        assert_eq!(table.rows(), &[first, second]);
    }

    #[test]
    fn table_exposes_row_and_column_cell_access() {
        let first = RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("a".to_string()))),
        ]);
        let second = RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("b".to_string()))),
        ]);
        let mut table = Table::from_rows(vec![first.clone(), second.clone()]);

        assert_eq!(table.row(0), Some(&first));
        assert_eq!(
            table.cell(1, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(2)))
        );

        table
            .set_cell(
                1,
                "name",
                Value::Scalar(ScalarValue::String("beta".to_string())),
            )
            .expect("set cell");
        assert_eq!(
            table.cell(1, "name"),
            Some(&Value::Scalar(ScalarValue::String("beta".to_string())))
        );

        let id_cells = table.column_cells("id");
        assert_eq!(
            id_cells,
            vec![
                Some(&Value::Scalar(ScalarValue::Int32(1))),
                Some(&Value::Scalar(ScalarValue::Int32(2))),
            ]
        );
    }

    #[test]
    fn column_range_iteration_supports_stride() {
        let rows = (0..6)
            .map(|value| {
                RecordValue::new(vec![RecordField::new(
                    "id",
                    Value::Scalar(ScalarValue::Int32(value)),
                )])
            })
            .collect();
        let table = Table::from_rows(rows);

        let cells: Vec<(usize, Option<Value>)> = table
            .get_column_range("id", RowRange::with_stride(1, 6, 2))
            .expect("get strided range")
            .map(|cell| (cell.row_index, cell.value.cloned()))
            .collect();
        assert_eq!(
            cells,
            vec![
                (1, Some(Value::Scalar(ScalarValue::Int32(1)))),
                (3, Some(Value::Scalar(ScalarValue::Int32(3)))),
                (5, Some(Value::Scalar(ScalarValue::Int32(5)))),
            ]
        );
    }

    #[test]
    fn column_range_rejects_invalid_ranges() {
        let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(1)),
        )])]);

        let bad_stride = table.get_column_range("id", RowRange::with_stride(0, 1, 0));
        assert!(matches!(
            bad_stride,
            Err(TableError::InvalidRowStride { stride: 0 })
        ));

        let bad_end = table.get_column_range("id", RowRange::new(0, 2));
        assert!(matches!(
            bad_end,
            Err(TableError::InvalidRowRange {
                start: 0,
                end: 2,
                row_count: 1,
            })
        ));
    }

    #[test]
    fn schema_record_cell_defaults_to_empty_record_when_missing() {
        let schema =
            TableSchema::new(vec![ColumnSchema::record("meta")]).expect("create record schema");
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![]))
            .expect("missing record cell should be valid");

        assert_eq!(table.record_cell(0, "meta"), Ok(RecordValue::default()));
        assert_eq!(table.is_cell_defined(0, "meta"), Ok(true));
    }

    #[test]
    fn record_cell_requires_present_value_without_schema() {
        let table = Table::from_rows(vec![RecordValue::new(vec![])]);
        assert_eq!(
            table.record_cell(0, "meta"),
            Err(TableError::ColumnNotFound {
                row_index: 0,
                column: "meta".to_string(),
            })
        );
    }

    #[test]
    fn record_cell_rejects_non_record_schema_column() {
        let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)])
            .expect("create scalar schema");
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(7)),
            )]))
            .expect("push schema-compliant row");

        assert_eq!(
            table.record_cell(0, "id"),
            Err(TableError::SchemaColumnNotRecord {
                column: "id".to_string(),
            })
        );
    }

    #[test]
    fn record_column_range_defaults_missing_cells_for_record_schema() {
        let schema =
            TableSchema::new(vec![ColumnSchema::record("meta")]).expect("create record schema");

        let first = RecordValue::new(vec![RecordField::new(
            "flag",
            Value::Scalar(ScalarValue::Bool(true)),
        )]);
        let second = RecordValue::new(vec![RecordField::new(
            "flag",
            Value::Scalar(ScalarValue::Bool(false)),
        )]);
        let rows = vec![
            RecordValue::new(vec![RecordField::new("meta", Value::Record(first.clone()))]),
            RecordValue::new(vec![]),
            RecordValue::new(vec![RecordField::new(
                "meta",
                Value::Record(second.clone()),
            )]),
        ];
        let table = Table::from_rows_with_schema(rows, schema).expect("schema-valid rows");

        let cells: Vec<(usize, RecordValue)> = table
            .get_record_column_range("meta", RowRange::new(0, 3))
            .expect("iterate record column")
            .map(|cell| (cell.row_index, cell.value))
            .collect();

        assert_eq!(
            cells,
            vec![(0, first), (1, RecordValue::default()), (2, second),]
        );
    }

    #[test]
    fn record_column_range_without_schema_requires_all_rows_present() {
        let record = RecordValue::new(vec![RecordField::new(
            "meta",
            Value::Record(RecordValue::default()),
        )]);
        let table = Table::from_rows(vec![record, RecordValue::new(vec![])]);

        assert_eq!(
            table.get_record_column("meta").map(|iter| iter.count()),
            Err(TableError::ColumnNotFound {
                row_index: 1,
                column: "meta".to_string(),
            })
        );
    }

    #[test]
    fn record_column_range_rejects_non_record_cells() {
        let table = Table::from_rows(vec![
            RecordValue::new(vec![RecordField::new(
                "meta",
                Value::Record(RecordValue::default()),
            )]),
            RecordValue::new(vec![RecordField::new(
                "meta",
                Value::Scalar(ScalarValue::Int32(9)),
            )]),
        ]);

        assert_eq!(
            table.get_record_column("meta").map(|iter| iter.count()),
            Err(TableError::ColumnTypeMismatch {
                row_index: 1,
                column: "meta".to_string(),
                expected: "record",
                found: casacore_types::ValueKind::Scalar,
            })
        );
    }

    #[test]
    fn set_record_cell_updates_row() {
        let schema =
            TableSchema::new(vec![ColumnSchema::record("meta")]).expect("create record schema");
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![]))
            .expect("push schema-compliant row");
        let payload = RecordValue::new(vec![RecordField::new(
            "code",
            Value::Scalar(ScalarValue::Int32(42)),
        )]);

        table
            .set_record_cell(0, "meta", payload.clone())
            .expect("set record cell");
        assert_eq!(table.record_cell(0, "meta"), Ok(payload));
    }

    #[test]
    fn put_column_range_streams_values_without_column_vecs() {
        let mut table = Table::from_rows(vec![
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("a".to_string())),
            )]),
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("b".to_string())),
            )]),
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("c".to_string())),
            )]),
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("d".to_string())),
            )]),
        ]);

        let written = table
            .put_column_range(
                "name",
                RowRange::with_stride(0, 4, 2),
                ["x", "y"]
                    .into_iter()
                    .map(|value| Value::Scalar(ScalarValue::String(value.to_string()))),
            )
            .expect("put strided range");
        assert_eq!(written, 2);
        assert_eq!(
            table.cell(0, "name"),
            Some(&Value::Scalar(ScalarValue::String("x".to_string())))
        );
        assert_eq!(
            table.cell(1, "name"),
            Some(&Value::Scalar(ScalarValue::String("b".to_string())))
        );
        assert_eq!(
            table.cell(2, "name"),
            Some(&Value::Scalar(ScalarValue::String("y".to_string())))
        );
        assert_eq!(
            table.cell(3, "name"),
            Some(&Value::Scalar(ScalarValue::String("d".to_string())))
        );
    }

    #[test]
    fn put_column_range_checks_value_count() {
        let mut table = Table::from_rows(vec![
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("a".to_string())),
            )]),
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("b".to_string())),
            )]),
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("c".to_string())),
            )]),
        ]);

        let too_few = table.put_column_range(
            "name",
            RowRange::new(0, 3),
            ["x", "y"]
                .into_iter()
                .map(|value| Value::Scalar(ScalarValue::String(value.to_string()))),
        );
        assert_eq!(
            too_few,
            Err(TableError::ColumnWriteTooFewValues {
                expected: 3,
                provided: 2,
            })
        );

        let mut table = Table::from_rows(vec![
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("a".to_string())),
            )]),
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("b".to_string())),
            )]),
            RecordValue::new(vec![RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("c".to_string())),
            )]),
        ]);
        let too_many = table.put_column_range(
            "name",
            RowRange::new(0, 3),
            ["x", "y", "z", "w"]
                .into_iter()
                .map(|value| Value::Scalar(ScalarValue::String(value.to_string()))),
        );
        assert_eq!(
            too_many,
            Err(TableError::ColumnWriteTooManyValues { expected: 3 })
        );
    }

    #[test]
    fn fixed_array_schema_enforces_defined_shape() {
        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Int32,
            vec![2],
        )])
        .expect("create schema");
        let mut table = Table::with_schema(schema);

        let missing = table.add_row(RecordValue::new(vec![]));
        assert_eq!(
            missing,
            Err(TableError::SchemaColumnMissing {
                row_index: 0,
                column: "data".to_string(),
            })
        );

        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::from_i32_vec(vec![1, 2])),
            )]))
            .expect("push valid fixed-shape row");

        let wrong_shape = table.add_row(RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_i32_vec(vec![3])),
        )]));
        assert_eq!(
            wrong_shape,
            Err(TableError::ArrayShapeMismatch {
                row_index: 1,
                column: "data".to_string(),
                expected: vec![2],
                found: vec![1],
            })
        );
    }

    #[test]
    fn variable_array_schema_allows_undefined_and_checks_ndim() {
        let schema = TableSchema::new(vec![ColumnSchema::array_variable(
            "payload",
            PrimitiveType::Int32,
            Some(1),
        )])
        .expect("schema");
        let mut table = Table::with_schema(schema);

        table
            .add_row(RecordValue::new(vec![]))
            .expect("undefined variable-shape cell should be allowed");

        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "payload",
                Value::Array(ArrayValue::from_i32_vec(vec![1, 2, 3])),
            )]))
            .expect("1d array should satisfy ndim=1");

        let two_d = Array2::from_shape_vec((1, 2), vec![4, 5])
            .expect("shape")
            .into_dyn();
        let error = table.set_cell(0, "payload", Value::Array(ArrayValue::Int32(two_d)));
        assert_eq!(
            error,
            Err(TableError::ArrayNdimMismatch {
                row_index: 0,
                column: "payload".to_string(),
                expected: 1,
                found: 2,
            })
        );
    }

    #[test]
    fn table_schema_round_trips_through_disk_storage() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::array_fixed("data", PrimitiveType::Int32, vec![2]),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema.clone());
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(42))),
                RecordField::new("data", Value::Array(ArrayValue::from_i32_vec(vec![7, 9]))),
            ]))
            .expect("push schema-compliant row");
        table.keywords_mut().push(RecordField::new(
            "observer",
            Value::Scalar(ScalarValue::String("rust-test".to_string())),
        ));

        let root = unique_test_dir("table_schema_round_trip");
        std::fs::create_dir_all(&root).expect("create test dir");

        table
            .save(TableOptions::new(&root))
            .expect("save disk-backed table");
        let reopened = Table::open(TableOptions::new(&root)).expect("open disk-backed table");

        assert_eq!(reopened.row_count(), 1);
        assert_eq!(reopened.schema(), Some(&schema));
        assert_eq!(
            reopened.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(42)))
        );
        assert_eq!(
            reopened.keywords().get("observer"),
            Some(&Value::Scalar(ScalarValue::String("rust-test".to_string())))
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }

    #[test]
    fn table_keywords_round_trip_through_disk_storage() {
        let schema = TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)])
            .expect("schema");
        let mut table = Table::from_rows_with_schema(
            vec![RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(42)),
            )])],
            schema,
        )
        .expect("create table");
        table.keywords_mut().push(RecordField::new(
            "observer",
            Value::Scalar(ScalarValue::String("rust-test".to_string())),
        ));

        let root = unique_test_dir("table_keywords_round_trip");
        std::fs::create_dir_all(&root).expect("create test dir");

        table
            .save(TableOptions::new(&root))
            .expect("save disk-backed table");
        let reopened = Table::open(TableOptions::new(&root)).expect("open disk-backed table");

        assert_eq!(reopened.row_count(), 1);
        assert_eq!(
            reopened.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(42)))
        );
        assert_eq!(
            reopened.keywords().get("observer"),
            Some(&Value::Scalar(ScalarValue::String("rust-test".to_string())))
        );

        std::fs::remove_dir_all(&root).expect("cleanup test dir");
    }

    #[test]
    fn iter_column_chunks_batches_rows() {
        let rows: Vec<RecordValue> = (0..7)
            .map(|v| {
                RecordValue::new(vec![RecordField::new(
                    "id",
                    Value::Scalar(ScalarValue::Int32(v)),
                )])
            })
            .collect();
        let table = Table::from_rows(rows);

        let chunks: Vec<Vec<(usize, i32)>> = table
            .iter_column_chunks("id", RowRange::new(0, 7), 3)
            .expect("chunk iter")
            .map(|chunk| {
                chunk
                    .into_iter()
                    .map(|cell| {
                        let v = match cell.value {
                            Some(Value::Scalar(ScalarValue::Int32(n))) => n,
                            _ => panic!("expected i32"),
                        };
                        (cell.row_index, *v)
                    })
                    .collect()
            })
            .collect();

        assert_eq!(
            chunks,
            vec![
                vec![(0, 0), (1, 1), (2, 2)],
                vec![(3, 3), (4, 4), (5, 5)],
                vec![(6, 6)],
            ]
        );
    }

    #[test]
    fn iter_column_chunks_with_stride() {
        let rows: Vec<RecordValue> = (0..6)
            .map(|v| {
                RecordValue::new(vec![RecordField::new(
                    "id",
                    Value::Scalar(ScalarValue::Int32(v)),
                )])
            })
            .collect();
        let table = Table::from_rows(rows);

        let chunks: Vec<Vec<usize>> = table
            .iter_column_chunks("id", RowRange::with_stride(0, 6, 2), 2)
            .expect("chunk iter")
            .map(|chunk| chunk.into_iter().map(|cell| cell.row_index).collect())
            .collect();

        assert_eq!(chunks, vec![vec![0, 2], vec![4]]);
    }

    #[test]
    fn get_array_cell_returns_borrow() {
        let array = ArrayValue::from_f64_vec(vec![1.0, 2.0, 3.0]);
        let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(array.clone()),
        )])]);

        let borrowed = table.get_array_cell(0, "data").expect("get array cell");
        assert_eq!(borrowed, &array);
    }

    #[test]
    fn get_array_cell_rejects_non_array() {
        let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(42)),
        )])]);

        assert!(matches!(
            table.get_array_cell(0, "id"),
            Err(TableError::ColumnTypeMismatch { .. })
        ));
    }

    #[test]
    fn get_array_cell_rejects_missing() {
        let table = Table::from_rows(vec![RecordValue::new(vec![])]);

        assert!(matches!(
            table.get_array_cell(0, "data"),
            Err(TableError::ColumnNotFound { .. })
        ));
    }

    #[test]
    fn get_scalar_cell_returns_borrow() {
        let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(42)),
        )])]);

        let borrowed = table.get_scalar_cell(0, "id").expect("get scalar cell");
        assert_eq!(borrowed, &ScalarValue::Int32(42));
    }

    #[test]
    fn get_scalar_cell_rejects_non_scalar() {
        let table = Table::from_rows(vec![RecordValue::new(vec![RecordField::new(
            "data",
            Value::Array(ArrayValue::from_i32_vec(vec![1, 2])),
        )])]);

        assert!(matches!(
            table.get_scalar_cell(0, "data"),
            Err(TableError::ColumnTypeMismatch { .. })
        ));
    }

    fn unique_test_dir(prefix: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir().join(format!("casacore_tables_{prefix}_{nanos}"))
    }
}
