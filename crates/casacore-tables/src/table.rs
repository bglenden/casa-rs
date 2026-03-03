// SPDX-License-Identifier: LGPL-3.0-or-later
use std::path::{Path, PathBuf};

use casacore_types::{ArrayValue, RecordValue, ScalarValue, Value, ValueKind};
use thiserror::Error;

use crate::schema::{ArrayShapeContract, ColumnSchema, ColumnType, SchemaError, TableSchema};
use crate::storage::{CompositeStorage, StorageManager, StorageSnapshot};
use crate::table_impl::TableImpl;

/// Which data manager to use when writing table data.
///
/// This choice is recorded in the table descriptor on disk so that C++ casacore
/// can select the correct storage-manager plugin when reopening the table.
/// Both variants produce files that are binary-compatible with upstream casacore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DataManagerKind {
    /// Simple whole-column AipsIO streaming (legacy format).
    ///
    /// Each column is stored as a single flat AipsIO stream. This is the
    /// simplest on-disk layout and is compatible with older versions of
    /// casacore. It is the default for this crate because it requires no
    /// extra configuration and produces self-contained files.
    #[default]
    StManAipsIO,
    /// Bucket-based storage (the default in C++ casacore).
    ///
    /// Data is partitioned into fixed-size buckets, which allows efficient
    /// random access and in-place updates. This is the storage manager used
    /// by default when creating tables with C++ casacore.
    StandardStMan,
}

/// Configuration for opening or saving a [`Table`] to disk.
///
/// `TableOptions` bundles the filesystem path with the choice of storage
/// manager. In C++ casacore this information is passed directly to the
/// `Table(name, option)` constructor. Here it is factored out into a
/// separate builder so that callers can construct options independently of
/// the table itself.
///
/// # Example
///
/// ```rust
/// use casacore_tables::{TableOptions, DataManagerKind};
///
/// let opts = TableOptions::new("/tmp/my_table")
///     .with_data_manager(DataManagerKind::StandardStMan);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    path: PathBuf,
    data_manager: DataManagerKind,
}

impl TableOptions {
    /// Creates options targeting `path` with the default data manager ([`DataManagerKind::StManAipsIO`]).
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            data_manager: DataManagerKind::default(),
        }
    }

    /// Overrides the data manager, returning the updated options.
    pub fn with_data_manager(mut self, kind: DataManagerKind) -> Self {
        self.data_manager = kind;
        self
    }

    /// Returns the filesystem path for this table.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the data manager that will be used when saving.
    pub fn data_manager(&self) -> DataManagerKind {
        self.data_manager
    }
}

/// A contiguous, optionally strided range of row indices.
///
/// `RowRange` selects which rows participate in a column read or write
/// operation. It is the row-axis analogue of C++ casacore's `Slicer`.
///
/// # Semantics
///
/// The range is half-open: rows are selected from `start` (inclusive) to
/// `end` (exclusive) stepping by `stride`. For example, `RowRange::with_stride(0, 6, 2)`
/// selects rows 0, 2, and 4.
///
/// # Example
///
/// ```rust
/// use casacore_tables::RowRange;
///
/// // Every row in a 10-row table:
/// let all = RowRange::new(0, 10);
///
/// // Every other row:
/// let evens = RowRange::with_stride(0, 10, 2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    start: usize,
    end: usize,
    stride: usize,
}

impl RowRange {
    /// Creates a contiguous range `[start, end)` with stride 1.
    pub const fn new(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
            stride: 1,
        }
    }

    /// Creates a strided range `[start, end)` stepping by `stride`.
    pub const fn with_stride(start: usize, end: usize, stride: usize) -> Self {
        Self { start, end, stride }
    }

    /// Returns the first row index (inclusive).
    pub const fn start(&self) -> usize {
        self.start
    }

    /// Returns the past-the-end row index (exclusive).
    pub const fn end(&self) -> usize {
        self.end
    }

    /// Returns the step between successive selected rows.
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

/// A borrowed reference to a single cell value together with its row index.
///
/// `ColumnCellRef` is the item type yielded by [`ColumnCellIter`] and
/// [`ColumnChunkIter`]. The `value` field is `None` when the cell is absent
/// from the row (possible for columns that allow undefined cells).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ColumnCellRef<'a> {
    /// Zero-based index of the row from which this cell was read.
    pub row_index: usize,
    /// The cell value, or `None` if the cell is absent for this row.
    pub value: Option<&'a Value>,
}

/// An owned record cell value together with its row index.
///
/// `RecordColumnCell` is the item type yielded by [`RecordColumnIter`]. Unlike
/// [`ColumnCellRef`], the value is always present: missing record cells are
/// substituted with an empty [`RecordValue`] when the column schema permits
/// absent cells.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordColumnCell {
    /// Zero-based index of the row from which this cell was read.
    pub row_index: usize,
    /// The record value for this row (never absent; defaults to empty record).
    pub value: RecordValue,
}

/// An iterator over cells in a single column, yielding one [`ColumnCellRef`] per row.
///
/// Obtain a `ColumnCellIter` via [`Table::get_column`] or [`Table::get_column_range`].
/// The iterator borrows the table's row data and does not allocate per cell.
/// For batch processing, see [`ColumnChunkIter`] via [`Table::iter_column_chunks`].
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

/// An iterator over cells in a record column, yielding one [`RecordColumnCell`] per row.
///
/// Obtain a `RecordColumnIter` via [`Table::get_record_column`] or
/// [`Table::get_record_column_range`]. When a [`TableSchema`] is attached and
/// the column is typed as [`crate::schema::ColumnType::Record`], rows whose
/// record cell is absent yield an empty [`RecordValue`] rather than an error.
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

/// An iterator that yields column cells in fixed-size batches.
///
/// Each call to `next` returns a `Vec<ColumnCellRef>` containing up to
/// `chunk_size` cells. The final chunk may be smaller if the remaining row
/// count is not a multiple of `chunk_size`. An empty table (or an exhausted
/// range) causes `next` to return `None` immediately.
///
/// Obtain a `ColumnChunkIter` via [`Table::iter_column_chunks`]. This iterator
/// is useful for processing columns in memory-bounded passes without
/// materializing the entire column at once.
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

/// Errors that can occur when reading or writing a [`Table`].
#[derive(Debug, Error, PartialEq, Eq)]
pub enum TableError {
    /// A row index exceeded the number of rows in the table.
    #[error("row index {row_index} is out of bounds for table with {row_count} rows")]
    RowOutOfBounds { row_index: usize, row_count: usize },
    /// A [`RowRange`] had `start > end` or `end > row_count`.
    #[error("row range [{start}, {end}) is invalid for table with {row_count} rows")]
    InvalidRowRange {
        start: usize,
        end: usize,
        row_count: usize,
    },
    /// A [`RowRange`] stride was zero, which would produce an infinite loop.
    #[error("row stride must be >= 1, got {stride}")]
    InvalidRowStride { stride: usize },
    /// The requested column was absent from the row (and no schema default applies).
    #[error("column \"{column}\" not found in row {row_index}")]
    ColumnNotFound { row_index: usize, column: String },
    /// The column name is not declared in the attached [`TableSchema`].
    #[error("column \"{column}\" does not exist in schema")]
    SchemaColumnUnknown { column: String },
    /// A record-specific operation was attempted on a non-record schema column.
    #[error("column \"{column}\" is not a record column in schema")]
    SchemaColumnNotRecord { column: String },
    /// A required column (non-optional per schema) was absent from a row.
    #[error("schema column \"{column}\" is missing in row {row_index}")]
    SchemaColumnMissing { row_index: usize, column: String },
    /// A row contained a column name that is not declared in the schema.
    #[error("row {row_index} contains unknown column \"{column}\" not present in schema")]
    RowContainsUnknownColumn { row_index: usize, column: String },
    /// A cell held a value of the wrong type for its schema column.
    #[error(
        "row {row_index} column \"{column}\" has unexpected type {found:?}; expected {expected}"
    )]
    ColumnTypeMismatch {
        row_index: usize,
        column: String,
        expected: &'static str,
        found: ValueKind,
    },
    /// An array cell had a shape that did not match the schema's fixed shape.
    #[error("row {row_index} column \"{column}\" has shape {found:?}; expected {expected:?}")]
    ArrayShapeMismatch {
        row_index: usize,
        column: String,
        expected: Vec<usize>,
        found: Vec<usize>,
    },
    /// An array cell had a number of dimensions that did not match the schema.
    #[error("row {row_index} column \"{column}\" has ndim {found}; expected {expected}")]
    ArrayNdimMismatch {
        row_index: usize,
        column: String,
        expected: usize,
        found: usize,
    },
    /// [`Table::put_column`] or [`Table::put_column_range`] received fewer values than rows.
    #[error("column write received too few values: expected {expected}, provided {provided}")]
    ColumnWriteTooFewValues { expected: usize, provided: usize },
    /// [`Table::put_column`] or [`Table::put_column_range`] received more values than rows.
    #[error("column write received too many values: expected {expected}")]
    ColumnWriteTooManyValues { expected: usize },
    /// A [`SchemaError`][crate::schema::SchemaError] was encountered during validation.
    #[error("schema error: {0}")]
    Schema(String),
    /// An I/O or storage-manager error occurred during [`Table::open`] or [`Table::save`].
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

/// The primary in-memory representation of a casacore table.
///
/// A casacore table is a rectangular data structure: a sequence of rows, each
/// of which is a record (map from column name to value). In addition to row
/// data, a table carries table-level keywords, per-column keywords, and an
/// optional [`TableSchema`] that constrains cell types and array shapes.
///
/// # Relationship to C++ casacore
///
/// In C++ casacore the same functionality is split across several classes:
///
/// | C++ class | Role |
/// |-----------|------|
/// | `Table` | Open/save; row count and keywords |
/// | `ScalarColumn<T>` | Read/write typed scalar columns |
/// | `ArrayColumn<T>` | Read/write typed array columns |
/// | `TableRecord` | Keyword records |
///
/// The Rust `Table` type unifies all of these into a single, dynamically
/// typed interface. Column type safety is enforced at runtime by the methods
/// ([`get_scalar_cell`][Table::get_scalar_cell],
/// [`get_array_cell`][Table::get_array_cell], etc.) rather than through
/// compile-time generics.
///
/// # Construction
///
/// | Method | When to use |
/// |--------|-------------|
/// | [`Table::new`] | Empty table, no schema |
/// | [`Table::with_schema`] | Empty table with a column schema |
/// | [`Table::from_rows`] | Pre-built rows, no schema |
/// | [`Table::from_rows_with_schema`] | Pre-built rows validated against a schema |
///
/// # Persistence
///
/// Use [`Table::save`] with a [`TableOptions`] to write to disk, and
/// [`Table::open`] to read back. The on-disk format is binary-compatible with
/// C++ casacore for both [`DataManagerKind::StManAipsIO`] and
/// [`DataManagerKind::StandardStMan`].
///
/// # Example
///
/// ```rust
/// use casacore_tables::{Table, TableOptions, RowRange};
/// use casacore_types::{RecordValue, RecordField, Value, ScalarValue};
///
/// let row = RecordValue::new(vec![
///     RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
/// ]);
/// let table = Table::from_rows(vec![row]);
/// assert_eq!(table.row_count(), 1);
/// ```
#[derive(Debug, Default)]
pub struct Table {
    inner: TableImpl,
}

impl Table {
    /// Creates a new, empty table with no rows, no schema, and no keywords.
    pub fn new() -> Self {
        Self {
            inner: TableImpl::new(),
        }
    }

    /// Creates a new, empty table with the given schema but no rows.
    ///
    /// Rows added later via [`add_row`][Table::add_row] will be validated
    /// against this schema.
    pub fn with_schema(schema: TableSchema) -> Self {
        let mut inner = TableImpl::new();
        inner.set_schema(Some(schema));
        Self { inner }
    }

    /// Creates a table from an existing `Vec` of rows without schema validation.
    ///
    /// This is a low-cost constructor: the `Vec` is moved in directly. No
    /// schema is attached, so any column structure is accepted.
    pub fn from_rows(rows: Vec<RecordValue>) -> Self {
        Self {
            inner: TableImpl::from_rows(rows),
        }
    }

    /// Creates a table from an existing `Vec` of rows, validated against `schema`.
    ///
    /// Returns [`TableError`] if any row violates the schema. On success, the
    /// schema is stored and future mutations are also validated.
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

    /// Opens an existing table from disk.
    ///
    /// Reads all rows, keywords, column keywords, and schema from the directory
    /// identified by `options.path()`. After loading, the table is validated
    /// against its schema (if one was persisted). Returns [`TableError::Storage`]
    /// if the directory cannot be read, or a schema error if the on-disk data
    /// violates the stored schema.
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

    /// Saves the table to disk.
    ///
    /// Validates the table against its schema (if any), then writes all rows,
    /// keywords, column keywords, and schema to the directory specified by
    /// `options.path()`. The data manager format is determined by
    /// `options.data_manager()`. The directory need not exist beforehand;
    /// the storage layer creates it.
    ///
    /// Returns [`TableError::Storage`] on I/O failure.
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
        let previous = self.inner.schema().cloned();
        self.inner.set_schema(Some(schema));
        if let Err(err) = self.validate() {
            self.inner.set_schema(previous);
            return Err(err);
        }
        Ok(())
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

        for (row_index, row) in self.rows().iter().enumerate() {
            validate_row_against_schema(row_index, row, schema)?;
        }
        Ok(())
    }

    /// Returns the number of rows in the table.
    pub fn row_count(&self) -> usize {
        self.inner.row_count()
    }

    /// Returns a slice over all rows in insertion order.
    pub fn rows(&self) -> &[RecordValue] {
        self.inner.rows()
    }

    /// Appends a row to the table.
    ///
    /// If a schema is attached, the row is validated before insertion.
    /// Returns [`TableError`] if the row violates the schema; the table is
    /// left unchanged in that case.
    pub fn add_row(&mut self, row: RecordValue) -> Result<(), TableError> {
        if let Some(schema) = self.schema() {
            validate_row_against_schema(self.row_count(), &row, schema)?;
        }
        self.inner.add_row(row);
        Ok(())
    }

    /// Returns a shared reference to the row at `row_index`, or `None` if out of bounds.
    pub fn row(&self, row_index: usize) -> Option<&RecordValue> {
        self.inner.row(row_index)
    }

    /// Returns an exclusive reference to the row at `row_index`, or `None` if out of bounds.
    ///
    /// Direct mutation through this reference bypasses schema validation.
    /// Use [`set_cell`][Table::set_cell] or [`add_row`][Table::add_row] for
    /// validated writes.
    pub fn row_mut(&mut self, row_index: usize) -> Option<&mut RecordValue> {
        self.inner.row_mut(row_index)
    }

    /// Returns a reference to the value at `(row_index, column)`, or `None` if absent.
    ///
    /// Returns `None` both when `row_index` is out of bounds and when the
    /// column is simply absent from the row. Use [`get_scalar_cell`][Table::get_scalar_cell]
    /// or [`get_array_cell`][Table::get_array_cell] for type-checked access with
    /// descriptive errors.
    pub fn cell(&self, row_index: usize, column: &str) -> Option<&Value> {
        self.row(row_index).and_then(|row| row.get(column))
    }

    /// Returns an iterator over every cell in `column`, covering all rows.
    ///
    /// This is a shorthand for `get_column_range(column, RowRange::new(0, row_count()))`.
    /// Returns [`TableError::SchemaColumnUnknown`] if a schema is attached and
    /// `column` is not declared in it.
    pub fn get_column<'a>(&'a self, column: &'a str) -> Result<ColumnCellIter<'a>, TableError> {
        self.get_column_range(column, RowRange::new(0, self.row_count()))
    }

    /// Returns an iterator over the cells in `column` within `row_range`.
    ///
    /// The iterator borrows the table's row data and yields one
    /// [`ColumnCellRef`] per selected row. Returns [`TableError`] if the
    /// column is unknown or the range is invalid.
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

    /// Writes a record value to `(row_index, column)`.
    ///
    /// This is a convenience wrapper around [`set_cell`][Table::set_cell] that
    /// wraps `value` in [`Value::Record`].
    pub fn set_record_cell(
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
    pub fn get_record_column<'a>(
        &'a self,
        column: &'a str,
    ) -> Result<RecordColumnIter<'a>, TableError> {
        self.get_record_column_range(column, RowRange::new(0, self.row_count()))
    }

    /// Returns an iterator over the record cells in `column` within `row_range`.
    ///
    /// Each item is a [`RecordColumnCell`] whose `value` is always populated:
    /// absent cells are defaulted to an empty [`RecordValue`] when the schema
    /// permits it. Returns [`TableError`] if the column is unknown, not a
    /// record column, or a cell has the wrong type.
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

    /// Writes values from an iterator into `column` for all rows.
    ///
    /// Shorthand for `put_column_range(column, RowRange::new(0, row_count()), values)`.
    /// Returns the number of cells written, or [`TableError`] if the value
    /// count does not match the row count.
    pub fn put_column<I>(&mut self, column: &str, values: I) -> Result<usize, TableError>
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

    /// Returns `true` if the cell at `(row_index, column)` is considered defined.
    ///
    /// A cell is defined if a value is present in the row. For record columns
    /// with a schema, a missing cell is still considered defined because it
    /// defaults to an empty record. Returns [`TableError`] if `row_index` is
    /// out of bounds or the column is unknown per the schema.
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

    /// Writes `value` to the cell at `(row_index, column)`.
    ///
    /// When a schema is attached the value is validated against the column
    /// schema before writing; new column names are allowed only if they are
    /// declared in the schema. Without a schema the column must already exist
    /// in the row (use [`add_row`][Table::add_row] to populate new rows first).
    ///
    /// Returns [`TableError`] if the row is out of bounds, the column is
    /// unknown per the schema, or the value violates the column type/shape.
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
    /// prefer [`Table::get_column`] or [`Table::iter_column_chunks`] which stream lazily.
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
