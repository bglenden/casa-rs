// SPDX-License-Identifier: LGPL-3.0-or-later
use std::path::{Path, PathBuf};

use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value, ValueKind};
use thiserror::Error;

#[cfg(unix)]
use crate::lock::LockFile;
use crate::lock::SyncData;
use crate::lock::{LockMode, LockOptions, LockType};
use crate::schema::{ArrayShapeContract, ColumnSchema, ColumnType, SchemaError, TableSchema};
use crate::storage::{CompositeStorage, StorageManager, StorageSnapshot};
use crate::table_impl::TableImpl;

/// Byte-ordering format for on-disk table data.
///
/// Controls how multi-byte values (integers, floats, complex numbers) are
/// stored in the table's data files. The choice is recorded in `table.dat`
/// so that readers can decode values correctly regardless of the host machine's
/// native byte order.
///
/// In C++ casacore this corresponds to `Table::EndianFormat`. The
/// `AipsrcEndian` variant from C++ is intentionally omitted — Rust callers
/// should pass an explicit format instead.
///
/// # Default
///
/// The default is [`LocalEndian`](EndianFormat::LocalEndian), which uses the
/// byte order of the machine that creates the table. This matches the C++
/// casacore default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EndianFormat {
    /// Store data in big-endian (network / "canonical") byte order.
    ///
    /// Corresponds to `Table::BigEndian` in C++ casacore. This was the only
    /// format supported by early casacore versions.
    BigEndian,
    /// Store data in little-endian byte order.
    ///
    /// Corresponds to `Table::LittleEndian` in C++ casacore.
    LittleEndian,
    /// Store data in the byte order of the host machine.
    ///
    /// On x86-64 and ARM64 this resolves to little-endian; on SPARC or
    /// PowerPC (big-endian mode) it resolves to big-endian.
    /// Corresponds to `Table::LocalEndian` in C++ casacore.
    #[default]
    LocalEndian,
}

impl EndianFormat {
    /// Resolves this format to a concrete big-endian flag.
    ///
    /// [`LocalEndian`](EndianFormat::LocalEndian) queries the host at compile
    /// time via `cfg!(target_endian = "big")`.
    pub fn is_big_endian(self) -> bool {
        match self {
            Self::BigEndian => true,
            Self::LittleEndian => false,
            Self::LocalEndian => cfg!(target_endian = "big"),
        }
    }

    /// Converts to the [`ByteOrder`](casacore_aipsio::ByteOrder) enum used
    /// by the lower-level AipsIO codec.
    pub fn to_byte_order(self) -> casacore_aipsio::ByteOrder {
        if self.is_big_endian() {
            casacore_aipsio::ByteOrder::BigEndian
        } else {
            casacore_aipsio::ByteOrder::LittleEndian
        }
    }
}

/// Sort order for table sorting operations.
///
/// Specifies whether a sort key column should be sorted in ascending (smallest
/// first) or descending (largest first) order. Multiple columns with different
/// orders can be combined in a single sort.
///
/// # C++ equivalent
///
/// `Sort::Order`: `Sort::Ascending` (−1) and `Sort::Descending` (1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Sort in ascending order (smallest first).
    Ascending,
    /// Sort in descending order (largest first).
    Descending,
}

/// The kind of table: plain (disk-backed) or memory (transient).
///
/// In C++ casacore this corresponds to `Table::TableType`. A
/// [`Plain`](TableKind::Plain) table can be loaded from and saved to disk
/// with any [`DataManagerKind`]. A [`Memory`](TableKind::Memory) table
/// holds all data exclusively in process memory; it is deleted when
/// dropped.
///
/// Memory tables can be materialized to disk via [`Table::save`], which
/// writes a plain table that is byte-identical to one created directly.
///
/// # C++ equivalent
///
/// `Table::Plain` and `Table::Memory`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableKind {
    /// A regular table, backed by (or destined for) disk storage.
    #[default]
    Plain,
    /// A transient in-memory table.
    ///
    /// All data is lost when the table is dropped. Locking is a no-op:
    /// [`has_lock`](Table::has_lock) always returns `true`, and
    /// [`lock`](Table::lock) / [`unlock`](Table::unlock) succeed without
    /// doing any I/O.
    ///
    /// C++ equivalent: `Table::Memory` / `MemoryTable`.
    Memory,
}

/// Which data manager to use when writing table data.
///
/// This choice is recorded in the table descriptor on disk so that C++ casacore
/// can select the correct storage-manager plugin when reopening the table.
/// All variants produce files that are binary-compatible with upstream casacore.
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
    /// Delta-compression storage manager (C++ `IncrementalStMan` / `ISMBase`).
    ///
    /// Stores column values only when they change from the previous row,
    /// making it extremely space-efficient for slowly-changing columns
    /// such as `ANTENNA1`, `FEED_ID`, or `SCAN_NUMBER` in a
    /// MeasurementSet. Along with `StandardStMan`, ISM is one of the two
    /// most commonly used storage managers in real radio astronomy data.
    IncrementalStMan,
}

/// Configuration for opening or saving a [`Table`] to disk.
///
/// `TableOptions` bundles the filesystem path with the choice of storage
/// manager and byte-ordering format. In C++ casacore this information is
/// passed directly to the `Table(name, option)` constructor. Here it is
/// factored out into a separate builder so that callers can construct
/// options independently of the table itself.
///
/// # Example
///
/// ```rust
/// use casacore_tables::{TableOptions, DataManagerKind, EndianFormat};
///
/// let opts = TableOptions::new("/tmp/my_table")
///     .with_data_manager(DataManagerKind::StandardStMan)
///     .with_endian_format(EndianFormat::BigEndian);
///
/// // Use IncrementalStMan for slowly-changing columns:
/// let ism_opts = TableOptions::new("/tmp/my_ism_table")
///     .with_data_manager(DataManagerKind::IncrementalStMan);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOptions {
    path: PathBuf,
    data_manager: DataManagerKind,
    endian_format: EndianFormat,
}

impl TableOptions {
    /// Creates options targeting `path` with the default data manager
    /// ([`DataManagerKind::StManAipsIO`]) and default endian format
    /// ([`EndianFormat::LocalEndian`]).
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            data_manager: DataManagerKind::default(),
            endian_format: EndianFormat::default(),
        }
    }

    /// Overrides the data manager, returning the updated options.
    pub fn with_data_manager(mut self, kind: DataManagerKind) -> Self {
        self.data_manager = kind;
        self
    }

    /// Overrides the endian format, returning the updated options.
    ///
    /// The endian format controls the byte ordering of multi-byte values
    /// in the table's data files. It is only used when saving; on open the
    /// format is detected from the existing `table.dat` marker.
    pub fn with_endian_format(mut self, format: EndianFormat) -> Self {
        self.endian_format = format;
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

    /// Returns the endian format that will be used when saving.
    pub fn endian_format(&self) -> EndianFormat {
        self.endian_format
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
    /// A lock could not be acquired within the allowed attempts.
    ///
    /// C++ equivalent: lock failure in `TableLockData::makeLock`.
    #[error("lock acquisition failed on \"{path}\": {message}")]
    LockFailed { path: String, message: String },
    /// A locking operation was attempted but the table has no lock state
    /// (opened without locking or created in-memory).
    #[error("table is not opened with locking; cannot {operation}")]
    NotLocked { operation: String },
    /// A lock-file I/O error occurred.
    #[error("lock I/O error on \"{path}\": {message}")]
    LockIo { path: String, message: String },
    /// A row index in a selection is out of range.
    #[error("row index {index} out of range (table has {count} rows)")]
    RowIndexOutOfRange { index: usize, count: usize },
    /// A column name in a selection does not exist.
    #[error("unknown column \"{name}\" in selection")]
    UnknownColumn { name: String },
    /// A reference table's parent could not be found.
    #[error("parent table not found at \"{path}\": {message}")]
    ParentTableNotFound { path: String, message: String },
    /// A reference table requires the parent to have been saved to disk.
    #[error("parent table has no disk path; save it first")]
    ParentNotSaved,
    /// A sort key column contains non-scalar values.
    ///
    /// Only scalar columns can be used as sort keys, matching C++ casacore's
    /// runtime check in `BaseTable::sort`.
    #[error("column \"{column}\" is not a scalar column; only scalar columns can be sort keys")]
    SortKeyNotScalar { column: String },
    /// A sort key column contains values without a total order.
    ///
    /// Complex32 and Complex64 columns have no natural total ordering and
    /// cannot be used as sort keys.
    #[error("column \"{column}\" contains unsortable values (Complex types have no total order)")]
    SortKeyUnsortable { column: String },
    /// No sort key columns were specified.
    #[error("at least one sort key column is required")]
    SortNoKeys,
    /// The tables in a concatenation have incompatible schemas.
    ///
    /// All tables passed to [`Table::concat`] must have identical schemas
    /// (same column names, types, and array shapes).
    ///
    /// C++ equivalent: `ConcatTable` constructor's schema check.
    #[error("concat table schema mismatch: {message}")]
    SchemaMismatch { message: String },
    /// A concatenation was attempted with zero tables.
    ///
    /// [`Table::concat`] requires at least one table.
    #[error("concat requires at least one table")]
    ConcatEmpty,
    /// A constituent table in a concatenation has no disk path.
    ///
    /// All constituent tables must be saved to disk before the `ConcatTable`
    /// can be persisted, since the on-disk format stores relative paths.
    #[error("constituent table {index} has no disk path; save it first")]
    ConstituentNotSaved { index: usize },
    /// No columns were supplied to [`crate::ColumnsIndex::new`].
    #[error("at least one column is required to build a ColumnsIndex")]
    IndexNoColumns,
    /// A column is non-scalar (array/record) and cannot be indexed.
    ///
    /// Only scalar columns can serve as index keys, matching C++ casacore's
    /// `ColumnsIndex` restriction.
    #[error("column \"{column}\" is not a scalar column; only scalar columns can be indexed")]
    IndexColumnNotScalar { column: String },
    /// A column has an unsortable type (Complex32/Complex64) and cannot be indexed.
    ///
    /// Complex types have no total order and therefore cannot be used as index keys.
    #[error("column \"{column}\" contains unsortable values (Complex types have no total order)")]
    IndexColumnUnsortable { column: String },
    /// [`crate::ColumnsIndex::lookup_unique`] found more than one matching row.
    #[error("index lookup_unique found {count} matching rows; expected at most 1")]
    IndexNotUnique { count: usize },
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
/// Internal lock state held by a [`Table`] when opened with locking.
///
/// Stores the open file descriptor (via [`LockFile`]), synchronization
/// counters, and the options needed to re-save and re-load the table on
/// lock transitions.
#[cfg(unix)]
struct LockState {
    path: PathBuf,
    lock_file: LockFile,
    sync_data: SyncData,
    options: LockOptions,
    data_manager: DataManagerKind,
    endian_format: EndianFormat,
}

#[cfg(unix)]
impl std::fmt::Debug for LockState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LockState")
            .field("path", &self.path)
            .field("mode", &self.options.mode)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct Table {
    inner: TableImpl,
    /// Filesystem path this table was last opened from or saved to.
    source_path: Option<PathBuf>,
    /// Whether this is a plain (disk-backed) or memory (transient) table.
    kind: TableKind,
    #[cfg(unix)]
    lock_state: Option<LockState>,
}

impl Table {
    /// Creates a new, empty table with no rows, no schema, and no keywords.
    pub fn new() -> Self {
        Self {
            inner: TableImpl::new(),
            source_path: None,
            kind: TableKind::Plain,
            #[cfg(unix)]
            lock_state: None,
        }
    }

    /// Creates a new, empty table with the given schema but no rows.
    ///
    /// Rows added later via [`add_row`][Table::add_row] will be validated
    /// against this schema.
    pub fn with_schema(schema: TableSchema) -> Self {
        let mut inner = TableImpl::new();
        inner.set_schema(Some(schema));
        Self {
            inner,
            source_path: None,
            kind: TableKind::Plain,
            #[cfg(unix)]
            lock_state: None,
        }
    }

    /// Creates a table from an existing `Vec` of rows without schema validation.
    ///
    /// This is a low-cost constructor: the `Vec` is moved in directly. No
    /// schema is attached, so any column structure is accepted.
    pub fn from_rows(rows: Vec<RecordValue>) -> Self {
        Self {
            inner: TableImpl::from_rows(rows),
            source_path: None,
            kind: TableKind::Plain,
            #[cfg(unix)]
            lock_state: None,
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
            source_path: None,
            kind: TableKind::Plain,
            #[cfg(unix)]
            lock_state: None,
        };
        table.validate()?;
        Ok(table)
    }

    // -----------------------------------------------------------------------
    // Memory-table constructors
    // -----------------------------------------------------------------------

    /// Creates a new, empty memory table with no rows and no schema.
    ///
    /// The table is transient: all data is lost when it is dropped. Locking
    /// operations are no-ops. Use [`save`](Table::save) to materialize the
    /// data to disk as a plain table.
    ///
    /// C++ equivalent: constructing a `Table` with `Table::Memory`.
    pub fn new_memory() -> Self {
        Self {
            inner: TableImpl::new(),
            source_path: None,
            kind: TableKind::Memory,
            #[cfg(unix)]
            lock_state: None,
        }
    }

    /// Creates a new memory table with the given schema but no rows.
    ///
    /// Rows added via [`add_row`](Table::add_row) will be validated against
    /// the schema. The table is transient: all data is lost when dropped.
    ///
    /// C++ equivalent: constructing a `Table` with `Table::Memory` and a
    /// `SetupNewTable` containing a `TableDesc`.
    pub fn with_schema_memory(schema: TableSchema) -> Self {
        let mut inner = TableImpl::new();
        inner.set_schema(Some(schema));
        Self {
            inner,
            source_path: None,
            kind: TableKind::Memory,
            #[cfg(unix)]
            lock_state: None,
        }
    }

    /// Creates a memory table from pre-built rows without schema validation.
    ///
    /// The `Vec` is moved in directly. The table is transient.
    pub fn from_rows_memory(rows: Vec<RecordValue>) -> Self {
        Self {
            inner: TableImpl::from_rows(rows),
            source_path: None,
            kind: TableKind::Memory,
            #[cfg(unix)]
            lock_state: None,
        }
    }

    /// Creates a memory table from rows validated against a schema.
    ///
    /// Returns [`TableError`] if any row violates the schema. On success the
    /// schema is stored and future mutations are validated. The table is
    /// transient.
    pub fn from_rows_with_schema_memory(
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
            source_path: None,
            kind: TableKind::Memory,
            #[cfg(unix)]
            lock_state: None,
        };
        table.validate()?;
        Ok(table)
    }

    // -----------------------------------------------------------------------
    // Table kind and memory conversion
    // -----------------------------------------------------------------------

    /// Returns the kind of this table (plain or memory).
    ///
    /// C++ equivalent: `Table::tableType()`.
    pub fn table_kind(&self) -> TableKind {
        self.kind
    }

    /// Returns `true` if this is a transient in-memory table.
    ///
    /// Equivalent to `self.table_kind() == TableKind::Memory`.
    pub fn is_memory(&self) -> bool {
        self.kind == TableKind::Memory
    }

    /// Creates an in-memory copy of this table.
    ///
    /// All rows, keywords, column keywords, and schema are cloned into a
    /// new table with [`TableKind::Memory`]. The source path and lock state
    /// are not copied.
    ///
    /// This can be called on any table (plain or memory). Calling it on a
    /// memory table produces an independent clone.
    ///
    /// C++ equivalent: `Table::copyToMemoryTable`.
    pub fn to_memory(&self) -> Self {
        Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                self.inner.rows().to_vec(),
                self.inner.keywords().clone(),
                self.inner.all_column_keywords().clone(),
                self.inner.schema().cloned(),
            ),
            source_path: None,
            kind: TableKind::Memory,
            #[cfg(unix)]
            lock_state: None,
        }
    }

    /// Opens an existing table from disk.
    ///
    /// Reads all rows, keywords, column keywords, and schema from the directory
    /// identified by `options.path()`. After loading, the table is validated
    /// against its schema (if one was persisted). Returns [`TableError::Storage`]
    /// if the directory cannot be read, or a schema error if the on-disk data
    /// violates the stored schema.
    ///
    /// If the on-disk table is a reference table (`RefTable` type marker), the
    /// parent table is opened automatically and the referenced rows are
    /// materialized into this table.
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
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            #[cfg(unix)]
            lock_state: None,
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
        storage.save(
            &options.path,
            &snapshot,
            options.data_manager,
            options.endian_format.is_big_endian(),
        )?;
        Ok(())
    }

    /// Returns the filesystem path this table was opened from or saved to,
    /// if any. In-memory tables that have never been persisted return `None`.
    pub fn path(&self) -> Option<&Path> {
        self.source_path.as_deref()
    }

    /// Sets the source path for this table.
    ///
    /// Normally set automatically by [`open`](Table::open) and
    /// [`save`](Table::save). You can call this explicitly before creating
    /// a [`RefTable`](crate::RefTable) that saves to disk, if the table
    /// was constructed in-memory but you want to establish a parent path.
    pub fn set_path(&mut self, path: impl AsRef<Path>) {
        self.source_path = Some(path.as_ref().to_path_buf());
    }

    /// Opens an existing table from disk with locking.
    ///
    /// Behaves like [`open`](Table::open) but also creates or opens the
    /// `table.lock` file and acquires a lock according to the given
    /// [`LockOptions`].
    ///
    /// - [`LockMode::PermanentLocking`]: acquires a write lock immediately;
    ///   fails if unavailable.
    /// - [`LockMode::PermanentLockingWait`]: acquires a write lock, waiting
    ///   indefinitely.
    /// - [`LockMode::AutoLocking`]: acquires a read lock immediately.
    /// - [`LockMode::UserLocking`]: no lock is acquired until
    ///   [`lock()`](Table::lock) is called.
    /// - [`LockMode::NoLocking`]: equivalent to [`open()`](Table::open).
    ///
    /// C++ equivalent: `Table(name, TableLock(...), Table::Old)`.
    #[cfg(unix)]
    pub fn open_with_lock(
        options: TableOptions,
        lock_opts: LockOptions,
    ) -> Result<Self, TableError> {
        if lock_opts.mode == LockMode::NoLocking {
            return Self::open(options);
        }

        let storage = CompositeStorage;
        let snapshot = storage.load(&options.path)?;
        let mut table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                snapshot.rows,
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
            ),
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            lock_state: None,
        };
        table.validate()?;

        let perm = matches!(
            lock_opts.mode,
            LockMode::PermanentLocking | LockMode::PermanentLockingWait
        );
        let mut lock_file =
            LockFile::create_or_open(&options.path, false, lock_opts.inspection_interval, perm)
                .map_err(|e| TableError::LockIo {
                    path: options.path.display().to_string(),
                    message: e.to_string(),
                })?;

        // Acquire initial lock based on mode.
        match lock_opts.mode {
            LockMode::PermanentLocking => {
                if !lock_file
                    .acquire(LockType::Write, 1)
                    .map_err(|e| TableError::LockIo {
                        path: options.path.display().to_string(),
                        message: e.to_string(),
                    })?
                {
                    return Err(TableError::LockFailed {
                        path: options.path.display().to_string(),
                        message: "table is locked by another process".into(),
                    });
                }
            }
            LockMode::PermanentLockingWait => {
                if !lock_file
                    .acquire(LockType::Write, 0)
                    .map_err(|e| TableError::LockIo {
                        path: options.path.display().to_string(),
                        message: e.to_string(),
                    })?
                {
                    return Err(TableError::LockFailed {
                        path: options.path.display().to_string(),
                        message: "could not acquire permanent lock".into(),
                    });
                }
            }
            LockMode::AutoLocking => {
                let _ = lock_file.acquire(LockType::Read, 1);
            }
            LockMode::UserLocking | LockMode::NoLocking => {}
        }

        // Read sync data if available.
        let sync_data = lock_file
            .read_sync_data()
            .map_err(|e| TableError::LockIo {
                path: options.path.display().to_string(),
                message: e.to_string(),
            })?
            .unwrap_or_else(SyncData::new);

        table.lock_state = Some(LockState {
            path: options.path.clone(),
            lock_file,
            sync_data,
            options: lock_opts,
            data_manager: options.data_manager,
            endian_format: options.endian_format,
        });

        Ok(table)
    }

    /// Acquires a lock on the table.
    ///
    /// Re-reads the table data from disk if another process modified it
    /// since the last lock was held.
    ///
    /// `nattempts`: number of lock attempts. 0 means wait indefinitely,
    /// 1 means try once without waiting.
    ///
    /// Returns `true` if the lock was acquired, `false` if it could not
    /// be acquired within the given attempts.
    ///
    /// C++ equivalent: `Table::lock(type, nattempts)`.
    #[cfg(unix)]
    pub fn lock(&mut self, lock_type: LockType, nattempts: u32) -> Result<bool, TableError> {
        // Memory tables always succeed — no file-based locking needed.
        // C++ equivalent: MemoryTable::lock() returns True.
        if self.kind == TableKind::Memory {
            return Ok(true);
        }
        let state = self
            .lock_state
            .as_mut()
            .ok_or_else(|| TableError::NotLocked {
                operation: "lock".into(),
            })?;

        let acquired =
            state
                .lock_file
                .acquire(lock_type, nattempts)
                .map_err(|e| TableError::LockIo {
                    path: state.path.display().to_string(),
                    message: e.to_string(),
                })?;

        if acquired {
            // Read sync data and check if we need to reload.
            if let Some(new_sync) =
                state
                    .lock_file
                    .read_sync_data()
                    .map_err(|e| TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    })?
            {
                if state.sync_data.needs_reload(&new_sync) {
                    // Another process modified the table — reload.
                    let storage = CompositeStorage;
                    let snapshot = storage.load(&state.path).map_err(|e| TableError::LockIo {
                        path: state.path.display().to_string(),
                        message: e.to_string(),
                    })?;
                    self.inner.replace_from_snapshot(
                        snapshot.rows,
                        snapshot.keywords,
                        snapshot.column_keywords,
                        snapshot.schema,
                    );
                    // Update our stored sync data.
                    if let Some(s) = self.lock_state.as_mut() {
                        s.sync_data = new_sync;
                    }
                }
            }
        }

        Ok(acquired)
    }

    /// Releases the current lock.
    ///
    /// If a write lock was held, the table is flushed to disk first and
    /// sync data is updated in the lock file.
    ///
    /// C++ equivalent: `Table::unlock()`.
    #[cfg(unix)]
    pub fn unlock(&mut self) -> Result<(), TableError> {
        // Memory tables have no lock to release.
        // C++ equivalent: MemoryTable::unlock() is a no-op.
        if self.kind == TableKind::Memory {
            return Ok(());
        }
        // Extract the info we need before borrowing self for save/schema.
        let (is_write_locked, save_opts) = {
            let state = self
                .lock_state
                .as_ref()
                .ok_or_else(|| TableError::NotLocked {
                    operation: "unlock".into(),
                })?;
            let wl = state.lock_file.has_lock(LockType::Write);
            let opts = TableOptions::new(&state.path)
                .with_data_manager(state.data_manager)
                .with_endian_format(state.endian_format);
            (wl, opts)
        };

        // If write-locked, flush data to disk.
        if is_write_locked {
            self.save(save_opts)?;

            // Gather sync info from immutable borrows.
            let nrrow = self.row_count() as u64;
            let nrcolumn = self.schema().map(|s| s.columns().len() as u32).unwrap_or(0);

            // Now borrow lock_state mutably for sync data update.
            let state = self.lock_state.as_mut().expect("lock_state present");
            state.sync_data.record_write(nrrow, nrcolumn, true, &[true]);

            state
                .lock_file
                .write_sync_data(&state.sync_data)
                .map_err(|e| TableError::LockIo {
                    path: state.path.display().to_string(),
                    message: e.to_string(),
                })?;
        }

        let state = self.lock_state.as_mut().expect("lock_state present");
        state.lock_file.release().map_err(|e| TableError::LockIo {
            path: state.path.display().to_string(),
            message: e.to_string(),
        })?;

        Ok(())
    }

    /// Returns `true` if the given lock type is currently held.
    ///
    /// Returns `false` if the table was not opened with locking.
    ///
    /// C++ equivalent: `Table::hasLock(type)`.
    #[cfg(unix)]
    pub fn has_lock(&self, lock_type: LockType) -> bool {
        // Memory tables always report holding the lock.
        // C++ equivalent: MemoryTable::hasLock() returns True.
        if self.kind == TableKind::Memory {
            return true;
        }
        self.lock_state
            .as_ref()
            .map(|s| s.lock_file.has_lock(lock_type))
            .unwrap_or(false)
    }

    /// Tests if the table is opened by another process.
    ///
    /// Checks the in-use indicator in the lock file. Returns `false` if the
    /// table was not opened with locking.
    ///
    /// C++ equivalent: `Table::isMultiUsed()`.
    #[cfg(unix)]
    pub fn is_multi_used(&self) -> bool {
        // Memory tables are never shared with another process.
        // C++ equivalent: MemoryTable::isMultiUsed() returns False.
        if self.kind == TableKind::Memory {
            return false;
        }
        self.lock_state
            .as_ref()
            .map(|s| s.lock_file.is_multi_used())
            .unwrap_or(false)
    }

    /// Returns the lock options, if locking is active.
    #[cfg(unix)]
    pub fn lock_options(&self) -> Option<&LockOptions> {
        self.lock_state.as_ref().map(|s| &s.options)
    }

    // -----------------------------------------------------------------------
    // Selection (RefTable creation)
    // -----------------------------------------------------------------------

    /// Creates a reference table containing only the specified rows.
    ///
    /// Row indices are validated against `row_count()`. The returned
    /// [`RefTable`](crate::RefTable) borrows `self` mutably; drop it to
    /// regain access to the parent.
    ///
    /// C++ equivalent: constructing a `RefTable` from a `Vector<rownr_t>`.
    pub fn select_rows(&mut self, indices: &[usize]) -> Result<crate::RefTable<'_>, TableError> {
        crate::RefTable::from_rows(self, indices.to_vec())
    }

    /// Creates a reference table containing only the named columns.
    ///
    /// All rows are included. Column names are validated against the schema.
    ///
    /// C++ equivalent: constructing a `RefTable` from a `Vector<String>`.
    pub fn select_columns(&mut self, names: &[&str]) -> Result<crate::RefTable<'_>, TableError> {
        crate::RefTable::from_columns(self, names)
    }

    /// Creates a reference table containing rows that satisfy `predicate`.
    ///
    /// Iterates all rows, calling `predicate` on each. Rows for which the
    /// closure returns `true` are included in the view.
    pub fn select<F>(&mut self, predicate: F) -> crate::RefTable<'_>
    where
        F: Fn(&RecordValue) -> bool,
    {
        crate::RefTable::from_predicate(self, predicate)
    }

    // -----------------------------------------------------------------------
    // Sorting
    // -----------------------------------------------------------------------

    /// Sorts the table by the given key columns, returning a [`RefTable`]
    /// with the rows in the new order.
    ///
    /// The result is an indirect sort: no data is moved, only the row
    /// index permutation changes. The returned [`RefTable`] has
    /// `row_order = false` (not in original ascending order).
    ///
    /// Only scalar columns with a total ordering can be sort keys.
    /// Complex columns are rejected. This matches C++ `Table::sort`.
    ///
    /// # Errors
    ///
    /// - [`TableError::SortNoKeys`] if `keys` is empty
    /// - [`TableError::SortKeyNotScalar`] if a key column is non-scalar
    /// - [`TableError::SortKeyUnsortable`] if a key column is Complex
    /// - [`TableError::UnknownColumn`] if a key column is not in schema
    ///
    /// # C++ equivalent
    ///
    /// `Table::sort(columnNames, sortOrders)`.
    ///
    /// [`RefTable`]: crate::RefTable
    pub fn sort(&mut self, keys: &[(&str, SortOrder)]) -> Result<crate::RefTable<'_>, TableError> {
        let permutation = crate::sorting::argsort(self, keys)?;
        crate::RefTable::from_rows(self, permutation)
    }

    /// Sorts the table by a single column using a custom comparison function.
    ///
    /// The closure receives two [`Value`] references from the specified column
    /// and must return an [`Ordering`]. This is the Rust analogue of passing
    /// a `BaseCompare` object to C++ `Table::sort`.
    ///
    /// [`Ordering`]: std::cmp::Ordering
    pub fn sort_by<F>(
        &mut self,
        column: &str,
        compare: F,
    ) -> Result<crate::RefTable<'_>, TableError>
    where
        F: Fn(&Value, &Value) -> std::cmp::Ordering,
    {
        let n = self.row_count();
        let mut indices: Vec<usize> = (0..n).collect();

        indices.sort_by(|&a, &b| {
            let va = self.cell(a, column);
            let vb = self.cell(b, column);
            match (va, vb) {
                (Some(a), Some(b)) => compare(a, b),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            }
        });

        crate::RefTable::from_rows(self, indices)
    }

    /// Returns an iterator that groups rows by equal values in the key columns.
    ///
    /// The table is first sorted by the key columns, then consecutive rows
    /// with equal key values are collected into [`TableGroup`] values.
    /// Each group contains the shared key values and the parent-table row
    /// indices for that group.
    ///
    /// Unlike [`sort`](Table::sort), this borrows the table immutably because
    /// it yields owned data rather than a mutable view.
    ///
    /// # C++ equivalent
    ///
    /// `casacore::TableIterator`.
    ///
    /// [`TableGroup`]: crate::TableGroup
    pub fn iter_groups(
        &self,
        keys: &[(&str, SortOrder)],
    ) -> Result<crate::sorting::TableIterator<'_>, TableError> {
        crate::sorting::TableIterator::new(self, keys)
    }

    /// Creates a [`crate::ConcatTable`] from two or more tables with the same schema.
    ///
    /// The resulting virtual table has a row count equal to the sum of all
    /// constituent tables. Row reads dispatch to the correct underlying table
    /// via binary search on cumulative row offsets. No data is copied.
    ///
    /// All tables must have identical schemas. Returns
    /// [`TableError::SchemaMismatch`] if they differ, or
    /// [`TableError::ConcatEmpty`] if the vector is empty.
    ///
    /// # C++ equivalent
    ///
    /// `ConcatTable(Block<Table>(...), Block<String>(), "")`.
    pub fn concat(tables: Vec<Table>) -> Result<crate::ConcatTable, TableError> {
        crate::ConcatTable::new(tables)
    }

    /// Creates a deep copy of this table at the given path.
    ///
    /// All rows, keywords, column keywords, and schema are written to a new
    /// table directory. The storage manager can differ from the source table,
    /// enabling format migration (e.g. `StManAipsIO` to `StandardStMan`).
    ///
    /// # C++ equivalent
    ///
    /// `Table::deepCopy` via `TableCopy::makeEmptyTable` +
    /// `TableCopy::copyRows`.
    pub fn deep_copy(&self, opts: TableOptions) -> Result<(), TableError> {
        self.save(opts)
    }

    /// Creates a shallow copy of this table at the given path.
    ///
    /// Copies schema, table keywords, and column keywords but **no row data**.
    /// The resulting table has the same structure but zero rows.
    ///
    /// # C++ equivalent
    ///
    /// `TableCopy::makeEmptyTable(name, ..., noRows=True)`.
    pub fn shallow_copy(&self, opts: TableOptions) -> Result<(), TableError> {
        self.validate()?;
        let snapshot = StorageSnapshot {
            rows: Vec::new(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
        };
        let storage = CompositeStorage;
        storage.save(
            &opts.path,
            &snapshot,
            opts.data_manager,
            opts.endian_format.is_big_endian(),
        )?;
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
        let schema = self
            .inner
            .schema()
            .ok_or_else(|| TableError::Schema("schema required for column operations".into()))?;
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
            for row in self.inner.rows_mut() {
                row.push(RecordField::new(col.name(), value.clone()));
            }
        }
        Ok(())
    }

    /// Removes a column from the table schema, all rows, and column keywords.
    ///
    /// Returns an error if no schema is attached or the column does not exist
    /// in the schema.
    ///
    /// C++ equivalent: `Table::removeColumn`.
    pub fn remove_column(&mut self, name: &str) -> Result<(), TableError> {
        self.inner
            .schema()
            .ok_or_else(|| TableError::Schema("schema required for column operations".into()))?;

        let mut schema = self.inner.schema().cloned().unwrap();
        schema.remove_column(name)?;
        self.inner.set_schema(Some(schema));

        for row in self.inner.rows_mut() {
            row.remove(name);
        }
        self.inner.remove_column_keywords(name);
        Ok(())
    }

    /// Renames a column in the table schema, all rows, and column keywords.
    ///
    /// Returns an error if no schema is attached, `old` does not exist, or
    /// `new` already exists.
    ///
    /// C++ equivalent: `Table::renameColumn`.
    pub fn rename_column(&mut self, old: &str, new: &str) -> Result<(), TableError> {
        self.inner
            .schema()
            .ok_or_else(|| TableError::Schema("schema required for column operations".into()))?;

        let mut schema = self.inner.schema().cloned().unwrap();
        schema.rename_column(old, new)?;
        self.inner.set_schema(Some(schema));

        for row in self.inner.rows_mut() {
            row.rename_field(old, new);
        }
        self.inner.rename_column_keywords(old, new.to_string());
        Ok(())
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
            self.inner.remove_row(idx);
        }
        Ok(())
    }

    /// Inserts a row at the given position.
    ///
    /// Index `0` inserts before the first row; [`row_count`](Table::row_count)
    /// appends at the end (equivalent to [`add_row`](Table::add_row)).
    /// If a schema is attached, the row is validated against it.
    ///
    /// C++ equivalent: constructing rows and adding them to a `Table`.
    pub fn insert_row(&mut self, index: usize, row: RecordValue) -> Result<(), TableError> {
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
        self.inner.insert_row(index, row);
        Ok(())
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

    use super::{DataManagerKind, EndianFormat, RowRange, Table, TableError, TableOptions};

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

    /// Build a small multi-type table for endian round-trip tests.
    fn build_endian_test_table() -> Table {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("i32_col", PrimitiveType::Int32),
            ColumnSchema::scalar("f64_col", PrimitiveType::Float64),
            ColumnSchema::scalar("str_col", PrimitiveType::String),
            ColumnSchema::array_fixed("arr_col", PrimitiveType::Float32, vec![3]),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("i32_col", Value::Scalar(ScalarValue::Int32(42))),
                RecordField::new("f64_col", Value::Scalar(ScalarValue::Float64(2.78))),
                RecordField::new(
                    "str_col",
                    Value::Scalar(ScalarValue::String("hello".into())),
                ),
                RecordField::new(
                    "arr_col",
                    Value::Array(ArrayValue::from_f32_vec(vec![1.0, 2.0, 3.0])),
                ),
            ]))
            .expect("row 0");
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("i32_col", Value::Scalar(ScalarValue::Int32(-7))),
                RecordField::new("f64_col", Value::Scalar(ScalarValue::Float64(-0.5))),
                RecordField::new(
                    "str_col",
                    Value::Scalar(ScalarValue::String("world".into())),
                ),
                RecordField::new(
                    "arr_col",
                    Value::Array(ArrayValue::from_f32_vec(vec![4.0, 5.0, 6.0])),
                ),
            ]))
            .expect("row 1");
        table
    }

    /// Verify a reopened table matches the endian test fixture.
    fn verify_endian_test_table(t: &Table) {
        assert_eq!(t.row_count(), 2);
        assert_eq!(
            t.cell(0, "i32_col"),
            Some(&Value::Scalar(ScalarValue::Int32(42)))
        );
        assert_eq!(
            t.cell(0, "f64_col"),
            Some(&Value::Scalar(ScalarValue::Float64(2.78)))
        );
        assert_eq!(
            t.cell(0, "str_col"),
            Some(&Value::Scalar(ScalarValue::String("hello".into())))
        );
        assert_eq!(
            t.cell(1, "i32_col"),
            Some(&Value::Scalar(ScalarValue::Int32(-7)))
        );
    }

    #[test]
    fn stmanaipsio_le_round_trip() {
        let table = build_endian_test_table();
        let root = unique_test_dir("aipsio_le_rt");
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::StManAipsIO)
                    .with_endian_format(EndianFormat::LittleEndian),
            )
            .expect("save LE");
        let reopened = Table::open(TableOptions::new(&root)).expect("open LE");
        verify_endian_test_table(&reopened);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn stmanaipsio_be_round_trip() {
        let table = build_endian_test_table();
        let root = unique_test_dir("aipsio_be_rt");
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::StManAipsIO)
                    .with_endian_format(EndianFormat::BigEndian),
            )
            .expect("save BE");
        let reopened = Table::open(TableOptions::new(&root)).expect("open BE");
        verify_endian_test_table(&reopened);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn ssm_le_round_trip() {
        let table = build_endian_test_table();
        let root = unique_test_dir("ssm_le_rt");
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::StandardStMan)
                    .with_endian_format(EndianFormat::LittleEndian),
            )
            .expect("save LE");
        let reopened = Table::open(TableOptions::new(&root)).expect("open LE");
        verify_endian_test_table(&reopened);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn ssm_be_round_trip() {
        let table = build_endian_test_table();
        let root = unique_test_dir("ssm_be_rt");
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::StandardStMan)
                    .with_endian_format(EndianFormat::BigEndian),
            )
            .expect("save BE");
        let reopened = Table::open(TableOptions::new(&root)).expect("open BE");
        verify_endian_test_table(&reopened);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn ism_le_round_trip() {
        let table = build_endian_test_table();
        let root = unique_test_dir("ism_le_rt");
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::IncrementalStMan)
                    .with_endian_format(EndianFormat::LittleEndian),
            )
            .expect("save LE");
        let reopened = Table::open(TableOptions::new(&root)).expect("open LE");
        verify_endian_test_table(&reopened);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn ism_be_round_trip() {
        let table = build_endian_test_table();
        let root = unique_test_dir("ism_be_rt");
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(
                TableOptions::new(&root)
                    .with_data_manager(DataManagerKind::IncrementalStMan)
                    .with_endian_format(EndianFormat::BigEndian),
            )
            .expect("save BE");
        let reopened = Table::open(TableOptions::new(&root)).expect("open BE");
        verify_endian_test_table(&reopened);
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    /// Test ISM delta compression: values that repeat across consecutive rows.
    #[test]
    fn ism_slowly_changing() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("SCAN_NUMBER", PrimitiveType::Int32),
            ColumnSchema::scalar("FLAG", PrimitiveType::Bool),
        ])
        .unwrap();

        let scans = [0, 0, 0, 1, 1, 1, 1, 2, 2, 2];
        let flags = [
            true, true, true, true, true, false, false, false, true, true,
        ];

        let rows: Vec<RecordValue> = scans
            .iter()
            .zip(flags.iter())
            .map(|(&s, &f)| {
                RecordValue::new(vec![
                    RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(s))),
                    RecordField::new("FLAG", Value::Scalar(ScalarValue::Bool(f))),
                ])
            })
            .collect();

        let table = Table::from_rows_with_schema(rows, schema).unwrap();
        let root = unique_test_dir("ism_slowly_changing");
        std::fs::create_dir_all(&root).expect("mkdir");
        table
            .save(TableOptions::new(&root).with_data_manager(DataManagerKind::IncrementalStMan))
            .expect("save ISM");

        let reopened = Table::open(TableOptions::new(&root)).expect("reopen");
        assert_eq!(reopened.row_count(), 10);
        for (i, (&expected_scan, &expected_flag)) in scans.iter().zip(flags.iter()).enumerate() {
            let scan = reopened.get_scalar_cell(i, "SCAN_NUMBER").unwrap();
            assert_eq!(
                *scan,
                ScalarValue::Int32(expected_scan),
                "row {i} SCAN_NUMBER"
            );
            let flag = reopened.get_scalar_cell(i, "FLAG").unwrap();
            assert_eq!(*flag, ScalarValue::Bool(expected_flag), "row {i} FLAG");
        }
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    #[test]
    fn default_endian_matches_host() {
        let table = build_endian_test_table();
        let root = unique_test_dir("default_endian");
        std::fs::create_dir_all(&root).expect("mkdir");
        table.save(TableOptions::new(&root)).expect("save default");

        // Read table.dat and check the endian marker
        let dat_path = root.join("table.dat");
        let reopened = Table::open(TableOptions::new(&root)).expect("open");
        verify_endian_test_table(&reopened);

        // Verify the table.dat file exists and the table round-trips
        assert!(dat_path.exists());
        std::fs::remove_dir_all(&root).expect("cleanup");
    }

    // ---- Wave 2: Schema mutation & row operations tests ----

    /// Build a 3-row table with an "id" (Int32) and "name" (String) column.
    fn build_mutation_test_table() -> Table {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .expect("schema");
        let mut table = Table::with_schema(schema);
        for i in 0..3 {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
                    RecordField::new(
                        "name",
                        Value::Scalar(ScalarValue::String(format!("row{i}"))),
                    ),
                ]))
                .expect("add row");
        }
        table
    }

    #[test]
    fn add_column_populates_existing_rows() {
        let mut table = build_mutation_test_table();
        table
            .add_column(
                ColumnSchema::scalar("score", PrimitiveType::Float64),
                Some(Value::Scalar(ScalarValue::Float64(0.0))),
            )
            .expect("add column");

        assert_eq!(table.schema().unwrap().columns().len(), 3);
        for i in 0..3 {
            assert_eq!(
                table.cell(i, "score"),
                Some(&Value::Scalar(ScalarValue::Float64(0.0)))
            );
        }
    }

    #[test]
    fn add_column_round_trips_through_disk() {
        let mut table = build_mutation_test_table();
        table
            .add_column(
                ColumnSchema::scalar("score", PrimitiveType::Float64),
                Some(Value::Scalar(ScalarValue::Float64(99.5))),
            )
            .expect("add column");

        for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
            let root = unique_test_dir(&format!("add_col_{dm:?}"));
            std::fs::create_dir_all(&root).expect("mkdir");
            table
                .save(TableOptions::new(&root).with_data_manager(dm))
                .expect("save");
            let reopened = Table::open(TableOptions::new(&root)).expect("open");
            assert_eq!(reopened.schema().unwrap().columns().len(), 3);
            for i in 0..3 {
                assert_eq!(
                    reopened.cell(i, "score"),
                    Some(&Value::Scalar(ScalarValue::Float64(99.5)))
                );
            }
            std::fs::remove_dir_all(&root).expect("cleanup");
        }
    }

    #[test]
    fn add_column_none_default_with_undefined() {
        use crate::schema::ColumnOptions;

        let mut table = build_mutation_test_table();
        table
            .add_column(
                ColumnSchema::scalar("opt", PrimitiveType::Int32)
                    .with_options(ColumnOptions {
                        direct: false,
                        undefined: true,
                    })
                    .expect("options"),
                None,
            )
            .expect("add column with None default");

        assert_eq!(table.schema().unwrap().columns().len(), 3);
        // Rows should not have the new field.
        for i in 0..3 {
            assert_eq!(table.cell(i, "opt"), None);
        }
    }

    #[test]
    fn add_column_none_default_without_undefined_errors() {
        let mut table = build_mutation_test_table();
        let result = table.add_column(
            ColumnSchema::scalar("required_col", PrimitiveType::Int32),
            None,
        );
        assert!(
            result.is_err(),
            "should error when no default and column requires values"
        );
    }

    #[test]
    fn add_column_rejects_duplicate() {
        let mut table = build_mutation_test_table();
        let result = table.add_column(
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            Some(Value::Scalar(ScalarValue::Int32(0))),
        );
        assert!(result.is_err());
    }

    #[test]
    fn remove_column_drops_from_all_rows() {
        let mut table = build_mutation_test_table();
        table.set_column_keywords(
            "name",
            RecordValue::new(vec![RecordField::new(
                "unit",
                Value::Scalar(ScalarValue::String("none".into())),
            )]),
        );

        table.remove_column("name").expect("remove column");

        assert_eq!(table.schema().unwrap().columns().len(), 1);
        assert!(!table.schema().unwrap().contains_column("name"));
        for i in 0..3 {
            assert_eq!(table.cell(i, "name"), None);
        }
        assert!(table.column_keywords("name").is_none());
    }

    #[test]
    fn remove_column_round_trips_through_disk() {
        let mut table = build_mutation_test_table();
        table.remove_column("name").expect("remove");

        for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
            let root = unique_test_dir(&format!("rm_col_{dm:?}"));
            std::fs::create_dir_all(&root).expect("mkdir");
            table
                .save(TableOptions::new(&root).with_data_manager(dm))
                .expect("save");
            let reopened = Table::open(TableOptions::new(&root)).expect("open");
            assert_eq!(reopened.schema().unwrap().columns().len(), 1);
            assert!(!reopened.schema().unwrap().contains_column("name"));
            assert_eq!(
                reopened.cell(0, "id"),
                Some(&Value::Scalar(ScalarValue::Int32(0)))
            );
            std::fs::remove_dir_all(&root).expect("cleanup");
        }
    }

    #[test]
    fn remove_column_missing_errors() {
        let mut table = build_mutation_test_table();
        assert!(table.remove_column("nonexistent").is_err());
    }

    #[test]
    fn rename_column_updates_rows_and_keywords() {
        let mut table = build_mutation_test_table();
        table.set_column_keywords(
            "name",
            RecordValue::new(vec![RecordField::new(
                "unit",
                Value::Scalar(ScalarValue::String("text".into())),
            )]),
        );

        table.rename_column("name", "label").expect("rename");

        assert!(table.schema().unwrap().contains_column("label"));
        assert!(!table.schema().unwrap().contains_column("name"));
        for i in 0..3 {
            assert!(table.cell(i, "label").is_some());
            assert_eq!(table.cell(i, "name"), None);
        }
        assert!(table.column_keywords("label").is_some());
        assert!(table.column_keywords("name").is_none());
    }

    #[test]
    fn rename_column_round_trips_through_disk() {
        let mut table = build_mutation_test_table();
        table.rename_column("name", "label").expect("rename");

        for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
            let root = unique_test_dir(&format!("rename_col_{dm:?}"));
            std::fs::create_dir_all(&root).expect("mkdir");
            table
                .save(TableOptions::new(&root).with_data_manager(dm))
                .expect("save");
            let reopened = Table::open(TableOptions::new(&root)).expect("open");
            assert!(reopened.schema().unwrap().contains_column("label"));
            assert!(!reopened.schema().unwrap().contains_column("name"));
            assert_eq!(
                reopened.cell(0, "label"),
                Some(&Value::Scalar(ScalarValue::String("row0".into())))
            );
            std::fs::remove_dir_all(&root).expect("cleanup");
        }
    }

    #[test]
    fn remove_rows_compacts() {
        let mut table = build_mutation_test_table();
        // Add 2 more rows so we have 5 total (ids 0..5)
        for i in 3..5 {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
                    RecordField::new(
                        "name",
                        Value::Scalar(ScalarValue::String(format!("row{i}"))),
                    ),
                ]))
                .expect("add row");
        }
        assert_eq!(table.row_count(), 5);

        // Remove rows at indices 1 and 3 (ids 1, 3)
        table.remove_rows(&[1, 3]).expect("remove rows");

        assert_eq!(table.row_count(), 3);
        // Remaining rows should be ids 0, 2, 4
        assert_eq!(
            table.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
        );
        assert_eq!(
            table.cell(1, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(2)))
        );
        assert_eq!(
            table.cell(2, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(4)))
        );
    }

    #[test]
    fn remove_rows_round_trips_through_disk() {
        let mut table = build_mutation_test_table();
        table.remove_rows(&[1]).expect("remove row 1");

        for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
            let root = unique_test_dir(&format!("rm_rows_{dm:?}"));
            std::fs::create_dir_all(&root).expect("mkdir");
            table
                .save(TableOptions::new(&root).with_data_manager(dm))
                .expect("save");
            let reopened = Table::open(TableOptions::new(&root)).expect("open");
            assert_eq!(reopened.row_count(), 2);
            assert_eq!(
                reopened.cell(0, "id"),
                Some(&Value::Scalar(ScalarValue::Int32(0)))
            );
            assert_eq!(
                reopened.cell(1, "id"),
                Some(&Value::Scalar(ScalarValue::Int32(2)))
            );
            std::fs::remove_dir_all(&root).expect("cleanup");
        }
    }

    #[test]
    fn remove_rows_rejects_out_of_bounds() {
        let mut table = build_mutation_test_table();
        assert!(matches!(
            table.remove_rows(&[5]),
            Err(TableError::RowOutOfBounds {
                row_index: 5,
                row_count: 3
            })
        ));
    }

    #[test]
    fn remove_rows_rejects_unsorted() {
        let mut table = build_mutation_test_table();
        assert!(table.remove_rows(&[2, 1]).is_err());
    }

    #[test]
    fn insert_row_at_position() {
        let mut table = build_mutation_test_table();
        let new_row = RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
            RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("inserted".into())),
            ),
        ]);

        table.insert_row(1, new_row).expect("insert at 1");

        assert_eq!(table.row_count(), 4);
        assert_eq!(
            table.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
        );
        assert_eq!(
            table.cell(1, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(99)))
        );
        assert_eq!(
            table.cell(2, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(1)))
        );
        assert_eq!(
            table.cell(3, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(2)))
        );
    }

    #[test]
    fn insert_row_at_end() {
        let mut table = build_mutation_test_table();
        let new_row = RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
            RecordField::new(
                "name",
                Value::Scalar(ScalarValue::String("appended".into())),
            ),
        ]);
        table.insert_row(3, new_row).expect("insert at end");
        assert_eq!(table.row_count(), 4);
        assert_eq!(
            table.cell(3, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(99)))
        );
    }

    #[test]
    fn insert_row_rejects_out_of_bounds() {
        let mut table = build_mutation_test_table();
        let new_row = RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
            RecordField::new("name", Value::Scalar(ScalarValue::String("bad".into()))),
        ]);
        assert!(matches!(
            table.insert_row(10, new_row),
            Err(TableError::RowOutOfBounds { .. })
        ));
    }

    #[test]
    fn insert_row_validates_against_schema() {
        let mut table = build_mutation_test_table();
        // Missing required "id" column
        let bad_row = RecordValue::new(vec![RecordField::new(
            "name",
            Value::Scalar(ScalarValue::String("only name".into())),
        )]);
        assert!(table.insert_row(0, bad_row).is_err());
    }

    // ---- Locking integration tests ----

    #[cfg(unix)]
    mod lock_tests {
        use super::*;
        use crate::lock::{LockMode, LockOptions, LockType};

        fn build_test_table_on_disk(dir: &std::path::Path, dm: DataManagerKind) -> TableOptions {
            let schema = TableSchema::new(vec![
                ColumnSchema::scalar("id", PrimitiveType::Int32),
                ColumnSchema::scalar("name", PrimitiveType::String),
            ])
            .unwrap();
            let mut table = Table::with_schema(schema);
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                    RecordField::new("name", Value::Scalar(ScalarValue::String("alice".into()))),
                ]))
                .unwrap();
            let opts = TableOptions::new(dir.join("test.tbl")).with_data_manager(dm);
            table.save(opts.clone()).unwrap();
            opts
        }

        #[test]
        fn open_with_permanent_lock_acquires_immediately() {
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
            let lock_opts = LockOptions::new(LockMode::PermanentLocking);

            let table = Table::open_with_lock(opts, lock_opts).unwrap();
            assert!(table.has_lock(LockType::Write));
            assert!(!table.has_lock(LockType::Read));
            assert_eq!(table.row_count(), 1);
        }

        #[test]
        fn open_with_user_lock_has_no_lock_until_explicit() {
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
            let lock_opts = LockOptions::new(LockMode::UserLocking);

            let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
            assert!(!table.has_lock(LockType::Write));
            assert!(!table.has_lock(LockType::Read));

            // Acquire write lock explicitly.
            assert!(table.lock(LockType::Write, 1).unwrap());
            assert!(table.has_lock(LockType::Write));
        }

        #[test]
        fn lock_unlock_cycle_user_mode() {
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
            let lock_opts = LockOptions::new(LockMode::UserLocking);

            let mut table = Table::open_with_lock(opts, lock_opts).unwrap();
            assert!(table.lock(LockType::Write, 1).unwrap());
            assert!(table.has_lock(LockType::Write));

            table.unlock().unwrap();
            assert!(!table.has_lock(LockType::Write));
        }

        #[test]
        fn unlock_flushes_write_to_disk() {
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
            let lock_opts = LockOptions::new(LockMode::UserLocking);

            let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
            assert!(table.lock(LockType::Write, 1).unwrap());

            // Add a row while holding the write lock.
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                    RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
                ]))
                .unwrap();

            // Unlock should flush to disk.
            table.unlock().unwrap();

            // Reopen without locking and verify.
            let reopened = Table::open(opts).unwrap();
            assert_eq!(reopened.row_count(), 2);
        }

        #[test]
        fn lock_reloads_after_external_modification() {
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
            let lock_opts = LockOptions::new(LockMode::UserLocking);

            // Open table A with locking, acquire and release.
            let mut table_a = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
            assert!(table_a.lock(LockType::Write, 1).unwrap());
            table_a.unlock().unwrap();

            // Simulate another process: open with locking, modify, unlock.
            {
                let mut table_b = Table::open_with_lock(opts.clone(), lock_opts.clone()).unwrap();
                assert!(table_b.lock(LockType::Write, 1).unwrap());
                table_b
                    .add_row(RecordValue::new(vec![
                        RecordField::new("id", Value::Scalar(ScalarValue::Int32(99))),
                        RecordField::new(
                            "name",
                            Value::Scalar(ScalarValue::String("external".into())),
                        ),
                    ]))
                    .unwrap();
                table_b.unlock().unwrap();
                // table_b dropped here, releasing lock file fd.
            }

            // Re-acquire the lock on table_a — should reload.
            assert!(table_a.lock(LockType::Write, 1).unwrap());
            assert_eq!(table_a.row_count(), 2);
        }

        #[test]
        fn no_lock_backward_compat() {
            // Existing open()/save() API works unchanged.
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
            let table = Table::open(opts).unwrap();
            assert_eq!(table.row_count(), 1);
            // has_lock returns false for non-locked tables.
            assert!(!table.has_lock(LockType::Write));
        }

        #[test]
        fn lock_file_created_on_disk() {
            let tmp = tempfile::TempDir::new().unwrap();
            let opts = build_test_table_on_disk(tmp.path(), DataManagerKind::StManAipsIO);
            let lock_opts = LockOptions::new(LockMode::PermanentLocking);

            let _table = Table::open_with_lock(opts, lock_opts).unwrap();
            let lock_path = tmp.path().join("test.tbl").join("table.lock");
            assert!(lock_path.exists(), "table.lock should be created");
        }

        #[test]
        fn lock_on_non_locked_table_errors() {
            let table = Table::new();
            let mut table = table;
            let result = table.lock(LockType::Write, 1);
            assert!(matches!(result, Err(TableError::NotLocked { .. })));
        }

        #[test]
        fn permanent_lock_round_trip_both_dms() {
            for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
                let tmp = tempfile::TempDir::new().unwrap();
                let opts = build_test_table_on_disk(tmp.path(), dm);
                let lock_opts = LockOptions::new(LockMode::PermanentLocking);

                let mut table = Table::open_with_lock(opts.clone(), lock_opts).unwrap();
                assert!(table.has_lock(LockType::Write));
                assert_eq!(table.row_count(), 1);

                // Add a row while permanently locked.
                table
                    .add_row(RecordValue::new(vec![
                        RecordField::new("id", Value::Scalar(ScalarValue::Int32(2))),
                        RecordField::new("name", Value::Scalar(ScalarValue::String("bob".into()))),
                    ]))
                    .unwrap();

                // Unlock (flushes), then drop releases the lock.
                table.unlock().unwrap();

                // Reopen and verify.
                let reopened = Table::open(opts).unwrap();
                assert_eq!(reopened.row_count(), 2);
            }
        }
    }

    // -------------------------------------------------------------------
    // Memory table tests
    // -------------------------------------------------------------------

    fn memory_schema() -> TableSchema {
        TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("name", PrimitiveType::String),
        ])
        .unwrap()
    }

    fn memory_row(id: i32, name: &str) -> RecordValue {
        RecordValue::new(vec![
            RecordField::new("id", Value::Scalar(ScalarValue::Int32(id))),
            RecordField::new("name", Value::Scalar(ScalarValue::String(name.into()))),
        ])
    }

    #[test]
    fn new_memory_creates_transient_table() {
        let table = Table::new_memory();
        assert!(table.is_memory());
        assert_eq!(table.table_kind(), super::TableKind::Memory);
        assert_eq!(table.row_count(), 0);
        assert!(table.path().is_none());
    }

    #[test]
    fn with_schema_memory_validates_rows() {
        let mut table = Table::with_schema_memory(memory_schema());
        assert!(table.is_memory());
        table.add_row(memory_row(1, "alice")).unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn from_rows_memory_basic() {
        let rows = vec![memory_row(1, "a"), memory_row(2, "b")];
        let table = Table::from_rows_memory(rows);
        assert!(table.is_memory());
        assert_eq!(table.row_count(), 2);
    }

    #[test]
    fn from_rows_with_schema_memory_validates() {
        let rows = vec![memory_row(1, "a")];
        let table = Table::from_rows_with_schema_memory(rows, memory_schema()).unwrap();
        assert!(table.is_memory());
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn memory_table_full_crud_cycle() {
        let mut table = Table::with_schema_memory(memory_schema());
        // add_row
        table.add_row(memory_row(1, "alice")).unwrap();
        table.add_row(memory_row(2, "bob")).unwrap();
        assert_eq!(table.row_count(), 2);

        // set_cell
        table
            .set_cell(
                0,
                "name",
                Value::Scalar(ScalarValue::String("ALICE".into())),
            )
            .unwrap();
        assert_eq!(
            table.cell(0, "name"),
            Some(&Value::Scalar(ScalarValue::String("ALICE".into())))
        );

        // remove_rows
        table.remove_rows(&[1]).unwrap();
        assert_eq!(table.row_count(), 1);

        // add_column
        table
            .add_column(
                ColumnSchema::scalar("score", PrimitiveType::Float64),
                Some(Value::Scalar(ScalarValue::Float64(0.0))),
            )
            .unwrap();
        assert!(table.schema().unwrap().contains_column("score"));

        // remove_column
        table.remove_column("score").unwrap();
        assert!(!table.schema().unwrap().contains_column("score"));
    }

    #[test]
    fn memory_table_save_materializes_to_disk() {
        let mut table = Table::with_schema_memory(memory_schema());
        table.add_row(memory_row(42, "test")).unwrap();
        table.keywords_mut().push(RecordField::new(
            "origin",
            Value::Scalar(ScalarValue::String("memory".into())),
        ));

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("materialized.tbl");
        table.save(TableOptions::new(&path)).unwrap();

        // Reopen as a plain table.
        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        assert!(!reopened.is_memory());
        assert_eq!(reopened.row_count(), 1);
        assert_eq!(
            reopened.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(42)))
        );
        assert_eq!(
            reopened.keywords().get("origin"),
            Some(&Value::Scalar(ScalarValue::String("memory".into())))
        );
    }

    #[test]
    fn memory_table_save_with_both_data_managers() {
        for dm in [DataManagerKind::StManAipsIO, DataManagerKind::StandardStMan] {
            let mut table = Table::with_schema_memory(memory_schema());
            table.add_row(memory_row(1, "a")).unwrap();

            let tmp = tempfile::tempdir().unwrap();
            let path = tmp.path().join(format!("test_{dm:?}.tbl"));
            table
                .save(TableOptions::new(&path).with_data_manager(dm))
                .unwrap();

            let reopened = Table::open(TableOptions::new(&path)).unwrap();
            assert_eq!(reopened.row_count(), 1);
        }
    }

    #[test]
    fn to_memory_copies_all_data() {
        let mut plain = Table::with_schema(memory_schema());
        plain.add_row(memory_row(1, "orig")).unwrap();
        plain.keywords_mut().push(RecordField::new(
            "key",
            Value::Scalar(ScalarValue::Int32(99)),
        ));

        let mem = plain.to_memory();
        assert!(mem.is_memory());
        assert!(mem.path().is_none());
        assert_eq!(mem.row_count(), 1);
        assert_eq!(
            mem.cell(0, "id"),
            Some(&Value::Scalar(ScalarValue::Int32(1)))
        );
        assert_eq!(
            mem.keywords().get("key"),
            Some(&Value::Scalar(ScalarValue::Int32(99)))
        );
        assert!(mem.schema().is_some());
    }

    #[test]
    fn to_memory_from_disk_table() {
        let mut table = Table::with_schema(memory_schema());
        table.add_row(memory_row(5, "disk")).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("source.tbl");
        table.save(TableOptions::new(&path)).unwrap();

        let disk = Table::open(TableOptions::new(&path)).unwrap();
        let mem = disk.to_memory();
        assert!(mem.is_memory());
        assert!(mem.path().is_none());
        assert_eq!(mem.row_count(), 1);
    }

    #[test]
    fn memory_table_sort_and_select() {
        let mut table = Table::with_schema_memory(memory_schema());
        for (id, name) in [(3, "c"), (1, "a"), (2, "b")] {
            table.add_row(memory_row(id, name)).unwrap();
        }

        // Sort.
        let sorted = table.sort(&[("id", super::SortOrder::Ascending)]).unwrap();
        assert_eq!(sorted.row_count(), 3);
        assert_eq!(
            sorted.cell(0, "id").unwrap(),
            &Value::Scalar(ScalarValue::Int32(1))
        );
        drop(sorted);

        // Select by predicate.
        let view = table.select(
            |row| matches!(row.get("id"), Some(Value::Scalar(ScalarValue::Int32(i))) if *i >= 2),
        );
        assert_eq!(view.row_count(), 2);
    }

    #[test]
    fn memory_table_iter_groups() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("group", PrimitiveType::String),
            ColumnSchema::scalar("val", PrimitiveType::Int32),
        ])
        .unwrap();
        let mut table = Table::with_schema_memory(schema);
        for (g, v) in [("a", 1), ("b", 2), ("a", 3)] {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("group", Value::Scalar(ScalarValue::String(g.into()))),
                    RecordField::new("val", Value::Scalar(ScalarValue::Int32(v))),
                ]))
                .unwrap();
        }

        let groups: Vec<_> = table
            .iter_groups(&[("group", super::SortOrder::Ascending)])
            .unwrap()
            .collect();
        assert_eq!(groups.len(), 2);
    }

    #[cfg(unix)]
    #[test]
    fn memory_table_lock_is_noop() {
        use crate::lock::LockType;

        let table = Table::new_memory();
        assert!(table.has_lock(LockType::Write));
        assert!(table.has_lock(LockType::Read));
        assert!(!table.is_multi_used());
    }

    #[cfg(unix)]
    #[test]
    fn memory_table_lock_unlock_succeed() {
        use crate::lock::LockType;

        let mut table = Table::new_memory();
        assert!(table.lock(LockType::Write, 1).unwrap());
        table.unlock().unwrap();
    }

    #[test]
    fn plain_table_kind_is_default() {
        let table = Table::new();
        assert!(!table.is_memory());
        assert_eq!(table.table_kind(), super::TableKind::Plain);
    }
}
