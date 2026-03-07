// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use casacore_types::{
    ArrayValue, Complex64, RecordField, RecordValue, ScalarValue, Value, ValueKind,
};
use thiserror::Error;

#[cfg(unix)]
use crate::lock::LockFile;
use crate::lock::SyncData;
use crate::lock::{LockMode, LockOptions, LockType};
use crate::schema::{ArrayShapeContract, ColumnSchema, ColumnType, SchemaError, TableSchema};
use crate::storage::virtual_engine::VirtualColumnBinding;
use crate::storage::{CompositeStorage, StorageManager, StorageSnapshot, TableInfo};
use crate::table_impl::TableImpl;

mod columns;
mod io;
mod locking;
mod mutation;
mod query;
mod virtual_columns;

#[cfg(test)]
mod tests;

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
    /// Tiled storage: single hypercube for the entire column.
    ///
    /// All rows must have the same array shape. Data is stored in
    /// rectangular tiles within a single hypercube whose last dimension
    /// is the row count. This is the standard format for large fixed-shape
    /// array columns in measurement sets and images.
    ///
    /// C++ equivalent: `TiledColumnStMan`.
    TiledColumnStMan,
    /// Tiled storage: one hypercube per unique array shape.
    ///
    /// Rows with different array shapes are automatically grouped into
    /// separate hypercubes. An internal row map tracks which cube holds
    /// each row. This is the standard format for variable-shape array
    /// columns (e.g. visibilities with varying channel counts).
    ///
    /// C++ equivalent: `TiledShapeStMan`.
    TiledShapeStMan,
    /// Tiled storage: one hypercube per row.
    ///
    /// Each row has its own cube, allowing fully variable shapes per row.
    /// Most memory-intensive variant; use `TiledShapeStMan` when many rows
    /// share shapes.
    ///
    /// C++ equivalent: `TiledCellStMan`.
    TiledCellStMan,
    /// Tiled storage: user-controlled hypercube assignment.
    ///
    /// Like `TiledShapeStMan` but with explicit row-to-cube assignment
    /// rather than automatic shape-based grouping. Found in some older
    /// datasets.
    ///
    /// C++ equivalent: `TiledDataStMan`.
    TiledDataStMan,
}

/// Per-column data manager binding for [`Table::save_with_bindings`].
///
/// Specifies which storage manager to use for a particular column.
/// Columns not listed in the bindings map use the default DM from
/// [`TableOptions`].
///
/// # Example
///
/// ```rust
/// use casacore_tables::{ColumnBinding, DataManagerKind};
///
/// let binding = ColumnBinding {
///     data_manager: DataManagerKind::TiledColumnStMan,
///     tile_shape: Some(vec![4, 32]),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ColumnBinding {
    /// The storage manager to use for this column.
    pub data_manager: DataManagerKind,
    /// Optional tile shape (only used with tiled storage managers).
    pub tile_shape: Option<Vec<usize>>,
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
    tile_shape: Option<Vec<usize>>,
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
            tile_shape: None,
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

    /// Overrides the tile shape for tiled storage managers, returning the
    /// updated options.
    ///
    /// The tile shape must include the row dimension as the last element
    /// for `TiledColumnStMan` and `TiledShapeStMan`. For `TiledCellStMan`,
    /// the tile shape covers only the cell dimensions.
    ///
    /// If not set, a reasonable default is chosen automatically.
    pub fn with_tile_shape(mut self, shape: Vec<usize>) -> Self {
        self.tile_shape = Some(shape);
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

    /// Returns the tile shape, if set.
    pub fn tile_shape(&self) -> Option<&[usize]> {
        self.tile_shape.as_deref()
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

/// An n-dimensional sub-region selector for array-valued cells.
///
/// Specifies `start`, `end` (exclusive), and `stride` along each dimension.
/// All three vectors must have equal length matching the array dimensionality.
///
/// # C++ equivalent
///
/// `casacore::Slicer` — `casa/Arrays/Slicer.h`.
///
/// # Example
///
/// ```rust
/// use casacore_tables::Slicer;
///
/// // Select rows 0..4 step 2, columns 1..3 step 1 from a 2-D array:
/// let s = Slicer::new(vec![0, 1], vec![4, 3], vec![2, 1]).unwrap();
/// assert_eq!(s.ndim(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slicer {
    start: Vec<usize>,
    end: Vec<usize>,
    stride: Vec<usize>,
}

impl Slicer {
    /// Creates a new slicer with the given start, end (exclusive), and stride.
    ///
    /// Returns an error if the vectors have different lengths or any stride is zero.
    pub fn new(start: Vec<usize>, end: Vec<usize>, stride: Vec<usize>) -> Result<Self, TableError> {
        if start.len() != end.len() || start.len() != stride.len() {
            return Err(TableError::SlicerDimensionMismatch {
                start_ndim: start.len(),
                end_ndim: end.len(),
                stride_ndim: stride.len(),
            });
        }
        for (i, &s) in stride.iter().enumerate() {
            if s == 0 {
                return Err(TableError::SlicerZeroStride { axis: i });
            }
        }
        for (i, (&s, &e)) in start.iter().zip(end.iter()).enumerate() {
            if s > e {
                return Err(TableError::SlicerInvalidRange {
                    axis: i,
                    start: s,
                    end: e,
                });
            }
        }
        Ok(Self { start, end, stride })
    }

    /// Creates a contiguous slicer (stride 1 on all axes).
    pub fn contiguous(start: Vec<usize>, end: Vec<usize>) -> Result<Self, TableError> {
        let stride = vec![1; start.len()];
        Self::new(start, end, stride)
    }

    /// Number of dimensions.
    pub fn ndim(&self) -> usize {
        self.start.len()
    }

    /// Start indices (inclusive) along each dimension.
    pub fn start(&self) -> &[usize] {
        &self.start
    }

    /// End indices (exclusive) along each dimension.
    pub fn end(&self) -> &[usize] {
        &self.end
    }

    /// Stride along each dimension.
    pub fn stride(&self) -> &[usize] {
        &self.stride
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

/// Result of a TaQL SELECT query via [`Table::query_result()`].
///
/// Simple SELECTs (only column references) produce a zero-copy
/// [`View`](QueryResult::View). SELECTs with computed expressions, GROUP BY,
/// or aggregate functions produce a [`Materialized`](QueryResult::Materialized)
/// in-memory table containing the evaluated result rows.
///
/// Both variants support reading column names and row data.
///
/// C++ equivalent: the result of `tableCommand()` — either a `Table` reference
/// or a newly materialized `Table` from `makeProjectExprTable()`.
pub enum QueryResult<'a> {
    /// A zero-copy view into the source table (row-index mapping).
    View(crate::RefTable<'a>),
    /// An owned in-memory table with computed/aggregated values (boxed to
    /// keep the enum size small).
    Materialized(Box<Table>),
}

impl std::fmt::Debug for QueryResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::View(v) => write!(f, "QueryResult::View({} rows)", v.row_count()),
            Self::Materialized(t) => write!(f, "QueryResult::Materialized({} rows)", t.row_count()),
        }
    }
}

impl QueryResult<'_> {
    /// Returns the number of result rows.
    pub fn row_count(&self) -> usize {
        match self {
            QueryResult::View(v) => v.row_count(),
            QueryResult::Materialized(t) => t.row_count(),
        }
    }

    /// Returns the column names of the result.
    pub fn column_names(&self) -> Vec<String> {
        match self {
            QueryResult::View(v) => v.column_names().to_vec(),
            QueryResult::Materialized(t) => t
                .schema()
                .map(|s| s.columns().iter().map(|c| c.name().to_string()).collect())
                .unwrap_or_default(),
        }
    }

    /// Returns the row at the given index, or `None` if out of bounds.
    pub fn row(&self, index: usize) -> Option<&RecordValue> {
        match self {
            QueryResult::View(v) => v.row(index),
            QueryResult::Materialized(t) => t.row(index),
        }
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
    /// A TaQL query error occurred.
    ///
    /// Wraps [`crate::taql::TaqlError`] for convenience when using the
    /// [`Table::query`] or [`Table::execute_taql`] methods.
    #[error("TaQL error: {0}")]
    Taql(String),
    /// Slicer dimension vectors have different lengths.
    #[error("slicer dimension mismatch: start={start_ndim}, end={end_ndim}, stride={stride_ndim}")]
    SlicerDimensionMismatch {
        start_ndim: usize,
        end_ndim: usize,
        stride_ndim: usize,
    },
    /// A slicer stride was zero on the given axis.
    #[error("slicer stride is zero on axis {axis}")]
    SlicerZeroStride { axis: usize },
    /// A slicer range has start > end on the given axis.
    #[error("slicer range invalid on axis {axis}: start={start} > end={end}")]
    SlicerInvalidRange {
        axis: usize,
        start: usize,
        end: usize,
    },
    /// A slicer index is out of bounds for the array shape.
    #[error("slicer out of bounds on axis {axis}: index {index} >= extent {extent}")]
    SlicerOutOfBounds {
        axis: usize,
        index: usize,
        extent: usize,
    },
    /// A cell is not an array (slice operations require array cells).
    #[error("cell at row {row} column \"{column}\" is not an array")]
    CellNotArray { row: usize, column: String },
    /// The number of data elements doesn't match the number of selected rows.
    #[error(
        "column slice length mismatch: {rows} rows selected but {data_len} data elements provided"
    )]
    ColumnSliceLengthMismatch { rows: usize, data_len: usize },
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

/// A casacore table — the fundamental persistent data container.
///
/// A `Table` holds a set of named columns (scalar or array) plus keyword
/// metadata, backed either by on-disk storage or an in-memory buffer.
///
/// # Lifecycle
///
/// * **Open** — [`Table::open`] reads an existing table directory.
/// * **Create** — [`Table::create`] builds a new table from a schema.
/// * **Save** — [`Table::save`] flushes deferred writes to disk.
///
/// # C++ equivalent
///
/// `casacore::Table` — the main user-facing class in the Tables module.
#[derive(Default)]
pub struct Table {
    inner: TableImpl,
    /// Filesystem path this table was last opened from or saved to.
    source_path: Option<PathBuf>,
    /// Whether this is a plain (disk-backed) or memory (transient) table.
    kind: TableKind,
    /// Names of columns backed by virtual engines (ForwardColumnEngine,
    /// ScaledArrayEngine, etc.). Empty for tables with no virtual columns.
    virtual_columns: HashSet<String>,
    /// Virtual column bindings for save — describes how each virtual column
    /// maps to its engine and configuration.
    virtual_bindings: Vec<VirtualColumnBinding>,
    /// Table metadata (type/subtype) persisted in `table.info`.
    table_info: TableInfo,
    /// Data manager info extracted from table.dat (empty for memory tables).
    dm_info: Vec<crate::storage::DataManagerInfo>,
    /// Optional external lock synchronization hook.
    external_sync: Option<Box<dyn crate::lock::ExternalLockSync>>,
    /// When `true`, the table directory is deleted on [`Drop`].
    ///
    /// Set via [`mark_for_delete`](Table::mark_for_delete), cleared via
    /// [`unmark_for_delete`](Table::unmark_for_delete).
    marked_for_delete: bool,
    #[cfg(unix)]
    lock_state: Option<LockState>,
}

impl std::fmt::Debug for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("Table");
        s.field("inner", &self.inner)
            .field("source_path", &self.source_path)
            .field("kind", &self.kind)
            .field("virtual_columns", &self.virtual_columns)
            .field("table_info", &self.table_info)
            .field("marked_for_delete", &self.marked_for_delete)
            .field(
                "external_sync",
                &self.external_sync.as_ref().map(|_| "<hook>"),
            );
        #[cfg(unix)]
        s.field("lock_state", &self.lock_state);
        s.finish()
    }
}

// ── Constructors and table kind ──────────────────────────────────────

impl Table {
    /// Creates a new, empty table with no rows, no schema, and no keywords.
    pub fn new() -> Self {
        Self {
            inner: TableImpl::new(),
            source_path: None,
            kind: TableKind::Plain,
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: TableInfo::default(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
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
            virtual_columns: HashSet::new(),
            virtual_bindings: Vec::new(),
            table_info: self.table_info.clone(),
            dm_info: vec![],
            external_sync: None,
            marked_for_delete: false,
            #[cfg(unix)]
            lock_state: None,
        }
    }
}

// ── Drop ──────────────────────────────────────────────────────────────

impl Drop for Table {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            if self.kind != TableKind::Memory {
                let had_write_lock = self
                    .lock_state
                    .as_ref()
                    .is_some_and(|state| state.lock_file.has_lock(LockType::Write));

                if had_write_lock {
                    let save_opts = self.lock_state.as_ref().map(|state| {
                        TableOptions::new(&state.path)
                            .with_data_manager(state.data_manager)
                            .with_endian_format(state.endian_format)
                    });

                    if let Some(save_opts) = save_opts
                        && self.save(save_opts).is_ok()
                    {
                        let nrrow = self.row_count() as u64;
                        let nrcolumn = self.schema().map(|s| s.columns().len() as u32).unwrap_or(0);
                        if let Some(state) = self.lock_state.as_mut() {
                            state.sync_data.record_write(nrrow, nrcolumn, true, &[true]);
                            let _ = state.lock_file.write_sync_data(&state.sync_data);
                        }
                    }
                }
            }
        }

        if self.marked_for_delete {
            if let Some(path) = &self.source_path {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }
}
