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
        let virtual_cols = snapshot.virtual_columns;
        let info = snapshot.table_info;
        let table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                snapshot.rows,
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
            ),
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            virtual_columns: virtual_cols,
            virtual_bindings: Vec::new(),
            table_info: info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            marked_for_delete: false,
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
            table_info: self.table_info.clone(),
            virtual_columns: self.virtual_columns.clone(),
            virtual_bindings: self.virtual_bindings.clone(),
            dm_info: vec![],
        };
        let storage = CompositeStorage;
        storage.save(
            &options.path,
            &snapshot,
            options.data_manager,
            options.endian_format.is_big_endian(),
            options.tile_shape.as_deref(),
        )?;
        Ok(())
    }

    /// Save the table with per-column data manager bindings.
    ///
    /// Columns listed in `bindings` are stored using their specified DM;
    /// all other stored columns use the default DM from `options`.
    ///
    /// This allows mixing storage managers within one table, for example
    /// scalars in StandardStMan and arrays in TiledColumnStMan.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use casacore_tables::{Table, TableOptions, DataManagerKind, ColumnBinding};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let table = Table::default();
    /// let mut bindings = HashMap::new();
    /// bindings.insert("DATA".to_string(), ColumnBinding {
    ///     data_manager: DataManagerKind::TiledColumnStMan,
    ///     tile_shape: Some(vec![4, 32]),
    /// });
    /// table.save_with_bindings(
    ///     TableOptions::new("/tmp/my_table"),
    ///     &bindings,
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn save_with_bindings(
        &self,
        options: TableOptions,
        bindings: &std::collections::HashMap<String, ColumnBinding>,
    ) -> Result<(), TableError> {
        self.validate()?;
        let snapshot = StorageSnapshot {
            rows: self.inner.rows().to_vec(),
            keywords: self.inner.keywords().clone(),
            column_keywords: self.inner.all_column_keywords().clone(),
            schema: self.inner.schema().cloned(),
            table_info: self.table_info.clone(),
            virtual_columns: self.virtual_columns.clone(),
            virtual_bindings: self.virtual_bindings.clone(),
            dm_info: vec![],
        };
        let storage = CompositeStorage;
        storage.save_with_bindings(
            &options.path,
            &snapshot,
            options.data_manager,
            options.endian_format.is_big_endian(),
            options.tile_shape.as_deref(),
            bindings,
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

    /// Returns the table metadata (type and subtype) from `table.info`.
    ///
    /// Tables loaded from disk carry the persisted values; newly created
    /// tables return the default (empty strings).
    ///
    /// # C++ equivalent
    ///
    /// `Table::tableInfo()`.
    pub fn info(&self) -> &TableInfo {
        &self.table_info
    }

    /// Replaces the table metadata (type and subtype).
    ///
    /// The new values are persisted on the next [`save`](Table::save).
    ///
    /// # C++ equivalent
    ///
    /// `Table::tableInfo()` (mutable overload) followed by `Table::flushTableInfo()`.
    pub fn set_info(&mut self, info: TableInfo) {
        self.table_info = info;
    }

    /// Returns data manager information for this table.
    ///
    /// Each [`crate::storage::DataManagerInfo`] describes one storage manager
    /// instance and
    /// the columns it manages. The list is populated when a table is loaded
    /// from disk; for memory-only tables the list is empty.
    ///
    /// # C++ equivalent
    ///
    /// `Table::dataManagerInfo()`.
    pub fn data_manager_info(&self) -> &[crate::storage::DataManagerInfo] {
        &self.dm_info
    }

    /// Returns a human-readable summary of the table's structure.
    ///
    /// Includes row count, column names and types, and (for disk-loaded
    /// tables) data manager assignments.
    ///
    /// # C++ equivalent
    ///
    /// `Table::showStructure(ostream)`, `showtableinfo` utility.
    pub fn show_structure(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        let _ = writeln!(out, "Table: {} rows", self.row_count());

        if !self.table_info.table_type.is_empty() || !self.table_info.sub_type.is_empty() {
            let _ = writeln!(
                out,
                "  Type = {}  SubType = {}",
                self.table_info.table_type, self.table_info.sub_type
            );
        }

        if let Some(schema) = self.schema() {
            let _ = writeln!(out, "Columns ({}):", schema.columns().len());
            for col in schema.columns() {
                let type_str = match col.column_type() {
                    crate::schema::ColumnType::Scalar => {
                        format!(
                            "Scalar {}",
                            col.data_type()
                                .map_or("Record".into(), |dt| format!("{dt:?}"))
                        )
                    }
                    crate::schema::ColumnType::Array(contract) => {
                        let dt = col.data_type().map_or("?".into(), |dt| format!("{dt:?}"));
                        format!("Array<{dt}> {contract:?}")
                    }
                    crate::schema::ColumnType::Record => "Record".to_string(),
                };
                let _ = writeln!(out, "  {} : {}", col.name(), type_str);
            }
        }

        if !self.dm_info.is_empty() {
            let _ = writeln!(out, "Data managers ({}):", self.dm_info.len());
            for dm in &self.dm_info {
                let _ = writeln!(
                    out,
                    "  [{}] {} -> [{}]",
                    dm.seq_nr,
                    dm.dm_type,
                    dm.columns.join(", ")
                );
            }
        }

        out
    }

    /// Returns a formatted tree of the table's keyword sets.
    ///
    /// Includes both table-level keywords and per-column keywords.
    ///
    /// # C++ equivalent
    ///
    /// `TableRecord::print(ostream)`.
    pub fn show_keywords(&self) -> String {
        use std::fmt::Write;
        let mut out = String::new();

        let kw = self.keywords();
        if !kw.fields().is_empty() {
            let _ = writeln!(out, "Table keywords:");
            for field in kw.fields() {
                let _ = writeln!(out, "  {} = {:?}", field.name, field.value);
            }
        }

        let col_kw = self.inner.all_column_keywords();
        for (col_name, rec) in col_kw {
            if !rec.fields().is_empty() {
                let _ = writeln!(out, "Column \"{}\" keywords:", col_name);
                for field in rec.fields() {
                    let _ = writeln!(out, "  {} = {:?}", field.name, field.value);
                }
            }
        }

        out
    }

    // -------------------------------------------------------------------
    // Lifecycle operations
    // -------------------------------------------------------------------

    /// Writes the current in-memory state back to the table's source path.
    ///
    /// The table must have been loaded with [`open`](Table::open) or
    /// previously saved with [`save`](Table::save) so that
    /// [`path`](Table::path) is `Some`. Returns an error if no source path
    /// is set.
    ///
    /// This is the Rust equivalent of the C++ `Table::flush()` call.
    pub fn flush(&self) -> Result<(), TableError> {
        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("cannot flush: table has no source path".into()))?;
        let opts = TableOptions::new(path);
        self.save(opts)
    }

    /// Discards all in-memory changes and reloads the table from disk.
    ///
    /// The table must have a source path (set by [`open`](Table::open) or
    /// [`save`](Table::save)). After resync the in-memory state matches the
    /// on-disk state exactly.
    ///
    /// # C++ equivalent
    ///
    /// `Table::resync()`.
    pub fn resync(&mut self) -> Result<(), TableError> {
        let path = self
            .source_path
            .as_ref()
            .ok_or_else(|| TableError::Storage("cannot resync: table has no source path".into()))?
            .clone();
        let opts = TableOptions::new(&path);
        let mut reloaded = Table::open(opts)?;
        self.inner = std::mem::take(&mut reloaded.inner);
        self.virtual_columns = std::mem::take(&mut reloaded.virtual_columns);
        self.virtual_bindings = std::mem::take(&mut reloaded.virtual_bindings);
        self.table_info = std::mem::take(&mut reloaded.table_info);
        // Preserve source_path, kind, marked_for_delete, and lock_state.
        Ok(())
    }

    /// Marks this table for deletion when it is dropped.
    ///
    /// If the table has a [`source_path`](Table::path), the table directory
    /// is recursively removed when the `Table` value is dropped.
    ///
    /// # C++ equivalent
    ///
    /// `Table::markForDelete()`.
    pub fn mark_for_delete(&mut self) {
        self.marked_for_delete = true;
    }

    /// Installs an external lock synchronization hook.
    ///
    /// The hook's methods are called around every file-level lock
    /// acquire/release pair so that an external lock manager can stay in
    /// sync. Pass `None` to remove a previously installed hook.
    ///
    /// # C++ equivalent
    ///
    /// `TableLockData::setExternalLockSync()`.
    pub fn set_external_sync(&mut self, sync: Option<Box<dyn crate::lock::ExternalLockSync>>) {
        self.external_sync = sync;
    }

    /// Clears the mark-for-delete flag.
    ///
    /// # C++ equivalent
    ///
    /// `Table::unmarkForDelete()`.
    pub fn unmark_for_delete(&mut self) {
        self.marked_for_delete = false;
    }

    /// Returns `true` if this table is marked for deletion on drop.
    ///
    /// # C++ equivalent
    ///
    /// `Table::isMarkedForDelete()`.
    pub fn is_marked_for_delete(&self) -> bool {
        self.marked_for_delete
    }

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
        for row in source.rows() {
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
        let src_rows = source.rows();
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

    // -------------------------------------------------------------------
    // Virtual column API
    // -------------------------------------------------------------------

    /// Returns `true` if the named column is a virtual column.
    ///
    /// Virtual columns are materialized from other data (e.g. forwarded
    /// from another table, or computed as `stored * scale + offset`). They
    /// behave like regular columns in memory, but their on-disk representation
    /// is through a virtual engine rather than a storage manager.
    ///
    /// # C++ equivalent
    ///
    /// `TableColumn::isVirtual()`.
    pub fn is_virtual_column(&self, name: &str) -> bool {
        self.virtual_columns.contains(name)
    }

    /// Bind a column as a `ForwardColumnEngine` column.
    ///
    /// The column `column` will read its values from the same-named column
    /// in the table at `ref_table`. On save, the column is backed by a
    /// `ForwardColumnEngine` DM entry; on reload, values are copied from
    /// the referenced table.
    ///
    /// The column must already exist in the schema. The referenced table
    /// must exist on disk at save time.
    ///
    /// # C++ equivalent
    ///
    /// `ForwardColumnEngine::addColumn(...)`.
    pub fn bind_forward_column(
        &mut self,
        column: &str,
        ref_table: &Path,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == column) {
                return Err(TableError::SchemaColumnUnknown {
                    column: column.to_string(),
                });
            }
        }
        self.virtual_columns.insert(column.to_string());
        self.virtual_bindings.push(VirtualColumnBinding::Forward {
            col_name: column.to_string(),
            ref_table: ref_table.to_path_buf(),
        });
        Ok(())
    }

    /// Bind a column as a `ScaledArrayEngine` column.
    ///
    /// The column `virtual_col` computes `stored_col * scale + offset`.
    /// Both columns must exist in the schema. The stored column holds
    /// integer or float data; the virtual column exposes Float64 values.
    ///
    /// # C++ equivalent
    ///
    /// `ScaledArrayEngine<Double,Int>(virtualCol, storedCol, scale, offset)`.
    pub fn bind_scaled_array_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: f64,
        offset: f64,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::ScaledArray {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
            });
        Ok(())
    }

    /// Bind a column as a `ScaledComplexData` column.
    ///
    /// The stored column holds integer data with a prepended dimension of 2
    /// for real/imaginary parts. The virtual column exposes Complex32 or
    /// Complex64 values computed as:
    /// - `re_virtual = re_stored * scale.re + offset.re`
    /// - `im_virtual = im_stored * scale.im + offset.im`
    ///
    /// Both columns must exist in the schema.
    ///
    /// # C++ equivalent
    ///
    /// `ScaledComplexData<Complex,Short>(virtualCol, storedCol, scale, offset)`.
    pub fn bind_scaled_complex_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: Complex64,
        offset: Complex64,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::ScaledComplexData {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
            });
        Ok(())
    }

    /// Bind a column as a `BitFlagsEngine` column.
    ///
    /// The column `virtual_col` produces `(stored_col & read_mask) != 0`.
    /// Both columns must exist in the schema. The stored column holds
    /// integer data; the virtual column exposes Bool values.
    ///
    /// # C++ equivalent
    ///
    /// `BitFlagsEngine<uChar>(virtualCol, storedCol)`.
    pub fn bind_bitflags_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        read_mask: u32,
        write_mask: u32,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings.push(VirtualColumnBinding::BitFlags {
            virtual_col: virtual_col.to_string(),
            stored_col: stored_col.to_string(),
            read_mask,
            write_mask,
        });
        Ok(())
    }

    /// Bind a column as a `CompressFloat` column.
    ///
    /// The column `virtual_col` decompresses stored Int16 data from
    /// `stored_col` using FITS-style linear scaling:
    /// `virtual[i] = (stored == -32768) ? NaN : stored * scale + offset`.
    ///
    /// # C++ equivalent
    ///
    /// `CompressFloat(virtualCol, storedCol, scale, offset)`.
    pub fn bind_compress_float_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: f32,
        offset: f32,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::CompressFloat {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
            });
        Ok(())
    }

    /// Bind a column as a `CompressComplex` or `CompressComplexSD` column.
    ///
    /// The column `virtual_col` decompresses stored Int32 data from
    /// `stored_col` into complex values.
    ///
    /// # C++ equivalent
    ///
    /// `CompressComplex` / `CompressComplexSD`.
    pub fn bind_compress_complex_column(
        &mut self,
        virtual_col: &str,
        stored_col: &str,
        scale: f32,
        offset: f32,
        single_dish: bool,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == virtual_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: virtual_col.to_string(),
                });
            }
            if !schema.columns().iter().any(|c| c.name() == stored_col) {
                return Err(TableError::SchemaColumnUnknown {
                    column: stored_col.to_string(),
                });
            }
        }
        self.virtual_columns.insert(virtual_col.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::CompressComplex {
                virtual_col: virtual_col.to_string(),
                stored_col: stored_col.to_string(),
                scale,
                offset,
                single_dish,
            });
        Ok(())
    }

    /// Bind a column as a `ForwardColumnIndexedRowEngine` column.
    ///
    /// Like `ForwardColumnEngine` but remaps rows via an index column.
    /// For row `r`, reads `idx = row_map_col[r]`, then reads the
    /// forwarded column at row `idx` in the referenced table.
    ///
    /// # C++ equivalent
    ///
    /// `ForwardColumnIndexedRowEngine`.
    pub fn bind_forward_column_indexed(
        &mut self,
        column: &str,
        ref_table: &Path,
        row_column: &str,
    ) -> Result<(), TableError> {
        if let Some(schema) = self.inner.schema() {
            if !schema.columns().iter().any(|c| c.name() == column) {
                return Err(TableError::SchemaColumnUnknown {
                    column: column.to_string(),
                });
            }
        }
        self.virtual_columns.insert(column.to_string());
        self.virtual_bindings
            .push(VirtualColumnBinding::ForwardIndexedRow {
                col_name: column.to_string(),
                ref_table: ref_table.to_path_buf(),
                row_column: row_column.to_string(),
            });
        Ok(())
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
        let info = snapshot.table_info;
        let mut table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                snapshot.rows,
                snapshot.keywords,
                snapshot.column_keywords,
                snapshot.schema,
            ),
            source_path: Some(options.path.clone()),
            kind: TableKind::Plain,
            virtual_columns: snapshot.virtual_columns,
            virtual_bindings: Vec::new(),
            table_info: info,
            dm_info: snapshot.dm_info,
            external_sync: None,
            marked_for_delete: false,
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
            LockMode::AutoLocking | LockMode::DefaultLocking => {
                let _ = lock_file.acquire(LockType::Read, 1);
            }
            LockMode::AutoNoReadLocking => {
                // Skip read lock on open — only write locks are acquired.
            }
            LockMode::UserLocking | LockMode::UserNoReadLocking | LockMode::NoLocking => {}
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

        // Notify external sync hook before acquiring.
        if let Some(sync) = &self.external_sync {
            match lock_type {
                LockType::Read => sync.acquire_read(),
                LockType::Write => sync.acquire_write(),
            }
        }

        let state = self
            .lock_state
            .as_mut()
            .ok_or_else(|| TableError::NotLocked {
                operation: "lock".into(),
            })?;

        // NoRead modes skip the file-level read lock entirely.
        if lock_type == LockType::Read && state.options.mode.skip_read_lock() {
            return Ok(true);
        }

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
                    self.virtual_columns = snapshot.virtual_columns;
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

        // Notify external sync hook after release.
        if let Some(sync) = &self.external_sync {
            sync.release();
        }

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
    // Row-set algebra
    // -----------------------------------------------------------------------

    /// Returns row indices present in **either** `a` or `b` (union).
    ///
    /// The result is sorted and deduplicated. If both inputs are already
    /// sorted, this runs in O(n) via a merge; otherwise it falls back to
    /// sort + dedup.
    ///
    /// # C++ equivalent
    ///
    /// `TableExprNode::operator|` (set union on row numbers).
    pub fn row_union(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_sorted() && b.is_sorted() {
            let mut result = Vec::with_capacity(a.len() + b.len());
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    std::cmp::Ordering::Less => {
                        result.push(a[i]);
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => {
                        result.push(b[j]);
                        j += 1;
                    }
                    std::cmp::Ordering::Equal => {
                        result.push(a[i]);
                        i += 1;
                        j += 1;
                    }
                }
            }
            result.extend_from_slice(&a[i..]);
            result.extend_from_slice(&b[j..]);
            result
        } else {
            let mut set: Vec<usize> = a.iter().chain(b.iter()).copied().collect();
            set.sort_unstable();
            set.dedup();
            set
        }
    }

    /// Returns row indices present in **both** `a` and `b` (intersection).
    ///
    /// The result is sorted. If both inputs are already sorted, this runs
    /// in O(n) via a merge; otherwise it falls back to hash + sort.
    ///
    /// # C++ equivalent
    ///
    /// `TableExprNode::operator&` (set intersection on row numbers).
    pub fn row_intersection(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_sorted() && b.is_sorted() {
            let mut result = Vec::new();
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    std::cmp::Ordering::Less => i += 1,
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        result.push(a[i]);
                        i += 1;
                        j += 1;
                    }
                }
            }
            result
        } else {
            let set_b: std::collections::HashSet<usize> = b.iter().copied().collect();
            let mut result: Vec<usize> = a.iter().copied().filter(|x| set_b.contains(x)).collect();
            result.sort_unstable();
            result.dedup();
            result
        }
    }

    /// Returns row indices present in `a` but not in `b` (difference).
    ///
    /// The result is sorted. If both inputs are already sorted, this runs
    /// in O(n) via a merge; otherwise it falls back to hash + sort.
    ///
    /// # C++ equivalent
    ///
    /// `TableExprNode::operator-` (set difference on row numbers).
    pub fn row_difference(a: &[usize], b: &[usize]) -> Vec<usize> {
        if a.is_sorted() && b.is_sorted() {
            let mut result = Vec::new();
            let (mut i, mut j) = (0, 0);
            while i < a.len() && j < b.len() {
                match a[i].cmp(&b[j]) {
                    std::cmp::Ordering::Less => {
                        result.push(a[i]);
                        i += 1;
                    }
                    std::cmp::Ordering::Greater => j += 1,
                    std::cmp::Ordering::Equal => {
                        i += 1;
                        j += 1;
                    }
                }
            }
            result.extend_from_slice(&a[i..]);
            result
        } else {
            let set_b: std::collections::HashSet<usize> = b.iter().copied().collect();
            let mut result: Vec<usize> = a.iter().copied().filter(|x| !set_b.contains(x)).collect();
            result.sort_unstable();
            result.dedup();
            result
        }
    }

    // -----------------------------------------------------------------------
    // Array cell slicing
    // -----------------------------------------------------------------------

    /// Reads a sub-region of an array cell.
    ///
    /// Returns a new `Value::Array` containing only the elements selected by
    /// the [`Slicer`]. The cell must be an array-valued cell; returns
    /// [`TableError::CellNotArray`] otherwise.
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::getSlice(rownr, slicer)`.
    pub fn get_cell_slice(
        &self,
        column: &str,
        row: usize,
        slicer: &Slicer,
    ) -> Result<Value, TableError> {
        let cell = self
            .cell(row, column)
            .ok_or_else(|| TableError::ColumnNotFound {
                row_index: row,
                column: column.to_string(),
            })?;
        match cell {
            Value::Array(av) => {
                let shape = av.shape();
                validate_slicer_bounds(slicer, shape, row, column)?;
                Ok(Value::Array(slice_array_value(av, slicer)))
            }
            _ => Err(TableError::CellNotArray {
                row,
                column: column.to_string(),
            }),
        }
    }

    /// Writes a sub-region of an array cell.
    ///
    /// Loads the full cell, replaces the slice region with `data`, and writes
    /// the updated array back. Both the existing cell and `data` must be
    /// arrays.
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::putSlice(rownr, slicer, array)`.
    pub fn put_cell_slice(
        &mut self,
        column: &str,
        row: usize,
        slicer: &Slicer,
        data: &ArrayValue,
    ) -> Result<(), TableError> {
        let cell = self
            .inner
            .row_mut(row)
            .and_then(|r| r.get_mut(column))
            .ok_or_else(|| TableError::ColumnNotFound {
                row_index: row,
                column: column.to_string(),
            })?;
        match cell {
            Value::Array(av) => {
                let shape = av.shape();
                validate_slicer_bounds(slicer, shape, row, column)?;
                put_slice_array_value(av, slicer, data);
                Ok(())
            }
            _ => Err(TableError::CellNotArray {
                row,
                column: column.to_string(),
            }),
        }
    }

    /// Reads a sub-region of an array cell for each row in `row_range`,
    /// returning one sliced value per row.
    ///
    /// Combines row selection ([`RowRange`]) with array slicing ([`Slicer`]).
    /// Each returned element is the slice of the array cell for that row.
    ///
    /// # Errors
    ///
    /// - [`TableError::CellNotArray`] if a cell in the column is scalar
    /// - [`TableError::SlicerDimensionMismatch`] if slicer ndim != array ndim
    /// - [`TableError::SlicerOutOfBounds`] if slicer exceeds array shape
    /// - [`TableError::ColumnNotFound`] if `column` does not exist
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::getColumnRange(slicer, rowRange)`.
    pub fn get_column_slice(
        &self,
        column: &str,
        row_range: RowRange,
        slicer: &Slicer,
    ) -> Result<Vec<Value>, TableError> {
        let mut results = Vec::new();
        for row in row_range.iter() {
            if row >= self.row_count() {
                break;
            }
            results.push(self.get_cell_slice(column, row, slicer)?);
        }
        Ok(results)
    }

    /// Writes a sub-region of an array cell for each row in `row_range`.
    ///
    /// `data` must have one element per selected row. Each element replaces
    /// the corresponding slice region in that row's array cell.
    ///
    /// # Errors
    ///
    /// - [`TableError::CellNotArray`] if a cell in the column is scalar
    /// - [`TableError::SlicerDimensionMismatch`] if slicer ndim != array ndim
    /// - [`TableError::SlicerOutOfBounds`] if slicer exceeds array shape
    /// - [`TableError::ColumnNotFound`] if `column` does not exist
    /// - [`TableError::ColumnSliceLengthMismatch`] if `data` does not contain
    ///   one slice per selected in-bounds row
    ///
    /// # C++ equivalent
    ///
    /// `ArrayColumn<T>::putColumnRange(slicer, rowRange, data)`.
    pub fn put_column_slice(
        &mut self,
        column: &str,
        row_range: RowRange,
        slicer: &Slicer,
        data: &[ArrayValue],
    ) -> Result<(), TableError> {
        let rows: Vec<usize> = row_range
            .iter()
            .take_while(|&r| r < self.row_count())
            .collect();
        if rows.len() != data.len() {
            return Err(TableError::ColumnSliceLengthMismatch {
                rows: rows.len(),
                data_len: data.len(),
            });
        }
        for (row, patch) in rows.into_iter().zip(data.iter()) {
            self.put_cell_slice(column, row, slicer, patch)?;
        }
        Ok(())
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

    /// Groups rows by key columns in natural (insertion) order, without sorting.
    ///
    /// Consecutive rows with equal key values are grouped together, but
    /// non-adjacent duplicates appear as separate groups. This is useful when
    /// the table is already sorted or when group ordering must match the
    /// on-disk row order.
    ///
    /// # C++ equivalent
    ///
    /// `TableIterator` constructed with `TableIterator::NoSort`.
    pub fn iter_groups_nosort(
        &self,
        key_columns: &[&str],
    ) -> Result<crate::sorting::TableIterator<'_>, TableError> {
        crate::sorting::TableIterator::new_nosort(self, key_columns)
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
            table_info: self.table_info.clone(),
            virtual_columns: std::collections::HashSet::new(),
            virtual_bindings: Vec::new(),
            dm_info: vec![],
        };
        let storage = CompositeStorage;
        storage.save(
            &opts.path,
            &snapshot,
            opts.data_manager,
            opts.endian_format.is_big_endian(),
            opts.tile_shape.as_deref(),
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

    // ── TaQL query methods ──────────────────────────────────────────

    /// Executes a TaQL SELECT query and returns a [`RefTable`](crate::RefTable) view.
    ///
    /// This is a convenience method that parses the query, executes it, and
    /// wraps the result in a [`RefTable`](crate::RefTable). Only SELECT statements are accepted;
    /// for UPDATE/INSERT/DELETE use [`execute_taql`](Table::execute_taql).
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Taql`] if the query is invalid or execution fails.
    ///
    /// # Examples
    ///
    /// ```rust
    /// # use casacore_tables::{Table, TableSchema, ColumnSchema};
    /// # use casacore_types::*;
    /// # let schema = TableSchema::new(vec![
    /// #     ColumnSchema::scalar("id", PrimitiveType::Int32),
    /// #     ColumnSchema::scalar("flux", PrimitiveType::Float64),
    /// # ]).unwrap();
    /// # let mut table = Table::with_schema(schema);
    /// # for i in 0..5 {
    /// #     table.add_row(RecordValue::new(vec![
    /// #         RecordField::new("id", Value::Scalar(ScalarValue::Int32(i))),
    /// #         RecordField::new("flux", Value::Scalar(ScalarValue::Float64(i as f64))),
    /// #     ])).unwrap();
    /// # }
    /// let view = table.query("SELECT * WHERE flux > 2.0").unwrap();
    /// assert_eq!(view.row_count(), 2);
    /// ```
    ///
    /// C++ equivalent: `tableCommand()` with a SELECT query.
    pub fn query(&mut self, taql: &str) -> Result<crate::RefTable<'_>, TableError> {
        let stmt = crate::taql::parse(taql).map_err(|e| TableError::Taql(e.to_string()))?;
        let result =
            crate::taql::execute(&stmt, self).map_err(|e| TableError::Taql(e.to_string()))?;
        match result {
            crate::taql::TaqlResult::Select {
                row_indices,
                columns,
            } => {
                if columns.is_empty() {
                    crate::RefTable::from_rows(self, row_indices)
                } else {
                    crate::RefTable::from_rows_and_columns(self, row_indices, &columns)
                }
            }
            _ => Err(TableError::Taql(
                "Table::query() only supports SELECT statements; use execute_taql() for mutations"
                    .to_string(),
            )),
        }
    }

    /// Executes any TaQL statement (SELECT, UPDATE, INSERT, DELETE).
    ///
    /// Returns a [`TaqlResult`](crate::taql::TaqlResult) describing the outcome.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::Taql`] if the query is invalid or execution fails.
    ///
    /// C++ equivalent: `tableCommand()`.
    pub fn execute_taql(&mut self, taql: &str) -> Result<crate::taql::TaqlResult, TableError> {
        let stmt = crate::taql::parse(taql).map_err(|e| TableError::Taql(e.to_string()))?;
        crate::taql::execute(&stmt, self).map_err(|e| TableError::Taql(e.to_string()))
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

// ── Slicer helpers ────────────────────────────────────────────────────

fn validate_slicer_bounds(
    slicer: &Slicer,
    shape: &[usize],
    row: usize,
    column: &str,
) -> Result<(), TableError> {
    if slicer.ndim() != shape.len() {
        return Err(TableError::SlicerDimensionMismatch {
            start_ndim: slicer.ndim(),
            end_ndim: shape.len(),
            stride_ndim: slicer.ndim(),
        });
    }
    for (axis, ((&s, &e), &ext)) in slicer
        .start()
        .iter()
        .zip(slicer.end().iter())
        .zip(shape.iter())
        .enumerate()
    {
        if e > ext {
            return Err(TableError::SlicerOutOfBounds {
                axis,
                index: e,
                extent: ext,
            });
        }
        let _ = (s, row, column); // suppress unused warnings
    }
    Ok(())
}

/// Build ndarray `SliceInfoElem` vector from a `Slicer`.
fn slicer_to_slice_elems(slicer: &Slicer) -> Vec<ndarray::SliceInfoElem> {
    slicer
        .start()
        .iter()
        .zip(slicer.end().iter())
        .zip(slicer.stride().iter())
        .map(|((&s, &e), &st)| ndarray::SliceInfoElem::Slice {
            start: s as isize,
            end: Some(e as isize),
            step: st as isize,
        })
        .collect()
}

/// Extract a sub-array from `av` using `slicer`.
fn slice_array_value(av: &ArrayValue, slicer: &Slicer) -> ArrayValue {
    use ndarray::SliceInfoElem;
    let elems = slicer_to_slice_elems(slicer);
    let si: Vec<SliceInfoElem> = elems;

    macro_rules! do_slice {
        ($arr:expr) => {{
            let view = $arr.slice_each_axis(|ax| match si[ax.axis.index()] {
                SliceInfoElem::Slice { start, end, step } => ndarray::Slice { start, end, step },
                _ => unreachable!(),
            });
            view.to_owned()
        }};
    }

    match av {
        ArrayValue::Bool(a) => ArrayValue::Bool(do_slice!(a)),
        ArrayValue::UInt8(a) => ArrayValue::UInt8(do_slice!(a)),
        ArrayValue::UInt16(a) => ArrayValue::UInt16(do_slice!(a)),
        ArrayValue::UInt32(a) => ArrayValue::UInt32(do_slice!(a)),
        ArrayValue::Int16(a) => ArrayValue::Int16(do_slice!(a)),
        ArrayValue::Int32(a) => ArrayValue::Int32(do_slice!(a)),
        ArrayValue::Int64(a) => ArrayValue::Int64(do_slice!(a)),
        ArrayValue::Float32(a) => ArrayValue::Float32(do_slice!(a)),
        ArrayValue::Float64(a) => ArrayValue::Float64(do_slice!(a)),
        ArrayValue::Complex32(a) => ArrayValue::Complex32(do_slice!(a)),
        ArrayValue::Complex64(a) => ArrayValue::Complex64(do_slice!(a)),
        ArrayValue::String(a) => ArrayValue::String(do_slice!(a)),
    }
}

/// Write `data` into a sub-region of `target` specified by `slicer`.
fn put_slice_array_value(target: &mut ArrayValue, slicer: &Slicer, data: &ArrayValue) {
    use ndarray::SliceInfoElem;
    let elems = slicer_to_slice_elems(slicer);
    let si: Vec<SliceInfoElem> = elems;

    macro_rules! do_put {
        ($dst:expr, $src:expr) => {{
            let mut view = $dst.slice_each_axis_mut(|ax| match si[ax.axis.index()] {
                SliceInfoElem::Slice { start, end, step } => ndarray::Slice { start, end, step },
                _ => unreachable!(),
            });
            view.assign($src);
        }};
    }

    match (target, data) {
        (ArrayValue::Bool(t), ArrayValue::Bool(s)) => do_put!(t, s),
        (ArrayValue::UInt8(t), ArrayValue::UInt8(s)) => do_put!(t, s),
        (ArrayValue::UInt16(t), ArrayValue::UInt16(s)) => do_put!(t, s),
        (ArrayValue::UInt32(t), ArrayValue::UInt32(s)) => do_put!(t, s),
        (ArrayValue::Int16(t), ArrayValue::Int16(s)) => do_put!(t, s),
        (ArrayValue::Int32(t), ArrayValue::Int32(s)) => do_put!(t, s),
        (ArrayValue::Int64(t), ArrayValue::Int64(s)) => do_put!(t, s),
        (ArrayValue::Float32(t), ArrayValue::Float32(s)) => do_put!(t, s),
        (ArrayValue::Float64(t), ArrayValue::Float64(s)) => do_put!(t, s),
        (ArrayValue::Complex32(t), ArrayValue::Complex32(s)) => do_put!(t, s),
        (ArrayValue::Complex64(t), ArrayValue::Complex64(s)) => do_put!(t, s),
        (ArrayValue::String(t), ArrayValue::String(s)) => do_put!(t, s),
        _ => {} // type mismatch silently ignored (validated at higher level)
    }
}

impl Drop for Table {
    fn drop(&mut self) {
        if self.marked_for_delete {
            if let Some(path) = &self.source_path {
                let _ = std::fs::remove_dir_all(path);
            }
        }
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

    // -------------------------------------------------------------------
    // Virtual column tests
    // -------------------------------------------------------------------

    #[test]
    fn forward_column_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("base_table");
        let fwd_path = dir.path().join("fwd_table");

        // Create and save a base table with some data.
        let base_schema = TableSchema::new(vec![ColumnSchema::scalar(
            "value",
            casacore_types::PrimitiveType::Float64,
        )])
        .unwrap();
        let mut base = Table::with_schema(base_schema);
        for v in [1.5, 2.5, 3.5] {
            base.add_row(RecordValue::new(vec![RecordField::new(
                "value",
                Value::Scalar(ScalarValue::Float64(v)),
            )]))
            .unwrap();
        }
        base.save(TableOptions::new(&base_path)).unwrap();

        // Create a forwarding table that references the base table's "value" column.
        let fwd_schema = TableSchema::new(vec![ColumnSchema::scalar(
            "value",
            casacore_types::PrimitiveType::Float64,
        )])
        .unwrap();
        let mut fwd = Table::with_schema(fwd_schema);
        for _ in 0..3 {
            fwd.add_row(RecordValue::new(vec![RecordField::new(
                "value",
                Value::Scalar(ScalarValue::Float64(0.0)),
            )]))
            .unwrap();
        }
        fwd.bind_forward_column("value", &base_path).unwrap();
        fwd.save(TableOptions::new(&fwd_path)).unwrap();

        // Reopen and verify forwarded values.
        let reopened = Table::open(TableOptions::new(&fwd_path)).unwrap();
        assert_eq!(reopened.row_count(), 3);
        assert!(reopened.is_virtual_column("value"));
        for (i, expected) in [1.5, 2.5, 3.5].iter().enumerate() {
            let val = reopened.cell(i, "value").unwrap();
            match val {
                Value::Scalar(ScalarValue::Float64(v)) => {
                    assert!(
                        (v - expected).abs() < 1e-10,
                        "row {i}: expected {expected}, got {v}"
                    );
                }
                other => panic!("row {i}: expected Float64, got {other:?}"),
            }
        }
    }

    #[test]
    fn scaled_array_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let table_path = dir.path().join("scaled_table");

        let scale = 2.5;
        let offset = 10.0;

        // Schema: stored_col (Int32 scalar), virtual_col (Float64 scalar).
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("stored_col", casacore_types::PrimitiveType::Int32),
            ColumnSchema::scalar("virtual_col", casacore_types::PrimitiveType::Float64),
        ])
        .unwrap();

        let mut table = Table::with_schema(schema);
        for i in [1i32, 2, 3] {
            // Only stored_col has meaningful data; virtual_col is a placeholder.
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("stored_col", Value::Scalar(ScalarValue::Int32(i))),
                    RecordField::new("virtual_col", Value::Scalar(ScalarValue::Float64(0.0))),
                ]))
                .unwrap();
        }
        table
            .bind_scaled_array_column("virtual_col", "stored_col", scale, offset)
            .unwrap();
        table.save(TableOptions::new(&table_path)).unwrap();

        // Reopen and verify: virtual = stored * 2.5 + 10.0
        let reopened = Table::open(TableOptions::new(&table_path)).unwrap();
        assert_eq!(reopened.row_count(), 3);
        assert!(reopened.is_virtual_column("virtual_col"));
        assert!(!reopened.is_virtual_column("stored_col"));

        for (i, stored) in [1i32, 2, 3].iter().enumerate() {
            let expected = (*stored as f64) * scale + offset;
            let val = reopened.cell(i, "virtual_col").unwrap();
            match val {
                Value::Scalar(ScalarValue::Float64(v)) => {
                    assert!(
                        (v - expected).abs() < 1e-10,
                        "row {i}: expected {expected}, got {v}"
                    );
                }
                other => panic!("row {i}: expected Float64, got {other:?}"),
            }
        }
    }

    #[test]
    fn is_virtual_column_empty_for_plain_table() {
        let table = Table::new();
        assert!(!table.is_virtual_column("anything"));
    }

    #[test]
    fn multi_dm_round_trip() {
        // Test that a table with both stored and virtual columns produces
        // multiple DM entries in table.dat after save/reload.
        let dir = tempfile::tempdir().unwrap();
        let base_path = dir.path().join("base");
        let main_path = dir.path().join("main");

        // Base table for forward column.
        let base_schema = TableSchema::new(vec![ColumnSchema::scalar(
            "fwd_col",
            casacore_types::PrimitiveType::Float64,
        )])
        .unwrap();
        let mut base = Table::with_schema(base_schema);
        base.add_row(RecordValue::new(vec![RecordField::new(
            "fwd_col",
            Value::Scalar(ScalarValue::Float64(42.0)),
        )]))
        .unwrap();
        base.save(TableOptions::new(&base_path)).unwrap();

        // Main table with stored + forward + scaled columns.
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("stored_int", casacore_types::PrimitiveType::Int32),
            ColumnSchema::scalar("fwd_col", casacore_types::PrimitiveType::Float64),
            ColumnSchema::scalar("scaled_col", casacore_types::PrimitiveType::Float64),
        ])
        .unwrap();

        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("stored_int", Value::Scalar(ScalarValue::Int32(5))),
                RecordField::new("fwd_col", Value::Scalar(ScalarValue::Float64(0.0))),
                RecordField::new("scaled_col", Value::Scalar(ScalarValue::Float64(0.0))),
            ]))
            .unwrap();

        table.bind_forward_column("fwd_col", &base_path).unwrap();
        table
            .bind_scaled_array_column("scaled_col", "stored_int", 3.0, 1.0)
            .unwrap();
        table.save(TableOptions::new(&main_path)).unwrap();

        // Reopen and verify all columns.
        let reopened = Table::open(TableOptions::new(&main_path)).unwrap();
        assert_eq!(reopened.row_count(), 1);
        assert!(!reopened.is_virtual_column("stored_int"));
        assert!(reopened.is_virtual_column("fwd_col"));
        assert!(reopened.is_virtual_column("scaled_col"));

        // stored_int should be 5
        match reopened.cell(0, "stored_int").unwrap() {
            Value::Scalar(ScalarValue::Int32(v)) => assert_eq!(*v, 5),
            other => panic!("expected Int32(5), got {other:?}"),
        }

        // fwd_col should be 42.0 (from base table)
        match reopened.cell(0, "fwd_col").unwrap() {
            Value::Scalar(ScalarValue::Float64(v)) => {
                assert!((v - 42.0).abs() < 1e-10, "fwd_col: expected 42.0, got {v}");
            }
            other => panic!("expected Float64(42.0), got {other:?}"),
        }

        // scaled_col should be 5 * 3.0 + 1.0 = 16.0
        match reopened.cell(0, "scaled_col").unwrap() {
            Value::Scalar(ScalarValue::Float64(v)) => {
                assert!(
                    (v - 16.0).abs() < 1e-10,
                    "scaled_col: expected 16.0, got {v}"
                );
            }
            other => panic!("expected Float64(16.0), got {other:?}"),
        }
    }

    // -------------------------------------------------------------------
    // TableInfo round-trip tests
    // -------------------------------------------------------------------

    #[test]
    fn table_info_default_is_empty() {
        let table = Table::new();
        assert_eq!(table.info().table_type, "");
        assert_eq!(table.info().sub_type, "");
    }

    #[test]
    fn table_info_set_and_get() {
        use crate::storage::TableInfo;
        let mut table = Table::new();
        table.set_info(TableInfo {
            table_type: "MeasurementSet".to_string(),
            sub_type: "UVFITS".to_string(),
        });
        assert_eq!(table.info().table_type, "MeasurementSet");
        assert_eq!(table.info().sub_type, "UVFITS");
    }

    #[test]
    fn table_info_round_trip_disk() {
        use crate::storage::TableInfo;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("info_test.tbl");

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        table.set_info(TableInfo {
            table_type: "MeasurementSet".to_string(),
            sub_type: "UVFITS".to_string(),
        });
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "id",
                Value::Scalar(ScalarValue::Int32(42)),
            )]))
            .unwrap();
        table.save(TableOptions::new(&path)).unwrap();

        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        assert_eq!(reopened.info().table_type, "MeasurementSet");
        assert_eq!(reopened.info().sub_type, "UVFITS");
    }

    #[test]
    fn table_info_empty_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_info.tbl");

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("id", PrimitiveType::Int32)]).unwrap();
        let table = Table::with_schema(schema);
        table.save(TableOptions::new(&path)).unwrap();

        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        assert_eq!(reopened.info().table_type, "");
        assert_eq!(reopened.info().sub_type, "");
    }

    #[test]
    fn table_info_preserved_by_to_memory() {
        use crate::storage::TableInfo;
        let mut table = Table::new();
        table.set_info(TableInfo {
            table_type: "Catalog".to_string(),
            sub_type: "".to_string(),
        });
        let mem = table.to_memory();
        assert_eq!(mem.info().table_type, "Catalog");
    }

    #[test]
    fn table_info_preserved_by_deep_copy() {
        use crate::storage::TableInfo;
        let dir = tempfile::tempdir().unwrap();
        let src_path = dir.path().join("src.tbl");
        let dst_path = dir.path().join("dst.tbl");

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Float64)]).unwrap();
        let mut table = Table::with_schema(schema);
        table.set_info(TableInfo {
            table_type: "Sky".to_string(),
            sub_type: "Model".to_string(),
        });
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Float64(1.0)),
            )]))
            .unwrap();
        table.save(TableOptions::new(&src_path)).unwrap();

        let original = Table::open(TableOptions::new(&src_path)).unwrap();
        original.deep_copy(TableOptions::new(&dst_path)).unwrap();

        let copy = Table::open(TableOptions::new(&dst_path)).unwrap();
        assert_eq!(copy.info().table_type, "Sky");
        assert_eq!(copy.info().sub_type, "Model");
    }

    // -------------------------------------------------------------------
    // Lifecycle operation tests
    // -------------------------------------------------------------------

    #[test]
    fn flush_writes_changes_to_disk() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("flush_test.tbl");

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(1)),
            )]))
            .unwrap();
        table.save(TableOptions::new(&path)).unwrap();

        // Reopen, mutate, and flush
        let mut table = Table::open(TableOptions::new(&path)).unwrap();
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(2)),
            )]))
            .unwrap();
        table.flush().unwrap();

        // Reopen and verify both rows
        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        assert_eq!(reopened.row_count(), 2);
    }

    #[test]
    fn flush_without_path_fails() {
        let table = Table::new();
        assert!(table.flush().is_err());
    }

    #[test]
    fn resync_discards_in_memory_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("resync_test.tbl");

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(1)),
            )]))
            .unwrap();
        table.save(TableOptions::new(&path)).unwrap();

        // Open from disk, add a row in memory (not saved)
        let mut table = Table::open(TableOptions::new(&path)).unwrap();
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(2)),
            )]))
            .unwrap();
        assert_eq!(table.row_count(), 2);

        // Resync discards the unsaved row
        table.resync().unwrap();
        assert_eq!(table.row_count(), 1);
    }

    #[test]
    fn resync_without_path_fails() {
        let mut table = Table::new();
        assert!(table.resync().is_err());
    }

    #[test]
    fn mark_for_delete_removes_directory_on_drop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("delete_me.tbl");

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let table = Table::with_schema(schema);
        table.save(TableOptions::new(&path)).unwrap();
        assert!(path.exists());

        let mut table = Table::open(TableOptions::new(&path)).unwrap();
        table.mark_for_delete();
        assert!(table.is_marked_for_delete());

        drop(table);
        assert!(!path.exists(), "table directory should be deleted on drop");
    }

    #[test]
    fn unmark_for_delete_prevents_removal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("keep_me.tbl");

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let table = Table::with_schema(schema);
        table.save(TableOptions::new(&path)).unwrap();

        let mut table = Table::open(TableOptions::new(&path)).unwrap();
        table.mark_for_delete();
        table.unmark_for_delete();
        assert!(!table.is_marked_for_delete());

        drop(table);
        assert!(path.exists(), "table directory should still exist");
    }

    // -------------------------------------------------------------------
    // Locking extension tests
    // -------------------------------------------------------------------

    #[test]
    fn lock_mode_resolve() {
        use crate::lock::LockMode;
        assert_eq!(LockMode::DefaultLocking.resolve(), LockMode::AutoLocking);
        assert_eq!(LockMode::AutoLocking.resolve(), LockMode::AutoLocking);
        assert_eq!(LockMode::NoLocking.resolve(), LockMode::NoLocking);
    }

    #[test]
    fn lock_mode_skip_read_lock() {
        use crate::lock::LockMode;
        assert!(LockMode::AutoNoReadLocking.skip_read_lock());
        assert!(LockMode::UserNoReadLocking.skip_read_lock());
        assert!(!LockMode::AutoLocking.skip_read_lock());
        assert!(!LockMode::UserLocking.skip_read_lock());
    }

    #[test]
    fn external_sync_hook_ordering() {
        use crate::lock::ExternalLockSync;
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct Recorder(Arc<Mutex<Vec<&'static str>>>);
        impl ExternalLockSync for Recorder {
            fn acquire_read(&self) {
                self.0.lock().unwrap().push("acquire_read");
            }
            fn acquire_write(&self) {
                self.0.lock().unwrap().push("acquire_write");
            }
            fn release(&self) {
                self.0.lock().unwrap().push("release");
            }
        }

        let log = Arc::new(Mutex::new(Vec::new()));
        let recorder = Recorder(log.clone());

        // Verify the trait is object-safe and can be boxed
        let _: Box<dyn ExternalLockSync> = Box::new(recorder);

        // Verify the log works
        let recorder2 = Recorder(log.clone());
        recorder2.acquire_read();
        recorder2.acquire_write();
        recorder2.release();

        let events = log.lock().unwrap();
        assert_eq!(&*events, &["acquire_read", "acquire_write", "release"]);
    }

    // -------------------------------------------------------------------
    // Set algebra tests
    // -------------------------------------------------------------------

    #[test]
    fn row_union_merges_and_deduplicates() {
        assert_eq!(
            Table::row_union(&[0, 2, 4], &[1, 2, 3]),
            vec![0, 1, 2, 3, 4]
        );
    }

    #[test]
    fn row_intersection_keeps_common() {
        assert_eq!(
            Table::row_intersection(&[0, 1, 2, 3], &[2, 3, 4]),
            vec![2, 3]
        );
    }

    #[test]
    fn row_difference_removes_second() {
        assert_eq!(Table::row_difference(&[0, 1, 2, 3], &[1, 3]), vec![0, 2]);
    }

    #[test]
    fn row_set_ops_with_empty() {
        assert!(Table::row_intersection(&[0, 1], &[]).is_empty());
        assert_eq!(Table::row_union(&[], &[3, 1]), vec![1, 3]);
        assert_eq!(Table::row_difference(&[5, 3], &[]), vec![3, 5]);
    }

    // -------------------------------------------------------------------
    // NoSort iteration tests
    // -------------------------------------------------------------------

    #[test]
    fn iter_groups_nosort_preserves_natural_order() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        // Insert pattern: A, B, A — nosort should yield 3 groups
        for v in [1, 2, 1] {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "k",
                    Value::Scalar(ScalarValue::Int32(v)),
                )]))
                .unwrap();
        }

        let groups: Vec<_> = table.iter_groups_nosort(&["k"]).unwrap().collect();
        assert_eq!(
            groups.len(),
            3,
            "nosort should not merge non-adjacent duplicates"
        );
        assert_eq!(groups[0].row_indices, vec![0]); // k=1
        assert_eq!(groups[1].row_indices, vec![1]); // k=2
        assert_eq!(groups[2].row_indices, vec![2]); // k=1 again (separate group)
    }

    #[test]
    fn iter_groups_nosort_merges_consecutive_equal() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("k", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        for v in [1, 1, 2, 2, 2] {
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "k",
                    Value::Scalar(ScalarValue::Int32(v)),
                )]))
                .unwrap();
        }

        let groups: Vec<_> = table.iter_groups_nosort(&["k"]).unwrap().collect();
        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].row_indices, vec![0, 1]); // k=1
        assert_eq!(groups[1].row_indices, vec![2, 3, 4]); // k=2
    }

    // -------------------------------------------------------------------
    // Slicer and cell slicing tests
    // -------------------------------------------------------------------

    #[test]
    fn slicer_contiguous_2d() {
        use super::Slicer;
        let s = Slicer::contiguous(vec![0, 1], vec![2, 3]).unwrap();
        assert_eq!(s.ndim(), 2);
        assert_eq!(s.start(), &[0, 1]);
        assert_eq!(s.end(), &[2, 3]);
        assert_eq!(s.stride(), &[1, 1]);
    }

    #[test]
    fn slicer_rejects_zero_stride() {
        use super::Slicer;
        assert!(Slicer::new(vec![0], vec![5], vec![0]).is_err());
    }

    #[test]
    fn slicer_rejects_start_gt_end() {
        use super::Slicer;
        assert!(Slicer::new(vec![5], vec![3], vec![1]).is_err());
    }

    #[test]
    fn get_cell_slice_2d() {
        use super::Slicer;
        use casacore_types::ArrayD;
        use ndarray::IxDyn;

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Float64,
            vec![3, 4],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);

        // 3x4 array filled with value = row*10 + col
        let arr: ArrayD<f64> =
            ArrayD::from_shape_fn(IxDyn(&[3, 4]), |idx| (idx[0] * 10 + idx[1]) as f64);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(arr)),
            )]))
            .unwrap();

        // Slice rows 1..3, cols 2..4
        let slicer = Slicer::contiguous(vec![1, 2], vec![3, 4]).unwrap();
        let sliced = table.get_cell_slice("data", 0, &slicer).unwrap();

        match sliced {
            Value::Array(ArrayValue::Float64(a)) => {
                assert_eq!(a.shape(), &[2, 2]);
                assert_eq!(a[[0, 0]], 12.0); // row=1, col=2
                assert_eq!(a[[0, 1]], 13.0); // row=1, col=3
                assert_eq!(a[[1, 0]], 22.0); // row=2, col=2
                assert_eq!(a[[1, 1]], 23.0); // row=2, col=3
            }
            other => panic!("expected Float64 array, got {other:?}"),
        }
    }

    #[test]
    fn put_cell_slice_2d() {
        use super::Slicer;
        use casacore_types::ArrayD;
        use ndarray::IxDyn;

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Float64,
            vec![3, 4],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);

        let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[3, 4]));
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Float64(arr)),
            )]))
            .unwrap();

        // Write 99.0 into the [1..3, 0..2] sub-region
        let patch: ArrayD<f64> = ArrayD::from_elem(IxDyn(&[2, 2]), 99.0);
        let slicer = Slicer::contiguous(vec![1, 0], vec![3, 2]).unwrap();
        table
            .put_cell_slice("data", 0, &slicer, &ArrayValue::Float64(patch))
            .unwrap();

        match table.cell(0, "data").unwrap() {
            Value::Array(ArrayValue::Float64(a)) => {
                assert_eq!(a[[0, 0]], 0.0); // untouched
                assert_eq!(a[[1, 0]], 99.0); // patched
                assert_eq!(a[[2, 1]], 99.0); // patched
                assert_eq!(a[[0, 3]], 0.0); // untouched
            }
            other => panic!("expected Float64 array, got {other:?}"),
        }
    }

    #[test]
    fn get_cell_slice_with_stride() {
        use super::Slicer;
        use casacore_types::ArrayD;
        use ndarray::IxDyn;

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Int32,
            vec![6],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);

        let arr: ArrayD<i32> = ArrayD::from_shape_fn(IxDyn(&[6]), |idx| idx[0] as i32);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "data",
                Value::Array(ArrayValue::Int32(arr)),
            )]))
            .unwrap();

        // Every other element: [0, 2, 4]
        let slicer = Slicer::new(vec![0], vec![6], vec![2]).unwrap();
        let sliced = table.get_cell_slice("data", 0, &slicer).unwrap();

        match sliced {
            Value::Array(ArrayValue::Int32(a)) => {
                assert_eq!(a.as_slice().unwrap(), &[0, 2, 4]);
            }
            other => panic!("expected Int32 array, got {other:?}"),
        }
    }

    #[test]
    fn get_cell_slice_scalar_cell_fails() {
        use super::Slicer;

        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![RecordField::new(
                "x",
                Value::Scalar(ScalarValue::Int32(42)),
            )]))
            .unwrap();

        let slicer = Slicer::contiguous(vec![0], vec![1]).unwrap();
        assert!(table.get_cell_slice("x", 0, &slicer).is_err());
    }

    #[test]
    fn get_column_slice_multiple_rows() {
        use super::{RowRange, Slicer};
        use casacore_types::ArrayD;
        use ndarray::IxDyn;

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Int32,
            vec![4],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);

        for i in 0..3 {
            let arr = ArrayD::from_shape_fn(IxDyn(&[4]), |idx| (i * 10 + idx[0]) as i32);
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Int32(arr)),
                )]))
                .unwrap();
        }

        // Slice elements [1..3] from rows 0 and 1
        let slicer = Slicer::contiguous(vec![1], vec![3]).unwrap();
        let results = table
            .get_column_slice("data", RowRange::new(0, 2), &slicer)
            .unwrap();

        assert_eq!(results.len(), 2);
        match &results[0] {
            Value::Array(ArrayValue::Int32(a)) => assert_eq!(a.as_slice().unwrap(), &[1, 2]),
            other => panic!("expected Int32 array, got {other:?}"),
        }
        match &results[1] {
            Value::Array(ArrayValue::Int32(a)) => assert_eq!(a.as_slice().unwrap(), &[11, 12]),
            other => panic!("expected Int32 array, got {other:?}"),
        }
    }

    #[test]
    fn put_column_slice_multiple_rows() {
        use super::{RowRange, Slicer};
        use casacore_types::ArrayD;
        use ndarray::IxDyn;

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Float64,
            vec![4],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);

        for _ in 0..3 {
            let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[4]));
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Float64(arr)),
                )]))
                .unwrap();
        }

        // Patch elements [1..3] in rows 0 and 2 (stride=2)
        let slicer = Slicer::contiguous(vec![1], vec![3]).unwrap();
        let patches = vec![
            ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 11.0)),
            ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 22.0)),
        ];
        table
            .put_column_slice("data", RowRange::with_stride(0, 3, 2), &slicer, &patches)
            .unwrap();

        // Row 0: [0, 11, 11, 0]
        match table.cell(0, "data").unwrap() {
            Value::Array(ArrayValue::Float64(a)) => {
                assert_eq!(a.as_slice().unwrap(), &[0.0, 11.0, 11.0, 0.0]);
            }
            other => panic!("unexpected {other:?}"),
        }
        // Row 1: untouched
        match table.cell(1, "data").unwrap() {
            Value::Array(ArrayValue::Float64(a)) => {
                assert_eq!(a.as_slice().unwrap(), &[0.0, 0.0, 0.0, 0.0]);
            }
            other => panic!("unexpected {other:?}"),
        }
        // Row 2: [0, 22, 22, 0]
        match table.cell(2, "data").unwrap() {
            Value::Array(ArrayValue::Float64(a)) => {
                assert_eq!(a.as_slice().unwrap(), &[0.0, 22.0, 22.0, 0.0]);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn put_column_slice_length_mismatch() {
        use super::{RowRange, Slicer};
        use casacore_types::ArrayD;
        use ndarray::IxDyn;

        let schema = TableSchema::new(vec![ColumnSchema::array_fixed(
            "data",
            PrimitiveType::Float64,
            vec![4],
        )])
        .unwrap();
        let mut table = Table::with_schema(schema);

        for _ in 0..2 {
            let arr: ArrayD<f64> = ArrayD::zeros(IxDyn(&[4]));
            table
                .add_row(RecordValue::new(vec![RecordField::new(
                    "data",
                    Value::Array(ArrayValue::Float64(arr)),
                )]))
                .unwrap();
        }

        let slicer = Slicer::contiguous(vec![0], vec![2]).unwrap();
        // 2 rows selected but only 1 data element
        let patches = vec![ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[2]), 1.0))];
        let result = table.put_column_slice("data", RowRange::new(0, 2), &slicer, &patches);
        assert!(result.is_err());
    }

    // ---- Row copy and fill tests ----

    #[test]
    fn copy_rows_appends_all() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let mut dst = Table::with_schema(schema.clone());
        dst.add_row(RecordValue::new(vec![RecordField::new(
            "x",
            Value::Scalar(ScalarValue::Int32(0)),
        )]))
        .unwrap();

        let src = Table::from_rows_with_schema(
            vec![
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(1)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(2)),
                )]),
            ],
            schema,
        )
        .unwrap();

        dst.copy_rows(&src).unwrap();
        assert_eq!(dst.row_count(), 3);
        assert_eq!(
            dst.cell(2, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(2)))
        );
    }

    #[test]
    fn copy_rows_schema_mismatch() {
        let s1 = TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let s2 = TableSchema::new(vec![ColumnSchema::scalar("y", PrimitiveType::Int32)]).unwrap();
        let mut dst = Table::with_schema(s1);
        let src = Table::with_schema(s2);
        assert!(dst.copy_rows(&src).is_err());
    }

    #[test]
    fn copy_rows_with_mapping_selects_rows() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let src = Table::from_rows_with_schema(
            vec![
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(10)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(20)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(30)),
                )]),
            ],
            schema.clone(),
        )
        .unwrap();

        let mut dst = Table::with_schema(schema);
        dst.copy_rows_with_mapping(&src, &[2, 0]).unwrap();
        assert_eq!(dst.row_count(), 2);
        assert_eq!(
            dst.cell(0, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(30)))
        );
        assert_eq!(
            dst.cell(1, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(10)))
        );
    }

    #[test]
    fn copy_info_transfers_metadata() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let mut src = Table::with_schema(schema.clone());
        src.set_info(crate::TableInfo {
            table_type: "MeasurementSet".into(),
            sub_type: "".into(),
        });

        let mut dst = Table::with_schema(schema);
        dst.copy_info(&src);
        assert_eq!(dst.info().table_type, "MeasurementSet");
    }

    #[test]
    fn fill_column_sets_all_cells() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::from_rows_with_schema(
            vec![
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(1)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(2)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(3)),
                )]),
            ],
            schema,
        )
        .unwrap();

        table
            .fill_column("x", Value::Scalar(ScalarValue::Int32(99)))
            .unwrap();
        for i in 0..3 {
            assert_eq!(
                table.cell(i, "x"),
                Some(&Value::Scalar(ScalarValue::Int32(99)))
            );
        }
    }

    #[test]
    fn fill_column_range_sets_subset() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let mut table = Table::from_rows_with_schema(
            vec![
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(0)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(0)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(0)),
                )]),
                RecordValue::new(vec![RecordField::new(
                    "x",
                    Value::Scalar(ScalarValue::Int32(0)),
                )]),
            ],
            schema,
        )
        .unwrap();

        // Fill only rows 1 and 3 (stride=2 starting at 1)
        table
            .fill_column_range(
                "x",
                super::RowRange::with_stride(1, 4, 2),
                Value::Scalar(ScalarValue::Int32(77)),
            )
            .unwrap();

        assert_eq!(
            table.cell(0, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
        );
        assert_eq!(
            table.cell(1, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(77)))
        );
        assert_eq!(
            table.cell(2, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(0)))
        );
        assert_eq!(
            table.cell(3, "x"),
            Some(&Value::Scalar(ScalarValue::Int32(77)))
        );
    }

    // -------------------------------------------------------------------
    // Wave 24 — Data manager introspection
    // -------------------------------------------------------------------

    #[test]
    fn data_manager_info_empty_for_memory_table() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("x", PrimitiveType::Int32)]).unwrap();
        let table = Table::with_schema(schema);
        assert!(table.data_manager_info().is_empty());
    }

    #[test]
    fn data_manager_info_populated_after_roundtrip() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("a", PrimitiveType::Int32),
            ColumnSchema::scalar("b", PrimitiveType::Float64),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("a", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("b", Value::Scalar(ScalarValue::Float64(2.0))),
            ]))
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dm_info_test");
        table.save(TableOptions::new(&path)).unwrap();

        let reopened = Table::open(TableOptions::new(&path)).unwrap();
        let info = reopened.data_manager_info();
        assert!(!info.is_empty(), "should have at least one DM");
        // All columns should appear somewhere across the DMs
        let all_cols: Vec<&str> = info
            .iter()
            .flat_map(|dm| dm.columns.iter().map(|s| s.as_str()))
            .collect();
        assert!(all_cols.contains(&"a"));
        assert!(all_cols.contains(&"b"));
    }

    #[test]
    fn show_structure_contains_columns_and_rows() {
        let schema = TableSchema::new(vec![
            ColumnSchema::scalar("id", PrimitiveType::Int32),
            ColumnSchema::scalar("flux", PrimitiveType::Float64),
        ])
        .unwrap();
        let mut table = Table::with_schema(schema);
        table
            .add_row(RecordValue::new(vec![
                RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
                RecordField::new("flux", Value::Scalar(ScalarValue::Float64(1.5))),
            ]))
            .unwrap();

        let output = table.show_structure();
        assert!(output.contains("1 rows"), "should show row count");
        assert!(output.contains("id"), "should list id column");
        assert!(output.contains("flux"), "should list flux column");
        assert!(output.contains("Scalar"), "should show scalar type");
    }

    #[test]
    fn show_keywords_includes_table_and_column_keywords() {
        let schema =
            TableSchema::new(vec![ColumnSchema::scalar("flux", PrimitiveType::Float64)]).unwrap();
        let mut table = Table::with_schema(schema);
        *table.keywords_mut() = RecordValue::new(vec![RecordField::new(
            "telescope",
            Value::Scalar(ScalarValue::String("ALMA".into())),
        )]);
        table.set_column_keywords(
            "flux",
            RecordValue::new(vec![RecordField::new(
                "unit",
                Value::Scalar(ScalarValue::String("Jy".into())),
            )]),
        );

        let output = table.show_keywords();
        assert!(
            output.contains("Table keywords:"),
            "should have table keywords header"
        );
        assert!(
            output.contains("telescope"),
            "should show telescope keyword"
        );
        assert!(
            output.contains("Column \"flux\" keywords:"),
            "should have column keywords header"
        );
        assert!(output.contains("unit"), "should show unit keyword");
    }
}
