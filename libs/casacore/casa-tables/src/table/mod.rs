// SPDX-License-Identifier: LGPL-3.0-or-later
use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_core::{
    ArrayValue, Complex32, Complex64, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
    ValueKind,
};
use thiserror::Error;

#[cfg(unix)]
use crate::lock::LockFile;
use crate::lock::SyncData;
#[cfg(unix)]
use crate::lock::publish_persisted_write;
use crate::lock::{LockMode, LockOptions, LockType};
use crate::schema::{ArrayShapeContract, ColumnSchema, ColumnType, SchemaError, TableSchema};
use crate::storage::virtual_engine::VirtualColumnBinding;
use crate::storage::{CompositeStorage, StorageSnapshot, TableInfo};
use crate::table::columns::collect_undefined_cells_for_schema;
use crate::table_impl::TableImpl;

mod asdm_reference;
mod cache_paths;
mod columns;
mod description;
mod dm_info;
mod generated_array;
mod incremental_check;
mod io;
mod locking;
mod memory;
mod memory_manager;
mod mutation;
mod packed_write;
mod persisted_rows;
mod query;
mod reference_writes;
mod root_rows;
mod row_changes;
pub use row_changes::{
    ColumnAddition, CompressedComplexArray, EncodedArrayAppend, ManagerAddition, PreparedTableEdit,
    RecordAppendOptions,
};
mod streamed_scalar;
mod structure;
mod virtual_columns;

pub use cache_paths::{PlainReferenceRead, TablePartNames, table_cache_name};
pub use description::StoredTableDescription;
pub use generated_array::{GeneratedArrayColumn, GeneratedArrayValueRun};
pub use incremental_check::{IncrementalCorruption, IncrementalLayoutCheck};
pub use streamed_scalar::{
    STREAMING_SCALAR_COLUMN_BUFFER_BYTES, StreamedScalarColumn, StreamedScalarType,
    StreamedScalarValues, StreamingScalarColumnWriter,
};

#[cfg(test)]
mod tests;

mod access;
mod selected_arrays;
mod write_options;

pub use access::ColumnCellIter;
pub use access::ColumnCellRef;
pub use access::ColumnChunkIter;
pub use access::PreparedRowAppender;
pub(crate) use access::PreparedRowSlot;
pub(crate) use access::PreparedRowSlotKind;
pub use access::PreparedTableRow;
pub use access::PreparedTableRowMut;
pub use access::RecordColumnCell;
pub use access::RecordColumnIter;
pub use access::RequiredScalarColumnDestination;
pub use access::RequiredScalarColumnValues;
pub use access::RequiredScalarColumnValuesMut;
pub use access::RowRange;
pub use access::Slicer;
pub use access::TableCell;
pub use access::TableCellMut;
pub use access::TableColumn;
pub use access::TableColumnMut;
pub use access::TableRow;
pub use access::TableRowMut;
pub use access::TableWritePlan;
pub use selected_arrays::SelectedArray1D;
pub use selected_arrays::SelectedArray1DCells;
pub use selected_arrays::SelectedArray1DCellsMut;
pub use selected_arrays::SelectedArray1DShape;
pub use selected_arrays::SelectedArray2D;
pub use selected_arrays::SelectedArray2DCells;
pub use selected_arrays::SelectedArray2DCellsMut;
pub use selected_arrays::SelectedArray2DShape;
pub use selected_arrays::SelectedReadFootprint;
pub use write_options::ColumnOverride;
pub use write_options::ColumnOverrides;
pub use write_options::GeneratedScalarColumn;
pub use write_options::GeneratedScalarValueRun;

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
/// This is the main public hook for steering large-write performance.
/// For heterogeneous tables such as MeasurementSets, explicit bindings let
/// the save path serialize compact column groups with storage-manager-specific
/// code paths instead of treating the entire row as one generic record payload.
///
/// As a rule of thumb:
/// - use tiled storage managers for large array-valued columns
/// - use [`DataManagerKind::IncrementalStMan`] for slowly changing scalar
///   columns
/// - use [`DataManagerKind::StandardStMan`] for scalar columns that do not
///   compress well under ISM
///
/// Callers writing large tables should prefer explicit bindings over relying
/// on a single default data manager for every column.
///
/// # Example
///
/// ```rust
/// use casa_tables::{ColumnBinding, DataManagerKind};
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
/// use casa_tables::{TableOptions, DataManagerKind, EndianFormat};
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
    data_manager_group: Option<String>,
    casacore_default_ssm: bool,
    preserve_ssm_group_name: bool,
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
            data_manager_group: None,
            casacore_default_ssm: false,
            preserve_ssm_group_name: false,
            endian_format: EndianFormat::default(),
            tile_shape: None,
        }
    }

    /// Targets `path` instead, keeping every other option, returning the
    /// updated options.
    pub fn with_path(mut self, path: impl AsRef<Path>) -> Self {
        self.path = path.as_ref().to_path_buf();
        self
    }

    /// Overrides the data manager, returning the updated options.
    pub fn with_data_manager(mut self, kind: DataManagerKind) -> Self {
        self.data_manager = kind;
        self
    }

    /// Overrides the data-manager group name persisted in column descriptors.
    ///
    /// Most callers should use the default group matching the manager type.
    /// This exists for interoperability surfaces, such as casacore
    /// `TableLogSink`, that use a conventional group name (`"SSM"`).
    pub fn with_data_manager_group(mut self, group: impl Into<String>) -> Self {
        self.data_manager_group = Some(group.into());
        self
    }

    /// Uses casacore's zero-argument `StandardStMan` 32-row bucket sizing and
    /// binds the manager name from the group instead of `"SSM"`.
    pub fn with_casacore_default_ssm(mut self, enabled: bool) -> Self {
        self.casacore_default_ssm = enabled;
        self.preserve_ssm_group_name = enabled;
        self
    }

    /// Binds a `StandardStMan` manager's stored name from `data_manager_group`
    /// while preserving `casacore_default_ssm` bucket sizing independently.
    pub(crate) fn with_preserved_ssm_group_name(mut self, enabled: bool) -> Self {
        self.preserve_ssm_group_name = enabled;
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

    /// Returns whether casacore's zero-argument `StandardStMan` layout is used.
    pub fn casacore_default_ssm(&self) -> bool {
        self.casacore_default_ssm
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

    /// Returns the row at the given index.
    pub fn row(&self, index: usize) -> Result<&RecordValue, TableError> {
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
    RowOutOfBounds {
        /// Requested zero-based row index.
        row_index: usize,
        /// Number of rows available.
        row_count: usize,
    },
    /// A [`RowRange`] had `start > end` or `end > row_count`.
    #[error("row range [{start}, {end}) is invalid for table with {row_count} rows")]
    InvalidRowRange {
        /// First zero-based row in the requested range.
        start: usize,
        /// Exclusive zero-based end of the requested range.
        end: usize,
        /// Number of rows available.
        row_count: usize,
    },
    /// A [`RowRange`] stride was zero, which would produce an infinite loop.
    #[error("row stride must be >= 1, got {stride}")]
    InvalidRowStride {
        /// Requested step between successive selected rows.
        stride: usize,
    },
    /// The requested column was absent from the row (and no schema default applies).
    #[error("column \"{column}\" not found in row {row_index}")]
    ColumnNotFound {
        /// Zero-based row whose cell was absent.
        row_index: usize,
        /// Name of the requested column.
        column: String,
    },
    /// The column name is not declared in the attached [`TableSchema`].
    #[error("column \"{column}\" does not exist in schema")]
    SchemaColumnUnknown {
        /// Name of the column missing from the schema.
        column: String,
    },
    /// A record-specific operation was attempted on a non-record schema column.
    #[error("column \"{column}\" is not a record column in schema")]
    SchemaColumnNotRecord {
        /// Name of the column declared with a non-record type.
        column: String,
    },
    /// Prepared row access currently requires an attached schema.
    #[error("prepared row access requires an attached schema")]
    PreparedRowRequiresSchema,
    /// Prepared row access currently supports only scalar and array schema columns.
    #[error("prepared row access does not yet support record column \"{column}\"")]
    PreparedRowRecordColumnUnsupported {
        /// Name of the record column requested through prepared access.
        column: String,
    },
    /// A required column (non-optional per schema) was absent from a row.
    #[error("schema column \"{column}\" is missing in row {row_index}")]
    SchemaColumnMissing {
        /// Zero-based row being validated.
        row_index: usize,
        /// Name of the missing required column.
        column: String,
    },
    /// A row contained a column name that is not declared in the schema.
    #[error("row {row_index} contains unknown column \"{column}\" not present in schema")]
    RowContainsUnknownColumn {
        /// Zero-based row being validated.
        row_index: usize,
        /// Name absent from the current schema.
        column: String,
    },
    /// A cell held a value of the wrong type for its schema column.
    #[error(
        "row {row_index} column \"{column}\" has unexpected type {found:?}; expected {expected}"
    )]
    ColumnTypeMismatch {
        /// Zero-based row containing the incompatible cell.
        row_index: usize,
        /// Name of the incompatible column.
        column: String,
        /// Declared scalar, array, or record category.
        expected: &'static str,
        /// Actual value category in the cell.
        found: ValueKind,
    },
    /// A scalar or array had the wrong primitive element type.
    #[error(
        "row {row_index} column \"{column}\" has primitive type {found:?}; expected {expected:?}"
    )]
    PrimitiveTypeMismatch {
        /// Zero-based row containing the incompatible cell.
        row_index: usize,
        /// Name of the incompatible column.
        column: String,
        /// Declared scalar or array element type.
        expected: PrimitiveType,
        /// Actual scalar or array element type.
        found: PrimitiveType,
    },
    /// An array cell had a shape that did not match the schema's fixed shape.
    #[error("row {row_index} column \"{column}\" has shape {found:?}; expected {expected:?}")]
    ArrayShapeMismatch {
        /// Zero-based row containing the incompatible array.
        row_index: usize,
        /// Name of the fixed-shape column.
        column: String,
        /// Declared extents in array-axis order.
        expected: Vec<usize>,
        /// Actual extents in array-axis order.
        found: Vec<usize>,
    },
    /// An array cell had a number of dimensions that did not match the schema.
    #[error("row {row_index} column \"{column}\" has ndim {found}; expected {expected}")]
    ArrayNdimMismatch {
        /// Zero-based row containing the incompatible array.
        row_index: usize,
        /// Name of the column with declared rank.
        column: String,
        /// Declared number of array axes.
        expected: usize,
        /// Actual number of array axes.
        found: usize,
    },
    /// [`TableColumnMut::put`] or [`TableColumnMut::put_range`] received fewer values than rows.
    #[error("column write received too few values: expected {expected}, provided {provided}")]
    ColumnWriteTooFewValues {
        /// Number of selected rows to populate.
        expected: usize,
        /// Number of values received before input ended.
        provided: usize,
    },
    /// [`TableColumnMut::put`] or [`TableColumnMut::put_range`] received more values than rows.
    #[error("column write received too many values: expected {expected}")]
    ColumnWriteTooManyValues {
        /// Number of selected rows to populate.
        expected: usize,
    },
    /// Generated scalar run starts were malformed.
    #[error("invalid generated scalar runs: {message}")]
    InvalidGeneratedScalarRuns {
        /// Diagnostic for the invalid run start or extent.
        message: String,
    },
    /// Generated array run starts were malformed.
    #[error("invalid generated array runs: {message}")]
    InvalidGeneratedArrayRuns {
        /// Diagnostic for the invalid run start, extent, or cell data.
        message: String,
    },
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
    LockFailed {
        /// Path of the table whose lock was requested.
        path: String,
        /// Reason the lock could not be acquired.
        message: String,
    },
    /// A locking operation was attempted but the table has no lock state
    /// (opened without locking or created in-memory).
    #[error("table is not opened with locking; cannot {operation}")]
    NotLocked {
        /// Requested locking operation.
        operation: String,
    },
    /// A lock-file I/O error occurred.
    #[error("lock I/O error on \"{path}\": {message}")]
    LockIo {
        /// Path of the table associated with the lock file.
        path: String,
        /// Underlying I/O diagnostic.
        message: String,
    },
    /// A row index in a selection is out of range.
    #[error("row index {index} out of range (table has {count} rows)")]
    RowIndexOutOfRange {
        /// Zero-based parent row in the selection.
        index: usize,
        /// Number of available parent rows.
        count: usize,
    },
    /// A column name in a selection does not exist.
    #[error("unknown column \"{name}\" in selection")]
    UnknownColumn {
        /// Requested column name.
        name: String,
    },
    /// A reference table's parent could not be found.
    #[error("parent table not found at \"{path}\": {message}")]
    ParentTableNotFound {
        /// Resolved path of the referenced parent table.
        path: String,
        /// Underlying parent-open diagnostic.
        message: String,
    },
    /// A reference table requires the parent to have been saved to disk.
    #[error("parent table has no disk path; save it first")]
    ParentNotSaved,
    /// A sort key column contains non-scalar values.
    ///
    /// Only scalar columns can be used as sort keys, matching C++ casacore's
    /// runtime check in `BaseTable::sort`.
    #[error("column \"{column}\" is not a scalar column; only scalar columns can be sort keys")]
    SortKeyNotScalar {
        /// Name of the non-scalar sort column.
        column: String,
    },
    /// A sort key column contains values without a total order.
    ///
    /// Complex32 and Complex64 columns have no natural total ordering and
    /// cannot be used as sort keys.
    #[error("column \"{column}\" contains unsortable values (Complex types have no total order)")]
    SortKeyUnsortable {
        /// Name of the sort column without a supported ordering.
        column: String,
    },
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
    SchemaMismatch {
        /// Description of the incompatible schema entry.
        message: String,
    },
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
    ConstituentNotSaved {
        /// Zero-based position in the concatenation input list.
        index: usize,
    },
    /// No columns were supplied to [`crate::ColumnsIndex::new`].
    #[error("at least one column is required to build a ColumnsIndex")]
    IndexNoColumns,
    /// A column is non-scalar (array/record) and cannot be indexed.
    ///
    /// Only scalar columns can serve as index keys, matching C++ casacore's
    /// `ColumnsIndex` restriction.
    #[error("column \"{column}\" is not a scalar column; only scalar columns can be indexed")]
    IndexColumnNotScalar {
        /// Name of the non-scalar index column.
        column: String,
    },
    /// A column has an unsortable type (Complex32/Complex64) and cannot be indexed.
    ///
    /// Complex types have no total order and therefore cannot be used as index keys.
    #[error("column \"{column}\" contains unsortable values (Complex types have no total order)")]
    IndexColumnUnsortable {
        /// Name of the index column without a supported ordering.
        column: String,
    },
    /// [`crate::ColumnsIndex::lookup_unique`] found more than one matching row.
    #[error("index lookup_unique found {count} matching rows; expected at most 1")]
    IndexNotUnique {
        /// Number of matching rows for the supplied key.
        count: usize,
    },
    /// A TaQL query error occurred.
    ///
    /// Wraps [`crate::taql::TaqlError`] for convenience when using the
    /// [`Table::query`] or [`Table::execute_taql`] methods.
    #[error("TaQL error: {0}")]
    Taql(String),
    /// Slicer dimension vectors have different lengths.
    #[error("slicer dimension mismatch: start={start_ndim}, end={end_ndim}, stride={stride_ndim}")]
    SlicerDimensionMismatch {
        /// Number of supplied start coordinates.
        start_ndim: usize,
        /// Number of supplied end coordinates.
        end_ndim: usize,
        /// Number of supplied strides.
        stride_ndim: usize,
    },
    /// A slicer stride was zero on the given axis.
    #[error("slicer stride is zero on axis {axis}")]
    SlicerZeroStride {
        /// Zero-based array axis with a zero step.
        axis: usize,
    },
    /// A slicer range has start > end on the given axis.
    #[error("slicer range invalid on axis {axis}: start={start} > end={end}")]
    SlicerInvalidRange {
        /// Zero-based array axis with reversed bounds.
        axis: usize,
        /// First zero-based index requested on the axis.
        start: usize,
        /// Last zero-based index requested on the axis.
        end: usize,
    },
    /// A slicer index is out of bounds for the array shape.
    #[error("slicer out of bounds on axis {axis}: index {index} >= extent {extent}")]
    SlicerOutOfBounds {
        /// Zero-based array axis whose bound is invalid.
        axis: usize,
        /// Invalid zero-based index on the axis.
        index: usize,
        /// Number of elements along that axis.
        extent: usize,
    },
    /// A cell is not an array (slice operations require array cells).
    #[error("cell at row {row} column \"{column}\" is not an array")]
    CellNotArray {
        /// Zero-based row requested by the slice.
        row: usize,
        /// Name of the non-array column.
        column: String,
    },
    /// The number of data elements doesn't match the number of selected rows.
    #[error(
        "column slice length mismatch: {rows} rows selected but {data_len} data elements provided"
    )]
    ColumnSliceLengthMismatch {
        /// Number of selected rows.
        rows: usize,
        /// Number of input values supplied for the selection.
        data_len: usize,
    },
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
    /// The table's change count when its state last matched the disk (open,
    /// reload, flush or a full save to its own directory). Releasing a write
    /// lock writes the table only when the count has moved since, as
    /// casacore's `PlainTable::putFile` writes only what changed. It is
    /// atomic because `Table::save` writes through a shared borrow.
    flushed_generation: std::sync::atomic::AtomicU64,
    /// Whether this handle has written the table on disk, or started to,
    /// under the write lock it holds, and has not yet published that write
    /// in the sync data. Releasing the lock publishes the write when this is
    /// set, and only a successful publication clears it.
    ///
    /// It is set before a write starts, so a write that fails part way is
    /// still announced. A reload never clears it: a [`Table::resync`] makes
    /// the handle match the disk again, but other processes have not been
    /// told that the disk changed. It is atomic because `Table::flush` and
    /// `Table::save` write through a shared borrow.
    unpublished_write: std::sync::atomic::AtomicBool,
    /// `table.dat` as it was before the unpublished write started, read when
    /// `unpublished_write` is set and dropped once the write is published.
    /// The publication compares the file with it (#880): a write that left
    /// the file as it was keeps the row count published before.
    table_dat_before_write: std::sync::OnceLock<Vec<u8>>,
}

#[cfg(unix)]
impl LockState {
    /// Whether the table changed since its state last matched the disk.
    fn has_unflushed_changes(&self, table: &TableImpl) -> bool {
        table.has_dirty_changes().unwrap_or_else(|| {
            table.generation()
                != self
                    .flushed_generation
                    .load(std::sync::atomic::Ordering::Relaxed)
        })
    }

    /// Whether this lock period wrote the table on disk without yet
    /// publishing the write.
    fn has_unpublished_write(&self) -> bool {
        self.unpublished_write
            .load(std::sync::atomic::Ordering::Relaxed)
    }
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
/// * **Create** — [`Table::with_schema`] builds a new table from a schema.
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
    /// Explicit scientific-data provider used by TaQL `meas.*` functions.
    measures: Option<Arc<dyn casa_core::measures::MeasuresProvider>>,
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
    /// The logical heap retained by values in this table's lazy cell caches.
    /// Fully materialized row storage cannot be admitted to this cache budget.
    #[doc(hidden)]
    pub fn retained_cell_cache_bytes(&self) -> Option<usize> {
        self.inner.retained_cell_cache_bytes()
    }

    /// Return the checked logical heap bytes retained by a freshly opened,
    /// lazy table metadata capability.
    ///
    /// The projection covers schema and lookup storage, table and column
    /// keywords, data-manager descriptors, paths, virtual-column names, table
    /// info, and retained lock state. It returns `None` if row/cell caches,
    /// pending mutations, virtual bindings, or opaque external providers are
    /// present, so bounded readers cannot silently admit an unmodeled owner.
    /// Collection allocator headers and hash-table control bytes remain outside
    /// this logical payload projection.
    #[doc(hidden)]
    pub fn retained_read_metadata_bytes(&self) -> Option<usize> {
        self.retained_table_metadata_bytes(false)
    }

    /// Logical owned heap for a metadata writer, including explicit eager rows.
    /// Returns `None` for unmodeled lazy cell caches or pending column writes.
    /// Uses the same collection-capacity projection as the read-only ledger.
    #[doc(hidden)]
    pub fn retained_owned_metadata_bytes(&self) -> Option<usize> {
        self.retained_table_metadata_bytes(true)
    }

    fn retained_table_metadata_bytes(&self, eager: bool) -> Option<usize> {
        if !self.virtual_bindings.is_empty()
            || self.external_sync.is_some()
            || self.measures.is_some()
        {
            return None;
        }
        let mut bytes = if eager {
            self.inner.retained_owned_metadata_heap_bytes()?
        } else {
            self.inner.retained_lazy_metadata_heap_bytes()?
        }
        .checked_add(
            self.virtual_columns
                .capacity()
                .checked_mul(size_of::<String>())?,
        )?
        .checked_add(
            self.virtual_bindings
                .capacity()
                .checked_mul(size_of::<VirtualColumnBinding>())?,
        )?
        .checked_add(self.table_info.table_type.capacity())?
        .checked_add(self.table_info.sub_type.capacity())?
        .checked_add(
            self.table_info
                .readme
                .capacity()
                .checked_mul(size_of::<String>())?,
        )?
        .checked_add(
            self.dm_info
                .capacity()
                .checked_mul(size_of::<crate::storage::DataManagerInfo>())?,
        )?;
        if let Some(path) = &self.source_path {
            bytes = bytes.checked_add(crate::table_impl::path_heap_bytes(path))?;
        }
        for column in &self.virtual_columns {
            bytes = bytes.checked_add(column.capacity())?;
        }
        for line in &self.table_info.readme {
            bytes = bytes.checked_add(line.capacity())?;
        }
        for manager in &self.dm_info {
            bytes = bytes.checked_add(manager.dm_type.capacity())?.checked_add(
                manager
                    .columns
                    .capacity()
                    .checked_mul(size_of::<String>())?,
            )?;
            for column in &manager.columns {
                bytes = bytes.checked_add(column.capacity())?;
            }
        }
        #[cfg(unix)]
        if let Some(lock) = &self.lock_state {
            bytes = bytes
                .checked_add(crate::table_impl::path_heap_bytes(&lock.path))?
                .checked_add(lock.lock_file.retained_heap_bytes())?
                .checked_add(
                    lock.sync_data
                        .data_man_change_counters
                        .capacity()
                        .checked_mul(size_of::<u32>())?,
                )?;
        }
        Some(bytes)
    }

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
            measures: None,
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
            measures: None,
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
            measures: None,
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
        let undefined_cells = collect_undefined_cells_for_schema(&rows, &schema);
        let table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                rows,
                undefined_cells,
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
            measures: None,
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
            measures: None,
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
            measures: None,
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
            measures: None,
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
        let undefined_cells = collect_undefined_cells_for_schema(&rows, &schema);
        let table = Self {
            inner: TableImpl::with_rows_keywords_and_schema(
                rows,
                undefined_cells,
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
            measures: None,
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

    /// Attach the explicit scientific-data provider used by TaQL `meas.*`.
    pub fn set_measures_provider(
        &mut self,
        measures: Arc<dyn casa_core::measures::MeasuresProvider>,
    ) {
        self.measures = Some(measures);
    }

    /// Return the scientific-data provider attached to this table, if any.
    pub(crate) fn measures_provider(
        &self,
    ) -> Option<&Arc<dyn casa_core::measures::MeasuresProvider>> {
        self.measures.as_ref()
    }
}

// ── Drop ──────────────────────────────────────────────────────────────

impl Drop for Table {
    fn drop(&mut self) {
        #[cfg(unix)]
        {
            if self.kind != TableKind::Memory || self.is_named_memory_manager() {
                // A write lock that changed nothing writes and publishes
                // nothing, as in casacore.
                let has_changes_to_flush = self.lock_state.as_ref().is_some_and(|state| {
                    state.lock_file.has_lock(LockType::Write)
                        && state.has_unflushed_changes(&self.inner)
                });

                if has_changes_to_flush && let Err(error) = self.flush_locked_changes() {
                    tracing::warn!(%error, "could not flush changes while dropping locked table");
                }

                // Whatever part of this save, or of an earlier write under
                // the lock, reached the disk is published, describing the
                // table as persisted.
                if let Some(state) = self.lock_state.as_ref()
                    && state.lock_file.has_lock(LockType::Write)
                    && state.has_unpublished_write()
                {
                    let _ = publish_persisted_write(
                        &state.lock_file,
                        &state.path,
                        state.table_dat_before_write.get().map(Vec::as_slice),
                    );
                }
            }
        }

        if self.marked_for_delete
            && let Some(path) = &self.source_path
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}
