// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

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
/// use casa_tables::RowRange;
///
/// // Every row in a 10-row table:
/// let all = RowRange::new(0, 10);
///
/// // Every other row:
/// let evens = RowRange::with_stride(0, 10, 2);
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowRange {
    pub(super) start: usize,
    pub(super) end: usize,
    pub(super) stride: usize,
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

    pub(super) fn validate(&self, row_count: usize) -> Result<(), TableError> {
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

    pub(super) fn len(&self) -> usize {
        if self.start >= self.end || self.stride == 0 {
            0
        } else {
            1 + ((self.end - self.start - 1) / self.stride)
        }
    }

    pub(super) fn iter(&self) -> RowRangeIter {
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
/// use casa_tables::Slicer;
///
/// // Select rows 0..4 step 2, columns 1..3 step 1 from a 2-D array:
/// let s = Slicer::new(vec![0, 1], vec![4, 3], vec![2, 1]).unwrap();
/// assert_eq!(s.ndim(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Slicer {
    pub(super) start: Vec<usize>,
    pub(super) end: Vec<usize>,
    pub(super) stride: Vec<usize>,
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
/// Obtain a `ColumnCellIter` via [`Table::column_accessor`] and then
/// [`TableColumn::iter`] or [`TableColumn::iter_range`].
/// The iterator borrows the table's row data and does not allocate per cell.
/// For batch processing, see [`ColumnChunkIter`] via [`TableColumn::chunks`].
pub struct ColumnCellIter<'a> {
    pub(super) row_data: &'a [RecordValue],
    pub(super) column: String,
    pub(super) rows: RowRangeIter,
}

impl<'a> Iterator for ColumnCellIter<'a> {
    type Item = ColumnCellRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|row_index| ColumnCellRef {
            row_index,
            value: self.row_data[row_index].get(&self.column),
        })
    }
}

/// An iterator over cells in a record column, yielding one [`RecordColumnCell`] per row.
///
/// Obtain a `RecordColumnIter` via [`Table::column_accessor`] and then
/// [`TableColumn::record_iter`] or [`TableColumn::record_iter_range`]. When a [`TableSchema`] is attached and
/// the column is typed as [`crate::schema::ColumnType::Record`], rows whose
/// record cell is absent yield an empty [`RecordValue`] rather than an error.
pub struct RecordColumnIter<'a> {
    pub(super) row_data: &'a [RecordValue],
    pub(super) column: String,
    pub(super) rows: RowRangeIter,
    pub(super) default_missing: bool,
}

impl<'a> Iterator for RecordColumnIter<'a> {
    type Item = RecordColumnCell;

    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|row_index| {
            let value = match self.row_data[row_index].get(&self.column) {
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
/// Obtain a `ColumnChunkIter` via [`Table::column_accessor`] and then
/// [`TableColumn::chunks`]. This iterator is useful for processing columns in
/// memory-bounded passes without materializing the entire column at once.
pub struct ColumnChunkIter<'a> {
    pub(super) inner: ColumnCellIter<'a>,
    pub(super) chunk_size: usize,
}

impl<'a> Iterator for ColumnChunkIter<'a> {
    type Item = Vec<ColumnCellRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let chunk: Vec<_> = self.inner.by_ref().take(self.chunk_size).collect();
        if chunk.is_empty() { None } else { Some(chunk) }
    }
}

/// Read-only row accessor for a [`Table`].
///
/// This is the canonical row-oriented read surface for normal clients.
#[derive(Debug, Clone, Copy)]
pub struct TableRow<'a> {
    pub(crate) table: &'a Table,
}

/// Mutable row accessor for a [`Table`].
///
/// Use this for row-oriented updates instead of reaching directly into storage
/// internals.
#[derive(Debug)]
pub struct TableRowMut<'a> {
    pub(crate) table: &'a mut Table,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PreparedRowSlotKind {
    Scalar,
    Array,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedRowSlot {
    pub(crate) column: String,
    pub(crate) kind: PreparedRowSlotKind,
    pub(crate) schema_column: ColumnSchema,
}

/// Read-only reusable row buffer for selected scalar/array columns.
///
/// Prepared row access compiles a stable slot order once, then reloads this
/// same record-shaped buffer for each row. Callers can cache slot indices with
/// [`PreparedTableRow::column_index`] and read the same offset repeatedly
/// without per-row record allocation.
#[derive(Debug, Clone)]
pub struct PreparedTableRow<'a> {
    pub(crate) table: &'a Table,
    pub(crate) slots: Vec<PreparedRowSlot>,
    pub(crate) column_indices: HashMap<String, usize>,
    pub(crate) cached_rows: Option<Vec<RecordValue>>,
    pub(crate) row_index: Option<usize>,
    pub(crate) row: RecordValue,
    pub(crate) row_materialized: bool,
}

/// Mutable reusable row buffer for selected scalar/array columns.
///
/// The buffer is reused across rows and flushes back through the table
/// accessor layer, preserving lazy sparse writes rather than exposing
/// storage-manager internals directly.
#[derive(Debug)]
pub struct PreparedTableRowMut<'a> {
    pub(crate) table: &'a mut Table,
    pub(crate) slots: Vec<PreparedRowSlot>,
    pub(crate) column_indices: HashMap<String, usize>,
    pub(crate) row_index: Option<usize>,
    pub(crate) row: RecordValue,
    pub(crate) row_materialized: bool,
    pub(crate) dirty: bool,
}

/// Schema-bound high-throughput row appender.
///
/// Construction borrows the table mutably, so its schema cannot change while
/// the appender exists. Each future row receives only the value checks that
/// cannot be proven until the row is supplied.
#[derive(Debug)]
pub struct PreparedRowAppender<'a> {
    pub(crate) table: &'a mut Table,
    pub(crate) schema: TableSchema,
}

/// Validated persistence scope for a table write.
///
/// Each save operation validates all knowable bindings, columns, rows, and
/// layout constraints before beginning I/O. An I/O error leaves the in-memory
/// table and pending sparse updates intact, so callers may fix the destination
/// and retry through the same plan.
///
/// The plan holds an exclusive table borrow for its lifetime. Consequently,
/// Rust prevents schema changes between validation and persistence rather than
/// relying on a runtime invalidation convention.
///
/// # Safe fast path
///
/// ```no_run
/// use casa_tables::{ColumnSchema, Table, TableOptions, TableSchema};
/// use casa_core::{PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
///
/// let schema = TableSchema::new(vec![
///     ColumnSchema::scalar("id", PrimitiveType::Int32),
/// ])?;
/// let mut table = Table::with_schema(schema);
/// table.prepare_row_appender()?.append(RecordValue::new(vec![
///     RecordField::new("id", Value::Scalar(ScalarValue::Int32(1))),
/// ]))?;
/// {
///     let mut row = table.row_accessor_mut().prepare(&["id"])?;
///     row.seek(0)?;
///     row.set_value_at(0, Value::Scalar(ScalarValue::Int32(2)))?;
///     row.flush()?;
/// }
/// table.prepare_write().save(TableOptions::new("example.table"))?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct TableWritePlan<'a> {
    pub(crate) table: &'a mut Table,
    pub(crate) write_lock: Option<&'a crate::TableWriteLock>,
}

/// Read-only column accessor for a [`Table`].
///
/// This follows the same public-accessor direction as casacore's
/// `ROTableColumn`, `ScalarColumn<T>`, and `ArrayColumn<T>`.
#[derive(Debug, Clone)]
pub struct TableColumn<'a> {
    pub(crate) table: &'a Table,
    pub(crate) column: String,
}

/// Mutable column accessor for a [`Table`].
///
/// Use this for column-oriented updates while keeping storage-manager details
/// internal to `casa-tables`.
#[derive(Debug)]
pub struct TableColumnMut<'a> {
    pub(crate) table: &'a mut Table,
    pub(crate) column: String,
}

/// Required scalar values loaded as typed column vectors.
///
/// Unlike [`TableColumn::scalar_cells_owned`], this form is for callers that
/// know every row in the requested column must be present and want to avoid a
/// per-row `ScalarValue` wrapper in hot column-oriented paths.
#[derive(Debug, Clone, PartialEq)]
pub enum RequiredScalarColumnValues {
    /// Boolean scalar column values.
    Bool(Vec<bool>),
    /// 32-bit signed integer scalar column values.
    Int32(Vec<i32>),
    /// 32-bit floating-point scalar column values.
    Float32(Vec<f32>),
    /// 64-bit floating-point scalar column values.
    Float64(Vec<f64>),
}

impl RequiredScalarColumnValues {
    /// Number of rows represented by this column vector.
    pub fn len(&self) -> usize {
        match self {
            Self::Bool(values) => values.len(),
            Self::Int32(values) => values.len(),
            Self::Float32(values) => values.len(),
            Self::Float64(values) => values.len(),
        }
    }

    /// Returns true when this column vector is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Caller-owned typed vector for one required scalar-column read.
#[derive(Debug)]
pub enum RequiredScalarColumnValuesMut<'a> {
    /// Boolean scalar destination.
    Bool(&'a mut Vec<bool>),
    /// 32-bit signed integer scalar destination.
    Int32(&'a mut Vec<i32>),
    /// 32-bit floating-point scalar destination.
    Float32(&'a mut Vec<f32>),
    /// 64-bit floating-point scalar destination.
    Float64(&'a mut Vec<f64>),
}

/// Named caller-owned destination for a required scalar-column batch read.
#[derive(Debug)]
pub struct RequiredScalarColumnDestination<'a> {
    pub(super) column: &'a str,
    pub(super) values: RequiredScalarColumnValuesMut<'a>,
}

impl<'a> RequiredScalarColumnDestination<'a> {
    /// Bind one column name to its typed reusable vector.
    pub const fn new(column: &'a str, values: RequiredScalarColumnValuesMut<'a>) -> Self {
        Self { column, values }
    }

    /// Column name bound to this destination.
    pub const fn column(&self) -> &str {
        self.column
    }

    pub(crate) fn values_mut(&mut self) -> &mut RequiredScalarColumnValuesMut<'a> {
        &mut self.values
    }
}

/// Read-only cell accessor for a [`Table`].
///
/// Cell accessors tie a row index and column name together so callers can
/// choose row-, column-, or direct-cell-oriented code without touching
/// storage-manager internals.
#[derive(Debug, Clone)]
pub struct TableCell<'a> {
    pub(crate) table: &'a Table,
    pub(crate) row_index: usize,
    pub(crate) column: String,
}

/// Mutable cell accessor for a [`Table`].
#[derive(Debug)]
pub struct TableCellMut<'a> {
    pub(crate) table: &'a mut Table,
    pub(crate) row_index: usize,
    pub(crate) column: String,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct RowRangeIter {
    pub(super) next: usize,
    pub(super) end: usize,
    pub(super) stride: usize,
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
