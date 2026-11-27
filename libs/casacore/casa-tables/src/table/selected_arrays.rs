// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// What a typed selected-row array read holds in memory besides its packed
/// output.
///
/// The typed selected readers ([`TableColumn::fill_array_cells_2d_channel_range_typed_uncached`],
/// [`TableColumn::fill_array_cells_1d_typed_uncached`] and their `load`
/// forms) pack a selection of rows, and for 2-D cells a channel range, into
/// flat layouts. What they hold beyond that depends on the column's data
/// manager ([`Table::selected_channel_read_footprint`],
/// [`Table::selected_cell_read_footprint`]): some managers stream the
/// selection, the others are read cell by cell. A cell-by-cell read never
/// escalates to a whole-column read, so [`WholeCells`](Self::WholeCells) is a
/// true bound a memory planner can charge per selected row with
/// [`staging_bytes_per_row`](Self::staging_bytes_per_row) and once per read
/// with [`staging_fixed_bytes`](Self::staging_fixed_bytes).
///
/// A cell-by-cell read stages only the selected rows' cells, however the
/// selection is strided: `StandardStMan` is read bucket by bucket, keeping the
/// selected rows of each, and `StManAipsIO` row by row. Two costs are outside
/// this footprint because the selection does not bound them: a reader's I/O
/// buffers, which depend on the stored layout (one `StandardStMan` bucket plus
/// 32 KiB of read-ahead; a tiled manager's recent tiles and the process-wide table cache, see
/// [`set_table_cache_budget_bytes`](crate::set_table_cache_budget_bytes)), and
/// data-manager metadata a reader parses for any read (bucket indices, tile
/// layouts), which depends on the stored table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectedReadFootprint {
    /// The data manager streams the selected rows (and channels) into the
    /// layout tile by tile, or the table is held in memory: nothing is held
    /// per selected row.
    Streamed,
    /// Each selected row's whole stored cell is read and held until the
    /// selection is packed.
    WholeCells,
}

impl SelectedReadFootprint {
    /// Bytes besides the packed output held per selected row whose stored
    /// cell has `stored_cell_bytes` bytes of values.
    ///
    /// A whole-cell read holds the cell's values and one owned cell slot per
    /// row (the slot's array header and the read's row bookkeeping).
    #[must_use]
    pub const fn staging_bytes_per_row(self, stored_cell_bytes: usize) -> Option<usize> {
        match self {
            Self::Streamed => Some(0),
            Self::WholeCells => stored_cell_bytes.checked_add(WHOLE_CELL_SLOT_BYTES),
        }
    }

    /// Bytes besides the packed output and the per-row staging held once per
    /// read of cells with `stored_cell_bytes` bytes of values: a whole-cell
    /// read decodes through a buffer of at most one cell, plus its file and
    /// decoding state.
    #[must_use]
    pub const fn staging_fixed_bytes(self, stored_cell_bytes: usize) -> Option<usize> {
        match self {
            Self::Streamed => Some(0),
            Self::WholeCells => stored_cell_bytes.checked_add(WHOLE_CELL_READ_FIXED_BYTES),
        }
    }
}

/// Owned-cell slot and row bookkeeping of one whole-cell read row: the
/// `Option<ArrayValue>` slot (array header, shape and strides inline) plus
/// the reader's per-row request and offset entries.
pub(super) const WHOLE_CELL_SLOT_BYTES: usize =
    size_of::<Option<casa_core::ArrayValue>>() + 4 * size_of::<usize>();

/// Per-read file and decoding state of a whole-cell read besides its
/// one-cell buffer.
pub(super) const WHOLE_CELL_READ_FIXED_BYTES: usize = 64 * 1024;

/// Typed selected 2-D array cells packed as `[channel][row][axis0]`.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectedArray2D<T> {
    pub(super) row_count: usize,
    pub(super) axis0_count: usize,
    pub(super) channel_count: usize,
    pub(super) values: Vec<T>,
}

impl<T> SelectedArray2D<T> {
    /// Create a packed selected-cell block.
    pub fn new(row_count: usize, axis0_count: usize, channel_count: usize, values: Vec<T>) -> Self {
        Self {
            row_count,
            axis0_count,
            channel_count,
            values,
        }
    }

    /// Number of selected rows represented by this block.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Size of axis 0 in every selected 2-D cell.
    pub fn axis0_count(&self) -> usize {
        self.axis0_count
    }

    /// Number of selected axis-1 channels represented by this block.
    pub fn channel_count(&self) -> usize {
        self.channel_count
    }

    /// Borrow packed values laid out as `[channel][row][axis0]`.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// Consume the block and return packed values laid out as `[channel][row][axis0]`.
    pub fn into_values(self) -> Vec<T> {
        self.values
    }
}

/// Typed selected 1-D array cells packed as `[row][axis0]`.
#[derive(Clone, Debug, PartialEq)]
pub struct SelectedArray1D<T> {
    pub(super) row_count: usize,
    pub(super) axis0_count: usize,
    pub(super) values: Vec<T>,
}

impl<T> SelectedArray1D<T> {
    /// Create a packed selected-cell block.
    pub fn new(row_count: usize, axis0_count: usize, values: Vec<T>) -> Self {
        Self {
            row_count,
            axis0_count,
            values,
        }
    }

    /// Number of selected rows represented by this block.
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Size of axis 0 in every selected 1-D cell.
    pub fn axis0_count(&self) -> usize {
        self.axis0_count
    }

    /// Borrow packed values laid out as `[row][axis0]`.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    /// Consume the block and return packed values laid out as `[row][axis0]`.
    pub fn into_values(self) -> Vec<T> {
        self.values
    }
}

/// Typed selected 1-D array cells for MS row-array primitive types.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectedArray1DCells {
    /// Boolean cells, packed as `[row][axis0]`.
    Bool(SelectedArray1D<bool>),
    /// 32-bit float cells, packed as `[row][axis0]`.
    Float32(SelectedArray1D<f32>),
    /// 64-bit float cells, packed as `[row][axis0]`.
    Float64(SelectedArray1D<f64>),
    /// 32-bit complex cells, packed as `[row][axis0]`.
    Complex32(SelectedArray1D<Complex32>),
    /// 64-bit complex cells, packed as `[row][axis0]`.
    Complex64(SelectedArray1D<Complex64>),
}

impl SelectedArray1DCells {
    /// Primitive type represented by this block.
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Bool(_) => PrimitiveType::Bool,
            Self::Float32(_) => PrimitiveType::Float32,
            Self::Float64(_) => PrimitiveType::Float64,
            Self::Complex32(_) => PrimitiveType::Complex32,
            Self::Complex64(_) => PrimitiveType::Complex64,
        }
    }

    /// Number of selected rows represented by this block.
    pub fn row_count(&self) -> usize {
        match self {
            Self::Bool(values) => values.row_count(),
            Self::Float32(values) => values.row_count(),
            Self::Float64(values) => values.row_count(),
            Self::Complex32(values) => values.row_count(),
            Self::Complex64(values) => values.row_count(),
        }
    }

    /// Size of axis 0 in every selected 1-D cell.
    pub fn axis0_count(&self) -> usize {
        match self {
            Self::Bool(values) => values.axis0_count(),
            Self::Float32(values) => values.axis0_count(),
            Self::Float64(values) => values.axis0_count(),
            Self::Complex32(values) => values.axis0_count(),
            Self::Complex64(values) => values.axis0_count(),
        }
    }
}

/// Caller-owned typed vectors for selected 1-D array-cell reads.
///
/// The storage manager resizes and fills the supplied vector directly. This
/// is the reusable-buffer counterpart to [`SelectedArray1DCells`].
#[derive(Debug)]
pub enum SelectedArray1DCellsMut<'a> {
    /// Boolean destination, packed as `[row][axis0]`.
    Bool(&'a mut Vec<bool>),
    /// 32-bit float destination, packed as `[row][axis0]`.
    Float32(&'a mut Vec<f32>),
    /// 64-bit float destination, packed as `[row][axis0]`.
    Float64(&'a mut Vec<f64>),
    /// 32-bit complex destination, packed as `[row][axis0]`.
    Complex32(&'a mut Vec<Complex32>),
    /// 64-bit complex destination, packed as `[row][axis0]`.
    Complex64(&'a mut Vec<Complex64>),
}

impl SelectedArray1DCellsMut<'_> {
    /// Primitive type accepted by this destination.
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Bool(_) => PrimitiveType::Bool,
            Self::Float32(_) => PrimitiveType::Float32,
            Self::Float64(_) => PrimitiveType::Float64,
            Self::Complex32(_) => PrimitiveType::Complex32,
            Self::Complex64(_) => PrimitiveType::Complex64,
        }
    }
}

/// Typed selected 2-D array cells for MS visibility-column primitive types.
#[derive(Clone, Debug, PartialEq)]
pub enum SelectedArray2DCells {
    /// Boolean cells, packed as `[channel][row][axis0]`.
    Bool(SelectedArray2D<bool>),
    /// 32-bit float cells, packed as `[channel][row][axis0]`.
    Float32(SelectedArray2D<f32>),
    /// 64-bit float cells, packed as `[channel][row][axis0]`.
    Float64(SelectedArray2D<f64>),
    /// 32-bit complex cells, packed as `[channel][row][axis0]`.
    Complex32(SelectedArray2D<Complex32>),
    /// 64-bit complex cells, packed as `[channel][row][axis0]`.
    Complex64(SelectedArray2D<Complex64>),
}

impl SelectedArray2DCells {
    /// Primitive type represented by this block.
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Bool(_) => PrimitiveType::Bool,
            Self::Float32(_) => PrimitiveType::Float32,
            Self::Float64(_) => PrimitiveType::Float64,
            Self::Complex32(_) => PrimitiveType::Complex32,
            Self::Complex64(_) => PrimitiveType::Complex64,
        }
    }

    /// Number of selected rows represented by this block.
    pub fn row_count(&self) -> usize {
        match self {
            Self::Bool(values) => values.row_count(),
            Self::Float32(values) => values.row_count(),
            Self::Float64(values) => values.row_count(),
            Self::Complex32(values) => values.row_count(),
            Self::Complex64(values) => values.row_count(),
        }
    }

    /// Size of axis 0 in every selected 2-D cell.
    pub fn axis0_count(&self) -> usize {
        match self {
            Self::Bool(values) => values.axis0_count(),
            Self::Float32(values) => values.axis0_count(),
            Self::Float64(values) => values.axis0_count(),
            Self::Complex32(values) => values.axis0_count(),
            Self::Complex64(values) => values.axis0_count(),
        }
    }

    /// Number of selected axis-1 channels represented by this block.
    pub fn channel_count(&self) -> usize {
        match self {
            Self::Bool(values) => values.channel_count(),
            Self::Float32(values) => values.channel_count(),
            Self::Float64(values) => values.channel_count(),
            Self::Complex32(values) => values.channel_count(),
            Self::Complex64(values) => values.channel_count(),
        }
    }
}

/// Caller-owned typed vectors for selected 2-D array-cell reads.
///
/// The storage manager resizes and fills the supplied vector directly. This
/// is the reusable-buffer counterpart to [`SelectedArray2DCells`].
#[derive(Debug)]
pub enum SelectedArray2DCellsMut<'a> {
    /// Boolean destination, packed as `[channel][row][axis0]`.
    Bool(&'a mut Vec<bool>),
    /// 32-bit float destination, packed as `[channel][row][axis0]`.
    Float32(&'a mut Vec<f32>),
    /// 64-bit float destination, packed as `[channel][row][axis0]`.
    Float64(&'a mut Vec<f64>),
    /// 32-bit complex destination, packed as `[channel][row][axis0]`.
    Complex32(&'a mut Vec<Complex32>),
    /// 64-bit complex destination, packed as `[channel][row][axis0]`.
    Complex64(&'a mut Vec<Complex64>),
    /// Boolean destination, packed as `[row][channel][axis0]`.
    RowChannelBool(&'a mut Vec<bool>),
    /// 32-bit float destination, packed as `[row][channel][axis0]`.
    RowChannelFloat32(&'a mut Vec<f32>),
    /// 32-bit complex destination, packed as `[row][channel][axis0]`.
    RowChannelComplex32(&'a mut Vec<Complex32>),
}

impl SelectedArray2DCellsMut<'_> {
    /// Primitive type accepted by this destination.
    pub fn primitive_type(&self) -> PrimitiveType {
        match self {
            Self::Bool(_) | Self::RowChannelBool(_) => PrimitiveType::Bool,
            Self::Float32(_) | Self::RowChannelFloat32(_) => PrimitiveType::Float32,
            Self::Float64(_) => PrimitiveType::Float64,
            Self::Complex32(_) | Self::RowChannelComplex32(_) => PrimitiveType::Complex32,
            Self::Complex64(_) => PrimitiveType::Complex64,
        }
    }
}

/// Shape of one packed selected 1-D array-cell read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedArray1DShape {
    /// Number of selected rows.
    pub row_count: usize,
    /// Size of axis 0 in every selected cell.
    pub axis0_count: usize,
}

/// Shape of one packed selected 2-D array-cell read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedArray2DShape {
    /// Number of selected rows.
    pub row_count: usize,
    /// Size of axis 0 in every selected cell.
    pub axis0_count: usize,
    /// Number of selected axis-1 channels.
    pub channel_count: usize,
}
