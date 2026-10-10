// SPDX-License-Identifier: LGPL-3.0-or-later

//! Typed selected-row reads served cell by cell, packed into the typed
//! layouts.
//!
//! `TiledShapeStMan` streams selected rows and channels straight into the
//! 2-D packed layouts, and the tiled and incremental managers stream 1-D
//! rows. Every other manager casacore writes (`StManAipsIO`, `StandardStMan`
//! and the other tiled managers for 2-D cells) is read cell by cell through
//! the general selected-row reader and packed here, so a MeasurementSet reads
//! the same whatever manager CASA chose for a column.
//!
//! A read served here holds every selected row's whole stored cell until it
//! is packed ([`SelectedReadFootprint::WholeCells`]); it never reads a whole
//! column, so that footprint is a true bound for a memory planner. The
//! classifiers [`channel_read_footprint`] and [`cell_read_footprint`] name
//! the managers the typed readers stream, and the readers' dispatch uses the
//! same lists.

use std::path::Path;

use casa_types::{ArrayValue, Complex32, Complex64};
use ndarray::ArrayD;

use super::{CompositeStorage, StorageError, table_control::TableDatContents};
use crate::table::{
    SelectedArray1D, SelectedArray1DCells, SelectedArray1DCellsMut, SelectedArray1DShape,
    SelectedArray2D, SelectedArray2DCells, SelectedArray2DCellsMut, SelectedArray2DShape,
    SelectedReadFootprint,
};

/// What the typed selected 2-D channel-range readers hold for a column
/// stored by `data_manager`: only `TiledShapeStMan` streams the selected
/// channels.
pub(crate) fn channel_read_footprint(data_manager: &str) -> SelectedReadFootprint {
    match data_manager {
        "TiledShapeStMan" => SelectedReadFootprint::Streamed,
        _ => SelectedReadFootprint::WholeCells,
    }
}

/// What the typed selected 1-D readers hold for a column stored by
/// `data_manager`: the tiled column managers and `IncrementalStMan` stream
/// the selected rows.
pub(crate) fn cell_read_footprint(data_manager: &str) -> SelectedReadFootprint {
    match data_manager {
        "IncrementalStMan" | "TiledColumnStMan" | "TiledShapeStMan" => {
            SelectedReadFootprint::Streamed
        }
        _ => SelectedReadFootprint::WholeCells,
    }
}

/// One typed selected read that its data manager serves cell by cell.
pub(super) struct WholeCellRead<'a> {
    storage: &'a CompositeStorage,
    table_path: &'a Path,
    table_dat: &'a TableDatContents,
    column: &'a str,
    selected_rows: &'a [usize],
}

impl CompositeStorage {
    /// A typed selected read of `column` that its data manager serves cell by
    /// cell.
    pub(super) fn whole_cell_read<'a>(
        &'a self,
        table_path: &'a Path,
        table_dat: &'a TableDatContents,
        column: &'a str,
        selected_rows: &'a [usize],
    ) -> WholeCellRead<'a> {
        WholeCellRead {
            storage: self,
            table_path,
            table_dat,
            column,
            selected_rows,
        }
    }
}

impl WholeCellRead<'_> {
    /// The whole stored cells of the selected rows, read row by row.
    ///
    /// A layout its data manager can only read as a whole column is refused:
    /// a typed selected read holds at most the selected rows' cells.
    fn cells(&self) -> Result<Vec<Option<ArrayValue>>, StorageError> {
        if self.selected_rows.is_empty() {
            return Ok(Vec::new());
        }
        self.storage
            .load_selected_array_cells(
                self.table_path,
                self.table_dat,
                self.column,
                self.selected_rows,
            )?
            .ok_or_else(|| {
                StorageError::FormatMismatch(format!(
                    "typed selected reads of column '{}' need its cells read row by row, but \
                     its data manager can read this layout only as a whole column",
                    self.column
                ))
            })
    }

    /// Load a 2-D channel range in the stored element type, packed
    /// `[channel][row][axis0]`; `None` when a selected cell is undefined.
    pub(super) fn load_2d(
        &self,
        channel_start: usize,
        channel_count: usize,
    ) -> Result<Option<SelectedArray2DCells>, StorageError> {
        load_2d(self.column, &self.cells()?, channel_start, channel_count)
    }

    /// Fill a 2-D channel-range destination; `None` when a selected cell is
    /// undefined.
    pub(super) fn fill_2d(
        &self,
        channel_start: usize,
        channel_count: usize,
        destination: SelectedArray2DCellsMut<'_>,
    ) -> Result<Option<SelectedArray2DShape>, StorageError> {
        fill_2d(
            self.column,
            &self.cells()?,
            channel_start,
            channel_count,
            destination,
        )
    }

    /// Load 1-D cells in the stored element type.
    pub(super) fn load_1d(&self) -> Result<SelectedArray1DCells, StorageError> {
        load_1d(self.column, &self.cells()?)
    }

    /// Fill a 1-D destination.
    pub(super) fn fill_1d(
        &self,
        destination: SelectedArray1DCellsMut<'_>,
    ) -> Result<SelectedArray1DShape, StorageError> {
        fill_1d(self.column, &self.cells()?, destination)
    }
}

/// An element type the packed layouts carry.
trait Element: Copy {
    fn cells(value: &ArrayValue) -> Option<&ArrayD<Self>>;
}

macro_rules! element {
    ($type:ty, $variant:ident) => {
        impl Element for $type {
            fn cells(value: &ArrayValue) -> Option<&ArrayD<Self>> {
                match value {
                    ArrayValue::$variant(values) => Some(values),
                    _ => None,
                }
            }
        }
    };
}

element!(bool, Bool);
element!(f32, Float32);
element!(f64, Float64);
element!(Complex32, Complex32);
element!(Complex64, Complex64);

/// How a 2-D `[axis0, channel]` cell block is packed.
#[derive(Clone, Copy)]
enum Layout2D {
    /// `[channel][row][axis0]`.
    ChannelRow,
    /// `[row][channel][axis0]`.
    RowChannel,
}

fn typed<'a, T: Element>(
    column: &str,
    value: &'a ArrayValue,
) -> Result<&'a ArrayD<T>, StorageError> {
    T::cells(value).ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "selected column '{column}' does not have the requested element type"
        ))
    })
}

/// Pack 1-D cells as `[row][axis0]`; `None` when a selected cell is undefined.
fn pack_1d<T: Element>(
    column: &str,
    cells: &[Option<ArrayValue>],
    values: &mut Vec<T>,
) -> Result<Option<SelectedArray1DShape>, StorageError> {
    values.clear();
    let mut axis0_count = None;
    for cell in cells {
        let Some(cell) = cell else {
            return Ok(None);
        };
        let cell = typed::<T>(column, cell)?;
        if cell.ndim() != 1 || *axis0_count.get_or_insert(cell.len()) != cell.len() {
            return Err(StorageError::FormatMismatch(format!(
                "selected 1-D column '{column}' has cells of differing shape"
            )));
        }
        values.extend(cell.iter().copied());
    }
    Ok(Some(SelectedArray1DShape {
        row_count: cells.len(),
        axis0_count: axis0_count.unwrap_or(0),
    }))
}

/// Pack channels `channel_start..channel_start + channel_count` of 2-D
/// `[axis0, channel]` cells; `None` when a selected cell is undefined.
fn pack_2d<T: Element>(
    column: &str,
    cells: &[Option<ArrayValue>],
    channel_start: usize,
    channel_count: usize,
    layout: Layout2D,
    values: &mut Vec<T>,
) -> Result<Option<SelectedArray2DShape>, StorageError> {
    values.clear();
    let mut typed_cells = Vec::with_capacity(cells.len());
    let mut axis0_count = None;
    for cell in cells {
        let Some(cell) = cell else {
            return Ok(None);
        };
        let cell = typed::<T>(column, cell)?;
        let shape = cell.shape();
        if shape.len() != 2
            || *axis0_count.get_or_insert(shape[0]) != shape[0]
            || channel_start + channel_count > shape[1]
        {
            return Err(StorageError::FormatMismatch(format!(
                "selected 2-D column '{column}' cells do not cover the requested channels"
            )));
        }
        typed_cells.push(cell);
    }
    let axis0_count = axis0_count.unwrap_or(0);
    let channels = channel_start..channel_start + channel_count;
    values.reserve(cells.len() * channel_count * axis0_count);
    match layout {
        Layout2D::ChannelRow => {
            for channel in channels {
                for cell in &typed_cells {
                    values.extend((0..axis0_count).map(|axis0| cell[[axis0, channel]]));
                }
            }
        }
        Layout2D::RowChannel => {
            for cell in &typed_cells {
                for channel in channels.clone() {
                    values.extend((0..axis0_count).map(|axis0| cell[[axis0, channel]]));
                }
            }
        }
    }
    Ok(Some(SelectedArray2DShape {
        row_count: cells.len(),
        axis0_count,
        channel_count,
    }))
}

/// Fill a 1-D destination from whole-cell reads.
fn fill_1d(
    column: &str,
    cells: &[Option<ArrayValue>],
    destination: SelectedArray1DCellsMut<'_>,
) -> Result<SelectedArray1DShape, StorageError> {
    match destination {
        SelectedArray1DCellsMut::Bool(values) => pack_1d(column, cells, values),
        SelectedArray1DCellsMut::Float32(values) => pack_1d(column, cells, values),
        SelectedArray1DCellsMut::Float64(values) => pack_1d(column, cells, values),
        SelectedArray1DCellsMut::Complex32(values) => pack_1d(column, cells, values),
        SelectedArray1DCellsMut::Complex64(values) => pack_1d(column, cells, values),
    }?
    .ok_or_else(|| {
        StorageError::FormatMismatch(format!(
            "selected 1-D column '{column}' has undefined cells"
        ))
    })
}

/// Load 1-D cells in the element type they are stored with.
fn load_1d(
    column: &str,
    cells: &[Option<ArrayValue>],
) -> Result<SelectedArray1DCells, StorageError> {
    macro_rules! load {
        ($variant:ident) => {{
            let mut values = Vec::new();
            let shape = fill_1d(
                column,
                cells,
                SelectedArray1DCellsMut::$variant(&mut values),
            )?;
            SelectedArray1DCells::$variant(SelectedArray1D::new(
                shape.row_count,
                shape.axis0_count,
                values,
            ))
        }};
    }
    Ok(match cells.iter().flatten().next() {
        Some(ArrayValue::Bool(_)) => load!(Bool),
        Some(ArrayValue::Float32(_)) => load!(Float32),
        Some(ArrayValue::Complex32(_)) => load!(Complex32),
        Some(ArrayValue::Complex64(_)) => load!(Complex64),
        _ => load!(Float64),
    })
}

/// Fill a 2-D channel-range destination from whole-cell reads; `None` when
/// a selected cell is undefined.
fn fill_2d(
    column: &str,
    cells: &[Option<ArrayValue>],
    channel_start: usize,
    channel_count: usize,
    destination: SelectedArray2DCellsMut<'_>,
) -> Result<Option<SelectedArray2DShape>, StorageError> {
    use Layout2D::{ChannelRow, RowChannel};
    let (start, count) = (channel_start, channel_count);
    match destination {
        SelectedArray2DCellsMut::Bool(values) => {
            pack_2d(column, cells, start, count, ChannelRow, values)
        }
        SelectedArray2DCellsMut::Float32(values) => {
            pack_2d(column, cells, start, count, ChannelRow, values)
        }
        SelectedArray2DCellsMut::Float64(values) => {
            pack_2d(column, cells, start, count, ChannelRow, values)
        }
        SelectedArray2DCellsMut::Complex32(values) => {
            pack_2d(column, cells, start, count, ChannelRow, values)
        }
        SelectedArray2DCellsMut::Complex64(values) => {
            pack_2d(column, cells, start, count, ChannelRow, values)
        }
        SelectedArray2DCellsMut::RowChannelBool(values) => {
            pack_2d(column, cells, start, count, RowChannel, values)
        }
        SelectedArray2DCellsMut::RowChannelFloat32(values) => {
            pack_2d(column, cells, start, count, RowChannel, values)
        }
        SelectedArray2DCellsMut::RowChannelComplex32(values) => {
            pack_2d(column, cells, start, count, RowChannel, values)
        }
    }
}

/// Load a 2-D channel range in the element type it is stored with, packed
/// `[channel][row][axis0]`; `None` when a selected cell is undefined.
fn load_2d(
    column: &str,
    cells: &[Option<ArrayValue>],
    channel_start: usize,
    channel_count: usize,
) -> Result<Option<SelectedArray2DCells>, StorageError> {
    macro_rules! load {
        ($variant:ident) => {{
            let mut values = Vec::new();
            fill_2d(
                column,
                cells,
                channel_start,
                channel_count,
                SelectedArray2DCellsMut::$variant(&mut values),
            )?
            .map(|shape| {
                SelectedArray2DCells::$variant(SelectedArray2D::new(
                    shape.row_count,
                    shape.axis0_count,
                    shape.channel_count,
                    values,
                ))
            })
        }};
    }
    Ok(match cells.iter().flatten().next() {
        Some(ArrayValue::Bool(_)) => load!(Bool),
        Some(ArrayValue::Float32(_)) => load!(Float32),
        Some(ArrayValue::Float64(_)) => load!(Float64),
        Some(ArrayValue::Complex64(_)) => load!(Complex64),
        _ => load!(Complex32),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use ndarray::{IxDyn, ShapeBuilder};

    /// A `[axis0, channel]` cell of 2 x 3 values `100 row + 10 channel + axis0`,
    /// stored Fortran order as casacore stores it.
    fn cell(row: usize) -> Option<ArrayValue> {
        let mut cell = ArrayD::from_elem(IxDyn(&[2, 3]).f(), 0.0_f32);
        for axis0 in 0..2 {
            for channel in 0..3 {
                cell[[axis0, channel]] = (100 * row + 10 * channel + axis0) as f32;
            }
        }
        Some(ArrayValue::Float32(cell))
    }

    #[test]
    fn two_dimensional_cells_pack_a_channel_range_in_both_layouts() {
        let cells = [cell(0), cell(1)];
        let mut row_channel = Vec::new();
        let shape = fill_2d(
            "DATA",
            &cells,
            1,
            2,
            SelectedArray2DCellsMut::RowChannelFloat32(&mut row_channel),
        )
        .expect("pack")
        .expect("defined");
        assert_eq!(
            (shape.row_count, shape.axis0_count, shape.channel_count),
            (2, 2, 2)
        );
        assert_eq!(
            row_channel,
            [10.0, 11.0, 20.0, 21.0, 110.0, 111.0, 120.0, 121.0]
        );
        let mut channel_row = Vec::new();
        fill_2d(
            "DATA",
            &cells,
            1,
            2,
            SelectedArray2DCellsMut::Float32(&mut channel_row),
        )
        .expect("pack")
        .expect("defined");
        assert_eq!(
            channel_row,
            [10.0, 11.0, 110.0, 111.0, 20.0, 21.0, 120.0, 121.0]
        );
    }

    #[test]
    fn undefined_and_mismatched_cells_are_reported() {
        let mut values = Vec::new();
        assert!(
            fill_2d(
                "FLAG",
                &[None],
                0,
                1,
                SelectedArray2DCellsMut::Bool(&mut values)
            )
            .expect("undefined is not an error")
            .is_none()
        );
        let uvw = Some(ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[3]), 1.5)));
        let mut floats = Vec::new();
        assert!(
            fill_1d(
                "UVW",
                std::slice::from_ref(&uvw),
                SelectedArray1DCellsMut::Float32(&mut floats)
            )
            .is_err()
        );
        let SelectedArray1DCells::Float64(loaded) =
            load_1d("UVW", &[uvw.clone(), uvw]).expect("load")
        else {
            panic!("UVW loads as f64")
        };
        assert_eq!(loaded.values(), &[1.5; 6]);
    }
}
