// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shape-validated whole cells grow one flat buffer in source row order.

use super::array_rows::ReadCell;
use super::*;
use crate::storage::stman_array_file::CanonicalArrayElement;
use crate::table::{SelectedArray2D, SelectedArray2DCells};
use casa_core::{Complex32, Complex64};

struct FixedCells<T> {
    shape: [usize; 2],
    values: Vec<T>,
    order: Vec<usize>,
    ordered: bool,
    stored_shape: Vec<usize>,
}

impl<T: CanonicalArrayElement> ReadCell for FixedCells<T> {
    type Cell = ();
    fn indirect(
        &mut self,
        reader: Option<&mut StManArrayFileReader>,
        offset: i64,
        _data_type: CasacoreDataType,
        desc: &ColumnDescContents,
        (row, slot): (usize, usize),
    ) -> Result<Option<()>, StorageError> {
        let Some(reader) = reader else {
            return if offset == 0 {
                Ok(None)
            } else {
                Err(StorageError::FormatMismatch(format!(
                    "SSM column {} row {row} has no array file",
                    desc.col_name
                )))
            };
        };
        if !reader.append_fixed_shape(
            offset,
            &self.shape,
            &mut self.stored_shape,
            &mut self.values,
        )? {
            return Ok(None);
        }
        self.order.try_reserve(1).map_err(|error| {
            StorageError::FormatMismatch(format!("cannot allocate fixed array row order: {error}"))
        })?;
        self.ordered &= slot == self.order.len();
        self.order.push(slot);
        Ok(Some(()))
    }
    fn direct(
        &mut self,
        _raw: &ColumnRawData,
        _desc: &ColumnDescContents,
        _position: (usize, usize),
        _rows: usize,
    ) -> Result<Option<()>, StorageError> {
        Ok(None)
    }
}

fn read<T: CanonicalArrayElement>(
    path: &Path,
    blob: &[u8],
    descs: &[&ColumnDescContents],
    column: usize,
    rows: &[usize],
    shape: [usize; 2],
) -> Result<Option<SelectedArray2D<T>>, StorageError> {
    let count = shape[0]
        .checked_mul(shape[1])
        .filter(|&count| count > 0)
        .ok_or_else(|| {
            StorageError::FormatMismatch("fixed array shape is empty or overflows".into())
        })?;
    let total = count
        .checked_mul(rows.len())
        .filter(|&total| {
            total
                .checked_mul(std::mem::size_of::<T>())
                .is_some_and(|bytes| bytes <= isize::MAX as usize)
        })
        .ok_or_else(|| {
            StorageError::FormatMismatch("fixed selected array size overflows".into())
        })?;
    let mut cells = FixedCells {
        shape,
        values: Vec::new(),
        order: Vec::new(),
        ordered: true,
        stored_shape: Vec::with_capacity(2),
    };
    let Some(defined) = array_rows::read(path, blob, descs, column, rows, &mut cells)? else {
        return Ok(None);
    };
    if defined.iter().any(Option::is_none) {
        return Ok(None);
    }
    let values = if shape[1] == 1 && cells.ordered {
        cells.values
    } else {
        // Allocate the transposed result only after all stored shapes match.
        let mut values = Vec::new();
        values.try_reserve_exact(total).map_err(|error| {
            StorageError::FormatMismatch(format!("cannot allocate packed fixed arrays: {error}"))
        })?;
        values.resize(total, T::default());
        for (source_row, slot) in cells.order.into_iter().enumerate() {
            for channel in 0..shape[1] {
                let source = (source_row * shape[1] + channel) * shape[0];
                let target = (channel * rows.len() + slot) * shape[0];
                values[target..target + shape[0]]
                    .copy_from_slice(&cells.values[source..source + shape[0]]);
            }
        }
        values
    };
    Ok(Some(SelectedArray2D::new(
        rows.len(),
        shape[0],
        shape[1],
        values,
    )))
}

/// Whole indirect cells, all of exactly `shape`, packed channel/row/axis0.
/// Undefined or differently-shaped cells return None; all I/O errors propagate.
pub(crate) fn read_fixed_2d_cells(
    path: &Path,
    blob: &[u8],
    descs: &[&ColumnDescContents],
    column: usize,
    rows: &[usize],
    shape: [usize; 2],
) -> Result<Option<SelectedArray2DCells>, StorageError> {
    let Some(desc) = descs.get(column) else {
        return Err(StorageError::FormatMismatch(
            "SSM fixed column index is out of range".into(),
        ));
    };
    if !is_ssm_array_file_indirect(desc) {
        return Ok(None);
    }
    macro_rules! load {
        ($type:ty, $variant:ident) => {
            read::<$type>(path, blob, descs, column, rows, shape)
                .map(|cells| cells.map(SelectedArray2DCells::$variant))
        };
    }
    match desc.require_primitive_type()? {
        casa_core::PrimitiveType::Bool => load!(bool, Bool),
        casa_core::PrimitiveType::Float32 => load!(f32, Float32),
        casa_core::PrimitiveType::Float64 => load!(f64, Float64),
        casa_core::PrimitiveType::Complex32 => load!(Complex32, Complex32),
        casa_core::PrimitiveType::Complex64 => load!(Complex64, Complex64),
        _ => Ok(None),
    }
}
