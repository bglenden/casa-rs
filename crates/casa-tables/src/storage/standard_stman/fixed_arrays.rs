// SPDX-License-Identifier: LGPL-3.0-or-later
//! Packed fixed-shape reads reuse bucket registration and one cell buffer.

use super::array_rows::ReadCell;
use super::*;
use crate::storage::stman_array_file::CanonicalArrayElement;
use crate::table::{SelectedArray2D, SelectedArray2DCells};
use casa_types::{Complex32, Complex64};

struct FixedCells<T> {
    shape: [usize; 2],
    rows: usize,
    values: Vec<T>,
    cell: Vec<T>,
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
        if !reader.read_fixed_shape_into(
            offset,
            &self.shape,
            &mut self.stored_shape,
            &mut self.cell,
        )? {
            return Ok(None);
        }
        let width = self.shape[0];
        for (channel, values) in self.cell.chunks_exact(width).enumerate() {
            let start = (channel * self.rows + slot) * width;
            self.values[start..start + width].copy_from_slice(values);
        }
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
    let total = count.checked_mul(rows.len()).ok_or_else(|| {
        StorageError::FormatMismatch("fixed selected array size overflows".into())
    })?;
    let mut cells = FixedCells {
        shape,
        rows: rows.len(),
        values: vec![T::default(); total],
        cell: vec![T::default(); count],
        stored_shape: Vec::with_capacity(2),
    };
    let Some(defined) = array_rows::read(path, blob, descs, column, rows, &mut cells)? else {
        return Ok(None);
    };
    if defined.iter().any(Option::is_none) {
        return Ok(None);
    }
    Ok(Some(SelectedArray2D::new(
        rows.len(),
        shape[0],
        shape[1],
        cells.values,
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
        casa_types::PrimitiveType::Bool => load!(bool, Bool),
        casa_types::PrimitiveType::Float32 => load!(f32, Float32),
        casa_types::PrimitiveType::Float64 => load!(f64, Float64),
        casa_types::PrimitiveType::Complex32 => load!(Complex32, Complex32),
        casa_types::PrimitiveType::Complex64 => load!(Complex64, Complex64),
        _ => Ok(None),
    }
}
