// SPDX-License-Identifier: LGPL-3.0-or-later
//! Which of a `TiledDataStMan`'s bound columns its tiles hold.
//!
//! A hypercolumn has data, coordinate and id columns, and all of them are
//! bound to its tiled manager. Only the data columns are stored in the
//! tiles. Each hypercube keeps its id values and coordinate vectors in its
//! values record. casacore lays a tile out over the data columns, sorted by
//! descending canonical pixel size, with Bool (bit-packed) last; the sort
//! is stable, so equal sizes keep their binding order.
//!
//! # C++ reference
//!
//! `TiledStMan::setup` (the sort), `TiledStMan::getLengthOffset` (the
//! layout), `TSMDataColumn::TSMDataColumn` (the pixel sizes) and
//! `TiledStMan::initCoordinates`, which defines every coordinate in every
//! hypercube's values record.

use std::cmp::Reverse;

use casa_types::Value;

use super::super::StorageError;
use super::super::data_type::CasacoreDataType;
use super::super::table_control::ColumnDescContents;
use super::{TiledStManHeader, TsmCubeInfo, tile_element_size};

/// The tile columns of one tiled manager.
pub(super) struct TileColumns {
    /// The manager's header with `col_data_types` holding only the tile
    /// columns, in tile order. Tile layout functions take this header.
    pub(super) header: TiledStManHeader,
    /// For each bound column, in binding order, its index among the tile
    /// columns, or `None` for a column kept in the hypercube values record.
    pub(super) tile_index: Vec<Option<usize>>,
}

/// Split a tiled manager's bound columns, in binding order, into its tile
/// columns and the columns its hypercube values records hold.
///
/// A bound column whose name is a field of a hypercube's values record is
/// an id or coordinate column; every other bound column is a data column.
///
/// A row maps to the last hypercube axis only. casacore spreads rows over
/// every axis after a data column's cell axes (`TiledDataStMan::getHypercube`),
/// so a hypercolumn with more than one such axis is refused, not misread.
pub(super) fn tile_columns(
    header: &TiledStManHeader,
    bound: &[&ColumnDescContents],
) -> Result<TileColumns, StorageError> {
    if bound.len() != header.col_data_types.len() {
        return Err(StorageError::FormatMismatch(format!(
            "tiled manager {} binds {} columns but its header lists {}",
            header.seq_nr,
            bound.len(),
            header.col_data_types.len()
        )));
    }
    let mut data: Vec<usize> = (0..bound.len())
        .filter(|&column| {
            !header
                .cubes
                .iter()
                .any(|cube| cube.values.get(&bound[column].col_name).is_some())
        })
        .collect();
    if let Some(&column) = data
        .iter()
        .find(|&&column| bound[column].nrdim > 0 && header.nrdim as i32 != bound[column].nrdim + 1)
    {
        return Err(StorageError::FormatMismatch(format!(
            "TiledDataStMan hypercolumn {} has {} axes for the {}-dimensional cells of '{}'; \
             more than one row axis is not supported",
            header.hypercolumn_name, header.nrdim, bound[column].nrdim, bound[column].col_name
        )));
    }
    data.sort_by_key(|&column| Reverse(tile_pixel_size(header.col_data_types[column])));

    let mut tile_index = vec![None; bound.len()];
    for (tile, &column) in data.iter().enumerate() {
        tile_index[column] = Some(tile);
    }
    let mut tile_header = header.clone();
    tile_header.col_data_types = data
        .iter()
        .map(|&column| header.col_data_types[column])
        .collect();
    Ok(TileColumns {
        header: tile_header,
        tile_index,
    })
}

/// The cell of `column`, a column the hypercube's values record holds, in
/// any row of that hypercube: its id value.
///
/// A coordinate column's cell depends on the row's position along the
/// coordinate's axis; that is not supported and is refused rather than
/// read as the whole coordinate vector.
pub(super) fn hypercube_cell(cube: &TsmCubeInfo, column: &str) -> Result<Value, StorageError> {
    match cube.values.get(column) {
        Some(value @ Value::Scalar(_)) => Ok(value.clone()),
        Some(_) => Err(StorageError::FormatMismatch(format!(
            "TiledDataStMan coordinate column '{column}' is not supported"
        ))),
        None => Err(StorageError::FormatMismatch(format!(
            "TiledDataStMan hypercube has no value for its id column '{column}'"
        ))),
    }
}

/// casacore's `tilePixelSize`: a pixel's canonical size, 0 for Bool.
fn tile_pixel_size(data_type: CasacoreDataType) -> usize {
    match data_type {
        CasacoreDataType::TpBool => 0,
        other => tile_element_size(other),
    }
}

#[cfg(test)]
mod tests {
    use casa_types::{RecordField, RecordValue, ScalarValue};

    use super::*;

    fn column(name: &str, data_type: CasacoreDataType, nrdim: i32) -> ColumnDescContents {
        ColumnDescContents {
            class_name: String::new(),
            col_name: name.to_string(),
            comment: String::new(),
            data_manager_type: "TiledDataStMan".to_string(),
            data_manager_group: "TiledData".to_string(),
            data_type,
            option: 0,
            nrdim,
            shape: Vec::new(),
            max_length: 0,
            keywords: RecordValue::default(),
            is_array: nrdim > 0,
            primitive_type: None,
        }
    }

    fn header(types: &[CasacoreDataType], values: RecordValue) -> TiledStManHeader {
        TiledStManHeader {
            big_endian: false,
            seq_nr: 0,
            nrrow: 2,
            col_data_types: types.to_vec(),
            hypercolumn_name: "TiledData".into(),
            max_cache_size: 0,
            nrdim: 3,
            files: Vec::new(),
            cubes: vec![TsmCubeInfo {
                values,
                extensible: false,
                cube_shape: vec![2, 3, 2],
                tile_shape: vec![2, 3, 1],
                file_seq_nr: 0,
                file_offset: 0,
            }],
        }
    }

    #[test]
    fn id_columns_leave_the_tiles_and_data_columns_sort_by_pixel_size() {
        let values = RecordValue::new(vec![RecordField::new(
            "id",
            Value::Scalar(ScalarValue::Int32(10)),
        )]);
        let bound = [
            column("flag", CasacoreDataType::TpBool, 2),
            column("id", CasacoreDataType::TpInt, 0),
            column("weight", CasacoreDataType::TpFloat, 2),
            column("data", CasacoreDataType::TpComplex, 2),
            column("count", CasacoreDataType::TpInt, 2),
        ];
        let types: Vec<_> = bound.iter().map(|desc| desc.data_type).collect();
        let bound: Vec<_> = bound.iter().collect();
        let columns = tile_columns(&header(&types, values), &bound).expect("tile columns");

        assert_eq!(
            columns.tile_index,
            [Some(3), None, Some(1), Some(0), Some(2)]
        );
        assert_eq!(
            columns.header.col_data_types,
            [
                CasacoreDataType::TpComplex,
                CasacoreDataType::TpFloat,
                CasacoreDataType::TpInt,
                CasacoreDataType::TpBool,
            ]
        );
    }

    #[test]
    fn a_binding_that_disagrees_with_the_header_is_refused() {
        let header = header(&[CasacoreDataType::TpFloat], RecordValue::default());
        let data = column("data", CasacoreDataType::TpFloat, 2);
        let id = column("id", CasacoreDataType::TpInt, 0);
        assert!(tile_columns(&header, &[&data, &id]).is_err());
    }

    #[test]
    fn a_hypercolumn_with_more_than_one_row_axis_is_refused() {
        // Three hypercube axes for one-dimensional cells: casacore spreads
        // rows over the last two.
        let header = header(&[CasacoreDataType::TpFloat], RecordValue::default());
        let data = column("data", CasacoreDataType::TpFloat, 1);
        assert!(tile_columns(&header, &[&data]).is_err());
        let data = column("data", CasacoreDataType::TpFloat, 2);
        assert!(tile_columns(&header, &[&data]).is_ok());
    }
}
