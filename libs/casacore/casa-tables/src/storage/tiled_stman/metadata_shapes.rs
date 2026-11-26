// SPDX-License-Identifier: LGPL-3.0-or-later
//! Array shapes, definedness and packed whole-cell reads using retained headers.
use super::*;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

pub(crate) struct Packed2DSelection<'a> {
    pub(crate) rows: &'a [usize],
    pub(crate) widen_complex: bool,
}

impl TiledReadMetadata {
    /// Read uniformly shaped cells into a single Fortran-order column.
    /// Determine eligibility from the shape map before reading tile data.
    pub(crate) fn load_uniform_2d(
        &self,
        path: &Path,
        seq_nr: u32,
        bound: &[&ColumnDescContents],
        table_desc: &TableDescContents,
        bound_index: usize,
        selection: Packed2DSelection<'_>,
    ) -> Result<Option<ArrayValue>, StorageError> {
        let Packed2DSelection {
            rows,
            widen_complex,
        } = selection;
        let read = self.header(seq_nr)?;
        let TiledVariant::Shape {
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
            ..
        } = &read.variant
        else {
            return Ok(None);
        };
        let tiles = tile_columns(
            &read.variant,
            Cow::Borrowed(&read.header),
            bound,
            table_desc,
            rows_reached(rows),
        )?;
        let Some(target) = tiles.tile_index[bound_index] else {
            return Ok(None);
        };
        let header = &tiles.header;
        let mapping = ShapeRowMapping {
            nr_used_row_map: *nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
        };
        let Some([axis0, channels]) = uniform_shape_2d(header, &mapping, rows)? else {
            return Ok(None);
        };
        let dt = header.col_data_types[target];
        let col_desc = bound[bound_index];
        let shape = [axis0, channels, rows.len()];
        // The first read benefits from coalesced streaming. A later sparse
        // read can warm the shared tiles when the caller revisits this manager.
        let reuse_sparse_tiles = read.packed_read_seen.set(()).is_err();
        macro_rules! read_column {
            ($dest:ident, $value:ident) => {{
                let mut values = Vec::new();
                let filled = fill_tiled_column_rows_shape_variant_2d_channel_range_typed(
                    path,
                    seq_nr,
                    header,
                    target,
                    col_desc,
                    dt,
                    tile_element_size(dt),
                    rows,
                    &mapping,
                    0,
                    channels,
                    SelectedArray2DCellsMut::$dest(&mut values),
                    widen_complex,
                    reuse_sparse_tiles,
                )?;
                if filled.is_none() {
                    return Ok(None);
                }
                let array = ArrayD::from_shape_vec(IxDyn(&shape).f(), values)
                    .map_err(|error| StorageError::FormatMismatch(error.to_string()))?;
                Ok(Some(ArrayValue::$value(array)))
            }};
        }
        match dt {
            CasacoreDataType::TpBool => read_column!(RowChannelBool, Bool),
            CasacoreDataType::TpFloat => read_column!(RowChannelFloat32, Float32),
            CasacoreDataType::TpComplex if widen_complex => read_column!(Complex64, Complex64),
            CasacoreDataType::TpComplex => read_column!(RowChannelComplex32, Complex32),
            _ => Ok(None),
        }
    }

    pub(crate) fn shape_cell_is_defined(
        &self,
        dm_seq_nr: u32,
        target_col_idx: usize,
        row_index: usize,
    ) -> Result<bool, StorageError> {
        let read_header = self.header(dm_seq_nr)?;
        if target_col_idx >= read_header.header.col_data_types.len() {
            return Err(StorageError::FormatMismatch(format!(
                "tiled column index {target_col_idx} out of range for data manager {dm_seq_nr}"
            )));
        }
        let TiledVariant::Shape {
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
            ..
        } = &read_header.variant
        else {
            return Err(StorageError::FormatMismatch(format!(
                "metadata-only array-cell definedness requires TiledShapeStMan Shape variant for data manager {dm_seq_nr}"
            )));
        };
        Ok(shape_row_location(
            &read_header.header,
            &ShapeRowMapping {
                nr_used_row_map: *nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
            row_index,
        )?
        .is_some())
    }

    pub(crate) fn array_shapes(
        &self,
        dm_seq_nr: u32,
        bound: &[&ColumnDescContents],
        table_desc: &super::super::table_control::TableDescContents,
        column: usize,
        rows: &[usize],
    ) -> Result<Vec<Option<Vec<usize>>>, StorageError> {
        let read = self.header(dm_seq_nr)?;
        let tiles = tile_columns(
            &read.variant,
            std::borrow::Cow::Borrowed(&read.header),
            bound,
            table_desc,
            rows_reached(rows),
        )?;
        let target = tiles.tile_index.get(column).ok_or_else(|| {
            StorageError::FormatMismatch("tiled shape column index is out of range".to_string())
        })?;
        let header = tiles.header.as_ref();
        rows.iter()
            .map(|&row| {
                let Some((cube_index, position)) = row_location(&read.variant, header, row)? else {
                    return Ok(None);
                };
                let Some(cube) = header.cubes.get(cube_index) else {
                    return Ok(None);
                };
                if cube.file_seq_nr < 0 || cube.cube_shape.is_empty() {
                    return Ok(None);
                }
                if target.is_none() {
                    return match tile_columns::hypercube_cell(cube, &bound[column].col_name)? {
                        Value::Array(array) => Ok(Some(array.shape().to_vec())),
                        _ => Err(StorageError::FormatMismatch(
                            "hypercube shape request is not an array".to_string(),
                        )),
                    };
                }
                if matches!(read.variant, TiledVariant::Cell { .. }) {
                    Ok(Some(cube.cube_shape.clone()))
                } else {
                    let rank = cube.cube_shape.len() - 1;
                    if position >= cube.cube_shape[rank] {
                        return Ok(None);
                    }
                    Ok(Some(cube.cube_shape[..rank].to_vec()))
                }
            })
            .collect()
    }
}

fn uniform_shape_2d(
    header: &TiledStManHeader,
    mapping: &ShapeRowMapping<'_>,
    rows: &[usize],
) -> Result<Option<[usize; 2]>, StorageError> {
    let mut uniform = None;
    for &row in rows {
        let Some((cube_index, position)) = shape_row_location(header, mapping, row)? else {
            return Ok(None);
        };
        let cube = &header.cubes[cube_index];
        let &[axis0, channels, cube_rows] = cube.cube_shape.as_slice() else {
            return Ok(None);
        };
        if cube.file_seq_nr < 0 || axis0 == 0 || channels == 0 || position >= cube_rows {
            return Ok(None);
        }
        let shape = [axis0, channels];
        if uniform.is_some_and(|expected| shape != expected) {
            return Ok(None);
        }
        uniform = Some(shape);
    }
    Ok(uniform)
}

fn row_location(
    variant: &TiledVariant,
    header: &TiledStManHeader,
    row: usize,
) -> Result<Option<(usize, usize)>, StorageError> {
    match variant {
        TiledVariant::Column { .. } => Ok(Some((0, row))),
        TiledVariant::Cell { .. } => Ok(Some((row, 0))),
        TiledVariant::Shape {
            nr_used_row_map,
            row_map,
            cube_map,
            pos_map,
            ..
        } => shape_row_location(
            header,
            &ShapeRowMapping {
                nr_used_row_map: *nr_used_row_map,
                row_map,
                cube_map,
                pos_map,
            },
            row,
        ),
        TiledVariant::Data {
            row_map,
            cube_map,
            pos_map,
            ..
        } => {
            if row_map.len() != cube_map.len() || row_map.len() != pos_map.len() {
                return Err(StorageError::FormatMismatch(
                    "invalid TiledDataStMan row-map lengths".to_string(),
                ));
            }
            let Some(chunk) = row_map
                .partition_point(|&start| start <= row as u64)
                .checked_sub(1)
            else {
                return Ok(None);
            };
            let position = (pos_map[chunk] as usize)
                .checked_add(row - row_map[chunk] as usize)
                .ok_or_else(|| {
                    StorageError::FormatMismatch("tiled row position overflows usize".to_string())
                })?;
            Ok(Some((cube_map[chunk] as usize, position)))
        }
    }
}
