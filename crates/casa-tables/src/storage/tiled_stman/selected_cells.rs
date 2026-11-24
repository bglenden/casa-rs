// SPDX-License-Identifier: LGPL-3.0-or-later
//! Read complete selected cells in tile order without retaining whole cubes.

use std::collections::BTreeMap;
use std::path::Path;

use super::{
    ArrayValue, CasacoreDataType, ColumnDescContents, SHARED_TILE_BIG_ENDIAN, SelectedCubeRow,
    SelectedTiles, StorageError, TILE_READ_SESSION_RECENT_TILE_COUNT, TileReadSession,
    TiledStManHeader, TsmCubeInfo, Value, copy_tile_to_cube, decode_array_value, linear_to_nd,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn load_selected_rows_from_touched_cubes(
    table_path: &Path,
    dm_seq_nr: u32,
    header: &TiledStManHeader,
    target_col_idx: usize,
    col_desc: &ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
    output_len: usize,
    patches_by_cube: BTreeMap<usize, Vec<SelectedCubeRow>>,
) -> Result<Vec<Option<ArrayValue>>, StorageError> {
    let column = SelectedColumn {
        table_path,
        dm_seq_nr,
        header,
        target_col_idx,
        col_desc,
        dt,
        elem_size,
    };
    let mut outputs = vec![None; output_len];
    let mut session = TileReadSession::default();
    for (cube_idx, patches) in patches_by_cube {
        let Some(cube) = header.cubes.get(cube_idx) else {
            continue;
        };
        if cube.file_seq_nr >= 0 && !cube.cube_shape.is_empty() {
            column.read_cube(cube_idx, cube, &patches, &mut outputs, &mut session)?;
        }
    }
    Ok(outputs)
}

struct SelectedColumn<'a> {
    table_path: &'a Path,
    dm_seq_nr: u32,
    header: &'a TiledStManHeader,
    target_col_idx: usize,
    col_desc: &'a ColumnDescContents,
    dt: CasacoreDataType,
    elem_size: usize,
}

impl SelectedColumn<'_> {
    fn read_cube(
        &self,
        cube_idx: usize,
        cube: &TsmCubeInfo,
        patches: &[SelectedCubeRow],
        outputs: &mut [Option<ArrayValue>],
        session: &mut TileReadSession,
    ) -> Result<(), StorageError> {
        let tiles = SelectedTiles::new(
            self.table_path,
            self.dm_seq_nr,
            self.header,
            self.target_col_idx,
            self.dt,
            cube_idx,
            cube,
        )?;
        let cell_axes = cube.cube_shape.len() - 1;
        let cell_shape = &cube.cube_shape[..cell_axes];
        let tile_shape = &cube.tile_shape[..cell_axes];
        let tile_row_bytes = tile_shape.iter().product::<usize>() * self.elem_size;
        let rows_by_tile = tiles.rows_by_tile(patches);
        if tile_shape == cell_shape {
            for (row_tile, rows) in rows_by_tile {
                let tile = tiles.shared(row_tile, session)?;
                for row in rows {
                    let start = tiles.row_offset(row) * self.elem_size;
                    outputs[row.out_idx] =
                        Some(self.decode(&tile[start..start + tile_row_bytes], cell_shape)?);
                }
            }
            return Ok(());
        }

        let count_per_axis: Vec<_> = cell_shape
            .iter()
            .zip(tile_shape)
            .map(|(&cell, &tile)| cell.div_ceil(tile))
            .collect();
        let tiles_per_cell = count_per_axis.iter().product::<usize>();
        let cell_bytes = cell_shape.iter().product::<usize>() * self.elem_size;
        let stream =
            rows_by_tile.len().saturating_mul(tiles_per_cell) > TILE_READ_SESSION_RECENT_TILE_COUNT;
        let mut streamed_tile = Vec::new();
        for (row_tile, rows) in rows_by_tile {
            // Assemble just this stripe. Each tile supplies all selected rows
            // it contains, even when one stripe exceeds the shared cache.
            let mut raw_cells = vec![vec![0; cell_bytes]; rows.len()];
            for cell_tile in 0..tiles_per_cell {
                let loaded = tiles.load(
                    row_tile * tiles_per_cell + cell_tile,
                    stream.then_some(tiles_per_cell - cell_tile),
                    session,
                    &mut streamed_tile,
                )?;
                let tile = loaded.as_deref().unwrap_or(&streamed_tile);
                let start: Vec<_> = linear_to_nd(cell_tile, &count_per_axis)
                    .into_iter()
                    .zip(tile_shape)
                    .map(|(index, &tile)| index * tile)
                    .collect();
                let extent: Vec<_> = cell_shape
                    .iter()
                    .zip(&start)
                    .zip(tile_shape)
                    .map(|((&cell, &start), &tile)| (cell - start).min(tile))
                    .collect();
                for (&row, raw) in rows.iter().zip(&mut raw_cells) {
                    let offset = tiles.row_offset(row) * self.elem_size;
                    copy_tile_to_cube(
                        &tile[offset..offset + tile_row_bytes],
                        tile_shape,
                        raw,
                        cell_shape,
                        &start,
                        &extent,
                        self.elem_size,
                    );
                }
            }
            for (row, raw) in rows.into_iter().zip(raw_cells) {
                outputs[row.out_idx] = Some(self.decode(&raw, cell_shape)?);
            }
        }
        Ok(())
    }

    fn decode(&self, raw: &[u8], shape: &[usize]) -> Result<ArrayValue, StorageError> {
        match decode_array_value(raw, shape, self.dt, SHARED_TILE_BIG_ENDIAN)? {
            Value::Array(array) => Ok(array),
            other => Err(StorageError::FormatMismatch(format!(
                "tiled array column {} decoded as non-array value {:?}",
                self.col_desc.col_name,
                other.kind()
            ))),
        }
    }
}
