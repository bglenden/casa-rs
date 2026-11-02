// SPDX-License-Identifier: LGPL-3.0-or-later
//! One hypercube's tiles of the target column, as a typed selected read
//! walks them.
//!
//! A typed selected read copies each selected row's cell (a 1-D cell, or a
//! channel range of a 2-D cell) out of the tiles that hold it. A tile can
//! hold any part of a cell: CASA's ALMA filler tiles `WEIGHT` and `SIGMA`
//! one correlation to a tile (`TiledShapeStMan`, tile `[1, 131072]`), so a
//! cell's axis 0 lies in several tiles. The read gathers the parts: for each
//! row tile (and channel tile) it visits each tile along axis 0 in turn,
//! whose [`Axis0Span`] says which elements of the cell's axis 0 it holds; a
//! cell's element `start + i` of axis 0 lies at offset `i` of the tile's
//! axis 0. A tile that spans axis 0, the usual layout, gives one span from
//! 0, and the walk is the single-tile copy.
//!
//! # C++ reference
//!
//! `TSMCube::accessSection` reads a section across every tile it
//! intersects; `TSMCube::getTile` numbers the tiles in Fortran order over
//! the tile grid, which is also their bucket order in the file.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use super::{
    CasacoreDataType, SelectedCubeRow, StorageError, TilePixel, TileReadSession, TiledStManHeader,
    TsmCubeInfo, compute_tile_layout, dot_product, fortran_order_strides, load_shared_column_tile,
    load_streamed_column_tile,
};

/// The part of a cell's axis 0 that one tile along axis 0 holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Axis0Span {
    /// The tile's position along axis 0 of the tile grid.
    pub(super) tile: usize,
    /// The cell's first element of axis 0 in the tile.
    pub(super) start: usize,
    /// How many elements of the cell's axis 0 the tile holds: the tile's
    /// length along axis 0, or fewer in a last tile that pads the axis.
    pub(super) len: usize,
}

/// One cube's tiles of the target column for a typed selected read: where
/// each tile lies in the file, which part of a cell it holds, and where a
/// row's elements lie in it.
pub(super) struct SelectedTiles<'a> {
    table_path: &'a Path,
    dm_seq_nr: u32,
    header: &'a TiledStManHeader,
    cube_idx: usize,
    cube: &'a TsmCubeInfo,
    target_col_idx: usize,
    dt: CasacoreDataType,
    bucket_size: usize,
    col_offset: usize,
    tile_grid_strides: Vec<usize>,
    /// Rows a tile holds.
    row_tile_size: usize,
    /// Elements a row holds in a tile: the product of the tile's cell axes.
    row_tile_nelem: usize,
    /// The cell's length along axis 0.
    axis0_count: usize,
    /// The tile's length along axis 0: within a tile, the stride of a 2-D
    /// cell's channel axis.
    pub(super) tile_axis0_count: usize,
    /// Tiles along axis 0.
    axis0_tiles: usize,
}

impl<'a> SelectedTiles<'a> {
    /// The tiles of cube `cube_idx` (`cube`) for column `target_col_idx`
    /// of type `dt`: a cube of cells and rows, whose tile shape has the
    /// cube's rank and no empty axis.
    pub(super) fn new(
        table_path: &'a Path,
        dm_seq_nr: u32,
        header: &'a TiledStManHeader,
        target_col_idx: usize,
        dt: CasacoreDataType,
        cube_idx: usize,
        cube: &'a TsmCubeInfo,
    ) -> Result<Self, StorageError> {
        if cube.cube_shape.len() < 2
            || cube.tile_shape.len() != cube.cube_shape.len()
            || cube.tile_shape.contains(&0)
        {
            return Err(StorageError::FormatMismatch(format!(
                "typed selected read found tile shape {:?} for cube shape {:?}",
                cube.tile_shape, cube.cube_shape
            )));
        }
        let cell_ndim = cube.cube_shape.len() - 1;
        let (axis0_count, tile_axis0_count) = (cube.cube_shape[0], cube.tile_shape[0]);
        let tiles_per_dim: Vec<usize> = cube
            .cube_shape
            .iter()
            .zip(&cube.tile_shape)
            .map(|(&shape, &tile)| shape.div_ceil(tile))
            .collect();
        let (bucket_size, col_offsets) =
            compute_tile_layout(&header.col_data_types, &cube.tile_shape);
        Ok(Self {
            table_path,
            dm_seq_nr,
            header,
            cube_idx,
            cube,
            target_col_idx,
            dt,
            bucket_size,
            col_offset: col_offsets[target_col_idx],
            tile_grid_strides: fortran_order_strides(&tiles_per_dim),
            row_tile_size: cube.tile_shape[cell_ndim],
            row_tile_nelem: cube.tile_shape[..cell_ndim].iter().product(),
            axis0_count,
            tile_axis0_count,
            axis0_tiles: axis0_count.div_ceil(tile_axis0_count).max(1),
        })
    }

    /// Tiles along axis 0: one when a tile spans the cell's axis 0.
    pub(super) fn axis0_tiles(&self) -> usize {
        self.axis0_tiles
    }

    /// The part of the cell's axis 0 each tile along axis 0 holds, in
    /// order.
    pub(super) fn axis0_spans(&self) -> impl Iterator<Item = Axis0Span> + use<> {
        let (count, tile_len) = (self.axis0_count, self.tile_axis0_count);
        (0..self.axis0_tiles).map(move |tile| {
            let start = tile * tile_len;
            Axis0Span {
                tile,
                start,
                len: tile_len.min(count.saturating_sub(start)),
            }
        })
    }

    /// The selected rows of `patches` by the row tile that holds them.
    pub(super) fn rows_by_tile(
        &self,
        patches: &[SelectedCubeRow],
    ) -> BTreeMap<usize, Vec<SelectedCubeRow>> {
        let mut rows = BTreeMap::<usize, Vec<SelectedCubeRow>>::new();
        for &patch in patches {
            rows.entry(patch.pos_in_cube / self.row_tile_size)
                .or_default()
                .push(patch);
        }
        rows
    }

    /// The offset, in elements, of `selected`'s row in its tile.
    pub(super) fn row_offset(&self, selected: SelectedCubeRow) -> usize {
        (selected.pos_in_cube % self.row_tile_size) * self.row_tile_nelem
    }

    /// The index of the tile at `position` in the tile grid: the tile along
    /// axis 0 first, the row tile last.
    pub(super) fn tile_index(&self, position: &[usize]) -> usize {
        dot_product(position, &self.tile_grid_strides)
    }

    /// The tiles from `span`'s tile at one channel tile to the last tile
    /// along axis 0 at the channel tile `channel_tiles_after` further on, in
    /// one row tile: a run of consecutive buckets in the file, since the
    /// spans of each channel tile come before the next channel tile's.
    pub(super) fn run_from(&self, span: Axis0Span, channel_tiles_after: usize) -> usize {
        channel_tiles_after * self.axis0_tiles + (self.axis0_tiles - span.tile)
    }

    /// Tile `tile_index` of the target column, through the shared cache.
    pub(super) fn shared(
        &self,
        tile_index: usize,
        session: &mut TileReadSession,
    ) -> Result<Arc<[u8]>, StorageError> {
        load_shared_column_tile(
            self.table_path,
            self.dm_seq_nr,
            self.header,
            self.cube_idx,
            self.cube,
            self.target_col_idx,
            self.bucket_size,
            self.col_offset,
            self.dt,
            tile_index,
            session,
        )
    }

    /// Tile `tile_index` of the target column: with `run`, the tiles left
    /// in its run of consecutive buckets, streamed into `streamed` past the
    /// shared cache (`None` returned); otherwise through the shared cache.
    pub(super) fn load(
        &self,
        tile_index: usize,
        run: Option<usize>,
        session: &mut TileReadSession,
        streamed: &mut Vec<u8>,
    ) -> Result<Option<Arc<[u8]>>, StorageError> {
        let Some(run) = run else {
            return self.shared(tile_index, session).map(Some);
        };
        load_streamed_column_tile(
            self.table_path,
            self.dm_seq_nr,
            self.header,
            self.cube,
            self.bucket_size,
            self.col_offset,
            self.dt,
            tile_index,
            run,
            session,
            streamed,
        )?;
        Ok(None)
    }
}

/// Copy a run of `len` pixels of `T` from tile bytes `tile` at byte `src`
/// to value bytes `values` at byte `dst`. A one-pixel run, which a tile one
/// element deep along axis 0 gives for every channel of every row, moves
/// as one fixed-size copy rather than a call to `memcpy`.
#[inline(always)]
pub(super) fn copy_pixels<T: TilePixel>(
    values: &mut [u8],
    dst: usize,
    tile: &[u8],
    src: usize,
    len: usize,
) {
    if len == 1 {
        values[dst..dst + T::ELEM_SIZE].copy_from_slice(&tile[src..src + T::ELEM_SIZE]);
    } else {
        let bytes = len * T::ELEM_SIZE;
        values[dst..dst + bytes].copy_from_slice(&tile[src..src + bytes]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cube(cube_shape: Vec<usize>, tile_shape: Vec<usize>) -> TsmCubeInfo {
        TsmCubeInfo {
            values: casa_types::RecordValue::default(),
            extensible: false,
            cube_shape,
            tile_shape,
            file_seq_nr: 0,
            file_offset: 0,
        }
    }

    fn header() -> TiledStManHeader {
        TiledStManHeader {
            big_endian: false,
            seq_nr: 0,
            nrrow: 0,
            col_data_types: vec![CasacoreDataType::TpFloat],
            hypercolumn_name: String::new(),
            max_cache_size: 0,
            nrdim: 2,
            files: Vec::new(),
            cubes: Vec::new(),
        }
    }

    fn spans(cube_shape: Vec<usize>, tile_shape: Vec<usize>) -> Vec<(usize, usize, usize)> {
        let (header, cube) = (header(), cube(cube_shape, tile_shape));
        SelectedTiles::new(
            Path::new(""),
            0,
            &header,
            0,
            CasacoreDataType::TpFloat,
            0,
            &cube,
        )
        .expect("tiles")
        .axis0_spans()
        .map(|span| (span.tile, span.start, span.len))
        .collect()
    }

    /// A tile that spans axis 0, exactly or padding it, holds it all from
    /// 0; split tiles hold consecutive parts, the last one partial when the
    /// tile pads the axis.
    #[test]
    fn axis0_spans_cover_the_cell_once() {
        assert_eq!(spans(vec![4, 10], vec![4, 5]), [(0, 0, 4)]);
        assert_eq!(spans(vec![2, 10], vec![4, 5]), [(0, 0, 2)]);
        assert_eq!(
            spans(vec![4, 10], vec![1, 5]),
            [(0, 0, 1), (1, 1, 1), (2, 2, 1), (3, 3, 1)]
        );
        assert_eq!(spans(vec![4, 10], vec![3, 5]), [(0, 0, 3), (1, 3, 1)]);
        assert_eq!(spans(vec![4, 6, 10], vec![2, 4, 5]), [(0, 0, 2), (1, 2, 2)]);
    }

    /// Tiles number in Fortran order over the grid, so a row tile's spans
    /// of consecutive channel tiles are consecutive buckets.
    #[test]
    fn a_row_tile_reads_its_channel_and_axis0_tiles_as_one_run() {
        let (header, cube) = (header(), cube(vec![4, 6, 10], vec![1, 2, 5]));
        let tiles = SelectedTiles::new(
            Path::new(""),
            0,
            &header,
            0,
            CasacoreDataType::TpFloat,
            0,
            &cube,
        )
        .expect("tiles");
        assert_eq!(tiles.axis0_tiles(), 4);
        // Row tile 1 starts at bucket 4 * 3; read from channel tile 1 to
        // the last, 2, its run ends at bucket 4 * 3 + 4 * 3 - 1.
        let mut expected = 4 * 3 + 4;
        for channel_tile in 1..3 {
            for span in tiles.axis0_spans() {
                assert_eq!(
                    tiles.tile_index(&[span.tile, channel_tile, 1]),
                    expected,
                    "consecutive buckets"
                );
                assert_eq!(tiles.run_from(span, 2 - channel_tile), 24 - expected);
                expected += 1;
            }
        }
    }

    /// A tile shape empty along an axis, or not of the cube's rank, is
    /// refused rather than divided by or indexed past.
    #[test]
    fn a_malformed_tile_shape_is_refused() {
        let header = header();
        for (cube_shape, tile_shape) in [
            (vec![4, 10], vec![0, 5]),
            (vec![4, 10], vec![4, 0]),
            (vec![4, 6, 10], vec![4, 0, 5]),
            (vec![4, 6, 10], vec![4, 5]),
            (vec![10], vec![5]),
        ] {
            let cube = cube(cube_shape, tile_shape);
            let tiles = SelectedTiles::new(
                Path::new(""),
                0,
                &header,
                0,
                CasacoreDataType::TpFloat,
                0,
                &cube,
            );
            assert!(
                tiles.is_err(),
                "{:?} {:?}",
                cube.cube_shape,
                cube.tile_shape
            );
        }
    }
}
