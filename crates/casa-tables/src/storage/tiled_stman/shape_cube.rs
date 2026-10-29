// SPDX-License-Identifier: LGPL-3.0-or-later
//! One hypercube of a streamed `TiledShapeStMan` column, written a row at a
//! time.
//!
//! A row fills its slot in every tile across the cell, in the band of tiles
//! that holds it along the row axis. Tiles are numbered with the row axis
//! slowest, so a band is contiguous in the cube's file. A cube whose band
//! fits the writer's budget assembles each band in memory and writes it with
//! one call when its last row arrives. Otherwise each tile row is written in
//! place as it is pushed, a Bool tile row by reading back the bytes it
//! shares with its neighbours. The cubes of a column share one tile-row
//! scratch, held by the column's writer ([`ShapeCubeWorkingSet`]).

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::{
    StreamedPrimitiveTilePlan, StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType,
    StreamingTiledShapeWriter, compute_tile_layout, copy_cube_to_tile, linear_to_nd,
};
use crate::storage::StorageError;
use crate::storage::canonical::write_bool_bits_from_bytes;

#[derive(Debug)]
pub(super) struct DirectShapeCubeWriter {
    pub(super) temp_data_path: PathBuf,
    pub(super) file: File,
    pub(super) cell_shape: Vec<usize>,
    pub(super) row_count: usize,
    pub(super) rows_written: usize,
    pub(super) tile_shape: Vec<usize>,
    tile_plans: Vec<StreamedPrimitiveTilePlan>,
    pub(super) non_row_tile_count: usize,
    pub(super) rows_per_tile: usize,
    tile_cell_nelem: usize,
    pub(super) bucket_size: usize,
    /// Every tile of the band being assembled; empty when the cube writes
    /// each tile row in place.
    band: Vec<u8>,
}

/// The bytes a cube works in while it writes a row.
///
/// The encoded row and a Bool tile row's packed bytes live only while the
/// row is written; the tile row is the writer's scratch, shared by every
/// cube and sized for the largest. A column holds, at once, its largest
/// encoded row, its largest tile row and its largest packed Bool tile row,
/// which may come from different cubes ([`Self::max`], [`Self::bytes`]).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ShapeCubeWorkingSet {
    /// One row's cell, encoded.
    pub(super) row_bytes: usize,
    /// One tile row of the cell.
    pub(super) tile_row_bytes: usize,
    /// One Bool tile row's packed bits and the byte they may share.
    pub(super) packed_bool_bytes: usize,
}

impl ShapeCubeWorkingSet {
    /// The larger of each part of `self` and `other`.
    pub(super) fn max(self, other: Self) -> Self {
        Self {
            row_bytes: self.row_bytes.max(other.row_bytes),
            tile_row_bytes: self.tile_row_bytes.max(other.tile_row_bytes),
            packed_bool_bytes: self.packed_bool_bytes.max(other.packed_bool_bytes),
        }
    }

    /// Every part, held at once.
    pub(super) fn bytes(self) -> Option<usize> {
        self.row_bytes
            .checked_add(self.tile_row_bytes)?
            .checked_add(self.packed_bool_bytes)
    }
}

/// What a cube of one layout is: the bytes it works in per row, its tiles
/// along each axis of the cube (the row axis last), and one tile's bytes.
struct Geometry {
    working_set: ShapeCubeWorkingSet,
    tiles_per_dim: Vec<usize>,
    tile_cell_nelem: usize,
    bucket_size: usize,
}

impl Geometry {
    /// The geometry of a cube of `layout`, which it checks: rows, cell
    /// extents and a tile of the cell's rank and the row axis, none empty.
    fn of(
        layout: &StreamedTiledShapeCubeLayout,
        value_type: StreamedTiledShapeValueType,
    ) -> Result<Self, StorageError> {
        if layout.row_count == 0 {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape layout {:?} has no rows",
                layout.cell_shape
            )));
        }
        if layout.cell_shape.is_empty() || layout.cell_shape.contains(&0) {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape cell dimensions must be positive: {:?}",
                layout.cell_shape
            )));
        }
        if layout.tile_shape.len() != layout.cell_shape.len() + 1 || layout.tile_shape.contains(&0)
        {
            return Err(StorageError::FormatMismatch(format!(
                "streamed TiledShape tile rank must be cell rank + row axis; cell={:?} tile={:?}",
                layout.cell_shape, layout.tile_shape
            )));
        }
        let elem_size = value_type.elem_size();
        let overflow = |what: &str| {
            StorageError::FormatMismatch(format!("streamed TiledShape {what} overflows usize"))
        };
        let product = |extents: &[usize], what: &str| {
            extents
                .iter()
                .try_fold(1usize, |acc, &dim| acc.checked_mul(dim))
                .ok_or_else(|| overflow(what))
        };
        let cell_rank = layout.cell_shape.len();
        let cell_nelem = product(&layout.cell_shape, "cell size")?;
        let tile_cell_nelem = product(&layout.tile_shape[..cell_rank], "tile size")?;
        let row_bytes = cell_nelem
            .checked_mul(elem_size)
            .ok_or_else(|| overflow("row byte size"))?;
        let tile_row_bytes = tile_cell_nelem
            .checked_mul(elem_size)
            .ok_or_else(|| overflow("tile-row size"))?;
        let packed_bool_bytes = if value_type == StreamedTiledShapeValueType::Bool {
            tile_cell_nelem.div_ceil(8) + 1
        } else {
            0
        };
        let working_set = ShapeCubeWorkingSet {
            row_bytes,
            tile_row_bytes,
            packed_bool_bytes,
        };
        working_set.bytes().ok_or_else(|| overflow("working set"))?;
        let mut cube_shape = layout.cell_shape.clone();
        cube_shape.push(layout.row_count);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(layout.tile_shape.iter())
            .map(|(&cube, &tile)| cube.div_ceil(tile))
            .collect();
        product(&tiles_per_dim, "tile count")?;
        let (bucket_size, _) =
            compute_tile_layout(&[value_type.casacore_type()], &layout.tile_shape);
        Ok(Self {
            working_set,
            tiles_per_dim,
            tile_cell_nelem,
            bucket_size,
        })
    }

    /// Tiles across the cell: one band's.
    fn non_row_tile_count(&self) -> usize {
        self.tiles_per_dim[..self.tiles_per_dim.len() - 1]
            .iter()
            .product()
    }
}

/// The budget from which a column writer of `rows` rows, working in
/// `working` bytes, assembles its cubes' bands (`bands` bytes) in memory:
/// both, beside the largest row map the rows can need.
pub(super) fn buffered_bytes(working: usize, bands: usize, rows: usize) -> usize {
    working
        .saturating_add(bands)
        .saturating_add(rows.saturating_mul(3 * size_of::<u32>()))
}

impl StreamingTiledShapeWriter {
    /// The bytes a writer of `value_type` cells in `layouts` works in, the
    /// least budget it is created with (its largest encoded row, tile row
    /// and packed Bool tile row), and the budget from which it assembles
    /// its bands in memory ([`buffered_bytes`]), which it then holds.
    ///
    /// # Errors
    ///
    /// A layout the writer refuses, or sizes that overflow.
    pub fn resident_bytes(
        value_type: StreamedTiledShapeValueType,
        layouts: &[StreamedTiledShapeCubeLayout],
    ) -> Result<(usize, usize), StorageError> {
        let mut working_set = ShapeCubeWorkingSet::default();
        let (mut bands, mut rows) = (0usize, 0usize);
        for layout in layouts {
            let geometry = Geometry::of(layout, value_type)?;
            working_set = working_set.max(geometry.working_set);
            bands = bands.saturating_add(geometry.non_row_tile_count() * geometry.bucket_size);
            rows = rows.saturating_add(layout.row_count);
        }
        let working = working_set.bytes().ok_or_else(|| {
            StorageError::FormatMismatch("streamed TiledShape working set overflows usize".into())
        })?;
        Ok((working, buffered_bytes(working, bands, rows)))
    }
}

impl DirectShapeCubeWriter {
    /// Create the cube's file at `temp_data_path`, sized for every planned
    /// row, and return the writer with the bytes it works in per row.
    pub(super) fn create(
        temp_data_path: PathBuf,
        layout: StreamedTiledShapeCubeLayout,
        value_type: StreamedTiledShapeValueType,
    ) -> Result<(Self, ShapeCubeWorkingSet), StorageError> {
        let Geometry {
            working_set,
            tiles_per_dim,
            tile_cell_nelem,
            bucket_size,
        } = Geometry::of(&layout, value_type)?;
        let cell_rank = layout.cell_shape.len();
        let cell_tiles_per_dim = &tiles_per_dim[..cell_rank];
        let non_row_tile_count = cell_tiles_per_dim.iter().product::<usize>();
        let tile_plans = (0..non_row_tile_count)
            .map(|tile_index| {
                let tile_pos = linear_to_nd(tile_index, cell_tiles_per_dim);
                let cell_start: Vec<usize> = tile_pos
                    .iter()
                    .enumerate()
                    .map(|(axis, &pos)| pos * layout.tile_shape[axis])
                    .collect();
                let actual_extent: Vec<usize> = cell_start
                    .iter()
                    .enumerate()
                    .map(|(axis, &start)| {
                        std::cmp::min(layout.tile_shape[axis], layout.cell_shape[axis] - start)
                    })
                    .collect();
                StreamedPrimitiveTilePlan {
                    cell_start,
                    actual_extent,
                }
            })
            .collect();
        let file_bytes = tiles_per_dim
            .iter()
            .product::<usize>()
            .checked_mul(bucket_size)
            .ok_or_else(|| {
                StorageError::FormatMismatch("streamed TiledShape file size overflows usize".into())
            })?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&temp_data_path)?;
        file.set_len(file_bytes as u64)?;
        let rows_per_tile = *layout.tile_shape.last().expect("validated tile row axis");
        let cube = Self {
            temp_data_path,
            file,
            cell_shape: layout.cell_shape,
            row_count: layout.row_count,
            rows_written: 0,
            tile_shape: layout.tile_shape,
            tile_plans,
            non_row_tile_count,
            rows_per_tile,
            tile_cell_nelem,
            bucket_size,
            band: Vec::new(),
        };
        Ok((cube, working_set))
    }

    /// Bytes of one band: every tile across the cell, one tile along the row
    /// axis.
    pub(super) fn band_bytes(&self) -> usize {
        self.non_row_tile_count * self.bucket_size
    }

    /// Assemble each band in memory from now on.
    pub(super) fn buffer_bands(&mut self) {
        self.band = vec![0; self.band_bytes()];
    }

    /// Write the next row's cell, `encoded` in the cube's cell order, one
    /// tile row at a time through `tile_row`, the column's scratch of at
    /// least this cube's tile-row bytes.
    pub(super) fn write_row(
        &mut self,
        value_type: StreamedTiledShapeValueType,
        encoded: &[u8],
        tile_row: &mut [u8],
    ) -> Result<(), StorageError> {
        let elem_size = value_type.elem_size();
        let cell_rank = self.cell_shape.len();
        let row_tile_index = self.rows_written / self.rows_per_tile;
        let row_in_tile = self.rows_written % self.rows_per_tile;
        let tile_row_bytes = self.tile_cell_nelem * elem_size;
        let tile_row = &mut tile_row[..tile_row_bytes];
        for (cell_tile_index, plan) in self.tile_plans.iter().enumerate() {
            tile_row.fill(0);
            copy_cube_to_tile(
                encoded,
                &self.cell_shape,
                tile_row,
                &self.tile_shape[..cell_rank],
                &plan.cell_start,
                &plan.actual_extent,
                elem_size,
            );
            let band_offset = cell_tile_index * self.bucket_size;
            let bucket_start = row_tile_index * self.band_bytes() + band_offset;
            if value_type == StreamedTiledShapeValueType::Bool {
                let bit_offset = row_in_tile * self.tile_cell_nelem;
                let byte_offset = bit_offset / 8;
                let bit_in_byte = bit_offset % 8;
                let byte_count = (bit_in_byte + self.tile_cell_nelem).div_ceil(8);
                if self.band.is_empty() {
                    let mut packed = vec![0u8; byte_count];
                    self.file
                        .seek(SeekFrom::Start((bucket_start + byte_offset) as u64))?;
                    self.file.read_exact(&mut packed)?;
                    write_bool_bits_from_bytes(&mut packed, bit_in_byte, tile_row);
                    self.file
                        .seek(SeekFrom::Start((bucket_start + byte_offset) as u64))?;
                    self.file.write_all(&packed)?;
                } else {
                    let start = band_offset + byte_offset;
                    write_bool_bits_from_bytes(
                        &mut self.band[start..start + byte_count],
                        bit_in_byte,
                        tile_row,
                    );
                }
            } else {
                let row_offset = row_in_tile * tile_row_bytes;
                if self.band.is_empty() {
                    self.file
                        .seek(SeekFrom::Start((bucket_start + row_offset) as u64))?;
                    self.file.write_all(tile_row)?;
                } else {
                    let start = band_offset + row_offset;
                    self.band[start..start + tile_row_bytes].copy_from_slice(tile_row);
                }
            }
        }
        self.rows_written += 1;
        if !self.band.is_empty() && self.rows_written.is_multiple_of(self.rows_per_tile) {
            self.write_band(row_tile_index)?;
        }
        Ok(())
    }

    /// Write the last band if rows ended inside it. Its rows past the last
    /// are zero, as they are in the file.
    pub(super) fn finish_band(&mut self) -> Result<(), StorageError> {
        if !self.band.is_empty() && !self.rows_written.is_multiple_of(self.rows_per_tile) {
            self.write_band(self.rows_written / self.rows_per_tile)?;
        }
        Ok(())
    }

    fn write_band(&mut self, row_tile_index: usize) -> Result<(), StorageError> {
        self.file
            .seek(SeekFrom::Start((row_tile_index * self.band.len()) as u64))?;
        self.file.write_all(&self.band)?;
        self.band.fill(0);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use num_complex::Complex32;

    use super::super::{StreamedTiledShapeColumn, StreamingTiledShapeWriter};
    use super::*;

    /// Two shapes, each tiled in two tiles across the cell; the first ends
    /// inside a band, and undefined rows fall between them.
    fn layouts() -> Vec<StreamedTiledShapeCubeLayout> {
        vec![
            StreamedTiledShapeCubeLayout::new(vec![2, 5], 9, vec![2, 3, 3]),
            StreamedTiledShapeCubeLayout::new(vec![2, 4], 5, vec![1, 4, 2]),
        ]
    }

    /// The pushes: `Some(0)` a row of the first shape, `Some(1)` of the
    /// second, `None` an undefined row.
    const ORDER: [Option<usize>; 14] = [
        Some(0),
        Some(0),
        Some(1),
        None,
        Some(0),
        Some(1),
        Some(1),
        Some(0),
        None,
        Some(0),
        Some(1),
        Some(0),
        None,
        Some(0),
    ];

    fn write(
        dir: &std::path::Path,
        name: &str,
        value_type: StreamedTiledShapeValueType,
        budget: usize,
    ) -> (StreamedTiledShapeColumn, Vec<Vec<u8>>) {
        let mut writer =
            StreamingTiledShapeWriter::create(dir.join(name), value_type, layouts(), budget, true)
                .expect("create writer");
        let shapes = [[2usize, 5], [2, 4]];
        for (row, shape) in ORDER.iter().enumerate() {
            let Some(shape) = shape else {
                writer.push_undefined_row().expect("undefined row");
                continue;
            };
            let cell = shapes[*shape];
            let samples = cell[0] * cell[1];
            if value_type == StreamedTiledShapeValueType::Bool {
                let values = (0..samples)
                    .map(|sample| (row * 7 + sample * 3) % 5 < 2)
                    .collect::<Vec<_>>();
                writer.push_bool_row(&cell, &values).expect("bool row");
            } else {
                let values = (0..samples)
                    .map(|sample| Complex32::new(row as f32, sample as f32 + 0.5))
                    .collect::<Vec<_>>();
                writer
                    .push_complex32_row(&cell, &values)
                    .expect("complex row");
            }
        }
        let column = writer.finish().expect("finish");
        let files = column
            .cubes
            .iter()
            .map(|cube| std::fs::read(&cube.temp_data_path).expect("cube file"))
            .collect();
        (column, files)
    }

    #[test]
    fn band_buffered_cubes_write_the_files_of_in_place_rows() {
        let dir = tempfile::tempdir().expect("tempdir");
        for value_type in [
            StreamedTiledShapeValueType::Complex32,
            StreamedTiledShapeValueType::Bool,
        ] {
            // A budget that holds the working set and the largest row map,
            // but not the bands, keeps every tile row in place.
            let elem = value_type.elem_size();
            let packed = if value_type == StreamedTiledShapeValueType::Bool {
                6usize.div_ceil(8) + 1
            } else {
                0
            };
            let working = 10 * elem + 6 * elem + packed;
            let in_place_budget = working + ORDER.len() * 3 * size_of::<u32>();
            let (direct, direct_files) = write(
                dir.path(),
                &format!("{value_type:?}.direct"),
                value_type,
                in_place_budget,
            );
            let (banded, banded_files) = write(
                dir.path(),
                &format!("{value_type:?}.banded"),
                value_type,
                1 << 20,
            );
            assert!(
                banded.max_resident_bytes() > direct.max_resident_bytes(),
                "{value_type:?}: the large budget buffers bands"
            );
            assert_eq!(direct_files.len(), 2);
            assert_eq!(direct_files, banded_files, "{value_type:?} cube files");
            assert_eq!(direct.row_map, banded.row_map);
            assert_eq!(direct.cube_map, banded.cube_map);
            assert_eq!(direct.pos_map, banded.pos_map);
        }
    }

    /// [`StreamingTiledShapeWriter::resident_bytes`] states the least
    /// budget a writer is created with and the budget from which it
    /// buffers its bands.
    #[test]
    fn resident_bytes_are_the_least_and_the_band_buffering_budgets() {
        let dir = tempfile::tempdir().expect("tempdir");
        for value_type in [
            StreamedTiledShapeValueType::Complex32,
            StreamedTiledShapeValueType::Bool,
        ] {
            let (working, buffered) =
                StreamingTiledShapeWriter::resident_bytes(value_type, &layouts()).expect("bytes");
            let refused = StreamingTiledShapeWriter::create(
                dir.path().join(format!("{value_type:?}.less")),
                value_type,
                layouts(),
                working - 1,
                true,
            );
            assert!(refused.is_err(), "{value_type:?}: below the working set");
            let peak = |name: &str, budget| {
                write(dir.path(), name, value_type, budget)
                    .0
                    .max_resident_bytes()
            };
            let (below, at) = (
                peak(&format!("{value_type:?}.below"), buffered - 1),
                peak(&format!("{value_type:?}.at"), buffered),
            );
            assert!(below < at, "{value_type:?}: bands buffer from {buffered}");
            assert!(at <= buffered, "{value_type:?}: held {at}, beyond {buffered}");
        }
    }
}
