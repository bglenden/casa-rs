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
//! shares with its neighbours.

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use super::{
    StreamedPrimitiveTilePlan, StreamedTiledShapeCubeLayout, StreamedTiledShapeValueType,
    compute_tile_layout, copy_cube_to_tile, linear_to_nd,
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
    /// One tile row of the row being written.
    tile_row: Vec<u8>,
}

impl DirectShapeCubeWriter {
    /// Create the cube's file at `temp_data_path`, sized for every planned
    /// row, and return the writer with the bytes it works in per row (the
    /// encoded row, a tile row, and a Bool tile row's packed bytes).
    pub(super) fn create(
        temp_data_path: PathBuf,
        layout: StreamedTiledShapeCubeLayout,
        value_type: StreamedTiledShapeValueType,
    ) -> Result<(Self, usize), StorageError> {
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
        let working_bytes = row_bytes
            .checked_add(tile_row_bytes)
            .and_then(|bytes| bytes.checked_add(packed_bool_bytes))
            .ok_or_else(|| overflow("working set"))?;

        let mut cube_shape = layout.cell_shape.clone();
        cube_shape.push(layout.row_count);
        let tiles_per_dim: Vec<usize> = cube_shape
            .iter()
            .zip(layout.tile_shape.iter())
            .map(|(&cube, &tile)| cube.div_ceil(tile))
            .collect();
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
        let (bucket_size, _) =
            compute_tile_layout(&[value_type.casacore_type()], &layout.tile_shape);
        let file_bytes = product(&tiles_per_dim, "tile count")?
            .checked_mul(bucket_size)
            .ok_or_else(|| overflow("file size"))?;
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
            tile_row: Vec::new(),
        };
        Ok((cube, working_bytes))
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

    /// Write the next row's cell, `encoded` in the cube's cell order.
    pub(super) fn write_row(
        &mut self,
        value_type: StreamedTiledShapeValueType,
        encoded: &[u8],
    ) -> Result<(), StorageError> {
        let elem_size = value_type.elem_size();
        let cell_rank = self.cell_shape.len();
        let row_tile_index = self.rows_written / self.rows_per_tile;
        let row_in_tile = self.rows_written % self.rows_per_tile;
        let tile_row_bytes = self.tile_cell_nelem * elem_size;
        self.tile_row.resize(tile_row_bytes, 0);
        for (cell_tile_index, plan) in self.tile_plans.iter().enumerate() {
            self.tile_row.fill(0);
            copy_cube_to_tile(
                encoded,
                &self.cell_shape,
                &mut self.tile_row,
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
                    write_bool_bits_from_bytes(&mut packed, bit_in_byte, &self.tile_row);
                    self.file
                        .seek(SeekFrom::Start((bucket_start + byte_offset) as u64))?;
                    self.file.write_all(&packed)?;
                } else {
                    let start = band_offset + byte_offset;
                    write_bool_bits_from_bytes(
                        &mut self.band[start..start + byte_count],
                        bit_in_byte,
                        &self.tile_row,
                    );
                }
            } else {
                let row_offset = row_in_tile * tile_row_bytes;
                if self.band.is_empty() {
                    self.file
                        .seek(SeekFrom::Start((bucket_start + row_offset) as u64))?;
                    self.file.write_all(&self.tile_row)?;
                } else {
                    let start = band_offset + row_offset;
                    self.band[start..start + tile_row_bytes].copy_from_slice(&self.tile_row);
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
}
