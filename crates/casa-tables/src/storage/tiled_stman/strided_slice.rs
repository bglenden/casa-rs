// SPDX-License-Identifier: LGPL-3.0-or-later

use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{
    StorageError, TilePixel, TiledFileIO, checked_storage_product, fortran_order_strides,
    tile_as_typed,
};

#[derive(Clone, Copy, Default)]
struct TileRun {
    offset: usize,
    count: usize,
}

impl TiledFileIO {
    pub(super) fn get_strided_slice<T: TilePixel>(
        &mut self,
        start: &[usize],
        shape: &[usize],
        stride: &[usize],
    ) -> Result<ArrayD<T>, StorageError> {
        let rank = self.cube_shape.len();
        if rank == 0
            || start.len() != rank
            || shape.len() != rank
            || stride.len() != rank
            || stride.contains(&0)
            || shape.contains(&0)
            || (0..rank).any(|axis| {
                (shape[axis] - 1)
                    .checked_mul(stride[axis])
                    .and_then(|range| start[axis].checked_add(range))
                    .is_none_or(|last| last >= self.cube_shape[axis])
            })
        {
            return Err(StorageError::FormatMismatch(
                "invalid tiled slice bounds or destination".into(),
            ));
        }

        let mut values = vec![T::default(); checked_storage_product(shape, "slice size")?];
        let output_strides = fortran_order_strides(shape);
        let mut samples = vec![0; rank];
        let mut runs = vec![TileRun::default(); rank];
        let tile_strides = self.tile_strides.clone();

        loop {
            let mut tile = 0;
            let mut output_base = 0;
            for axis in 0..rank {
                let position = start[axis] + samples[axis] * stride[axis];
                let tile_len = self.tile_shape[axis];
                let offset = position % tile_len;
                tile += position / tile_len * self.tiles_per_dim_strides[axis];
                output_base += samples[axis] * output_strides[axis];
                runs[axis] = TileRun {
                    offset,
                    count: (1 + (tile_len - offset - 1) / stride[axis])
                        .min(shape[axis] - samples[axis]),
                };
            }
            let tile = tile_as_typed::<T>(self.get_cached_tile(tile)?);
            gather_tile(
                tile,
                &mut values[output_base..],
                &runs,
                stride,
                &tile_strides,
                &output_strides,
            );

            // Advance along the fastest remaining axis with selected samples.
            // The traversal makes no lookups for tiles between samples.
            let mut more_tiles = false;
            for axis in 0..rank {
                samples[axis] += runs[axis].count;
                if samples[axis] < shape[axis] {
                    more_tiles = true;
                    break;
                }
                samples[axis] = 0;
            }
            if !more_tiles {
                break;
            }
        }
        ArrayD::from_shape_vec(IxDyn(shape).f(), values)
            .map_err(|error| StorageError::FormatMismatch(format!("array shape: {error}")))
    }
}

fn gather_tile<T: TilePixel>(
    tile: &[T],
    output: &mut [T],
    runs: &[TileRun],
    strides: &[usize],
    tile_strides: &[usize],
    output_strides: &[usize],
) {
    let rows = runs[1..].iter().map(|run| run.count).product::<usize>();
    let width = runs[0].count;
    let inner_stride = strides[0];
    for mut row in 0..rows {
        let mut tile_offset = runs[0].offset;
        let mut output_offset = 0;
        for axis in 1..runs.len() {
            let position = row % runs[axis].count;
            row /= runs[axis].count;
            tile_offset += (runs[axis].offset + position * strides[axis]) * tile_strides[axis];
            output_offset += position * output_strides[axis];
        }
        let row = &tile[tile_offset..=tile_offset + (width - 1) * inner_stride];
        let destination = &mut output[output_offset..output_offset + width];
        if inner_stride == 1 {
            destination.copy_from_slice(row);
        } else {
            for (value, &sample) in destination.iter_mut().zip(row.iter().step_by(inner_stride)) {
                *value = sample;
            }
        }
    }
}
