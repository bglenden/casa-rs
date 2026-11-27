// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use ndarray::Dimension;

use casa_images::ImageError;
use casa_images::bounded_read_cursor;

use super::super::super::axes::StridedSection;
use super::super::SubimagePlan;
use super::super::read_window::PixelSections;

pub(super) struct Sample {
    pub(super) index: usize,
    pub(super) value: f32,
}

pub(super) struct Pixels {
    shape: Vec<usize>,
    strides: Vec<usize>,
    pub(super) samples: Vec<Sample>,
}

impl Pixels {
    pub(super) fn read(
        reader: &PixelSections<'_, f32>,
        plan: &SubimagePlan,
        threshold: f32,
    ) -> Result<Self, ImageError> {
        let shape = plan.shape.clone();
        let strides = pixel_strides(&shape)?;
        let mut samples = Vec::new();
        visit_selection(reader, plan, |position, data, valid| {
            for (index, &value) in data.indexed_iter() {
                if value >= threshold && valid.as_ref().is_none_or(|valid| valid[&index]) {
                    let index = plan
                        .kept_axes
                        .iter()
                        .zip(&strides)
                        .map(|(&axis, step)| (position[axis] + index.slice()[axis]) * step)
                        .sum();
                    samples.push(Sample { index, value });
                }
            }
        })?;
        // CASA discovers the first pixel in each island with the final
        // axis varying fastest, regardless of the native tile layout.
        samples.sort_unstable_by_key(|sample| sample.index);
        Ok(Self {
            shape,
            strides,
            samples,
        })
    }

    pub(super) fn rank(&self) -> usize {
        self.shape.len()
    }

    pub(super) fn coordinate(&self, index: usize, axis: usize) -> usize {
        index / self.strides[axis] % self.shape[axis]
    }

    pub(super) fn position(&self, point: usize) -> Vec<usize> {
        let index = self.samples[point].index;
        (0..self.rank())
            .map(|axis| self.coordinate(index, axis))
            .collect()
    }

    pub(super) fn neighbors<'a>(
        &'a self,
        point: usize,
        offsets: &'a [[isize; 3]],
    ) -> impl Iterator<Item = usize> + 'a {
        let index = self.samples[point].index;
        let current: [usize; 3] = std::array::from_fn(|axis| {
            if axis < self.rank() {
                self.coordinate(index, axis)
            } else {
                0
            }
        });
        offsets.iter().copied().filter_map(move |offset| {
            let mut adjacent = index;
            for (axis, shift) in offset.into_iter().enumerate().take(self.rank()) {
                let to = current[axis].checked_add_signed(shift)?;
                if to >= self.shape[axis] {
                    return None;
                }
                adjacent = adjacent.checked_add_signed(shift * self.strides[axis] as isize)?;
            }
            self.samples
                .binary_search_by_key(&adjacent, |sample| sample.index)
                .ok()
        })
    }

    pub(super) fn bounds(&self, region: &[usize]) -> (Vec<usize>, Vec<usize>) {
        let mut start = self.shape.clone();
        let mut stop = vec![0; self.rank()];
        for &point in region {
            for axis in 0..self.rank() {
                let value = self.coordinate(self.samples[point].index, axis);
                start[axis] = start[axis].min(value);
                stop[axis] = stop[axis].max(value + 1);
            }
        }
        (start, stop)
    }
}

pub(super) fn pixel_strides(shape: &[usize]) -> Result<Vec<usize>, ImageError> {
    let Some(total) = shape
        .iter()
        .try_fold(1usize, |n, &d| n.checked_mul(d))
        .filter(|&n| n <= isize::MAX as usize)
    else {
        return Err(ImageError::InvalidMetadata(
            "Decomposition pixel index exceeds the supported range".into(),
        ));
    };
    let mut suffix = total;
    Ok(shape
        .iter()
        .map(|&length| {
            suffix /= length;
            suffix
        })
        .collect())
}

pub(super) fn visit_selection(
    reader: &PixelSections<'_, f32>,
    plan: &SubimagePlan,
    mut visit: impl FnMut(&[usize], ArrayD<f32>, Option<ArrayD<bool>>),
) -> Result<(), ImageError> {
    let packed_shape = plan.selection.packed_shape();
    let unit = vec![1; packed_shape.len()];
    let selected = StridedSection {
        start: &plan.selection.start,
        shape: &packed_shape,
        stride: plan.selection.stride.as_deref().unwrap_or(&unit),
    };
    let cursor = bounded_read_cursor(selected.dense_shape(&packed_shape))?;
    for tile in selected.cursors(cursor) {
        let tile = tile?;
        let (data, valid) = reader.read(
            &selected.source_start(&tile.position),
            &tile.shape,
            selected.stride,
        )?;
        visit(&tile.position, data, valid);
    }
    Ok(())
}

pub(super) fn neighborhood(rank: usize, naxis: usize) -> Vec<[isize; 3]> {
    // In the CASA bounds-only path for a higher-rank cube, neighbors remain
    // in the first two axes of each independent plane.
    let mut result = Vec::new();
    for x in -1_isize..=1 {
        for y in -1_isize..=1 {
            for z in -1_isize..=1 {
                let steps = x.unsigned_abs() + y.unsigned_abs() + z.unsigned_abs();
                if steps > 0 && steps <= naxis && (rank >= 2 || y == 0) && (rank == 3 || z == 0) {
                    result.push([x, y, z]);
                }
            }
        }
    }
    result
}

pub(super) fn connected(
    pixels: &Pixels,
    points: impl IntoIterator<Item = usize>,
    offsets: &[[isize; 3]],
    accept: impl Fn(usize) -> bool,
    visited: &mut [bool],
) -> Vec<Vec<usize>> {
    let mut regions = Vec::new();
    for point in points {
        if visited[point] || !accept(point) {
            continue;
        }
        visited[point] = true;
        let mut region = vec![point];
        let mut next = 0;
        while next < region.len() {
            for neighbor in pixels.neighbors(region[next], offsets) {
                if !visited[neighbor] && accept(neighbor) {
                    visited[neighbor] = true;
                    region.push(neighbor);
                }
            }
            next += 1;
        }
        region.sort_unstable();
        regions.push(region);
    }
    regions
}
