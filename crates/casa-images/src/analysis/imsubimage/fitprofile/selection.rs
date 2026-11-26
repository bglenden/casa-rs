// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateType;
use casa_core::{ArrayD, RecordValue};
use casa_lattices::{TraversalCursorIter, TraversalSpec};
use ndarray::{Axis, AxisDescription, Slice, Zip};

use crate::analysis::axes::stokes_for_axis;
use crate::analysis::lel_mask::PreparedMask;
use crate::{ImageError, ImageInterface};

use super::super::SubimagePlan;
use super::super::read_window::PixelSections;
use super::super::region::SubimageSelection;
use super::ProfileRegion;

pub(in crate::analysis) struct ProfileSelection {
    pub(in crate::analysis) plan: SubimagePlan,
    pub(super) axis: usize,
    pub(super) row_shape: Vec<usize>,
    pub(super) stride: Vec<usize>,
    pub(in crate::analysis) lanes: usize,
}

impl ProfileSelection {
    pub(super) fn new(
        input: &dyn ImageInterface<f32>,
        coordinates: RecordValue,
        region: ProfileRegion<'_>,
        axis: i64,
        beam_record: Option<&RecordValue>,
    ) -> Result<Self, ImageError> {
        let axis = if axis < 0 {
            input.find_axis(CoordinateType::Spectral).unwrap_or(0)
        } else {
            axis as usize
        };
        if axis >= input.ndim() {
            return Err(ImageError::CasaSelection(format!(
                "Specified fit axis {axis} must be less than the number of image axes ({})",
                input.ndim(),
            )));
        }
        let stokes = region
            .stokes
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let stokes = if stokes.is_none()
            && region.region.is_none()
            && let Some(axis) = input.find_axis(CoordinateType::Stokes)
            && input.shape()[axis] > 1
        {
            stokes_for_axis(input.coordinates(), axis)?
                .first()
                .map(|stokes| stokes.name())
        } else {
            stokes
        };
        let selection = SubimageSelection::resolve_section(
            input,
            region.pixel_box,
            region.region,
            region.chans,
            stokes,
        )?;
        let plan = SubimagePlan::from_selection(
            input,
            Some(coordinates),
            selection,
            false,
            &[],
            beam_record,
        )?;
        Self::from_plan(plan, axis)
    }

    pub(in crate::analysis) fn from_plan(
        plan: SubimagePlan,
        axis: usize,
    ) -> Result<Self, ImageError> {
        let stride = plan
            .selection
            .stride
            .clone()
            .unwrap_or_else(|| vec![1; plan.shape.len()]);
        let mut row_shape = plan.shape.clone();
        row_shape[axis] = 1;
        let lanes = row_shape
            .iter()
            .try_fold(1_usize, |size, &dim| size.checked_mul(dim))
            .ok_or_else(|| ImageError::CasaSelection("Profile grid is too large".into()))?;
        Ok(Self {
            plan,
            axis,
            row_shape,
            stride,
            lanes,
        })
    }

    pub(in crate::analysis) fn reader<'a>(
        &'a self,
        input: &'a dyn ImageInterface<f32>,
        mask: Option<&str>,
        stretch: bool,
        destinations: impl IntoIterator<Item = &'a Path>,
    ) -> Result<ProfileLaneReader<'a>, ImageError> {
        let windows = PixelSections {
            image: input,
            selection: &self.plan.selection,
            mask: PreparedMask::new_for_outputs(
                mask,
                input.shape(),
                input.coordinates(),
                stretch,
                destinations,
            )?,
        };
        let mut geometry = vec![1; self.plan.shape.len()];
        let length = self.plan.shape[self.axis];
        geometry[self.axis] = length;
        // Traverse spatial profiles in pixel order. Keep blocks with complete
        // spectra in memory, up to one million samples or one long spectrum.
        let mut budget = (1_048_576 / length).max(1);
        for (axis, size) in geometry.iter_mut().enumerate() {
            if axis != self.axis {
                *size = self.plan.shape[axis].min(budget);
                budget /= *size;
            }
        }
        Ok(ProfileLaneReader {
            selection: self,
            windows,
            geometry,
            cached: None,
        })
    }

    pub(in crate::analysis) fn position(&self, mut lane: usize) -> Vec<usize> {
        self.row_shape
            .iter()
            .map(|&length| {
                let index = lane % length;
                lane /= length;
                index
            })
            .collect()
    }

    pub(super) fn source_pixel(&self, pixel: &[usize]) -> Vec<usize> {
        pixel
            .iter()
            .zip(&self.plan.selection.start)
            .zip(&self.stride)
            .map(|((&pixel, &start), &stride)| start + pixel * stride)
            .collect()
    }

    pub(super) fn selected_planes(&self, good: &[i64]) -> Result<Vec<bool>, ImageError> {
        if good.iter().any(|&plane| plane < 0) {
            return Err(ImageError::CasaSelection(
                "All planes must be nonnegative".into(),
            ));
        }
        let mut valid = vec![good.is_empty(); self.plan.shape[self.axis]];
        let start = self.plan.selection.start[self.axis];
        let stride = self.stride[self.axis];
        for &plane in good {
            if let Some(offset) = (plane as usize).checked_sub(start)
                && offset.is_multiple_of(stride)
                && let Some(selected) = valid.get_mut(offset / stride)
            {
                *selected = true;
            }
        }
        Ok(valid)
    }
}

pub(in crate::analysis) struct ProfileLaneReader<'a> {
    selection: &'a ProfileSelection,
    windows: PixelSections<'a, f32>,
    geometry: Vec<usize>,
    cached: Option<ProfileBlock>,
}

struct ProfileBlock {
    origin: Vec<usize>,
    pixels: ArrayD<f32>,
    mask: Option<ArrayD<bool>>,
}

impl ProfileLaneReader<'_> {
    pub(in crate::analysis) fn read(&mut self, position: &[usize]) -> Result<Lane, ImageError> {
        let load = self.cached.as_ref().is_none_or(|block| {
            position
                .iter()
                .zip(&block.origin)
                .zip(block.pixels.shape())
                .any(|((&pixel, &origin), &extent)| pixel < origin || pixel - origin >= extent)
        });
        if load {
            let origin: Vec<_> = position
                .iter()
                .zip(&self.geometry)
                .map(|(&pixel, &count)| pixel / count * count)
                .collect();
            self.cached = None;
            self.cached = Some(self.load_block(origin)?);
        }
        let block = self.cached.as_ref().expect("profile block populated");
        let offset: Vec<_> = position
            .iter()
            .zip(&block.origin)
            .map(|(&pixel, &start)| pixel - start)
            .collect();
        let slice = |dimension: AxisDescription| {
            let axis = dimension.axis.index();
            if axis == self.selection.axis {
                Slice::from(..)
            } else {
                let pixel = offset[axis] as isize;
                Slice::new(pixel, Some(pixel + 1), 1)
            }
        };
        let values: Vec<_> = block
            .pixels
            .slice_each_axis(slice)
            .iter()
            .copied()
            .collect();
        let valid = block.mask.as_ref().map_or_else(
            || vec![true; values.len()],
            |mask| mask.slice_each_axis(slice).iter().copied().collect(),
        );
        Ok(Lane {
            values,
            valid,
            has_mask: block.mask.is_some(),
        })
    }

    fn load_block(&self, origin: Vec<usize>) -> Result<ProfileBlock, ImageError> {
        let shape: Vec<_> = self
            .selection
            .plan
            .shape
            .iter()
            .zip(&origin)
            .zip(&self.geometry)
            .map(|((&extent, &start), &width)| width.min(extent - start))
            .collect();
        let start = self.selection.source_pixel(&origin);
        let (pixels, mask) = self.windows.read(&start, &shape, &self.selection.stride)?;
        Ok(ProfileBlock {
            origin,
            pixels,
            mask,
        })
    }

    pub(super) fn mean(&mut self) -> Result<Lane, ImageError> {
        let axis = self.selection.axis;
        let length = self.selection.plan.shape[axis];
        let mut mean = MeanLane::new(length);
        let mut cursor = self.geometry.clone();
        cursor[axis] = 1;
        for tile in TraversalCursorIter::new(
            self.selection.row_shape.clone(),
            cursor,
            TraversalSpec::tiles(),
        ) {
            let block = self.load_block(tile?.position)?;
            mean.add_block(&block, axis);
        }
        Ok(mean.finish())
    }
}

pub(in crate::analysis) struct Lane {
    pub(in crate::analysis) values: Vec<f32>,
    pub(in crate::analysis) valid: Vec<bool>,
    pub(super) has_mask: bool,
}

pub(super) struct MeanLane {
    sums: Vec<f64>,
    count: Vec<usize>,
}

impl MeanLane {
    pub(super) fn new(length: usize) -> Self {
        Self {
            sums: vec![0.; length],
            count: vec![0; length],
        }
    }

    pub(super) fn add(&mut self, lane: &Lane) {
        for (index, (&value, &valid)) in lane.values.iter().zip(&lane.valid).enumerate() {
            if valid {
                self.sums[index] += f64::from(value);
                self.count[index] += 1;
            }
        }
    }

    fn add_block(&mut self, block: &ProfileBlock, axis: usize) {
        let axis = Axis(axis);
        for (channel, values) in block.pixels.axis_iter(axis).enumerate() {
            let (sum, count) = if let Some(mask) = &block.mask {
                Zip::from(values).and(mask.index_axis(axis, channel)).fold(
                    (0_f64, 0_usize),
                    |(sum, count), &value, &valid| {
                        if valid {
                            (sum + f64::from(value), count + 1)
                        } else {
                            (sum, count)
                        }
                    },
                )
            } else {
                (
                    values.iter().map(|&value| f64::from(value)).sum(),
                    values.len(),
                )
            };
            self.sums[channel] += sum;
            self.count[channel] += count;
        }
    }

    pub(super) fn finish(self) -> Lane {
        let valid: Vec<_> = self.count.iter().map(|&count| count > 0).collect();
        let has_mask = valid.iter().any(|&value| !value);
        let values = self
            .sums
            .into_iter()
            .zip(self.count)
            .map(|(sum, count)| {
                if count > 0 {
                    (sum / count as f64) as f32
                } else {
                    0.
                }
            })
            .collect();
        Lane {
            values,
            valid,
            has_mask,
        }
    }
}
