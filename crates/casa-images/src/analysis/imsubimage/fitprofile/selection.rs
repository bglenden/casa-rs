// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateType;
use casa_types::RecordValue;

use crate::analysis::lel_mask::PreparedMask;
use crate::{ImageError, ImageInterface};

use super::super::read_window::PixelSections;
use super::super::region::SubimageSelection;
use super::super::{SubimagePlan, SubimageRegion};

pub(super) struct ProfileSelection {
    pub(super) plan: SubimagePlan,
    pub(super) axis: usize,
    pub(super) row_shape: Vec<usize>,
    pub(super) stride: Vec<usize>,
    pub(super) lanes: usize,
}

impl ProfileSelection {
    pub(super) fn new(
        input: &dyn ImageInterface<f32>,
        coordinates: RecordValue,
        pixel_box: Option<&str>,
        region: Option<SubimageRegion<'_>>,
        channels: Option<&str>,
        stokes: Option<&str>,
        axis: i64,
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
        let selection =
            SubimageSelection::resolve_section(input, pixel_box, region, channels, stokes)?;
        let plan =
            SubimagePlan::from_selection(input, Some(coordinates), selection, false, &[], None)?;
        let stride = plan
            .selection
            .stride
            .clone()
            .unwrap_or_else(|| vec![1; input.ndim()]);
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

    pub(super) fn reader<'a>(
        &'a self,
        input: &'a dyn ImageInterface<f32>,
        mask: Option<&str>,
        stretch: bool,
    ) -> Result<PixelSections<'a, f32>, ImageError> {
        Ok(PixelSections {
            image: input,
            selection: &self.plan.selection,
            mask: PreparedMask::new(mask, input.shape(), input.coordinates(), stretch)?,
        })
    }

    pub(super) fn position(&self, mut lane: usize) -> Vec<usize> {
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

    pub(super) fn read_lane(
        &self,
        source: &PixelSections<'_, f32>,
        position: &[usize],
    ) -> Result<Lane, ImageError> {
        let mut count = vec![1; self.plan.shape.len()];
        count[self.axis] = self.plan.shape[self.axis];
        let (values, valid) = source.read(&self.source_pixel(position), &count, &self.stride)?;
        let values: Vec<_> = values.iter().copied().collect();
        let valid = valid.map_or_else(
            || vec![true; values.len()],
            |mask| mask.iter().copied().collect(),
        );
        Ok(Lane { values, valid })
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

pub(super) struct Lane {
    pub(super) values: Vec<f32>,
    pub(super) valid: Vec<bool>,
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

    pub(super) fn finish(self) -> Lane {
        let valid = self.count.iter().map(|&count| count > 0).collect();
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
        Lane { values, valid }
    }
}
