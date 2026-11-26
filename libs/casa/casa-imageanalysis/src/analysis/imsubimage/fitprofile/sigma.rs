// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::ArrayD;
use ndarray::IxDyn;

use crate::analysis::axes::StridedSection;
use casa_images::bounded_read_cursor;
use casa_images::{ImageError, ImageInterface};

use super::selection::{Lane, ProfileSelection};

/// Standard deviations for a spectral profile objective.
#[derive(Clone, Copy)]
pub enum ProfileSigma<'a> {
    /// Owned by the caller as a channel vector, broadcast array or full array.
    Values(&'a ArrayD<f32>),
    /// Native sigma image with full or broadcast geometry.
    Image(&'a dyn ImageInterface<f32>),
}

pub(super) struct Sigma<'a> {
    source: ProfileSigma<'a>,
    broadcast: bool,
    maximum: f32,
}

impl<'a> Sigma<'a> {
    pub(super) fn new(
        input: ProfileSigma<'a>,
        source_shape: &[usize],
        axis: usize,
    ) -> Result<Self, ImageError> {
        let sigma_shape = input.shape();
        let broadcast = sigma_shape != source_shape;
        if broadcast {
            let count = source_shape[axis];
            if sigma_shape.len() == 1 {
                if sigma_shape[0] != count {
                    return Err(invalid(
                        "A one dimensional standard deviation spectrum must have the same number of pixels as the input image has along its axis to be fit",
                    ));
                }
            } else {
                let mut expected = vec![1; source_shape.len()];
                expected[axis] = count;
                if sigma_shape != expected {
                    return Err(invalid(format!(
                        "If the shape of the standard deviation image differs from the input, the standard deviation image must have shape {expected:?}",
                    )));
                }
            }
        }
        let mut maximum: f32 = 0.;
        let mut include = |value| {
            if value < 0. {
                return Err(invalid("All sigma values must be non-negative"));
            }
            maximum = maximum.max(value);
            Ok(())
        };
        match &input {
            ProfileSigma::Values(values) => values.iter().copied().try_for_each(include)?,
            ProfileSigma::Image(image) => {
                let start = vec![0; sigma_shape.len()];
                let stride = vec![1; sigma_shape.len()];
                let section = StridedSection {
                    start: &start,
                    shape: sigma_shape,
                    stride: &stride,
                };
                for tile in section.cursors(bounded_read_cursor(sigma_shape.to_vec())?) {
                    let tile = tile?;
                    image
                        .get_slice(&tile.position, &tile.shape, &stride)?
                        .iter()
                        .copied()
                        .try_for_each(&mut include)?;
                }
            }
        }
        Ok(Self {
            source: input,
            broadcast,
            maximum,
        })
    }

    pub(super) fn full_size_image(&self) -> Option<&dyn ImageInterface<f32>> {
        match self.source {
            ProfileSigma::Image(image) if !self.broadcast => Some(image),
            _ => None,
        }
    }

    pub(super) fn lane(
        &self,
        selection: &ProfileSelection,
        position: &[usize],
    ) -> Result<Lane, ImageError> {
        let axis = selection.axis;
        let length = selection.plan.shape[axis];
        let absolute = selection.source_pixel(position);
        self.read(&absolute, axis, length, selection.stride[axis])
    }

    pub(super) fn read(
        &self,
        absolute: &[usize],
        axis: usize,
        length: usize,
        skip: usize,
    ) -> Result<Lane, ImageError> {
        let start_axis = absolute[axis];
        let mut indices = if self.source.shape().len() == 1 {
            vec![start_axis]
        } else if self.broadcast {
            let mut start = vec![0; absolute.len()];
            start[axis] = start_axis;
            start
        } else {
            absolute.to_vec()
        };
        let sigma_axis = if indices.len() == 1 { 0 } else { axis };
        let mut lanes = match &self.source {
            ProfileSigma::Values(values) => {
                let data = (0..length)
                    .map(|channel| {
                        indices[sigma_axis] = start_axis + channel * skip;
                        values[IxDyn(&indices)]
                    })
                    .collect();
                Lane {
                    values: data,
                    valid: vec![true; length],
                    has_mask: false,
                }
            }
            ProfileSigma::Image(image) => {
                let mut shape = vec![1; indices.len()];
                let mut stride = shape.clone();
                shape[sigma_axis] = length;
                stride[sigma_axis] = skip;
                let values = image
                    .get_slice(&indices, &shape, &stride)?
                    .iter()
                    .copied()
                    .collect();
                let valid = image.get_mask_slice(&indices, &shape, &stride)?;
                let has_mask = valid.is_some();
                let valid =
                    valid.map_or_else(|| vec![true; length], |mask| mask.iter().copied().collect());
                Lane {
                    values,
                    valid,
                    has_mask,
                }
            }
        };
        for (value, valid) in lanes.values.iter_mut().zip(&mut lanes.valid) {
            if self.maximum > 0. && value.is_finite() {
                *value /= self.maximum;
            } else {
                *value = 0.;
                *valid = false;
            }
        }
        Ok(lanes)
    }
}

impl ProfileSigma<'_> {
    fn shape(&self) -> &[usize] {
        match self {
            Self::Values(values) => values.shape(),
            Self::Image(image) => image.shape(),
        }
    }
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
