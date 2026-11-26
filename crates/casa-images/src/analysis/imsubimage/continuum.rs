// SPDX-License-Identifier: LGPL-3.0-or-later

mod polynomial;

use casa_coordinates::CoordinateType;
use casa_core::{ArrayD, RecordValue};
use ndarray::{Axis, IxDyn, Zip};

use crate::{ImageError, ImageInterface, MutableImageInterface};

use super::super::axes::StridedSection;
use super::read_window::PixelSections;
use super::{SubimageOptions, SubimagePlan, SubimageRegion};
use polynomial::{Coverage, Fit, Prediction};

const MAX_TILE_SAMPLES: usize = 256 * 1024;

/// Polynomial continuum subtraction along an image's spectral axis.
pub struct ImageContinuum {
    /// Geometry and metadata of the selected image output.
    pub section: SubimagePlan,
    axis: usize,
    order: usize,
    fit_channels: Vec<usize>,
}

/// Counts of the selected profiles and of the profiles eligible for fitting.
#[derive(Default)]
pub struct ContinuumSummary {
    /// Number of spectral profiles in the selected image.
    pub profiles: usize,
    /// Number of profiles with a valid input pixel.
    pub attempted: usize,
    /// Number of fits with finite fitted parameters.
    pub valid: usize,
}

fn invalid(reason: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(reason.into())
}

impl ImageContinuum {
    /// Resolve an image's spectral selection and the allowed fit order.
    ///
    /// # Errors
    /// The source has no spectral axis, the region is invalid, or the order
    /// and channel request cannot describe a fit.
    pub fn new(
        image: &dyn ImageInterface<f32>,
        record: RecordValue,
        region: Option<SubimageRegion<'_>>,
        channels: &[i64],
        order: i64,
        beam_record: &RecordValue,
    ) -> Result<Self, ImageError> {
        let order =
            usize::try_from(order).map_err(|_| invalid("Polynomial order cannot be negative"))?;
        let axis = image
            .find_axis(CoordinateType::Spectral)
            .ok_or_else(|| invalid("This image has no spectral axis"))?;
        let section = SubimagePlan::new_preserving_beams(
            image,
            Some(record),
            SubimageOptions {
                region,
                dropdeg: false,
                keepaxes: &[],
            },
            beam_record,
        )?;
        let length = section.shape[axis];
        if order >= length - 1 {
            return Err(invalid(format!(
                "There are not enough points ({length}) along the fit axis to fit {} unknowns",
                order.saturating_add(1),
            )));
        }
        let fit_channels = if channels == [-1] {
            Vec::new()
        } else {
            let origin = section.selection.start[axis];
            let mut results = Vec::new();
            for &channel in channels {
                let channel = usize::try_from(channel)
                    .map_err(|_| invalid("All planes must be nonnegative"))?;
                if let Some(local) = channel.checked_sub(origin) {
                    results.push(local);
                }
            }
            results.sort_unstable();
            results.dedup();
            results
        };
        Ok(Self {
            section,
            axis,
            order,
            fit_channels,
        })
    }

    /// Fit spectral profiles and stream selected residuals and model values.
    ///
    /// # Errors
    /// Input or output storage failed, or the polynomial work space is too large.
    pub fn copy_to(
        &self,
        source: &dyn ImageInterface<f32>,
        residual: &mut dyn MutableImageInterface<f32>,
        mut model: Option<&mut dyn MutableImageInterface<f32>>,
    ) -> Result<ContinuumSummary, ImageError> {
        self.section.copy_metadata(source, residual)?;
        if let Some(model) = model.as_deref_mut() {
            self.section.copy_metadata(source, model)?;
        }
        let selection = &self.section.selection;
        let axis = self.axis;
        let mut lines = self.section.shape.clone();
        let length = lines[axis];
        lines[axis] = 1;
        let unit = vec![1; lines.len()];
        let input = StridedSection {
            start: &selection.start,
            shape: &self.section.shape,
            stride: selection.stride.as_deref().unwrap_or(&unit),
        };
        let positions = StridedSection {
            start: &vec![0; lines.len()],
            shape: &lines,
            stride: &unit,
        };
        let reader = PixelSections {
            image: source,
            selection,
            mask: None,
        };

        let coefficients = self.order + 1;
        let candidate_count = if self.fit_channels.is_empty() {
            length
        } else {
            self.fit_channels
                .iter()
                .take_while(|&&index| index < length)
                .count()
        };
        let matrix_side = coefficients.min(candidate_count);
        let fit_slots = matrix_side.saturating_mul(matrix_side + 1);
        let mut lane_budget = (MAX_TILE_SAMPLES / fit_slots.max(16)).max(1);
        let cursor: Vec<_> = source
            .nice_cursor_shape()
            .into_iter()
            .enumerate()
            .map(|(index, size)| {
                let stride = input.stride[index];
                let width = size.min(lane_budget).div_ceil(stride).min(lines[index]);
                lane_budget = (lane_budget / ((width - 1) * stride + 1)).max(1);
                width
            })
            .collect();
        let axial_budget = MAX_TILE_SAMPLES
            / cursor
                .iter()
                .enumerate()
                .map(|(index, &width)| (width - 1) * input.stride[index] + 1)
                .product::<usize>();
        let axial_block = axial_budget.div_ceil(input.stride[axis]);
        let mut counts = ContinuumSummary::default();
        let mut has_mask = false;

        for line_tile in positions.cursors(cursor) {
            let tile = line_tile?;
            let count = tile.shape.iter().product::<usize>();
            counts.profiles += count;
            let mut position = tile.position;
            let mut shape = tile.shape;
            let mut coverage = (0..count).map(|_| Coverage::default()).collect::<Vec<_>>();
            self.visit_profiles(
                &reader,
                &input,
                &mut position,
                &mut shape,
                axial_block,
                |lane, index, value| {
                    coverage[lane].observe(index, value, self.selected(index));
                },
            )?;
            let mut fits = coverage
                .into_iter()
                .map(|coverage| coverage.prepare(coefficients))
                .collect::<Result<Vec<_>, _>>()?;
            if fits.iter().any(Fit::needs_samples) {
                self.visit_profiles(
                    &reader,
                    &input,
                    &mut position,
                    &mut shape,
                    axial_block,
                    |lane, index, value| {
                        if self.selected(index) {
                            fits[lane].add(index, value);
                        }
                    },
                )?;
            }
            let fits: Vec<_> = fits.into_iter().map(Fit::solve).collect();
            counts.attempted += fits
                .iter()
                .filter(|fit| !matches!(fit, Prediction::Input))
                .count();
            counts.valid += fits
                .iter()
                .filter(|fit| !matches!(fit, Prediction::Input | Prediction::Invalid))
                .count();
            let invalidate_fits = fits.iter().any(|fit| matches!(fit, Prediction::Invalid));

            for first in (0..length).step_by(axial_block) {
                position[axis] = first;
                shape[axis] = axial_block.min(length - first);
                let (mut pixels, mask) =
                    reader.read(&input.source_start(&position), &shape, input.stride)?;
                let mut predictions = model.is_some().then(|| ArrayD::default(IxDyn(&shape)));
                if let Some(predictions) = &mut predictions {
                    Zip::from(predictions.lanes_mut(Axis(axis)))
                        .and(pixels.lanes(Axis(axis)))
                        .for_each(|mut model, input| model.assign(&input));
                    for (mut lane, fit) in predictions.lanes_mut(Axis(axis)).into_iter().zip(&fits)
                    {
                        if !matches!(fit, Prediction::Input) {
                            for (offset, value) in lane.iter_mut().enumerate() {
                                *value = fit.evaluate(first + offset) as f32;
                            }
                        }
                    }
                }
                for (mut lane, fit) in pixels.lanes_mut(Axis(axis)).into_iter().zip(&fits) {
                    if !matches!(fit, Prediction::Input) {
                        for (offset, value) in lane.iter_mut().enumerate() {
                            *value = (f64::from(*value) - fit.evaluate(first + offset)) as f32;
                        }
                    }
                }
                residual.put_slice(&pixels, &position)?;
                if let Some(model) = model.as_deref_mut() {
                    model.put_slice(predictions.as_ref().expect("model output"), &position)?;
                }
                let mut mask = mask;
                if invalidate_fits {
                    let validity =
                        mask.get_or_insert_with(|| ArrayD::from_elem(IxDyn(&shape), true));
                    for (mut lane, fit) in validity.lanes_mut(Axis(axis)).into_iter().zip(&fits) {
                        if matches!(fit, Prediction::Invalid) {
                            lane.fill(false);
                        }
                    }
                }
                if let Some(mask) = mask.filter(|mask| mask.iter().any(|&value| !value)) {
                    if !has_mask {
                        residual.create_filled_mask("mask0", true)?;
                        residual.set_default_mask("mask0")?;
                        if let Some(model) = model.as_deref_mut() {
                            model.create_filled_mask("mask0", true)?;
                            model.set_default_mask("mask0")?;
                        }
                        has_mask = true;
                    }
                    residual.put_mask_slice("mask0", &mask, &position)?;
                    if let Some(model) = model.as_deref_mut() {
                        model.put_mask_slice("mask0", &mask, &position)?;
                    }
                }
            }
        }
        Ok(counts)
    }

    fn selected(&self, index: usize) -> bool {
        self.fit_channels.is_empty() || self.fit_channels.binary_search(&index).is_ok()
    }

    fn visit_profiles(
        &self,
        reader: &PixelSections<'_, f32>,
        input: &StridedSection<'_>,
        position: &mut [usize],
        shape: &mut [usize],
        block: usize,
        mut visit: impl FnMut(usize, usize, f32),
    ) -> Result<(), ImageError> {
        let axis = self.axis;
        let length = self.section.shape[axis];
        for first in (0..length).step_by(block) {
            position[axis] = first;
            shape[axis] = block.min(length - first);
            let (pixels, mask) = reader.read(&input.source_start(position), shape, input.stride)?;
            let valid = mask.unwrap_or_else(|| ArrayD::from_elem(IxDyn(shape), true));
            for (line, (pixels, valid)) in pixels
                .lanes(Axis(axis))
                .into_iter()
                .zip(valid.lanes(Axis(axis)))
                .enumerate()
            {
                for (offset, (&value, &valid)) in pixels.iter().zip(&valid).enumerate() {
                    if valid {
                        visit(line, first + offset, value);
                    }
                }
            }
        }
        Ok(())
    }
}
