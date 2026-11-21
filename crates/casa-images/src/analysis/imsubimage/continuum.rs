// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::CoordinateType;
use casa_types::{ArrayD, RecordValue};
use ndarray::{Axis, IxDyn, Zip};

use crate::{ImageError, ImageInterface, MutableImageInterface};

use super::super::axes::StridedSection;
use super::read_window::PixelSections;
use super::{SubimageOptions, SubimagePlan, SubimageRegion};

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
        let section = SubimagePlan::new_with_beams(
            image,
            Some(record),
            SubimageOptions {
                region,
                dropdeg: false,
                keepaxes: &[],
            },
            Some(beam_record),
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
        let fit_slots = coefficients.saturating_mul(coefficients + 1);
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
            let mut fits = (0..count)
                .map(|_| PolynomialFit::new(coefficients))
                .collect::<Result<Vec<_>, _>>()?;
            let mut position = tile.position;
            let mut shape = tile.shape;
            for first in (0..length).step_by(axial_block) {
                position[axis] = first;
                shape[axis] = axial_block.min(length - first);
                let (pixels, mask) =
                    reader.read(&input.source_start(&position), &shape, input.stride)?;
                let validity = mask.unwrap_or_else(|| ArrayD::from_elem(IxDyn(&shape), true));
                for ((pixels, valid), fit) in pixels
                    .lanes(Axis(axis))
                    .into_iter()
                    .zip(validity.lanes(Axis(axis)))
                    .zip(&mut fits)
                {
                    for (offset, (&pixel, &valid)) in pixels.iter().zip(&valid).enumerate() {
                        if valid {
                            fit.has_data = true;
                            let index = first + offset;
                            if self.fit_channels.is_empty()
                                || self.fit_channels.binary_search(&index).is_ok()
                            {
                                if pixel.is_finite() {
                                    fit.add(abscissa(index, length), f64::from(pixel));
                                } else {
                                    fit.nonfinite = true;
                                }
                            }
                        }
                    }
                }
            }
            counts.attempted += fits.iter().filter(|fit| fit.has_data).count();
            counts.valid += fits
                .iter()
                .filter(|fit| fit.has_data && !fit.nonfinite)
                .count();
            let invalidate_fits = fits.iter().any(|fit| fit.nonfinite);
            for fit in &mut fits {
                fit.solve();
            }

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
                        if fit.has_data {
                            for (offset, value) in lane.iter_mut().enumerate() {
                                *value = fit.evaluate(abscissa(first + offset, length)) as f32;
                            }
                        }
                    }
                }
                for (mut lane, fit) in pixels.lanes_mut(Axis(axis)).into_iter().zip(&fits) {
                    if fit.has_data {
                        for (offset, value) in lane.iter_mut().enumerate() {
                            *value = (f64::from(*value)
                                - fit.evaluate(abscissa(first + offset, length)))
                                as f32;
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
                        if fit.nonfinite {
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
}

fn abscissa(index: usize, length: usize) -> f64 {
    2.0 * index as f64 / (length - 1) as f64 - 1.0
}

// Update a QR factorization one data row at a time. Chebyshev polynomials
// span the same fits as powers of the pixel index with better conditioning.
struct PolynomialFit {
    size: usize,
    matrix: Vec<f64>,
    rhs: Vec<f64>,
    row: Vec<f64>,
    count: usize,
    has_data: bool,
    nonfinite: bool,
}

impl PolynomialFit {
    fn new(size: usize) -> Result<Self, ImageError> {
        let total = size
            .checked_mul(size)
            .ok_or_else(|| invalid("Polynomial fit storage is too large"))?;
        let mut matrix = Vec::new();
        matrix
            .try_reserve_exact(total)
            .map_err(|_| invalid("Polynomial fit storage is too large"))?;
        matrix.resize(total, 0.0);
        Ok(Self {
            size,
            matrix,
            rhs: vec![0.0; size],
            row: vec![0.0; size],
            count: 0,
            has_data: false,
            nonfinite: false,
        })
    }

    fn add(&mut self, x: f64, mut value: f64) {
        self.count += 1;
        let mut previous = 1.0;
        let mut current = x;
        for element in &mut self.row {
            *element = previous;
            (previous, current) = (current, 2.0 * x * current - previous);
        }
        for index in 0..self.size {
            let pivot = index * self.size + index;
            let norm = self.matrix[pivot].hypot(self.row[index]);
            if norm == 0.0 {
                continue;
            }
            let cosine = self.matrix[pivot] / norm;
            let sine = self.row[index] / norm;
            self.matrix[pivot] = norm;
            let tail = &mut self.matrix[pivot + 1..(index + 1) * self.size];
            for (t, b) in tail.iter_mut().zip(&mut self.row[index + 1..]) {
                (*t, *b) = (cosine * *t + sine * *b, -sine * *t + cosine * *b);
            }
            let rhs = &mut self.rhs[index];
            (*rhs, value) = (cosine * *rhs + sine * value, -sine * *rhs + cosine * value);
        }
    }

    fn solve(&mut self) {
        let usable = (0..self.count.min(self.size))
            .take_while(|&index| self.matrix[index * self.size + index] > 1e-12)
            .count();
        self.rhs.truncate(usable);
        for index in (0..usable).rev() {
            let following = &self.rhs[index + 1..];
            let pivot = index * self.size + index;
            let product: f64 = following
                .iter()
                .zip(&self.matrix[pivot + 1..])
                .map(|(&coefficient, &matrix)| coefficient * matrix)
                .sum();
            self.rhs[index] = (self.rhs[index] - product) / self.matrix[pivot];
        }
    }

    fn evaluate(&self, x: f64) -> f64 {
        if self.nonfinite {
            return f64::NAN;
        }
        let mut previous = 1.0;
        let mut current = x;
        let mut value = 0.0;
        for &coefficient in &self.rhs {
            value += coefficient * previous;
            (previous, current) = (current, 2.0 * x * current - previous);
        }
        value
    }
}
