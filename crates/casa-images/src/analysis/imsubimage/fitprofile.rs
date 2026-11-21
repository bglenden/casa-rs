// SPDX-License-Identifier: LGPL-3.0-or-later

//! Fits one-dimensional spectral models along an image pixel axis.

mod axis;
mod curve;
mod estimates;
mod models;
mod products;
mod results;
mod selection;
mod sigma;
mod solver;

use std::path::Path;

use casa_types::{
    RecordValue,
    quanta::{Quantity, Unit},
};
use ndarray::IxDyn;

use crate::{ImageError, ImageInterface};

use super::{SubimageRegion, read_window::PixelSections};
use axis::{ProfileAxis, automatic_divisor};
use curve::Curve;
use models::{PeakLists, PolynomialEstimate, ProfileModels, SpectralPolynomial};
use products::{Product, ProfileProducts};
use results::ProfileResults;
use selection::{Lane, MeanLane, ProfileSelection};
pub use sigma::ProfileSigma;
use sigma::Sigma;
use solver::{Sample, Solution};

/// Pixel selection and fit controls for a set of image profiles.
pub struct ProfileFitOptions<'a> {
    /// Pixel box, image region, channel expression and Stokes selection.
    pub selection: ProfileRegion<'a>,
    /// Zero-based pixel axis, or a negative value to choose the input spectral axis.
    pub axis: i64,
    /// Additional LEL mask expression.
    pub mask: Option<&'a str>,
    /// Enable broadcasting of compatible expression masks.
    pub stretch: bool,
    /// Fit each selected spatial profile independently, otherwise fit their mean.
    pub multifit: bool,
    /// Minimum number of unmasked pixels to attempt a profile on a masked image.
    pub min_points: usize,
    /// Automatically seed at most this many Gaussian lines if estimates are absent.
    pub gaussians: usize,
    /// Optional continuum polynomial order.
    pub polynomial: Option<usize>,
    /// File containing comma-separated Gaussian peak, center, width and fixed flags.
    pub estimates_file: Option<&'a Path>,
    /// Supplied line, multiplet and spectral polynomial estimates.
    pub estimates: ProfileEstimates<'a>,
    /// Accepted ranges of amplitude, local pixel center and pixel FWHM.
    pub good_ranges: [Option<[f64; 2]>; 3],
    /// Original image pixel indices along the fit axis to include in the fit.
    pub planes: &'a [i64],
    /// Normalization divisor for a world-coordinate spectral law, or automatic.
    pub divisor: ProfileDivisor,
    /// Optional sigma image or array, broadcast along the other axes if needed.
    pub sigma: Option<ProfileSigma<'a>>,
    /// Additional artifact paths that must remain distinct from input mask operands.
    pub other_artifact: Option<&'a Path>,
    /// Attachment-local restoring beams, if different from the native table metadata.
    pub beam_record: Option<&'a RecordValue>,
}

/// Region expression applied before fitting profiles.
#[derive(Default)]
pub struct ProfileRegion<'a> {
    /// Inclusive direction pixel box.
    pub pixel_box: Option<&'a str>,
    /// Region record or supported region expression.
    pub region: Option<SubimageRegion<'a>>,
    /// Selected spectral channels.
    pub chans: Option<&'a str>,
    /// Selected Stokes planes.
    pub stokes: Option<&'a str>,
}

/// Abscissa normalization used by power-log and log-transformed models.
#[derive(Clone, Default)]
pub enum ProfileDivisor {
    /// Derive the divisor from the range of the selected coordinate.
    #[default]
    Automatic,
    /// Positive value in the fit axis's native units.
    Number(f64),
    /// A physical quantity convertible to the fit axis's native units.
    Quantity(Quantity),
}

/// Initial parameters for Gaussian or Lorentzian singlets and Gaussian multiplets.
#[derive(Clone, Copy, Default)]
pub struct ProfilePeaks<'a> {
    /// One peak value per component.
    pub amplitudes: &'a [f64],
    /// One center in selected profile pixels per component.
    pub centers: &'a [f64],
    /// One full width at half maximum, in pixels, per component.
    pub widths: &'a [f64],
    /// Per-component fixed flags using p, c and f for the three parameters.
    pub fixed: &'a [String],
}

impl<'a> From<ProfilePeaks<'a>> for PeakLists<'a> {
    fn from(peaks: ProfilePeaks<'a>) -> Self {
        Self {
            amplitudes: peaks.amplitudes,
            centers: peaks.centers,
            widths: peaks.widths,
            fixed: peaks.fixed,
        }
    }
}

/// Supplied profile model families and parameter constraints.
#[derive(Clone, Copy, Default)]
pub struct ProfileEstimates<'a> {
    /// Independent peaks.
    pub singlets: ProfilePeaks<'a>,
    /// One Gaussian or Lorentzian function name per singlet.
    pub functions: &'a [String],
    /// Parameters of all Gaussian multiplet components.
    pub multiplets: ProfilePeaks<'a>,
    /// Component counts for each multiplet.
    pub multiplet_sizes: &'a [i64],
    /// Multiplet amplitude ratios, center offsets and width ratios to the first line.
    pub multiplet_ties: [&'a [f64]; 3],
    /// `plp` for power-log or `ltp` for log-transformed polynomial.
    pub spectral_kind: &'a str,
    /// Initial spectral polynomial coefficients.
    pub spectral_values: &'a [f64],
    /// One fixed flag for each spectral polynomial coefficient.
    pub spectral_fixed: &'a [bool],
}

/// Destination names for the native products of a profile fit.
#[derive(Clone, Copy, Default)]
pub struct ProfileOutputNames<'a> {
    /// Model and residual image paths.
    pub spectra: [&'a str; 2],
    /// Base names for amplitude, amplitude error, center, center error, width,
    /// width error, integral and integral error images.
    pub lines: [&'a str; 8],
    /// Base names for spectral polynomial coefficient and coefficient error images.
    pub coefficients: [&'a str; 2],
    /// Output name for the normalized, broadcast sigma image.
    pub sigma: &'a str,
}

/// An image profile request with resolved axes, model representation and output names.
pub struct ImageProfileFit<'a> {
    source: &'a dyn ImageInterface<f32>,
    original_coordinates: RecordValue,
    options: ProfileFitOptions<'a>,
    selection: ProfileSelection,
    models: ProfileModels,
    automatic: usize,
    products: Vec<Product>,
}

impl<'a> ImageProfileFit<'a> {
    /// Validate the selected geometry and estimates and enumerate possible products.
    ///
    /// # Errors
    /// An input selection, axis, model or estimate is invalid or unsupported.
    pub fn new(
        source: &'a dyn ImageInterface<f32>,
        coordinates: RecordValue,
        mut options: ProfileFitOptions<'a>,
        names: ProfileOutputNames<'_>,
    ) -> Result<Self, ImageError> {
        let region = &mut options.selection;
        let selection = ProfileSelection::new(
            source,
            coordinates.clone(),
            region.pixel_box,
            region.region.take(),
            region.chans,
            region.stokes,
            options.axis,
            options.beam_record,
        )?;
        let models = models(&mut options)?;
        if models.spectral_polynomial.is_some()
            && (options.polynomial.is_some()
                || !models.singlets.is_empty()
                || !models.multiplets.is_empty())
        {
            return Err(invalid(
                "A spectral polynomial cannot be fit simultaneously with other functions",
            ));
        }
        let automatic = if models.singlets.is_empty()
            && models.multiplets.is_empty()
            && models.spectral_polynomial.is_none()
            && options.estimates_file.is_none()
        {
            options.gaussians
        } else {
            0
        };
        let length = selection.plan.shape[selection.axis];
        let unknowns = automatic.saturating_mul(3).saturating_add(
            options
                .polynomial
                .map_or(0, |order| order.saturating_add(1)),
        );
        if unknowns >= length {
            return Err(invalid(format!(
                "There are not enough points ({length}) along the fit axis to fit {unknowns} unknowns"
            )));
        }
        if unknowns == 0
            && models.singlets.is_empty()
            && models.multiplets.is_empty()
            && models.spectral_polynomial.is_none()
        {
            return Err(invalid(
                "There is nothing to fit. Specify a line or polynomial model",
            ));
        }
        let products = products::destinations(
            &names,
            &models,
            automatic,
            options.sigma.is_some(),
            options.multifit,
        );
        Ok(Self {
            source,
            original_coordinates: coordinates,
            options,
            selection,
            models,
            automatic,
            products,
        })
    }

    /// Names of the possible output products, in the order accepted by [Self::fit].
    pub fn destinations(&self) -> impl Iterator<Item = &str> {
        self.products.iter().map(|product| product.name.as_str())
    }

    /// Fit the selected profiles and write requested output products to staged paths.
    ///
    /// Returns the result record and its five fit count history messages.
    /// Some solution images are absent when no profile converges. Model, residual,
    /// and sigma products are filled in bounded image sections. The caller publishes
    /// any completed staged products at their requested destinations.
    ///
    /// # Errors
    /// Input reads, mask or sigma preparation, native metadata or output writes failed.
    pub fn fit(self, staged: &[Option<&Path>]) -> Result<(RecordValue, [String; 5]), ImageError> {
        let axis = ProfileAxis::new(&self.selection, self.models.spectral_polynomial.is_some())?;
        let plane_mask = self.selection.selected_planes(self.options.planes)?;
        let reader = self.selection.reader(
            self.source,
            self.options.mask,
            self.options.stretch,
            self.destinations()
                .map(Path::new)
                .chain(self.options.other_artifact),
        )?;
        let sigma = self
            .options
            .sigma
            .map(|input| Sigma::new(input, self.source.shape(), self.selection.axis))
            .transpose()?;
        let mut products = ProfileProducts::new(&self, staged)?;
        let shape = if self.options.multifit {
            self.selection.row_shape.clone()
        } else {
            vec![1; self.source.ndim()]
        };
        let mut results = ProfileResults::new(
            shape,
            &self.models,
            self.automatic,
            self.options.polynomial.is_some(),
        );
        let lanes = if self.options.multifit {
            self.selection.lanes
        } else {
            1
        };
        let mut previous = Vec::<(Vec<usize>, Vec<f64>)>::new();
        let mut attempts = 0;
        for lane_number in 0..lanes {
            let position = self.selection.position(lane_number);
            let world_pixel = self.world_pixel(&position);
            let (data, weights) = self.read_fit_lane(&reader, sigma.as_ref(), &position)?;
            let outcome = self.fit_lane(
                &axis,
                &position,
                &world_pixel,
                &data,
                weights.as_ref(),
                &plane_mask,
                &previous,
            )?;
            if let Some(outcome) = &outcome {
                attempts += 1;
                results.attempted[IxDyn(&position)] = outcome.attempted;
                results.direction(&position, axis.direction(&world_pixel));
                if let Some((curve, solution)) = &outcome.solution {
                    results.record_fit(
                        &position,
                        &world_pixel,
                        &axis,
                        curve,
                        solution,
                        outcome.valid,
                    )?;
                    if outcome.valid
                        && self.options.multifit
                        && self.automatic == 0
                        && self.has_supplied_models()
                    {
                        previous.push((position.clone(), solution.values.clone()));
                    }
                }
            }
            products.write_spectrum(&position, self.selection.axis, &data, outcome.as_ref())?;
        }
        let count = |flags: &casa_types::ArrayD<bool>| flags.iter().filter(|&&flag| flag).count();
        let succeeded = count(&results.succeeded);
        let converged = count(&results.converged);
        let valid = count(&results.valid);
        let summary = [
            format!("Number of profiles       = {lanes}"),
            format!("Number of fits attempted = {attempts}"),
            format!("Number succeeded         = {succeeded}"),
            format!("Number converged         = {converged}"),
            format!("Number valid             = {valid}"),
        ];
        let record = results.finish(&axis.unit, self.source.units(), axis.has_direction());
        products.finish(
            &self,
            staged,
            &record,
            converged > 0,
            sigma.as_ref(),
            &axis.unit,
        )?;
        Ok((record, summary))
    }

    fn world_pixel(&self, position: &[usize]) -> Vec<f64> {
        position
            .iter()
            .enumerate()
            .map(|(axis, &pixel)| {
                if self.options.multifit || axis == self.selection.axis {
                    pixel as f64
                } else {
                    (self.selection.plan.shape[axis] - 1) as f64 * 0.5
                }
            })
            .collect()
    }

    fn read_fit_lane(
        &self,
        reader: &PixelSections<'_, f32>,
        sigma: Option<&Sigma<'_>>,
        position: &[usize],
    ) -> Result<(Lane, Option<Lane>), ImageError> {
        if self.options.multifit {
            let data = self.selection.read_lane(reader, position)?;
            let weights = sigma
                .map(|sigma| sigma.lane(&self.selection, position))
                .transpose()?;
            return Ok((data, weights));
        }
        let length = self.selection.plan.shape[self.selection.axis];
        let mut mean = MeanLane::new(length);
        let mut mean_sigma = sigma.map(|_| MeanLane::new(length));
        for lane in 0..self.selection.lanes {
            let lane_pixel = self.selection.position(lane);
            mean.add(&self.selection.read_lane(reader, &lane_pixel)?);
            if let (Some(sigma), Some(mean_sigma)) = (sigma, &mut mean_sigma) {
                let mut weights = sigma.lane(&self.selection, &lane_pixel)?;
                for (valid, &value) in weights.valid.iter_mut().zip(&weights.values) {
                    *valid &= value > 0.;
                }
                mean_sigma.add(&weights);
            }
        }
        Ok((mean.finish(), mean_sigma.map(MeanLane::finish)))
    }

    fn has_supplied_models(&self) -> bool {
        !self.models.singlets.is_empty()
            || !self.models.multiplets.is_empty()
            || self.models.spectral_polynomial.is_some()
    }

    fn fit_lane(
        &self,
        axis: &ProfileAxis<'_>,
        position: &[usize],
        world_pixel: &[f64],
        data: &Lane,
        weights: Option<&Lane>,
        planes: &[bool],
        previous: &[(Vec<usize>, Vec<f64>)],
    ) -> Result<Option<FitLane>, ImageError> {
        if data.has_mask
            && data.valid.iter().filter(|&&valid| valid).count() < self.options.min_points
        {
            return Ok(None);
        }
        let x = self.abscissa(axis, world_pixel, data.values.len())?;
        let logarithmic = matches!(
            self.models
                .spectral_polynomial
                .as_ref()
                .map(|poly| poly.kind),
            Some(SpectralPolynomial::LogTransformed)
        );
        let mut mask = vec![false; x.len()];
        let mut samples = Vec::with_capacity(x.len());
        for (index, &x) in x.iter().enumerate() {
            let y = f64::from(data.values[index]);
            let y = if logarithmic { y.ln() } else { y };
            let weight = weights.map_or(1., |sigma| {
                if sigma.valid[index] && sigma.values[index] > 0. {
                    f64::from(sigma.values[index]).powi(-2)
                } else {
                    0.
                }
            });
            mask[index] = data.valid[index] && x.is_finite() && y.is_finite();
            if planes[index] && mask[index] && weight > 0. {
                samples.push(Sample { x, y, weight });
            }
        }

        let automatic = estimates::automatic(&data.values, self.automatic);
        let mut curve = Curve::new(&self.models, &automatic, self.options.polynomial);
        if let Some(near) = nearest(position, previous) {
            curve.parameters.copy_from_slice(near);
        }
        let attempted = !curve.parameters.is_empty();
        let solution = attempted.then(|| solver::solve(&curve, &samples)).flatten();
        let valid = solution
            .as_ref()
            .is_some_and(|solution| self.accepts(&curve, solution));
        Ok(Some(FitLane {
            x,
            mask,
            valid,
            attempted,
            logarithmic,
            solution: solution.map(|solution| (curve, solution)),
        }))
    }

    fn abscissa(
        &self,
        axis: &ProfileAxis<'_>,
        position: &[f64],
        length: usize,
    ) -> Result<Vec<f64>, ImageError> {
        let Some(law) = &self.models.spectral_polynomial else {
            return Ok((0..length).map(|pixel| pixel as f64).collect());
        };
        let mut x = (0..length)
            .map(|pixel| axis.native(position, pixel as f64).map(f64::abs))
            .collect::<Result<Vec<_>, _>>()?;
        let divisor = match &self.options.divisor {
            ProfileDivisor::Number(value) if *value != 0. => *value,
            ProfileDivisor::Quantity(value) if value.value() != 0. => {
                let unit = Unit::new(&axis.unit).map_err(|error| invalid(error.to_string()))?;
                value
                    .get_value_in(&unit)
                    .map_err(|error| invalid(error.to_string()))?
            }
            _ => automatic_divisor(&x),
        };
        if !divisor.is_finite() || divisor <= 0. {
            return Err(invalid("Abscissa divisor must be finite and positive"));
        }
        let log_x = matches!(law.kind, SpectralPolynomial::LogTransformed);
        for value in &mut x {
            *value /= divisor;
            if log_x && divisor != 1. {
                *value = value.ln();
            }
        }
        Ok(x)
    }

    fn accepts(&self, curve: &Curve, solution: &Solution) -> bool {
        solution.converged
            && solution
                .values
                .iter()
                .chain(&solution.errors)
                .all(|value| value.is_finite())
            && curve.lines.iter().all(|line| {
                let values = line.parameters(&solution.values);
                let errors = line.errors(&solution.errors);
                self.options
                    .good_ranges
                    .iter()
                    .enumerate()
                    .all(|(index, range)| {
                        let Some([low, high]) = *range else {
                            return true;
                        };
                        let value = if index == 2 {
                            values[index].abs()
                        } else {
                            values[index]
                        };
                        value >= low.min(high)
                            && value <= high.max(low)
                            && (index == 1 || value == 0. || (errors[index] / value).abs() <= 100.)
                    })
            })
    }
}

struct FitLane {
    x: Vec<f64>,
    mask: Vec<bool>,
    valid: bool,
    attempted: bool,
    logarithmic: bool,
    solution: Option<(Curve, Solution)>,
}

fn nearest<'a>(pixel: &[usize], previous: &'a [(Vec<usize>, Vec<f64>)]) -> Option<&'a [f64]> {
    let mut best = None;
    let mut nearest = u128::MAX;
    for (position, values) in previous.iter().rev() {
        let distance = position
            .iter()
            .zip(pixel)
            .fold(0_u128, |sum, (&left, &right)| {
                sum.saturating_add((left.abs_diff(right) as u128).pow(2))
            });
        if distance < nearest {
            nearest = distance;
            best = Some(values.as_slice());
            if distance == 1 {
                break;
            }
        }
    }
    best
}

fn models(options: &mut ProfileFitOptions<'_>) -> Result<ProfileModels, ImageError> {
    let estimates = std::mem::take(&mut options.estimates);
    let mut singlets = PeakLists::from(estimates.singlets).singlets(estimates.functions)?;
    let multiplets = PeakLists::from(estimates.multiplets)
        .multiplets(estimates.multiplet_sizes, estimates.multiplet_ties)?;
    let spectral_polynomial = PolynomialEstimate::new(
        estimates.spectral_kind,
        estimates.spectral_values,
        estimates.spectral_fixed,
    )?;
    if let Some(path) = options.estimates_file {
        if !singlets.is_empty() || !multiplets.is_empty() || spectral_polynomial.is_some() {
            return Err(invalid(
                "Cannot specify both an estimates file and direct estimates",
            ));
        }
        singlets = estimates::read(path)?;
    }
    Ok(ProfileModels {
        singlets,
        multiplets,
        spectral_polynomial,
    })
}

fn invalid(message: impl Into<String>) -> ImageError {
    ImageError::CasaSelection(message.into())
}
