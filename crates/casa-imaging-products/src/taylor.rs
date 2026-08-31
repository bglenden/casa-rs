// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-local completion of one released MT-MFS normal family.

use casa_imaging_model::{
    ProductBlankingPolicy, ProductNormalization, ProductRole, ProductSupportComparison,
    ProductTerm, ProductValidityRule, RestoringBeamPolicy, SpectralWcs, TaylorSupportReference,
};
use casa_numerics::solve_symmetric_ldlt_casacore_dynamic;

use crate::AnalyticPrimaryBeamModel;
use crate::beam::{RestoringBeam, fit_restoring_beam};
use crate::error::ProductsError;
use crate::restore::{fft_convolve, gaussian_beam_image, normalize_plane};
use crate::source::ContinuumProductInputs;

pub(crate) struct TaylorProducts {
    shape: [usize; 2],
    psf: Vec<Vec<f32>>,
    residual: Vec<Vec<f32>>,
    model: Vec<Vec<f32>>,
    restored: Vec<Vec<f32>>,
    weight: Vec<Vec<f32>>,
    sum_weights: Vec<f32>,
    primary_beam: Vec<Vec<f32>>,
    pb_corrected: Vec<Vec<f32>>,
    alpha: Vec<f32>,
    alpha_error: Vec<f32>,
    alpha_validity: Vec<bool>,
    primary_beam_validity: Vec<bool>,
    clean_mask: Vec<f32>,
    fitted_beam: Option<RestoringBeam>,
    restoring_beam: Option<RestoringBeam>,
}

impl TaylorProducts {
    pub(crate) fn build(
        inputs: &ContinuumProductInputs<'_>,
        psf_cutoff: f32,
        primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    ) -> Result<Self, ProductsError> {
        let state = inputs.normal_state();
        let shape = state.shape();
        let cells = shape[0] * shape[1];
        let terms = state.coefficient_term_count();
        let moments = state.normal_moment_count();
        if terms < 2
            || moments != terms.saturating_mul(2).saturating_sub(1)
            || inputs.final_model().shape().coefficients() != terms
        {
            return Err(ProductsError::SourceLineageMismatch);
        }
        let principal_sum_weight = state
            .normal_moment(0)
            .ok_or(ProductsError::SourceLineageMismatch)?
            .sum_weight();
        if !(principal_sum_weight.is_finite() && principal_sum_weight > 0.0) {
            return Err(ProductsError::SourceLineageMismatch);
        }
        let normalization = inputs.problem().products().normalization();
        let mut psf = Vec::with_capacity(moments);
        let mut weight: Vec<Vec<f32>> = Vec::with_capacity(moments);
        let mut sum_weights = Vec::with_capacity(moments);
        for moment in 0..moments {
            let source = state
                .normal_moment(moment)
                .ok_or(ProductsError::SourceLineageMismatch)?;
            psf.push(normalize_plane(
                &source
                    .normal_approximation()
                    .iter()
                    .map(|value| value.re as f32)
                    .collect::<Vec<_>>(),
                ProductNormalization::UnitResponse,
                principal_sum_weight,
            )?);
            weight.push(
                source
                    .sensitivity()
                    .iter()
                    .map(|value| *value as f32)
                    .collect(),
            );
            sum_weights.push(source.sum_weight() as f32);
        }
        let residual = (0..terms)
            .map(|term| {
                let source = state
                    .coefficient_term(term)
                    .ok_or(ProductsError::SourceLineageMismatch)?;
                normalize_plane(
                    &source
                        .residual()
                        .iter()
                        .map(|value| value.re as f32)
                        .collect::<Vec<_>>(),
                    normalization,
                    principal_sum_weight,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let model = (0..terms)
            .map(|term| model_term(inputs, term, shape))
            .collect::<Result<Vec<_>, _>>()?;

        let peak = psf[0]
            .iter()
            .enumerate()
            .max_by(|(_, left), (_, right)| left.abs().total_cmp(&right.abs()))
            .map(|(index, _)| index)
            .ok_or(ProductsError::SourceLineageMismatch)?;
        let mut normal = vec![0.0; terms * terms];
        for row in 0..terms {
            for column in 0..terms {
                normal[row * terms + column] = f64::from(psf[row + column][peak]);
            }
        }
        let mut inverse = vec![0.0; terms * terms];
        for column in 0..terms {
            let mut unit = vec![0.0; terms];
            unit[column] = 1.0;
            let solution = solve_symmetric_ldlt_casacore_dynamic(normal.clone(), &unit)
                .ok_or_else(|| {
                    ProductsError::BeamFitFailed("singular Taylor normal block".into())
                })?;
            for row in 0..terms {
                inverse[row * terms + column] = solution[row];
            }
        }
        let mut principal_residual = vec![vec![0.0; cells]; terms];
        for cell in 0..cells {
            for row in 0..terms {
                principal_residual[row][cell] = (0..terms)
                    .map(|column| inverse[row * terms + column] * f64::from(residual[column][cell]))
                    .sum::<f64>() as f32;
            }
        }

        let needs_beam = inputs
            .problem()
            .product_graph()
            .publication()
            .members()
            .iter()
            .any(|ordinal| {
                inputs.problem().product_graph().nodes()[ordinal.ordinal()].beam()
                    != casa_imaging_model::ProductBeamRule::None
            });
        let fitted_beam = needs_beam
            .then(|| fit_restoring_beam(&psf[0], shape, inputs.cell_size_rad(), psf_cutoff))
            .transpose()?;
        let restoring_beam = match inputs.problem().products().restoring_beam() {
            RestoringBeamPolicy::None => None,
            RestoringBeamPolicy::PerPlane | RestoringBeamPolicy::Common => fitted_beam,
        };
        let restored = if let Some(beam) = restoring_beam {
            let kernel = gaussian_beam_image(shape, &beam, inputs.cell_size_rad());
            model
                .iter()
                .zip(&principal_residual)
                .map(|(model, residual)| {
                    let mut image =
                        fft_convolve(model, kernel.as_slice().expect("contiguous"), shape);
                    image
                        .iter_mut()
                        .zip(residual)
                        .for_each(|(image, residual)| *image += residual);
                    image
                })
                .collect()
        } else {
            vec![vec![0.0; cells]; terms]
        };

        let requests_primary_beam = inputs
            .problem()
            .product_graph()
            .publication()
            .members()
            .iter()
            .any(|ordinal| {
                matches!(
                    inputs.problem().product_graph().nodes()[ordinal.ordinal()].role(),
                    ProductRole::PrimaryBeam(_) | ProductRole::PbCorrectedImage(_)
                )
            });
        let pb0 = match primary_beam_model {
            Some(AnalyticPrimaryBeamModel::CasaEvlaCommon) => {
                analytic_evla_common_primary_beam(inputs, shape)?
            }
            None if requests_primary_beam => return Err(ProductsError::UnsupportedProblem),
            None => primary_beam_from_weight(&weight[0])?,
        };
        let mut primary_beam = vec![vec![0.0; cells]; terms];
        primary_beam[0] = pb0.clone();

        let validity = inputs.problem().products().validity();
        let pb_policy = validity.primary_beam();
        if pb_policy.comparison() != ProductSupportComparison::StrictlyGreater
            || pb_policy.blanking() != ProductBlankingPolicy::ZeroAndFalseMask
        {
            return Err(ProductsError::UnsupportedProblem);
        }
        let primary_beam_validity = pb0
            .iter()
            .map(|value| value.is_finite() && *value > pb_policy.cutoff())
            .collect::<Vec<_>>();
        let pb_corrected = restored
            .iter()
            .map(|image| {
                image
                    .iter()
                    .zip(&pb0)
                    .zip(&primary_beam_validity)
                    .map(|((value, pb), valid)| if *valid { *value / *pb } else { 0.0 })
                    .collect()
            })
            .collect::<Vec<_>>();

        let taylor_policy = validity.taylor();
        if taylor_policy.reference()
            != TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum
            || taylor_policy.comparison() != ProductSupportComparison::StrictlyGreater
            || taylor_policy.blanking() != ProductBlankingPolicy::ZeroAndFalseMask
        {
            return Err(ProductsError::UnsupportedProblem);
        }
        let threshold = principal_residual[0]
            .iter()
            .copied()
            .fold(f32::NEG_INFINITY, f32::max)
            * taylor_policy.peak_fraction();
        let alpha_validity = restored[0]
            .iter()
            .map(|value| value.is_finite() && *value > threshold)
            .collect::<Vec<_>>();
        let mut alpha = vec![0.0; cells];
        let mut alpha_error = vec![0.0; cells];
        for cell in 0..cells {
            if !alpha_validity[cell] {
                continue;
            }
            let image0 = restored[0][cell];
            let image1 = restored[1][cell];
            let residual0 = principal_residual[0][cell];
            let residual1 = principal_residual[1][cell];
            alpha[cell] = image1 / image0;
            alpha_error[cell] = ((image1 * residual0 / image0.powi(2)).powi(2)
                + (residual1 / image0).powi(2))
            .sqrt();
        }
        let clean_mask = weight[0]
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let selected = inputs
                    .reconstruction_mask()
                    .is_none_or(|mask| mask.support()[index]);
                (selected && value.is_finite() && *value > 0.0) as u8 as f32
            })
            .collect();

        Ok(Self {
            shape,
            psf,
            residual,
            model,
            restored,
            weight,
            sum_weights,
            primary_beam,
            pb_corrected,
            alpha,
            alpha_error,
            alpha_validity,
            primary_beam_validity,
            clean_mask,
            fitted_beam,
            restoring_beam,
        })
    }

    pub(crate) const fn fitted_beam(&self) -> Option<RestoringBeam> {
        self.fitted_beam
    }

    pub(crate) const fn restoring_beam(&self) -> Option<RestoringBeam> {
        self.restoring_beam
    }

    pub(crate) fn payload(&self, role: ProductRole) -> Result<Vec<f32>, ProductsError> {
        let term = |term: ProductTerm| match term {
            ProductTerm::Taylor(term) => Ok(term),
            ProductTerm::Single => Err(ProductsError::UnsupportedProductRole {
                role,
                catalog: crate::CONTINUUM_ALGORITHM_CATALOG_VERSION,
            }),
        };
        match role {
            ProductRole::Psf(value) => self.psf.get(term(value)?),
            ProductRole::Residual(value) => self.residual.get(term(value)?),
            ProductRole::Model(value) => self.model.get(term(value)?),
            ProductRole::RestoredImage(value) => self.restored.get(term(value)?),
            ProductRole::Weight(value) => self.weight.get(term(value)?),
            ProductRole::PrimaryBeam(value) => self.primary_beam.get(term(value)?),
            ProductRole::PbCorrectedImage(value) => self.pb_corrected.get(term(value)?),
            ProductRole::SpectralIndex => return Ok(self.alpha.clone()),
            ProductRole::SpectralIndexError => return Ok(self.alpha_error.clone()),
            ProductRole::CleanMask => return Ok(self.clean_mask.clone()),
            ProductRole::Sensitivity => self.weight.first(),
            ProductRole::SumWeights(value) => {
                return self
                    .sum_weights
                    .get(term(value)?)
                    .copied()
                    .map(|value| vec![value])
                    .ok_or(ProductsError::SourceLineageMismatch);
            }
            _ => None,
        }
        .cloned()
        .ok_or(ProductsError::UnsupportedProductRole {
            role,
            catalog: crate::CONTINUUM_ALGORITHM_CATALOG_VERSION,
        })
    }

    pub(crate) fn validity(&self, rule: ProductValidityRule) -> Result<Vec<bool>, ProductsError> {
        match rule {
            ProductValidityRule::All | ProductValidityRule::FinalNormalState => {
                Ok(vec![true; self.shape[0] * self.shape[1]])
            }
            ProductValidityRule::PrimaryBeam(_) => Ok(self.primary_beam_validity.clone()),
            ProductValidityRule::Taylor(_) => Ok(self.alpha_validity.clone()),
            ProductValidityRule::TaylorAndPrimaryBeam { .. } => Ok(self
                .alpha_validity
                .iter()
                .zip(&self.primary_beam_validity)
                .map(|(taylor, pb)| *taylor && *pb)
                .collect()),
        }
    }
}

fn primary_beam_from_weight(weight: &[f32]) -> Result<Vec<f32>, ProductsError> {
    // Retained only for product graphs that do not publish PB: CASA
    // SIImageStore::makePBFromWeight normalizes the principal weight image.
    let scale = weight
        .iter()
        .copied()
        .map(f32::abs)
        .fold(0.0_f32, f32::max)
        .sqrt();
    if !(scale.is_finite() && scale > 0.0) {
        return Err(ProductsError::GeneratedNonfinite);
    }
    Ok(weight
        .iter()
        .map(|value| value.abs().sqrt() / scale)
        .collect())
}

fn analytic_evla_common_primary_beam(
    inputs: &ContinuumProductInputs<'_>,
    shape: [usize; 2],
) -> Result<Vec<f32>, ProductsError> {
    let domain = inputs
        .problem()
        .geometry()
        .domains()
        .first()
        .ok_or(ProductsError::UnsupportedProblem)?;
    let direction = domain.direction();
    let reference_pixel = direction.reference_pixel();
    let increment_rad = direction.increment_rad();
    let frequency_hz = match inputs.problem().geometry().spectral().wcs() {
        SpectralWcs::Linear {
            reference_frequency_hz,
            ..
        } => *reference_frequency_hz,
        SpectralWcs::Tabular { .. } => return Err(ProductsError::UnsupportedProblem),
    };
    let coefficients = nearest_evla_common_coefficients(frequency_hz * 1.0e-6)
        .ok_or(ProductsError::UnsupportedProblem)?;
    let mut values = vec![0.0; shape[0] * shape[1]];
    for x in 0..shape[0] {
        for y in 0..shape[1] {
            let longitude = (x as f64 - reference_pixel[0]) * increment_rad[0];
            let latitude = (y as f64 - reference_pixel[1]) * increment_rad[1];
            values[x * shape[1] + y] =
                evla_common_power_pattern(longitude.hypot(latitude), frequency_hz, coefficients);
        }
    }
    Ok(values)
}

fn evla_common_power_pattern(radius_rad: f64, frequency_hz: f64, coefficients: [f64; 4]) -> f32 {
    if !(radius_rad.is_finite()
        && radius_rad >= 0.0
        && frequency_hz.is_finite()
        && frequency_hz > 0.0)
    {
        return 0.0;
    }
    // CASA PBMath1DEVLA::nearestVPArray(), PBMath1DPoly::fillPBArray(), and
    // PBMath1D::apply() use 10,000 samples and integer-truncated radial lookup.
    let radius_arcmin_ghz = radius_rad.to_degrees() * 60.0 * (frequency_hz / 1.0e9);
    if radius_arcmin_ghz > 58.0 {
        return 0.0;
    }
    let inverse_increment_radius = 9_999.0 / 58.0;
    let sample_index = (radius_arcmin_ghz * inverse_increment_radius).floor();
    let sampled_radius_arcmin_ghz = sample_index / inverse_increment_radius;
    let x2 = sampled_radius_arcmin_ghz * sampled_radius_arcmin_ghz;
    let mut response = 0.0;
    let mut power = 1.0;
    for coefficient in coefficients {
        response += coefficient * power;
        power *= x2;
    }
    if response <= 0.0 {
        0.0
    } else {
        response as f32
    }
}

fn nearest_evla_common_coefficients(frequency_mhz: f64) -> Option<[f64; 4]> {
    // Preserve PBMath1DEVLA::feed() precedence in its narrow band overlaps and
    // limitFreqForBand() clamping. Frequencies outside the represented CASA
    // bands fail closed instead of silently borrowing another band's beam.
    let (frequency_mhz, coefficients) = if frequency_mhz > 900.0 && frequency_mhz < 2003.0 {
        (
            frequency_mhz.clamp(1040.0, 2000.0),
            EVLA_L_BAND_COEFFICIENTS,
        )
    } else if frequency_mhz > 1990.0 && frequency_mhz < 4001.0 {
        (
            frequency_mhz.clamp(2052.0, 3948.0),
            EVLA_S_BAND_COEFFICIENTS,
        )
    } else if frequency_mhz > 3990.0 && frequency_mhz < 8001.0 {
        (
            frequency_mhz.clamp(4052.0, 7948.0),
            EVLA_C_BAND_COEFFICIENTS,
        )
    } else {
        return None;
    };
    let mut best = coefficients[0].1;
    let mut best_delta_mhz = f64::INFINITY;
    for &(candidate_frequency_mhz, candidate_coefficients) in coefficients {
        let delta_mhz = (frequency_mhz - candidate_frequency_mhz).abs();
        if delta_mhz < best_delta_mhz {
            best_delta_mhz = delta_mhz;
            best = candidate_coefficients;
        }
    }
    Some(best)
}

const EVLA_L_BAND_COEFFICIENTS: &[(f64, [f64; 4])] = &[
    (1040.0, [1.000, -1.529e-3, 8.69e-7, -1.88e-10]),
    (1104.0, [1.000, -1.486e-3, 8.15e-7, -1.68e-10]),
    (1168.0, [1.000, -1.439e-3, 7.53e-7, -1.45e-10]),
    (1232.0, [1.000, -1.450e-3, 7.87e-7, -1.63e-10]),
    (1296.0, [1.000, -1.428e-3, 7.62e-7, -1.54e-10]),
    (1360.0, [1.000, -1.449e-3, 8.02e-7, -1.74e-10]),
    (1424.0, [1.000, -1.462e-3, 8.23e-7, -1.83e-10]),
    (1488.0, [1.000, -1.455e-3, 7.92e-7, -1.63e-10]),
    (1552.0, [1.000, -1.435e-3, 7.54e-7, -1.49e-10]),
    (1680.0, [1.000, -1.443e-3, 7.74e-7, -1.57e-10]),
    (1744.0, [1.000, -1.462e-3, 8.02e-7, -1.69e-10]),
    (1808.0, [1.000, -1.488e-3, 8.38e-7, -1.83e-10]),
    (1872.0, [1.000, -1.486e-3, 8.26e-7, -1.75e-10]),
    (1936.0, [1.000, -1.459e-3, 7.93e-7, -1.62e-10]),
    (2000.0, [1.000, -1.508e-3, 8.31e-7, -1.68e-10]),
];

const EVLA_S_BAND_COEFFICIENTS: &[(f64, [f64; 4])] = &[
    (2052.0, [1.000, -1.429e-3, 7.52e-7, -1.47e-10]),
    (2180.0, [1.000, -1.389e-3, 7.06e-7, -1.33e-10]),
    (2436.0, [1.000, -1.377e-3, 6.90e-7, -1.27e-10]),
    (2564.0, [1.000, -1.381e-3, 6.92e-7, -1.26e-10]),
    (2692.0, [1.000, -1.402e-3, 7.23e-7, -1.40e-10]),
    (2820.0, [1.000, -1.433e-3, 7.62e-7, -1.54e-10]),
    (2948.0, [1.000, -1.433e-3, 7.46e-7, -1.42e-10]),
    (3052.0, [1.000, -1.467e-3, 8.05e-7, -1.70e-10]),
    (3180.0, [1.000, -1.497e-3, 8.38e-7, -1.80e-10]),
    (3308.0, [1.000, -1.504e-3, 8.37e-7, -1.77e-10]),
    (3436.0, [1.000, -1.521e-3, 8.63e-7, -1.88e-10]),
    (3564.0, [1.000, -1.505e-3, 8.37e-7, -1.75e-10]),
    (3692.0, [1.000, -1.521e-3, 8.51e-7, -1.79e-10]),
    (3820.0, [1.000, -1.534e-3, 8.57e-7, -1.77e-10]),
    (3948.0, [1.000, -1.516e-3, 8.30e-7, -1.66e-10]),
];

const EVLA_C_BAND_COEFFICIENTS: &[(f64, [f64; 4])] = &[
    (4052.0, [1.000, -1.406e-3, 7.41e-7, -1.48e-10]),
    (4180.0, [1.000, -1.385e-3, 7.09e-7, -1.36e-10]),
    (4308.0, [1.000, -1.380e-3, 7.08e-7, -1.37e-10]),
    (4436.0, [1.000, -1.362e-3, 6.95e-7, -1.35e-10]),
    (4564.0, [1.000, -1.365e-3, 6.92e-7, -1.31e-10]),
    (4692.0, [1.000, -1.339e-3, 6.56e-7, -1.17e-10]),
    (4820.0, [1.000, -1.371e-3, 7.06e-7, -1.40e-10]),
    (4948.0, [1.000, -1.358e-3, 6.91e-7, -1.34e-10]),
    (5052.0, [1.000, -1.360e-3, 6.91e-7, -1.33e-10]),
    (5180.0, [1.000, -1.353e-3, 6.74e-7, -1.25e-10]),
    (5308.0, [1.000, -1.359e-3, 6.82e-7, -1.27e-10]),
    (5436.0, [1.000, -1.380e-3, 7.05e-7, -1.37e-10]),
    (5564.0, [1.000, -1.376e-3, 6.99e-7, -1.31e-10]),
    (5692.0, [1.000, -1.405e-3, 7.39e-7, -1.47e-10]),
    (5820.0, [1.000, -1.394e-3, 7.29e-7, -1.45e-10]),
    (5948.0, [1.000, -1.428e-3, 7.57e-7, -1.57e-10]),
    (6052.0, [1.000, -1.445e-3, 7.68e-7, -1.50e-10]),
    (6148.0, [1.000, -1.422e-3, 7.38e-7, -1.38e-10]),
    (6308.0, [1.000, -1.463e-3, 7.94e-7, -1.62e-10]),
    (6436.0, [1.000, -1.478e-3, 8.22e-7, -1.74e-10]),
    (6564.0, [1.000, -1.473e-3, 8.00e-7, -1.62e-10]),
    (6692.0, [1.000, -1.455e-3, 7.76e-7, -1.53e-10]),
    (6820.0, [1.000, -1.487e-3, 8.22e-7, -1.72e-10]),
    (6948.0, [1.000, -1.472e-3, 8.05e-7, -1.67e-10]),
    (7052.0, [1.000, -1.470e-3, 8.01e-7, -1.64e-10]),
    (7180.0, [1.000, -1.503e-3, 8.50e-7, -1.84e-10]),
    (7308.0, [1.000, -1.482e-3, 8.19e-7, -1.72e-10]),
    (7436.0, [1.000, -1.498e-3, 8.22e-7, -1.66e-10]),
    (7564.0, [1.000, -1.490e-3, 8.18e-7, -1.66e-10]),
    (7692.0, [1.000, -1.481e-3, 7.98e-7, -1.56e-10]),
    (7820.0, [1.000, -1.474e-3, 7.94e-7, -1.57e-10]),
    (7948.0, [1.000, -1.448e-3, 7.69e-7, -1.51e-10]),
];

fn model_term(
    inputs: &ContinuumProductInputs<'_>,
    coefficient: usize,
    shape: [usize; 2],
) -> Result<Vec<f32>, ProductsError> {
    use casa_imaging_model::ModelCell;

    let model = inputs.final_model();
    if model.shape().domains().len() != 1
        || model.shape().polarizations() != 1
        || model.shape().domains()[0].pixels() != shape
    {
        return Err(ProductsError::SourceLineageMismatch);
    }
    let mut plane = vec![0.0; shape[0] * shape[1]];
    for y in 0..shape[1] {
        for x in 0..shape[0] {
            let index = model
                .shape()
                .flat_index(ModelCell::new(0, coefficient, 0, [x, y]))
                .ok_or(ProductsError::SourceLineageMismatch)?;
            plane[x * shape[1] + y] = model.samples()[index].value().value() as f32;
        }
    }
    Ok(plane)
}

#[cfg(test)]
mod tests {
    #[test]
    fn evla_common_s_band_uses_casa_sampled_power_lookup() {
        let frequency_hz = 2.091_980_123e9_f64;
        let coefficients = super::nearest_evla_common_coefficients(frequency_hz * 1.0e-6)
            .expect("T44 frequency is in EVLA S band");
        assert_eq!(coefficients, [1.000, -1.429e-3, 7.52e-7, -1.47e-10]);
        let sample_index = 2_210.0_f64;
        let inverse_increment = 9_999.0_f64 / 58.0;
        let sampled_radius_arcmin_ghz = sample_index / inverse_increment;
        let radius_arcmin_ghz = sampled_radius_arcmin_ghz + 0.75 / inverse_increment;
        let radius_rad = (radius_arcmin_ghz / 60.0 / (frequency_hz / 1.0e9_f64)).to_radians();
        let x2 = sampled_radius_arcmin_ghz * sampled_radius_arcmin_ghz;
        let expected_power = 1.0 - 1.429e-3 * x2 + 7.52e-7 * x2.powi(2) - 1.47e-10 * x2.powi(3);
        let actual = super::evla_common_power_pattern(radius_rad, frequency_hz, coefficients);
        assert!((actual - expected_power as f32).abs() < 1.0e-7);
        assert!(super::nearest_evla_common_coefficients(850.0).is_none());
    }
}
