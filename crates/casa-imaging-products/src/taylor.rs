// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-local completion of one released MT-MFS normal family.

use casa_imaging_deconvolution::FinalNormalStateWindow;
use casa_imaging_model::{
    ProductBlankingPolicy, ProductNormalization, ProductRole, ProductSupportComparison,
    ProductTerm, ProductValidityRule, RestoringBeamPolicy, TaylorSupportReference,
};
use casa_numerics::solve_symmetric_ldlt_casacore_dynamic;

use crate::AnalyticPrimaryBeamModel;
use crate::beam::{RestoringBeam, fit_restoring_beam, fixed_restoring_beam};
use crate::error::ProductsError;
use crate::normalization::{
    MosaicSensitivity, ResidualFamily, normalize_plane, normalized_psf_value, psf_peak,
};
use crate::restore::{fft_convolve, gaussian_beam_image, rescale_residual_to_beam};
use crate::source::ContinuumProductInputs;

pub(crate) struct TaylorProducts {
    shape: [usize; 2],
    psf: Vec<Vec<f32>>,
    principal_psf_peak: f32,
    residual: Vec<Vec<f32>>,
    model: Vec<Vec<f32>>,
    restored: Vec<Vec<f32>>,
    weight: Vec<Vec<f32>>,
    sensitivity: Vec<f32>,
    sum_weights: Vec<f32>,
    primary_beam: Vec<Vec<f32>>,
    pb_corrected: Vec<Vec<f32>>,
    alpha: Vec<f32>,
    alpha_pbcor: Vec<f32>,
    alpha_error: Vec<f32>,
    alpha_validity: Vec<bool>,
    primary_beam_validity: Vec<bool>,
    clean_mask: Vec<f32>,
    fitted_beam: Option<RestoringBeam>,
    restoring_beam: Option<RestoringBeam>,
}

fn primary_beam_correct_spectral_index(
    alpha: &[f32],
    principal_sensitivity: &[f32],
    weight_moments: &[Vec<f32>],
    inverse_normal: &[f64],
    terms: usize,
    alpha_validity: &[bool],
    primary_beam_validity: &[bool],
) -> Result<Vec<f32>, ProductsError> {
    let cells = alpha.len();
    debug_assert!(
        terms >= 2
            && principal_sensitivity.len() == cells
            && weight_moments.len() >= terms
            && inverse_normal.len() == terms * terms
            && alpha_validity.len() == cells
            && primary_beam_validity.len() == cells
            && weight_moments
                .iter()
                .take(terms)
                .all(|moment| moment.len() == cells),
        "one Taylor family's planes share its shape"
    );

    let mut corrected = vec![0.0; cells];
    for cell in 0..cells {
        if !alpha_validity[cell] || !primary_beam_validity[cell] {
            continue;
        }
        let sensitivity_moment = |moment: usize| {
            if moment == 0 {
                principal_sensitivity[cell]
            } else {
                weight_moments[moment][cell]
            }
        };
        let power_taylor0 = (0..terms)
            .map(|moment| inverse_normal[moment] * f64::from(sensitivity_moment(moment)))
            .sum::<f64>();
        let power_taylor1 = (0..terms)
            .map(|moment| inverse_normal[terms + moment] * f64::from(sensitivity_moment(moment)))
            .sum::<f64>();
        if !(power_taylor0.is_finite()
            && power_taylor0 > 0.0
            && power_taylor1.is_finite()
            && alpha[cell].is_finite())
        {
            return Err(ProductsError::GeneratedNonfinite);
        }
        // Mosaic sensitivity is a power response. Its fitted voltage/power-beam
        // Taylor ratio is therefore half the fitted power-response ratio.
        let primary_beam_alpha = 0.5 * power_taylor1 / power_taylor0;
        let value = f64::from(alpha[cell]) - primary_beam_alpha;
        corrected[cell] = value
            .is_finite()
            .then_some(value as f32)
            .ok_or(ProductsError::GeneratedNonfinite)?;
    }
    Ok(corrected)
}

/// The Taylor family read from the final normal state: PSF and weight
/// moments with their sums of weights, and each term's residual and model
/// in the units CASA publishes them in.
struct TaylorFamily {
    psf: Vec<Vec<f32>>,
    weight: Vec<Vec<f32>>,
    sum_weights: Vec<f32>,
    residual: Vec<Vec<f32>>,
    model: Vec<Vec<f32>>,
}

impl TaylorFamily {
    fn read(
        inputs: &ContinuumProductInputs<'_>,
        state: &FinalNormalStateWindow<'_>,
        primary_beam_model: Option<AnalyticPrimaryBeamModel>,
        directional: Option<MosaicSensitivity<'_>>,
        residual_sum_weight: f64,
    ) -> Result<Self, ProductsError> {
        let principal_sum_weight = state
            .normal_moment(0)
            .expect("the zeroth normal moment")
            .sum_weight();
        let published_sums = state.published_sum_weights();
        let moments = state.normal_moment_count();
        let mut psf = Vec::with_capacity(moments);
        let mut weight = Vec::with_capacity(moments);
        let mut sum_weights = Vec::with_capacity(moments);
        for (moment, published) in published_sums.iter().copied().enumerate().take(moments) {
            let source = state.normal_moment(moment).expect("every normal moment");
            let approximation = source
                .normal_approximation()
                .iter()
                .map(|value| value.re as f32)
                .collect::<Vec<_>>();
            psf.push(normalize_plane(&approximation, principal_sum_weight));
            // CASA divides only weight.tt0 by the principal sum weight; the
            // higher Taylor weight images keep their raw Σ W·s^m moments. Its
            // multi-term loop over every term is never called:
            // `SIImageStoreMultiTerm` declares `divideWeightBySumWt()`
            // (capital W), which does not override the base class's virtual
            // `divideWeightBySumwt()` that `SynthesisNormalizer` calls, so
            // the base body divides `weight()`, term 0, alone (CASA 6.7.6.14;
            // `docs/CASA (C++) bugs.md`). CASA's values on
            // `refim_alma_mosaic` agree: weight.ttN / sumwt.ttN is 0.6913
            // for N = 0, 1 and 2.
            let scale = if moment == 0
                && primary_beam_model == Some(AnalyticPrimaryBeamModel::MosaicSensitivity)
            {
                principal_sum_weight
            } else {
                1.0
            };
            weight.push(
                source
                    .sensitivity()
                    .iter()
                    .map(|value| (*value / scale) as f32)
                    .collect(),
            );
            if !published.is_finite() {
                return Err(ProductsError::GeneratedNonfinite);
            }
            sum_weights.push(published as f32);
        }
        let products = inputs.problem().products();
        let (normalization, policy) =
            (products.normalization(), products.validity().primary_beam());
        let residual = (0..state.coefficient_term_count())
            .map(|term| {
                let residual = state
                    .coefficient_term(term)
                    .expect("every coefficient term")
                    .residual();
                match directional {
                    Some(response) => residual
                        .iter()
                        .enumerate()
                        .map(|(index, value)| {
                            Ok(response.normalize_weighted_residual_sample(
                                value.re,
                                index,
                                normalization,
                                ResidualFamily::Taylor,
                                residual_sum_weight,
                                policy,
                            )? as f32)
                        })
                        .collect(),
                    None => Ok(normalize_plane(
                        &residual
                            .iter()
                            .map(|value| value.re as f32)
                            .collect::<Vec<_>>(),
                        residual_sum_weight,
                    )),
                }
            })
            .collect::<Result<Vec<_>, ProductsError>>()?;
        let mut model = (0..state.coefficient_term_count())
            .map(|term| model_term(inputs, term, state.shape()))
            .collect::<Result<Vec<_>, _>>()?;
        // CASA restores and publishes the flat-noise model in apparent units
        // (`SynthesisNormalizer::multiplyModelByWeight`).
        if let Some(response) = directional {
            for plane in &mut model {
                for (index, value) in plane.iter_mut().enumerate() {
                    *value = response.physical_to_apparent(
                        f64::from(*value),
                        index,
                        normalization,
                        policy,
                    )? as f32;
                }
            }
        }
        Ok(Self {
            psf,
            weight,
            sum_weights,
            residual,
            model,
        })
    }
}

/// The Taylor normal block at the PSF peak, inverted, and the principal
/// solution's residuals it gives.
///
/// CASA forms the block from the published PSFs, every term divided by the
/// term-0 PSF's peak (`SIImageStore::normPSF`, then
/// `SDAlgorithmMSMFS::restore`), as the minor cycle does. The sum-weight
/// normalized PSF peaks at one only for a gridder whose PSF peak is its
/// sum of weights; a mosaic's does not.
fn principal_residuals(
    psf: &[Vec<f32>],
    residual: &[Vec<f32>],
) -> Result<(Vec<f64>, Vec<Vec<f32>>), ProductsError> {
    let terms = residual.len();
    let peak = psf[0]
        .iter()
        .enumerate()
        .max_by(|(_, left), (_, right)| left.abs().total_cmp(&right.abs()))
        .map(|(index, _)| index)
        .expect("a PSF plane has pixels");
    let psf_scale = f64::from(psf_peak(psf[0].iter().copied())?);
    let mut normal = vec![0.0; terms * terms];
    for row in 0..terms {
        for column in 0..terms {
            normal[row * terms + column] = f64::from(psf[row + column][peak]) / psf_scale;
        }
    }
    let mut inverse = vec![0.0; terms * terms];
    for column in 0..terms {
        let mut unit = vec![0.0; terms];
        unit[column] = 1.0;
        let solution = solve_symmetric_ldlt_casacore_dynamic(normal.clone(), &unit)
            .ok_or_else(|| ProductsError::BeamFitFailed("singular Taylor normal block".into()))?;
        for row in 0..terms {
            inverse[row * terms + column] = solution[row];
        }
    }
    let cells = residual[0].len();
    let mut principal = vec![vec![0.0; cells]; terms];
    for cell in 0..cells {
        for row in 0..terms {
            principal[row][cell] = (0..terms)
                .map(|column| inverse[row * terms + column] * f64::from(residual[column][cell]))
                .sum::<f64>() as f32;
        }
    }
    Ok((inverse, principal))
}

/// Each term's model convolved with the restoring beam, plus its principal
/// residual. CASA restores every Taylor term with `SIImageStore::restore`,
/// whose `rescaleResolution` first brings a residual from the PSF's fitted
/// beam to a restoring beam the request gives; with the fitted beam itself
/// the residual is added as it is.
fn restored_terms(
    model: &[Vec<f32>],
    principal_residual: &[Vec<f32>],
    (fitted, beam): (RestoringBeam, RestoringBeam),
    shape: [usize; 2],
    cell_size_rad: [f64; 2],
) -> Result<Vec<Vec<f32>>, ProductsError> {
    let kernel = gaussian_beam_image(shape, &beam, cell_size_rad);
    model
        .iter()
        .zip(principal_residual)
        .map(|(model, residual)| {
            let rescaled;
            let residual = if beam == fitted {
                residual.as_slice()
            } else {
                rescaled = rescale_residual_to_beam(residual, shape, cell_size_rad, fitted, beam)?
                    .into_values();
                rescaled.as_slice()
            };
            let mut image = fft_convolve(model, kernel.as_slice().expect("contiguous"), shape);
            image
                .iter_mut()
                .zip(residual)
                .for_each(|(image, residual)| *image += residual);
            Ok(image)
        })
        .collect()
}

/// The mosaic's direction-dependent response a flat-noise or flat-sky
/// Taylor family is normalized by: the residual divides by the weight image
/// and the model is published in apparent units (CASA
/// `divideResidualByWeight`, `multiplyModelByWeight`). Unit-response
/// products keep the scalar sum-weight division whatever beam they publish.
fn directional_response<'a>(
    inputs: &ContinuumProductInputs<'_>,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    principal_sensitivity: &'a [f64],
    principal_sum_weight: f64,
) -> Result<Option<MosaicSensitivity<'a>>, ProductsError> {
    let directional = primary_beam_model == Some(AnalyticPrimaryBeamModel::MosaicSensitivity)
        && matches!(
            inputs.problem().products().normalization(),
            ProductNormalization::FlatNoise
                | ProductNormalization::FlatSky
                | ProductNormalization::PbSquare
        );
    directional
        .then(|| {
            MosaicSensitivity::new(principal_sensitivity)?
                .with_normal_sum_weight(principal_sum_weight)
        })
        .transpose()
}

/// The spectral index, its error and its support.
type SpectralIndexPlanes = (Vec<f32>, Vec<f32>, Vec<bool>);

/// The spectral index, its error and its support, from the principal
/// residuals and restored images of terms 0 and 1.
fn spectral_index(
    validity: casa_imaging_model::ProductValidityPolicies,
    principal_residual: &[Vec<f32>],
    restored: &[Vec<f32>],
) -> Result<SpectralIndexPlanes, ProductsError> {
    let policy = validity.taylor();
    if policy.reference() != TaylorSupportReference::PrincipalResidualTaylor0PositiveMaximum
        || policy.comparison() != ProductSupportComparison::StrictlyGreater
        || policy.blanking() != ProductBlankingPolicy::Zero
    {
        return Err(ProductsError::UnsupportedProblem);
    }
    Ok(taylor_alpha_products(
        &principal_residual[0],
        &principal_residual[1],
        &restored[0],
        &restored[1],
        policy.peak_fraction(),
    ))
}

/// Term 0's PB support, and every term's restored image divided by the PB
/// inside it.
fn primary_beam_corrected(
    validity: casa_imaging_model::ProductValidityPolicies,
    pb0: &[f32],
    restored: &[Vec<f32>],
) -> Result<(Vec<bool>, Vec<Vec<f32>>), ProductsError> {
    let policy = validity.primary_beam();
    if policy.comparison() != ProductSupportComparison::StrictlyGreater
        || policy.blanking() != ProductBlankingPolicy::Zero
    {
        return Err(ProductsError::UnsupportedProblem);
    }
    let support = pb0
        .iter()
        .map(|value| value.is_finite() && *value > policy.cutoff())
        .collect::<Vec<_>>();
    let corrected = restored
        .iter()
        .map(|image| {
            image
                .iter()
                .zip(pb0)
                .zip(&support)
                .map(|((value, pb), valid)| if *valid { *value / *pb } else { 0.0 })
                .collect()
        })
        .collect();
    Ok((support, corrected))
}

/// Whether the product list publishes a role matching `role`.
fn publishes(inputs: &ContinuumProductInputs<'_>, role: impl Fn(ProductRole) -> bool) -> bool {
    let list = inputs.problem().product_list();
    list.publication()
        .members()
        .iter()
        .any(|id| role(list.entries()[id.ordinal()].role()))
}

/// Taylor term 0's PB: the telescope's analytic beam at the first output
/// channel, or the mosaic's weight image.
fn principal_primary_beam(
    inputs: &ContinuumProductInputs<'_>,
    domain_role: &casa_imaging_model::ImageDomainRole,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    directional: Option<MosaicSensitivity<'_>>,
    weight0: &[f32],
) -> Result<Vec<f32>, ProductsError> {
    if let Some(beam) = primary_beam_model.and_then(|model| inputs.analytic_primary_beam(model)) {
        return beam.plane(domain_role, 0);
    }
    Ok(match primary_beam_model {
        Some(_) => match directional {
            Some(response) => response
                .weighted_primary_beam(inputs.problem().products().validity().primary_beam())?,
            None => MosaicSensitivity::primary_beam_from_weight(weight0)?,
        },
        None => {
            assert!(
                !publishes(inputs, |role| matches!(
                    role,
                    ProductRole::PrimaryBeam(_) | ProductRole::PbCorrectedImage(_)
                )),
                "planning refuses a PB product without a primary-beam model"
            );
            MosaicSensitivity::primary_beam_from_weight(weight0)?
        }
    })
}

impl TaylorProducts {
    pub(crate) fn build(
        inputs: &ContinuumProductInputs<'_>,
        psf_cutoff: f32,
        primary_beam_model: Option<AnalyticPrimaryBeamModel>,
    ) -> Result<Self, ProductsError> {
        let state = inputs.normal_state();
        // Binding the inputs established one domain, `terms` coefficient
        // terms and 2·terms − 1 normal moments.
        let state = &state.read_window(state.slab().core_range())?;
        let domain_role = state
            .domain(0)
            .expect("a Taylor run images one domain")
            .role();
        let shape = state.shape();
        let cells = shape[0] * shape[1];
        let terms = state.coefficient_term_count();
        let principal_normal = state.normal_moment(0).expect("the zeroth normal moment");
        let principal_sum_weight = principal_normal.sum_weight();
        // Direct CASA AW Taylor completion keeps two principal sums: the
        // WTCF normal sum scales PSF/sensitivity products, while the CFS
        // publication sum scales dirty/residual image products. They are
        // equal for non-AW direct Taylor reconstruction.
        let residual_sum_weight = state.published_sum_weights()[0];
        for sum_weight in [principal_sum_weight, residual_sum_weight] {
            if !(sum_weight.is_finite() && sum_weight > 0.0) {
                return Err(ProductsError::PrincipalSumWeight { sum_weight });
            }
        }
        let directional = directional_response(
            inputs,
            primary_beam_model,
            principal_normal.sensitivity(),
            principal_sum_weight,
        )?;
        let family = TaylorFamily::read(
            inputs,
            state,
            primary_beam_model,
            directional,
            residual_sum_weight,
        )?;
        let sensitivity = principal_normal
            .sensitivity()
            .iter()
            .map(|value| *value as f32)
            .collect::<Vec<_>>();
        let (inverse, principal_residual) = principal_residuals(&family.psf, &family.residual)?;

        let cell_size = inputs.cell_size_rad_for_domain(domain_role);
        let list = inputs.problem().product_list();
        let fitted_beam = list
            .publication()
            .members()
            .iter()
            .any(|id| {
                list.entries()[id.ordinal()].beam() != casa_imaging_model::ProductBeamRule::None
            })
            .then(|| fit_restoring_beam(&family.psf[0], shape, cell_size, psf_cutoff))
            .transpose()?;
        let restoring_beam = match inputs.problem().products().restoring_beam() {
            RestoringBeamPolicy::None => None,
            RestoringBeamPolicy::PerPlane | RestoringBeamPolicy::Common => fitted_beam,
            RestoringBeamPolicy::Fixed(beam) => Some(fixed_restoring_beam(beam)?),
        };
        let restored = match fitted_beam.zip(restoring_beam) {
            Some(beams) => {
                restored_terms(&family.model, &principal_residual, beams, shape, cell_size)?
            }
            None => vec![vec![0.0; cells]; terms],
        };

        let pb0 = principal_primary_beam(
            inputs,
            domain_role,
            primary_beam_model,
            directional,
            &family.weight[0],
        )?;
        let (primary_beam_validity, pb_corrected) =
            primary_beam_corrected(inputs.problem().products().validity(), &pb0, &restored)?;
        let mut primary_beam = vec![vec![0.0; cells]; terms];
        primary_beam[0] = pb0;

        let (alpha, alpha_error, alpha_validity) = spectral_index(
            inputs.problem().products().validity(),
            &principal_residual,
            &restored,
        )?;
        let alpha_pbcor = if publishes(inputs, |role| role == ProductRole::PbCorrectedSpectralIndex)
        {
            if primary_beam_model != Some(AnalyticPrimaryBeamModel::MosaicSensitivity) {
                return Err(ProductsError::UnsupportedProblem);
            }
            primary_beam_correct_spectral_index(
                &alpha,
                &sensitivity,
                &family.weight,
                &inverse,
                terms,
                &alpha_validity,
                &primary_beam_validity,
            )?
        } else {
            vec![0.0; cells]
        };

        Ok(Self {
            shape,
            principal_psf_peak: psf_peak(family.psf[0].iter().copied())?,
            psf: family.psf,
            residual: family.residual,
            model: family.model,
            restored,
            weight: family.weight,
            sensitivity,
            sum_weights: family.sum_weights,
            primary_beam,
            pb_corrected,
            alpha,
            alpha_pbcor,
            alpha_error,
            alpha_validity,
            primary_beam_validity,
            // A Taylor family is one channel of Stokes I.
            clean_mask: crate::plane::reconstruction_support_plane(
                inputs,
                domain_role,
                (0, 0),
                cells,
            ),
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
            ProductTerm::Single => Err(ProductsError::UnsupportedProductRole { role }),
        };
        match role {
            ProductRole::Psf(value) => {
                // Keep the solver/restoration Hessian's sum-weight normalization.
                // Fuse publication-only peak normalization into the payload copy.
                return Ok(self.psf[term(value)?]
                    .iter()
                    .map(|value| normalized_psf_value(*value, self.principal_psf_peak))
                    .collect());
            }
            ProductRole::Residual(value) => self.residual.get(term(value)?),
            ProductRole::Model(value) => self.model.get(term(value)?),
            ProductRole::RestoredImage(value) => self.restored.get(term(value)?),
            ProductRole::Weight(value) => self.weight.get(term(value)?),
            ProductRole::PrimaryBeam(value) => self.primary_beam.get(term(value)?),
            ProductRole::PbCorrectedImage(value) => self.pb_corrected.get(term(value)?),
            ProductRole::SpectralIndex => return Ok(self.alpha.clone()),
            ProductRole::PbCorrectedSpectralIndex => return Ok(self.alpha_pbcor.clone()),
            ProductRole::SpectralIndexError => return Ok(self.alpha_error.clone()),
            ProductRole::CleanMask => return Ok(self.clean_mask.clone()),
            ProductRole::Sensitivity => return Ok(self.sensitivity.clone()),
            ProductRole::SumWeights(value) => return Ok(vec![self.sum_weights[term(value)?]]),
            _ => None,
        }
        .cloned()
        .ok_or(ProductsError::UnsupportedProductRole { role })
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

fn taylor_alpha_products(
    principal_residual_taylor0: &[f32],
    principal_residual_taylor1: &[f32],
    restored_taylor0: &[f32],
    restored_taylor1: &[f32],
    peak_fraction: f32,
) -> (Vec<f32>, Vec<f32>, Vec<bool>) {
    let cells = restored_taylor0.len();
    let mut alpha = vec![0.0; cells];
    let mut alpha_error = vec![0.0; cells];
    let mut validity = vec![false; cells];
    let Some(positive_maximum) = principal_residual_taylor0
        .iter()
        .copied()
        .filter(|value| value.is_finite() && *value > 0.0)
        .reduce(f32::max)
    else {
        return (alpha, alpha_error, validity);
    };
    let threshold = positive_maximum * peak_fraction;
    for cell in 0..cells {
        let image0 = restored_taylor0[cell];
        if !(image0.is_finite() && image0 > threshold) {
            continue;
        }
        let image1 = restored_taylor1[cell];
        let residual0 = principal_residual_taylor0[cell];
        let residual1 = principal_residual_taylor1[cell];
        alpha[cell] = image1 / image0;
        alpha_error[cell] =
            ((image1 * residual0 / image0.powi(2)).powi(2) + (residual1 / image0).powi(2)).sqrt();
        validity[cell] = true;
    }
    (alpha, alpha_error, validity)
}

fn model_term(
    inputs: &ContinuumProductInputs<'_>,
    coefficient: usize,
    shape: [usize; 2],
) -> Result<Vec<f32>, ProductsError> {
    // A Taylor run images Stokes I on one domain (the availability gate and
    // the inputs' binding).
    let mut plane = vec![0.0; shape[0] * shape[1]];
    let samples = inputs.final_model().read_plane(0, coefficient, 0)?;
    for y in 0..shape[1] {
        for x in 0..shape[0] {
            plane[x * shape[1] + y] = samples[y * shape[0] + x].value().value() as f32;
        }
    }
    Ok(plane)
}

#[cfg(test)]
mod tests {
    use casa_imaging_model::ProductNormalization;

    use super::ResidualFamily;

    #[test]
    fn mosaic_primary_beam_takes_casa_abs_of_the_weight() {
        // `SIImageStore::makePBFromWeight`: sqrt(abs(weight)) / pbmax.
        let beam = super::MosaicSensitivity::primary_beam_from_weight(&[-0.25, 0.0, 1.0, 0.25])
            .expect("positive mosaic support");
        assert_eq!(beam, vec![0.5, 0.0, 1.0, 0.5]);
    }

    #[test]
    fn aw_taylor_normalization_keeps_residual_and_sensitivity_sum_weights_distinct() {
        let sensitivity = crate::MosaicSensitivity::new(&[16.0, 4.0, 1.0, 0.0])
            .expect("AW sensitivity")
            .with_normal_sum_weight(8.0)
            .expect("WTCF sum of weights");
        let open = casa_imaging_model::PrimaryBeamValidityPolicy::new(
            1.0e-6,
            casa_imaging_model::ProductSupportComparison::StrictlyGreater,
            casa_imaging_model::ProductBlankingPolicy::Zero,
        )
        .expect("PB policy");
        let raw = [32.0, 16.0, 8.0, 4.0];
        // CASA divides residuals by CFS sumwt=4, then uses weight=S/WTCF_sumwt
        // with WTCF_sumwt=8 for its direction-dependent denominator.
        for (normalization, expected) in [
            (ProductNormalization::FlatNoise, [4.0, 4.0, 4.0, 0.0]),
            (ProductNormalization::FlatSky, [4.0, 8.0, 16.0, 0.0]),
        ] {
            let normalized = raw
                .iter()
                .enumerate()
                .map(|(index, value)| {
                    sensitivity
                        .normalize_weighted_residual_sample(
                            *value,
                            index,
                            normalization,
                            ResidualFamily::Taylor,
                            4.0,
                            open,
                        )
                        .expect("separate AW normalization sums")
                })
                .collect::<Vec<_>>();
            for (actual, expected) in normalized.iter().zip(expected) {
                // CASA's Float stages round √weight·PBScale.
                assert!(
                    (actual - expected).abs() <= 1.0e-6 * expected,
                    "{normalization:?}: {actual} != {expected}"
                );
            }
        }
    }

    #[test]
    fn mosaic_alpha_pbcor_uses_raw_scale_invariant_sensitivity_moments() {
        let inverse = [25.0 / 23.0, -10.0 / 23.0, -10.0 / 23.0, 50.0 / 23.0];
        let published_weights = [vec![0.44, 0.36, 0.0], vec![1.8, -0.2, 0.0]];
        let corrected = super::primary_beam_correct_spectral_index(
            &[1.0, -0.5, 7.0],
            &[4.4, 3.6, 0.0],
            &published_weights,
            &inverse,
            2,
            &[true, true, false],
            &[true, true, false],
        )
        .expect("PB spectral correction");
        assert_eq!(corrected, [0.75, -0.25, 0.0]);

        let scaled = super::primary_beam_correct_spectral_index(
            &[1.0, -0.5, 7.0],
            &[44.0, 36.0, 0.0],
            &[vec![0.44, 0.36, 0.0], vec![18.0, -2.0, 0.0]],
            &inverse,
            2,
            &[true, true, false],
            &[true, true, false],
        )
        .expect("scale-invariant PB spectral correction");
        assert_eq!(scaled, corrected);
    }

    #[test]
    fn negative_only_principal_residual_has_no_taylor_support_or_payload() {
        let (alpha, alpha_error, validity) = super::taylor_alpha_products(
            &[f32::NEG_INFINITY, f32::NAN, -4.0, -1.0, 0.0],
            &[1.0; 5],
            &[8.0, 4.0, 2.0, 1.0, 0.5],
            &[2.0; 5],
            0.1,
        );
        assert!(validity.iter().all(|valid| !valid));
        assert!(alpha.iter().all(|value| *value == 0.0));
        assert!(alpha_error.iter().all(|value| *value == 0.0));
    }
}
