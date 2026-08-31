// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-local completion of one released MT-MFS normal family.

use casa_imaging_model::{
    ProductBlankingPolicy, ProductNormalization, ProductRole, ProductSupportComparison,
    ProductTerm, ProductValidityRule, RestoringBeamPolicy, TaylorSupportReference,
};
use casa_numerics::solve_symmetric_ldlt_casacore_dynamic;

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

        // CASA SIImageStore::makePBFromWeight uses the principal weight image.
        let pb_scale = weight[0]
            .iter()
            .copied()
            .map(f32::abs)
            .fold(0.0_f32, f32::max)
            .sqrt();
        if !(pb_scale.is_finite() && pb_scale > 0.0) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        let pb0 = weight[0]
            .iter()
            .map(|value| value.abs().sqrt() / pb_scale)
            .collect::<Vec<_>>();
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
