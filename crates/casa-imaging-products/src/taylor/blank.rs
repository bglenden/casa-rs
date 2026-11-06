// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA MVC's unrestored, all-zero Float Taylor family (#1069).

use casa_imaging_deconvolution::FinalNormalStateWindow;
use casa_imaging_model::ProductRole;

use super::{TaylorFamily, TaylorPlane, TaylorProducts};
use crate::{AnalyticPrimaryBeamModel, ContinuumProductInputs, ProductsError};

/// Publish only the already-blank Taylor products and common beam. This path
/// never inverts a singular Hessian or supplies a restoring beam. The generic
/// direct-Taylor nonpositive-weight refusal remains in `SolvedFamily::read`.
pub(super) fn build(
    inputs: &ContinuumProductInputs<'_>,
    state: &FinalNormalStateWindow<'_>,
    primary_beam_model: Option<AnalyticPrimaryBeamModel>,
) -> Result<TaylorProducts, ProductsError> {
    let list = inputs.problem().product_list();
    if state.coefficient_term_count() != 2
        || list.publication().members().iter().any(|id| {
            !matches!(
                list.entries()[id.ordinal()].role(),
                ProductRole::Psf(_)
                    | ProductRole::Residual(_)
                    | ProductRole::SumWeights(_)
                    | ProductRole::PrimaryBeam(_)
            )
        })
    {
        return Err(ProductsError::InvalidTaylorMomentReduction);
    }
    let family = TaylorFamily::read(inputs, (state, 0), primary_beam_model, None, 1.0)?;
    if family.sum_weights.iter().any(|value| *value != 0.0)
        || family.psf.iter().flatten().any(|value| *value != 0.0)
        || family.residual.iter().flatten().any(|value| *value != 0.0)
        || family.model.iter().flatten().any(|value| *value != 0.0)
    {
        return Err(ProductsError::InvalidTaylorMomentReduction);
    }
    let beam = inputs
        .taylor_primary_beam()
        .expect("MVC beam was checked")
        .to_vec();
    let cutoff = inputs
        .problem()
        .products()
        .validity()
        .primary_beam()
        .cutoff();
    let validity = beam
        .iter()
        .map(|value| value.is_finite() && *value > cutoff)
        .collect();
    Ok(TaylorProducts {
        shape: state.shape(),
        planes: vec![TaylorPlane {
            psf: family.psf,
            principal_psf_peak: 0.0,
            residual: family.residual,
            model: family.model,
            restored: Vec::new(),
            weight: family.weight,
            sensitivity: Vec::new(),
            sum_weights: family.sum_weights,
            primary_beam: vec![beam, Vec::new()],
            pb_corrected: Vec::new(),
            spectral_index: None,
            primary_beam_validity: validity,
            clean_mask: Vec::new(),
            fitted_beam: None,
            restoring_beam: None,
        }],
    })
}
