// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-facing projection of the deconvolver's PSF beam fit.

pub use casa_imaging_deconvolution::RestoringBeam;
use casa_imaging_model::FixedRestoringBeam;

use crate::error::ProductsError;

/// Fit a restoring beam from one real PSF plane stored x-major.
///
/// # Errors
///
/// Returns [`ProductsError::BeamFitFailed`] when CASA's Gaussian fit does
/// not converge.
pub fn fit_restoring_beam(
    psf: &[f32],
    shape: [usize; 2],
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> Result<RestoringBeam, ProductsError> {
    casa_imaging_deconvolution::fit_restoring_beam(psf, shape, cell_size_rad, cutoff)
        .map_err(|error| ProductsError::BeamFitFailed(error.to_string()))
}

/// A plane's own beam, or `None` when its fit fails, as
/// `SIImageStore::makeImageBeamSet` warns and leaves it to its domain's largest
/// beam ([`crate::generation`]).
pub(crate) fn fit_plane_beam(
    psf: &[f32],
    shape: [usize; 2],
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> Option<RestoringBeam> {
    fit_restoring_beam(psf, shape, cell_size_rad, cutoff)
        .inspect_err(|error| {
            tracing::warn!(
                "Error Gaussian fit to PSF : {error} : Setting restoring beam to largest valid beam."
            );
        })
        .ok()
}

/// The restoring beam a request fixed, as the products restore with it and
/// record it.
///
/// # Errors
///
/// [`ProductsError::BeamFitFailed`] if the model's validated beam is not a
/// restoring beam, which cannot happen for a beam the model accepted.
pub(crate) fn fixed_restoring_beam(
    beam: FixedRestoringBeam,
) -> Result<RestoringBeam, ProductsError> {
    RestoringBeam::new(
        beam.major_fwhm_rad(),
        beam.minor_fwhm_rad(),
        beam.position_angle_rad(),
    )
    .map_err(|error| ProductsError::BeamFitFailed(error.to_string()))
}
