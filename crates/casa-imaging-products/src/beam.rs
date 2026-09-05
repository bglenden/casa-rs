// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-facing projection of the reconstruction owner's PSF beam fit.

pub use casa_imaging_reconstruction::RestoringBeam;

use crate::error::ProductsError;

/// Fit a restoring beam from one real PSF plane stored x-major.
///
/// # Errors
///
/// Returns [`ProductsError::BeamFitFailed`] when the reconstruction owner's
/// CASA-style Gaussian fit does not converge.
pub fn fit_restoring_beam(
    psf: &[f32],
    shape: [usize; 2],
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> Result<RestoringBeam, ProductsError> {
    casa_imaging_reconstruction::fit_restoring_beam(psf, shape, cell_size_rad, cutoff)
        .map_err(|error| ProductsError::BeamFitFailed(error.to_string()))
}
