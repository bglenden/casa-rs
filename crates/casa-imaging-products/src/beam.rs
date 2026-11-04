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

#[cfg(test)]
mod tests {
    use super::*;

    /// `makeImageBeamSet` warns and leaves a plane whose fit fails to the
    /// largest beam; a fit that works is the plane's own.
    #[test]
    fn a_plane_whose_fit_is_refused_has_no_beam_of_its_own() {
        let lobe = |centre: [f64; 2]| {
            let mut psf = vec![0.0_f32; 64 * 64];
            for x in 0..64 {
                for y in 0..64 {
                    let [dx, dy] = [x as f64 - centre[0], y as f64 - centre[1]];
                    psf[x * 64 + y] = (-(dx * dx + dy * dy) / 8.0).exp() as f32;
                }
            }
            psf
        };
        let cells = [1.0e-5, 1.0e-5];
        assert!(fit_plane_beam(&lobe([32.0, 32.0]), [64, 64], cells, 0.35).is_some());
        assert!(fit_plane_beam(&lobe([8.0, 40.0]), [64, 64], cells, 0.35).is_none());
    }
}
