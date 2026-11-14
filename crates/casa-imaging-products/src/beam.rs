// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-facing projection of the deconvolver's PSF beam fit.

pub use casa_imaging_deconvolution::RestoringBeam;
use casa_imaging_deconvolution::{CASA_TINY_PSF_BEAM_RAD, PsfBeamFitError};
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

/// What fitting one plane's PSF gives.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum PlaneBeam {
    /// The plane's own beam.
    Fitted(RestoringBeam),
    /// The plane has no PSF, and so no beam.
    Blank,
    /// The fit was refused for a weak peak, an off-centre peak, or collinear
    /// samples. `SIImageStore::makeImageBeamSet` provides the domain beam.
    Refused,
}

/// [`fit_restoring_beam`], as `SIImageStore::makeImageBeamSet` catches the
/// refusal; any other failure stays a failure.
///
/// # Errors
///
/// As [`fit_restoring_beam`], except for an off-centre peak or collinear samples.
pub(crate) fn fit_plane_beam(
    psf: &[f32],
    shape: [usize; 2],
    cell_size_rad: [f64; 2],
    cutoff: f32,
) -> Result<PlaneBeam, ProductsError> {
    match casa_imaging_deconvolution::fit_restoring_beam(psf, shape, cell_size_rad, cutoff) {
        Ok(beam) => Ok(PlaneBeam::Fitted(beam)),
        Err(
            error @ (PsfBeamFitError::PeakOutsideInnerQuarter | PsfBeamFitError::CollinearMainLobe),
        ) => {
            tracing::warn!(
                "Error Gaussian fit to PSF : {error} : Setting restoring beam to largest valid beam."
            );
            Ok(PlaneBeam::Refused)
        }
        Err(error) => Err(ProductsError::BeamFitFailed(error.to_string())),
    }
}

/// The beams of `fits`, `planes` of them to a domain: a refused plane takes
/// its domain's largest (`getMaxAreaBeam`), or, where no plane has one, the
/// tiny beam `SIImageStore::makeImageBeamSet` stores instead of a beam of no
/// area. A plane with no PSF keeps none.
pub(crate) fn settle_beams(fits: &[PlaneBeam], planes: usize) -> Box<[Option<RestoringBeam>]> {
    let mut settled = Vec::with_capacity(fits.len());
    for domain in fits.chunks(planes.max(1)) {
        let largest = domain
            .iter()
            .filter_map(|fit| match fit {
                PlaneBeam::Fitted(beam) => Some(*beam),
                _ => None,
            })
            .max_by(|left, right| {
                (left.major_fwhm_rad() * left.minor_fwhm_rad())
                    .total_cmp(&(right.major_fwhm_rad() * right.minor_fwhm_rad()))
            });
        settled.extend(domain.iter().map(|fit| match fit {
            PlaneBeam::Fitted(beam) => Some(*beam),
            PlaneBeam::Blank => None,
            PlaneBeam::Refused => Some(largest.unwrap_or_else(|| {
                RestoringBeam::new(CASA_TINY_PSF_BEAM_RAD, CASA_TINY_PSF_BEAM_RAD, 0.0)
                    .expect("a positive round beam")
            })),
        }));
    }
    settled.into_boxed_slice()
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

    fn round(fwhm: f64) -> RestoringBeam {
        RestoringBeam::new(fwhm, fwhm, 0.0).expect("a beam")
    }

    /// A lobe at `centre` of a plane of `[64, 32]`, x-major.
    fn lobe(centre: [f64; 2]) -> Vec<f32> {
        let mut psf = vec![0.0_f32; 64 * 32];
        for x in 0..64 {
            for y in 0..32 {
                let [dx, dy] = [x as f64 - centre[0], y as f64 - centre[1]];
                psf[x * 32 + y] = (-(dx * dx + dy * dy) / 8.0).exp() as f32;
            }
        }
        psf
    }

    /// Only the inner-quarter refusal is caught: a fit that fails otherwise
    /// stays a failure. The plane's axes are told apart on a non-square one.
    #[test]
    fn only_a_peak_outside_the_inner_quarter_is_left_to_another_plane() {
        let cells = [1.0e-5, 1.0e-5];
        let fit = |psf: &[f32]| fit_plane_beam(psf, [64, 32], cells, 0.35);
        assert!(matches!(fit(&lobe([32.0, 16.0])), Ok(PlaneBeam::Fitted(_))));
        // Inside both quarters of the plane, though y = 20 is outside x's.
        assert!(matches!(fit(&lobe([32.0, 20.0])), Ok(PlaneBeam::Fitted(_))));
        assert_eq!(fit(&lobe([8.0, 16.0])).expect("a fit"), PlaneBeam::Refused);
        assert_eq!(fit(&lobe([32.0, 26.0])).expect("a fit"), PlaneBeam::Refused);
        assert!(matches!(
            fit(&vec![f32::NAN; 64 * 32]),
            Err(ProductsError::BeamFitFailed(_))
        ));
    }

    /// `makeImageBeamSet`: each domain's refused planes take its largest
    /// beam, a domain with none a tiny one, and a blank plane keeps none.
    #[test]
    fn a_refused_plane_takes_its_domains_largest_beam() {
        let (small, large) = (round(1.0e-5), round(2.0e-5));
        let settled = settle_beams(
            &[
                PlaneBeam::Fitted(small),
                PlaneBeam::Refused,
                PlaneBeam::Fitted(large),
                PlaneBeam::Blank,
                PlaneBeam::Refused,
                PlaneBeam::Blank,
            ],
            3,
        );
        assert_eq!(settled[..4], [Some(small), Some(large), Some(large), None]);
        let tiny = settled[4].expect("a beam");
        assert!(tiny.major_fwhm_rad() < 1.0e-11);
        assert_eq!(settled[5], None);
    }
}
