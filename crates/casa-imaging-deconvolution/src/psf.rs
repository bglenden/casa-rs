// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the minor cycle needs to know about a PSF, measured once per PSF.

use crate::Error;
use crate::beam::{
    DEFAULT_PSF_FIT_CUTOFF, RestoringBeam, fit_restoring_beam,
    fitted_psf_sidelobe_fraction_with_beam,
};
use crate::plane::{PlaneShape, psf_peak};

/// The measurements of one PSF plane that every minor cycle reuses: they
/// change only when the PSF does, so a run measures them once.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PsfSummary {
    peak: usize,
    beam: RestoringBeam,
    sidelobe: f64,
    clark: ClarkPatch,
}

/// Clark's PSF patch (`SDAlgorithmClarkClean2`): the square the active
/// pixels are updated within, and the largest PSF value outside it.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClarkPatch {
    /// Half the patch width per axis; an even patch ends one pixel early on
    /// its positive side, as in casacore.
    pub radius: [usize; 2],
    /// The patch width per axis.
    pub size: [usize; 2],
    /// The largest PSF magnitude outside the patch.
    pub exterior_sidelobe: f64,
}

impl PsfSummary {
    /// Measure a PSF plane normalised to peak 1.
    ///
    /// The peak is found as `MatrixCleaner::findPSFMaxAbs` finds it; the
    /// beam is CASA's main-lobe fit in pixel units with `psfcutoff` 0.35;
    /// the sidelobe is CASA's fitted sidelobe level
    /// (`SIImageStore::getPSFSidelobeLevel`).
    ///
    /// # Errors
    ///
    /// When the PSF has no positive finite peak or the main lobe cannot be
    /// fitted.
    pub fn new(psf: &[f64], shape: PlaneShape) -> Result<Self, Error> {
        let peak = psf_peak(psf, shape).ok_or(Error::PsfPeak)?;
        if !(psf[peak].is_finite() && psf[peak] > 0.0) {
            return Err(Error::PsfPeak);
        }
        let single = psf.iter().map(|value| *value as f32).collect::<Vec<_>>();
        let axes = [shape.nx, shape.ny];
        let beam = fit_restoring_beam(&single, axes, [1.0, 1.0], DEFAULT_PSF_FIT_CUTOFF)?;
        let sidelobe = fitted_psf_sidelobe_fraction_with_beam(&single, axes, beam)?;
        Ok(Self {
            peak,
            beam,
            sidelobe,
            clark: ClarkPatch::new(psf, shape, peak, beam),
        })
    }

    /// Storage index of the PSF peak.
    #[must_use]
    pub const fn peak(&self) -> usize {
        self.peak
    }

    /// The fitted main lobe, in pixels.
    #[must_use]
    pub const fn beam(&self) -> RestoringBeam {
        self.beam
    }

    /// The fitted sidelobe level the cycle threshold scales by.
    #[must_use]
    pub const fn sidelobe(&self) -> f64 {
        self.sidelobe
    }

    /// Clark's patch.
    #[must_use]
    pub const fn clark(&self) -> ClarkPatch {
        self.clark
    }
}

impl ClarkPatch {
    /// At least four pixels, otherwise the ceiling of the fitted widths;
    /// a `3·ncent + 1` square capped by the plane.
    fn new(psf: &[f64], shape: PlaneShape, peak: usize, beam: RestoringBeam) -> Self {
        let central = 4_usize
            .max(beam.major_fwhm_rad().ceil() as usize)
            .max(beam.minor_fwhm_rad().ceil() as usize);
        let requested = 3 * central + 1;
        let size = [requested.min(shape.nx), requested.min(shape.ny)];
        let radius = [size[0] / 2, size[1] / 2];
        let [px, py] = shape.pixel(peak);
        let exterior_sidelobe = psf
            .iter()
            .enumerate()
            .filter(|(index, _)| {
                let [x, y] = shape.pixel(*index);
                x.abs_diff(px) > radius[0] || y.abs_diff(py) > radius[1]
            })
            .fold(0.0_f64, |maximum, (_, value)| maximum.max(value.abs()));
        Self {
            radius,
            size,
            exterior_sidelobe,
        }
    }
}
