// SPDX-License-Identifier: LGPL-3.0-or-later
//! What the minor cycle needs to know about a PSF, measured once per PSF.

use crate::Error;
use crate::beam::{
    DEFAULT_PSF_FIT_CUTOFF, RestoringBeam, fit_main_lobe, fitted_psf_sidelobe_fraction_with_beam,
    peak_outside_inner_quarter,
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
    off_centre: bool,
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
    /// The largest PSF magnitude outside the patch, by casacore's
    /// asymmetric box about the plane centre; 0 when the patch is the plane.
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
        Self::with_cutoff(psf, shape, DEFAULT_PSF_FIT_CUTOFF)
    }

    /// Measure a PSF using the requested CASA main-lobe fit cutoff.
    ///
    /// # Errors
    ///
    /// When the PSF has no positive finite peak or the main lobe cannot be fitted.
    pub fn with_cutoff(psf: &[f64], shape: PlaneShape, cutoff: f32) -> Result<Self, Error> {
        Self::measure(psf, shape, cutoff, None)
    }

    /// Measure a PSF with a fitted beam recovered from its image metadata.
    /// The beam widths use pixels, as in [`Self::beam`].
    ///
    /// # Errors
    ///
    /// When the PSF has no positive finite peak.
    pub fn with_saved_beam(
        psf: &[f64],
        shape: PlaneShape,
        beam: RestoringBeam,
    ) -> Result<Self, Error> {
        Self::measure(psf, shape, DEFAULT_PSF_FIT_CUTOFF, Some(beam))
    }

    /// A blank plane's saved beam for automatic-mask common-beam selection.
    /// No solver may use its peak or patch: the plane has no PSF samples.
    #[must_use]
    pub fn blank_with_saved_beam(beam: RestoringBeam) -> Self {
        Self {
            peak: 0,
            beam,
            sidelobe: 0.0,
            clark: ClarkPatch {
                radius: [0, 0],
                size: [0, 0],
                exterior_sidelobe: 0.0,
            },
            off_centre: false,
        }
    }

    /// Copy this summary with the beam used by automatic masking. The PSF's
    /// sidelobe and Clark patch remain those measured from the PSF beam.
    #[must_use]
    pub const fn with_mask_beam(mut self, beam: RestoringBeam) -> Self {
        self.beam = beam;
        self
    }

    fn measure(
        psf: &[f64],
        shape: PlaneShape,
        cutoff: f32,
        saved_beam: Option<RestoringBeam>,
    ) -> Result<Self, Error> {
        let peak = psf_peak(psf, shape).ok_or(Error::PsfPeak)?;
        if !(psf[peak].is_finite() && psf[peak] > 0.0) {
            return Err(Error::PsfPeak);
        }
        let single = psf.iter().map(|value| *value as f32).collect::<Vec<_>>();
        let axes = [shape.nx, shape.ny];
        // A plane peaking away from its centre has no beam of its own in CASA,
        // which takes its domain's largest and subtracts a Gaussian at the
        // centre, far from that peak, which so stays the sidelobe. Its beam
        // here is a pixel until the run knows the other planes
        // ([`Self::with_domain_beam`]).
        let off_centre = peak_outside_inner_quarter(&single, axes)?;
        let (beam, sidelobe) = if off_centre {
            let level = psf
                .iter()
                .fold(0.0_f64, |level, value| level.max(value.abs()));
            (
                match saved_beam {
                    Some(beam) => beam,
                    None => RestoringBeam::new(1.0, 1.0, 0.0)?,
                },
                level,
            )
        } else {
            let beam = match saved_beam {
                Some(beam) => beam,
                None => fit_main_lobe(&single, axes, [1.0, 1.0], cutoff)?,
            };
            let sidelobe = fitted_psf_sidelobe_fraction_with_beam(&single, axes, beam)?;
            (beam, sidelobe)
        };
        Ok(Self {
            peak,
            beam,
            sidelobe,
            clark: ClarkPatch::new(psf, shape, beam),
            off_centre,
        })
    }

    /// Whether the plane's PSF peaks outside its inner quarter, so it has no
    /// beam of its own (`SIImageStore::makeImageBeamSet`).
    #[must_use]
    pub const fn off_centre(&self) -> bool {
        self.off_centre
    }

    /// This summary of the normalised PSF `psf` with its domain's largest
    /// beam, and Clark's patch for it, as CASA gives a plane with none of its
    /// own (`SDAlgorithmClarkClean2` reads the patch from the beam set). The
    /// sidelobe stays as measured.
    #[must_use]
    pub fn with_domain_beam(self, psf: &[f64], shape: PlaneShape, beam: RestoringBeam) -> Self {
        Self {
            beam,
            clark: ClarkPatch::new(psf, shape, beam),
            ..self
        }
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
    fn new(psf: &[f64], shape: PlaneShape, beam: RestoringBeam) -> Self {
        let central = 4_usize
            .max(beam.major_fwhm_rad().ceil() as usize)
            .max(beam.minor_fwhm_rad().ceil() as usize);
        let requested = 3 * central + 1;
        let size = [requested.min(shape.nx), requested.min(shape.ny)];
        let radius = [size[0] / 2, size[1] / 2];
        // ClarkCleanLatModel::absMaxBeyondDist about the PSF origin
        // (psfShape/2): rows beyond ±d, and in the other rows the columns
        // left of −d and from +d on. A patch as large as the plane leaves no
        // exterior (getPsfPatch's user value, 0).
        let [cx, cy] = shape.centre();
        let exterior_sidelobe = if size == [shape.nx, shape.ny] {
            0.0
        } else {
            psf.iter()
                .enumerate()
                .filter(|(index, _)| {
                    let [x, y] = shape.pixel(*index);
                    y + radius[1] < cy
                        || y > cy + radius[1]
                        || x + radius[0] < cx
                        || x >= cx + radius[0]
                })
                .fold(0.0_f64, |maximum, (_, value)| maximum.max(value.abs()))
        };
        Self {
            radius,
            size,
            exterior_sidelobe,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A smooth main lobe at `centre` of a 64-pixel plane, x-major.
    fn lobe(centre: [usize; 2]) -> Vec<f64> {
        let mut psf = vec![0.0; 64 * 64];
        for x in 0..64 {
            for y in 0..64 {
                let dx = x as f64 - centre[0] as f64;
                let dy = y as f64 - centre[1] as f64;
                psf[x * 64 + y] = (-(dx * dx + dy * dy) / 8.0).exp();
            }
        }
        psf
    }

    /// `getPSFSidelobeLevel` subtracts a Gaussian at the image's centre, so a
    /// plane peaking far from it keeps that peak as its sidelobe, where one
    /// peaking at the centre subtracts its main lobe.
    #[test]
    fn a_plane_peaking_off_centre_has_its_peak_as_its_sidelobe() {
        let shape = PlaneShape::new(64, 64);
        let centred = PsfSummary::new(&lobe([32, 32]), shape).expect("a summary");
        assert!(centred.sidelobe() < 0.2, "{}", centred.sidelobe());
        let off = PsfSummary::new(&lobe([8, 40]), shape).expect("a summary");
        assert_eq!(off.sidelobe(), 1.0);
        // No fit, so a spike too narrow for one is no failure.
        let mut spike = vec![0.0; 64 * 64];
        spike[32 * 64 + 32] = 0.99;
        spike[8 * 64 + 40] = 1.0;
        assert_eq!(
            PsfSummary::new(&spike, shape)
                .expect("a summary")
                .sidelobe(),
            1.0
        );
    }
}
