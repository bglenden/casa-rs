// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pixels a plane's measurements read, as tclean's image store reads
//! them.
//!
//! tclean's residual carries the `.pb`'s support (`pb > |pblimit|`) as its
//! pixel mask wherever the run has a `.pb` and `pblimit` is not negative
//! (`SIImageStore::divideResidualByWeight` copies it). The residual's
//! peaks (`getPeakResidual`, `getPeakResidualWithinMask`) and its
//! automatic mask's statistics, pruning and support
//! (`SDMaskHandler::autoMask`) read only the pixels it keeps. The `nsigma`
//! noise reads the pixels where the `.pb` exceeds `pbmask`, 0
//! (`SIImageStore::calcRobustRMS`), or every pixel when none does
//! (CAS-13318); every beam CASA publishes is zero where it is not above
//! `|pblimit|`, so with `pbmask` 0 these too are the beam's support. Under
//! an automatic mask, the noise is the mask's own statistics, except at
//! the run's first boundary, where the mask takes `calcRobustRMS`'s
//! (`SynthesisDeconvolver::initMinorCycle`, `SDMaskHandler::autoMask`).
//!
//! The beam is the weight image's under a response (`makePBFromWeight`)
//! and the telescope's analytic beam otherwise (`makePBImage`), the one
//! the products publish.

use casa_imaging_deconvolution::{
    AutoMaskPlane, NoiseEstimate, NoiseMaskExclusion, RobustNoise, Support, plane_noise,
};
use casa_imaging_model::ImageDomainRole;
use casa_imaging_products::{MosaicSensitivity, ProductsError};

use super::{MinorCycleRunError, MinorCycleSetup, PlaneKey, ResponseBinding};

/// Whether a minor cycle under `setup` reads its planes' beams: for the
/// `nsigma` noise, or for the residual's pixel mask.
pub(super) const fn reads_beam(setup: &MinorCycleSetup<'_>) -> bool {
    setup.nsigma > 0.0 || setup.residual_pixel_mask
}

/// The valid pixels of plane `key` where its `.pb` exceeds the cutoff,
/// x-major: the weight image's Float beam under `response`, else the run's
/// analytic beam at the plane's channel (channel zero for a Taylor
/// family). `None` for a run without a `.pb`.
pub(super) fn beam_pixels(
    setup: &MinorCycleSetup<'_>,
    (roles, key, taylor): (&[ImageDomainRole], PlaneKey, bool),
    valid: &[bool],
    response: Option<&ResponseBinding<'_>>,
) -> Result<Option<Vec<bool>>, MinorCycleRunError> {
    if let Some(binding) = response {
        let beam = MosaicSensitivity::new(&binding.sensitivity)?
            .with_normal_sum_weight(binding.sum_weight)?;
        let policy = binding.response.policy();
        let pixels = valid
            .iter()
            .enumerate()
            .map(|(index, valid)| {
                Ok(*valid && beam.weighted_primary_beam_positive_at(index, policy)?)
            })
            .collect::<Result<Vec<_>, ProductsError>>()?;
        return Ok(Some(pixels));
    }
    let Some(beam) = &setup.primary_beam else {
        return Ok(None);
    };
    let channel = if taylor { 0 } else { key.channel };
    let mut pixels = beam.support(&roles[key.domain], channel)?;
    for (kept, valid) in pixels.iter_mut().zip(valid) {
        *kept &= *valid;
    }
    Ok(Some(pixels))
}

/// One plane's pixels as its preparation measures them: its valid pixels
/// and their part inside the `.pb`, when the run reads it.
pub(super) struct PlanePixels<'p> {
    pub(super) valid: &'p Support,
    pub(super) beam: Option<Support>,
}

impl PlanePixels<'_> {
    /// The pixels the residual's pixel mask keeps: the beam's under
    /// `masks_residual`, else every valid pixel.
    pub(super) fn residual_mask(&self, masks_residual: bool) -> &Support {
        match &self.beam {
            Some(beam) if masks_residual => beam,
            _ => self.valid,
        }
    }

    /// The pixels `calcRobustRMS` measures: the beam's, or every valid
    /// pixel when the beam keeps none or the run has none.
    pub(super) fn robust_rms(&self) -> &Support {
        match &self.beam {
            Some(beam) if !beam.is_empty() => beam,
            _ => self.valid,
        }
    }

    /// The pixels the plane's `nsigma` noise reads under `setup`, at a
    /// boundary after `completed` major cycles.
    pub(super) fn noise(&self, setup: &MinorCycleSetup<'_>, completed: usize) -> &Support {
        if setup.automask && completed > 0 {
            self.residual_mask(setup.residual_pixel_mask)
        } else {
            self.robust_rms()
        }
    }
}

/// The plane an automatic mask reads, of `residual` with its `valid`
/// pixels and their part inside the `.pb`, `beam`, under `setup`: its
/// pixel mask, and at the first boundary of a run with `nsigma`, the
/// pixels `calcRobustRMS` measured when they differ.
pub(super) fn automask_plane(
    setup: &MinorCycleSetup<'_>,
    first: bool,
    residual: Vec<f64>,
    valid: Vec<bool>,
    beam: Option<Vec<bool>>,
) -> AutoMaskPlane {
    let Some(beam) = beam else {
        return AutoMaskPlane {
            residual,
            valid,
            statistics: None,
        };
    };
    // `calcRobustRMS` measures the beam's pixels, or every valid pixel
    // when the beam keeps none.
    let beam_measured = beam.iter().any(|kept| *kept);
    let reuses = first && setup.nsigma > 0.0;
    let (valid, statistics) = match (setup.residual_pixel_mask, reuses, beam_measured) {
        (true, true, false) => (beam, Some(valid)),
        (true, _, _) => (beam, None),
        (false, true, true) => (valid, Some(beam)),
        (false, _, _) => (valid, None),
    };
    AutoMaskPlane {
        residual,
        valid,
        statistics,
    }
}

/// tclean's robust noise of one plane under `fastnoise=False`
/// (`SIImageStore::calcRobustRMS`, `SDMaskHandler::autoMask`): Chauvenet
/// statistics of the measured pixels without a mask, the classical
/// statistics outside the mask otherwise, and zero for a plane whose
/// automatic mask stopped evolving.
pub(super) fn robust_plane_noise(
    residual: &[f64],
    measured: &Support,
    exclusion: NoiseMaskExclusion<'_>,
) -> Option<RobustNoise> {
    let measured = measured.as_slice();
    match exclusion {
        NoiseMaskExclusion::Stopped => Some(RobustNoise {
            median: 0.0,
            rms: 0.0,
        }),
        NoiseMaskExclusion::Unmasked => {
            plane_noise(residual, measured, NoiseEstimate::Robust, None, false)
        }
        NoiseMaskExclusion::Mask { support, full } => plane_noise(
            residual,
            measured,
            NoiseEstimate::Robust,
            Some(support),
            full,
        ),
    }
}
