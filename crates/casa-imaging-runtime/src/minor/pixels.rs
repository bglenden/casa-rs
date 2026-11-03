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
//! (`SIImageStore::calcRobustRMS`), or every pixel when no plane of the
//! image keeps one (CAS-13318, decided over the whole cube: a plane the
//! beam leaves empty in an image it does not measures nothing); every beam
//! CASA publishes is zero where it is not above
//! `|pblimit|`, so with `pbmask` 0 these too are the beam's support. Under
//! an automatic mask, the noise is the mask's own statistics, except at
//! the run's first boundary, where the mask takes `calcRobustRMS`'s
//! (`SynthesisDeconvolver::initMinorCycle`, `SDMaskHandler::autoMask`).
//!
//! The beam is the weight image's under a response (`makePBFromWeight`)
//! and the telescope's analytic beam otherwise (`makePBImage`), the one
//! the products publish.

use std::borrow::Cow;

use casa_imaging_deconvolution::{
    AutoMaskPlane, FinalNormalState, ModelGeneration, NoiseEstimate, NoiseMaskExclusion,
    RobustNoise, Support, plane_noise,
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

    /// The pixels `calcRobustRMS` measures: the beam's, even none, while
    /// the plane's image domain has a beam pixel in some plane
    /// (`domain_beam`); every valid pixel otherwise, or when the run has
    /// no beam.
    pub(super) fn robust_rms(&self, domain_beam: bool) -> &Support {
        match &self.beam {
            Some(beam) if domain_beam => beam,
            _ => self.valid,
        }
    }

    /// The pixels the plane's `nsigma` noise reads under `setup`, at a
    /// boundary after `completed` major cycles, in an image domain whose
    /// beam keeps a pixel in some plane when `domain_beam`.
    pub(super) fn noise(
        &self,
        setup: &MinorCycleSetup<'_>,
        completed: usize,
        domain_beam: bool,
    ) -> &Support {
        if setup.automask && completed > 0 {
            self.residual_mask(setup.residual_pixel_mask)
        } else {
            self.robust_rms(domain_beam)
        }
    }
}

/// The plane an automatic mask reads, of `residual` with its `valid`
/// pixels and their part inside the `.pb`, `beam`, under `setup`: its
/// pixel mask, and at the `first` boundary of a run with `nsigma`, the
/// pixels `calcRobustRMS` measured when they differ, the beam's while its
/// image domain keeps a beam pixel in some plane (`domain_beam`).
pub(super) fn automask_plane(
    setup: &MinorCycleSetup<'_>,
    (first, domain_beam): (bool, bool),
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
    // when the image's beam keeps none.
    let beam_measured = domain_beam;
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

/// Whether each image domain's `.pb` keeps a pixel in some plane of `keys`
/// that holds data: CAS-13318's fallback to every pixel is
/// `calcRobustRMS`'s for an image whose beam keeps none, decided over the
/// whole cube. Planes are read one at a time until one keeps a pixel.
pub(super) fn domain_beams(
    normal: &FinalNormalState,
    base: &ModelGeneration,
    setup: &MinorCycleSetup<'_>,
    taylor: bool,
    keys: &[PlaneKey],
) -> Result<Box<[bool]>, MinorCycleRunError> {
    let roles = base.shape().domain_roles();
    let mut beams = vec![false; normal.domain_count()];
    for key in keys {
        if beams[key.domain] || !super::holds_data(normal, taylor, *key) {
            continue;
        }
        beams[key.domain] = beam_keeps_a_pixel(normal, setup, (roles, *key, taylor))?;
    }
    Ok(beams.into_boxed_slice())
}

/// Whether plane `key`'s `.pb` exceeds its cutoff anywhere: the weight
/// image's Float beam under a response (its polarization's Taylor family's
/// principal one),
/// else the analytic beam at the plane's channel; true for a run without
/// a beam, which never falls back.
fn beam_keeps_a_pixel(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup<'_>,
    (roles, key, taylor): (&[ImageDomainRole], PlaneKey, bool),
) -> Result<bool, MinorCycleRunError> {
    let Some(response) = setup.response else {
        return match &setup.primary_beam {
            Some(beam) => Ok(beam
                .support(&roles[key.domain], if taylor { 0 } else { key.channel })?
                .iter()
                .any(|kept| *kept)),
            None => Ok(true),
        };
    };
    let (sensitivity, sum_weight) = if taylor {
        let window = normal.read_window(normal.slab().core_range())?;
        let principal = window
            .normal_moment(0, key.polarization)
            .expect("a Taylor family has its zeroth moment");
        (
            Cow::Owned(principal.sensitivity().to_vec()),
            principal.sum_weight(),
        )
    } else {
        let plane = normal.read_plane(key.domain, key.channel, key.polarization)?;
        (plane.read_sensitivity()?, plane.sum_weight())
    };
    let beam = MosaicSensitivity::new(&sensitivity)?.with_normal_sum_weight(sum_weight)?;
    for index in 0..sensitivity.len() {
        if beam.weighted_primary_beam_positive_at(index, response.policy())? {
            return Ok(true);
        }
    }
    Ok(false)
}
