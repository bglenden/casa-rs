// SPDX-License-Identifier: LGPL-3.0-or-later
//! The pixels a plane's measurements read, as tclean's image store reads
//! them.
//!
//! tclean's residual carries the `.pb`'s support (`pb > |pblimit|`) as its
//! pixel mask wherever the run has a `.pb` and `pblimit` is not negative
//! (`SIImageStore::divideResidualByWeight` copies it). The residual's
//! peaks (`getPeakResidual`, `getPeakResidualWithinMask`) read only the
//! pixels it keeps. Every beam CASA publishes is zero where it is not
//! above `|pblimit|`, so `pb > pbmask` keeps no pixel outside that support,
//! and at tclean's default `pbmask` of 0 it is the support.
//!
//! * The `nsigma` noise reads the pixels where the `.pb` exceeds `pbmask`
//!   (`SIImageStore::calcRobustRMS`), or every pixel when no plane of the
//!   image keeps one (CAS-13318, decided over the whole cube: a plane the
//!   beam leaves empty in an image it does not measures nothing).
//! * An automatic mask's statistics, pruning and support read the
//!   residual's pixel mask (`SDMaskHandler::autoMask`), which a `pbmask`
//!   above zero replaces with `pb > pbmask`, whatever `pblimit`'s sign
//!   (`autoMaskWithinPB`). Under an automatic mask the noise is the mask's
//!   own statistics, except at the run's first boundary, where the mask
//!   takes `calcRobustRMS`'s (`SynthesisDeconvolver::initMinorCycle`).
//! * A mask cut to the primary beam (`usemask='pb'`, or a named mask under
//!   a `pbmask` above zero) keeps `pb > pbmask` in every plane
//!   (`SDMaskHandler::makePBMask`), read here for the mask's plans
//!   ([`plane_beam`]).
//!
//! The beam is the weight image's under a response (`makePBFromWeight`)
//! and the telescope's analytic beam otherwise (`makePBImage`), the one
//! the products publish.

use std::borrow::Cow;

#[cfg(test)]
std::thread_local! {
    static DOMAIN_BEAM_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static CHANNEL_BEAM_FORMS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
#[path = "pixels_tests.rs"]
mod tests;

use casa_imaging_deconvolution::{
    AutoMaskPlane, FinalNormalState, ModelGeneration, NoiseEstimate, NoiseMaskExclusion,
    RobustNoise, Support, plane_noise,
};
use casa_imaging_model::ImageDomainRole;
use casa_imaging_products::{MosaicSensitivity, ProductsError};

use super::{MinorCycleRunError, MinorCycleSetup, PlaneKey, ResponseBinding};

/// Whether a minor cycle under `setup` reads its planes' beams: for the
/// `nsigma` noise, for the residual's pixel mask, or for an automatic
/// mask's pixel mask at a `pbmask` above zero.
pub(super) const fn reads_beam(setup: &MinorCycleSetup<'_>) -> bool {
    setup.nsigma > 0.0 || setup.residual_pixel_mask || (setup.automask && setup.pbmask > 0.0)
}

/// tclean's `pbmask` as its Float `.pb` is compared with it.
pub(super) fn pbmask(setup: &MinorCycleSetup<'_>) -> f32 {
    setup.pbmask as f32
}

/// One plane's `.pb` inside its valid pixels, x-major: where it is
/// positive, above the cutoff (`pb > |pblimit|`, the residual's pixel
/// mask), and where it exceeds a `pbmask` above zero (`pb > pbmask`).
#[derive(Clone)]
pub(super) struct BeamPixels {
    support: Vec<bool>,
    /// `None` at a `pbmask` of 0, where it is `support`.
    within: Option<Vec<bool>>,
}

impl BeamPixels {
    /// The pixels of `valid` where the published `.pb`, `pb` (zero at or
    /// below the cutoff), is positive and where it exceeds `pbmask`.
    fn new(
        pb: impl Iterator<Item = Result<f32, ProductsError>>,
        valid: &[bool],
        pbmask: f32,
    ) -> Result<Self, ProductsError> {
        let mut support = Vec::with_capacity(valid.len());
        let mut within = (pbmask > 0.0).then(|| Vec::with_capacity(valid.len()));
        for (pb, valid) in pb.zip(valid) {
            let pb = pb?;
            support.push(*valid && pb > 0.0);
            if let Some(within) = &mut within {
                within.push(*valid && pb > pbmask);
            }
        }
        Ok(Self { support, within })
    }

    /// A channel's analytic beam, independent of its polarizations' validity.
    pub(super) fn analytic(
        setup: &MinorCycleSetup<'_>,
        (roles, key, taylor): (&[ImageDomainRole], PlaneKey, bool),
    ) -> Result<Self, MinorCycleRunError> {
        #[cfg(test)]
        CHANNEL_BEAM_FORMS.with(|count| count.set(count.get() + 1));
        let beam = setup.primary_beam.as_ref().expect("an analytic beam");
        let channel = if taylor { 0 } else { key.channel };
        let pb = beam.pb(&roles[key.domain], channel)?;
        let level = pbmask(setup);
        let mut support = Vec::with_capacity(pb.len());
        let mut within = (level > 0.0).then(|| Vec::with_capacity(pb.len()));
        for value in pb {
            support.push(value > 0.0);
            if let Some(within) = &mut within {
                within.push(value > level);
            }
        }
        Ok(Self { support, within })
    }

    /// Intersect this channel's support with one polarization's valid pixels.
    pub(super) fn for_valid(&self, valid: &[bool]) -> Self {
        let intersect = |support: &[bool]| {
            support
                .iter()
                .zip(valid)
                .map(|(beam, valid)| *beam && *valid)
                .collect()
        };
        Self {
            support: intersect(&self.support),
            within: self.within.as_deref().map(intersect),
        }
    }

    /// The pixels above the cutoff: the residual's pixel mask.
    pub(super) fn support(self) -> Vec<bool> {
        self.support
    }

    /// The pixels above `pbmask`.
    pub(super) fn within(self) -> Vec<bool> {
        self.within.unwrap_or(self.support)
    }

    /// Both, as supports of `shape`: the pixels above the cutoff, and those
    /// above a `pbmask` above zero.
    pub(super) fn into_supports(
        self,
        shape: casa_imaging_deconvolution::PlaneShape,
    ) -> (Support, Option<Support>) {
        (
            Support::new(shape, self.support),
            self.within.map(|within| Support::new(shape, within)),
        )
    }
}

/// The valid pixels of plane `key` inside its `.pb` ([`BeamPixels`]): the
/// weight image's Float beam under `response`, else the run's analytic
/// beam at the plane's channel (channel zero for a Taylor family). `None`
/// for a run without a `.pb`.
pub(super) fn beam_pixels(
    setup: &MinorCycleSetup<'_>,
    (roles, key, taylor): (&[ImageDomainRole], PlaneKey, bool),
    valid: &[bool],
    response: Option<&ResponseBinding<'_>>,
) -> Result<Option<BeamPixels>, MinorCycleRunError> {
    if let Some(binding) = response {
        let beam = MosaicSensitivity::new(&binding.sensitivity)?
            .with_normal_sum_weight(binding.sum_weight)?;
        let policy = binding.response.policy();
        let pb = (0..valid.len()).map(|index| beam.weighted_primary_beam_at(index, policy));
        return Ok(Some(BeamPixels::new(pb, valid, pbmask(setup))?));
    }
    let Some(beam) = &setup.primary_beam else {
        return Ok(None);
    };
    let channel = if taylor { 0 } else { key.channel };
    let pb = beam.pb(&roles[key.domain], channel)?;
    Ok(Some(BeamPixels::new(
        pb.into_iter().map(Ok),
        valid,
        pbmask(setup),
    )?))
}

/// One plane's pixels as its preparation measures them: its valid pixels
/// and, when the run reads the `.pb`, their part inside it and their part
/// above a `pbmask` above zero.
pub(super) struct PlanePixels<'p> {
    pub(super) valid: &'p Support,
    pub(super) beam: Option<Support>,
    pub(super) within: Option<Support>,
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

    /// The pixels where the `.pb` exceeds `pbmask`, when the run reads it.
    fn within_beam(&self) -> Option<&Support> {
        self.within.as_ref().or(self.beam.as_ref())
    }

    /// The pixels `calcRobustRMS` measures: those where the `.pb` exceeds
    /// `pbmask`, even none, while the plane's image domain has such a pixel
    /// in some plane (`domain_beam`); every valid pixel otherwise, or when
    /// the run has no beam.
    pub(super) fn robust_rms(&self, domain_beam: bool) -> &Support {
        match self.within_beam() {
            Some(beam) if domain_beam => beam,
            _ => self.valid,
        }
    }

    /// The pixels the plane's `nsigma` noise reads under `setup`, at a
    /// boundary after `completed` major cycles, in an image domain whose
    /// beam keeps a pixel above `pbmask` in some plane when `domain_beam`:
    /// after the first boundary of an automatic mask, the mask's pixel
    /// mask, `pb > pbmask` at a `pbmask` above zero.
    pub(super) fn noise(
        &self,
        setup: &MinorCycleSetup<'_>,
        completed: usize,
        domain_beam: bool,
    ) -> &Support {
        if !(setup.automask && completed > 0) {
            return self.robust_rms(domain_beam);
        }
        match self.within_beam() {
            Some(within) if setup.pbmask > 0.0 => within,
            _ => self.residual_mask(setup.residual_pixel_mask),
        }
    }
}

/// The plane an automatic mask reads, of `residual` with its `valid`
/// pixels and their part inside the `.pb`, `beam`, under `setup`: its
/// pixel mask, and at the `first` boundary of a run with `nsigma`, the
/// pixels `calcRobustRMS` measured when they differ, those above `pbmask`
/// while its image domain keeps such a pixel in some plane
/// (`domain_beam`). At a `pbmask` above zero the pixel mask is
/// `pb > pbmask` (`SDMaskHandler::autoMaskWithinPB`), which also cuts the
/// mask the plane makes.
pub(super) fn automask_plane(
    setup: &MinorCycleSetup<'_>,
    (first, domain_beam): (bool, bool),
    residual: Vec<f64>,
    valid: Vec<bool>,
    beam: Option<BeamPixels>,
) -> AutoMaskPlane {
    let Some(beam) = beam else {
        return AutoMaskPlane {
            residual,
            valid,
            statistics: None,
        };
    };
    // `calcRobustRMS` measures the pixels above `pbmask`, or every valid
    // pixel when the image's beam keeps none.
    let reuses = first && setup.nsigma > 0.0;
    if setup.pbmask > 0.0 {
        return AutoMaskPlane {
            residual,
            statistics: (reuses && !domain_beam).then_some(valid),
            valid: beam.within(),
        };
    }
    let beam = beam.support();
    let (valid, statistics) = match (setup.residual_pixel_mask, reuses, domain_beam) {
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

/// Whether each image domain's `.pb` exceeds `pbmask` at a pixel in some
/// plane of `keys` that holds data: CAS-13318's fallback to every pixel is
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
        // Analytic beams do not depend on polarization. If an earlier
        // polarization had data, this channel's beam was already tested.
        if setup.response.is_none()
            && setup.primary_beam.is_some()
            && (0..key.polarization).any(|polarization| {
                super::holds_data(
                    normal,
                    taylor,
                    PlaneKey {
                        polarization,
                        ..*key
                    },
                )
            })
        {
            continue;
        }
        // A run without a beam never falls back.
        #[cfg(test)]
        DOMAIN_BEAM_PROBES.with(|count| count.set(count.get() + 1));
        beams[key.domain] = plane_beam(normal, setup, (roles, *key, taylor), pbmask(setup))?
            .is_none_or(|kept| kept.contains(&true));
    }
    Ok(beams.into_boxed_slice())
}

/// The pixels of plane `key`, x-major, whose `.pb` exceeds `level`, zero
/// or more (`pb > level`): the weight image's Float beam under a response
/// (its polarization's Taylor family's principal one), else the analytic beam at the
/// plane's channel (channel zero for a Taylor family). A plane no weighted
/// sample reached has a zero `.pb`; `None` for a run without a `.pb`.
pub(super) fn plane_beam(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup<'_>,
    (roles, key, taylor): (&[ImageDomainRole], PlaneKey, bool),
    level: f32,
) -> Result<Option<Vec<bool>>, MinorCycleRunError> {
    let Some(response) = setup.response else {
        let Some(beam) = &setup.primary_beam else {
            return Ok(None);
        };
        let channel = if taylor { 0 } else { key.channel };
        return Ok(Some(
            beam.pb(&roles[key.domain], channel)?
                .into_iter()
                .map(|pb| pb > level)
                .collect(),
        ));
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
    // A plane whose rows all miss the grid holds an AW weight image (they
    // weigh at the uv origin) and no sum: `SIImageStore::divideImageByWeightVal`
    // zeroes it, so it has no `.pb` to cut by.
    let Some(beam) = MosaicSensitivity::try_new(&sensitivity)?.filter(|_| sum_weight > 0.0) else {
        return Ok(Some(vec![false; sensitivity.len()]));
    };
    let beam = beam.with_normal_sum_weight(sum_weight)?;
    let policy = response.policy();
    let kept = (0..sensitivity.len())
        .map(|index| Ok(beam.weighted_primary_beam_at(index, policy)? > level))
        .collect::<Result<Vec<_>, ProductsError>>()?;
    Ok(Some(kept))
}
