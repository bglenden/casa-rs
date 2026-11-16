// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel-scoped automatic-mask reads and measurements.

use super::*;

/// Inputs for an automatic mask's channel-scoped materialization.
pub(super) struct Automatic<'a> {
    pub(super) normal: &'a FinalNormalState,
    pub(super) base: &'a ModelGeneration,
    pub(super) setup: &'a MinorCycleSetup<'a>,
    pub(super) taylor: bool,
    pub(super) cache: &'a PsfCache,
    pub(super) plans: &'a ImageDomainReconstructionMaskPlans,
    pub(super) team: &'a WorkerTeam,
    pub(super) measurements: &'a std::sync::Mutex<Vec<Option<Measured>>>,
    pub(super) share_beam: bool,
}

impl Automatic<'_> {
    /// Build masks after all of their PSF summaries are available.
    pub(super) fn materialize(
        &self,
    ) -> Result<casa_imaging_deconvolution::ImageDomainMaskMaterialization, MinorCycleRunError>
    {
        let Self {
            normal,
            base,
            setup,
            taylor,
            cache,
            plans,
            team,
            measurements,
            share_beam,
        } = *self;
        let reading = Reading {
            normal,
            base,
            setup,
            taylor,
            summaries: &cache.summaries,
            first: plans.completed_major_cycles() == 0,
            beams: cache.beams.as_deref(),
        };
        let read_plane = |key: MaskPlaneKey| {
            read(&reading, absolute(normal, taylor, key), None)
                .map(|plane| plane.map(|(plane, _)| plane))
        };
        let psf = |key: MaskPlaneKey| {
            let key = absolute(normal, taylor, key);
            let summary = cache.summaries.get(&key).copied().or_else(|| {
                saved_psf_beam(normal, setup, key).map(PsfSummary::blank_with_saved_beam)
            });
            let beam = saved_beam(normal, &setup.mask_beams, key);
            match (summary, beam) {
                (Some(summary), Some(beam)) => Some(summary.with_mask_beam(beam)),
                (None, Some(beam)) => Some(PsfSummary::blank_with_saved_beam(beam)),
                (summary, None) => summary,
            }
        };
        let open = |_key: MaskPlaneKey| -> Result<
            Box<dyn casa_imaging_deconvolution::AutoMaskChannel<MinorCycleRunError> + '_>,
            MinorCycleRunError,
        > {
            Ok(Box::new(Shared {
                reading: &reading,
                plans,
                measured: measurements,
                beam: None,
                valid: None,
            }))
        };
        let source = AutoMaskSource {
            channel: share_beam.then_some(&open),
            read: &read_plane,
            psf: &psf,
            workers: team.workers(),
        };
        let roles = base.shape().domain_roles();
        let beam = |key: MaskPlaneKey, level: f32| {
            pixels::plane_beam(
                normal,
                setup,
                (roles, absolute(normal, taylor, key), taylor),
                level,
            )
        };
        let sources = MaskSources {
            auto: Some(&source),
            beam: Some(&beam),
        };
        team.install(|| plans.materialize(base, normal, &sources))
    }
}

/// What an automatic mask reads its planes from, with each plane's PSF
/// measurements, and whether it masks the run's first boundary.
pub(super) struct Reading<'a> {
    pub(super) normal: &'a FinalNormalState,
    pub(super) base: &'a ModelGeneration,
    pub(super) setup: &'a MinorCycleSetup<'a>,
    pub(super) taylor: bool,
    pub(super) summaries: &'a BTreeMap<PlaneKey, PsfSummary>,
    pub(super) first: bool,
    /// [`PsfCache`]'s beams, when the cycle reads them.
    pub(super) beams: Option<&'a [bool]>,
}

/// The plane `key` as its automatic mask reads it: the residual the minor
/// cycle cleans (term 0 of a Taylor family) and the pixels it reads
/// ([`pixels::automask_plane`]); `None` for a plane with no data.
type ReadPlane = Option<(AutoMaskPlane, Option<Vec<bool>>)>;

pub(super) fn read(
    reading: &Reading<'_>,
    key: PlaneKey,
    shared: Option<&pixels::BeamPixels>,
) -> Result<ReadPlane, MinorCycleRunError> {
    let Reading {
        normal,
        base,
        setup,
        taylor,
        summaries,
        first,
        beams,
    } = *reading;
    if !holds_data(normal, taylor, key) {
        return Ok(None);
    }
    let shape = plane_shape(normal, key);
    let valid = valid_support(normal, base, taylor, key, shape)?;
    let (residual, response) = if taylor {
        let peak = summaries
            .get(&key)
            .ok_or(casa_imaging_deconvolution::Error::PsfPeak)?
            .peak();
        taylor_principal_residual(normal, setup, key.polarization, peak)?
    } else {
        let plane = normal.read_plane(key.domain, key.channel, key.polarization)?;
        let raw = plane.read_residual()?;
        single_residual(
            normal,
            setup,
            key,
            raw.iter().map(|value| value.re),
            plane.sum_weight(),
        )?
    };
    let original_valid = shared.map(|_| valid.clone());
    let beam = if let Some(shared) = shared {
        Some(shared.for_valid(&valid))
    } else if pixels::reads_beam(setup) {
        pixels::beam_pixels(
            setup,
            (base.shape().domain_roles(), key, taylor),
            &valid,
            response.as_ref(),
        )?
    } else {
        None
    };
    let domain_beam = beams.is_none_or(|beams| beams[key.domain]);
    Ok(Some((
        pixels::automask_plane(setup, (first, domain_beam), residual, valid, beam),
        original_valid,
    )))
}

/// A channel's analytic beam stays live only while its polarizations mask
/// and measure their residuals. The output contains scalar measurements.
pub(super) struct Shared<'a, 'b> {
    pub(super) reading: &'a Reading<'b>,
    pub(super) plans: &'a ImageDomainReconstructionMaskPlans,
    pub(super) measured: &'a std::sync::Mutex<Vec<Option<Measured>>>,
    pub(super) beam: Option<pixels::BeamPixels>,
    pub(super) valid: Option<Vec<bool>>,
}

impl casa_imaging_deconvolution::AutoMaskChannel<MinorCycleRunError> for Shared<'_, '_> {
    fn read(&mut self, key: MaskPlaneKey) -> Result<Option<AutoMaskPlane>, MinorCycleRunError> {
        let reading = self.reading;
        let key = absolute(reading.normal, reading.taylor, key);
        if !holds_data(reading.normal, reading.taylor, key) {
            self.valid = None;
            return Ok(None);
        }
        if self.beam.is_none() {
            self.beam = Some(pixels::BeamPixels::analytic(
                reading.setup,
                (reading.base.shape().domain_roles(), key, reading.taylor),
            )?);
        }
        let (plane, valid) =
            read(reading, key, self.beam.as_ref())?.expect("a plane with data is read");
        self.valid = valid;
        Ok(Some(plane))
    }

    fn masked(
        &mut self,
        key: MaskPlaneKey,
        plane: Option<&AutoMaskPlane>,
        mask: &[bool],
    ) -> Result<(), MinorCycleRunError> {
        let Some(plane) = plane else {
            return Ok(());
        };
        let reading = self.reading;
        let absolute = absolute(reading.normal, reading.taylor, key);
        let shape = plane_shape(reading.normal, absolute);
        let valid = self.valid.take().expect("a read plane has valid pixels");
        let support = Support::new(
            shape,
            mask.iter()
                .zip(&valid)
                .map(|(mask, valid)| *mask && *valid)
                .collect(),
        );
        let (beam, within) = self
            .beam
            .as_ref()
            .expect("a read plane has a beam")
            .for_valid(&valid)
            .into_supports(shape);
        let valid = Support::new(shape, valid);
        let pixels = pixels::PlanePixels {
            valid: &valid,
            beam: Some(beam),
            within,
        };
        let setup = reading.setup;
        let mut statistics = PlaneStatistics::measure(
            &plane.residual,
            &support,
            pixels.residual_mask(setup.residual_pixel_mask),
            0.0,
            setup.automask,
        );
        if setup.nsigma > 0.0 {
            let domain_beam = reading.beams.is_none_or(|beams| beams[key.domain]);
            let measured = pixels.noise(setup, self.plans.completed_major_cycles(), domain_beam);
            statistics.noise = match setup.noise {
                NoiseEstimate::Fast => robust_noise(&plane.residual, measured),
                NoiseEstimate::Robust => {
                    let exclusion = self
                        .plans
                        .automatic_noise_exclusion(key.domain, (key.channel, key.polarization))
                        .expect("an automatic channel has an automatic plan");
                    pixels::robust_plane_noise(&plane.residual, measured, exclusion)
                }
            };
        }
        let summary = *reading
            .summaries
            .get(&absolute)
            .expect("automatic PSFs are measured");
        let index = if reading.taylor {
            key.polarization
        } else {
            (key.polarization * reading.normal.domain_count() + key.domain)
                * reading.normal.channel_count()
                + key.channel
        };
        self.measured.lock().expect("measurements lock")[index] = Some(Measured {
            statistics,
            summary,
        });
        Ok(())
    }
}
