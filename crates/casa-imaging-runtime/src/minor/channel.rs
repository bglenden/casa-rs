// SPDX-License-Identifier: LGPL-3.0-or-later
//! Channel-scoped automatic-mask reads and measurements.

use super::*;

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
pub(super) fn read(
    reading: &Reading<'_>,
    key: PlaneKey,
    shared: Option<&pixels::BeamPixels>,
) -> Result<Option<(AutoMaskPlane, Option<Vec<bool>>)>, MinorCycleRunError> {
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
