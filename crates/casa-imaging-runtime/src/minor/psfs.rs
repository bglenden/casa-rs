// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-plane PSF summaries and saved beam selection.

use super::{
    Arc, FinalNormalState, MinorCycleRunError, MinorCycleSetup, ModelGeneration, PlaneData,
    PlaneKey, PlaneShape, PsfCache, PsfSummary, RestoringBeam, WorkerTeam, beams, holds_data,
    normalise, plane_shape, psf_peak_value, read_psf,
};

impl PsfCache {
    /// Measure a reused single-term PSF in the saved image's units.
    #[must_use]
    pub fn for_reused_single_psf() -> Self {
        Self {
            reuses_single_psf: true,
            ..Self::default()
        }
    }

    pub(super) fn summarize_loaded(
        &self,
        normal: &FinalNormalState,
        base: &ModelGeneration,
        setup: &MinorCycleSetup<'_>,
        key: PlaneKey,
        plane: &PlaneData<'_>,
    ) -> Result<PsfSummary, MinorCycleRunError> {
        match self.summaries.get(&key) {
            Some(summary) => Ok(*summary),
            None => summarize_psf(
                normal,
                base,
                setup,
                key,
                &plane.psf[0],
                plane.shape,
                self.reuses_single_psf.then_some(plane.psf_peak),
            ),
        }
    }
}

/// A saved grid is for the main domain, ordered by absolute channel and polarization.
pub(super) fn saved_psf_beam(
    normal: &FinalNormalState,
    setup: &MinorCycleSetup<'_>,
    key: PlaneKey,
) -> Option<RestoringBeam> {
    saved_beam(normal, &setup.psf_beams, key)
}

pub(super) fn saved_beam(
    normal: &FinalNormalState,
    beams: &Option<Arc<[RestoringBeam]>>,
    key: PlaneKey,
) -> Option<RestoringBeam> {
    (key.domain == 0)
        .then(|| {
            beams
                .as_ref()?
                .get(key.channel * normal.polarization_count() + key.polarization)
                .copied()
        })
        .flatten()
}

pub(super) fn summarize_psf(
    normal: &FinalNormalState,
    base: &ModelGeneration,
    setup: &MinorCycleSetup<'_>,
    key: PlaneKey,
    psf: &[f64],
    shape: PlaneShape,
    image_peak: Option<f64>,
) -> Result<PsfSummary, MinorCycleRunError> {
    let increment = base
        .shape()
        .direction(key.domain)
        .expect("domain direction")
        .increment_rad();
    let cell = [1.0, (increment[1] / increment[0]).abs()];
    let saved_beam = saved_psf_beam(normal, setup, key);
    Ok(match (image_peak, saved_beam) {
        (Some(peak), _) => {
            PsfSummary::with_image_peak(psf, shape, setup.psf_cutoff, saved_beam, cell, peak)?
        }
        (None, Some(beam)) => PsfSummary::with_saved_beam_and_cell(psf, shape, beam, cell)?,
        (None, None) => PsfSummary::with_cutoff_and_cell(psf, shape, setup.psf_cutoff, cell)?,
    })
}

/// Measure into the cache the PSF of each plane of `keys` that holds data
/// and is not yet measured, side by side on the team.
pub(super) fn measure_psfs(
    normal: &FinalNormalState,
    base: &ModelGeneration,
    taylor: bool,
    cache: &mut PsfCache,
    keys: &[PlaneKey],
    setup: &MinorCycleSetup<'_>,
    team: &WorkerTeam,
) -> Result<(), MinorCycleRunError> {
    let mut slots = keys
        .iter()
        .filter(|key| !cache.summaries.contains_key(key) && holds_data(normal, taylor, **key))
        .map(|key| (*key, None))
        .collect::<Vec<_>>();
    let reused = cache.reuses_single_psf;
    team.for_each_mut(&mut slots, |_, (key, summary)| {
        let shape = plane_shape(normal, *key);
        let psf = read_psf(normal, taylor, *key)?;
        let peak = psf_peak_value(&psf, shape)?;
        *summary = Some(summarize_psf(
            normal,
            base,
            setup,
            *key,
            &normalise(psf, peak),
            shape,
            reused.then_some(peak),
        )?);
        Ok::<_, MinorCycleRunError>(())
    })?;
    for (key, summary) in slots {
        if let Some(summary) = summary {
            cache.summaries.insert(key, summary);
        }
    }
    beams::settle_refused_fits(normal, base, taylor, cache, setup.psf_beams.is_some(), None)
}
