// SPDX-License-Identifier: LGPL-3.0-or-later
//! Per-plane PSF summaries and saved beam selection.

use super::{
    Arc, FinalNormalState, MinorCycleRunError, MinorCycleSetup, ModelGeneration, PlaneKey,
    PlaneShape, PsfCache, PsfSummary, RestoringBeam, WorkerTeam, beams, holds_data, normalise,
    plane_shape, psf_peak_value, read_psf,
};

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
) -> Result<PsfSummary, MinorCycleRunError> {
    let increment = base
        .shape()
        .direction(key.domain)
        .expect("domain direction")
        .increment_rad();
    let cell = [1.0, (increment[1] / increment[0]).abs()];
    Ok(match saved_psf_beam(normal, setup, key) {
        Some(beam) => PsfSummary::with_saved_beam_and_cell(psf, shape, beam, cell)?,
        None => PsfSummary::with_cutoff_and_cell(psf, shape, setup.psf_cutoff, cell)?,
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
        )?);
        Ok::<_, MinorCycleRunError>(())
    })?;
    for (key, summary) in slots {
        if let Some(summary) = summary {
            cache.summaries.insert(key, summary);
        }
    }
    beams::settle_off_centre(normal, taylor, cache, setup.psf_beams.is_some())
}
