// SPDX-License-Identifier: LGPL-3.0-or-later
//! Mask and residual statistics before the controller chooses a minor cycle.

use super::*;

/// Form the masks of `completion` and measure every plane: its peak
/// residual, noise and PSF sidelobe (CASA's `initminorcycle`).
///
/// # Errors
///
/// When a mask, a plane or a PSF cannot be read or measured.
pub fn prepare_minor_cycle(
    completion: &MajorCycleCompletion,
    mask_plans: &ImageDomainReconstructionMaskPlans,
    setup: &MinorCycleSetup<'_>,
    cache: &mut PsfCache,
    team: &WorkerTeam,
) -> Result<PreparedMinorCycle, MinorCycleRunError> {
    prepare_minor_cycle_with_residual_mask(completion, mask_plans, setup, cache, team, None)
}

/// Prepare a minor cycle with an existing Taylor residual's pixel mask.
/// CASA uses it for the controller's peak, while the CLEAN mask continues
/// to define the support of the component solver.
///
/// # Errors
///
/// A plane, PSF, or mask cannot be read or measured.
///
/// # Panics
///
/// The supplied mask must match a single-polarization, single-domain
/// Taylor completion.
pub fn prepare_minor_cycle_with_residual_mask(
    completion: &MajorCycleCompletion,
    mask_plans: &ImageDomainReconstructionMaskPlans,
    setup: &MinorCycleSetup<'_>,
    cache: &mut PsfCache,
    team: &WorkerTeam,
    residual_mask: Option<&Support>,
) -> Result<PreparedMinorCycle, MinorCycleRunError> {
    let normal = completion.normal_state();
    if let Some(mask) = residual_mask {
        let [nx, ny] = normal.shape();
        assert!(
            is_taylor(normal)
                && normal.domain_count() == 1
                && normal.polarization_count() == 1
                && mask.shape() == PlaneShape::new(nx, ny),
            "the saved pixel mask must describe the single Taylor plane"
        );
    }
    let base = completion.final_model();
    let taylor = is_taylor(normal);
    for beams in [&setup.psf_beams, &setup.mask_beams].into_iter().flatten() {
        let channels = if taylor {
            1
        } else {
            normal.slab().total_channels()
        };
        let expected = channels * normal.polarization_count();
        if beams.len() != expected {
            return Err(MinorCycleRunError::SavedBeamGridLength {
                expected,
                actual: beams.len(),
            });
        }
    }
    let keys = plane_keys(normal, taylor);
    let share_beam = pixels::reads_beam(setup)
        && setup.primary_beam.is_some()
        && setup.response.is_none()
        && mask_plans.automatic();
    let measurements = std::sync::Mutex::new(
        (0..if share_beam { keys.len() } else { 0 })
            .map(|_| None::<Measured>)
            .collect::<Vec<_>>(),
    );
    if pixels::reads_beam(setup) && setup.nsigma > 0.0 && cache.beams.is_none() {
        cache.beams = Some(pixels::domain_beams(normal, base, setup, taylor, &keys)?);
    }
    // A mask cut to the primary beam reads each plane's `.pb`.
    let roles = base.shape().domain_roles();
    let beam = |key: MaskPlaneKey, level: f32| {
        pixels::plane_beam(
            normal,
            setup,
            (roles, absolute(normal, taylor, key), taylor),
            level,
        )
    };
    let (masks, auto_masks) = if mask_plans.automatic() {
        // An automatic mask smooths by its channel block's common beam and
        // thresholds by the image's sidelobe level, so every plane's PSF is
        // measured first.
        measure_psfs(normal, base, taylor, cache, &keys, setup, team)?;
        channel::Automatic {
            normal,
            base,
            setup,
            taylor,
            cache,
            plans: mask_plans,
            team,
            measurements: &measurements,
            share_beam,
        }
        .materialize()?
    } else {
        let sources = MaskSources {
            auto: None,
            beam: Some(&beam),
        };
        mask_plans.materialize(base, normal, &sources)?
    }
    .into_parts();
    let inputs = Inputs {
        normal,
        base,
        masks: &masks,
        setup,
        taylor,
    };
    let mut measured = measurements.into_inner().expect("measurements lock");
    let mut slots = keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            (
                *key,
                measured.get_mut(index).and_then(Option::take),
                None::<RetainedPlane>,
            )
        })
        .collect::<Vec<_>>();
    let missing = keys
        .iter()
        .filter(|key| !cache.summaries.contains_key(key))
        .count();
    // A cycle's only plane is solved straight after it is measured, so its
    // load is kept for the solve rather than read twice.
    let retain = keys.len() == 1 && setup.response.is_none();
    let shared = &*cache;
    team.for_each_mut(&mut slots, |_, (key, out, kept)| {
        if out.is_some() && !retain {
            return Ok(());
        }
        let Some(plane) = load(&inputs, *key)? else {
            return Ok(());
        };
        if out.is_some() {
            *kept = Some(RetainedPlane {
                key: *key,
                residual: plane.residual,
                psf: plane.psf,
                psf_peak: plane.psf_peak,
                support: plane.support,
                valid: plane.valid,
            });
            return Ok(());
        }
        let summary = shared.summarize_loaded(normal, base, setup, *key, &plane)?;
        let pixels = plane_pixels(&inputs, *key, &plane)?;
        let mut statistics = PlaneStatistics::measure(
            &plane.residual[0],
            &plane.support,
            residual_mask.unwrap_or_else(|| pixels.residual_mask(setup.residual_pixel_mask)),
            0.0,
            setup.automask,
        );
        if setup.nsigma > 0.0 {
            let domain_beam = shared
                .beams
                .as_deref()
                .is_none_or(|beams| beams[key.domain]);
            let measured = pixels.noise(setup, mask_plans.completed_major_cycles(), domain_beam);
            statistics.noise = match setup.noise {
                NoiseEstimate::Fast => robust_noise(&plane.residual[0], measured),
                NoiseEstimate::Robust => {
                    let local = (
                        key.channel - normal.slab().core_range().start,
                        key.polarization,
                    );
                    let exclusion = mask_plans.noise_exclusion(key.domain, local, &masks);
                    pixels::robust_plane_noise(&plane.residual[0], measured, exclusion)
                }
            };
        }
        drop(pixels);
        *out = Some(Measured {
            statistics,
            summary,
        });
        if retain && plane.response.is_none() {
            *kept = Some(RetainedPlane {
                key: *key,
                residual: plane.residual,
                psf: plane.psf,
                psf_peak: plane.psf_peak,
                support: plane.support,
                valid: plane.valid,
            });
        }
        Ok::<_, MinorCycleRunError>(())
    })?;
    let mut planes = Vec::with_capacity(keys.len());
    let mut blank = Vec::new();
    let mut retained = None;
    for (key, measured, kept) in slots {
        let Some(Measured {
            statistics,
            summary,
        }) = measured
        else {
            blank.push((
                key,
                rows::blank_statistics(normal, &masks, key, setup.automask),
            ));
            continue;
        };
        cache.summaries.entry(key).or_insert(summary);
        planes.push((key, statistics));
        retained = retained.or(kept);
    }
    // Both cycle controls and Clark's patch use the settled PSF beam.
    let saved = setup.psf_beams.is_some();
    beams::settle_refused_fits(normal, base, taylor, cache, saved, retained.as_ref())?;
    // Each image domain is one CASA image store.
    let mut statistics = ResidualStatistics::empty();
    let domains = planes
        .iter()
        .map(|(key, _)| key.domain)
        .collect::<BTreeSet<_>>();
    for domain in domains {
        statistics.include_store(
            planes
                .iter()
                .filter(|(key, _)| key.domain == domain)
                .map(|(key, plane)| (plane, cache.summaries[key].sidelobe())),
            setup.nsigma,
        );
    }
    tracing::debug!(
        planes = planes.len(),
        psf_fits = missing,
        "minor cycle prepared"
    );
    Ok(PreparedMinorCycle {
        masks,
        auto_masks,
        planes,
        blank,
        retained,
        statistics,
    })
}
