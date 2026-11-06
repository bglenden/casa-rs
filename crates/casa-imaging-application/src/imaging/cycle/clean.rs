// SPDX-License-Identifier: LGPL-3.0-or-later
//! The shared minor/major cleaning loop.

use super::*;

mod mvc;
pub(super) use mvc::MvcState;

pub(super) struct CleanInputs<'a> {
    pub(super) run: Run<'a>,
    pub(super) outcome: ImagingOutcome,
    pub(super) mvc: Option<MvcState<'a>>,
    pub(super) controls: casa_imaging_model::ReconstructionControls,
    pub(super) setup: MinorCycleSetup<'a>,
    pub(super) mask_plans: ImageDomainReconstructionMaskPlans,
    pub(super) cancel: Cancel,
    pub(super) diagnostics: Diagnostics,
}

pub(super) fn run(
    inputs: CleanInputs<'_>,
    summary: &mut RunSummary,
) -> Result<ImagingOutcome, ImagingError> {
    let CleanInputs {
        mut run,
        mut outcome,
        mut mvc,
        controls,
        setup,
        mut mask_plans,
        cancel,
        diagnostics,
    } = inputs;
    let mut controller = Controller::new(&controls);
    let mut cache = PsfCache::default();
    // What outlives each minor cycle: the masks it forms (the latest, and the
    // one an automatic mask evolves from, every holder sharing a
    // generation's support), the PSF summaries it measures and a Clark
    // refresh it caches.
    let mut held = Reservation::none();
    // tclean skips the major cycle after a minor cycle that cleans nothing,
    // updates the mask and still asks `hasConverged` for the stop it reports.
    let mut cleaned_nothing = false;
    loop {
        let ordinal = outcome.minor_cycles.len() + 1;
        let name = format!("minor-cycle preparation {ordinal}");
        // A plane preparation keeps for the solve stays charged until the
        // solve, which the minor cycle's charge then leaves out; the solve
        // drops the plane, and the charge goes with the minor cycle.
        let (prepared, kept) = run_phase(name, &cancel, diagnostics, summary, || {
            if ordinal == 1 {
                held = run.admit(
                    "reconstruction masks",
                    2 * mask_bytes(&outcome.scientific, &mask_plans)
                        + psf_cache_bytes(&outcome.scientific)
                        + std::mem::size_of_val(outcome.plane_rows.start_model_flux.as_slice())
                            as u64,
                )?;
            }
            let mut preparing = run.admit(
                "minor-cycle preparation",
                prepare_bytes(
                    &outcome.scientific,
                    &mask_plans,
                    &setup,
                    &cache,
                    run.teams.compute().workers(),
                ),
            )?;
            let prepared = prepare_minor_cycle(
                &outcome.scientific,
                &mask_plans,
                &setup,
                &mut cache,
                run.teams.compute(),
            )?;
            let kept = preparing.split(prepared.retained_bytes());
            Ok::<_, ImagingError>((prepared, kept))
        })?;
        let statistics = prepared.statistics;
        // CASA's summary uses the final initminorcycle statistics, including
        // a convergence check that stops before another minor cycle executes.
        outcome.final_residual_statistics = Some(statistics);
        if diagnostics.science_trace {
            trace::residual(outcome.major_cycle_count, &statistics);
        }
        let stop = controller.clean_complete(&statistics);
        if let Some(stop) = stop {
            tracing::info!("imaging stopped: {stop:?} (stopcode {})", stop.code());
        }
        if cleaned_nothing {
            outcome.stop = stop;
            outcome.masks = Some(ReconstructionMaskSet::Domains(prepared.masks().clone()));
            break;
        }
        if stop.is_some() {
            outcome.stop = stop;
            outcome
                .masks
                .get_or_insert_with(|| ReconstructionMaskSet::Domains(prepared.masks().clone()));
            break;
        }
        let cycle_controls = controller.cycle_controls(&statistics);
        if diagnostics.science_trace {
            trace::cycle_controls(ordinal, &cycle_controls);
        }
        let started = Instant::now();
        let name = format!("minor cycle {ordinal}");
        let (minor, next_plans) = run_phase(name, &cancel, diagnostics, summary, || {
            let cached = cache.refresh_bytes(&outcome.scientific);
            let mut solving = run.admit(
                "minor cycle",
                run_bytes(
                    &outcome.scientific,
                    &setup,
                    &cycle_controls,
                    &cache,
                    run.teams.compute().workers(),
                )
                .saturating_sub(prepared.retained_bytes()),
            )?;
            let minor = run_minor_cycle(
                prepared,
                &outcome.scientific,
                &setup,
                &cycle_controls,
                &mut cache,
                run.teams.compute(),
                (&run.host, &run.policy),
            )?;
            let refresh = cache.refresh_bytes(&outcome.scientific);
            if refresh > cached {
                held.join(solving.split(refresh - cached));
            }
            let plans = next_masks(
                &mask_plans,
                &minor,
                ordinal,
                cycle_controls.threshold_reached,
            )?;
            Ok::<_, ImagingError>((minor, plans))
        })?;
        drop(kept);
        mask_plans = next_plans;
        let global_threshold = controls
            .threshold_jy_per_beam()
            .max(statistics.nsigma_threshold);
        // The outcome keeps every cycle's record, with a copy of its mask's
        // support and its traced components (#706).
        held.join(run.admit("minor-cycle record", record_bytes(&minor))?);
        let cycle = account_minor_cycle(
            &mut outcome,
            &mut controller,
            &minor,
            &cycle_controls,
            global_threshold,
            started,
        );
        summary.minor_cycles = cycle;
        summary.minor_iterations = outcome.total_minor_iterations;
        if diagnostics.science_trace {
            trace::minor_cycle(&outcome.minor_cycles[cycle - 1]);
        }
        let last = controller.budget_spent() || controller.last_cycle(&statistics);
        if minor.summary.iterations == 0 {
            cleaned_nothing = true;
            continue;
        }
        let name = format!("major cycle {}", outcome.major_cycle_count + 1);
        let scientific = outcome.scientific;
        let major = run_phase(name, &cancel, diagnostics, summary, || {
            if let Some(state) = mvc.as_mut() {
                Ok(Major {
                    completion: state.refresh(
                        &mut run,
                        scientific,
                        (minor.terms, minor.results),
                        last,
                    )?,
                    visibility: None,
                })
            } else {
                run.refresh(scientific, (minor.terms, minor.results), last)
            }
        })?;
        controller.end_major_cycle();
        outcome.scientific = major.completion;
        outcome.visibility_products = major.visibility;
        outcome.major_cycle_count += 1;
    }
    summary.stop = outcome.stop;
    if let Some(state) = mvc {
        let (cube, taylor) = state.finish(outcome.scientific);
        outcome.scientific = cube;
        outcome.mvc_taylor = Some(taylor);
    }
    if run.visibility_write.is_some() && outcome.visibility_products.is_none() {
        outcome = run_phase("model prediction", &cancel, diagnostics, summary, || {
            predict_final_model(&mut run, outcome)
        })?;
    }
    outcome.planes_per_wave = run.planes_per_wave;
    outcome.retained = std::mem::replace(&mut run.retained, Reservation::none());
    outcome.retained.join(held);
    Ok(outcome)
}
