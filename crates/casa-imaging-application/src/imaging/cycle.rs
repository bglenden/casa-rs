// SPDX-License-Identifier: LGPL-3.0-or-later
//! The loop of major and minor cycles on the major-cycle pass.
//!
//! The initial pass grids the data and the PSF; each later pass grids the
//! residual `V − A·m` of the model the preceding minor cycle produced.
//! Imaging weights are generated once, before the initial pass, and reused
//! by every pass. The final pass also writes the model column when the run
//! asks for it.

use std::cell::OnceCell;
use std::path::Path;
use std::time::Instant;

use casa_imaging_deconvolution::{CleanStop, Controller, CycleControls, NormalStoragePlan};
use casa_imaging_deconvolution::{
    ImageDomainReconstructionMaskPlans, MajorCycle, MajorCycleCompletion, ModelGeneration,
    ModelLifecycle, ModelStoragePlan, PassAppender, ReconstructionMaskPlan, ReconstructionMaskSet,
};
use casa_imaging_model::{CompiledProblem, ModelDeltaTerm, SpectralWcs};
use casa_imaging_operator::{Basis, GridPrecision, ModeSet, PlaneRange, WeightingGeneration};
use casa_imaging_products::{
    FacetSumWeights, MinorCycleImageResponse, VisibilityProductCompletion,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BoundedSource, CHUNK_PLACEMENTS, Cancel, MajorCyclePass, MeasuredRowWork,
    ModelPreparation, Partition, PassDomain, PassError, PassSummary, Residency, VisibilitySink,
    WaveDemand, WeightCache, WorkerTeams, needs_row_work, row_work_pass_bytes, run_major_cycle,
    run_row_work_pass,
};
use casa_imaging_runtime::{
    Admitted, CubeState, Demand, Diagnostics, HostResources, MinorCycleOutcome, MinorCycleSetup,
    MinorLayout, PsfCache, Reservation, ResourcePolicy, RunSummary, admit, free_memory, mask_bytes,
    prepare_bytes, prepare_bytes_for, prepare_minor_cycle, run_bytes, run_bytes_for,
    run_minor_cycle, run_phase,
};
use casa_ms::ResolvedSelectedObservationAccess;

use super::charts::{Chart, ChartStitch, charts};
use super::images::{pass_images, prepare_model};
use super::measurement::{
    DomainOperator, KernelAdmission, domain_operator, native_spacing_hz, selected_correlations,
};
use super::setup::{self, RefreshDemand, SetupDemand, WorkerChoice};
use super::source::{MeasurementSetSource, PlaneBounds};
use super::visibility_write::{VisibilityWriteTarget, VisibilityWriter};
use super::{ImagingError, trace};
use crate::{AwCatalogDeployment, NativeMinorCycleOutcome, NativeMinorCycleStopReason};

/// What one native imaging run needs besides the compiled problem.
pub(crate) struct ImagingInputs<'a> {
    pub(crate) problem: &'a CompiledProblem,
    pub(crate) access: ResolvedSelectedObservationAccess,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    pub(crate) image_response: Option<MinorCycleImageResponse>,
    pub(crate) visibility_write: Option<VisibilityWriteTarget>,
    pub(crate) host: HostResources,
    pub(crate) policy: ResourcePolicy,
    /// Where the run's worker teams go once its setup forms them
    /// ([`setup::fit_workers`]): the passes stream on one, the minor cycles
    /// and the products compute on the other.
    pub(crate) teams: &'a OnceCell<WorkerTeams>,
    pub(crate) cancel: Cancel,
    /// Where the paged cube state of a channel-local run lives.
    pub(crate) spill_directory: &'a Path,
    /// The AW catalog of an A-projection run.
    pub(crate) aw_catalog: Option<AwCatalogDeployment>,
    pub(crate) backend: BackendChoice,
    /// The requested grid precision; `None` is plan decision D2's rule.
    pub(crate) grid_precision: Option<GridPrecision>,
    pub(crate) diagnostics: Diagnostics,
}

/// The final reconciliation and the record of the cycles that led to it.
pub(crate) struct ImagingOutcome {
    pub(crate) scientific: MajorCycleCompletion,
    pub(crate) masks: Option<ReconstructionMaskSet>,
    pub(crate) minor_cycles: Vec<NativeMinorCycleOutcome>,
    pub(crate) stop: Option<CleanStop>,
    pub(crate) major_cycle_count: usize,
    pub(crate) total_minor_iterations: usize,
    pub(crate) total_actual_minor_iterations: usize,
    pub(crate) visibility_products: Option<VisibilityProductCompletion>,
    pub(crate) planes_per_wave: Option<u32>,
    /// Each faceted domain's facets' own sums of weights, which its
    /// `.sumwt` publishes.
    pub(crate) facet_sum_weights: FacetSumWeights,
    /// The charge of the memory `scientific` holds resident, released with
    /// it.
    pub(crate) retained: Reservation,
}

/// Fixed parts of one run shared by every pass.
struct Run<'a> {
    problem: &'a CompiledProblem,
    /// One operator per chart, the pass domains.
    domains: Vec<DomainOperator>,
    /// Each pass domain's image domain and facet.
    charts: Vec<Chart>,
    /// The image domains' images from their charts.
    stitch: ChartStitch,
    source: MeasurementSetSource<'a>,
    weighting: WeightingGeneration,
    /// The charge of the weighting's density grid, held with it.
    _weights: Reservation,
    teams: &'a WorkerTeams,
    cancel: Cancel,
    host: HostResources,
    policy: ResourcePolicy,
    cube: Option<CubeState>,
    /// The run's model lifecycle: its generations are paged with the cube
    /// state, or resident.
    lifecycle: ModelLifecycle,
    native_spacing_hz: f64,
    backend: BackendChoice,
    /// Whether each Metal backend logs its timing.
    metal_stage_timing: bool,
    visibility_write: Option<VisibilityWriteTarget>,
    /// What the visibility write holds, admitted with the pass that writes;
    /// zero when nothing is written.
    visibility_write_bytes: u64,
    /// Planes per wave of the most finely waved pass so far.
    planes_per_wave: Option<u32>,
    /// Whether the initial pass also grids the sensitivity image
    /// (`Mode::Weight`): a kernel set with weight taps on any domain.
    weight_image: bool,
    /// The charge of the memory the latest completion holds resident (its
    /// normal state and model), held while it lives.
    retained: Reservation,
    /// The imaging weights the run keeps for its samples across passes,
    /// with their charge, once a pass has admitted them ([`Run::keep_weights`]).
    kept_weights: Option<(WeightCache, Reservation)>,
    /// The first pass's gridding work by anchor row of each region-split
    /// domain, and its work in each mode when a row-work pass measured it,
    /// with their charge: passes cut their strips to equal shares of it
    /// (#736), a pass that grids the weight image with its work on the
    /// centre row ([`casa_imaging_runtime::pass::ModeWork::weighed`]).
    row_work: Option<(MeasuredRowWork, Reservation)>,
}

/// What a pass's admission planned ([`Run::admit_pass`]): the waves it
/// holds, and the placements a row chunk takes (#736).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PassPlan {
    residency: Residency,
    chunk_placements: usize,
}

/// One reconciled major cycle.
struct Major {
    completion: MajorCycleCompletion,
    visibility: Option<VisibilityProductCompletion>,
}

impl ImagingOutcome {
    /// The outcome of a run that has done its initial major cycle `major`.
    fn initial(major: Major, run: &Run<'_>) -> Self {
        Self {
            scientific: major.completion,
            masks: None,
            minor_cycles: Vec::new(),
            stop: None,
            major_cycle_count: 1,
            total_minor_iterations: 0,
            total_actual_minor_iterations: 0,
            visibility_products: major.visibility,
            planes_per_wave: run.planes_per_wave,
            facet_sum_weights: run.stitch.facet_sum_weights(),
            retained: Reservation::none(),
        }
    }
}

/// Run every major and minor cycle of `inputs.problem`.
///
/// The loop is tclean's: after each major cycle the controller decides from
/// the fresh residual whether cleaning is complete (`cleanComplete`);
/// otherwise a minor cycle runs under its controls and a major cycle
/// refreshes the residual. A minor cycle that cleans nothing ends the run
/// without another major cycle. Each cycle, and each minor cycle's
/// preparation (its masks and residual statistics), is a phase of
/// `summary`, so every step that can fail is inside one; `summary` also
/// takes the resolved precision, the minor-cycle totals as each cycle
/// completes and the stop reason. A cancelled run stops at the next block
/// boundary of a pass or before the next phase.
pub(crate) fn run(
    inputs: ImagingInputs<'_>,
    summary: &mut RunSummary,
) -> Result<ImagingOutcome, ImagingError> {
    let problem = inputs.problem;
    let controls = problem.reconstruction().controls();
    let cleaning = controls.max_minor_iterations() > 0;
    let setup = minor_setup(problem, inputs.image_response, &inputs.masks);
    let mut mask_plans = inputs.masks.clone();
    let cancel = inputs.cancel.clone();
    let diagnostics = inputs.diagnostics;
    // What a minor cycle holds on a team of so many workers, from the
    // layout the first pass forms, before any image exists (#737): the masks
    // it keeps and the larger of its two halves.
    let layout = MinorLayout::planned(problem);
    let iterations = Controller::new(&controls).cycle_iterations();
    let minor = |workers: usize| {
        if cleaning {
            2 * layout.mask_bytes()
                + prepare_bytes_for(&layout, &mask_plans, &setup, true, workers)
                    .max(run_bytes_for(&layout, &setup, iterations, 0, workers))
        } else {
            0
        }
    };
    // What a refresh pass holds beside its wave (#763 review): every cycle
    // before it keeps its record, up to the last refresh `nmajor` allows,
    // and the minor cycle's terms, which its solve holds too (#778 review).
    let refresh = cleaning.then(|| RefreshDemand {
        masks: 2 * layout.mask_bytes(),
        cached: layout.refresh_bytes(&setup),
        results: layout.result_bytes(&setup, iterations),
        records: layout.record_bytes() * controls.maximum_major_cycles().unwrap_or(1).max(1) as u64,
    });
    // Operators, the workers, the source, the imaging weights and the paged
    // cube state. The choice of workers is recorded as it is made, so a
    // setup that then fails still reports it.
    let mut chosen = None;
    let opened = run_phase("setup", &cancel, diagnostics, summary, || {
        Run::open(inputs, (&minor, refresh), &mut chosen)
    });
    if let Some(workers) = chosen {
        summary.workers = workers.cores.workers;
        summary.workers_policy = workers.policy.workers;
        summary.workers_reason = workers.reason;
    }
    let mut run = opened?;
    summary.precision = run
        .domains
        .first()
        .map(|domain| domain.operator.precision());
    let major = run_phase("major cycle 1", &cancel, diagnostics, summary, || {
        run.initial(!cleaning)
    })?;
    let mut outcome = ImagingOutcome::initial(major, &run);
    if !cleaning {
        outcome.retained = std::mem::replace(&mut run.retained, Reservation::none());
        return Ok(outcome);
    }
    let mut controller = Controller::new(&controls);
    let mut cache = PsfCache::default();
    // What outlives each minor cycle: the masks it forms (the latest, and the
    // one an automatic mask evolves from) and a Clark refresh it caches.
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
                held = run.admit("reconstruction masks", 2 * mask_bytes(&outcome.scientific))?;
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
            run.refresh(scientific, (minor.terms, minor.results), last)
        })?;
        controller.end_major_cycle();
        outcome.scientific = major.completion;
        outcome.visibility_products = major.visibility;
        outcome.major_cycle_count += 1;
    }
    summary.stop = outcome.stop;
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

/// Fold one minor cycle into the run's totals and its controller
/// (`mergeMinorCycleSummary`), record and log it; returns its ordinal.
fn account_minor_cycle(
    outcome: &mut ImagingOutcome,
    controller: &mut Controller,
    minor: &MinorCycleOutcome,
    cycle_controls: &CycleControls,
    global_threshold: f64,
    started: Instant,
) -> usize {
    let entering = (
        outcome.total_minor_iterations,
        outcome.total_actual_minor_iterations,
    );
    controller.record_minor_cycle(minor.summary.iterations, minor.summary.peak);
    outcome.total_minor_iterations = controller.iterations();
    outcome.total_actual_minor_iterations += minor.summary.components;
    let cycle = outcome.minor_cycles.len() + 1;
    let record = minor_cycle_record(
        cycle,
        minor,
        cycle_controls,
        global_threshold,
        entering,
        (
            outcome.total_minor_iterations,
            outcome.total_actual_minor_iterations,
        ),
    );
    tracing::info!(
        "imaging minor cycle {cycle}: {} iterations ({} total), peak {:.6} -> {:.6} Jy, \
         cycle threshold {:.6} Jy, stop {:?}, {:.2} s",
        record.iterations,
        record.total_iterations,
        record.initial_peak_flux,
        record.final_peak_flux,
        record.effective_threshold,
        record.stop_reason,
        started.elapsed().as_secs_f64(),
    );
    outcome.minor_cycles.push(record);
    outcome.masks = Some(ReconstructionMaskSet::Domains(minor.masks.clone()));
    cycle
}

/// Predict the final model once more to write the model column: the pass
/// that ended the run was not known to be the last when it ran.
fn predict_final_model(
    run: &mut Run<'_>,
    mut outcome: ImagingOutcome,
) -> Result<ImagingOutcome, ImagingError> {
    let major = run.refresh(outcome.scientific, (Vec::new(), Reservation::none()), true)?;
    outcome.scientific = major.completion;
    outcome.visibility_products = major.visibility;
    outcome.major_cycle_count += 1;
    Ok(outcome)
}

/// The minor cycle's view of the run.
fn minor_setup(
    problem: &CompiledProblem,
    response: Option<MinorCycleImageResponse>,
    masks: &ImageDomainReconstructionMaskPlans,
) -> MinorCycleSetup {
    let controls = problem.reconstruction().controls();
    MinorCycleSetup {
        algorithm: problem.reconstruction().algorithm().clone(),
        accounting: controls.hogbom_iteration_accounting(),
        response,
        nsigma: controls.noise_sigma().unwrap_or(0.0),
        automask: matches!(
            masks.primary(),
            ReconstructionMaskPlan::AutoMultithresh { .. }
        ),
    }
}

impl<'a> Run<'a> {
    /// The operators, worker teams, source and imaging weights of a run.
    /// The teams take the most workers, up to the policy's, whose charges
    /// fit beside the operators and the source ([`setup::fit_workers`],
    /// #737); a paged kernel set is admitted for the count chosen, and each
    /// later phase is admitted as it starts. `minor` is what a minor cycle
    /// holds on a team of so many workers and `refresh` what a refresh pass
    /// holds beside its wave. `chosen` receives the choice as it is made.
    fn open(
        inputs: ImagingInputs<'a>,
        (minor, refresh): (&dyn Fn(usize) -> u64, Option<RefreshDemand>),
        chosen: &mut Option<WorkerChoice>,
    ) -> Result<Self, ImagingError> {
        let problem = inputs.problem;
        let backend = inputs.backend;
        let correlations = selected_correlations(problem)?;
        let selected = inputs
            .access
            .into_deferred()
            .open(problem)
            .map_err(|error| ImagingError::Observation(Box::new(error)))?;
        let dish_classes = selected.antenna_response_classes();
        let (host, policy) = (inputs.host, inputs.policy);
        // The passes' workers hold the kernels: at most the policy's.
        let ceiling = policy.cores(&host);
        let admission = KernelAdmission {
            host: &host,
            policy: &policy,
            workers: ceiling.streaming_workers,
        };
        // One operator per chart: each image domain, or each facet of a
        // faceted one.
        let charts = charts(problem);
        let mut domains = charts
            .iter()
            .map(|chart| {
                domain_operator(
                    problem,
                    chart.extent(&problem.geometry().domains()[chart.domain])?,
                    &correlations,
                    backend,
                    inputs.grid_precision,
                    inputs.aw_catalog.as_ref(),
                    &dish_classes,
                    admission,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let weight_image = domains.iter().any(|domain| domain.weight_image);
        if inputs.visibility_write.is_some() && problem.geometry().domains().len() > 1 {
            return Err(ImagingError::Unsupported {
                reason: "visibilities are written back for one image domain",
            });
        }
        let cancel = inputs.cancel;
        let channel_local = matches!(domains[0].operator.basis(), Basis::ChannelLocal { .. });
        let mut source = MeasurementSetSource::new(
            problem,
            selected,
            domains[0].operator.basis().planes(),
            plane_bounds(problem),
            domains
                .iter()
                .any(|domain| domain.operator.cf().pointing_ramp()),
            dish_classes,
        );
        let stitch = ChartStitch::new(problem, charts.clone());
        let visibility_write_bytes = inputs
            .visibility_write
            .as_ref()
            .map(VisibilityWriteTarget::retained_bytes)
            .transpose()
            .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?
            .unwrap_or(0);
        let initial = initial_modes(weight_image);
        let demand = SetupDemand {
            problem,
            domains: &domains,
            stitch: &stitch,
            block: source.maximum_block(),
            native_spacing_hz: native_spacing_hz(problem),
            backend,
            modes: initial,
            // A cube's model is paged with its state.
            model_bytes: if channel_local {
                0
            } else {
                ModelLifecycle::new(problem, ModelStoragePlan::resident(usize::MAX)?)
                    .resident_bytes()
            },
            write: inputs.visibility_write.as_ref().map(|_| {
                let (modes, with_model) = write_pass(problem, initial);
                (modes, with_model, visibility_write_bytes)
            }),
            spill_directory: inputs.spill_directory,
            weight_image,
            minor,
            refresh,
            kept_weights: kept_weight_bytes(
                problem,
                &domains,
                source.maximum_block().channels(),
                free_memory(&host, &policy),
            )
            .map_or(0, |(_, bytes)| bytes),
            free: free_memory(&host, &policy),
        };
        let choice = setup::fit_workers(ceiling, demand.free, |cores| demand.bytes(cores))?;
        *chosen = Some(choice.clone());
        if let Some(reason) = &choice.reason {
            tracing::warn!(
                "imaging on {} workers, not the resource policy's {}: {reason}",
                choice.cores.workers,
                choice.policy.workers
            );
        }
        tracing::info!(
            "workers: {} computing, {} streaming beside the source's producer",
            choice.cores.workers,
            choice.cores.streaming_workers
        );
        let formed = WorkerTeams::new(choice.cores).map_err(ImagingError::Pass)?;
        let teams = inputs.teams.get_or_init(move || formed);
        // An automatic AW cell cache takes what the chosen workers leave
        // free (#449).
        let mut spare = demand.free.saturating_sub(demand.bytes(choice.cores)?);
        for domain in &mut domains {
            domain.admit_kernels(admission, choice.cores.streaming_workers, &mut spare)?;
        }
        let streaming = teams.streaming();
        let started = Instant::now();
        let (weighting, weights) = setup::weighting(
            problem,
            &domains[0],
            &mut source,
            streaming,
            &cancel,
            (&host, &policy),
        )?;
        tracing::info!(
            "imaging weights: {} workers, {:.2} s",
            streaming.workers(),
            started.elapsed().as_secs_f64()
        );
        // The minor cycles, the widest team, access the cube state's planes.
        let cube = channel_local
            .then(|| {
                setup::cube_state(
                    problem,
                    inputs.spill_directory,
                    (teams.compute().workers(), weight_image),
                    &host,
                    &policy,
                )
            })
            .transpose()?;
        let lifecycle = ModelLifecycle::new(
            problem,
            match &cube {
                Some(cube) => cube.model_storage()?,
                None => ModelStoragePlan::resident(usize::MAX)?,
            },
        );
        let run = Self {
            problem,
            domains,
            stitch,
            charts,
            source,
            weighting,
            _weights: weights,
            teams,
            cancel,
            host,
            policy,
            cube,
            lifecycle,
            native_spacing_hz: native_spacing_hz(problem),
            backend,
            metal_stage_timing: inputs.diagnostics.metal_stage_timing,
            visibility_write: inputs.visibility_write,
            visibility_write_bytes,
            planes_per_wave: None,
            weight_image,
            retained: Reservation::none(),
            kept_weights: None,
            row_work: None,
        };
        if run.visibility_write.is_some() {
            // The pass that writes must hold every plane, and is refused now
            // rather than after the cycles that precede it.
            let (modes, with_model) = write_pass(problem, initial);
            if run
                .admit_pass(modes, with_model, run.visibility_write_bytes)?
                .value
                .residency
                != Residency::All
            {
                return Err(ImagingError::Pass(PassError::VisibilityWriteWaves));
            }
        }
        Ok(run)
    }

    /// The initial major cycle: data and PSF, from an empty model; it writes
    /// visibilities when it is also final.
    fn initial(&mut self, last: bool) -> Result<Major, ImagingError> {
        self.measure_row_work()?;
        // The model is admitted with the pass, before it is allocated.
        let Admitted {
            value: plan,
            reservation: pass,
        } = self.admit_pass(
            self.initial_modes(),
            false,
            self.lifecycle.resident_bytes() + self.write_bytes(last),
        )?;
        let model = self
            .lifecycle
            .prepare_final_model(self.lifecycle.initial_empty()?, [])?;
        let cycle = MajorCycle::initial(self.problem, model, self.normal_storage(plan.residency)?)?;
        let major = self.reconcile(cycle, true, last, plan)?;
        self.retain(&major, pass)?;
        Ok(major)
    }

    /// The major cycle after a minor cycle: the residual of the model updated
    /// by `terms`.
    fn refresh(
        &mut self,
        completion: MajorCycleCompletion,
        (terms, results): (Vec<ModelDeltaTerm>, Reservation),
        last: bool,
    ) -> Result<Major, ImagingError> {
        // The retained charge still covers the normal state and model the
        // pass refreshes; the pass adds its scratch and the new residual,
        // and the minor cycle's charge covers its terms until the model
        // holds them.
        let (normal_state, model) = completion.into_parts();
        let Admitted {
            value: plan,
            reservation: mut pass,
        } = self.admit_pass(ModeSet::DATA, true, self.write_bytes(last))?;
        let model = self.lifecycle.prepare_final_model(model, terms)?;
        let cycle = MajorCycle::refresh(
            self.problem,
            normal_state,
            model,
            self.normal_storage(plan.residency)?,
        )?;
        let major = self.reconcile(cycle, false, last, plan)?;
        pass.join(results);
        self.retain(&major, pass)?;
        Ok(major)
    }

    /// What the pass holds for the visibility write: the write's bytes on
    /// the last pass, which writes.
    const fn write_bytes(&self, last: bool) -> u64 {
        if last { self.visibility_write_bytes } else { 0 }
    }

    /// Admit `memory` bytes for `phase` under the run's policy.
    fn admit(&self, phase: &'static str, memory: u64) -> Result<Reservation, ImagingError> {
        Ok(admit(&self.host, &self.policy, &Demand { phase, memory })?)
    }

    /// Hand the pass's charge on to what `major` holds resident: join it to
    /// the retained charge and keep only the completion's resident bytes.
    fn retain(&mut self, major: &Major, pass: Reservation) -> Result<(), ImagingError> {
        let resident = major.completion.resident_bytes()?;
        self.retained.join(pass);
        debug_assert!(
            resident <= self.retained.memory(),
            "the pass admitted what it hands on: {resident} bytes resident, {} charged",
            self.retained.memory()
        );
        if resident > self.retained.memory() {
            // The pass's plan undercounted what it hands on: admitted after
            // the fact, so said.
            tracing::warn!(
                resident,
                charged = self.retained.memory(),
                "a pass handed on more than it admitted"
            );
            self.retained.join(admit(
                &self.host,
                &self.policy,
                &Demand {
                    phase: "retained normal state",
                    memory: resident - self.retained.memory(),
                },
            )?);
        }
        self.retained.retain(resident);
        Ok(())
    }

    /// Run `cycle`'s pass and finish it. The initial pass grids the data;
    /// every refresh grids the residual of the cycle's model.
    fn reconcile(
        &mut self,
        mut cycle: MajorCycle,
        initial: bool,
        last: bool,
        plan: PassPlan,
    ) -> Result<Major, ImagingError> {
        let residency = plan.residency;
        let modes = if initial {
            self.initial_modes()
        } else {
            ModeSet::DATA
        };
        let transform = self.problem.visibility_transform();
        let mut writer = self
            .visibility_write
            .as_ref()
            .filter(|_| last)
            .map(|target| VisibilityWriter::begin(target, transform))
            .transpose()
            .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?;
        if let Residency::Waves { planes_per_wave } = residency {
            self.planes_per_wave = Some(
                self.planes_per_wave
                    .map_or(planes_per_wave, |planes| planes.min(planes_per_wave)),
            );
        }
        let started = Instant::now();
        let (model, state) = cycle.parts();
        let summary = self.pass(
            modes,
            (!initial).then_some(model),
            plan,
            state,
            initial,
            writer.as_mut(),
        )?;
        let pass_seconds = started.elapsed().as_secs_f64();
        let visibility = writer
            .map(VisibilityWriter::complete)
            .transpose()
            .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?
            .map(VisibilityProductCompletion::new);
        let completion = cycle.finish(summary.samples, summary.blocks)?;
        tracing::info!(
            "imaging major cycle: {} samples, pass {pass_seconds:.2} s, total {:.2} s",
            summary.samples,
            started.elapsed().as_secs_f64(),
        );
        self.keep_row_work(MeasuredRowWork {
            rows: summary.row_work,
            modes: Vec::new(),
        });
        Ok(Major {
            completion,
            visibility,
        })
    }

    /// Normal storage whose paged window holds the planes of one wave of
    /// `residency`.
    fn normal_storage(&self, residency: Residency) -> Result<NormalStoragePlan, ImagingError> {
        let planes = self.domains[0].operator.basis().planes() as usize;
        Ok(match &self.cube {
            Some(cube) => cube.normal_storage(match residency {
                Residency::All => planes,
                Residency::Waves { planes_per_wave } => planes_per_wave as usize,
            })?,
            None => NormalStoragePlan::resident(planes)?,
        })
    }

    /// The modes of the initial pass ([`initial_modes`]).
    const fn initial_modes(&self) -> ModeSet {
        initial_modes(self.weight_image)
    }

    /// The waves of a pass accumulating `modes`, with a model when
    /// `with_model`, that fit the free memory beside `besides` bytes the
    /// pass also allocates (an initial model) and the faceted domains'
    /// stitched images ([`ChartStitch::pass_bytes`]), and the reservation of
    /// all three: planned once per major cycle for the pass and its normal
    /// storage.
    fn admit_pass(
        &self,
        modes: ModeSet,
        with_model: bool,
        besides: u64,
    ) -> Result<Admitted<PassPlan>, ImagingError> {
        let besides = besides + self.stitch.pass_bytes(modes.psf);
        let workers = self.teams.streaming().workers();
        let domains = pass_domains(&self.domains, workers, self.row_work(modes));
        let mut demand = WaveDemand {
            domains: &domains,
            modes,
            with_model,
            native_spacing_hz: self.native_spacing_hz,
            workers,
            backend: self.backend,
            block: self.source.maximum_block(),
            chunk_placements: CHUNK_PLACEMENTS,
        };
        let free = free_memory(&self.host, &self.policy).saturating_sub(besides);
        let residency = Residency::plan(&demand, free);
        let planes = match residency {
            Residency::All => demand.planes(),
            Residency::Waves { planes_per_wave } => planes_per_wave,
        };
        // Region owners share larger slices more evenly; take the largest
        // that fits beside the waves just planned (#736).
        demand.chunk_placements = demand.plan_chunk_placements(planes, free);
        let reservation = admit(
            &self.host,
            &self.policy,
            &Demand {
                phase: "major-cycle pass",
                memory: demand.bytes(planes) + besides,
            },
        )?;
        Ok(Admitted {
            value: PassPlan {
                residency,
                chunk_placements: demand.chunk_placements,
            },
            reservation,
        })
    }

    /// Keep the imaging weights of every sample across passes (#659) when a
    /// single-wave pass places one image domain's rows a channel at a time
    /// and the slots, four bytes per selected (row, channel), take at most a
    /// sixteenth of the free memory; charged before they are allocated.
    /// Wider runs weigh every pass.
    fn keep_weights(&self, residency: Residency) -> Option<(WeightCache, Reservation)> {
        if residency != Residency::All {
            return None;
        }
        let (samples, bytes) = kept_weight_bytes(
            self.problem,
            &self.domains,
            self.source.maximum_block().channels(),
            free_memory(&self.host, &self.policy),
        )?;
        let charge = self.admit("kept imaging weights", bytes).ok()?;
        Some((WeightCache::new(samples), charge))
    }

    /// Measure each region-split domain's gridding work by anchor row before
    /// the first pass, over a traversal of flags and weights only
    /// ([`run_row_work_pass`]), so that the first pass cuts balanced strips
    /// too (#736). A run without several region owners measures nothing,
    /// and the pass is charged while it runs.
    fn measure_row_work(&mut self) -> Result<(), ImagingError> {
        if self.row_work.is_some() {
            return Ok(());
        }
        let team = self.teams.streaming();
        let work = {
            let domains = pass_domains(&self.domains, team.workers(), None);
            if !needs_row_work(&domains) {
                return Ok(());
            }
            let _charge = self.admit(
                "region row-work pass",
                row_work_pass_bytes(&domains, team.workers()),
            )?;
            let started = Instant::now();
            let work = run_row_work_pass(
                &domains,
                &self.weighting,
                &mut self.source,
                team,
                &self.cancel,
            )?;
            tracing::info!(
                "imaging region row work: measured in {:.2} s",
                started.elapsed().as_secs_f64()
            );
            work
        };
        self.keep_row_work(work);
        Ok(())
    }

    /// The kept row work ([`Self::keep_row_work`]), for a pass of `modes`.
    fn row_work(&self, modes: ModeSet) -> Option<(&MeasuredRowWork, ModeSet)> {
        self.row_work.as_ref().map(|(work, _)| (work, modes))
    }

    /// Keep the first pass's gridding work by anchor row, and the work in
    /// each mode a row-work pass measured, for the passes' strips
    /// ([`Partition::regions_balanced`]), charged before it is kept. Every
    /// pass places the same samples, so one histogram serves the run.
    /// Without a region-split domain, or without room, the later passes
    /// keep strips of equal height.
    fn keep_row_work(&mut self, row_work: MeasuredRowWork) {
        if self.row_work.is_some() || row_work.rows.iter().all(Option::is_none) {
            return;
        }
        let bytes = row_work
            .rows
            .iter()
            .flatten()
            .map(|rows| size_of_val(rows.as_slice()) as u64)
            .sum::<u64>()
            + size_of_val(row_work.modes.as_slice()) as u64;
        match self.admit("region row work", bytes) {
            Ok(charge) => self.row_work = Some((row_work, charge)),
            Err(_) => tracing::info!(
                "imaging region row work: no room to keep {bytes} bytes; \
                 later passes keep strips of equal height"
            ),
        }
    }

    /// One pass over every image domain, appending its images to `state`.
    fn pass(
        &mut self,
        modes: ModeSet,
        model: Option<&ModelGeneration>,
        plan: PassPlan,
        mut state: PassAppender<'_>,
        initial: bool,
        mut writer: Option<&mut VisibilityWriter<'_>>,
    ) -> Result<PassSummary, ImagingError> {
        let PassPlan {
            residency,
            chunk_placements,
        } = plan;
        if self.kept_weights.is_none() {
            self.kept_weights = self.keep_weights(residency);
        }
        let team = self.teams.streaming();
        let domains = pass_domains(&self.domains, team.workers(), self.row_work(modes));
        let charts = &self.charts;
        let prepare = |chart: usize, planes: PlaneRange| {
            let generation = model.expect("the closure is installed only with a model");
            let Chart { domain, window } = charts[chart];
            prepare_model(
                &self.domains[chart].operator,
                generation,
                (domain, window),
                planes,
            )
            .map_err(|error| PassError::Model(Box::new(error)))
        };
        let pass = MajorCyclePass {
            domains: &domains,
            weighting: &self.weighting,
            modes,
            model: model.map(|_| &prepare as &ModelPreparation<'_>),
            residency,
            native_spacing_hz: self.native_spacing_hz,
            backend: self.backend,
            metal_stage_timing: self.metal_stage_timing,
            weights: self.kept_weights.as_ref().map(|(cache, _)| cache),
            chunk_placements,
        };
        let predictions = writer.as_deref().map(VisibilityWriter::needs_predictions);
        let mut write = |block: &_, predictions: &[_]| match writer.as_deref_mut() {
            Some(writer) => writer.write(block, predictions),
            None => Ok(()),
        };
        let mut sink = predictions.map(|predictions| VisibilitySink {
            predictions,
            write: &mut write,
        });
        Ok(run_major_cycle(
            &pass,
            &mut self.source,
            team,
            &self.cancel,
            &mut |chart, images| {
                let basis = self.domains[chart].operator.basis();
                let images = self
                    .stitch
                    .accept(chart, pass_images(chart, &images, initial, basis))
                    .map_err(|error| PassError::Images(error.into()))?;
                match images {
                    Some(images) => state
                        .append(images)
                        .map_err(|error| PassError::Images(Box::new(error))),
                    None => Ok(()),
                }
            },
            sink.as_mut(),
        )?)
    }
}

/// The samples and bytes of the imaging weights a run keeps across its
/// passes ([`Run::keep_weights`]): one image domain placed a channel at a
/// time, `channels` per row, whose slots take at most a sixteenth of
/// `free`; `None` when the run weighs every pass instead.
fn kept_weight_bytes(
    problem: &CompiledProblem,
    domains: &[DomainOperator],
    channels: usize,
    free: u64,
) -> Option<(usize, u64)> {
    if domains.len() != 1 || !domains[0].resampler.is_direct() {
        return None;
    }
    let rows = problem
        .observation()
        .sources()
        .iter()
        .map(|source| source.selection().rows().selected_row_count())
        .sum::<u64>();
    let samples = usize::try_from(rows).ok()?.checked_mul(channels)?;
    let bytes = WeightCache::bytes(samples);
    (bytes <= free / 16).then_some((samples, bytes))
}

/// Every image domain's operator, resampler and partition: planes split
/// among `workers` for a channel-local basis, grid strips otherwise, cut to
/// equal shares of an earlier pass's gridding work by anchor row when the
/// run has it (`row_work`, per domain; #736), for a pass of the modes given
/// with it: one that grids the weight image adds that work to the centre
/// row's owner ([`casa_imaging_runtime::pass::ModeWork::weighed`]).
pub(super) fn pass_domains<'d>(
    domains: &'d [DomainOperator],
    workers: usize,
    row_work: Option<(&MeasuredRowWork, ModeSet)>,
) -> Vec<PassDomain<'d>> {
    domains
        .iter()
        .enumerate()
        .map(|(index, domain)| PassDomain {
            operator: &domain.operator,
            resampler: &domain.resampler,
            partition: match domain.operator.basis() {
                Basis::ChannelLocal { .. } => Partition::Planes { owners: workers },
                Basis::Constant | Basis::Taylor { .. } => {
                    match row_work.and_then(|(work, modes)| {
                        let rows = work.rows.get(index)?.as_ref()?;
                        Some((
                            rows,
                            work.modes.get(index).and_then(|w| w.weighed(rows, modes)),
                        ))
                    }) {
                        Some((rows, weighed)) => Partition::regions_balanced(
                            &domain.operator,
                            workers,
                            weighed.as_deref().unwrap_or(rows),
                        ),
                        None => Partition::regions(&domain.operator, workers),
                    }
                }
            },
        })
        .collect()
}

/// The next cycle's mask plans: automask evolves from the masks just used,
/// and knows whether the cycle cleaned to the global threshold.
fn next_masks(
    plans: &ImageDomainReconstructionMaskPlans,
    outcome: &MinorCycleOutcome,
    cycle: usize,
    threshold_reached: bool,
) -> Result<ImageDomainReconstructionMaskPlans, ImagingError> {
    let stopped = outcome
        .auto_masks
        .iter()
        .map(|evidence| evidence.is_some_and(|evidence| evidence.channel_stopped))
        .collect::<Vec<_>>();
    Ok(plans.next_cycle(&outcome.masks, cycle, threshold_reached, &stopped)?)
}

/// The modes of the initial pass: data and PSF, plus the sensitivity image
/// when a kernel set has weight taps (mosaic, AW), `weight_image`.
const fn initial_modes(weight_image: bool) -> ModeSet {
    ModeSet {
        data: true,
        psf: true,
        weight: weight_image,
    }
}

/// The modes of the pass that writes visibilities, and whether it has a
/// model: the initial pass, of modes `initial`, without cleaning, and a
/// residual refresh otherwise.
fn write_pass(problem: &CompiledProblem, initial: ModeSet) -> (ModeSet, bool) {
    if problem.reconstruction().controls().max_minor_iterations() > 0 {
        (ModeSet::DATA, true)
    } else {
        (initial, false)
    }
}

/// The output-frame frequency envelope of a plane range of a channel-local
/// basis: from the low edge of its first channel to the high edge of its
/// last.
fn plane_bounds(problem: &CompiledProblem) -> Option<PlaneBounds> {
    if !matches!(
        problem.reconstruction().basis(),
        casa_imaging_model::ReconstructionBasis::ChannelLocal { .. }
    ) {
        return None;
    }
    let SpectralWcs::Linear {
        reference_pixel,
        reference_frequency_hz,
        increment_hz,
        ..
    } = *problem.geometry().spectral().wcs()
    else {
        return None;
    };
    let centre = move |channel: u32| {
        reference_frequency_hz + (f64::from(channel) - reference_pixel) * increment_hz
    };
    Some(Box::new(move |planes: PlaneRange| {
        let half = increment_hz.abs() / 2.0;
        let [first, last] = [centre(planes.start), centre(planes.end - 1)];
        [first.min(last) - half, first.max(last) + half]
    }))
}

/// The record of one minor cycle; `global_threshold` is the run's
/// threshold or n-sigma threshold, whichever is larger.
/// Bytes [`minor_cycle_record`] copies from `outcome`: its primary mask's
/// support and its traced components.
fn record_bytes(outcome: &MinorCycleOutcome) -> u64 {
    (std::mem::size_of_val(outcome.masks.primary().support())
        + std::mem::size_of_val(outcome.summary.trace.as_slice())) as u64
}

fn minor_cycle_record(
    cycle: usize,
    outcome: &MinorCycleOutcome,
    controls: &CycleControls,
    global_threshold: f64,
    (iterations_entering, actual_iterations_entering): (usize, usize),
    (total_iterations, total_actual_iterations): (usize, usize),
) -> NativeMinorCycleOutcome {
    let summary = &outcome.summary;
    let mask = outcome.masks.primary();
    NativeMinorCycleOutcome {
        cycle,
        iterations_entering,
        iterations: summary.iterations,
        total_iterations,
        actual_iterations_entering,
        actual_iterations: summary.components,
        total_actual_iterations,
        total_flux: summary.absolute_flux,
        initial_peak_flux: summary.start_peak,
        final_peak_flux: summary.peak,
        noise_rms: summary.noise_rms,
        effective_threshold: controls.threshold.max(global_threshold),
        global_threshold,
        cycle_threshold: Some(controls.threshold),
        stop_reason: NativeMinorCycleStopReason::from_planes(&summary.stops),
        clark_refreshes: summary.refreshes,
        associated_replay_ordinal: cycle,
        recorded_components: summary.trace.clone(),
        mask_support: mask.support().to_vec(),
        auto_mask: outcome.auto_masks[0],
    }
}
