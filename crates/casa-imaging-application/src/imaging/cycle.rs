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

use casa_imaging_deconvolution::{
    CleanStop, Controller, CycleControls, NormalStoragePlan, ResidualStatistics,
};
use casa_imaging_deconvolution::{
    ImageDomainReconstructionMaskPlans, MajorCycle, MajorCycleCompletion, ModelGeneration,
    ModelLifecycle, ModelStoragePlan, PassAppender, ReconstructionMaskPlan, ReconstructionMaskSet,
};
use casa_imaging_model::{CompiledProblem, ModelDeltaTerm, UncorrectedImageMaskPolicy};
use casa_imaging_operator::{Basis, GridPrecision, ModeSet, PlaneRange, WeightingGeneration};
use casa_imaging_products::{
    AnalyticPrimaryBeam, FacetSumWeights, MinorCycleImageResponse, VisibilityProductCompletion,
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
    prepare_bytes, prepare_bytes_for, prepare_minor_cycle, psf_cache_bytes, run_bytes,
    run_bytes_for, run_minor_cycle, run_phase,
};

use super::charts::{Chart, ChartStitch, charts};
use super::images::{pass_images, prepare_model};
use super::measurement::{
    DomainOperator, KernelAdmission, domain_operator, native_spacing_hz, selected_correlations,
};
use super::observation_source::PlaneBounds;
use super::observation_source::{BatchRoute, ObservationSource};
use super::setup::{self, RefreshDemand, SetupDemand, WorkerChoice};
use super::visibility_write::{VisibilityWriteTarget, VisibilityWriter};
use super::{ImagingError, trace};
use crate::compile::StartModel;

mod clean;
mod continuation;
mod prediction;
mod separate_psf;
pub(crate) use separate_psf::form as form_separate_psf;
mod seeding;
use crate::restart::ExistingProducts;
use crate::{
    AwCatalogDeployment, Awp2Deployment, NativeMinorCycleOutcome, NativeMinorCycleStopReason,
    PlaneRows,
};
pub(crate) use continuation::Forms;

/// What one native imaging run needs besides the compiled problem.
pub(crate) struct ImagingInputs<'a> {
    pub(crate) problem: &'a CompiledProblem,
    pub(crate) mvc: Option<MvcCycleSpec<'a>>,
    pub(crate) psf_cutoff: f32,
    pub(crate) psf_beams: Option<std::sync::Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
    pub(crate) mask_beams: Option<std::sync::Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
    /// The observation's batches the passes read.
    pub(crate) source: &'a BatchRoute,
    pub(crate) separate_psf: Option<Admitted<Vec<f32>>>,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    /// The main domain's start model, which the initial pass seeds the
    /// model from.
    pub(crate) start_model: Option<StartModel>,
    /// The PSF, sums of weights and residual an earlier run left, which the
    /// run reads where it does not form them.
    pub(crate) existing: Option<ExistingProducts>,
    /// What the run forms of its normal state (`calcpsf`, `calcres`).
    pub(crate) forms: Forms,
    pub(crate) image_response: Option<MinorCycleImageResponse>,
    /// The analytic primary beam of a run without a response, which its
    /// minor cycles measure their statistics inside.
    pub(crate) primary_beam: Option<AnalyticPrimaryBeam<'a>>,
    pub(crate) visibility_write: Option<VisibilityWriteTarget<'a>>,
    /// What the products hold on one lane, and the source's charge, which
    /// the run releases before them ([`SetupDemand::products`]).
    pub(crate) products: (u64, u64),
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
    pub(crate) awp2: Option<Awp2Deployment>,
    pub(crate) backend: BackendChoice,
    /// The requested grid precision; `None` is `f64` on the CPU, `f32` on Metal.
    pub(crate) grid_precision: Option<GridPrecision>,
    pub(crate) ft_padding: Option<casa_imaging_operator::PaddingFactor>,
    pub(crate) diagnostics: Diagnostics,
}

impl ImagingInputs<'_> {
    fn kernel_deployments(&self) -> (Option<(&AwCatalogDeployment, f64)>, Option<&Awp2Deployment>) {
        // Availability restricts AW to a single MeasurementSet.
        let aw = self.aw_catalog.as_ref().map(|catalog| {
            (
                catalog,
                self.source.stores[0].plan.gridding_frequency_scale(),
            )
        });
        (aw, self.awp2.as_ref())
    }
}

/// The independently compiled Taylor view used by an MVC cleaning cycle.
#[derive(Clone, Copy)]
pub(crate) struct MvcCycleSpec<'a> {
    pub(crate) taylor_problem: &'a CompiledProblem,
    pub(crate) start: Option<&'a crate::compile::MvcStart>,
    pub(crate) expansion_reference_hz: f64,
    pub(crate) controls: casa_imaging_products::ContinuumProductControls,
    pub(crate) centres: Option<&'a casa_imaging_products::PrimaryBeamCentres>,
}

/// The final reconciliation and the record of the cycles that led to it.
pub(crate) struct ImagingOutcome {
    pub(crate) scientific: MajorCycleCompletion,
    pub(crate) mvc_taylor: Option<Admitted<crate::mvc::TaylorCompletion>>,
    pub(crate) masks: Option<ReconstructionMaskSet>,
    pub(crate) minor_cycles: Vec<NativeMinorCycleOutcome>,
    pub(crate) plane_rows: PlaneRows,
    pub(crate) final_residual_statistics: Option<ResidualStatistics>,
    pub(crate) stop: Option<CleanStop>,
    pub(crate) major_cycle_count: usize,
    pub(crate) total_minor_iterations: usize,
    pub(crate) total_actual_minor_iterations: usize,
    /// Last MEM entropy multiplier, if any step ran.
    pub(crate) mem_alpha: Option<f32>,
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
    source: Box<dyn BoundedSource + 'a>,
    separate_psf: Option<Admitted<Vec<f32>>>,
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
    /// The start model the initial generation is seeded from; with one,
    /// the initial pass grids the residual of its prediction.
    start_model: Option<StartModel>,
    /// The flat-noise response the start model is made physical with,
    /// after an initial pass without it has formed the weights
    /// ([`seeding`]); `None` when it is physical already.
    reseed: Option<MinorCycleImageResponse>,
    /// What the run reads in place of forming it ([`continuation`]).
    existing: Option<ExistingProducts>,
    forms: Forms,
    native_spacing_hz: f64,
    backend: BackendChoice,
    /// Whether each Metal backend logs its timing.
    metal_stage_timing: bool,
    visibility_write: Option<VisibilityWriteTarget<'a>>,
    /// What the visibility write holds, admitted with the pass that writes;
    /// zero when nothing is written.
    visibility_write_bytes: u64,
    /// Planes per wave of the most finely waved pass so far.
    planes_per_wave: Option<u32>,
    /// Each main-image plane's start-model flux, in `.model`'s units.
    start_model_flux: Vec<([usize; 3], f64)>,
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
/// holds, the placements a row chunk takes (#736) and the sets of chunks its
/// owners pipeline over (#449).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PassPlan {
    residency: Residency,
    chunk_placements: usize,
    chunk_sets: usize,
}

/// One reconciled major cycle.
struct Major {
    completion: MajorCycleCompletion,
    visibility: Option<VisibilityProductCompletion>,
}

impl ImagingOutcome {
    /// The outcome of a run that has formed its first normal state `major`:
    /// its initial major cycle, unless the run read its residual rather
    /// than form it (`calcres=False`), which CASA does not count.
    fn initial(major: Major, run: &Run<'_>) -> Self {
        Self {
            scientific: major.completion,
            mvc_taylor: None,
            masks: None,
            minor_cycles: Vec::new(),
            plane_rows: PlaneRows {
                start_model_flux: run.start_model_flux.clone(),
                cycles: Vec::new(),
            },
            final_residual_statistics: None,
            stop: None,
            major_cycle_count: usize::from(run.forms.residual),
            total_minor_iterations: 0,
            total_actual_minor_iterations: 0,
            mem_alpha: None,
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
    let mvc = inputs.mvc;
    let reconstruction = mvc.map_or(problem, |view| view.taylor_problem);
    let controls = reconstruction.reconstruction().controls();
    let cleaning = controls.max_minor_iterations() > 0;
    // MVC's cube-to-Taylor reduction has already applied the beam response.
    let minor_response = if mvc.is_some() {
        None
    } else {
        inputs.image_response
    };
    let setup = minor_setup(
        reconstruction,
        (minor_response, inputs.primary_beam),
        &inputs.masks,
        inputs.psf_cutoff,
        inputs.psf_beams.clone(),
        inputs.mask_beams.clone(),
    );
    let mask_plans = inputs.masks.clone();
    let cancel = inputs.cancel.clone();
    let diagnostics = inputs.diagnostics;
    // What a minor cycle holds on a team of so many workers, from the
    // layout the first pass forms, before any image exists (#737): the masks
    // it keeps and the larger of its two halves.
    let layout = MinorLayout::planned(reconstruction);
    let iterations = Controller::new(&controls).cycle_iterations();
    let minor = |workers: usize| {
        if cleaning {
            2 * layout.mask_bytes(&mask_plans)
                + layout.psf_cache_bytes()
                + prepare_bytes_for(&layout, &mask_plans, &setup, true, workers)
                    .max(run_bytes_for(&layout, &setup, iterations, 0, workers))
        } else {
            0
        }
    };
    let refresh = refresh_demand(&inputs, cleaning.then_some((&layout, &setup, iterations)));
    // Operators, the workers, the source, the imaging weights and the paged
    // cube state. The choice of workers is recorded as it is made, so a
    // setup that then fails still reports it.
    let mut chosen = None;
    let opened = run_phase("setup", &cancel, diagnostics, summary, || {
        Run::open(inputs, (&minor, refresh), &mut chosen, false)
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
    let major = run_phase(
        run.forms.initial_phase(),
        &cancel,
        diagnostics,
        summary,
        || run.initial(!cleaning, mvc.is_some_and(|view| view.start.is_some())),
    )?;
    let mut outcome = ImagingOutcome::initial(major, &run);
    if !cleaning {
        if run.visibility_write.is_some() && outcome.visibility_products.is_none() {
            outcome = run_phase("model prediction", &cancel, diagnostics, summary, || {
                predict_final_model(&mut run, outcome)
            })?;
        }
        outcome.retained = std::mem::replace(&mut run.retained, Reservation::none());
        return Ok(outcome);
    }
    let mvc = if let Some(spec) = mvc {
        let (state, taylor) = clean::MvcState::begin(spec, outcome.scientific, &mut run)?;
        outcome.scientific = taylor;
        outcome.plane_rows.start_model_flux = std::mem::take(&mut run.start_model_flux);
        Some(state)
    } else {
        None
    };
    clean::run(
        clean::CleanInputs {
            run,
            outcome,
            mvc,
            controls,
            setup,
            mask_plans,
            cancel,
            diagnostics,
        },
        summary,
    )
}

/// What a refresh pass holds beside its wave (#763 review), or `None` for a
/// run that refreshes nothing. A cleaning run, with its minor cycle's
/// `layout`, `setup` and cycle `iterations`: every cycle before the pass
/// keeps its record, up to the last refresh `nmajor` allows, and the minor
/// cycle's terms, which its solve holds too (#778 review). Without cleaning,
/// a run that reseeds refreshes its initial pass ([`seeding`]), and so does
/// one that reads its PSF and forms its residual, or reads its residual and
/// writes the model ([`continuation`]).
fn refresh_demand(
    inputs: &ImagingInputs<'_>,
    cleaning: Option<(&MinorLayout, &MinorCycleSetup, usize)>,
) -> Option<RefreshDemand> {
    if let Some((layout, setup, iterations)) = cleaning {
        let controls = inputs
            .mvc
            .map_or(inputs.problem, |view| view.taylor_problem)
            .reconstruction()
            .controls();
        return Some(RefreshDemand {
            masks: 2 * layout.mask_bytes(&inputs.masks) + layout.psf_cache_bytes(),
            cached: layout.refresh_bytes(setup),
            results: layout.result_bytes(setup, iterations),
            records: layout.record_bytes()
                * controls.maximum_major_cycles().unwrap_or(1).max(1) as u64,
        });
    }
    (seeding::reseed_response(inputs.start_model.as_ref(), inputs.image_response).is_some()
        || inputs
            .forms
            .refreshes_without_cleaning(inputs.visibility_write.is_some()))
    .then_some(RefreshDemand {
        masks: 0,
        cached: 0,
        results: 0,
        records: 0,
    })
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
    if minor.mem_alpha.is_some() {
        outcome.mem_alpha = minor.mem_alpha;
    }
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
    outcome.plane_rows.cycles.push(minor.summary.planes.clone());
    outcome.masks = Some(ReconstructionMaskSet::Domains(minor.masks.clone()));
    cycle
}

/// Predict the final model once more to write the model column: the pass
/// that ended the run was not known to be the last when it ran, or the run
/// formed no residual with its model (CASA's `predictModel` after
/// `calcres=False` with no iterations). That run's residual stands as the
/// earlier run left it: it is read again over the one the prediction's pass
/// formed.
fn predict_final_model(
    run: &mut Run<'_>,
    mut outcome: ImagingOutcome,
) -> Result<ImagingOutcome, ImagingError> {
    if run
        .visibility_write
        .as_ref()
        .is_some_and(VisibilityWriteTarget::predicts_final_model)
    {
        let plan = prediction::admit_model(run)?;
        let corrected = if run
            .visibility_write
            .as_ref()
            .is_some_and(VisibilityWriteTarget::writes_corrected)
        {
            prediction::write_corrected(run)?
        } else {
            0
        };
        let model = prediction::write_model(run, outcome.scientific.final_model(), plan)?;
        outcome.visibility_products = Some(VisibilityProductCompletion::new(corrected + model));
        return Ok(outcome);
    }
    let mut major = run.refresh(
        outcome.scientific,
        (Vec::new(), Vec::new(), Reservation::none()),
        true,
    )?;
    if outcome.major_cycle_count == 0 {
        let visibility = major.visibility.take();
        major = run.reread_residual(major.completion)?;
        major.visibility = visibility;
    } else {
        outcome.major_cycle_count += 1;
    }
    outcome.scientific = major.completion;
    outcome.visibility_products = major.visibility;
    Ok(outcome)
}

/// The minor cycle's view of the run: its `response` or analytic
/// `primary_beam`, and whether its residual carries the beam's support as
/// a pixel mask, as its published residual does
/// ([`UncorrectedImageMaskPolicy::PrimaryBeam`]).
fn minor_setup<'a>(
    problem: &CompiledProblem,
    (response, primary_beam): (
        Option<MinorCycleImageResponse>,
        Option<AnalyticPrimaryBeam<'a>>,
    ),
    masks: &ImageDomainReconstructionMaskPlans,
    psf_cutoff: f32,
    psf_beams: Option<std::sync::Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
    mask_beams: Option<std::sync::Arc<[casa_imaging_deconvolution::RestoringBeam]>>,
) -> MinorCycleSetup<'a> {
    let controls = problem.reconstruction().controls();
    MinorCycleSetup {
        psf_cutoff,
        psf_beams,
        mask_beams,
        algorithm: problem.reconstruction().algorithm().clone(),
        accounting: controls.hogbom_iteration_accounting(),
        response,
        saved_taylor_residual: false,
        nsigma: controls.noise_sigma().unwrap_or(0.0),
        noise: controls.noise_estimate(),
        automask: matches!(
            masks.primary(),
            ReconstructionMaskPlan::AutoMultithresh { .. }
        ),
        primary_beam,
        residual_pixel_mask: problem.products().validity().uncorrected_mask()
            == UncorrectedImageMaskPolicy::PrimaryBeam,
        pbmask: controls.primary_beam_mask(),
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
        psf_only: bool,
    ) -> Result<Self, ImagingError> {
        let problem = inputs.problem;
        let backend = inputs.backend;
        let correlations = selected_correlations(problem)?;
        let route = inputs.source;
        let keys = route.keys();
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
                    inputs.ft_padding,
                    inputs.kernel_deployments(),
                    &keys,
                    admission,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let weight_image = !psf_only && domains.iter().any(|domain| domain.weight_image);
        if inputs.visibility_write.is_some() && problem.geometry().domains().len() > 1 {
            return Err(ImagingError::Unsupported {
                reason: "visibilities are written back for one image domain",
            });
        }
        let cancel = inputs.cancel;
        let channel_local = matches!(domains[0].operator.basis(), Basis::ChannelLocal { .. });
        let planes = domains[0].operator.basis().planes();
        let pointing_ramp = domains
            .iter()
            .any(|domain| domain.operator.cf().pointing_ramp());
        let mut source: Box<dyn BoundedSource + 'a> = Box::new(ObservationSource::new(
            problem,
            route,
            planes,
            plane_bounds(problem),
            pointing_ramp,
        ));
        let stitch = ChartStitch::new(problem, charts.clone());
        let visibility_write_bytes = inputs
            .visibility_write
            .as_ref()
            .map(VisibilityWriteTarget::retained_bytes)
            .transpose()
            .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?
            .unwrap_or(0);
        let initial = ModeSet {
            data: !psf_only,
            psf: true,
            weight: weight_image,
        };
        let reseed = seeding::reseed_response(inputs.start_model.as_ref(), inputs.image_response);
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
            start_model: inputs.start_model.as_ref().map(|start| {
                start.seeding_bytes() + reseed.map_or(0, |_| start.conversion_bytes())
            }),
            reseeds: reseed.is_some(),
            write: inputs.visibility_write.as_ref().map(|_| {
                let seeded = (inputs.start_model.is_some(), reseed.is_some());
                let (modes, with_model) = write_pass(problem, inputs.forms, initial, seeded);
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
            products: inputs.products.0,
            source: inputs.products.1,
            free: free_memory(&host, &policy),
        };
        let choice = setup::fit_workers(ceiling, demand.free, |cores| demand.bytes(cores))?;
        *chosen = Some(choice.clone());
        // The products run after the passes: a run whose products do not
        // fit even on one lane is refused now, not after its passes.
        drop(admit(
            &host,
            &policy,
            &Demand {
                phase: "products",
                memory: demand.products_bytes(choice.cores)?,
            },
        )?);
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
        // free (#449). They include a cube's cache at its quarter, so what
        // the cells take comes back to the cube's sizing below (#719).
        let unraised = demand.free.saturating_sub(demand.bytes(choice.cores)?);
        let mut spare = unraised;
        for domain in &mut domains {
            domain.admit_kernels(admission, choice.cores.streaming_workers, &mut spare)?;
        }
        let raised = unraised.saturating_sub(spare);
        let streaming = teams.streaming();
        let started = Instant::now();
        let (weighting, weights) = setup::weighting(
            problem,
            &domains[0],
            source.as_mut(),
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
                    raised,
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
            separate_psf: inputs.separate_psf,
            weighting,
            _weights: weights,
            teams,
            cancel,
            host,
            policy,
            cube,
            lifecycle,
            start_model: inputs.start_model,
            reseed,
            existing: inputs.existing,
            forms: inputs.forms,
            native_spacing_hz: native_spacing_hz(problem),
            backend,
            metal_stage_timing: inputs.diagnostics.metal_stage_timing,
            visibility_write: inputs.visibility_write,
            visibility_write_bytes,
            planes_per_wave: None,
            start_model_flux: Vec::new(),
            weight_image,
            retained: Reservation::none(),
            kept_weights: None,
            row_work: None,
        };
        run.refuse_a_waved_write()?;
        Ok(run)
    }

    /// The pass that writes visibilities must hold every plane: a run whose
    /// write would take waves is refused as it opens, rather than after the
    /// cycles that precede it.
    fn refuse_a_waved_write(&self) -> Result<(), ImagingError> {
        if self.visibility_write.is_none() {
            return Ok(());
        }
        let (modes, with_model) = write_pass(
            self.problem,
            self.forms,
            self.initial_modes(),
            (self.start_model.is_some(), self.reseed.is_some()),
        );
        // Without a minor cycle the first major cycle writes, and seeds
        // beside it.
        let seeding = if writes_initially(self.problem, self.forms) {
            self.seeding_bytes()
        } else {
            0
        };
        let plan = self.admit_pass(
            modes,
            with_model,
            self.visibility_write_bytes + seeding,
            true,
        )?;
        if plan.value.residency != Residency::All {
            return Err(ImagingError::Pass(PassError::VisibilityWriteWaves));
        }
        Ok(())
    }

    /// The major cycle after a minor cycle: the residual of the model updated
    /// by `terms`.
    fn refresh(
        &mut self,
        completion: MajorCycleCompletion,
        (terms, dense, results): (
            Vec<ModelDeltaTerm>,
            Vec<casa_imaging_deconvolution::DenseModelPlane>,
            Reservation,
        ),
        last: bool,
    ) -> Result<Major, ImagingError> {
        // The retained charge still covers the normal state and model the
        // pass refreshes; the pass adds its scratch and the new residual,
        // and the minor cycle's charge covers its terms until the model
        // holds them.
        let (normal_state, mut model) = completion.into_parts();
        let saved_seed = self.reseed.filter(|_| {
            normal_state.sample_count() == 0
                && !self.forms.residual
                && self
                    .existing
                    .as_ref()
                    .and_then(ExistingProducts::taylor_mask_read_bytes)
                    .is_some()
        });
        let seeding = saved_seed.map_or(0, |_| self.seeding_bytes());
        let Admitted {
            value: plan,
            reservation: mut pass,
        } = self.admit_pass(
            ModeSet::DATA,
            true,
            self.write_bytes(last) + seeding,
            self.writes(last),
        )?;
        if let Some(response) = saved_seed {
            let _seeding = pass.split(seeding);
            let start = self
                .start_model
                .clone()
                .expect("a restart model to convert");
            drop(model);
            model = self.physical_generation(&start, &normal_state, response)?;
        }
        let model = if dense.is_empty() {
            self.lifecycle.prepare_final_model(model, terms)?
        } else {
            assert!(
                terms.is_empty(),
                "a minor cycle uses one model update representation"
            );
            self.lifecycle.prepare_final_dense_model(model, dense)?
        };
        let cycle = MajorCycle::refresh(
            self.problem,
            normal_state,
            model,
            self.normal_storage(plan.residency)?,
        )?;
        let major = self.reconcile(cycle, ModeSet::DATA, true, last, plan)?;
        pass.join(results);
        self.retain(&major, pass)?;
        Ok(major)
    }

    /// What the pass holds for the visibility write: the write's bytes on
    /// the last pass, which writes.
    fn write_bytes(&self, last: bool) -> u64 {
        if self.writes(last) {
            self.visibility_write_bytes
        } else {
            0
        }
    }

    /// The separate parent prediction and corrected-data traversals run
    /// after the final tuned major cycle.
    fn writes(&self, last: bool) -> bool {
        last && self
            .visibility_write
            .as_ref()
            .is_some_and(|target| !target.predicts_final_model())
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

    /// Run `cycle`'s pass of `modes` and finish it. The initial pass grids
    /// the data and PSF, predicting the cycle's model when `with_model` (a
    /// start model's), or the PSF alone ([`continuation`]); every refresh
    /// grids the residual of the cycle's model.
    fn reconcile(
        &mut self,
        mut cycle: MajorCycle,
        modes: ModeSet,
        with_model: bool,
        last: bool,
        plan: PassPlan,
    ) -> Result<Major, ImagingError> {
        let residency = plan.residency;
        let initial = modes.psf;
        let transform = self.problem.visibility_transform();
        let writing = self.writes(last);
        let mut writer = self
            .visibility_write
            .as_ref()
            .filter(|_| writing)
            .map(|target| target.begin(transform))
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
            with_model.then_some(model),
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
    /// storage. A pass that `writes` visibilities keeps one set of chunks,
    /// since it does not pipeline its owners.
    fn admit_pass(
        &self,
        modes: ModeSet,
        with_model: bool,
        besides: u64,
        writes: bool,
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
            chunk_sets: 1,
        };
        let free = free_memory(&self.host, &self.policy).saturating_sub(besides);
        let residency = Residency::plan(&demand, free);
        let planes = match residency {
            Residency::All => demand.planes(),
            Residency::Waves { planes_per_wave } => planes_per_wave,
        };
        // Region owners pipeline over a second set of chunks, and share
        // larger slices more evenly; take what fits beside the waves just
        // planned (#736, #449).
        (demand.chunk_placements, demand.chunk_sets) = demand.plan_chunks(planes, free, writes);
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
                chunk_sets: demand.chunk_sets,
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
                row_work_pass_bytes(&domains, team.workers(), self.weight_image),
            )?;
            let started = Instant::now();
            let work = run_row_work_pass(
                &domains,
                &self.weighting,
                self.source.as_mut(),
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
        let bytes = row_work.bytes();
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
            chunk_sets,
        } = plan;
        if self.kept_weights.is_none() {
            self.kept_weights = self.keep_weights(residency);
        }
        if self.domains.len() > 1
            && self
                .problem
                .science()
                .measurement_equation()
                .w_projection()
                .is_some()
        {
            for domain in &self.domains {
                tracing::debug!(
                    target: "casa_imaging_application::w_kernel_in_pass",
                    reserved_bytes = domain.retained_kernel_bytes(),
                    "W-facet kernel retained during pass"
                );
            }
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
            chunk_sets,
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
            self.source.as_mut(),
            team,
            &self.cancel,
            &mut |chart, images| {
                let basis = self.domains[chart].operator.basis();
                let images = self
                    .stitch
                    .accept(chart, pass_images(chart, &images, initial, basis))
                    .map_err(|error| PassError::Images(error.into()))?;
                match images {
                    Some(mut images) => {
                        // Keep the main view's sensitivity and sums. Only the
                        // unnormalised PSF samples come from the separate view.
                        let mut psf_resident = Reservation::none();
                        if initial && let Some(separate) = self.separate_psf.take() {
                            images.psf = Some(separate.value);
                            psf_resident = separate.reservation;
                        }
                        // A pass of the PSF alone takes the residual it does
                        // not form ([`continuation`]).
                        if !modes.data {
                            continuation::set_residual(
                                self.existing.as_ref(),
                                &mut images,
                                basis.data_terms(),
                            )
                            .map_err(|error| PassError::Images(Box::new(error)))?;
                        }
                        let appended = state
                            .append(images)
                            .map_err(|error| PassError::Images(Box::new(error)));
                        drop(psf_resident);
                        appended
                    }
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
        .map(|source| source.selection().samples())
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
///
/// That weighed histogram is a transient of 8 bytes per grid row for each
/// region domain, dropped once the strips are cut. It goes uncharged, like
/// the prefix sums [`Partition::regions_balanced`] builds while cutting:
/// 32 KB at 4096².
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
    Ok(plans.next_cycle(
        &outcome.masks,
        &outcome.auto_masks,
        cycle,
        threshold_reached,
    )?)
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
/// model: the initial pass, of modes `initial`, without cleaning, with the
/// model when it was `seeded` from a start model; a residual refresh after
/// cleaning, after the initial pass of a run that `reseeds` ([`seeding`]),
/// or after a run's PSF or residual was read ([`continuation`]).
fn write_pass(
    problem: &CompiledProblem,
    forms: Forms,
    initial: ModeSet,
    (seeded, reseeds): (bool, bool),
) -> (ModeSet, bool) {
    if writes_initially(problem, forms) && !reseeds {
        (initial, seeded)
    } else {
        (ModeSet::DATA, true)
    }
}

/// Whether the first major cycle writes visibilities: a run without
/// cleaning that forms its PSF and residual in one pass, whose write is
/// beside a start model's seeding ([`write_pass`]).
fn writes_initially(problem: &CompiledProblem, forms: Forms) -> bool {
    problem.reconstruction().controls().max_minor_iterations() == 0 && forms.psf && forms.residual
}

/// The gridding-frame frequency envelope of a plane range of a channel-local
/// basis: from the low edge of its first channel to the high edge of its
/// last.
pub(crate) fn plane_bounds(problem: &CompiledProblem) -> Option<PlaneBounds> {
    if !matches!(
        problem.reconstruction().basis(),
        casa_imaging_model::ReconstructionBasis::ChannelLocal { .. }
    ) {
        return None;
    }
    let [reference_pixel, reference_frequency_hz, increment_hz] =
        problem.geometry().spectral().linear_grid()?;
    let centre = move |channel: u32| {
        reference_frequency_hz + (f64::from(channel) - reference_pixel) * increment_hz
    };
    Some(Box::new(move |planes: PlaneRange| {
        let half = increment_hz.abs() / 2.0;
        let [first, last] = [centre(planes.start), centre(planes.end - 1)];
        [first.min(last) - half, first.max(last) + half]
    }))
}

/// Bytes the run keeps from `outcome`: its primary mask's first plane, its
/// traced components and its planes' rows.
fn record_bytes(outcome: &MinorCycleOutcome) -> u64 {
    (std::mem::size_of_val(primary_plane(outcome))
        + std::mem::size_of_val(outcome.summary.trace.as_slice())
        + std::mem::size_of_val(outcome.summary.planes.as_slice())) as u64
}

/// The support of the primary mask's first plane (the first channel and
/// polarization of the main image).
fn primary_plane(outcome: &MinorCycleOutcome) -> &[bool] {
    outcome
        .masks
        .primary()
        .plane(0, 0)
        .expect("every mask has a first plane")
}

/// The record of one minor cycle; `global_threshold` is the run's
/// threshold or n-sigma threshold, whichever is larger. It keeps the
/// primary mask's first plane and its automatic evidence.
fn minor_cycle_record(
    cycle: usize,
    outcome: &MinorCycleOutcome,
    controls: &CycleControls,
    global_threshold: f64,
    (iterations_entering, actual_iterations_entering): (usize, usize),
    (total_iterations, total_actual_iterations): (usize, usize),
) -> NativeMinorCycleOutcome {
    let summary = &outcome.summary;
    NativeMinorCycleOutcome {
        cycle,
        iterations_entering,
        iterations: summary.iterations,
        total_iterations,
        actual_iterations_entering,
        actual_iterations: summary.components,
        total_actual_iterations,
        total_flux: summary.absolute_flux,
        model_flux: summary.model_flux,
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
        mask_support: primary_plane(outcome).to_vec(),
        auto_mask: outcome.auto_masks[0]
            .as_ref()
            .map(|evidence| *evidence.primary()),
    }
}
