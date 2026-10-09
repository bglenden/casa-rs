// SPDX-License-Identifier: LGPL-3.0-or-later
//! The loop of major and minor cycles on the major-cycle pass.
//!
//! The initial pass grids the data and the PSF (the residual of a start
//! model when there is one); each later pass grids the residual `V − A·m`
//! of the model the preceding minor cycle produced. Imaging weights are
//! generated once, before the initial pass, and reused by every pass. The
//! final pass also writes the model column when the run asks for it.

use std::path::Path;
use std::time::Instant;

use casa_imaging_model::{CompiledProblem, ModelInputCommitment, SpectralWcs, WeightingScheme};
use casa_imaging_operator::{BandwidthTaper, Basis, ModeSet, PlaneRange, WeightingGeneration};
use casa_imaging_products::VisibilityProductCompletion;
use casa_imaging_reconstruction::runtime_adapter::{
    NormalStoragePlan, ReconstructionPlaneWorkspace,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, ImageDomainReconstructionMaskPlans, MajorCycleCompletion,
    MajorCycleOwner, MajorCyclePreparation, MinorCycleImageResponse, MinorCycleProgram,
    ModelGeneration, ModelLifecycle, ModelStoragePlan, PassNormalState, ReconstructionMaskSet,
    WeightingGenerationId,
};
use casa_imaging_runtime::pass::{
    BackendChoice, Cancel, MajorCyclePass, ModelPreparation, Partition, PassDomain, PassError,
    PassSummary, Residency, VisibilitySink, WaveDemand, WorkerTeam, run_density_pass,
    run_major_cycle,
};
use casa_imaging_runtime::{
    AcceleratorKind, CubeState, MinorCycleOutcome, ResourceAuthority, ResourcePolicy,
    run_minor_cycle,
};
use casa_ms::ResolvedSelectedObservationAccess;

use super::ImagingError;
use super::images::{pass_images, prepare_model};
use super::measurement::{
    DomainOperator, density_shape, domain_operator, native_spacing_hz, selected_correlations,
};
use super::source::{MeasurementSetSource, PlaneBounds};
use super::visibility_write::{VisibilityWriteTarget, VisibilityWriter};
use crate::{AwCatalogDeployment, NativeMinorCycleOutcome};

/// What one native imaging run needs besides the compiled problem.
pub(crate) struct ImagingInputs<'a> {
    pub(crate) problem: &'a CompiledProblem,
    pub(crate) access: ResolvedSelectedObservationAccess,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    pub(crate) image_response: Option<MinorCycleImageResponse>,
    pub(crate) visibility_write: Option<VisibilityWriteTarget>,
    pub(crate) authority: &'a ResourceAuthority,
    pub(crate) policy: &'a ResourcePolicy,
    pub(crate) spill_directory: &'a Path,
    /// The AW catalog of an A-projection run.
    pub(crate) aw_catalog: Option<AwCatalogDeployment>,
    pub(crate) backend: BackendChoice,
}

/// The final reconciliation and the record of the cycles that led to it.
pub(crate) struct ImagingOutcome {
    pub(crate) scientific: MajorCycleCompletion,
    pub(crate) masks: Option<ReconstructionMaskSet>,
    pub(crate) minor_cycles: Vec<NativeMinorCycleOutcome>,
    pub(crate) major_cycle_count: usize,
    pub(crate) total_minor_iterations: usize,
    pub(crate) total_actual_minor_iterations: usize,
    pub(crate) visibility_products: Option<VisibilityProductCompletion>,
    pub(crate) workers: usize,
    pub(crate) planes_per_wave: Option<u32>,
}

/// Fixed parts of one run shared by every pass.
struct Run<'a> {
    problem: &'a CompiledProblem,
    domains: Vec<DomainOperator>,
    source: MeasurementSetSource<'a>,
    weighting: WeightingGeneration,
    weighting_id: WeightingGenerationId,
    team: WorkerTeam,
    cancel: Cancel,
    cube: Option<CubeState>,
    budget: u64,
    native_spacing_hz: f64,
    backend: BackendChoice,
    attempts: u64,
    visibility_write: Option<VisibilityWriteTarget>,
    /// Planes per wave of the most finely waved pass so far.
    planes_per_wave: Option<u32>,
    /// Whether the initial pass also grids the sensitivity image
    /// (`Mode::Weight`): a kernel set with weight taps on any domain.
    weight_image: bool,
}

/// One reconciled major cycle and the lifecycle that owns its model.
struct Major {
    lifecycle: ModelLifecycle,
    completion: MajorCycleCompletion,
    visibility: Option<VisibilityProductCompletion>,
}

/// Run every major and minor cycle of `inputs.problem`.
pub(crate) fn run(inputs: ImagingInputs<'_>) -> Result<ImagingOutcome, ImagingError> {
    let problem = inputs.problem;
    let controls = problem.reconstruction().controls();
    let cleaning = controls.max_minor_iterations() > 0;
    let image_response = inputs.image_response;
    let mut mask_plans = inputs.masks.clone();
    let mut run = Run::open(inputs)?;
    let mut major = run.initial(!cleaning)?;
    if !cleaning {
        return Ok(ImagingOutcome {
            scientific: major.completion,
            masks: None,
            minor_cycles: Vec::new(),
            major_cycle_count: 1,
            total_minor_iterations: 0,
            total_actual_minor_iterations: 0,
            visibility_products: major.visibility,
            workers: run.team.workers(),
            planes_per_wave: run.planes_per_wave,
        });
    }
    // CASA's `nmajor = -1` leaves the major-cycle count open, but every
    // productive cycle spends at least one of the minor-iteration budget.
    let maximum_cycles = controls
        .maximum_major_cycles()
        .unwrap_or(controls.max_minor_iterations());
    let clark_reuse = ReconstructionPlaneWorkspace::clark_reuse_bytes(problem);
    let mut minor_cycles = Vec::new();
    let mut totals = (0_usize, 0_usize);
    for cycle in 1.. {
        let remaining = (cycle > 1).then(|| controls.max_minor_iterations() - totals.0);
        let program = minor_program(problem, image_response, remaining)?
            .with_clark_workspace_reuse(clark_reuse);
        let started = Instant::now();
        let outcome = run_minor_cycle(
            major.completion,
            &major.lifecycle,
            &mask_plans,
            program,
            &run.team,
        )?;
        let entering = totals;
        totals = (
            totals
                .0
                .checked_add(outcome.evidence.controller_iterations())
                .ok_or(ImagingError::IterationOverflow)?,
            totals
                .1
                .checked_add(outcome.evidence.iterations())
                .ok_or(ImagingError::IterationOverflow)?,
        );
        let record = minor_cycle_record(cycle, &outcome, entering, totals);
        tracing::info!(
            "imaging minor cycle {cycle}: {} iterations ({} total), peak {:.6} -> {:.6} Jy, \
             threshold {:.6} Jy, stop {:?}, {:.2} s",
            record.iterations,
            totals.0,
            record.initial_peak_flux,
            record.final_peak_flux,
            record.effective_threshold,
            record.stop_reason,
            started.elapsed().as_secs_f64(),
        );
        minor_cycles.push(record);
        let continue_cleaning = cycle < maximum_cycles
            && totals.0 < controls.max_minor_iterations()
            && outcome.evidence.requests_reconciliation();
        let next_masks = next_masks(&mask_plans, &outcome, cycle)?;
        let masks = outcome.masks.clone();
        major = run.refresh(outcome, !continue_cleaning)?;
        if continue_cleaning {
            mask_plans = next_masks;
            continue;
        }
        return Ok(ImagingOutcome {
            scientific: major.completion,
            masks: Some(masks),
            minor_cycles,
            major_cycle_count: cycle + 1,
            total_minor_iterations: totals.0,
            total_actual_minor_iterations: totals.1,
            visibility_products: major.visibility,
            workers: run.team.workers(),
            planes_per_wave: run.planes_per_wave,
        });
    }
    unreachable!("the cycle counter is unbounded")
}

impl<'a> Run<'a> {
    /// The operators, worker team, source and imaging weights of a run.
    fn open(inputs: ImagingInputs<'a>) -> Result<Self, ImagingError> {
        let problem = inputs.problem;
        let backend = inputs.backend;
        if backend == BackendChoice::Metal
            && !inputs
                .authority
                .topology()
                .accelerators
                .iter()
                .any(|accelerator| accelerator.kind == AcceleratorKind::Metal)
        {
            return Err(ImagingError::Unsupported {
                reason: "the Metal backend needs a unified-memory Metal 3 device",
            });
        }
        let correlations = selected_correlations(problem)?;
        let selected = inputs
            .access
            .into_deferred()
            .open(problem)
            .map_err(|error| ImagingError::Observation(Box::new(error)))?;
        let dish_classes = selected.antenna_response_classes();
        let domains = problem
            .geometry()
            .domains()
            .iter()
            .map(|domain| {
                domain_operator(
                    problem,
                    domain,
                    &correlations,
                    backend,
                    inputs.aw_catalog.as_ref(),
                    &dish_classes,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        let weight_image = domains.iter().any(|domain| domain.weight_image);
        if inputs.visibility_write.is_some() && domains.len() > 1 {
            return Err(ImagingError::Unsupported {
                reason: "visibilities are written back for one image domain",
            });
        }
        let (workers, memory) = inputs.authority.phase_budget(inputs.policy)?;
        let team = WorkerTeam::new(workers)?;
        let cancel = Cancel::new();
        let main = &domains[0].operator;
        let mut source = MeasurementSetSource::new(
            problem,
            selected,
            main.basis().planes(),
            plane_bounds(problem),
            domains
                .iter()
                .any(|domain| domain.operator.cf().pointing_ramp()),
            dish_classes,
        );
        let started = Instant::now();
        let weighting = weighting(problem, &domains[0], &mut source, &team, &cancel)?;
        tracing::info!(
            "imaging weights: {workers} workers, {:.2} s",
            started.elapsed().as_secs_f64()
        );
        let cube = matches!(main.basis(), Basis::ChannelLocal { .. })
            .then(|| cube_state(problem, inputs.spill_directory, workers, memory))
            .transpose()?;
        let run = Self {
            problem,
            domains,
            source,
            weighting,
            weighting_id: WeightingGenerationId::next(),
            team,
            cancel,
            cube,
            budget: memory.saturating_sub(memory / 4),
            native_spacing_hz: native_spacing_hz(problem),
            backend,
            attempts: 0,
            visibility_write: inputs.visibility_write,
            planes_per_wave: None,
            weight_image,
        };
        if run.visibility_write.is_some() {
            // The pass that writes is the initial one without cleaning and a
            // residual refresh otherwise; it must hold every plane, and is
            // refused now rather than after the cycles that precede it.
            let cleaning = problem.reconstruction().controls().max_minor_iterations() > 0;
            let (modes, with_model) = if cleaning {
                (ModeSet::DATA, true)
            } else {
                (run.initial_modes(), start_model(problem))
            };
            if run.residency(modes, with_model)? != Residency::All {
                return Err(ImagingError::Pass(PassError::VisibilityWriteWaves));
            }
        }
        Ok(run)
    }

    /// The initial major cycle: data and PSF, from the start model when the
    /// problem has one; it writes visibilities when it is also final.
    fn initial(&mut self, last: bool) -> Result<Major, ImagingError> {
        let mut lifecycle = ModelLifecycle::bind(
            ExecutableModelProblem::from_compiled(self.problem.clone())?,
            self.next_attempt(),
            1,
            self.model_storage()?,
        )?;
        let start_model = !matches!(lifecycle.contract().input(), ModelInputCommitment::Empty);
        let named = if start_model {
            lifecycle.initial_reprojected()?
        } else {
            lifecycle.initial_empty()?
        };
        let preparation = MajorCyclePreparation::prepare(&lifecycle, named, None)?;
        let residency = self.residency(self.initial_modes(), start_model)?;
        let state = PassNormalState::initial(
            self.problem,
            self.weighting_id,
            preparation.final_model_generation(),
            self.normal_storage(residency)?,
        )?;
        self.reconcile(lifecycle, state, preparation, None, last, residency)
    }

    /// The major cycle after a minor cycle: the residual of the updated model.
    fn refresh(&mut self, outcome: MinorCycleOutcome, last: bool) -> Result<Major, ImagingError> {
        let MinorCycleOutcome {
            normal_state,
            continuation,
            masks,
            delta,
            ..
        } = outcome;
        let terms = delta.map(|delta| delta.terms().to_vec());
        let attempt = self.next_attempt();
        let (lifecycle, named) = ModelLifecycle::continue_from(
            ExecutableModelProblem::from_compiled(self.problem.clone())?,
            attempt,
            1,
            continuation,
            self.model_storage()?,
        )?;
        let delta = terms
            .filter(|terms| !terms.is_empty())
            .map(|terms| lifecycle.compile_delta(&named, terms))
            .transpose()?;
        let preparation = MajorCyclePreparation::prepare(&lifecycle, named, delta)?;
        let residency = self.residency(ModeSet::DATA, true)?;
        let state = PassNormalState::refresh(
            self.problem,
            normal_state,
            preparation.final_model_generation(),
            self.normal_storage(residency)?,
        )?;
        self.reconcile(lifecycle, state, preparation, Some(&masks), last, residency)
    }

    /// Run one pass into `state` and reconcile it with the prepared model.
    /// The initial pass grids the start model's residual only when there is
    /// a start model; every refresh grids the residual.
    fn reconcile(
        &mut self,
        mut lifecycle: ModelLifecycle,
        mut state: PassNormalState,
        preparation: MajorCyclePreparation,
        masks: Option<&ReconstructionMaskSet>,
        last: bool,
        residency: Residency,
    ) -> Result<Major, ImagingError> {
        let initial = masks.is_none();
        let modes = if initial {
            self.initial_modes()
        } else {
            ModeSet::DATA
        };
        let with_model = !initial || start_model(self.problem);
        let model = with_model.then(|| preparation.final_model());
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
        let summary = self.pass(
            modes,
            model,
            residency,
            &mut state,
            initial,
            writer.as_mut(),
        )?;
        let pass_seconds = started.elapsed().as_secs_f64();
        let final_model = preparation.final_model_generation();
        let visibility = writer
            .map(|writer| writer.complete(final_model))
            .transpose()
            .map_err(|error| ImagingError::Pass(PassError::VisibilityWrite(error)))?
            .map(|samples| {
                VisibilityProductCompletion::new(
                    self.problem.problem_id(),
                    final_model,
                    self.weighting_id,
                    samples,
                )
            });
        let normal = state.finish(summary.samples, summary.blocks)?;
        let mut owner = MajorCycleOwner::from_complete_data(normal, preparation)?;
        if let Some(masks) = masks {
            owner = owner.bind_reconstruction_masks(masks)?;
        }
        let completion = owner.reconcile(&mut lifecycle)?;
        tracing::info!(
            "imaging major cycle: {} samples, pass {pass_seconds:.2} s, total {:.2} s",
            summary.samples,
            started.elapsed().as_secs_f64(),
        );
        Ok(Major {
            lifecycle,
            completion,
            visibility,
        })
    }

    fn next_attempt(&mut self) -> casa_imaging_model::ModelExecutionAttemptId {
        self.attempts += 1;
        let mut identity = [0_u8; 32];
        identity[0] = 1;
        identity[24..].copy_from_slice(&self.attempts.to_be_bytes());
        casa_imaging_model::ModelExecutionAttemptId::new(
            casa_imaging_model::LogicalIdentity::from_sha256(identity),
        )
    }

    fn model_storage(&self) -> Result<ModelStoragePlan, ImagingError> {
        Ok(match &self.cube {
            Some(cube) => cube.model_storage()?,
            None => ModelStoragePlan::resident(usize::MAX)?,
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

    /// The modes of the initial pass: data and PSF, plus the sensitivity
    /// image when a kernel set has weight taps (mosaic, AW).
    const fn initial_modes(&self) -> ModeSet {
        ModeSet {
            data: true,
            psf: true,
            weight: self.weight_image,
        }
    }

    /// The waves of a pass accumulating `modes`, with a model when
    /// `with_model`, that fit the budget: planned once per major cycle for
    /// both the pass and its normal storage.
    fn residency(&self, modes: ModeSet, with_model: bool) -> Result<Residency, ImagingError> {
        let domains = pass_domains(&self.domains, self.team.workers());
        Ok(Residency::plan(
            &WaveDemand {
                domains: &domains,
                modes,
                with_model,
                native_spacing_hz: self.native_spacing_hz,
                workers: self.team.workers(),
                backend: self.backend,
            },
            self.budget,
        )?)
    }

    /// One pass over every image domain, appending its images to `state`.
    fn pass(
        &mut self,
        modes: ModeSet,
        model: Option<&ModelGeneration>,
        residency: Residency,
        state: &mut PassNormalState,
        initial: bool,
        mut writer: Option<&mut VisibilityWriter<'_>>,
    ) -> Result<PassSummary, ImagingError> {
        let domains = pass_domains(&self.domains, self.team.workers());
        let prepare = |domain: usize, planes: PlaneRange| {
            let generation = model.expect("the closure is installed only with a model");
            prepare_model(&self.domains[domain].operator, generation, domain, planes)
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
            &self.team,
            &self.cancel,
            &mut |domain, images| {
                let basis = self.domains[domain].operator.basis();
                state
                    .append(pass_images(domain, &images, initial, basis))
                    .map_err(|error| PassError::Images(Box::new(error)))
            },
            sink.as_mut(),
        )?)
    }
}

/// Every image domain's operator, resampler and partition: planes split
/// among `workers` for a channel-local basis, grid strips otherwise.
fn pass_domains(domains: &[DomainOperator], workers: usize) -> Vec<PassDomain<'_>> {
    domains
        .iter()
        .map(|domain| PassDomain {
            operator: &domain.operator,
            resampler: &domain.resampler,
            partition: match domain.operator.basis() {
                Basis::ChannelLocal { .. } => Partition::Planes { owners: workers },
                Basis::Constant | Basis::Taylor { .. } => {
                    Partition::regions(&domain.operator, workers)
                }
            },
        })
        .collect()
}

/// Whether `problem` starts from a model (CASA `startmodel`).
fn start_model(problem: &CompiledProblem) -> bool {
    !matches!(
        problem.model_lifecycle().input(),
        ModelInputCommitment::Empty
    )
}

/// The next cycle's mask plans: automask evolves from the masks just used.
fn next_masks(
    plans: &ImageDomainReconstructionMaskPlans,
    outcome: &MinorCycleOutcome,
    cycle: usize,
) -> Result<ImageDomainReconstructionMaskPlans, ImagingError> {
    let ReconstructionMaskSet::Domains(applied) = &outcome.masks else {
        unreachable!("the minor cycle masks every image domain")
    };
    let stopped = (0..applied.len())
        .map(|domain| outcome.auto_masks[domain].is_some_and(|evidence| evidence.channel_stopped))
        .collect::<Vec<_>>();
    Ok(plans.next_cycle(
        applied,
        cycle,
        outcome.evidence.cycle_threshold_is_global(),
        &stopped,
    )?)
}

/// CASA's imaging weights for the run: the input weights (natural) or the
/// density weights of one traversal on the main domain's cells, which every
/// domain shares (`SynthesisImagerVi2::weight`).
fn weighting(
    problem: &CompiledProblem,
    main: &DomainOperator,
    source: &mut MeasurementSetSource<'_>,
    team: &WorkerTeam,
    cancel: &Cancel,
) -> Result<WeightingGeneration, ImagingError> {
    let contract = problem.weighting();
    let taper = contract.uv_taper();
    let (robust, bandwidth) = match contract.scheme() {
        WeightingScheme::Natural => return Ok(WeightingGeneration::Natural { taper }),
        WeightingScheme::Uniform => (None, None),
        WeightingScheme::Briggs { robust } => (Some(robust), None),
        WeightingScheme::BriggsBandwidthTaper { robust } => {
            let spectral = problem.geometry().spectral();
            let last = spectral.output_channels().saturating_sub(1);
            let (Some(first_hz), Some(last_hz)) = (
                spectral.channel_centre_hz(0),
                spectral.channel_centre_hz(last),
            ) else {
                return Err(ImagingError::Unsupported {
                    reason: "the image spectral axis has no channels",
                });
            };
            (
                Some(robust),
                Some(BandwidthTaper::from_frequency_range(first_hz, last_hz)?),
            )
        }
    };
    let shape = density_shape(problem, &problem.geometry().domains()[0])?;
    let grid = run_density_pass(&main.operator, &main.resampler, shape, source, team, cancel)?;
    Ok(WeightingGeneration::density(
        grid, robust, bandwidth, taper,
    )?)
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

/// The paged cube state of a channel-local run, with a cache as large as
/// the whole cube when a quarter of the memory budget allows.
fn cube_state(
    problem: &CompiledProblem,
    directory: &Path,
    workers: usize,
    memory: u64,
) -> Result<CubeState, ImagingError> {
    let target = problem.model_lifecycle().target();
    let domains = target
        .domains()
        .iter()
        .map(|domain| domain.pixels())
        .collect::<Vec<_>>();
    let planes = target.coefficients() * target.polarizations();
    let (minimum, full) = CubeState::cache_limits(directory, &domains, planes, workers)?;
    let cache = full.min(minimum.max(usize::try_from(memory / 4).unwrap_or(usize::MAX)));
    Ok(CubeState::new(directory, &domains, planes, cache)?)
}

fn minor_program(
    problem: &CompiledProblem,
    response: Option<MinorCycleImageResponse>,
    remaining: Option<usize>,
) -> Result<MinorCycleProgram, ImagingError> {
    let mut program = MinorCycleProgram::for_problem(problem)?.record_component_sequence(64)?;
    if let Some(remaining) = remaining {
        program = program.limit_iterations(remaining)?;
    }
    if let Some(response) = response {
        program = program.with_image_response(response);
    }
    Ok(program)
}

fn minor_cycle_record(
    cycle: usize,
    outcome: &MinorCycleOutcome,
    (iterations_entering, actual_iterations_entering): (usize, usize),
    (total_iterations, total_actual_iterations): (usize, usize),
) -> NativeMinorCycleOutcome {
    let evidence = &outcome.evidence;
    let mask = outcome.masks.primary();
    NativeMinorCycleOutcome {
        cycle,
        iterations_entering,
        iterations: evidence.controller_iterations(),
        total_iterations,
        actual_iterations_entering,
        actual_iterations: evidence.iterations(),
        total_actual_iterations,
        total_flux: evidence.total_flux(),
        initial_peak_flux: evidence.initial_peak_flux(),
        final_peak_flux: evidence.final_peak_flux(),
        noise_rms: evidence.noise_rms(),
        effective_threshold: evidence.effective_threshold(),
        global_threshold: evidence.global_threshold(),
        cycle_threshold: evidence.cycle_threshold(),
        stop_reason: evidence.stop_reason().into(),
        clark_refreshes: evidence.clark_refreshes(),
        associated_replay_ordinal: cycle,
        recorded_components: evidence.recorded_components().copied().collect(),
        mask_support: mask.support().to_vec(),
        mask_generation: mask.generation_id(),
        mask_model_generation: mask.model_generation(),
        mask_normal_state: mask.normal_state_completion(),
        auto_mask: outcome.auto_masks[0],
    }
}
