// SPDX-License-Identifier: LGPL-3.0-or-later
//! The loop of major and minor cycles on the major-cycle pass.
//!
//! The initial pass grids the data and the PSF (the residual of a start
//! model when there is one); each later pass grids the residual `V − A·m`
//! of the model the preceding minor cycle produced. Imaging weights are
//! generated once, before the initial pass, and reused by every pass.

use std::path::Path;

use casa_imaging_model::{CompiledProblem, ModelInputCommitment, SpectralWcs, WeightingScheme};
use casa_imaging_operator::{
    BandwidthTaper, Basis, ModeSet, PlaneRange, SPHEROIDAL_SUPPORT, WeightingGeneration,
};
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
    Cancel, MajorCyclePass, ModelPreparation, Partition, PassError, PassSummary, Residency,
    WorkerTeam, run_density_pass, run_major_cycle,
};
use casa_imaging_runtime::{
    CubeState, MinorCycleOutcome, ResourceAuthority, ResourcePolicy, run_minor_cycle,
};
use casa_ms::ResolvedSelectedObservationAccess;

use super::ImagingError;
use super::images::{pass_images, prepare_model};
use super::measurement::{DomainOperator, density_shape, domain_operator, selected_correlations};
use super::source::{MeasurementSetSource, PlaneBounds};
use crate::NativeMinorCycleOutcome;

/// What one native imaging run needs besides the compiled problem.
pub(crate) struct ImagingInputs<'a> {
    pub(crate) problem: &'a CompiledProblem,
    pub(crate) access: ResolvedSelectedObservationAccess,
    pub(crate) masks: ImageDomainReconstructionMaskPlans,
    pub(crate) image_response: Option<MinorCycleImageResponse>,
    pub(crate) authority: &'a ResourceAuthority,
    pub(crate) policy: &'a ResourcePolicy,
    pub(crate) spill_directory: &'a Path,
}

/// The final reconciliation and the record of the cycles that led to it.
pub(crate) struct ImagingOutcome {
    pub(crate) scientific: MajorCycleCompletion,
    pub(crate) masks: Option<ReconstructionMaskSet>,
    pub(crate) minor_cycles: Vec<NativeMinorCycleOutcome>,
    pub(crate) major_cycle_count: usize,
    pub(crate) total_minor_iterations: usize,
    pub(crate) total_actual_minor_iterations: usize,
    pub(crate) workers: usize,
}

/// Fixed parts of one run shared by every pass.
struct Run<'a> {
    problem: &'a CompiledProblem,
    domains: Vec<DomainOperator>,
    weighting: WeightingGeneration,
    weighting_id: WeightingGenerationId,
    team: WorkerTeam,
    cancel: Cancel,
    cube: Option<CubeState>,
    budget: u64,
    attempts: u64,
}

/// Run every major and minor cycle of `inputs.problem`.
pub(crate) fn run(inputs: ImagingInputs<'_>) -> Result<ImagingOutcome, ImagingError> {
    let problem = inputs.problem;
    let correlations = selected_correlations(problem)?;
    let domains = problem
        .geometry()
        .domains()
        .iter()
        .map(|domain| domain_operator(problem, domain, &correlations))
        .collect::<Result<Vec<_>, _>>()?;
    let (workers, memory) = inputs.authority.phase_budget(inputs.policy)?;
    let team = WorkerTeam::new(workers)?;
    let cancel = Cancel::new();
    let selected = inputs
        .access
        .into_deferred()
        .open(problem)
        .map_err(|error| ImagingError::Observation(Box::new(error)))?;
    let main = &domains[0].operator;
    let planes = main.basis().planes();
    let mut source = MeasurementSetSource::new(problem, selected, planes, plane_bounds(problem));
    let weighting = weighting(problem, &domains[0], &mut source, &team, &cancel)?;
    let cube = matches!(main.basis(), Basis::ChannelLocal { .. })
        .then(|| cube_state(problem, inputs.spill_directory, workers, memory))
        .transpose()?;
    let budget = memory.saturating_sub(memory / 4);
    let mut run = Run {
        problem,
        domains,
        weighting,
        weighting_id: WeightingGenerationId::next(),
        team,
        cancel,
        cube,
        budget,
        attempts: 0,
    };

    let mut lifecycle = run.lifecycle()?;
    let start_model = !matches!(lifecycle.contract().input(), ModelInputCommitment::Empty);
    let named = if start_model {
        lifecycle.initial_reprojected()?
    } else {
        lifecycle.initial_empty()?
    };
    let preparation = MajorCyclePreparation::prepare(&lifecycle, named, None)?;
    let mut state = PassNormalState::initial(
        problem,
        preparation.final_model_generation(),
        run.normal_storage(ModeSet::DATA_PSF, start_model)?,
    )?;
    let model = start_model.then(|| preparation.final_model());
    let summary = run.pass(&mut source, ModeSet::DATA_PSF, model, &mut state, true)?;
    let normal = state.finish(problem, run.weighting_id, summary.samples, summary.blocks)?;
    let mut completion =
        MajorCycleOwner::from_complete_data(normal, preparation)?.reconcile(&mut lifecycle)?;

    let controls = problem.reconstruction().controls();
    if controls.max_minor_iterations() == 0 {
        return Ok(ImagingOutcome {
            scientific: completion,
            masks: None,
            minor_cycles: Vec::new(),
            major_cycle_count: 1,
            total_minor_iterations: 0,
            total_actual_minor_iterations: 0,
            workers,
        });
    }
    // CASA's `nmajor = -1` leaves the major-cycle count open, but every
    // productive cycle spends at least one of the minor-iteration budget.
    let maximum_cycles = controls
        .maximum_major_cycles()
        .unwrap_or(controls.max_minor_iterations());
    let clark_reuse = ReconstructionPlaneWorkspace::clark_reuse_bytes(problem);
    let mut mask_plans = inputs.masks;
    let mut cycle = 1_usize;
    let mut total_iterations = 0_usize;
    let mut total_actual = 0_usize;
    let mut minor_cycles = Vec::new();
    loop {
        let remaining = (cycle > 1).then(|| controls.max_minor_iterations() - total_iterations);
        let program = minor_program(problem, inputs.image_response, remaining)?
            .with_clark_workspace_reuse(clark_reuse);
        let outcome = run_minor_cycle(completion, &lifecycle, &mask_plans, program, &run.team)?;
        let entering = (total_iterations, total_actual);
        total_iterations = total_iterations
            .checked_add(outcome.evidence.controller_iterations())
            .ok_or(ImagingError::IterationOverflow)?;
        total_actual = total_actual
            .checked_add(outcome.evidence.iterations())
            .ok_or(ImagingError::IterationOverflow)?;
        let record =
            minor_cycle_record(cycle, &outcome, entering, (total_iterations, total_actual));
        tracing::info!(
            cycle,
            iterations = record.iterations,
            total_iterations,
            initial_peak = record.initial_peak_flux,
            final_peak = record.final_peak_flux,
            threshold = record.effective_threshold,
            stop = ?record.stop_reason,
            "imaging minor cycle"
        );
        minor_cycles.push(record);
        let continue_cleaning = cycle < maximum_cycles
            && total_iterations < controls.max_minor_iterations()
            && outcome.evidence.requests_reconciliation();
        let ReconstructionMaskSet::Domains(applied) = &outcome.masks else {
            unreachable!("the minor cycle masks every image domain")
        };
        let stopped = (0..applied.len())
            .map(|domain| {
                outcome.auto_masks[domain].is_some_and(|evidence| evidence.channel_stopped)
            })
            .collect::<Vec<_>>();
        let next_masks = mask_plans.next_cycle(
            applied,
            cycle,
            outcome.evidence.cycle_threshold_is_global(),
            &stopped,
        )?;
        let MinorCycleOutcome {
            normal_state,
            continuation,
            masks,
            delta,
            ..
        } = outcome;
        let terms = delta.map(|delta| delta.terms().to_vec());
        let (next_lifecycle, named) = ModelLifecycle::continue_from(
            ExecutableModelProblem::from_compiled(problem.clone())?,
            run.next_attempt(),
            1,
            continuation,
            run.model_storage()?,
        )?;
        lifecycle = next_lifecycle;
        let delta = terms
            .filter(|terms| !terms.is_empty())
            .map(|terms| lifecycle.compile_delta(&named, terms))
            .transpose()?;
        let preparation = MajorCyclePreparation::prepare(&lifecycle, named, delta)?;
        let mut state = PassNormalState::refresh(
            problem,
            normal_state,
            preparation.final_model_generation(),
            run.normal_storage(ModeSet::DATA, true)?,
        )?;
        let summary = run.pass(
            &mut source,
            ModeSet::DATA,
            Some(preparation.final_model()),
            &mut state,
            false,
        )?;
        let normal = state.finish(problem, run.weighting_id, summary.samples, summary.blocks)?;
        completion = MajorCycleOwner::from_complete_data(normal, preparation)?
            .bind_reconstruction_masks(&masks)?
            .reconcile(&mut lifecycle)?;
        if continue_cleaning {
            mask_plans = next_masks;
            cycle += 1;
            continue;
        }
        return Ok(ImagingOutcome {
            scientific: completion,
            masks: Some(masks),
            minor_cycles,
            major_cycle_count: cycle + 1,
            total_minor_iterations: total_iterations,
            total_actual_minor_iterations: total_actual,
            workers,
        });
    }
}

impl Run<'_> {
    fn next_attempt(&mut self) -> casa_imaging_model::ModelExecutionAttemptId {
        self.attempts += 1;
        let mut identity = [0_u8; 32];
        identity[0] = 1;
        identity[24..].copy_from_slice(&self.attempts.to_be_bytes());
        casa_imaging_model::ModelExecutionAttemptId::new(
            casa_imaging_model::LogicalIdentity::from_sha256(identity),
        )
    }

    fn lifecycle(&mut self) -> Result<ModelLifecycle, ImagingError> {
        let attempt = self.next_attempt();
        Ok(ModelLifecycle::bind(
            ExecutableModelProblem::from_compiled(self.problem.clone())?,
            attempt,
            1,
            self.model_storage()?,
        )?)
    }

    fn model_storage(&self) -> Result<ModelStoragePlan, ImagingError> {
        Ok(match &self.cube {
            Some(cube) => cube.model_storage()?,
            None => ModelStoragePlan::resident(usize::MAX)?,
        })
    }

    fn normal_storage(
        &self,
        modes: ModeSet,
        with_model: bool,
    ) -> Result<NormalStoragePlan, ImagingError> {
        let planes = self.domains[0].operator.basis().planes() as usize;
        Ok(match &self.cube {
            Some(cube) => {
                let window = match self.residency(0, modes, with_model)? {
                    Residency::All => planes,
                    Residency::Waves { planes_per_wave } => planes_per_wave as usize,
                };
                cube.normal_storage(window)?
            }
            None => NormalStoragePlan::resident(planes)?,
        })
    }

    fn residency(
        &self,
        domain: usize,
        modes: ModeSet,
        with_model: bool,
    ) -> Result<Residency, ImagingError> {
        Ok(Residency::plan(
            &self.domains[domain].operator,
            modes,
            with_model,
            self.team.workers(),
            self.budget,
        )?)
    }

    fn partition(&self, domain: usize) -> Partition {
        let operator = &self.domains[domain].operator;
        match operator.basis() {
            Basis::ChannelLocal { .. } => Partition::Planes {
                owners: self.team.workers(),
            },
            Basis::Constant | Basis::Taylor { .. } => Partition::regions(
                operator.geometry(),
                self.team.workers(),
                usize::from(SPHEROIDAL_SUPPORT / 2),
            ),
        }
    }

    /// One pass over every image domain, appending its images to `state`.
    fn pass(
        &self,
        source: &mut MeasurementSetSource<'_>,
        modes: ModeSet,
        model: Option<&ModelGeneration>,
        state: &mut PassNormalState,
        initial: bool,
    ) -> Result<PassSummary, ImagingError> {
        let mut total = PassSummary::default();
        for (index, domain) in self.domains.iter().enumerate() {
            source.set_domain(index as u32);
            let prepare = |planes: PlaneRange| {
                let generation = model.expect("the closure is installed only with a model");
                prepare_model(&domain.operator, generation, index, planes)
                    .map_err(|error| PassError::Model(Box::new(error)))
            };
            let pass = MajorCyclePass {
                operator: &domain.operator,
                resampler: &domain.resampler,
                weighting: &self.weighting,
                modes,
                model: model.map(|_| &prepare as &ModelPreparation<'_>),
                partition: self.partition(index),
                residency: self.residency(index, modes, model.is_some())?,
            };
            let summary =
                run_major_cycle(&pass, source, &self.team, &self.cancel, &mut |images| {
                    state
                        .append(pass_images(index, &images, initial))
                        .map_err(|error| PassError::Images(Box::new(error)))
                })?;
            if index == 0 {
                total = summary;
            }
        }
        Ok(total)
    }
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
    let shape = density_shape(problem, &problem.geometry().domains()[0]);
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
    let [width, height] = target.domains()[0].pixels();
    let planes = target.sample_count() / (width * height);
    let (minimum, full) = CubeState::cache_limits(directory, [width, height], planes, workers)?;
    let cache = full.min(minimum.max(usize::try_from(memory / 4).unwrap_or(usize::MAX)));
    Ok(CubeState::new(directory, [width, height], planes, cache)?)
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
