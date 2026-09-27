// SPDX-License-Identifier: LGPL-3.0-or-later

//! Initial cube phase on the existing source, resource and reconciliation DAG.
//! Native storage and band ownership replace the historical numerical executor.

use super::{
    execute::{self, WavePlan},
    plan::NativePhasePlan,
    prepare::{NativePreparation, PreparedNative},
};
use crate::{
    AttemptBoundObservationCompletion, FenceKind, FinalMajorPhaseInput, ImplementationRegistry,
    IoMeasurement, MajorCycleOperatorResult, MajorCycleOperatorState, ManagedSpillStorage,
    ObservationReadCompletionContext, PhysicalWorkBinding, ReconstructionCyclePhaseCompletion,
    ResourceMeasurement, RetainedArtifactPermit, SelectedObservationSourceResources,
    SpectralCycleExecutionPolicy, SpectralPassIdentity, SpectralPassPhase, WeightingExecutionState,
    WeightingPlanFragment, WeightingReplayCompletion, WeightingStreamingMode, WorkExecutionContext,
    WorkImplementation, WorkImplementationId, WorkKind, WorkMeasurements, WorkNodeId,
    complete_data_operator::{PendingCubeRefresh, PendingStreamingCubeFold},
    cube_state_plan::CubeStatePlan,
};
use casa_imaging_model::{
    CompiledProblem, CorrelationType, LogicalIdentity, ModelExecutionAttemptId,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FinalNormalState, ImageDomainReconstructionMaskPlans,
    MajorCyclePreparation, MinorCycleProgram, ModelLifecycle, MuellerMatrix, PolarizationOperator,
    ReconstructionMaskSet, SpectralOperatorSpecification, WeightingPlan, plan_weighting,
    runtime_adapter::{BandPlan, BandResult, ReconstructionPlaneWorkspace, SpectralOperatorPass},
};
use casa_ms::DeferredSelectedObservationAccess;
use std::{
    io,
    sync::{Arc, Mutex},
};

struct State {
    selected: Option<DeferredSelectedObservationAccess>,
    weighting: WeightingExecutionState,
    native: Option<PreparedNative>,
    bands: Vec<BandPlan>,
    lifecycle: Option<ModelLifecycle>,
    model: Option<MajorCyclePreparation>,
    complete: Option<MajorCycleOperatorResult>,
    pass_input: Option<FinalMajorPhaseInput>,
    prior: Option<FinalNormalState>,
    masks: Option<ReconstructionMaskSet>,
    replay: Option<WeightingReplayCompletion>,
    minor: Option<ReconstructionCyclePhaseCompletion>,
    #[cfg(test)]
    fenced: bool,
    #[cfg(test)]
    published: bool,
}

/// Native cube executor; application CLEAN and publication stay shared.
pub struct InitialCube {
    problem: CompiledProblem,
    implementation: WorkImplementationId,
    weighting: WeightingPlan,
    preparation: Option<crate::weighting::NativePreparationPlan>,
    source: SelectedObservationSourceResources,
    pass: SpectralPassIdentity,
    read: WorkNodeId,
    prepare: WorkNodeId,
    reconcile: WorkNodeId,
    output_hz: Vec<f64>,
    storage: ManagedSpillStorage,
    native_plan: NativePhasePlan,
    cube_state: CubeStatePlan,
    minor: Option<(
        WorkNodeId,
        ImageDomainReconstructionMaskPlans,
        MinorCycleProgram,
    )>,
    imported: bool,
    phase_input_artifact: Option<(crate::ArtifactIdentity, u64)>,
    #[cfg(test)]
    failure: Failure,
    state: Mutex<State>,
}

/// Owned native samples and their original source completion. No new source
/// traversal or content verification is invented when this moves between phases.
pub struct NativeReplay {
    native: PreparedNative,
    replay: WeightingReplayCompletion,
    managed: Arc<crate::cube_state_plan::ManagedCubeRun>,
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Failure {
    None,
    SourceFence,
    ReconciliationNode,
}

impl InitialCube {
    /// Identify the migrated capability from compiled science and source shape.
    /// Other scientific modes retain their own execution owners; a failed native
    /// phase never retries through another implementation.
    pub fn supports(problem: &CompiledProblem) -> io::Result<bool> {
        if problem.weighting().scheme() != casa_imaging_model::WeightingScheme::Natural
            || !matches!(
                problem.model_lifecycle().input(),
                casa_imaging_model::ModelInputCommitment::Empty
            )
            || problem.visibility_transform().is_some()
            || !matches!(
                problem.reconstruction().basis(),
                casa_imaging_model::ReconstructionBasis::ChannelLocal { .. }
            )
        {
            return Ok(false);
        }
        let [source] = problem.selected_observation().read_set().sources() else {
            return Ok(false);
        };
        let ([dd], [spw], [pol]) = (
            source.selection().data_descriptions(),
            source.selection().spectral_windows(),
            source.selection().correlations(),
        ) else {
            return Ok(false);
        };
        if dd.spectral_window_id() != spw.spectral_window_id()
            || dd.polarization_id() != pol.polarization_id()
            || spw.channel_indices().len() < 2
        {
            return Ok(false);
        }
        let specification =
            SpectralOperatorSpecification::for_slab(problem, 0, 1).map_err(io::Error::other)?;
        Ok(BandPlan::supports(&specification))
    }

    /// Compose the initial source, paged state and native wave reservations.
    /// `enclosing_owner_bytes` counts live application owners not charged by
    /// these components; it must be supplied by complete application composition.
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn plan(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        selected: DeferredSelectedObservationAccess,
        workers: usize,
        source_slots: usize,
        enclosing_owner_bytes: u64,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
        Self::build(
            problem,
            registry,
            policy,
            storage,
            Some(selected),
            None,
            None,
            0,
            workers,
            source_slots,
            enclosing_owner_bytes,
            None,
        )
    }

    /// Prepare selected native data and execute the first cube major phase.
    /// Worker admission uses the normal host policy, bounded by useful planes.
    pub fn initial(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        selected: DeferredSelectedObservationAccess,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
        let workers = Self::workers(&problem, &policy)?;
        Self::build(
            problem,
            registry,
            policy,
            storage,
            Some(selected),
            None,
            None,
            0,
            workers,
            1,
            0,
            minor,
        )
    }

    /// Refresh the residual from the owned native store and immutable model epoch.
    #[allow(clippy::too_many_arguments)]
    pub fn refresh(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        retained: NativeReplay,
        input: FinalMajorPhaseInput,
        ordinal: u32,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
        let workers = Self::workers(&problem, &policy)?;
        let prior = input.evidence().normal_state();
        if ordinal == 0
            || retained.replay.problem_id() != problem.problem_id()
            || prior.problem_id() != problem.problem_id()
            || prior.weighting_generation() != retained.replay.weighting_generation()
            || prior.selected_generation() != retained.replay.selected_generation()
            || prior.continuum_transform_generation()
                != retained
                    .replay
                    .continuum_transform()
                    .map(|v| v.generation_id())
        {
            return Err(io::Error::other(
                "native refresh changed source, weighting or problem association",
            ));
        }
        Self::build(
            problem,
            registry,
            policy,
            storage,
            None,
            Some(retained),
            Some(input),
            ordinal,
            workers,
            1,
            0,
            minor,
        )
    }

    fn workers(
        problem: &CompiledProblem,
        policy: &SpectralCycleExecutionPolicy,
    ) -> io::Result<usize> {
        let workers = policy
            .authority
            .planning_worker_capacity(&policy.resource_policy)
            .map_err(io::Error::other)?
            .min(problem.geometry().spectral().output_channels() as u64);
        if workers == 0 {
            return Err(io::Error::other(
                "native cube has no admitted worker capacity",
            ));
        }
        usize::try_from(workers).map_err(io::Error::other)
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        selected: Option<DeferredSelectedObservationAccess>,
        retained: Option<NativeReplay>,
        pass_input: Option<FinalMajorPhaseInput>,
        ordinal: u32,
        workers: usize,
        source_slots: usize,
        enclosing_owner_bytes: u64,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
        if policy.visibility_write.is_some()
            || policy.aw_projection.is_some()
            || policy.aw_reader.is_some()
        {
            return Err(io::Error::other(
                "native cube comparison does not support AW or visibility writes",
            ));
        }
        let imported = retained.is_some();
        let phase_input_artifact = pass_input.as_ref().map(|input| {
            (
                input.identity(),
                (input.pending_delta_terms() * size_of::<casa_imaging_model::ModelDeltaTerm>())
                    as u64,
            )
        });
        let pass = SpectralPassIdentity::new(
            if imported {
                SpectralPassPhase::FinalMajor
            } else {
                SpectralPassPhase::InitialMajor
            },
            ordinal,
        );
        let (base, source) = crate::spectral_cycle_plan::base_physical(
            &problem,
            registry,
            &policy,
            pass,
            pass_input.as_ref().map(FinalMajorPhaseInput::identity),
        )
        .map_err(io::Error::other)?;
        let read = base
            .execution_dag()
            .nodes()
            .values()
            .find(|node| node.kind == WorkKind::ObservationRead)
            .ok_or_else(|| io::Error::other("native cube plan lacks source read"))?
            .id
            .clone();
        let prepare = base
            .observation_transaction()
            .final_model_preparation()
            .ok_or_else(|| io::Error::other("native cube plan lacks model preparation"))?
            .clone();
        let reconcile = base
            .observation_transaction()
            .post_replay_reconciliation()
            .ok_or_else(|| io::Error::other("native cube plan lacks reconciliation"))?
            .clone();
        let weighting =
            plan_weighting(&problem, policy.weighting_limits).map_err(io::Error::other)?;
        let preparation = if !imported {
            Some(
                crate::weighting::NativePreparationPlan::new(
                    &problem,
                    &weighting,
                    workers,
                    NativePhasePlan::source_store(&base, &problem)?.block_rows,
                )
                .map_err(io::Error::other)?,
            )
        } else {
            None
        };
        let physical = if imported {
            base
        } else {
            WeightingPlanFragment::streaming_for_pass(
                &weighting,
                read.clone(),
                source.clone(),
                policy.implementation.clone(),
                pass,
                WeightingStreamingMode::NaturalInitial,
                None,
            )
            .with_native_preparation(preparation.expect("initial native source plan"))
            .compose(&base)
            .map_err(io::Error::other)?
        };
        let minor = minor.map(|(masks, program)| {
            (
                WorkNodeId::new(format!("native-cube-minor-{ordinal}")),
                masks,
                program,
            )
        });
        let physical = if let Some((node, _, _)) = &minor {
            let resources = crate::spectral_cycle_plan::MinorCycleResources::for_worker_count(
                ReconstructionPlaneWorkspace::for_problem(&problem).map_err(io::Error::other)?,
                &policy,
                workers as u64,
            )
            .map_err(io::Error::other)?;
            crate::spectral_cycle_plan::append_minor(
                registry, physical, &policy, node, resources, 0,
            )
            .map_err(io::Error::other)?
        } else {
            physical
        };
        // The initial grain is one complete output plane, including its normal
        // write window. Wave sizing groups these grains under the shared budget.
        let state_terminal = minor
            .as_ref()
            .map_or(&reconcile, |(node, _, _)| node)
            .clone();
        let before_cube_state = physical;
        let spectral = problem.geometry().spectral();
        let output_hz = (0..spectral.output_channels())
            .map(|i| {
                spectral
                    .channel_centre_hz(i)
                    .ok_or_else(|| io::Error::other("native cube output axis is incomplete"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let bands = if let Some(retained) = &retained {
            retained
                .native
                .bands
                .iter()
                .map(BandPlan::residual_refresh)
                .collect()
        } else {
            (0..output_hz.len())
                .map(|channel| {
                    BandPlan::new(
                        &SpectralOperatorSpecification::for_slab(&problem, channel, 1)?,
                        SpectralOperatorPass::InitialMajor,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(io::Error::other)?
        };
        // Unit fixtures pass their controlled enclosing-owner bound explicitly.
        #[cfg(not(test))]
        let enclosing_owner_bytes = enclosing_owner_bytes.max(super::enclosing_memory_bytes(
            pass_input
                .as_ref()
                .map_or(Ok(0), |input| {
                    input.evidence().normal_state().retained_resident_bytes()
                })
                .map_err(io::Error::other)?,
            retained.as_ref().map_or(0, |replay| {
                u64::try_from(replay.managed.residency.live_payload_bytes()).unwrap_or(u64::MAX)
            }),
        )?);
        let shared_bytes = enclosing_owner_bytes
            .checked_add(Self::shared_bytes(&problem, output_hz.capacity())?)
            .ok_or_else(|| io::Error::other("native cube owner residency overflow"))?;
        let plan_native = |physical: &PhysicalWorkBinding| {
            if let Some(retained) = &retained {
                NativePhasePlan::new_for_pass(
                    physical,
                    &policy.authority,
                    &policy.resource_policy,
                    &storage,
                    retained.native.store.plan,
                    &bands,
                    shared_bytes,
                    workers,
                    source_slots,
                    true,
                )
            } else {
                NativePhasePlan::for_initial_source(
                    physical,
                    &policy.authority,
                    &policy.resource_policy,
                    &storage,
                    &problem,
                    &bands,
                    shared_bytes,
                    workers,
                    source_slots,
                )
            }
        };
        let run = retained.as_ref().map(|retained| retained.managed.clone());
        let cache_bytes = if run.is_some() {
            0
        } else {
            let (minimum, full) = CubeStatePlan::managed_cache_limits(&problem, &storage, workers)?;
            let native_preflight = plan_native(&before_cube_state)?;
            let minimum_state = CubeStatePlan::managed_streaming_cube(
                &problem,
                &storage,
                1,
                prepare.clone(),
                state_terminal.clone(),
                None,
                minimum,
            )?;
            let minimum_physical = minimum_state
                .compose(
                    registry,
                    policy.implementation.clone(),
                    &storage,
                    before_cube_state.clone(),
                    &read,
                    &reconcile,
                )
                .map_err(io::Error::other)?;
            let remaining = policy
                .authority
                .remaining_planning_memory_bytes(
                    &policy.resource_policy,
                    minimum_physical.execution_dag().resource_alternative(),
                )
                .map_err(io::Error::other)?;
            if std::env::var_os("CASA_RS_TRACE_IMAGING_STAGE_TIMING").is_some() {
                eprintln!(
                    "streaming_cube_cache_plan minimum_bytes={minimum} full_bytes={full} remaining_after_minimum_bytes={remaining} initial_workspace_bytes={}",
                    native_preflight.workspace_bytes
                );
            }
            let extra = remaining
                .saturating_sub(native_preflight.workspace_bytes)
                .min(
                    u64::try_from(full - minimum)
                        .map_err(|_| io::Error::other("cache size overflow"))?,
                );
            minimum
                .checked_add(
                    usize::try_from(extra).map_err(|_| io::Error::other("cache size overflow"))?,
                )
                .ok_or_else(|| io::Error::other("cache size overflow"))?
        };
        let cube_state = CubeStatePlan::managed_streaming_cube(
            &problem,
            &storage,
            1,
            prepare.clone(),
            state_terminal,
            run.clone(),
            cache_bytes,
        )?;
        let physical = cube_state
            .compose(
                registry,
                policy.implementation.clone(),
                &storage,
                before_cube_state,
                &read,
                &reconcile,
            )
            .map_err(io::Error::other)?;
        let minimum_cache = if run.is_some() {
            CubeStatePlan::managed_cache_limits(&problem, &storage, workers)?.0
        } else {
            cache_bytes
        };
        let native_plan = plan_with_cache_reclaim(run.as_deref(), minimum_cache, ordinal, || {
            plan_native(&physical)
        })?;
        eprintln!(
            "streaming_cube_managed_cache ordinal={ordinal} bytes={} retained_bytes={}",
            cube_state
                .managed_run()
                .expect("managed cube plan")
                .residency
                .limit_bytes(),
            cube_state.retained_memory_bytes(),
        );
        let physical = native_plan.compose(physical, &storage)?;
        let (native, replay) = retained.map_or((None, None), |mut retained| {
            retained.native.bands = bands.iter().map(BandPlan::residual_refresh).collect();
            (Some(retained.native), Some(retained.replay))
        });
        Ok((
            physical,
            Self {
                problem,
                implementation: policy.implementation,
                weighting,
                preparation,
                source,
                pass,
                read,
                prepare,
                reconcile,
                output_hz,
                storage,
                native_plan,
                cube_state,
                minor,
                imported,
                phase_input_artifact,
                #[cfg(test)]
                failure: Failure::None,
                state: Mutex::new(State {
                    selected,
                    weighting: WeightingExecutionState::new(),
                    native,
                    bands: if imported { Vec::new() } else { bands },
                    lifecycle: None,
                    model: None,
                    complete: None,
                    pass_input,
                    prior: None,
                    masks: None,
                    replay,
                    minor: None,
                    #[cfg(test)]
                    fenced: false,
                    #[cfg(test)]
                    published: false,
                }),
            },
        ))
    }

    /// Transfer the prepared store and its original source/weighting completion once.
    pub fn take_native_replay(&self) -> Option<NativeReplay> {
        let mut state = self.state.lock().ok()?;
        Some(NativeReplay {
            native: state.native.take()?,
            replay: state.replay.take()?,
            managed: self.cube_state.managed_run()?,
        })
    }

    /// Transfer the reconciled major-cycle scientific completion once.
    pub fn take_completion(&self) -> Option<MajorCycleOperatorResult> {
        self.state.lock().ok()?.complete.take()
    }

    /// Transfer the existing CLEAN controller's completion once.
    pub fn take_reconstruction_cycle_completion(
        &self,
    ) -> Option<ReconstructionCyclePhaseCompletion> {
        self.state.lock().ok()?.minor.take()
    }

    fn shared_bytes(problem: &CompiledProblem, output_capacity: usize) -> io::Result<u64> {
        let [source] = problem.selected_observation().read_set().sources() else {
            return Err(io::Error::other("native cube requires one selected source"));
        };
        let [spw] = source.selection().spectral_windows() else {
            return Err(io::Error::other("native cube requires one spectral window"));
        };
        // Stokes I with at most four correlations stays inside the polarization
        // operator's inline SmallVec storage; no matrix heap is omitted here.
        let bytes = spw
            .channel_indices()
            .len()
            .checked_mul(size_of::<u32>())
            .and_then(|n| n.checked_add(output_capacity.checked_mul(size_of::<f64>())?))
            .and_then(|n| {
                n.checked_add(
                    size_of::<Self>()
                        + size_of::<PolarizationOperator>()
                        + size_of::<[CorrelationType; 4]>(),
                )
            })
            .ok_or_else(|| io::Error::other("native cube owner residency overflow"))?;
        Ok(bytes as u64)
    }

    fn fragment(&self) -> WeightingPlanFragment<'_> {
        WeightingPlanFragment::streaming_for_pass(
            &self.weighting,
            self.read.clone(),
            self.source.clone(),
            self.implementation.clone(),
            self.pass,
            WeightingStreamingMode::NaturalInitial,
            None,
        )
        .with_native_preparation(self.preparation.expect("initial native source plan"))
    }
}

fn plan_with_cache_reclaim(
    run: Option<&crate::cube_state_plan::ManagedCubeRun>,
    minimum: usize,
    ordinal: u32,
    plan_native: impl Fn() -> io::Result<NativePhasePlan>,
) -> io::Result<NativePhasePlan> {
    let mut plan = match plan_native() {
        Ok(plan) => plan,
        Err(error) if run.is_some() && error.kind() == io::ErrorKind::OutOfMemory => {
            let run = run.expect("retained run was checked");
            let current = run.residency.limit_bytes();
            run.shrink_cache_to(minimum)?;
            eprintln!(
                "streaming_cube_cache_reclaimed ordinal={ordinal} previous_bytes={current} retained_bytes={minimum} returned_bytes={} reason=one_band_floor",
                current - minimum
            );
            plan_native()?
        }
        Err(error) => return Err(error),
    };
    if plan.workspace_bytes < plan.worker_wave_bytes {
        if let Some(run) = run {
            let current = run.residency.limit_bytes();
            let target = cache_target_for_worker_wave(
                current,
                minimum,
                plan.workspace_bytes,
                plan.worker_wave_bytes,
            )?
            .expect("worker wave is short of workspace");
            run.shrink_cache_to(target)?;
            eprintln!(
                "streaming_cube_cache_reclaimed ordinal={ordinal} previous_bytes={current} retained_bytes={target} returned_bytes={}",
                current - target
            );
            plan = plan_native()?;
        }
    }
    if plan.workspace_bytes < plan.worker_wave_bytes {
        return Err(io::Error::other(format!(
            "managed cube cache would strand the admitted worker wave: workspace={} wave={}",
            plan.workspace_bytes, plan.worker_wave_bytes,
        )));
    }
    if let Some(run) = run {
        // A partial cache below a band traversal's reuse distance can have zero
        // hits. Reclaim optional image cache only when the complete useful frame
        // working set fits; do not displace images for another thrashing cache.
        let deficit = plan
            .reuse_workspace_bytes
            .saturating_sub(plan.workspace_bytes);
        let current = run.residency.limit_bytes();
        if deficit > 0 && deficit <= current.saturating_sub(minimum) as u64 {
            let target = current - deficit as usize;
            run.shrink_cache_to(target)?;
            eprintln!(
                "streaming_cube_cache_reclaimed ordinal={ordinal} previous_bytes={current} retained_bytes={target} returned_bytes={deficit} reason=native_replay_reuse"
            );
            plan = plan_native()?;
        }
    }
    Ok(plan)
}

fn cache_target_for_worker_wave(
    current: usize,
    minimum: usize,
    available_workspace: u64,
    required_worker_wave: u64,
) -> io::Result<Option<usize>> {
    let Some(deficit) = required_worker_wave.checked_sub(available_workspace) else {
        return Ok(None);
    };
    if deficit == 0 {
        return Ok(None);
    }
    let deficit = usize::try_from(deficit)
        .map_err(|_| io::Error::other("worker-wave cache reclaim overflow"))?;
    let target = current
        .checked_sub(deficit)
        .filter(|target| *target >= minimum)
        .ok_or_else(|| io::Error::other("mandatory worker wave exceeds minimum managed cache"))?;
    Ok(Some(target))
}

#[cfg(test)]
mod cache_reclaim_tests {
    use super::*;

    #[test]
    fn later_four_worker_wave_reclaims_only_its_deficit() {
        let current = 1_610_049_685;
        let minimum = 22_072_120;
        let initial_workspace = 226_069_785;
        let later_wave = 299_707_509;
        assert_eq!(
            cache_target_for_worker_wave(current, minimum, initial_workspace, later_wave).unwrap(),
            Some(current - (later_wave - initial_workspace) as usize)
        );
        assert_eq!(
            cache_target_for_worker_wave(current, minimum, later_wave, later_wave).unwrap(),
            None
        );
        assert!(
            cache_target_for_worker_wave(minimum, minimum, initial_workspace, later_wave).is_err()
        );
    }
}

impl WorkImplementation for InitialCube {
    type Error = io::Error;
    fn implementation_id(&self) -> &WorkImplementationId {
        &self.implementation
    }
    fn execute(&self, context: WorkExecutionContext<'_>) -> io::Result<WorkMeasurements> {
        let started = std::time::Instant::now();
        let mut state = self.state.lock().unwrap();
        if context.node().id == self.prepare {
            let executable = ExecutableModelProblem::from_compiled(self.problem.clone())
                .map_err(io::Error::other)?;
            let attempt = ModelExecutionAttemptId::new(LogicalIdentity::from_sha256(
                context.attempt_id().as_bytes(),
            ));
            let storage = self.cube_state.model_storage(context)?;
            let (lifecycle, named, terms) = if let Some(input) = state.pass_input.take() {
                let (terms, continuation, prior, masks) = input.into_execution_parts();
                let (lifecycle, named) = ModelLifecycle::continue_from(
                    executable,
                    attempt,
                    context.lease_epoch(),
                    continuation,
                    storage,
                )
                .map_err(io::Error::other)?;
                state.prior = Some(prior);
                state.masks = Some(masks);
                (lifecycle, named, Some(terms))
            } else {
                if self.imported {
                    return Err(io::Error::other(
                        "native refresh lacks accepted model input",
                    ));
                }
                let lifecycle =
                    ModelLifecycle::bind(executable, attempt, context.lease_epoch(), storage)
                        .map_err(io::Error::other)?;
                let named = lifecycle.initial_empty().map_err(io::Error::other)?;
                (lifecycle, named, None)
            };
            let delta = terms
                .filter(|terms| !terms.is_empty())
                .map(|terms| {
                    lifecycle
                        .compile_delta(&named, terms.iter().copied())
                        .map_err(io::Error::other)
                })
                .transpose()?;
            state.model = Some(
                MajorCyclePreparation::prepare(&lifecycle, named, delta)
                    .map_err(io::Error::other)?,
            );
            state.lifecycle = Some(lifecycle);
        } else if context.node().id == self.read {
            if self.imported {
                if state.native.is_none()
                    || state
                        .replay
                        .as_ref()
                        .is_none_or(|replay| replay.problem_id() != self.problem.problem_id())
                {
                    return Err(io::Error::other("native import lacks its owned source"));
                }
            } else {
                let selected = state
                    .selected
                    .take()
                    .ok_or_else(|| io::Error::other("native source already consumed"))?;
                let fragment = self.fragment();
                fragment
                    .authorize_source_observation(
                        context,
                        &self.problem,
                        &selected
                            .certify_residency(&self.problem)
                            .map_err(io::Error::other)?,
                    )
                    .map_err(io::Error::other)?;
                let bands = std::mem::take(&mut state.bands);
                let input = NativePreparation::new(
                    &self.storage,
                    self.native_plan.store,
                    &self.problem,
                    &self.output_hz,
                    bands,
                )?;
                let source_open = std::time::Instant::now();
                let selected = selected.open(&self.problem).map_err(io::Error::other)?;
                let source_open_nanos = source_open.elapsed().as_nanos();
                state.native = Some(input.traverse(
                    context,
                    &fragment,
                    &self.problem,
                    selected,
                    &mut state.weighting,
                )?);
                if std::env::var_os("CASA_RS_TRACE_IMAGING_STAGE_TIMING").is_some() {
                    eprintln!(
                        "streaming_cube_source open_nanos={source_open_nanos} traversal={:?} stream={:?}",
                        state.weighting.latest_traversal_measurements(),
                        state.weighting.latest_stream_measurements()
                    );
                }
                #[cfg(test)]
                assert!(state.weighting.replay_completion().is_none());
            }
        } else if context.node().id == self.reconcile {
            #[cfg(test)]
            assert!(self.imported || state.fenced);
            let PreparedNative {
                mut store,
                bands,
                layout,
            } = state
                .native
                .take()
                .ok_or_else(|| io::Error::other("native source is not prepared"))?;
            let model = state
                .model
                .take()
                .ok_or_else(|| io::Error::other("native model is not prepared"))?;
            let mut correlations = [CorrelationType::StokesI; 4];
            for (target, (_, correlation)) in correlations.iter_mut().zip(&layout.correlations) {
                *target = *correlation;
            }
            let polarization = PolarizationOperator::compile(
                self.problem.reconstruction().polarization().coordinates(),
                &correlations[..layout.correlations.len()],
                [0.0; 2],
                MuellerMatrix::identity(),
            )
            .map_err(io::Error::other)?;
            let replay = state
                .replay
                .as_ref()
                .or_else(|| state.weighting.replay_completion())
                .ok_or_else(|| io::Error::other("native source fence is incomplete"))?;
            let retained_bands = bands.iter().map(BandPlan::residual_refresh).collect();
            let reconciliation = &self.reconcile;
            #[cfg(test)]
            let reconciliation = if self.failure == Failure::ReconciliationNode {
                &self.read
            } else {
                reconciliation
            };
            let normal_storage = self.cube_state.normal_storage()?;
            let mut refresh = if self.imported {
                Some(
                    PendingCubeRefresh::new(
                        context,
                        reconciliation,
                        &self.read,
                        &SpectralOperatorSpecification::new(&self.problem)
                            .map_err(io::Error::other)?,
                        state
                            .prior
                            .as_ref()
                            .ok_or_else(|| io::Error::other("native refresh lacks prior normal"))?,
                        model.final_model().generation_id(),
                        replay,
                        &normal_storage,
                    )
                    .map_err(io::Error::other)?,
                )
            } else {
                None
            };
            let mut fold = if self.imported {
                None
            } else {
                Some(
                    PendingStreamingCubeFold::new(context, reconciliation, replay, normal_storage)
                        .map_err(io::Error::other)?,
                )
            };
            let mut pending = bands.into_iter();
            while !pending.as_slice().is_empty() {
                let count = WavePlan::prefix(
                    store.plan,
                    pending.as_slice(),
                    self.native_plan.workers,
                    self.native_plan.source_slots,
                    self.native_plan.shared_bytes,
                    self.native_plan.workspace_bytes,
                )?;
                let jobs = pending.by_ref().take(count).collect::<Vec<_>>();
                let mut measurements = None;
                let mut append = |result| -> io::Result<()> {
                    match result {
                        BandResult::Residual(residual) => refresh
                            .as_mut()
                            .ok_or_else(|| {
                                io::Error::other("initial phase returned residual-only output")
                            })?
                            .append(residual)
                            .map_err(io::Error::other),
                        BandResult::Initial(normal) if !self.imported => {
                            let core = normal.slab().core_range();
                            let spec = SpectralOperatorSpecification::for_slab(
                                &self.problem,
                                core.start,
                                core.len(),
                            )
                            .map_err(io::Error::other)?;
                            fold.as_mut()
                                .ok_or_else(|| io::Error::other("initial cube fold is absent"))?
                                .append(&spec, normal)
                                .map_err(io::Error::other)
                        }
                        BandResult::Initial(_) => {
                            Err(io::Error::other("refresh returned initial normal fields"))
                        }
                    }
                };
                let wave = execute::execute(
                    &mut store,
                    jobs,
                    model.final_model(),
                    &layout,
                    &self.output_hz,
                    &polarization,
                    self.native_plan.workers,
                    self.native_plan.source_slots,
                    self.native_plan.shared_bytes,
                    self.native_plan.workspace_bytes,
                    self.pass.ordinal(),
                    &mut measurements,
                    &mut append,
                )?;
                if let Some(measurements) = measurements {
                    eprintln!(
                        "streaming_cube_wave ordinal={} bands={} workers={} measured={measurements:?} io={:?}",
                        self.pass.ordinal(),
                        wave.bands_consumed,
                        self.native_plan.workers,
                        wave.source
                    );
                }
                eprintln!(
                    "streaming_cube_completed_queue peak={} admitted_workers={}",
                    wave.peak_completed, self.native_plan.workers
                );
            }
            let complete = if let Some(refresh) = refresh {
                refresh.complete()
            } else {
                fold.ok_or_else(|| io::Error::other("native cube produced no bands"))?
                    .complete(replay)
            }
            .map_err(io::Error::other)?;
            if let Some(prior) = state.prior.take() {
                prior.retire_obsolete().map_err(io::Error::other)?;
            }
            state.native = Some(PreparedNative {
                store,
                bands: retained_bands,
                layout,
            });
            let mut operator =
                MajorCycleOperatorState::begin(complete, model).map_err(io::Error::other)?;
            if let Some(masks) = state.masks.take() {
                operator = operator
                    .bind_reconstruction_masks(&masks)
                    .map_err(io::Error::other)?;
            }
            state.complete = Some(
                operator
                    .reconcile(
                        context,
                        state
                            .lifecycle
                            .as_mut()
                            .ok_or_else(|| io::Error::other("native model lifecycle is absent"))?,
                    )
                    .map_err(io::Error::other)?,
            );
        } else if let Some((_, masks, program)) = self
            .minor
            .as_ref()
            .filter(|(node, _, _)| context.node().id == *node)
        {
            let complete = state
                .complete
                .take()
                .ok_or_else(|| io::Error::other("native minor lacks normal state"))?;
            let lifecycle = state
                .lifecycle
                .as_ref()
                .ok_or_else(|| io::Error::other("native minor lacks lifecycle"))?;
            let mut measurements = None;
            let minor = complete.run_reconstruction_cycle(
                lifecycle,
                masks,
                program.clone(),
                context,
                self.pass.ordinal(),
                &mut measurements,
            )?;
            if let Some(measurements) = measurements {
                eprintln!(
                    "streaming_cube_minor ordinal={} measured={measurements:?}",
                    self.pass.ordinal()
                );
            }
            state.minor = Some(minor);
        } else if !self.imported && context.node().id == *self.fragment().release_node() {
            if context.is_cleanup() {
                state
                    .weighting
                    .release(context, &self.fragment())
                    .map_err(io::Error::other)?;
            } else {
                state.replay = Some(
                    state
                        .weighting
                        .release_retaining_replay(context, &self.fragment())
                        .map_err(io::Error::other)?,
                );
            }
        }
        eprintln!(
            "streaming_cube_stage ordinal={} node={} elapsed_nanos={}",
            self.pass.ordinal(),
            context.node().id.as_str(),
            started.elapsed().as_nanos()
        );
        self.cube_state.log_measurements(&context.node().id);
        Ok(WorkMeasurements::new(
            context
                .node()
                .claims
                .iter()
                .map(|claim| {
                    ResourceMeasurement::new(
                        claim.resource.clone(),
                        claim.lifetime.clone(),
                        claim.amount,
                    )
                })
                .collect(),
            context
                .stage_prediction()
                .io()
                .iter()
                .map(|prediction| {
                    IoMeasurement::new(
                        prediction.kind(),
                        prediction.bytes(),
                        prediction.operations(),
                    )
                })
                .collect(),
            self.phase_input_artifact
                .filter(|_| context.node().id == self.prepare)
                .map(|(identity, bytes)| {
                    crate::ArtifactMeasurement::new(
                        identity,
                        Some(identity),
                        crate::ArtifactDisposition::Loaded,
                        bytes,
                        None,
                    )
                    .map_err(io::Error::other)
                })
                .transpose()?
                .into_iter()
                .collect(),
        ))
    }
    fn failure_measurements<'a>(&'a self, _: &'a io::Error) -> Option<&'a WorkMeasurements> {
        None
    }
    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> io::Result<WorkMeasurements> {
        #[cfg(test)]
        if _context.node().id == self.read && _fence == FenceKind::Io {
            if self.failure == Failure::SourceFence {
                return Err(io::Error::from_raw_os_error(5));
            }
            self.state.lock().unwrap().fenced = true;
        }
        Ok(WorkMeasurements::default())
    }
    fn complete_observation_read(
        &self,
        completion: ObservationReadCompletionContext,
    ) -> io::Result<AttemptBoundObservationCompletion> {
        #[cfg(test)]
        assert!(completion.settled_fences().contains(&FenceKind::Io));
        self.state
            .lock()
            .unwrap()
            .weighting
            .complete_replay(completion)
            .map_err(io::Error::other)
    }
    fn publish(&self, context: WorkExecutionContext<'_>) -> io::Result<()> {
        let ready = {
            let state = self.state.lock().unwrap();
            state.complete.is_some() || state.minor.is_some()
        };
        if context.node().kind != WorkKind::Publication || context.publication().is_none() || !ready
        {
            return Err(io::Error::other(
                "native cube commit lacks completed transaction authority",
            ));
        }
        #[cfg(test)]
        {
            self.state.lock().unwrap().published = true;
        }
        Ok(())
    }

    fn retain_artifact_resources(
        &self,
        owner: &WorkNodeId,
        permit: RetainedArtifactPermit,
    ) -> io::Result<bool> {
        if self.cube_state.retains_at(owner) {
            self.cube_state.retain(owner, permit)?;
            return Ok(true);
        }
        if !self.native_plan.retains_at(owner) {
            return Ok(false);
        }
        let mut state = self.state.lock().unwrap();
        self.native_plan.retain(
            &mut state
                .native
                .as_mut()
                .ok_or_else(|| io::Error::other("native store is absent at retention"))?
                .store,
            permit,
        )?;
        Ok(true)
    }

    fn abort_node_io(&self, owner: &WorkNodeId) -> io::Result<()> {
        if owner == &self.read {
            let mut state = self.state.lock().unwrap();
            state.native = None;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
