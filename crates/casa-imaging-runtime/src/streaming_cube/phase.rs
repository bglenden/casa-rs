// SPDX-License-Identifier: LGPL-3.0-or-later

//! Initial cube phase on the existing source, resource and reconciliation DAG.
//! Native storage and band ownership replace the historical numerical executor.

use super::{
    execute::{self, BandInput, WavePlan},
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
    complete_data_operator::{CompleteDataSlabResult, PendingCompleteDataSlabFold},
    cube_state_plan::CubeStatePlan,
};
use casa_imaging_model::{
    CompiledProblem, CorrelationType, LogicalIdentity, ModelExecutionAttemptId,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FinalNormalState, ImageDomainReconstructionMaskPlans,
    MajorCyclePreparation, MinorCycleProgram, ModelLifecycle, MuellerMatrix, PolarizationOperator,
    ReconstructionMaskSet, SpectralOperatorSpecification, WeightingPlan, plan_weighting,
    runtime_adapter::{BandPlan, ReconstructionPlaneWorkspace, SpectralOperatorPass},
};
use casa_ms::DeferredSelectedObservationAccess;
use std::{io, sync::Mutex};

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

/// Internal comparison executor; application CLEAN and publication stay shared.
pub struct InitialCube {
    problem: CompiledProblem,
    implementation: WorkImplementationId,
    weighting: WeightingPlan,
    preparation: Option<crate::weighting::ReplayPreparationPlan>,
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
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum Failure {
    None,
    SourceFence,
    ReconciliationNode,
}

impl InitialCube {
    /// Compose the initial source, paged state and native wave reservations.
    /// `enclosing_owner_bytes` counts live application owners not charged by
    /// these components; it must be supplied by complete application composition.
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
        Self::initial(
            problem,
            registry,
            policy,
            storage,
            selected,
            workers,
            source_slots,
            enclosing_owner_bytes,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn initial(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        selected: DeferredSelectedObservationAccess,
        workers: usize,
        source_slots: usize,
        enclosing_owner_bytes: u64,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
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
            minor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn refresh(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        retained: NativeReplay,
        input: FinalMajorPhaseInput,
        ordinal: u32,
        workers: usize,
        source_slots: usize,
        enclosing_owner_bytes: u64,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
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
            source_slots,
            enclosing_owner_bytes,
            minor,
        )
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
        let preparation = if !imported && workers > 1 {
            Some(
                crate::weighting::ReplayPreparationPlan::new(&problem, &weighting, workers)
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
            .with_replay_preparation(preparation)
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
        let mut cube_state = CubeStatePlan::streaming_cube(
            &problem,
            &storage,
            1,
            prepare.clone(),
            state_terminal.clone(),
            false,
        )?;
        let before_cube_state = physical;
        let mut physical = cube_state
            .compose(
                registry,
                policy.implementation.clone(),
                &storage,
                before_cube_state.clone(),
                &read,
                &reconcile,
            )
            .map_err(io::Error::other)?;
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
        #[cfg(all(casa_streaming_cube_comparison, not(test)))]
        let enclosing_owner_bytes = enclosing_owner_bytes.max(super::comparison_live_heap_bytes(
            pass_input
                .as_ref()
                .map_or(Ok(0), |input| {
                    input.evidence().normal_state().retained_resident_bytes()
                })
                .map_err(io::Error::other)?,
        )?);
        let shared_bytes = enclosing_owner_bytes
            .checked_add(Self::shared_bytes(&problem, output_hz.capacity())?)
            .ok_or_else(|| io::Error::other("native cube owner residency overflow"))?;
        let plan_native = |physical: &PhysicalWorkBinding| {
            if let Some(retained) = &retained {
                let prior_window_bytes =
                    casa_imaging_reconstruction::normal_state_window_residency_bytes(
                        problem.model_lifecycle().target().domains()[0].pixels(),
                        1,
                        output_hz.len(),
                        1,
                    )
                    .map_err(io::Error::other)?;
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
                    Some(prior_window_bytes),
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
        let mut native_plan = plan_native(&physical)?;
        let resident = CubeStatePlan::streaming_cube(
            &problem,
            &storage,
            1,
            prepare.clone(),
            state_terminal,
            true,
        )?;
        // Select storage before any payload allocation. Existing retained
        // generations and all base owners are already charged by the authority;
        // leave enough for preparation and at least one complete band job.
        let available = policy
            .authority
            .remaining_planning_memory_bytes(
                &policy.resource_policy,
                physical.execution_dag().resource_alternative(),
            )
            .map_err(io::Error::other)?;
        let additional = resident
            .retained_memory_bytes()
            .saturating_sub(cube_state.retained_memory_bytes());
        if additional <= available.saturating_sub(native_plan.minimum_workspace_bytes) {
            physical = resident
                .compose(
                    registry,
                    policy.implementation.clone(),
                    &storage,
                    before_cube_state,
                    &read,
                    &reconcile,
                )
                .map_err(io::Error::other)?;
            native_plan = plan_native(&physical)?;
            cube_state = resident;
            eprintln!(
                "streaming_cube_normal_storage ordinal={ordinal} backing=resident retained_bytes={}",
                cube_state.retained_memory_bytes()
            );
        } else {
            eprintln!(
                "streaming_cube_normal_storage ordinal={ordinal} backing=paged retained_bytes={}",
                cube_state.retained_memory_bytes()
            );
        }
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

    pub fn take_native_replay(&self) -> Option<NativeReplay> {
        let mut state = self.state.lock().ok()?;
        Some(NativeReplay {
            native: state.native.take()?,
            replay: state.replay.take()?,
        })
    }

    pub fn take_completion(&self) -> Option<MajorCycleOperatorResult> {
        self.state.lock().ok()?.complete.take()
    }

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
        .with_replay_preparation(self.preparation)
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
            let mut fold: Option<PendingCompleteDataSlabFold> = None;
            let mut pending = bands.into_iter();
            while !pending.as_slice().is_empty() {
                let count = if self.imported {
                    WavePlan::refresh_prefix(
                        store.plan,
                        pending.as_slice(),
                        self.native_plan.workers,
                        self.native_plan.source_slots,
                        self.native_plan.shared_bytes,
                        self.native_plan.workspace_bytes,
                        self.native_plan.prior_window_bytes,
                    )?
                } else {
                    WavePlan::initial_prefix(
                        store.plan,
                        pending.as_slice(),
                        self.native_plan.workers,
                        self.native_plan.source_slots,
                        self.native_plan.shared_bytes,
                        self.native_plan.workspace_bytes,
                    )?
                };
                let jobs = pending
                    .by_ref()
                    .take(count)
                    .map(|plan| {
                        Ok(BandInput {
                            prior: state
                                .prior
                                .as_ref()
                                .map(|prior| {
                                    prior
                                        .read_streaming_cube_window(plan.core())
                                        .map_err(io::Error::other)
                                })
                                .transpose()?,
                            plan,
                            fft: None,
                        })
                    })
                    .collect::<io::Result<Vec<_>>>()?;
                let mut measurements = None;
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
                )?;
                if let Some(measurements) = measurements {
                    eprintln!(
                        "streaming_cube_wave ordinal={} bands={} workers={} measured={measurements:?} io={:?}",
                        self.pass.ordinal(),
                        wave.bands.len(),
                        self.native_plan.workers,
                        wave.source
                    );
                }
                for (normal, _) in wave.bands {
                    let core = normal.slab().core_range();
                    let spec = SpectralOperatorSpecification::for_slab(
                        &self.problem,
                        core.start,
                        core.len(),
                    )
                    .map_err(io::Error::other)?;
                    let reconciliation = &self.reconcile;
                    #[cfg(test)]
                    let reconciliation = if self.failure == Failure::ReconciliationNode {
                        &self.read
                    } else {
                        reconciliation
                    };
                    let band = if self.imported {
                        CompleteDataSlabResult::from_streaming_cube_refresh(
                            context,
                            reconciliation,
                            &self.read,
                            &spec,
                            normal,
                            replay,
                        )
                        .map_err(io::Error::other)?
                    } else {
                        CompleteDataSlabResult::from_streaming_cube(
                            context,
                            reconciliation,
                            &spec,
                            normal,
                            replay,
                        )
                        .map_err(io::Error::other)?
                    };
                    fold = Some(
                        match fold {
                            None => band.begin_fold(&self.cube_state.normal_storage()?),
                            Some(prefix) => prefix.fold(band),
                        }
                        .map_err(io::Error::other)?,
                    );
                }
            }
            let fold = fold.ok_or_else(|| io::Error::other("native cube produced no bands"))?;
            let complete = if self.imported {
                fold.complete_gridded()
            } else {
                fold.complete(replay)
            }
            .map_err(io::Error::other)?;
            state.prior = None;
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
