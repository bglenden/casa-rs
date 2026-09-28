// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct-MS cube major phases on the shared model, CLEAN and publication DAG.

use super::bulk_wave::BulkWave;
use crate::complete_data_operator::{PendingCubeRefresh, PendingStreamingCubeFold};
use crate::cube_state_plan::CubeStatePlan;
use crate::weighting::bulk_source::BulkInputPlan;
use crate::*;
use casa_imaging_model::{
    CompiledProblem, CorrelationType, LogicalIdentity, ModelExecutionAttemptId,
};
use casa_imaging_reconstruction::runtime_adapter::{
    BandPlan, BandResult, ReconstructionPlaneWorkspace, SpectralOperatorPass,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FinalNormalState, ImageDomainReconstructionMaskPlans,
    MajorCyclePreparation, MinorCycleProgram, ModelLifecycle, MuellerMatrix, PolarizationOperator,
    ReconstructionMaskSet, SpectralOperatorSpecification, WeightingPlan, plan_weighting,
};
use casa_ms::DeferredSelectedObservationAccess;
use std::{
    io,
    sync::{Arc, Mutex},
};

/// Direct selected-input cube execution using the existing model and CLEAN owners.
pub struct BulkCubePhase {
    problem: CompiledProblem,
    implementation: WorkImplementationId,
    weighting: WeightingPlan,
    source: SelectedObservationSourceResources,
    pass: SpectralPassIdentity,
    read: WorkNodeId,
    prepare: WorkNodeId,
    reconcile: WorkNodeId,
    phase_input_artifact: Option<(ArtifactIdentity, u64)>,
    input: BulkInputPlan,
    workers: usize,
    workspace: u64,
    wave_bytes: u64,
    output_hz: Vec<f64>,
    cube_state: Option<CubeStatePlan>,
    mfs: Option<CompleteDataPlanFragment>,
    frozen_reservation: Option<Arc<FrozenWeightingReservation>>,
    minor: Option<(
        WorkNodeId,
        ImageDomainReconstructionMaskPlans,
        MinorCycleProgram,
    )>,
    state: Mutex<State>,
}

/// Frozen weighting and bounded normal/model residency shared between major passes.
/// Visibility payload is never retained here; each pass rebinds its source.
pub struct BulkCubeReplay {
    bands: Vec<BandPlan>,
    weighting: FrozenWeightingArtifact,
    managed: Option<Arc<crate::cube_state_plan::ManagedCubeRun>>,
}

struct State {
    selected: Option<DeferredSelectedObservationAccess>,
    weighting: WeightingExecutionState,
    imported: Option<FrozenWeightingArtifact>,
    bands: Vec<BandPlan>,
    replay: Option<BulkCubeReplay>,
    lifecycle: Option<ModelLifecycle>,
    model: Option<MajorCyclePreparation>,
    input: Option<FinalMajorPhaseInput>,
    prior: Option<FinalNormalState>,
    masks: Option<ReconstructionMaskSet>,
    fold: Option<PendingStreamingCubeFold>,
    refresh: Option<PendingCubeRefresh>,
    mfs_prepared: Option<CompleteDataPreparedState>,
    mfs_operator: Option<SpectralOperatorState>,
    complete: Option<MajorCycleOperatorResult>,
    minor: Option<ReconstructionCyclePhaseCompletion>,
}

impl BulkCubePhase {
    /// Test whether this compiled science is implemented by the direct cube kernel.
    pub fn supports(problem: &CompiledProblem) -> io::Result<bool> {
        super::phase::InitialCube::supports(problem)
    }

    /// Build an empty-model initial major phase with admitted shared-source waves.
    pub fn initial(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        selected: DeferredSelectedObservationAccess,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
        Self::build(
            problem, registry, policy, storage, selected, None, None, 0, minor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    /// Rebind the unchanged source under fresh locks for an updated model epoch.
    pub fn refresh(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        selected: DeferredSelectedObservationAccess,
        retained: BulkCubeReplay,
        input: FinalMajorPhaseInput,
        ordinal: u32,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
        if ordinal == 0 {
            return Err(io::Error::other("bulk refresh requires a later pass"));
        }
        Self::build(
            problem,
            registry,
            policy,
            storage,
            selected,
            Some(retained),
            Some(input),
            ordinal,
            minor,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build(
        problem: CompiledProblem,
        registry: &impl ImplementationRegistry,
        policy: SpectralCycleExecutionPolicy,
        storage: ManagedSpillStorage,
        selected: DeferredSelectedObservationAccess,
        retained: Option<BulkCubeReplay>,
        input: Option<FinalMajorPhaseInput>,
        ordinal: u32,
        minor: Option<(ImageDomainReconstructionMaskPlans, MinorCycleProgram)>,
    ) -> io::Result<(PhysicalWorkBinding, Self)> {
        if policy.visibility_write.is_some()
            || policy.aw_projection.is_some()
            || policy.aw_reader.is_some()
            || input.is_some() != retained.is_some()
        {
            return Err(io::Error::other("unsupported bulk cube phase combination"));
        }
        let workers = usize::try_from(
            policy
                .authority
                .planning_worker_capacity(&policy.resource_policy)
                .map_err(io::Error::other)?
                .min(problem.geometry().spectral().output_channels() as u64),
        )
        .map_err(io::Error::other)?;
        let phase_input_artifact = input.as_ref().map(|input| {
            (
                input.identity(),
                (input.pending_delta_terms() * size_of::<casa_imaging_model::ModelDeltaTerm>())
                    as u64,
            )
        });
        let pass = SpectralPassIdentity::new(
            if retained.is_some() {
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
            input.as_ref().map(FinalMajorPhaseInput::identity),
        )
        .map_err(io::Error::other)?;
        let read = base
            .execution_dag()
            .nodes()
            .values()
            .find(|n| n.kind == WorkKind::ObservationRead)
            .ok_or_else(|| io::Error::other("bulk phase lacks source node"))?
            .id
            .clone();
        let prepare = base
            .observation_transaction()
            .final_model_preparation()
            .ok_or_else(|| io::Error::other("bulk phase lacks model node"))?
            .clone();
        let reconcile = base
            .observation_transaction()
            .post_replay_reconciliation()
            .ok_or_else(|| io::Error::other("bulk phase lacks reconciliation"))?
            .clone();
        let weighting =
            plan_weighting(&problem, policy.weighting_limits).map_err(io::Error::other)?;
        let selected_source = &problem.selected_observation().read_set().sources()[0];
        let residency = selected
            .certify_residency(&problem)
            .map_err(io::Error::other)?;
        let channels = selected_source.selection().spectral_windows()[0]
            .channel_indices()
            .len();
        let correlations = selected_source.selection().correlations()[0]
            .products()
            .len();
        // A source row necessarily retains at least one Float and one flag per
        // selected correlation/channel. This bounds geometry even when source
        // metadata or a sparse physical span reduces the actual block depth.
        let budget = residency
            .content_budget(selected_source.measurement_set())
            .ok_or_else(|| io::Error::other("bulk source budget missing"))?
            .available_bytes();
        let rows = (budget
            / channels
                .checked_mul(correlations)
                .and_then(|n| n.checked_mul(5))
                .ok_or_else(|| io::Error::other("bulk row size overflow"))?)
        .min(selected_source.selection().rows().selected_row_count() as usize);
        let bulk_input = BulkInputPlan::new(&problem, rows)?;
        let output_hz = (0..problem.geometry().spectral().output_channels())
            .map(|i| {
                problem
                    .geometry()
                    .spectral()
                    .channel_centre_hz(i)
                    .ok_or_else(|| io::Error::other("missing output frequency"))
            })
            .collect::<io::Result<Vec<_>>>()?;
        let is_mfs = matches!(
            problem.reconstruction().basis(),
            casa_imaging_model::ReconstructionBasis::Constant
        );
        let bands = if is_mfs {
            Vec::new()
        } else if let Some(retained) = &retained {
            retained
                .bands
                .iter()
                .map(BandPlan::residual_refresh)
                .collect()
        } else {
            (0..output_hz.len())
                .map(|i| {
                    BandPlan::new(
                        &SpectralOperatorSpecification::for_slab(&problem, i, 1)?,
                        SpectralOperatorPass::InitialMajor,
                    )
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(io::Error::other)?
        };
        let frozen_reservation = if retained.is_none() {
            Some(Arc::new(
                FrozenWeightingReservation::acquire(
                    &policy.authority,
                    policy.resource_policy.clone(),
                    weighting.planned_residency(),
                    residency.replay_proof_retained_heap_bytes(),
                )
                .map_err(io::Error::other)?,
            ))
        } else {
            None
        };
        let minor = minor.map(|(masks, program)| {
            (
                WorkNodeId::new(format!("bulk-cube-minor-{ordinal}")),
                masks,
                program,
            )
        });
        let run = retained.as_ref().and_then(|r| r.managed.clone());
        let (minimum_cache, full_cache) = if is_mfs {
            (0, 0)
        } else {
            CubeStatePlan::managed_cache_limits(&problem, &storage, workers)?
        };
        let mut cube_state = if is_mfs {
            None
        } else {
            Some(CubeStatePlan::managed_streaming_cube(
                &problem,
                &storage,
                1,
                prepare.clone(),
                minor.as_ref().map_or(&reconcile, |m| &m.0).clone(),
                run.clone(),
                minimum_cache,
            )?)
        };
        let compose_state = |physical,
                             cube_state: &Option<CubeStatePlan>|
         -> io::Result<PhysicalWorkBinding> {
            let physical = if let Some((node, _, _)) = &minor {
                let resources = crate::spectral_cycle_plan::MinorCycleResources::for_worker_count(
                    ReconstructionPlaneWorkspace::for_problem(&problem)
                        .map_err(io::Error::other)?,
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
            match cube_state {
                Some(cube_state) => cube_state
                    .compose(
                        registry,
                        policy.implementation.clone(),
                        &storage,
                        physical,
                        &read,
                        &reconcile,
                    )
                    .map_err(io::Error::other),
                None => Ok(physical),
            }
        };
        let metadata = bands.iter().try_fold(0_u64, |sum, b| {
            sum.checked_add(b.preparation_metadata_bytes().map_err(io::Error::other)? as u64)
                .ok_or_else(|| io::Error::other("bulk band metadata overflow"))
        })?;
        let enclosing = super::enclosing_memory_bytes(
            input
                .as_ref()
                .filter(|_| !is_mfs)
                .map_or(Ok(0), |i| {
                    i.evidence().normal_state().retained_resident_bytes()
                })
                .map_err(io::Error::other)?,
            retained
                .as_ref()
                .and_then(|r| r.managed.as_ref())
                .map_or(0, |r| r.residency.live_payload_bytes() as u64),
        )?;
        let stacks = if workers > 1 {
            workers as u64 * crate::bounded_stream::BOUNDED_WORKER_STACK_BYTES as u64
        } else {
            0
        };
        let team = crate::bounded_stream::BoundedKernelPlan::new::<(), ()>(workers, 1, 0)
            .map_err(|e| io::Error::other(format!("bulk team: {e:?}")))?
            .capacity_bytes()
            - stacks;
        let fixed = bulk_input
            .bytes
            .checked_add(metadata)
            .and_then(|n| n.checked_add(enclosing))
            .and_then(|n| n.checked_add(team))
            .and_then(|n| n.checked_add(size_of::<Self>() as u64 + output_hz.capacity() as u64 * 8))
            .ok_or_else(|| io::Error::other("bulk fixed residency overflow"))?;
        let base_fragment = WeightingPlanFragment::streaming_for_pass(
            &weighting,
            read.clone(),
            source.clone(),
            policy.implementation.clone(),
            pass,
            WeightingStreamingMode::NaturalInitial,
            None,
        )
        .with_bulk_workspace(workers, fixed);
        let minimum = compose_state(
            base_fragment.compose(&base).map_err(io::Error::other)?,
            &cube_state,
        )?;
        let remaining_memory = || {
            policy.authority.remaining_planning_memory_bytes(
                &policy.resource_policy,
                minimum.execution_dag().resource_alternative(),
            )
        };
        let mut remaining = match remaining_memory() {
            Ok(bytes) => bytes,
            Err(ResourceError::Infeasible { resource, .. })
                if resource.starts_with("memory-domain:")
                    && run
                        .as_ref()
                        .is_some_and(|run| run.residency.limit_bytes() > minimum_cache) =>
            {
                run.as_ref()
                    .expect("retained optional cache")
                    .shrink_cache_to(minimum_cache)?;
                remaining_memory().map_err(io::Error::other)?
            }
            Err(error) => return Err(io::Error::other(error)),
        };
        if let Some(run) = &run {
            let floor = BulkWave::bytes(&bands[..workers.min(bands.len())])?;
            let deficit = floor.saturating_sub(remaining);
            let current = run.residency.limit_bytes();
            let reclaim = deficit.min(current.saturating_sub(minimum_cache) as u64);
            if reclaim > 0 {
                run.shrink_cache_to(current - reclaim as usize)?;
                remaining = remaining_memory().map_err(io::Error::other)?;
            }
        }
        let count = if is_mfs {
            0
        } else {
            BulkWave::prefix(&bands, remaining)?
        };
        let wave_bytes = BulkWave::bytes(&bands[..count])?;
        if !is_mfs && run.is_none() {
            let extra = remaining
                .saturating_sub(wave_bytes)
                .min((full_cache - minimum_cache) as u64) as usize;
            if extra > 0 {
                cube_state = Some(CubeStatePlan::managed_streaming_cube(
                    &problem,
                    &storage,
                    1,
                    prepare.clone(),
                    minor.as_ref().map_or(&reconcile, |m| &m.0).clone(),
                    None,
                    minimum_cache + extra,
                )?);
            }
        }
        let workspace = fixed
            .checked_add(wave_bytes)
            .ok_or_else(|| io::Error::other("bulk phase residency overflow"))?;
        let physical = compose_state(
            WeightingPlanFragment::streaming_for_pass(
                &weighting,
                read.clone(),
                source.clone(),
                policy.implementation.clone(),
                pass,
                WeightingStreamingMode::NaturalInitial,
                None,
            )
            .with_bulk_workspace(workers, workspace)
            .compose(&base)
            .map_err(io::Error::other)?,
            &cube_state,
        )?;
        let (physical, mfs) = if is_mfs {
            let complete = CompleteDataPlanFragment::new_with_preparation_node(
                &problem,
                1,
                read.clone(),
                WorkNodeId::new(format!("bulk-mfs-fft-{ordinal}")),
                if ordinal == 0 {
                    SpectralOperatorPass::InitialMajor
                } else {
                    SpectralOperatorPass::ResidualRefresh
                },
            )
            .map_err(io::Error::other)?;
            let (physical, complete) = complete.compose(&physical).map_err(io::Error::other)?;
            (physical, Some(complete))
        } else {
            (physical, None)
        };
        let imported = retained.map(|r| r.weighting);
        let execution = imported
            .as_ref()
            .map_or_else(WeightingExecutionState::new, |w| {
                WeightingExecutionState::with_frozen_artifact(w.clone())
            });
        eprintln!(
            "bulk_cube_plan ordinal={ordinal} workers={workers} wave_bands={count} wave_bytes={wave_bytes} input_rows_bound={rows} input_bytes={} cache_bytes={}",
            bulk_input.bytes,
            cube_state
                .as_ref()
                .and_then(CubeStatePlan::managed_run)
                .map_or(0, |run| run.residency.limit_bytes())
        );
        Ok((
            physical,
            Self {
                problem,
                implementation: policy.implementation,
                weighting,
                source,
                pass,
                read,
                prepare,
                reconcile,
                phase_input_artifact,
                input: bulk_input,
                workers,
                workspace,
                wave_bytes,
                output_hz,
                cube_state,
                mfs,
                frozen_reservation,
                minor,
                state: Mutex::new(State {
                    selected: Some(selected),
                    weighting: execution,
                    imported,
                    bands,
                    replay: None,
                    lifecycle: None,
                    model: None,
                    input,
                    prior: None,
                    masks: None,
                    fold: None,
                    refresh: None,
                    mfs_prepared: None,
                    mfs_operator: None,
                    complete: None,
                    minor: None,
                }),
            },
        ))
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
        .with_bulk_workspace(self.workers, self.workspace)
    }

    /// Transfer frozen source/weight associations and buffer residency once.
    pub fn take_native_replay(&self) -> Option<BulkCubeReplay> {
        self.state.lock().ok()?.replay.take()
    }
    /// Transfer the reconciled major-cycle result once.
    pub fn take_completion(&self) -> Option<MajorCycleOperatorResult> {
        self.state.lock().ok()?.complete.take()
    }
    /// Transfer the shared CLEAN controller's result once.
    pub fn take_reconstruction_cycle_completion(
        &self,
    ) -> Option<ReconstructionCyclePhaseCompletion> {
        self.state.lock().ok()?.minor.take()
    }

    fn read(&self, state: &mut State, context: WorkExecutionContext<'_>) -> io::Result<()> {
        let deferred = state
            .selected
            .take()
            .ok_or_else(|| io::Error::other("bulk source already opened"))?;
        let selected = if let Some(artifact) = state.imported.take() {
            artifact
                .rebind_selected(deferred, &self.problem)
                .map_err(io::Error::other)?
        } else {
            deferred.open(&self.problem).map_err(io::Error::other)?
        };
        let model = state
            .model
            .as_ref()
            .ok_or_else(|| io::Error::other("bulk model not prepared"))?;
        if let Some(fragment) = &self.mfs {
            let mut operator = state
                .mfs_prepared
                .take()
                .ok_or_else(|| io::Error::other("bulk MFS FFT preparation missing"))?
                .begin_streaming(context, &self.problem, fragment)
                .map_err(io::Error::other)?;
            operator
                .bind_major_cycle_model(model, state.prior.take())
                .map_err(io::Error::other)?;
            let mut operator = state.weighting.traverse_bulk(
                context,
                &self.fragment(),
                &self.problem,
                selected,
                self.input,
                None,
                super::bulk_mfs::MfsConsumer(operator),
            )?;
            state
                .weighting
                .frozen_artifact()
                .ok_or_else(|| io::Error::other("bulk MFS weighting completion missing"))?
                .authorize_derived_operator(&mut operator)
                .map_err(io::Error::other)?;
            state.mfs_operator = Some(operator);
            return Ok(());
        }
        let products = &self.problem.selected_observation().read_set().sources()[0]
            .selection()
            .correlations()[0];
        let mut correlations = [CorrelationType::StokesI; 4];
        for (target, product) in correlations.iter_mut().zip(products.products()) {
            *target = product.correlation_type();
        }
        let polarization = PolarizationOperator::compile(
            self.problem.reconstruction().polarization().coordinates(),
            &correlations[..products.products().len()],
            [0.0; 2],
            MuellerMatrix::identity(),
        )
        .map_err(io::Error::other)?;
        let initial = self.pass.ordinal() == 0;
        let mut first = true;
        let mut selected = Some(selected);
        let mut start = 0;
        while start < state.bands.len() {
            let count = BulkWave::prefix(&state.bands[start..], self.wave_bytes)?;
            let jobs = state.bands[start..start + count]
                .iter()
                .cloned()
                .map(|b| {
                    if initial && first {
                        b.initial_source_axis(self.input.channels)
                    } else {
                        Ok(b)
                    }
                })
                .collect::<Result<Vec<_>, _>>()
                .map_err(io::Error::other)?;
            let channels = jobs
                .iter()
                .map(BandPlan::native_range)
                .filter(|r| !r.is_empty())
                .reduce(|a, b| a.start.min(b.start)..a.end.max(b.end));
            let wave = BulkWave::new(
                jobs,
                model.final_model(),
                &self.output_hz,
                &polarization,
                (initial && first).then_some(state.bands.as_mut_slice()),
            );
            let first_channels = if initial {
                None
            } else {
                Some(channels.clone().ok_or_else(|| {
                    io::Error::other("residual wave has no native support window")
                })?)
            };
            let results = if first {
                state.weighting.traverse_bulk(
                    context,
                    &self.fragment(),
                    &self.problem,
                    selected.take().expect("first bulk source"),
                    self.input,
                    first_channels,
                    wave,
                )?
            } else {
                state.weighting.traverse_bulk_next(
                    context,
                    &self.fragment(),
                    &self.problem,
                    self.input,
                    channels.clone().unwrap_or(0..self.input.channels),
                    wave,
                )?
            };
            if first {
                let (summary, generation, _) = state
                    .weighting
                    .pending_replay_inputs()
                    .ok_or_else(|| io::Error::other("bulk traversal has no pending completion"))?;
                if initial {
                    state.fold = Some(
                        PendingStreamingCubeFold::during_read(
                            context,
                            &self.reconcile,
                            summary,
                            generation,
                            self.cube_state
                                .as_ref()
                                .expect("cube backing")
                                .normal_storage()?,
                        )
                        .map_err(io::Error::other)?,
                    );
                } else {
                    state.refresh = Some(
                        PendingCubeRefresh::during_read(
                            context,
                            &self.reconcile,
                            &SpectralOperatorSpecification::new(&self.problem)
                                .map_err(io::Error::other)?,
                            state.prior.as_ref().ok_or_else(|| {
                                io::Error::other("bulk refresh lacks prior normal")
                            })?,
                            model.final_model().generation_id(),
                            summary,
                            generation,
                            &self
                                .cube_state
                                .as_ref()
                                .expect("cube backing")
                                .normal_storage()?,
                        )
                        .map_err(io::Error::other)?,
                    );
                }
            }
            for result in results {
                match result {
                    BandResult::Initial(normal) => {
                        let core = normal.slab().core_range();
                        state
                            .fold
                            .as_mut()
                            .ok_or_else(|| io::Error::other("unexpected initial band"))?
                            .append(
                                &SpectralOperatorSpecification::for_slab(
                                    &self.problem,
                                    core.start,
                                    core.len(),
                                )
                                .map_err(io::Error::other)?,
                                normal,
                            )
                            .map_err(io::Error::other)?;
                    }
                    BandResult::Residual(residual) => state
                        .refresh
                        .as_mut()
                        .ok_or_else(|| io::Error::other("unexpected residual band"))?
                        .append(residual)
                        .map_err(io::Error::other)?,
                }
            }
            eprintln!(
                "bulk_cube_wave ordinal={} first_band={start} bands={count} workers={} native_window={channels:?} source={:?} stream={:?}",
                self.pass.ordinal(),
                self.workers,
                state.weighting.latest_traversal_measurements(),
                state.weighting.latest_stream_measurements()
            );
            start += count;
            first = false;
        }
        Ok(())
    }
}

impl WorkImplementation for BulkCubePhase {
    type Error = io::Error;
    fn implementation_id(&self) -> &WorkImplementationId {
        &self.implementation
    }
    fn execute(&self, context: WorkExecutionContext<'_>) -> io::Result<WorkMeasurements> {
        let started = std::time::Instant::now();
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("bulk state poisoned"))?;
        if context.node().id == self.prepare {
            let executable = ExecutableModelProblem::from_compiled(self.problem.clone())
                .map_err(io::Error::other)?;
            let attempt = ModelExecutionAttemptId::new(LogicalIdentity::from_sha256(
                context.attempt_id().as_bytes(),
            ));
            let storage = match &self.cube_state {
                Some(backing) => backing.model_storage(context)?,
                None => casa_imaging_reconstruction::ModelStoragePlan::resident(usize::MAX)
                    .map_err(io::Error::other)?,
            };
            let (lifecycle, named, terms) = if let Some(input) = state.input.take() {
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
                let lifecycle =
                    ModelLifecycle::bind(executable, attempt, context.lease_epoch(), storage)
                        .map_err(io::Error::other)?;
                let named = lifecycle.initial_empty().map_err(io::Error::other)?;
                (lifecycle, named, None)
            };
            let delta = terms
                .filter(|t| !t.is_empty())
                .map(|t| {
                    lifecycle
                        .compile_delta(&named, t.iter().copied())
                        .map_err(io::Error::other)
                })
                .transpose()?;
            state.model = Some(
                MajorCyclePreparation::prepare(&lifecycle, named, delta)
                    .map_err(io::Error::other)?,
            );
            state.lifecycle = Some(lifecycle);
        } else if let Some(fragment) = self
            .mfs
            .as_ref()
            .filter(|f| *f.preparation_node() == context.node().id)
        {
            state.mfs_prepared = Some(fragment.prepare(context).map_err(io::Error::other)?);
        } else if context.node().id == self.read {
            self.read(&mut state, context)?;
        } else if context.node().id == self.reconcile {
            let state = &mut *state;
            let replay = state
                .weighting
                .replay_completion()
                .ok_or_else(|| io::Error::other("bulk source fence has not completed"))?;
            let complete = if let Some(operator) = state.mfs_operator.take() {
                operator.complete(
                    replay,
                    &casa_imaging_reconstruction::runtime_adapter::NormalStoragePlan::resident(1)
                        .map_err(io::Error::other)?,
                )
            } else if let Some(refresh) = state.refresh.take() {
                refresh.complete_rebound(replay)
            } else {
                state
                    .fold
                    .take()
                    .ok_or_else(|| io::Error::other("bulk initial fold missing"))?
                    .complete(replay)
            }
            .map_err(io::Error::other)?;
            let model = state
                .model
                .take()
                .ok_or_else(|| io::Error::other("bulk model missing"))?;
            if let Some(prior) = state.prior.take() {
                prior.retire_obsolete().map_err(io::Error::other)?;
            }
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
                            .ok_or_else(|| io::Error::other("bulk lifecycle missing"))?,
                    )
                    .map_err(io::Error::other)?,
            );
        } else if let Some((_, masks, program)) =
            self.minor.as_ref().filter(|m| m.0 == context.node().id)
        {
            let complete = state
                .complete
                .take()
                .ok_or_else(|| io::Error::other("bulk CLEAN lacks normal state"))?;
            let mut measurements = None;
            state.minor = Some(
                complete.run_reconstruction_cycle(
                    state
                        .lifecycle
                        .as_ref()
                        .ok_or_else(|| io::Error::other("bulk CLEAN lacks lifecycle"))?,
                    masks,
                    program.clone(),
                    context,
                    self.pass.ordinal(),
                    &mut measurements,
                )?,
            );
        } else if context.node().id == *self.fragment().release_node() {
            if !context.is_cleanup() {
                let mut artifact = state
                    .weighting
                    .frozen_artifact()
                    .ok_or_else(|| io::Error::other("bulk weighting not frozen"))?;
                if let Some(reservation) = &self.frozen_reservation {
                    artifact = artifact
                        .with_cross_plan_reservation(reservation.clone())
                        .map_err(io::Error::other)?;
                }
                state.replay = Some(BulkCubeReplay {
                    bands: std::mem::take(&mut state.bands),
                    weighting: artifact,
                    managed: self
                        .cube_state
                        .as_ref()
                        .and_then(CubeStatePlan::managed_run),
                });
            }
            state
                .weighting
                .release(context, &self.fragment())
                .map_err(io::Error::other)?;
        }
        if let Some(cube_state) = &self.cube_state {
            cube_state.log_measurements(&context.node().id);
        }
        eprintln!(
            "bulk_cube_stage ordinal={} node={} elapsed_nanos={}",
            self.pass.ordinal(),
            context.node().id.as_str(),
            started.elapsed().as_nanos()
        );
        Ok(WorkMeasurements::new(
            context
                .node()
                .claims
                .iter()
                .map(|c| ResourceMeasurement::new(c.resource.clone(), c.lifetime.clone(), c.amount))
                .collect(),
            context
                .stage_prediction()
                .io()
                .iter()
                .map(|p| IoMeasurement::new(p.kind(), p.bytes(), p.operations()))
                .collect(),
            self.phase_input_artifact
                .filter(|_| context.node().id == self.prepare)
                .map(|(identity, bytes)| {
                    ArtifactMeasurement::new(
                        identity,
                        Some(identity),
                        ArtifactDisposition::Loaded,
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
        _: WorkExecutionContext<'_>,
        _: FenceKind,
    ) -> io::Result<WorkMeasurements> {
        Ok(WorkMeasurements::default())
    }
    fn complete_observation_read(
        &self,
        context: ObservationReadCompletionContext,
    ) -> io::Result<AttemptBoundObservationCompletion> {
        self.state
            .lock()
            .unwrap()
            .weighting
            .complete_replay(context)
            .map_err(io::Error::other)
    }
    fn publish(&self, context: WorkExecutionContext<'_>) -> io::Result<()> {
        let state = self.state.lock().unwrap();
        if context.node().kind != WorkKind::Publication
            || context.publication().is_none()
            || state.complete.is_none() && state.minor.is_none()
        {
            return Err(io::Error::other("bulk publication is not ready"));
        }
        Ok(())
    }
    fn retain_artifact_resources(
        &self,
        owner: &WorkNodeId,
        permit: RetainedArtifactPermit,
    ) -> io::Result<bool> {
        if let Some(cube_state) = self.cube_state.as_ref().filter(|s| s.retains_at(owner)) {
            cube_state.retain(owner, permit)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }
    fn abort_node_io(&self, owner: &WorkNodeId) -> io::Result<()> {
        if owner == &self.read {
            let mut state = self.state.lock().unwrap();
            state.fold = None;
            state.refresh = None;
        }
        Ok(())
    }
}
