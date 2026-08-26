// SPDX-License-Identifier: LGPL-3.0-or-later

//! Affine cross-plan state for serial CPU continuum reconstruction.

use std::{
    io,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc::SyncSender},
    thread::JoinHandle,
};

use casa_imaging_model::{
    CompiledProblem, LogicalIdentity, ModelDeltaTerm, ModelExecutionAttemptId, ModelInputCommitment,
};
use casa_imaging_reconstruction::{
    ExecutableModelProblem, FinalModelCompletion, FinalModelContinuation, FinalNormalState,
    MajorCyclePreparation, MinorCycleError, MinorCycleEvidence, MinorCycleProgram, ModelDeltaId,
    ModelLifecycle, ReconstructionMaskPlan, run_minor_cycle,
};

use crate::{
    AttemptBoundObservationCompletion, CompleteDataOperatorResult, CompleteDataPlanFragment,
    CompleteDataPreparedState, ContinuumPassIdentity, FenceKind, ImplementationContractMetadata,
    ImplementationRegistry, ImplementationRegistryId, IoMeasurement, LeaseResource,
    MajorCycleOperatorResult, MajorCycleOperatorState, ObservationReadCompletionContext,
    ResourceMeasurement, SelectedObservationSourceResources, SerialMfsOperatorState,
    WeightingExecutionState, WeightingPlanFragment, WeightingReplayCompletion,
    WorkExecutionContext, WorkImplementation, WorkImplementationId, WorkKind, WorkMeasurements,
};
use casa_imaging_reconstruction::WeightingPlan;
use casa_ms::{BoundSelectedObservation, ModelColumnTransaction, SelectedObservationCompletion};
use sha2::{Digest, Sha256};

pub(crate) const MODEL_COLUMN_WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;

/// Bounded consumer of final-model predictions produced inside the paired
/// final-major replay. Implementations may stage MODEL_DATA or residual-
/// visibility products, but receive no commit authority from this interface.
pub trait FinalVisibilitySink: Send {
    /// Bind the exact final model before any replay sample is consumed.
    fn bind(
        &mut self,
        problem: casa_imaging_model::CompiledProblemId,
        final_model: casa_imaging_reconstruction::ModelGenerationId,
    ) -> io::Result<()>;

    /// Start any private staging owned by the scheduled terminal replay.
    fn begin_staging(&mut self) -> io::Result<()>;

    /// Consume one bounded canonical selected-visibility block.
    fn consume(
        &mut self,
        samples: &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
    ) -> io::Result<()>;

    /// Close staging against the terminal selected/weighting replay proof.
    fn finish(&mut self, replay: &WeightingReplayCompletion) -> io::Result<()>;
}

/// Shared handle for the product-owned final-visibility staging completion.
#[derive(Clone)]
pub struct VisibilityProductStaging {
    state: Arc<Mutex<VisibilityProductStagingState>>,
    model_column: Option<Arc<ModelColumnStaging>>,
}

struct ModelColumnStaging {
    path: PathBuf,
    expected: casa_imaging_model::ObservationSourceState,
    selection: Arc<casa_imaging_model::ObservationSelection>,
    state: Mutex<ModelColumnStagingState>,
}

enum ModelColumnStagingState {
    Idle,
    Replaying(ModelColumnWorker),
    Staged(Box<StagedModelColumn>),
}

struct StagedModelColumn {
    transaction: ModelColumnTransaction,
    sample_count: u64,
    prepared: bool,
}

enum VisibilityProductStagingState {
    Unbound,
    Bound(casa_imaging_products::VisibilityProductAuthority),
    Finished(casa_imaging_products::VisibilityProductCompletion),
}

impl VisibilityProductStaging {
    /// Create empty staging and its runtime sink capability.
    #[must_use]
    pub fn new() -> (Self, Box<dyn FinalVisibilitySink>) {
        let state = Arc::new(Mutex::new(VisibilityProductStagingState::Unbound));
        (
            Self {
                state: Arc::clone(&state),
                model_column: None,
            },
            Box::new(VisibilityProductStaging {
                state,
                model_column: None,
            }),
        )
    }

    /// Create product staging paired with one storage-owner MODEL_DATA transaction.
    pub fn with_model_column(
        path: PathBuf,
        expected: casa_imaging_model::ObservationSourceState,
        selection: Arc<casa_imaging_model::ObservationSelection>,
    ) -> io::Result<(Self, Box<dyn FinalVisibilitySink>)> {
        let state = Arc::new(Mutex::new(VisibilityProductStagingState::Unbound));
        let model_column = Arc::new(ModelColumnStaging {
            path,
            expected,
            selection,
            state: Mutex::new(ModelColumnStagingState::Idle),
        });
        Ok((
            Self {
                state: Arc::clone(&state),
                model_column: Some(model_column.clone()),
            },
            Box::new(VisibilityProductStaging {
                state,
                model_column: Some(model_column),
            }),
        ))
    }

    /// Return the closed product completion after terminal replay.
    pub fn completion(&self) -> io::Result<casa_imaging_products::VisibilityProductCompletion> {
        let state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("visibility product staging poisoned"))?;
        match &*state {
            VisibilityProductStagingState::Finished(completion) => Ok(*completion),
            _ => Err(io::Error::other("visibility product staging is incomplete")),
        }
    }

    /// Return whether this staging handle owns a private `MODEL_DATA` transaction.
    #[must_use]
    pub const fn has_model_column(&self) -> bool {
        self.model_column.is_some()
    }

    /// Validate that private staging contains the terminal replay's exact sample set.
    pub fn prepare_model_column(&self, expected_samples: u64) -> io::Result<()> {
        let Some(staging) = &self.model_column else {
            return Err(io::Error::other("MODEL_DATA staging is not configured"));
        };
        let mut state = staging
            .state
            .lock()
            .map_err(|_| io::Error::other("MODEL_DATA staging state poisoned"))?;
        let ModelColumnStagingState::Staged(staged) = &mut *state else {
            return Err(io::Error::other(
                "MODEL_DATA staging worker did not finish inside terminal replay",
            ));
        };
        if staged.sample_count != expected_samples {
            return Err(io::Error::other(format!(
                "MODEL_DATA staged {} samples, expected {expected_samples}",
                staged.sample_count
            )));
        }
        staged.transaction.prepare().map_err(io::Error::other)?;
        staged.prepared = true;
        Ok(())
    }

    /// Publish staged MODEL_DATA after all conventional products are visible.
    pub fn commit_model_column(&self) -> io::Result<()> {
        let completion = self.completion()?;
        let Some(staging) = &self.model_column else {
            return Ok(());
        };
        let mut state = staging
            .state
            .lock()
            .map_err(|_| io::Error::other("MODEL_DATA staging state poisoned"))?;
        let staged = match std::mem::replace(&mut *state, ModelColumnStagingState::Idle) {
            ModelColumnStagingState::Staged(staged) if staged.prepared => *staged,
            other => {
                *state = other;
                return Err(io::Error::other(
                    "MODEL_DATA transaction was not prepared for publication",
                ));
            }
        };
        drop(state);
        staged
            .transaction
            .commit(completion.model_product().identity())
            .map_err(io::Error::other)
    }
}

impl FinalVisibilitySink for VisibilityProductStaging {
    fn bind(
        &mut self,
        problem: casa_imaging_model::CompiledProblemId,
        final_model: casa_imaging_reconstruction::ModelGenerationId,
    ) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("visibility product staging poisoned"))?;
        match &*state {
            VisibilityProductStagingState::Unbound => {
                *state = VisibilityProductStagingState::Bound(
                    casa_imaging_products::VisibilityProductAuthority::new(problem, final_model),
                );
                Ok(())
            }
            VisibilityProductStagingState::Bound(_) => Ok(()),
            VisibilityProductStagingState::Finished(completion)
                if completion.problem_id() == problem
                    && completion.final_model() == final_model =>
            {
                Ok(())
            }
            VisibilityProductStagingState::Finished(_) => Err(io::Error::other(
                "visibility products belong to another final model",
            )),
        }
    }

    fn begin_staging(&mut self) -> io::Result<()> {
        let Some(staging) = &self.model_column else {
            return Ok(());
        };
        let mut state = staging
            .state
            .lock()
            .map_err(|_| io::Error::other("MODEL_DATA staging state poisoned"))?;
        match &*state {
            ModelColumnStagingState::Idle => {
                *state = ModelColumnStagingState::Replaying(ModelColumnWorker::spawn(
                    staging.path.clone(),
                    staging.expected.clone(),
                    Arc::clone(&staging.selection),
                )?);
                Ok(())
            }
            ModelColumnStagingState::Replaying(_) => Ok(()),
            ModelColumnStagingState::Staged(_) => Err(io::Error::other(
                "MODEL_DATA staging replay already finished",
            )),
        }
    }

    fn consume(
        &mut self,
        samples: &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
    ) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("visibility product staging poisoned"))?;
        let VisibilityProductStagingState::Bound(authority) = &mut *state else {
            return Err(io::Error::other("visibility product staging is not bound"));
        };
        authority.consume(samples).map_err(io::Error::other)?;
        if let Some(staging) = &self.model_column {
            let state = staging
                .state
                .lock()
                .map_err(|_| io::Error::other("MODEL_DATA staging state poisoned"))?;
            let ModelColumnStagingState::Replaying(worker) = &*state else {
                return Err(io::Error::other("MODEL_DATA staging was not plan-prepared"));
            };
            worker.stage(samples)?;
        }
        Ok(())
    }

    fn finish(&mut self, replay: &WeightingReplayCompletion) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("visibility product staging poisoned"))?;
        let authority = match std::mem::replace(&mut *state, VisibilityProductStagingState::Unbound)
        {
            VisibilityProductStagingState::Bound(authority) => authority,
            other => {
                *state = other;
                return Err(io::Error::other("visibility product staging is not bound"));
            }
        };
        if let Some(staging) = &self.model_column {
            let mut staging_state = staging
                .state
                .lock()
                .map_err(|_| io::Error::other("MODEL_DATA staging state poisoned"))?;
            let worker = match std::mem::replace(&mut *staging_state, ModelColumnStagingState::Idle)
            {
                ModelColumnStagingState::Replaying(worker) => worker,
                other => {
                    *staging_state = other;
                    return Err(io::Error::other(
                        "MODEL_DATA staging worker was not active at replay completion",
                    ));
                }
            };
            drop(staging_state);
            let staged = worker.finish()?;
            *staging
                .state
                .lock()
                .map_err(|_| io::Error::other("MODEL_DATA staging state poisoned"))? =
                ModelColumnStagingState::Staged(Box::new(staged));
        }
        *state = VisibilityProductStagingState::Finished(
            authority.finish(replay.selected_generation(), replay.weighting_generation()),
        );
        Ok(())
    }
}

struct ModelColumnWorker {
    sender: SyncSender<ModelColumnCommand>,
    join: JoinHandle<io::Result<StagedModelColumn>>,
}

enum ModelColumnCommand {
    Stage {
        values: Vec<(u64, u32, u32, num_complex::Complex32)>,
        reply: SyncSender<Result<(), String>>,
    },
    Finish,
}

impl ModelColumnWorker {
    fn spawn(
        path: PathBuf,
        expected: casa_imaging_model::ObservationSourceState,
        selection: Arc<casa_imaging_model::ObservationSelection>,
    ) -> io::Result<Self> {
        // A rendezvous channel keeps exactly one copied replay block resident:
        // the scheduler-accounted producer cannot queue a second block while
        // the storage worker owns the first.
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(0);
        let join = std::thread::Builder::new()
            .name("model-data-staging".to_string())
            .stack_size(MODEL_COLUMN_WORKER_STACK_BYTES)
            .spawn(move || {
                let mut transaction =
                    match ModelColumnTransaction::begin(path, &expected, &selection) {
                        Ok(transaction) => {
                            let _ = ready_sender.send(Ok(()));
                            transaction
                        }
                        Err(error) => {
                            let message = error.to_string();
                            let _ = ready_sender.send(Err(message.clone()));
                            return Err(io::Error::other(message));
                        }
                    };
                let mut staged_samples = 0_u64;
                while let Ok(command) = receiver.recv() {
                    match command {
                        ModelColumnCommand::Stage { values, reply } => {
                            let result = (|| {
                                staged_samples =
                                    staged_samples.checked_add(values.len() as u64).ok_or_else(
                                        || io::Error::other("MODEL_DATA sample count overflowed"),
                                    )?;
                                for (row, channel, correlation, value) in values {
                                    transaction
                                        .stage(row, channel, correlation, value)
                                        .map_err(io::Error::other)?;
                                }
                                Ok::<(), io::Error>(())
                            })();
                            match result {
                                Ok(()) => {
                                    let _ = reply.send(Ok(()));
                                }
                                Err(error) => {
                                    let message = error.to_string();
                                    let _ = reply.send(Err(message.clone()));
                                    return Err(io::Error::other(message));
                                }
                            }
                        }
                        ModelColumnCommand::Finish => break,
                    }
                }
                Ok(StagedModelColumn {
                    transaction,
                    sample_count: staged_samples,
                    prepared: false,
                })
            })
            .map_err(io::Error::other)?;
        ready_receiver
            .recv()
            .map_err(|_| io::Error::other("MODEL_DATA worker stopped during startup"))?
            .map_err(io::Error::other)?;
        Ok(Self { sender, join })
    }

    fn stage(
        &self,
        samples: &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
    ) -> io::Result<()> {
        let values = samples
            .iter()
            .map(|sample| {
                let address = sample.address();
                let predicted = sample.predicted();
                (
                    address.physical_row,
                    address.channel_index,
                    address.correlation_index,
                    num_complex::Complex32::new(predicted.re as f32, predicted.im as f32),
                )
            })
            .collect();
        let (reply, response) = std::sync::mpsc::sync_channel(0);
        self.sender
            .send(ModelColumnCommand::Stage { values, reply })
            .map_err(|_| io::Error::other("MODEL_DATA staging worker stopped"))?;
        response
            .recv()
            .map_err(|_| io::Error::other("MODEL_DATA staging worker stopped"))?
            .map_err(io::Error::other)
    }

    fn finish(self) -> io::Result<StagedModelColumn> {
        self.sender
            .send(ModelColumnCommand::Finish)
            .map_err(|_| io::Error::other("MODEL_DATA staging worker stopped"))?;
        self.join
            .join()
            .map_err(|_| io::Error::other("MODEL_DATA staging worker panicked"))?
    }
}

/// Runtime-owned immutable registry for one serial CPU implementation bundle.
pub struct SerialContinuumRegistry<I> {
    id: ImplementationRegistryId,
    implementation_id: WorkImplementationId,
    metadata: ImplementationContractMetadata,
    implementation: I,
}

impl<I> SerialContinuumRegistry<I> {
    /// Bind one implementation and its exact compiled science contract.
    #[must_use]
    pub fn new(
        id: ImplementationRegistryId,
        implementation_id: WorkImplementationId,
        problem: &CompiledProblem,
        implementation: I,
    ) -> Self {
        Self {
            id,
            implementation_id,
            metadata: ImplementationContractMetadata::new(
                problem.problem_id(),
                problem.numerics_id(),
                problem.required_capabilities().clone(),
            ),
            implementation,
        }
    }

    /// Return the sole implementation identity owned by this bundle.
    #[must_use]
    pub const fn implementation_id(&self) -> &WorkImplementationId {
        &self.implementation_id
    }

    /// Borrow the stateful implementation owned by this registry.
    #[must_use]
    pub const fn implementation(&self) -> &I {
        &self.implementation
    }
}

impl<I: WorkImplementation> ImplementationRegistry for SerialContinuumRegistry<I> {
    type Implementation = I;

    fn registry_id(&self) -> ImplementationRegistryId {
        self.id
    }

    fn resolve(&self, id: &WorkImplementationId) -> Option<&Self::Implementation> {
        (id == &self.implementation_id).then_some(&self.implementation)
    }

    fn implementation_contract(
        &self,
        id: &WorkImplementationId,
    ) -> Option<ImplementationContractMetadata> {
        (id == &self.implementation_id).then(|| self.metadata.clone())
    }
}

/// Runtime-owned serial CPU implementation of one ordinary major-cycle plan.
pub struct SerialContinuumExecutor {
    id: WorkImplementationId,
    problem: CompiledProblem,
    weighting_plan: WeightingPlan,
    source_resources: SelectedObservationSourceResources,
    pass: ContinuumPassIdentity,
    complete_data: CompleteDataPlanFragment,
    minor_cycle: Option<SerialMinorCycleExecution>,
    final_visibility_sink: Option<Mutex<Box<dyn FinalVisibilitySink>>>,
    phase_input_artifact: Option<(crate::ArtifactIdentity, u64)>,
    state: Mutex<SerialContinuumExecutorState>,
}

struct SerialContinuumExecutorState {
    executable: Option<ExecutableModelProblem>,
    pass_input: Option<SerialContinuumPassInput>,
    selected: Option<BoundSelectedObservation>,
    selected_completion: Option<SelectedObservationCompletion>,
    weighting: WeightingExecutionState,
    prepared: Option<CompleteDataPreparedState>,
    operator: Option<SerialMfsOperatorState>,
    complete_data: Option<CompleteDataOperatorResult>,
    lifecycle: Option<ModelLifecycle>,
    preparation: Option<MajorCyclePreparation>,
    result: Option<MajorCycleOperatorResult>,
    minor_completion: Option<MinorCyclePhaseCompletion>,
}

/// Closed model input admitted by one ordinary serial continuum pass.
pub enum SerialContinuumPassInput {
    /// Derive the exact initial generation from the compiled input commitment.
    Initial,
    /// Rebind one accepted T21 update into the final-major execution authority.
    FinalMajor(FinalMajorPhaseInput),
}

/// Owner-independent affine update carried into a separately scheduled final major pass.
pub struct FinalMajorPhaseInput {
    terms: Box<[ModelDeltaTerm]>,
    source_delta: Option<ModelDeltaId>,
    evidence: Box<MinorCyclePhaseEvidence>,
}

impl FinalMajorPhaseInput {
    /// Return the owner-independent accepted-update identity bound into planning.
    #[must_use]
    pub fn identity(&self) -> crate::ArtifactIdentity {
        let mut hash = Sha256::new();
        hash.update(b"casa-rs-serial-continuum-final-major-input-v1");
        hash.update(self.evidence.minor_cycle.evidence_id().as_bytes());
        match self.source_delta {
            Some(delta) => {
                hash.update([1]);
                hash.update(delta.as_bytes());
            }
            None => hash.update([0]),
        }
        crate::ArtifactIdentity::from_sha256(hash.finalize().into())
    }

    /// Return the source-attempt delta identity, when T21 accepted components.
    #[must_use]
    pub const fn source_delta(&self) -> Option<ModelDeltaId> {
        self.source_delta
    }

    /// Return the authoritative T20/T21 handoff evidence.
    #[must_use]
    pub const fn evidence(&self) -> &MinorCyclePhaseEvidence {
        &self.evidence
    }

    fn into_execution_parts(self) -> (Box<[ModelDeltaTerm]>, FinalModelContinuation) {
        (self.terms, self.evidence.continuation)
    }
}

struct SerialMinorCycleExecution {
    node: crate::WorkNodeId,
    mask: ReconstructionMaskPlan,
    program: MinorCycleProgram,
}

impl SerialContinuumExecutor {
    /// Bind exact selected-observation and model owners to a composed pass.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        id: WorkImplementationId,
        problem: CompiledProblem,
        weighting_plan: WeightingPlan,
        source_resources: SelectedObservationSourceResources,
        pass: ContinuumPassIdentity,
        complete_data: CompleteDataPlanFragment,
        selected: BoundSelectedObservation,
        executable: ExecutableModelProblem,
        pass_input: SerialContinuumPassInput,
    ) -> Self {
        let phase_input_artifact = match &pass_input {
            SerialContinuumPassInput::Initial => None,
            SerialContinuumPassInput::FinalMajor(input) => Some((
                input.identity(),
                u64::try_from(input.terms.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<ModelDeltaTerm>() as u64),
            )),
        };
        Self {
            id,
            problem,
            weighting_plan,
            source_resources,
            pass,
            complete_data,
            minor_cycle: None,
            final_visibility_sink: None,
            phase_input_artifact,
            state: Mutex::new(SerialContinuumExecutorState {
                executable: Some(executable),
                pass_input: Some(pass_input),
                selected: Some(selected),
                selected_completion: None,
                weighting: WeightingExecutionState::new(),
                prepared: None,
                operator: None,
                complete_data: None,
                lifecycle: None,
                preparation: None,
                result: None,
                minor_completion: None,
            }),
        }
    }

    /// Attach the resource-accounted T21 work owned by an initial-major plan.
    #[must_use]
    pub fn with_minor_cycle(
        mut self,
        node: crate::WorkNodeId,
        mask: ReconstructionMaskPlan,
        program: MinorCycleProgram,
    ) -> Self {
        self.minor_cycle = Some(SerialMinorCycleExecution {
            node,
            mask,
            program,
        });
        self
    }

    /// Attach bounded final-prediction staging to a final-major pass.
    #[must_use]
    pub fn with_final_visibility_sink(mut self, sink: Box<dyn FinalVisibilitySink>) -> Self {
        self.final_visibility_sink = Some(Mutex::new(sink));
        self
    }

    fn fragment(&self) -> WeightingPlanFragment<'_> {
        WeightingPlanFragment::new_for_pass(
            &self.weighting_plan,
            crate::serial_continuum_plan::pass_node("transaction-read", self.pass),
            self.source_resources.clone(),
            self.id.clone(),
            self.id.clone(),
            self.id.clone(),
            self.pass,
        )
    }

    /// Consume the authoritative phase result after run success.
    pub fn take_completion(&self) -> Option<MajorCycleOperatorResult> {
        self.state.lock().ok()?.result.take()
    }

    /// Consume the accepted T21 completion after initial-plan success.
    pub fn take_minor_completion(&self) -> Option<MinorCyclePhaseCompletion> {
        self.state.lock().ok()?.minor_completion.take()
    }

    fn initialize_model(
        state: &mut SerialContinuumExecutorState,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), io::Error> {
        if state.lifecycle.is_some() {
            return Ok(());
        }
        let executable = state
            .executable
            .take()
            .ok_or_else(|| io::Error::other("executable model input missing"))?;
        let input = state
            .pass_input
            .take()
            .ok_or_else(|| io::Error::other("serial continuum pass input missing"))?;
        let attempt = ModelExecutionAttemptId::new(LogicalIdentity::from_sha256(
            context.attempt_id().as_bytes(),
        ));
        let epoch = context.lease_epoch();
        let (lifecycle, named, terms) = match input {
            SerialContinuumPassInput::Initial => {
                let mut lifecycle =
                    ModelLifecycle::bind(executable, attempt, epoch).map_err(io::Error::other)?;
                let named = match lifecycle.contract().input() {
                    ModelInputCommitment::Empty => lifecycle.initial_empty(),
                    ModelInputCommitment::ReprojectedSeed(_) => lifecycle.initial_reprojected(),
                    ModelInputCommitment::AlignedSeed { .. }
                    | ModelInputCommitment::Generation(_) => {
                        return Err(io::Error::other(
                            "serial continuum execution requires an owner-prepared direct model input",
                        ));
                    }
                }
                .map_err(io::Error::other)?;
                (lifecycle, named, None)
            }
            SerialContinuumPassInput::FinalMajor(input) => {
                let (terms, continuation) = input.into_execution_parts();
                let (lifecycle, named) =
                    ModelLifecycle::continue_from(executable, attempt, epoch, continuation)
                        .map_err(io::Error::other)?;
                (lifecycle, named, Some(terms))
            }
        };
        let delta = match terms {
            Some(terms) if !terms.is_empty() => Some(
                lifecycle
                    .compile_delta(&named, terms.iter().copied())
                    .map_err(io::Error::other)?,
            ),
            Some(_) | None => None,
        };
        state.preparation = Some(
            MajorCyclePreparation::prepare(&lifecycle, named, delta).map_err(io::Error::other)?,
        );
        state.lifecycle = Some(lifecycle);
        Ok(())
    }
}

impl WorkImplementation for SerialContinuumExecutor {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let fragment = self.fragment();
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("serial continuum state poisoned"))?;
        Self::initialize_model(&mut state, context)?;
        if let (Some(sink), Some(preparation)) =
            (&self.final_visibility_sink, state.preparation.as_ref())
        {
            sink.lock()
                .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                .bind(
                    self.problem.problem_id(),
                    preparation.final_model().generation_id(),
                )?;
        }
        if context.node().id == *self.complete_data.preparation_node() {
            state.prepared = Some(
                self.complete_data
                    .prepare(context)
                    .map_err(io::Error::other)?,
            );
        } else if context.node().id == *fragment.source_read_node() {
            let selected = state
                .selected
                .take()
                .ok_or_else(|| io::Error::other("selected observation already consumed"))?;
            state.selected_completion = Some(
                state
                    .weighting
                    .traverse_and_retain_source(context, &fragment, selected, &self.problem, |_| {
                        Ok::<_, io::Error>(())
                    })
                    .map_err(io::Error::other)?,
            );
        } else if context.node().id == *fragment.generation_node() {
            state
                .weighting
                .traverse_generation(context, &fragment, &self.problem)
                .map_err(io::Error::other)?;
        } else if context.node().id == *fragment.replay_node() {
            if let Some(sink) = &self.final_visibility_sink {
                sink.lock()
                    .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                    .begin_staging()?;
            }
            let prepared = state
                .prepared
                .take()
                .ok_or_else(|| io::Error::other("FFT preparation did not run"))?;
            let mut operator = state
                .weighting
                .begin_complete_data(context, &self.complete_data, &self.problem, prepared)
                .map_err(io::Error::other)?;
            operator
                .bind_major_cycle_model(
                    state
                        .preparation
                        .as_ref()
                        .ok_or_else(|| io::Error::other("major-cycle preparation missing"))?,
                )
                .map_err(io::Error::other)?;
            state.operator = Some(operator);
            let SerialContinuumExecutorState {
                weighting,
                operator,
                ..
            } = &mut *state;
            weighting
                .traverse_replay(context, &fragment, &self.problem, |block| {
                    let predicted = operator
                        .as_mut()
                        .ok_or_else(|| io::Error::other("complete-data operator missing"))?
                        .consume_weighted_block(block)
                        .map_err(io::Error::other)?;
                    if !predicted.is_empty()
                        && let Some(sink) = &self.final_visibility_sink
                    {
                        sink.lock()
                            .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                            .consume(predicted)?;
                    }
                    Ok::<(), io::Error>(())
                })
                .map_err(io::Error::other)?;
        } else if self
            .complete_data
            .reconciliation_node()
            .is_some_and(|node| context.node().id == *node)
        {
            let complete = state
                .complete_data
                .take()
                .ok_or_else(|| io::Error::other("complete-data evidence missing"))?;
            let preparation = state
                .preparation
                .take()
                .ok_or_else(|| io::Error::other("major-cycle preparation missing"))?;
            let owner =
                MajorCycleOperatorState::begin(complete, preparation).map_err(io::Error::other)?;
            let mut lifecycle = state
                .lifecycle
                .take()
                .ok_or_else(|| io::Error::other("model lifecycle missing"))?;
            state.result = Some(
                owner
                    .reconcile(context, &mut lifecycle)
                    .map_err(io::Error::other)?,
            );
            state.lifecycle = Some(lifecycle);
        } else if self
            .minor_cycle
            .as_ref()
            .is_some_and(|minor| context.node().id == minor.node)
        {
            let minor = self.minor_cycle.as_ref().expect("minor-cycle node matched");
            let result = state
                .result
                .take()
                .ok_or_else(|| io::Error::other("initial major completion missing"))?;
            let lifecycle = state
                .lifecycle
                .as_ref()
                .ok_or_else(|| io::Error::other("model lifecycle missing"))?;
            state.minor_completion = Some(
                InitialMajorPhaseCompletion::new(result)
                    .run_minor_cycle(lifecycle, &minor.mask, minor.program.clone())
                    .map_err(io::Error::other)?,
            );
        } else if context.node().id == *fragment.release_node() {
            state
                .weighting
                .release(context, &fragment)
                .map_err(io::Error::other)?;
        }
        let resources = context
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
            .collect();
        let io = context
            .node()
            .claims
            .iter()
            .filter_map(|claim| match claim.resource {
                LeaseResource::IoBuffer(kind) => Some(IoMeasurement::unobserved(kind)),
                _ => None,
            })
            .collect();
        let artifacts = self
            .phase_input_artifact
            .filter(|_| {
                self.complete_data
                    .reconciliation_node()
                    .is_some_and(|node| context.node().id == *node)
            })
            .map(|(identity, bytes)| {
                crate::ArtifactMeasurement::new(
                    identity,
                    Some(identity),
                    crate::ArtifactDisposition::Loaded,
                    bytes,
                    None,
                )
                .expect("Loaded input evidence is implementation-owned")
            })
            .into_iter()
            .collect();
        Ok(WorkMeasurements::new(resources, io, artifacts))
    }

    fn failure_measurements<'error>(
        &'error self,
        _error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        None
    }

    fn wait_for_fence(
        &self,
        _context: WorkExecutionContext<'_>,
        _fence: FenceKind,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn complete_observation_read(
        &self,
        completion: ObservationReadCompletionContext,
    ) -> Result<AttemptBoundObservationCompletion, Self::Error> {
        let fragment = self.fragment();
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("serial continuum state poisoned"))?;
        if completion.owner_node() == fragment.generation_node() {
            return state
                .weighting
                .complete_generation(completion)
                .map_err(io::Error::other);
        }
        if completion.owner_node() == fragment.replay_node() {
            let predecessor = state
                .weighting
                .complete_replay(completion)
                .map_err(io::Error::other)?;
            let operator = state
                .operator
                .take()
                .ok_or_else(|| io::Error::other("complete-data operator missing"))?;
            let replay = state
                .weighting
                .replay_completion()
                .ok_or_else(|| io::Error::other("replay completion missing"))?;
            if let Some(sink) = &self.final_visibility_sink {
                sink.lock()
                    .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                    .finish(replay)?;
            }
            state.complete_data = Some(operator.complete(replay).map_err(io::Error::other)?);
            return Ok(predecessor);
        }
        let selected = state
            .selected_completion
            .take()
            .ok_or_else(|| io::Error::other("selected-observation completion missing"))?;
        completion.bind(selected).map_err(io::Error::other)
    }

    fn publish(&self, context: WorkExecutionContext<'_>) -> Result<(), Self::Error> {
        if context.node().kind != WorkKind::Publication || context.publication().is_none() {
            return Err(io::Error::other(
                "serial continuum commit lacks transaction authority",
            ));
        }
        Ok(())
    }
}

/// Affine authoritative completion of the ordinary initial-major plan.
pub struct InitialMajorPhaseCompletion {
    result: MajorCycleOperatorResult,
}

impl InitialMajorPhaseCompletion {
    /// Adopt one successful initial-major result.
    #[must_use]
    pub const fn new(result: MajorCycleOperatorResult) -> Self {
        Self { result }
    }

    /// Run the resource-admitted T21 solve and retain its authoritative evidence.
    pub fn run_minor_cycle(
        self,
        lifecycle: &ModelLifecycle,
        mask_plan: &ReconstructionMaskPlan,
        program: MinorCycleProgram,
    ) -> Result<MinorCyclePhaseCompletion, MinorCycleError> {
        let completion = self.result.into_completion();
        let (normal_state, continuation) = completion.into_continuation();
        let (mask, auto_mask) = mask_plan.materialize(continuation.generation(), &normal_state)?;
        let minor = run_minor_cycle(
            lifecycle,
            continuation.generation(),
            &normal_state,
            &mask,
            program,
        )?;
        let (delta, evidence) = minor.into_parts();
        Ok(MinorCyclePhaseCompletion {
            normal_state,
            continuation,
            mask,
            delta,
            evidence,
            auto_mask,
        })
    }
}

/// Typed in-memory result carried from T21 into the ordinary final-major plan.
pub struct MinorCyclePhaseCompletion {
    normal_state: FinalNormalState,
    continuation: FinalModelContinuation,
    mask: casa_imaging_reconstruction::ReconstructionMask,
    delta: Option<casa_imaging_reconstruction::ModelDelta>,
    evidence: MinorCycleEvidence,
    auto_mask: Option<casa_imaging_reconstruction::AutoMultithreshEvidence>,
}

impl MinorCyclePhaseCompletion {
    /// Return the owner-minted T21 evidence.
    #[must_use]
    pub const fn evidence(&self) -> &MinorCycleEvidence {
        &self.evidence
    }

    /// Return auto-multithreshold diagnostics when that mask mode was selected.
    #[must_use]
    pub const fn auto_mask_evidence(
        &self,
    ) -> Option<casa_imaging_reconstruction::AutoMultithreshEvidence> {
        self.auto_mask
    }

    /// Return the immutable mask generation used for component placement.
    #[must_use]
    pub const fn mask(&self) -> &casa_imaging_reconstruction::ReconstructionMask {
        &self.mask
    }

    /// Consume the accepted model update into mandatory final-major preparation.
    pub fn into_final_major_input(self) -> FinalMajorPhaseInput {
        let source_delta = self.delta.as_ref().map(|delta| delta.delta_id());
        let terms = self
            .delta
            .as_ref()
            .map_or_else(Vec::new, |delta| delta.terms().to_vec());
        FinalMajorPhaseInput {
            terms: terms.into_boxed_slice(),
            source_delta,
            evidence: Box::new(MinorCyclePhaseEvidence {
                normal_state: self.normal_state,
                continuation: self.continuation,
                minor_cycle: self.evidence,
            }),
        }
    }
}

/// Authoritative initial-normal, initial-model, and T21 evidence retained in memory.
pub struct MinorCyclePhaseEvidence {
    normal_state: FinalNormalState,
    continuation: FinalModelContinuation,
    minor_cycle: MinorCycleEvidence,
}

impl MinorCyclePhaseEvidence {
    /// Return the initial authoritative normal state.
    #[must_use]
    pub const fn normal_state(&self) -> &FinalNormalState {
        &self.normal_state
    }

    /// Return the initial authoritative model completion.
    #[must_use]
    pub const fn initial_model_completion(&self) -> &FinalModelCompletion {
        self.continuation.completion()
    }

    /// Return the exact completed model generation retained for final-major input.
    #[must_use]
    pub const fn initial_model(&self) -> &casa_imaging_reconstruction::ModelGeneration {
        self.continuation.generation()
    }

    /// Return the accepted minor-cycle evidence.
    #[must_use]
    pub const fn minor_cycle(&self) -> &MinorCycleEvidence {
        &self.minor_cycle
    }
}
