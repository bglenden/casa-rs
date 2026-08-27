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
    CompleteDataPreparedState, FenceKind, FrozenWeightingArtifact, ImplementationContractMetadata,
    ImplementationRegistry, ImplementationRegistryId, IoMeasurement, LeaseResource,
    MajorCycleOperatorResult, MajorCycleOperatorState, ObservationReadCompletionContext,
    ResourceMeasurement, SelectedObservationSourceResources, SpectralOperatorState,
    SpectralPassIdentity, WeightingExecutionState, WeightingPlanFragment,
    WeightingReplayCompletion, WorkDependency, WorkExecutionContext, WorkImplementation,
    WorkImplementationId, WorkKind, WorkMeasurements, WorkNodeId,
};
use casa_imaging_reconstruction::WeightingPlan;
use casa_ms::{BoundSelectedObservation, ModelDataWrite, SelectedObservationCompletion};
use sha2::{Digest, Sha256};

pub(crate) const MODEL_COLUMN_WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;

/// Bounded consumer of final-model predictions produced inside the paired
/// final-major replay. Implementations may write selected `MODEL_DATA` cells
/// in place or retain residual-visibility products, but receive no publication
/// authority from this interface.
pub trait FinalVisibilitySink: Send {
    /// Bind the exact final model before any replay sample is consumed.
    fn bind(
        &mut self,
        problem: casa_imaging_model::CompiledProblemId,
        final_model: casa_imaging_reconstruction::ModelGenerationId,
    ) -> io::Result<()>;

    /// Start the one scheduled terminal replay.
    fn begin_replay(&mut self) -> io::Result<()>;

    /// Consume one bounded canonical selected-visibility block.
    fn consume(
        &mut self,
        samples: &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
    ) -> io::Result<()>;

    /// Close the stream against the terminal selected/weighting replay proof.
    fn finish(&mut self, replay: &WeightingReplayCompletion) -> io::Result<()>;
}

/// Shared handle for final-visibility product completion and optional in-place
/// `MODEL_DATA` replay.
#[derive(Clone)]
pub struct FinalVisibilityReplay {
    state: Arc<Mutex<FinalVisibilityReplayState>>,
    model_column: Option<Arc<ModelColumnWriteBinding>>,
}

struct ModelColumnWriteBinding {
    path: PathBuf,
    expected: casa_imaging_model::ObservationSourceState,
    selection: Arc<casa_imaging_model::ObservationSelection>,
    state: Mutex<ModelColumnWriteState>,
}

enum ModelColumnWriteState {
    Idle,
    Writing(ModelColumnWorker),
    Complete,
}

enum FinalVisibilityReplayState {
    Unbound,
    Bound(casa_imaging_products::VisibilityProductAuthority),
    Finished(casa_imaging_products::VisibilityProductCompletion),
}

impl FinalVisibilityReplay {
    /// Create an empty product stream and its runtime sink capability.
    #[must_use]
    pub fn new() -> (Self, Box<dyn FinalVisibilitySink>) {
        let state = Arc::new(Mutex::new(FinalVisibilityReplayState::Unbound));
        (
            Self {
                state: Arc::clone(&state),
                model_column: None,
            },
            Box::new(FinalVisibilityReplay {
                state,
                model_column: None,
            }),
        )
    }

    /// Create a product stream paired with one storage-owner MODEL_DATA writeback.
    pub fn with_model_column(
        path: PathBuf,
        expected: casa_imaging_model::ObservationSourceState,
        selection: Arc<casa_imaging_model::ObservationSelection>,
    ) -> io::Result<(Self, Box<dyn FinalVisibilitySink>)> {
        let state = Arc::new(Mutex::new(FinalVisibilityReplayState::Unbound));
        let model_column = Arc::new(ModelColumnWriteBinding {
            path,
            expected,
            selection,
            state: Mutex::new(ModelColumnWriteState::Idle),
        });
        Ok((
            Self {
                state: Arc::clone(&state),
                model_column: Some(model_column.clone()),
            },
            Box::new(FinalVisibilityReplay {
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
            .map_err(|_| io::Error::other("final-visibility replay poisoned"))?;
        match &*state {
            FinalVisibilityReplayState::Finished(completion) => Ok(*completion),
            _ => Err(io::Error::other("final-visibility replay is incomplete")),
        }
    }

    /// Return whether this handle owns a bounded in-place `MODEL_DATA` write.
    #[must_use]
    pub const fn has_model_column(&self) -> bool {
        self.model_column.is_some()
    }
}

impl FinalVisibilitySink for FinalVisibilityReplay {
    fn bind(
        &mut self,
        problem: casa_imaging_model::CompiledProblemId,
        final_model: casa_imaging_reconstruction::ModelGenerationId,
    ) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("final-visibility replay poisoned"))?;
        match &*state {
            FinalVisibilityReplayState::Unbound => {
                *state = FinalVisibilityReplayState::Bound(
                    casa_imaging_products::VisibilityProductAuthority::new(problem, final_model),
                );
                Ok(())
            }
            FinalVisibilityReplayState::Bound(_) => Ok(()),
            FinalVisibilityReplayState::Finished(completion)
                if completion.problem_id() == problem
                    && completion.final_model() == final_model =>
            {
                Ok(())
            }
            FinalVisibilityReplayState::Finished(_) => Err(io::Error::other(
                "visibility products belong to another final model",
            )),
        }
    }

    fn begin_replay(&mut self) -> io::Result<()> {
        let Some(column) = &self.model_column else {
            return Ok(());
        };
        let mut state = column
            .state
            .lock()
            .map_err(|_| io::Error::other("MODEL_DATA write state poisoned"))?;
        match &*state {
            ModelColumnWriteState::Idle => {
                *state = ModelColumnWriteState::Writing(ModelColumnWorker::spawn(
                    column.path.clone(),
                    column.expected.clone(),
                    Arc::clone(&column.selection),
                )?);
                Ok(())
            }
            ModelColumnWriteState::Writing(_) => Ok(()),
            ModelColumnWriteState::Complete => {
                Err(io::Error::other("MODEL_DATA write already finished"))
            }
        }
    }

    fn consume(
        &mut self,
        samples: &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
    ) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("final-visibility replay poisoned"))?;
        let FinalVisibilityReplayState::Bound(authority) = &mut *state else {
            return Err(io::Error::other("final-visibility replay is not bound"));
        };
        authority.consume(samples).map_err(io::Error::other)?;
        if let Some(column) = &self.model_column {
            let state = column
                .state
                .lock()
                .map_err(|_| io::Error::other("MODEL_DATA write state poisoned"))?;
            let ModelColumnWriteState::Writing(worker) = &*state else {
                return Err(io::Error::other("MODEL_DATA writer was not plan-prepared"));
            };
            worker.write(samples)?;
        }
        Ok(())
    }

    fn finish(&mut self, replay: &WeightingReplayCompletion) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("final-visibility replay poisoned"))?;
        let authority = match std::mem::replace(&mut *state, FinalVisibilityReplayState::Unbound) {
            FinalVisibilityReplayState::Bound(authority) => authority,
            other => {
                *state = other;
                return Err(io::Error::other("final-visibility replay is not bound"));
            }
        };
        let completion =
            authority.finish(replay.selected_generation(), replay.weighting_generation());
        if let Some(model_column) = &self.model_column {
            let mut write_state = model_column
                .state
                .lock()
                .map_err(|_| io::Error::other("MODEL_DATA write state poisoned"))?;
            let worker = match std::mem::replace(&mut *write_state, ModelColumnWriteState::Idle) {
                ModelColumnWriteState::Writing(worker) => worker,
                other => {
                    *write_state = other;
                    return Err(io::Error::other(
                        "MODEL_DATA writer was not active at replay completion",
                    ));
                }
            };
            drop(write_state);
            worker.finish(
                completion.sample_count(),
                completion.model_product().identity(),
            )?;
            *model_column
                .state
                .lock()
                .map_err(|_| io::Error::other("MODEL_DATA write state poisoned"))? =
                ModelColumnWriteState::Complete;
        }
        *state = FinalVisibilityReplayState::Finished(completion);
        Ok(())
    }
}

struct ModelColumnWorker {
    sender: SyncSender<ModelColumnCommand>,
    join: JoinHandle<io::Result<u64>>,
}

pub(crate) struct ModelDataCellWrite {
    address: casa_imaging_model::SelectedSampleAddress,
    value: num_complex::Complex32,
}

struct SelectedModelDataCoverage {
    selection: Arc<casa_imaging_model::ObservationSelection>,
    row: usize,
    channel: usize,
    correlation: usize,
    written: u64,
}

struct ExpectedModelDataAddress {
    physical_row: u64,
    data_description_id: u32,
    spectral_window_id: u32,
    polarization_id: u32,
    channel_index: u32,
    correlation_index: u32,
}

impl SelectedModelDataCoverage {
    fn new(selection: Arc<casa_imaging_model::ObservationSelection>) -> Self {
        Self {
            selection,
            row: 0,
            channel: 0,
            correlation: 0,
            written: 0,
        }
    }

    fn push(&mut self, address: casa_imaging_model::SelectedSampleAddress) -> io::Result<()> {
        let Some(expected) = self.expected()? else {
            return Err(io::Error::other(
                "MODEL_DATA replay exceeded the exact selected write set",
            ));
        };
        if address.physical_row != expected.physical_row
            || u32::try_from(address.data_description_id).ok() != Some(expected.data_description_id)
            || address.spectral_window_id != expected.spectral_window_id
            || address.polarization_id != expected.polarization_id
            || address.channel_index != expected.channel_index
            || address.correlation_index != expected.correlation_index
        {
            return Err(io::Error::other(format!(
                "MODEL_DATA replay address ({}, {}, {}) did not match selected address ({}, {}, {})",
                address.physical_row,
                address.channel_index,
                address.correlation_index,
                expected.physical_row,
                expected.channel_index,
                expected.correlation_index,
            )));
        }
        self.written = self
            .written
            .checked_add(1)
            .ok_or_else(|| io::Error::other("MODEL_DATA sample count overflowed"))?;
        self.advance()?;
        Ok(())
    }

    fn finish(&self, expected_samples: u64) -> io::Result<()> {
        if self.written != expected_samples || self.expected()?.is_some() {
            return Err(io::Error::other(format!(
                "MODEL_DATA wrote {} samples without exhausting the exact selected write set of {expected_samples}",
                self.written
            )));
        }
        Ok(())
    }

    fn expected(&self) -> io::Result<Option<ExpectedModelDataAddress>> {
        let Some(row) = self.selection.rows().ordered_main_rows().get(self.row) else {
            return Ok(None);
        };
        let data_description = self
            .selection
            .data_descriptions()
            .iter()
            .find(|selection| selection.data_description_id() == row.data_description_id())
            .ok_or_else(|| io::Error::other("selected MODEL_DATA DDID is absent"))?;
        let spectral_window = self
            .selection
            .spectral_windows()
            .iter()
            .find(|selection| {
                selection.spectral_window_id() == data_description.spectral_window_id()
            })
            .ok_or_else(|| io::Error::other("selected MODEL_DATA SPW is absent"))?;
        let correlations = self
            .selection
            .correlations()
            .iter()
            .find(|selection| selection.polarization_id() == data_description.polarization_id())
            .ok_or_else(|| io::Error::other("selected MODEL_DATA polarization is absent"))?;
        let channel = spectral_window
            .channel_indices()
            .get(self.channel)
            .ok_or_else(|| io::Error::other("selected MODEL_DATA channel is absent"))?;
        let correlation = correlations
            .products()
            .get(self.correlation)
            .ok_or_else(|| io::Error::other("selected MODEL_DATA correlation is absent"))?;
        Ok(Some(ExpectedModelDataAddress {
            physical_row: row.physical_row(),
            data_description_id: data_description.data_description_id(),
            spectral_window_id: data_description.spectral_window_id(),
            polarization_id: data_description.polarization_id(),
            channel_index: *channel,
            correlation_index: correlation.correlation_index(),
        }))
    }

    fn advance(&mut self) -> io::Result<()> {
        let row = self
            .selection
            .rows()
            .ordered_main_rows()
            .get(self.row)
            .ok_or_else(|| io::Error::other("selected MODEL_DATA row is absent"))?;
        let data_description = self
            .selection
            .data_descriptions()
            .iter()
            .find(|selection| selection.data_description_id() == row.data_description_id())
            .ok_or_else(|| io::Error::other("selected MODEL_DATA DDID is absent"))?;
        let spectral_window = self
            .selection
            .spectral_windows()
            .iter()
            .find(|selection| {
                selection.spectral_window_id() == data_description.spectral_window_id()
            })
            .ok_or_else(|| io::Error::other("selected MODEL_DATA SPW is absent"))?;
        let correlations = self
            .selection
            .correlations()
            .iter()
            .find(|selection| selection.polarization_id() == data_description.polarization_id())
            .ok_or_else(|| io::Error::other("selected MODEL_DATA polarization is absent"))?;
        self.correlation += 1;
        if self.correlation == correlations.products().len() {
            self.correlation = 0;
            self.channel += 1;
            if self.channel == spectral_window.channel_indices().len() {
                self.channel = 0;
                self.row += 1;
            }
        }
        Ok(())
    }
}

enum ModelColumnCommand {
    Write {
        values: Vec<ModelDataCellWrite>,
        reply: SyncSender<Result<(), String>>,
    },
    Finish {
        expected_samples: u64,
        generation: LogicalIdentity,
    },
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
            .name("model-data-write".to_string())
            .stack_size(MODEL_COLUMN_WORKER_STACK_BYTES)
            .spawn(move || {
                let mut coverage = SelectedModelDataCoverage::new(Arc::clone(&selection));
                let mut writer = match ModelDataWrite::begin(path, &expected, &selection) {
                    Ok(writer) => {
                        let _ = ready_sender.send(Ok(()));
                        writer
                    }
                    Err(error) => {
                        let message = error.to_string();
                        let _ = ready_sender.send(Err(message.clone()));
                        return Err(io::Error::other(message));
                    }
                };
                let mut written_samples = 0_u64;
                let completed_samples = loop {
                    let command = receiver
                        .recv()
                        .map_err(|_| io::Error::other("MODEL_DATA write controller stopped"))?;
                    match command {
                        ModelColumnCommand::Write { values, reply } => {
                            let result = (|| {
                                written_samples = written_samples
                                    .checked_add(values.len() as u64)
                                    .ok_or_else(|| {
                                        io::Error::other("MODEL_DATA sample count overflowed")
                                    })?;
                                for cell in values {
                                    coverage.push(cell.address)?;
                                    writer
                                        .write(
                                            cell.address.physical_row,
                                            cell.address.channel_index,
                                            cell.address.correlation_index,
                                            cell.value,
                                        )
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
                        ModelColumnCommand::Finish {
                            expected_samples,
                            generation,
                        } => {
                            coverage.finish(expected_samples)?;
                            writer.complete(generation).map_err(io::Error::other)?;
                            break written_samples;
                        }
                    }
                };
                Ok(completed_samples)
            })
            .map_err(io::Error::other)?;
        ready_receiver
            .recv()
            .map_err(|_| io::Error::other("MODEL_DATA worker stopped during startup"))?
            .map_err(io::Error::other)?;
        Ok(Self { sender, join })
    }

    fn write(
        &self,
        samples: &[casa_imaging_reconstruction::runtime_adapter::FinalVisibilitySample],
    ) -> io::Result<()> {
        let values = samples
            .iter()
            .map(|sample| {
                let address = sample.address();
                let predicted = sample.predicted();
                ModelDataCellWrite {
                    address,
                    value: num_complex::Complex32::new(predicted.re as f32, predicted.im as f32),
                }
            })
            .collect();
        let (reply, response) = std::sync::mpsc::sync_channel(0);
        self.sender
            .send(ModelColumnCommand::Write { values, reply })
            .map_err(|_| io::Error::other("MODEL_DATA writer stopped"))?;
        response
            .recv()
            .map_err(|_| io::Error::other("MODEL_DATA writer stopped"))?
            .map_err(io::Error::other)
    }

    fn finish(self, expected_samples: u64, generation: LogicalIdentity) -> io::Result<u64> {
        self.sender
            .send(ModelColumnCommand::Finish {
                expected_samples,
                generation,
            })
            .map_err(|_| io::Error::other("MODEL_DATA writer stopped"))?;
        self.join
            .join()
            .map_err(|_| io::Error::other("MODEL_DATA writer panicked"))?
    }
}

/// Runtime-owned immutable registry for one serial CPU implementation bundle.
pub struct SpectralCycleRegistry<I> {
    id: ImplementationRegistryId,
    implementation_id: WorkImplementationId,
    metadata: ImplementationContractMetadata,
    implementation: I,
}

impl<I> SpectralCycleRegistry<I> {
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

impl<I: WorkImplementation> ImplementationRegistry for SpectralCycleRegistry<I> {
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
pub struct SpectralCycleExecutor {
    id: WorkImplementationId,
    problem: CompiledProblem,
    weighting_plan: WeightingPlan,
    source_resources: SelectedObservationSourceResources,
    pass: SpectralPassIdentity,
    complete_data: CompleteDataPlanFragment,
    minor_cycle: Option<SerialMinorCycleExecution>,
    final_visibility_sink: Option<Mutex<Box<dyn FinalVisibilitySink>>>,
    phase_input_artifact: Option<(crate::ArtifactIdentity, u64)>,
    state: Mutex<SpectralCycleExecutorState>,
}

struct SpectralCycleExecutorState {
    executable: Option<ExecutableModelProblem>,
    pass_input: Option<SpectralCyclePassInput>,
    selected: Option<BoundSelectedObservation>,
    selected_completion: Option<SelectedObservationCompletion>,
    weighting: WeightingExecutionState,
    pending_frozen_reservation: Option<Arc<crate::FrozenWeightingReservation>>,
    frozen_weighting: Option<FrozenWeightingArtifact>,
    prepared: Option<CompleteDataPreparedState>,
    operator: Option<SpectralOperatorState>,
    complete_data: Option<CompleteDataOperatorResult>,
    lifecycle: Option<ModelLifecycle>,
    prepared_model: Option<PreparedFinalModel>,
    result: Option<MajorCycleOperatorResult>,
    minor_completion: Option<MinorCyclePhaseCompletion>,
}

/// One immutable final-model candidate bound to the exact plan node, attempt,
/// and lease epoch that prepared it.
struct PreparedFinalModel {
    owner_node: WorkNodeId,
    attempt: crate::ExecutionAttemptId,
    lease_epoch: u64,
    preparation: MajorCyclePreparation,
}

impl PreparedFinalModel {
    fn new(context: WorkExecutionContext<'_>, preparation: MajorCyclePreparation) -> Self {
        Self {
            owner_node: context.node().id.clone(),
            attempt: context.attempt_id(),
            lease_epoch: context.lease_epoch(),
            preparation,
        }
    }

    fn for_replay(
        &self,
        context: WorkExecutionContext<'_>,
    ) -> Result<&MajorCyclePreparation, io::Error> {
        let direct_predecessor = context
            .node()
            .dependencies
            .contains(&WorkDependency::Work(self.owner_node.clone()));
        if !direct_predecessor
            || context.attempt_id() != self.attempt
            || context.lease_epoch() != self.lease_epoch
        {
            return Err(io::Error::other(
                "terminal replay is not bound to its final-model preparation capability",
            ));
        }
        Ok(&self.preparation)
    }

    fn into_reconciliation(
        self,
        context: WorkExecutionContext<'_>,
    ) -> Result<MajorCyclePreparation, io::Error> {
        if context.attempt_id() != self.attempt || context.lease_epoch() != self.lease_epoch {
            return Err(io::Error::other(
                "post-replay reconciliation changed final-model preparation authority",
            ));
        }
        Ok(self.preparation)
    }
}

/// Closed model input admitted by one ordinary spectral cycle pass.
pub enum SpectralCyclePassInput {
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
        hash.update(b"casa-rs-spectral-cycle-final-major-input-v1");
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

impl SpectralCycleExecutor {
    /// Bind exact selected-observation and model owners to a composed pass.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        id: WorkImplementationId,
        problem: CompiledProblem,
        weighting_plan: WeightingPlan,
        source_resources: SelectedObservationSourceResources,
        pass: SpectralPassIdentity,
        complete_data: CompleteDataPlanFragment,
        selected: BoundSelectedObservation,
        executable: ExecutableModelProblem,
        pass_input: SpectralCyclePassInput,
    ) -> Self {
        let phase_input_artifact = match &pass_input {
            SpectralCyclePassInput::Initial => None,
            SpectralCyclePassInput::FinalMajor(input) => Some((
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
            state: Mutex::new(SpectralCycleExecutorState {
                executable: Some(executable),
                pass_input: Some(pass_input),
                selected: Some(selected),
                selected_completion: None,
                weighting: WeightingExecutionState::new(),
                pending_frozen_reservation: None,
                frozen_weighting: None,
                prepared: None,
                operator: None,
                complete_data: None,
                lifecycle: None,
                prepared_model: None,
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

    /// Attach bounded final-prediction replay output to a final-major pass.
    #[must_use]
    pub fn with_final_visibility_sink(mut self, sink: Box<dyn FinalVisibilitySink>) -> Self {
        self.final_visibility_sink = Some(Mutex::new(sink));
        self
    }

    /// Attach immutable weighting produced by the initial major to a later pass.
    #[must_use]
    pub fn with_frozen_weighting(mut self, artifact: FrozenWeightingArtifact) -> Self {
        self.state
            .get_mut()
            .expect("new spectral cycle executor mutex is not poisoned")
            .weighting = WeightingExecutionState::with_frozen_artifact(artifact);
        self
    }

    /// Attach the Resource Authority reservation spanning later major plans.
    #[must_use]
    pub fn with_frozen_weighting_reservation(
        mut self,
        reservation: crate::FrozenWeightingReservation,
    ) -> Self {
        self.state
            .get_mut()
            .expect("new spectral cycle executor mutex is not poisoned")
            .pending_frozen_reservation = Some(Arc::new(reservation));
        self
    }

    /// Consume immutable weighting retained after a successful major pass.
    pub fn take_frozen_weighting(&self) -> Option<FrozenWeightingArtifact> {
        self.state.lock().ok()?.frozen_weighting.take()
    }

    fn fragment(&self) -> WeightingPlanFragment<'_> {
        let mode = match self.pass.phase() {
            crate::SpectralPassPhase::FinalMajor => crate::WeightingStreamingMode::Reuse,
            crate::SpectralPassPhase::InitialMajor => match self.problem.weighting().scheme() {
                casa_imaging_model::WeightingScheme::Natural => {
                    crate::WeightingStreamingMode::NaturalInitial
                }
                casa_imaging_model::WeightingScheme::Uniform
                | casa_imaging_model::WeightingScheme::Briggs { .. }
                | casa_imaging_model::WeightingScheme::BriggsBandwidthTaper { .. } => {
                    crate::WeightingStreamingMode::DensityInitial
                }
            },
        };
        WeightingPlanFragment::streaming_for_pass(
            &self.weighting_plan,
            crate::spectral_cycle_plan::pass_node("transaction-read", self.pass),
            self.source_resources.clone(),
            self.id.clone(),
            self.pass,
            mode,
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

    fn prepare_final_model(
        state: &mut SpectralCycleExecutorState,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), io::Error> {
        if state.lifecycle.is_some() || state.prepared_model.is_some() {
            return Err(io::Error::other(
                "final-model preparation executed more than once",
            ));
        }
        let executable = state
            .executable
            .take()
            .ok_or_else(|| io::Error::other("executable model input missing"))?;
        let input = state
            .pass_input
            .take()
            .ok_or_else(|| io::Error::other("spectral cycle pass input missing"))?;
        let attempt = ModelExecutionAttemptId::new(LogicalIdentity::from_sha256(
            context.attempt_id().as_bytes(),
        ));
        let epoch = context.lease_epoch();
        let (lifecycle, named, terms) = match input {
            SpectralCyclePassInput::Initial => {
                let mut lifecycle =
                    ModelLifecycle::bind(executable, attempt, epoch).map_err(io::Error::other)?;
                let named = match lifecycle.contract().input() {
                    ModelInputCommitment::Empty => lifecycle.initial_empty(),
                    ModelInputCommitment::ReprojectedSeed(_) => lifecycle.initial_reprojected(),
                    ModelInputCommitment::AlignedSeed { .. }
                    | ModelInputCommitment::Generation(_) => {
                        return Err(io::Error::other(
                            "spectral cycle execution requires an owner-prepared direct model input",
                        ));
                    }
                }
                .map_err(io::Error::other)?;
                (lifecycle, named, None)
            }
            SpectralCyclePassInput::FinalMajor(input) => {
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
        let preparation =
            MajorCyclePreparation::prepare(&lifecycle, named, delta).map_err(io::Error::other)?;
        state.prepared_model = Some(PreparedFinalModel::new(context, preparation));
        state.lifecycle = Some(lifecycle);
        Ok(())
    }

    fn run_stream(
        &self,
        state: &mut SpectralCycleExecutorState,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        selected: Option<BoundSelectedObservation>,
    ) -> Result<(), io::Error> {
        if let Some(sink) = &self.final_visibility_sink {
            sink.lock()
                .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                .begin_replay()?;
        }
        let prepared = state
            .prepared
            .take()
            .ok_or_else(|| io::Error::other("FFT preparation did not run"))?;
        let mut operator = prepared
            .begin_streaming(context, &self.problem, &self.complete_data)
            .map_err(io::Error::other)?;
        operator
            .bind_major_cycle_model(
                state
                    .prepared_model
                    .as_ref()
                    .ok_or_else(|| io::Error::other("final-model preparation missing"))?
                    .for_replay(context)?,
            )
            .map_err(io::Error::other)?;
        state.operator = Some(operator);
        let SpectralCycleExecutorState {
            weighting,
            operator,
            ..
        } = state;
        let mut consume = |block: &casa_imaging_reconstruction::WeightingReplayChunk| {
            let predicted = operator
                .as_mut()
                .ok_or_else(|| io::Error::other("complete-data operator missing"))?
                .consume_streaming_block(block)
                .map_err(io::Error::other)?;
            if !predicted.is_empty()
                && let Some(sink) = &self.final_visibility_sink
            {
                sink.lock()
                    .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                    .consume(predicted)?;
            }
            Ok::<(), io::Error>(())
        };
        match fragment.streaming_mode() {
            Some(crate::WeightingStreamingMode::NaturalInitial)
            | Some(crate::WeightingStreamingMode::DensityInitial) => weighting
                .traverse_initial_stream(context, fragment, &self.problem, selected, &mut consume)
                .map_err(io::Error::other),
            Some(crate::WeightingStreamingMode::Reuse) => weighting
                .traverse_reuse_stream(
                    context,
                    fragment,
                    selected.ok_or_else(|| {
                        io::Error::other("later-major selected observation missing")
                    })?,
                    &self.problem,
                    &mut consume,
                )
                .map_err(io::Error::other),
            None => Err(io::Error::other("streaming weighting mode missing")),
        }
    }
}

impl WorkImplementation for SpectralCycleExecutor {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let fragment = self.fragment();
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("spectral cycle state poisoned"))?;
        let final_model_preparation =
            crate::spectral_cycle_plan::pass_node("final-model-preparation", self.pass);
        if context.node().id == final_model_preparation {
            Self::prepare_final_model(&mut state, context)?;
            if let (Some(sink), Some(prepared_model)) =
                (&self.final_visibility_sink, state.prepared_model.as_ref())
            {
                sink.lock()
                    .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                    .bind(
                        self.problem.problem_id(),
                        prepared_model.preparation.final_model().generation_id(),
                    )?;
            }
        } else if context.node().id == *self.complete_data.preparation_node() {
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
            match fragment.streaming_mode() {
                Some(crate::WeightingStreamingMode::NaturalInitial) => {
                    self.run_stream(&mut state, context, &fragment, Some(selected))?;
                }
                Some(crate::WeightingStreamingMode::DensityInitial) => {
                    state.selected_completion = Some(
                        state
                            .weighting
                            .traverse_density_source(context, &fragment, selected, &self.problem)
                            .map_err(io::Error::other)?,
                    );
                }
                Some(crate::WeightingStreamingMode::Reuse) => {
                    self.run_stream(&mut state, context, &fragment, Some(selected))?;
                }
                None => return Err(io::Error::other("streaming weighting mode missing")),
            }
        } else if context.node().id == *fragment.generation_node()
            && fragment.streaming_mode() == Some(crate::WeightingStreamingMode::DensityInitial)
        {
            self.run_stream(&mut state, context, &fragment, None)?;
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
                .prepared_model
                .take()
                .ok_or_else(|| io::Error::other("final-model preparation missing"))?
                .into_reconciliation(context)?;
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
            .filter(|_| context.node().id == final_model_preparation)
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
            .map_err(|_| io::Error::other("spectral cycle state poisoned"))?;
        if completion.owner_node() == fragment.streaming_node() {
            let predecessor = state
                .weighting
                .complete_replay(completion)
                .map_err(io::Error::other)?;
            let operator = state
                .operator
                .take()
                .ok_or_else(|| io::Error::other("complete-data operator missing"))?;
            let frozen_weighting = state.weighting.frozen_artifact().and_then(|artifact| {
                if let Some(reservation) = state.pending_frozen_reservation.take() {
                    Some(artifact.with_cross_plan_reservation(reservation))
                } else {
                    artifact.has_cross_plan_reservation().then_some(artifact)
                }
            });
            let replay = state
                .weighting
                .replay_completion()
                .ok_or_else(|| io::Error::other("replay completion missing"))?;
            if let Some(sink) = &self.final_visibility_sink {
                sink.lock()
                    .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                    .finish(replay)?;
            }
            let complete_data = operator.complete(replay).map_err(io::Error::other)?;
            state.frozen_weighting = frozen_weighting;
            state.complete_data = Some(complete_data);
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
                "spectral cycle commit lacks transaction authority",
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
