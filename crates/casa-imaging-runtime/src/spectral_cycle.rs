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
    ChannelCyclePolicy, ExecutableModelProblem, FinalModelCompletion, FinalModelContinuation,
    FinalNormalState, MajorCyclePreparation, MinorCycleProgram, ModelDeltaId, ModelLifecycle,
    ReconstructionCycle, ReconstructionCycleError, ReconstructionCycleEvidence,
    ReconstructionMaskPlan,
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
use casa_ms::{
    BoundSelectedObservation, SelectedObservationCompletion, SelectedVisibilityWrite,
    SelectedVisibilityWriteGenerations, SelectedVisibilityWriteTargets,
};
use sha2::{Digest, Sha256};

pub(crate) const VISIBILITY_WRITE_WORKER_STACK_BYTES: usize = 2 * 1024 * 1024;

/// Bounded consumer of final visibility samples produced inside the paired
/// terminal replay. Implementations may write selected visibility columns in
/// place or retain residual-visibility products, but receive no publication
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
/// selected-visibility writeback.
#[derive(Clone)]
pub struct FinalVisibilityReplay {
    state: Arc<Mutex<FinalVisibilityReplayState>>,
    visibility_write: Option<Arc<VisibilityWriteBinding>>,
}

struct VisibilityWriteBinding {
    path: PathBuf,
    expected: casa_imaging_model::ObservationSourceState,
    selection: Arc<casa_imaging_model::ObservationSelection>,
    targets: SelectedVisibilityWriteTargets,
    state: Mutex<VisibilityWriteState>,
}

enum VisibilityWriteState {
    Idle,
    Writing(VisibilityWriteWorker),
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
                visibility_write: None,
            },
            Box::new(FinalVisibilityReplay {
                state,
                visibility_write: None,
            }),
        )
    }

    /// Create a product stream paired with one storage-owner visibility writeback.
    pub fn with_visibility_write(
        path: PathBuf,
        expected: casa_imaging_model::ObservationSourceState,
        selection: Arc<casa_imaging_model::ObservationSelection>,
        targets: SelectedVisibilityWriteTargets,
    ) -> io::Result<(Self, Box<dyn FinalVisibilitySink>)> {
        let state = Arc::new(Mutex::new(FinalVisibilityReplayState::Unbound));
        let visibility_write = Arc::new(VisibilityWriteBinding {
            path,
            expected,
            selection,
            targets,
            state: Mutex::new(VisibilityWriteState::Idle),
        });
        Ok((
            Self {
                state: Arc::clone(&state),
                visibility_write: Some(visibility_write.clone()),
            },
            Box::new(FinalVisibilityReplay {
                state,
                visibility_write: Some(visibility_write),
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

    /// Return whether this handle writes `MODEL_DATA`.
    #[must_use]
    pub fn has_model_column(&self) -> bool {
        self.visibility_write
            .as_ref()
            .is_some_and(|write| write.targets.model_data())
    }

    /// Return whether this handle writes transformed observations to `CORRECTED_DATA`.
    #[must_use]
    pub fn has_corrected_data(&self) -> bool {
        self.visibility_write
            .as_ref()
            .is_some_and(|write| write.targets.corrected_data())
    }

    /// Return whether this handle owns any selected visibility writeback.
    #[must_use]
    pub fn has_visibility_write(&self) -> bool {
        self.visibility_write.is_some()
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
        let Some(write) = &self.visibility_write else {
            return Ok(());
        };
        let mut state = write
            .state
            .lock()
            .map_err(|_| io::Error::other("visibility write state poisoned"))?;
        match &*state {
            VisibilityWriteState::Idle => {
                *state = VisibilityWriteState::Writing(VisibilityWriteWorker::spawn(
                    write.path.clone(),
                    write.expected.clone(),
                    Arc::clone(&write.selection),
                    write.targets,
                )?);
                Ok(())
            }
            VisibilityWriteState::Writing(_) => Ok(()),
            VisibilityWriteState::Complete => {
                Err(io::Error::other("visibility write already finished"))
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
        if let Some(write) = &self.visibility_write {
            let state = write
                .state
                .lock()
                .map_err(|_| io::Error::other("visibility write state poisoned"))?;
            let VisibilityWriteState::Writing(worker) = &*state else {
                return Err(io::Error::other("visibility writer was not plan-prepared"));
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
        let completion = authority.finish(
            replay.selected_generation(),
            replay
                .continuum_transform()
                .map(|completion| completion.generation_id()),
            replay.weighting_generation(),
        );
        if let Some(transform) = replay.continuum_transform()
            && completion.sample_count() != transform.output_sample_count()
        {
            return Err(io::Error::other(format!(
                "final visibility emitted {} samples after the continuum transform admitted {} output-role samples and weighting retained {} spectral-support samples",
                completion.sample_count(),
                transform.output_sample_count(),
                replay.spectral_support_sample_count(),
            )));
        }
        if let Some(write) = &self.visibility_write {
            let mut write_state = write
                .state
                .lock()
                .map_err(|_| io::Error::other("visibility write state poisoned"))?;
            let worker = match std::mem::replace(&mut *write_state, VisibilityWriteState::Idle) {
                VisibilityWriteState::Writing(worker) => worker,
                other => {
                    *write_state = other;
                    return Err(io::Error::other(
                        "visibility writer was not active at replay completion",
                    ));
                }
            };
            drop(write_state);
            let corrected_data = write
                .targets
                .corrected_data()
                .then(|| {
                    replay
                        .continuum_transform()
                        .map(|transform| {
                            LogicalIdentity::from_sha256(transform.generation_id().as_bytes())
                        })
                        .ok_or_else(|| {
                            io::Error::other("CORRECTED_DATA write lacks continuum generation")
                        })
                })
                .transpose()?;
            worker.finish(
                completion.sample_count(),
                SelectedVisibilityWriteGenerations {
                    model_data: write
                        .targets
                        .model_data()
                        .then_some(completion.model_product().identity()),
                    corrected_data,
                },
            )?;
            *write
                .state
                .lock()
                .map_err(|_| io::Error::other("visibility write state poisoned"))? =
                VisibilityWriteState::Complete;
        }
        *state = FinalVisibilityReplayState::Finished(completion);
        Ok(())
    }
}

struct VisibilityWriteWorker {
    sender: SyncSender<VisibilityWriteCommand>,
    join: JoinHandle<io::Result<u64>>,
}

pub(crate) struct SelectedVisibilityCellWrite {
    address: casa_imaging_model::SelectedSampleAddress,
    predicted: num_complex::Complex32,
    observed: num_complex::Complex32,
}

struct SelectedVisibilityCoverage {
    selection: Arc<casa_imaging_model::ObservationSelection>,
    rows: Option<casa_imaging_model::SelectedRowsBuilder>,
    current_row: Option<(u64, u32)>,
    channel: usize,
    correlation: usize,
    written: u64,
}

impl SelectedVisibilityCoverage {
    fn new(selection: Arc<casa_imaging_model::ObservationSelection>) -> Self {
        Self {
            rows: Some(casa_imaging_model::SelectedRowsBuilder::new(
                selection.rows().source_row_count(),
            )),
            selection,
            current_row: None,
            channel: 0,
            correlation: 0,
            written: 0,
        }
    }

    fn push(&mut self, address: casa_imaging_model::SelectedSampleAddress) -> io::Result<()> {
        let data_description_id = u32::try_from(address.data_description_id)
            .map_err(|_| io::Error::other("visibility replay DDID is negative"))?;
        if self.current_row.is_none() {
            self.rows
                .as_mut()
                .expect("visibility row manifest remains active before finish")
                .push(casa_imaging_model::SelectedMainRow::new(
                    address.physical_row,
                    data_description_id,
                ))
                .map_err(io::Error::other)?;
            self.current_row = Some((address.physical_row, data_description_id));
        }
        if self.current_row != Some((address.physical_row, data_description_id)) {
            return Err(io::Error::other(
                "visibility replay changed physical row before completing its selected samples",
            ));
        }
        let data_description = self
            .selection
            .data_descriptions()
            .iter()
            .find(|selection| selection.data_description_id() == data_description_id)
            .ok_or_else(|| io::Error::other("selected visibility DDID is absent"))?;
        let spectral_window = self
            .selection
            .spectral_windows()
            .iter()
            .find(|selection| {
                selection.spectral_window_id() == data_description.spectral_window_id()
            })
            .ok_or_else(|| io::Error::other("selected visibility SPW is absent"))?;
        let correlations = self
            .selection
            .correlations()
            .iter()
            .find(|selection| selection.polarization_id() == data_description.polarization_id())
            .ok_or_else(|| io::Error::other("selected visibility polarization is absent"))?;
        let channel = spectral_window
            .channel_indices()
            .get(self.channel)
            .ok_or_else(|| io::Error::other("selected visibility channel is absent"))?;
        let correlation = correlations
            .products()
            .get(self.correlation)
            .ok_or_else(|| io::Error::other("selected visibility correlation is absent"))?;
        if address.spectral_window_id != data_description.spectral_window_id()
            || address.polarization_id != data_description.polarization_id()
            || address.channel_index != *channel
            || address.correlation_index != correlation.correlation_index()
        {
            return Err(io::Error::other(format!(
                "visibility replay address ({}, {}, {}) does not follow the selected coordinate order",
                address.physical_row, address.channel_index, address.correlation_index,
            )));
        }
        self.written = self
            .written
            .checked_add(1)
            .ok_or_else(|| io::Error::other("visibility write sample count overflowed"))?;
        self.correlation += 1;
        if self.correlation == correlations.products().len() {
            self.correlation = 0;
            self.channel += 1;
            if self.channel == spectral_window.channel_indices().len() {
                self.channel = 0;
                self.current_row = None;
            }
        }
        Ok(())
    }

    fn finish(&mut self, expected_samples: u64) -> io::Result<()> {
        let observed_rows = self
            .rows
            .take()
            .expect("visibility row manifest finishes once")
            .finish();
        if self.written != expected_samples
            || self.current_row.is_some()
            || observed_rows != *self.selection.rows()
        {
            return Err(io::Error::other(format!(
                "visibility writer wrote {} samples without exhausting the exact selected write set of {expected_samples}",
                self.written
            )));
        }
        Ok(())
    }
}

enum VisibilityWriteCommand {
    Write {
        values: Vec<SelectedVisibilityCellWrite>,
        reply: SyncSender<Result<(), String>>,
    },
    Finish {
        expected_samples: u64,
        generations: SelectedVisibilityWriteGenerations,
    },
}

impl VisibilityWriteWorker {
    fn spawn(
        path: PathBuf,
        expected: casa_imaging_model::ObservationSourceState,
        selection: Arc<casa_imaging_model::ObservationSelection>,
        targets: SelectedVisibilityWriteTargets,
    ) -> io::Result<Self> {
        // A rendezvous channel keeps exactly one copied replay block resident:
        // the scheduler-accounted producer cannot queue a second block while
        // the storage worker owns the first.
        let (sender, receiver) = std::sync::mpsc::sync_channel(0);
        let (ready_sender, ready_receiver) = std::sync::mpsc::sync_channel(0);
        let join = std::thread::Builder::new()
            .name("selected-visibility-write".to_string())
            .stack_size(VISIBILITY_WRITE_WORKER_STACK_BYTES)
            .spawn(move || {
                let mut coverage = SelectedVisibilityCoverage::new(Arc::clone(&selection));
                let mut writer =
                    match SelectedVisibilityWrite::begin(path, &expected, &selection, targets) {
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
                        .map_err(|_| io::Error::other("visibility write controller stopped"))?;
                    match command {
                        VisibilityWriteCommand::Write { values, reply } => {
                            let result = (|| {
                                written_samples = written_samples
                                    .checked_add(values.len() as u64)
                                    .ok_or_else(|| {
                                        io::Error::other("visibility write sample count overflowed")
                                    })?;
                                for cell in values {
                                    coverage.push(cell.address)?;
                                    if targets.model_data() {
                                        writer
                                            .write(
                                                casa_imaging_model::MsColumnKind::ModelData,
                                                cell.address.physical_row,
                                                cell.address.channel_index,
                                                cell.address.correlation_index,
                                                cell.predicted,
                                            )
                                            .map_err(io::Error::other)?;
                                    }
                                    if targets.corrected_data() {
                                        writer
                                            .write(
                                                casa_imaging_model::MsColumnKind::CorrectedData,
                                                cell.address.physical_row,
                                                cell.address.channel_index,
                                                cell.address.correlation_index,
                                                cell.observed,
                                            )
                                            .map_err(io::Error::other)?;
                                    }
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
                        VisibilityWriteCommand::Finish {
                            expected_samples,
                            generations,
                        } => {
                            coverage.finish(expected_samples)?;
                            writer.complete(generations).map_err(io::Error::other)?;
                            break written_samples;
                        }
                    }
                };
                Ok(completed_samples)
            })
            .map_err(io::Error::other)?;
        ready_receiver
            .recv()
            .map_err(|_| io::Error::other("visibility writer stopped during startup"))?
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
                let observed = sample.observed();
                SelectedVisibilityCellWrite {
                    address,
                    predicted: num_complex::Complex32::new(
                        predicted.re as f32,
                        predicted.im as f32,
                    ),
                    observed: num_complex::Complex32::new(observed.re as f32, observed.im as f32),
                }
            })
            .collect();
        let (reply, response) = std::sync::mpsc::sync_channel(0);
        self.sender
            .send(VisibilityWriteCommand::Write { values, reply })
            .map_err(|_| io::Error::other("visibility writer stopped"))?;
        response
            .recv()
            .map_err(|_| io::Error::other("visibility writer stopped"))?
            .map_err(io::Error::other)
    }

    fn finish(
        self,
        expected_samples: u64,
        generations: SelectedVisibilityWriteGenerations,
    ) -> io::Result<u64> {
        self.sender
            .send(VisibilityWriteCommand::Finish {
                expected_samples,
                generations,
            })
            .map_err(|_| io::Error::other("visibility writer stopped"))?;
        self.join
            .join()
            .map_err(|_| io::Error::other("visibility writer panicked"))?
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
    reconstruction_cycle: Option<SerialReconstructionCycleExecution>,
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
    reconstruction_cycle_completion: Option<ReconstructionCyclePhaseCompletion>,
}

#[derive(Debug)]
struct SpectralCycleNodeFailure {
    source: io::Error,
    measurements: WorkMeasurements,
}

impl std::fmt::Display for SpectralCycleNodeFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for SpectralCycleNodeFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// One immutable final-model candidate bound to the exact plan node, attempt,
/// and lease epoch that prepared it.
struct PreparedFinalModel {
    owner_node: WorkNodeId,
    attempt: crate::ExecutionAttemptId,
    lease_epoch: u64,
    preparation: MajorCyclePreparation,
    prior_normal_state: Option<FinalNormalState>,
}

impl PreparedFinalModel {
    fn new(
        context: WorkExecutionContext<'_>,
        preparation: MajorCyclePreparation,
        prior_normal_state: Option<FinalNormalState>,
    ) -> Self {
        Self {
            owner_node: context.node().id.clone(),
            attempt: context.attempt_id(),
            lease_epoch: context.lease_epoch(),
            preparation,
            prior_normal_state,
        }
    }

    fn take_for_replay(
        &mut self,
        context: WorkExecutionContext<'_>,
    ) -> Result<(&MajorCyclePreparation, Option<FinalNormalState>), io::Error> {
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
        let prior_normal_state = self.prior_normal_state.take();
        Ok((&self.preparation, prior_normal_state))
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
        if self.prior_normal_state.is_some() {
            return Err(io::Error::other(
                "post-replay reconciliation retained unconsumed prior normal state",
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
    evidence: Box<ReconstructionCyclePhaseEvidence>,
}

impl FinalMajorPhaseInput {
    /// Return the owner-independent accepted-update identity bound into planning.
    #[must_use]
    pub fn identity(&self) -> crate::ArtifactIdentity {
        let mut hash = Sha256::new();
        hash.update(b"casa-rs-spectral-cycle-final-major-input-v1");
        hash.update(self.evidence.reconstruction_cycle.evidence_id().as_bytes());
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
    pub const fn evidence(&self) -> &ReconstructionCyclePhaseEvidence {
        &self.evidence
    }

    fn into_execution_parts(
        self,
    ) -> (
        Box<[ModelDeltaTerm]>,
        FinalModelContinuation,
        FinalNormalState,
    ) {
        let ReconstructionCyclePhaseEvidence {
            normal_state,
            continuation,
            reconstruction_cycle: _,
        } = *self.evidence;
        (self.terms, continuation, normal_state)
    }
}

struct SerialReconstructionCycleExecution {
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
            reconstruction_cycle: None,
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
                reconstruction_cycle_completion: None,
            }),
        }
    }

    /// Attach one resource-accounted shared reconstruction cycle to an initial-major plan.
    #[must_use]
    pub fn with_reconstruction_cycle(
        mut self,
        node: crate::WorkNodeId,
        mask: ReconstructionMaskPlan,
        program: MinorCycleProgram,
    ) -> Self {
        self.reconstruction_cycle = Some(SerialReconstructionCycleExecution {
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
            crate::plan_continuum_transform_row(&self.problem)
                .expect("compiled transform row plan remains valid")
                .map(|plan| u64::try_from(plan.bytes()).expect("transform bytes fit u64")),
        )
    }

    /// Consume the authoritative phase result after run success.
    pub fn take_completion(&self) -> Option<MajorCycleOperatorResult> {
        self.state.lock().ok()?.result.take()
    }

    /// Consume the accepted T21 completion after initial-plan success.
    pub fn take_reconstruction_cycle_completion(
        &self,
    ) -> Option<ReconstructionCyclePhaseCompletion> {
        self.state
            .lock()
            .ok()?
            .reconstruction_cycle_completion
            .take()
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
        let (lifecycle, named, terms, prior_normal_state) = match input {
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
                (lifecycle, named, None, None)
            }
            SpectralCyclePassInput::FinalMajor(input) => {
                let (terms, continuation, prior_normal_state) = input.into_execution_parts();
                let (lifecycle, named) =
                    ModelLifecycle::continue_from(executable, attempt, epoch, continuation)
                        .map_err(io::Error::other)?;
                (lifecycle, named, Some(terms), Some(prior_normal_state))
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
        state.prepared_model = Some(PreparedFinalModel::new(
            context,
            preparation,
            prior_normal_state,
        ));
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
        if self.final_visibility_sink.is_some() {
            operator.enable_final_visibility_samples();
        }
        let (preparation, prior_normal_state) = state
            .prepared_model
            .as_mut()
            .ok_or_else(|| io::Error::other("final-model preparation missing"))?
            .take_for_replay(context)?;
        operator
            .bind_major_cycle_model(preparation, prior_normal_state)
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
                .consume_bounded_replay_chunk(block)
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
        let result = match fragment.streaming_mode() {
            Some(crate::WeightingStreamingMode::NaturalInitial)
            | Some(crate::WeightingStreamingMode::DensityInitial) => weighting
                .traverse_initial_bounded_stream(
                    context,
                    fragment,
                    &self.problem,
                    selected,
                    &mut consume,
                )
                .map_err(io::Error::other),
            Some(crate::WeightingStreamingMode::Reuse) => {
                let selected = selected
                    .ok_or_else(|| io::Error::other("later-major selected observation missing"))?;
                weighting
                    .traverse_reuse_bounded_stream(
                        context,
                        fragment,
                        selected,
                        &self.problem,
                        &mut consume,
                    )
                    .map_err(io::Error::other)
            }
            None => Err(io::Error::other("streaming weighting mode missing")),
        };
        if result.is_ok() {
            self.log_stream_measurements(weighting, "weighted-replay");
        }
        result
    }

    fn log_stream_measurements(&self, weighting: &WeightingExecutionState, stage: &str) {
        let (Some(traversal), Some(stream)) = (
            weighting.latest_traversal_measurements(),
            weighting.latest_stream_measurements(),
        ) else {
            eprintln!(
                "imaging_source_read_ahead_summary mode=bounded_spectral stage={stage} measurement_state=missing"
            );
            return;
        };
        let phase = match self.pass.phase() {
            crate::SpectralPassPhase::InitialMajor => "initial-major",
            crate::SpectralPassPhase::FinalMajor => "final-major",
        };
        let consumer_nanos = stream.execute_nanos.saturating_add(stream.commit_nanos);
        let read_bandwidth_mib_s = if traversal.source_read_nanos() == 0 {
            0.0
        } else {
            traversal.logical_output_bytes() as f64 * 1_000_000_000.0
                / traversal.source_read_nanos() as f64
                / (1024.0 * 1024.0)
        };
        let modeled_physical_read_bytes = traversal
            .modeled_physical_read_bytes()
            .map_or_else(|| "unavailable".to_owned(), |bytes| bytes.to_string());
        eprintln!(
            "imaging_source_read_ahead_summary mode=bounded_spectral stage={stage} phase={phase} ordinal={} enabled={} max_live_row_blocks={} queue_capacity={} live_row_block_high_water={} row_blocks={} pass_count={} stored_rows={} stored_samples={} selected_channel_runs={} streamed_samples={} source_bytes={} modeled_physical_read_bytes={} source_read_operations={} request_handoff_bytes={} selected_sample_handoff_bytes={} peak_consumer_scratch_current_bytes={} consumer_scratch_capacity_bytes={} allocated_storage_buffers={} reused_storage_buffers={} peak_live_current_bytes={} peak_live_capacity_bytes={} source_slots={} workers={} maximum_partitions_per_block={} planned_source_capacity_bytes={} ready_queue_high_water={} ready_queue_current_bytes_high_water={} ready_queue_capacity_bytes_high_water={} planned_kernel_window_capacity_bytes={} peak_kernel_window_capacity_bytes={} source_read_nanos={} source_fill_nanos={} source_arrangement_nanos={} stream_source_fill_nanos={} kernel_prepare_nanos={} kernel_execute_nanos={} kernel_commit_nanos={} producer_wait_nanos={} consumer_wait_nanos={} lease_return_nanos={} producer_consumer_overlap_nanos={} wall_nanos={} consumer_recv_blocked_ms={:.3} producer_send_blocked_ms={:.3} producer_consumer_overlap_ms={:.3} source_read_ms={:.3} source_route_ms={:.3} consumer_ms={:.3} source_prepare_ms={:.3} effective_read_bandwidth_mib_s={:.3}",
            self.pass.ordinal(),
            stream.source_slots > 1,
            stream.source_slots,
            stream.source_slots.saturating_sub(2),
            stream.peak_live_source_blocks,
            traversal.block_count(),
            traversal.source_pass_count(),
            traversal.stored_row_count(),
            traversal.stored_sample_count(),
            traversal.selected_channel_run_count(),
            traversal.selected_sample_count(),
            traversal.logical_output_bytes(),
            modeled_physical_read_bytes,
            traversal.source_read_operations(),
            traversal.request_handoff_bytes(),
            traversal.selected_sample_handoff_bytes(),
            traversal.peak_consumer_scratch_current_bytes(),
            traversal.consumer_scratch_capacity_bytes(),
            traversal.allocated_storage_buffers(),
            traversal.reused_storage_buffers(),
            stream.peak_live_source_current_bytes,
            stream.peak_live_source_capacity_bytes,
            stream.source_slots,
            stream.workers,
            stream.maximum_partitions_per_block,
            stream.planned_source_capacity_bytes,
            stream.ready_queue_high_water,
            stream.ready_queue_current_bytes_high_water,
            stream.ready_queue_capacity_bytes_high_water,
            stream.planned_kernel_window_capacity_bytes,
            stream.peak_kernel_window_capacity_bytes,
            traversal.source_read_nanos(),
            traversal.source_fill_nanos(),
            traversal.source_arrangement_nanos(),
            stream.source_fill_nanos,
            stream.prepare_nanos,
            stream.execute_nanos,
            stream.commit_nanos,
            stream.producer_wait_nanos,
            stream.consumer_wait_nanos,
            stream.lease_return_nanos,
            stream.overlap_nanos,
            stream.wall_nanos,
            stream.consumer_wait_nanos as f64 / 1_000_000.0,
            stream.producer_wait_nanos as f64 / 1_000_000.0,
            stream.overlap_nanos as f64 / 1_000_000.0,
            traversal.source_read_nanos() as f64 / 1_000_000.0,
            traversal.source_arrangement_nanos() as f64 / 1_000_000.0,
            consumer_nanos as f64 / 1_000_000.0,
            stream.prepare_nanos as f64 / 1_000_000.0,
            read_bandwidth_mib_s,
        );
    }

    fn node_measurements(
        &self,
        context: WorkExecutionContext<'_>,
        state: &SpectralCycleExecutorState,
        fragment: &WeightingPlanFragment<'_>,
    ) -> Result<WorkMeasurements, io::Error> {
        let final_model_preparation =
            crate::spectral_cycle_plan::pass_node("final-model-preparation", self.pass);
        let traversal_measurements = (context.node().id == *fragment.source_read_node()
            || (context.node().id == *fragment.generation_node()
                && fragment.streaming_mode()
                    == Some(crate::WeightingStreamingMode::DensityInitial)))
        .then(|| state.weighting.latest_traversal_measurements().copied())
        .flatten();
        let stream_measurements = (context.node().id == *fragment.source_read_node()
            || (context.node().id == *fragment.generation_node()
                && fragment.streaming_mode()
                    == Some(crate::WeightingStreamingMode::DensityInitial)))
        .then(|| state.weighting.latest_stream_measurements().copied())
        .flatten();
        let stream_queue_high_water = stream_measurements
            .map(|measurements| u64::try_from(measurements.ready_queue_high_water))
            .transpose()
            .map_err(|_| io::Error::other("stream queue high-water exceeds receipt domain"))?;
        let resources = context
            .node()
            .claims
            .iter()
            .map(|claim| {
                let peak = match (&claim.resource, traversal_measurements) {
                    (
                        LeaseResource::IoBuffer(crate::IoBufferKind::SourceReadAhead),
                        Some(measurements),
                    ) => measurements.peak_live_capacity_bytes(),
                    (LeaseResource::IoBuffer(crate::IoBufferKind::SourceReadAhead), None)
                        if stream_measurements.is_some() =>
                    {
                        stream_measurements
                            .expect("stream measurements were checked")
                            .peak_live_source_capacity_bytes
                    }
                    (LeaseResource::Queue { .. }, _)
                        if &claim.resource == fragment.source_queue()
                            && stream_queue_high_water.is_some() =>
                    {
                        stream_queue_high_water.expect("stream queue high-water was checked")
                    }
                    _ => claim.amount,
                };
                ResourceMeasurement::new(claim.resource.clone(), claim.lifetime.clone(), peak)
            })
            .collect();
        let io = context
            .node()
            .claims
            .iter()
            .filter_map(|claim| match claim.resource {
                LeaseResource::IoBuffer(crate::IoBufferKind::SourceReadAhead) => {
                    Some(match traversal_measurements {
                        Some(measurements) => IoMeasurement::new(
                            crate::IoBufferKind::SourceReadAhead,
                            measurements.logical_output_bytes(),
                            measurements.source_read_operations(),
                        ),
                        None if stream_measurements.is_some() => {
                            let measurements =
                                stream_measurements.expect("stream measurements were checked");
                            IoMeasurement::new(
                                crate::IoBufferKind::SourceReadAhead,
                                measurements.logical_source_bytes,
                                measurements.source_read_operations,
                            )
                        }
                        None => IoMeasurement::unobserved(crate::IoBufferKind::SourceReadAhead),
                    })
                }
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
}

impl WorkImplementation for SpectralCycleExecutor {
    type Error = io::Error;

    fn implementation_id(&self) -> &WorkImplementationId {
        &self.id
    }

    fn execute(&self, context: WorkExecutionContext<'_>) -> Result<WorkMeasurements, Self::Error> {
        let result = (|| -> Result<WorkMeasurements, io::Error> {
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
                                .traverse_density_source(
                                    context,
                                    &fragment,
                                    selected,
                                    &self.problem,
                                )
                                .map_err(io::Error::other)?,
                        );
                        self.log_stream_measurements(&state.weighting, "density");
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
                let owner = MajorCycleOperatorState::begin(complete, preparation)
                    .map_err(io::Error::other)?;
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
                .reconstruction_cycle
                .as_ref()
                .is_some_and(|cycle| context.node().id == cycle.node)
            {
                let cycle = self
                    .reconstruction_cycle
                    .as_ref()
                    .expect("reconstruction-cycle node matched");
                let result = state
                    .result
                    .take()
                    .ok_or_else(|| io::Error::other("initial major completion missing"))?;
                let lifecycle = state
                    .lifecycle
                    .as_ref()
                    .ok_or_else(|| io::Error::other("model lifecycle missing"))?;
                state.reconstruction_cycle_completion = Some(
                    InitialMajorPhaseCompletion::new(result)
                        .run_reconstruction_cycle(lifecycle, &cycle.mask, cycle.program.clone())
                        .map_err(io::Error::other)?,
                );
            } else if context.node().id == *fragment.release_node() {
                state
                    .weighting
                    .release(context, &fragment)
                    .map_err(io::Error::other)?;
            }
            self.node_measurements(context, &state, &fragment)
        })();
        result.map_err(|source| {
            let measurements = self.state.lock().ok().and_then(|state| {
                state
                    .weighting
                    .latest_stream_measurements()
                    .is_some()
                    .then(|| {
                        let fragment = self.fragment();
                        self.node_measurements(context, &state, &fragment).ok()
                    })
                    .flatten()
            });
            match measurements {
                Some(measurements) => io::Error::other(SpectralCycleNodeFailure {
                    source,
                    measurements,
                }),
                None => source,
            }
        })
    }

    fn failure_measurements<'error>(
        &'error self,
        error: &'error Self::Error,
    ) -> Option<&'error WorkMeasurements> {
        error
            .get_ref()?
            .downcast_ref::<SpectralCycleNodeFailure>()
            .map(|failure| &failure.measurements)
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

    /// Run one resource-admitted independent cycle over the complete channel slab.
    pub fn run_reconstruction_cycle(
        self,
        lifecycle: &ModelLifecycle,
        mask_plan: &ReconstructionMaskPlan,
        program: MinorCycleProgram,
    ) -> Result<ReconstructionCyclePhaseCompletion, ReconstructionCycleError> {
        let completion = self.result.into_completion();
        let (normal_state, continuation) = completion.into_continuation();
        let (mask, auto_mask) = mask_plan
            .materialize(continuation.generation(), &normal_state)
            .map_err(|error| ReconstructionCycleError::Minor(error.into()))?;
        let cycle = ReconstructionCycle::new(ChannelCyclePolicy::Independent, program).run(
            lifecycle,
            continuation.generation(),
            &normal_state,
            &mask,
        )?;
        let (delta, evidence) = cycle.into_parts();
        Ok(ReconstructionCyclePhaseCompletion {
            normal_state,
            continuation,
            mask,
            delta,
            evidence,
            auto_mask,
        })
    }
}

/// Typed slab-level result carried into the ordinary final-major plan.
pub struct ReconstructionCyclePhaseCompletion {
    normal_state: FinalNormalState,
    continuation: FinalModelContinuation,
    mask: casa_imaging_reconstruction::ReconstructionMask,
    delta: Option<casa_imaging_reconstruction::ModelDelta>,
    evidence: ReconstructionCycleEvidence,
    auto_mask: Option<casa_imaging_reconstruction::AutoMultithreshEvidence>,
}

impl ReconstructionCyclePhaseCompletion {
    /// Return reconstruction-owner evidence for every output channel.
    #[must_use]
    pub const fn evidence(&self) -> &ReconstructionCycleEvidence {
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
            evidence: Box::new(ReconstructionCyclePhaseEvidence {
                normal_state: self.normal_state,
                continuation: self.continuation,
                reconstruction_cycle: self.evidence,
            }),
        }
    }
}

/// Authoritative initial-normal, initial-model, and reconstruction-cycle evidence.
pub struct ReconstructionCyclePhaseEvidence {
    normal_state: FinalNormalState,
    continuation: FinalModelContinuation,
    reconstruction_cycle: ReconstructionCycleEvidence,
}

impl ReconstructionCyclePhaseEvidence {
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

    /// Return ordered reconstruction-cycle evidence for the complete slab.
    #[must_use]
    pub const fn reconstruction_cycle(&self) -> &ReconstructionCycleEvidence {
        &self.reconstruction_cycle
    }
}
