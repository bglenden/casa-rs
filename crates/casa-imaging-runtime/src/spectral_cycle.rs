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
    FinalNormalState, MajorCycleCompletion, MajorCyclePreparation, MinorCycleProgram, ModelDeltaId,
    ModelLifecycle, ReconstructionCycle, ReconstructionCycleError, ReconstructionCycleEvidence,
    ReconstructionMaskPlan,
};

use crate::complete_data_operator::GriddedNormalReplayCompilation;
use crate::{
    AttemptBoundObservationCompletion, CompleteDataOperatorResult, CompleteDataPlanFragment,
    CompleteDataPreparedState, FenceKind, FrozenGriddedNormalReplay, FrozenWeightingArtifact,
    ImplementationContractMetadata, ImplementationRegistry, ImplementationRegistryId,
    IoMeasurement, LeaseResource, MajorCycleOperatorResult, MajorCycleOperatorState,
    ObservationReadCompletionContext, ResourceMeasurement, SelectedObservationSourceResources,
    SpectralOperatorState, SpectralPassIdentity, WeightingExecutionState, WeightingPlanFragment,
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

    /// Abort an incomplete replay and synchronously join any storage worker.
    ///
    /// Returning guarantees that the sink no longer owns a storage lock or
    /// background worker, whether the replay failed or was cancelled.
    fn abort(&mut self) -> io::Result<()>;
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

impl Drop for VisibilityWriteBinding {
    fn drop(&mut self) {
        let state = self
            .state
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let VisibilityWriteState::Writing(worker) =
            std::mem::replace(state, VisibilityWriteState::Idle)
        {
            let _ = worker.abort();
        }
    }
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
            *write_state = VisibilityWriteState::Complete;
        }
        *state = FinalVisibilityReplayState::Finished(completion);
        Ok(())
    }

    fn abort(&mut self) -> io::Result<()> {
        let worker = self.visibility_write.as_ref().and_then(|write| {
            let mut state = write
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match std::mem::replace(&mut *state, VisibilityWriteState::Idle) {
                VisibilityWriteState::Writing(worker) => Some(worker),
                VisibilityWriteState::Idle | VisibilityWriteState::Complete => None,
            }
        });
        let worker_result = worker.map_or(Ok(()), VisibilityWriteWorker::abort);
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !matches!(*state, FinalVisibilityReplayState::Finished(_)) {
            *state = FinalVisibilityReplayState::Unbound;
        }
        worker_result
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
    Abort,
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
                        VisibilityWriteCommand::Abort => break written_samples,
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
        let Self { sender, join } = self;
        let send = sender.send(VisibilityWriteCommand::Finish {
            expected_samples,
            generations,
        });
        drop(sender);
        let joined = join
            .join()
            .map_err(|_| io::Error::other("visibility writer panicked"))?;
        match send {
            Ok(()) => joined,
            Err(_) => joined.and_then(|_| Err(io::Error::other("visibility writer stopped"))),
        }
    }

    fn abort(self) -> io::Result<()> {
        let Self { sender, join } = self;
        let send = sender.send(VisibilityWriteCommand::Abort);
        drop(sender);
        let joined = join
            .join()
            .map_err(|_| io::Error::other("visibility writer panicked"))?
            .map(|_| ());
        match send {
            Ok(()) => joined,
            Err(_) => joined.and_then(|_| Err(io::Error::other("visibility writer stopped"))),
        }
    }
}

/// Runtime-owned immutable registry for one CPU implementation bundle.
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

/// Runtime-owned CPU implementation of one ordinary major-cycle plan.
pub struct SpectralCycleExecutor {
    id: WorkImplementationId,
    problem: CompiledProblem,
    weighting_plan: WeightingPlan,
    source_resources: Option<SelectedObservationSourceResources>,
    pass: SpectralPassIdentity,
    complete_data: CompleteDataPlanFragment,
    reconstruction_cycle: Option<SerialReconstructionCycleExecution>,
    final_visibility_sink: Option<Mutex<Box<dyn FinalVisibilitySink>>>,
    phase_input_artifact: Option<(crate::ArtifactIdentity, u64)>,
    gridded_input_artifact: Option<crate::GriddedNormalReplayDescriptor>,
    mode: SpectralCycleExecutionMode,
    state: Mutex<SpectralCycleExecutorState>,
}

/// Transient bounded-execution evidence for the most recent complete-data pass.
///
/// This diagnostic surface is intentionally separate from the persisted receipt
/// schema. It reports physical scheduling and residency without exposing
/// reconstruction state or MeasurementSet contents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompleteDataStreamEvidence {
    planned_workers: u64,
    actual_workers: u64,
    active_worker_slots: u64,
    partitions_executed: u64,
    commits_completed: u64,
    peak_partial_dynamic_capacity_bytes: u64,
    peak_worker_stack_capacity_bytes: u64,
    peak_kernel_window_capacity_bytes: u64,
    planned_gridded_route_capacity_bytes: u64,
    prepare_nanos: u128,
    execute_nanos: u128,
    commit_nanos: u128,
    executed_work_identity_digest: [u8; 32],
    committed_work_identity_digest: [u8; 32],
    source_pass_count: u64,
    artifact_pass_count: u64,
    grid_resident_bytes: u64,
}

impl CompleteDataStreamEvidence {
    /// Return the worker count bound by the immutable stream plan.
    #[must_use]
    pub const fn planned_workers(self) -> u64 {
        self.planned_workers
    }

    /// Return the worker count supplied to the shared executor.
    #[must_use]
    pub const fn actual_workers(self) -> u64 {
        self.actual_workers
    }

    /// Return worker slots that executed at least one partition.
    #[must_use]
    pub const fn active_worker_slots(self) -> u64 {
        self.active_worker_slots
    }

    /// Return exact partition executions.
    #[must_use]
    pub const fn partitions_executed(self) -> u64 {
        self.partitions_executed
    }

    /// Return exact deterministic commits.
    #[must_use]
    pub const fn commits_completed(self) -> u64 {
        self.commits_completed
    }

    /// Return the peak dynamic bytes retained by simultaneous partials.
    #[must_use]
    pub const fn peak_partial_dynamic_capacity_bytes(self) -> u64 {
        self.peak_partial_dynamic_capacity_bytes
    }

    /// Return the peak explicit scoped-worker stack capacity.
    #[must_use]
    pub const fn peak_worker_stack_capacity_bytes(self) -> u64 {
        self.peak_worker_stack_capacity_bytes
    }

    /// Return the peak complete prepared/worker/partial window capacity.
    #[must_use]
    pub const fn peak_kernel_window_capacity_bytes(self) -> u64 {
        self.peak_kernel_window_capacity_bytes
    }

    /// Return the exact reusable route allocation admitted for gridded replay.
    #[must_use]
    pub const fn planned_gridded_route_capacity_bytes(self) -> u64 {
        self.planned_gridded_route_capacity_bytes
    }

    /// Return reconstruction partition preparation time.
    #[must_use]
    pub const fn prepare_nanos(self) -> u128 {
        self.prepare_nanos
    }

    /// Return worker execution time, measured by scheduler waves.
    #[must_use]
    pub const fn execute_nanos(self) -> u128 {
        self.execute_nanos
    }

    /// Return deterministic commit and reduction time.
    #[must_use]
    pub const fn commit_nanos(self) -> u128 {
        self.commit_nanos
    }

    /// Return the ordered identity digest of executed work.
    #[must_use]
    pub const fn executed_work_identity_digest(self) -> [u8; 32] {
        self.executed_work_identity_digest
    }

    /// Return the ordered identity digest of committed work.
    #[must_use]
    pub const fn committed_work_identity_digest(self) -> [u8; 32] {
        self.committed_work_identity_digest
    }

    /// Return exact selected-observation source passes.
    #[must_use]
    pub const fn source_pass_count(self) -> u64 {
        self.source_pass_count
    }

    /// Return exact gridded-normal artifact passes.
    #[must_use]
    pub const fn artifact_pass_count(self) -> u64 {
        self.artifact_pass_count
    }

    /// Return bytes in the one shared complete-data grid allocation.
    #[must_use]
    pub const fn grid_resident_bytes(self) -> u64 {
        self.grid_resident_bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SpectralCycleExecutionMode {
    Science,
    SelectedOutputOnly,
}

struct SpectralCycleExecutorState {
    executable: Option<ExecutableModelProblem>,
    pass_input: Option<SpectralCyclePassInput>,
    selected: Option<BoundSelectedObservation>,
    selected_completion: Option<SelectedObservationCompletion>,
    weighting: WeightingExecutionState,
    gridded_storage: Option<crate::GriddedNormalReplayStorage>,
    gridded_storage_ceiling: Option<u64>,
    gridded_compilation: Option<GriddedNormalReplayCompilation>,
    gridded_replay: Option<FrozenGriddedNormalReplay>,
    pending_frozen_reservation: Option<Arc<crate::FrozenWeightingReservation>>,
    frozen_weighting: Option<FrozenWeightingArtifact>,
    prepared: Option<CompleteDataPreparedState>,
    operator: Option<SpectralOperatorState>,
    complete_data: Option<CompleteDataOperatorResult>,
    lifecycle: Option<ModelLifecycle>,
    prepared_model: Option<PreparedFinalModel>,
    result: Option<MajorCycleOperatorResult>,
    reconstruction_cycle_completion: Option<ReconstructionCyclePhaseCompletion>,
    output_completion: Option<MajorCycleCompletion>,
    complete_data_source_pass_count: u64,
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
    /// Return transient scheduling and residency evidence for the latest
    /// completed complete-data stream.
    #[must_use]
    pub fn latest_complete_data_stream_evidence(&self) -> Option<CompleteDataStreamEvidence> {
        let state = self.state.lock().ok()?;
        let (stream, artifact_pass_count) = if let Some((replay, stream)) =
            state.gridded_replay.as_ref().and_then(|replay| {
                replay
                    .latest_stream_measurements()
                    .map(|stream| (replay, stream))
            }) {
            (
                stream,
                u64::from(replay.latest_read_measurements().is_some()),
            )
        } else {
            (state.weighting.latest_stream_measurements()?, 0)
        };
        Some(CompleteDataStreamEvidence {
            planned_workers: u64::try_from(stream.workers).ok()?,
            actual_workers: u64::try_from(stream.workers).ok()?,
            active_worker_slots: u64::try_from(stream.workers_with_nonzero_partitions).ok()?,
            partitions_executed: stream.partitions_executed,
            commits_completed: stream.commits_completed,
            peak_partial_dynamic_capacity_bytes: stream.peak_partial_dynamic_capacity_bytes,
            peak_worker_stack_capacity_bytes: stream.peak_worker_stack_capacity_bytes,
            peak_kernel_window_capacity_bytes: stream.peak_kernel_window_capacity_bytes,
            planned_gridded_route_capacity_bytes: u64::try_from(
                self.complete_data.residency().gridded_route_bytes(),
            )
            .ok()?,
            prepare_nanos: stream.prepare_nanos,
            execute_nanos: stream.execute_nanos,
            commit_nanos: stream.commit_nanos,
            executed_work_identity_digest: stream.executed_work_identity_digest,
            committed_work_identity_digest: stream.committed_work_identity_digest,
            source_pass_count: state.complete_data_source_pass_count,
            artifact_pass_count,
            grid_resident_bytes: u64::try_from(self.complete_data.residency().grid_bytes()).ok()?,
        })
    }

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
            source_resources: Some(source_resources),
            pass,
            complete_data,
            reconstruction_cycle: None,
            final_visibility_sink: None,
            phase_input_artifact,
            gridded_input_artifact: None,
            mode: SpectralCycleExecutionMode::Science,
            state: Mutex::new(SpectralCycleExecutorState {
                executable: Some(executable),
                pass_input: Some(pass_input),
                selected: Some(selected),
                selected_completion: None,
                weighting: WeightingExecutionState::new(),
                gridded_storage: None,
                gridded_storage_ceiling: None,
                gridded_compilation: None,
                gridded_replay: None,
                pending_frozen_reservation: None,
                frozen_weighting: None,
                prepared: None,
                operator: None,
                complete_data: None,
                lifecycle: None,
                prepared_model: None,
                result: None,
                reconstruction_cycle_completion: None,
                output_completion: None,
                complete_data_source_pass_count: 0,
            }),
        }
    }

    /// Bind a later major to the sealed gridded-normal capability, without a
    /// selected-observation owner or source resource surface.
    #[allow(clippy::too_many_arguments)]
    pub fn new_gridded(
        id: WorkImplementationId,
        problem: CompiledProblem,
        weighting_plan: WeightingPlan,
        pass: SpectralPassIdentity,
        complete_data: CompleteDataPlanFragment,
        executable: ExecutableModelProblem,
        pass_input: SpectralCyclePassInput,
        planned_gridded_normal: crate::PlannedGriddedNormalBinding,
    ) -> io::Result<Self> {
        let (planned_replay, replay) = planned_gridded_normal.into_replay().ok_or_else(|| {
            io::Error::other("later major requires a plan-issued gridded-normal replay")
        })?;
        let phase_input_artifact = match &pass_input {
            SpectralCyclePassInput::Initial => None,
            SpectralCyclePassInput::FinalMajor(input) => Some((
                input.identity(),
                u64::try_from(input.terms.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(std::mem::size_of::<ModelDeltaTerm>() as u64),
            )),
        };
        Ok(Self {
            id,
            problem,
            weighting_plan,
            source_resources: None,
            pass,
            complete_data,
            reconstruction_cycle: None,
            final_visibility_sink: None,
            phase_input_artifact,
            gridded_input_artifact: Some(planned_replay),
            mode: SpectralCycleExecutionMode::Science,
            state: Mutex::new(SpectralCycleExecutorState {
                executable: Some(executable),
                pass_input: Some(pass_input),
                selected: None,
                selected_completion: None,
                weighting: WeightingExecutionState::new(),
                gridded_storage: None,
                gridded_storage_ceiling: None,
                gridded_compilation: None,
                gridded_replay: Some(replay),
                pending_frozen_reservation: None,
                frozen_weighting: None,
                prepared: None,
                operator: None,
                complete_data: None,
                lifecycle: None,
                prepared_model: None,
                result: None,
                reconstruction_cycle_completion: None,
                output_completion: None,
                complete_data_source_pass_count: 0,
            }),
        })
    }

    /// Bind a terminal selected-output traversal. This mode may predict and
    /// write selected visibilities, but cannot accumulate residual science.
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new_selected_output(
        id: WorkImplementationId,
        problem: CompiledProblem,
        weighting_plan: WeightingPlan,
        source_resources: SelectedObservationSourceResources,
        pass: SpectralPassIdentity,
        complete_data: CompleteDataPlanFragment,
        selected: BoundSelectedObservation,
        completion: MajorCycleCompletion,
        frozen_weighting: FrozenWeightingArtifact,
    ) -> Self {
        Self {
            id,
            problem,
            weighting_plan,
            source_resources: Some(source_resources),
            pass,
            complete_data,
            reconstruction_cycle: None,
            final_visibility_sink: None,
            phase_input_artifact: None,
            gridded_input_artifact: None,
            mode: SpectralCycleExecutionMode::SelectedOutputOnly,
            state: Mutex::new(SpectralCycleExecutorState {
                executable: None,
                pass_input: None,
                selected: Some(selected),
                selected_completion: None,
                weighting: WeightingExecutionState::with_frozen_artifact(frozen_weighting.clone()),
                gridded_storage: None,
                gridded_storage_ceiling: None,
                gridded_compilation: None,
                gridded_replay: None,
                pending_frozen_reservation: None,
                frozen_weighting: Some(frozen_weighting),
                prepared: None,
                operator: None,
                complete_data: None,
                lifecycle: None,
                prepared_model: None,
                result: None,
                reconstruction_cycle_completion: None,
                output_completion: Some(completion),
                complete_data_source_pass_count: 0,
            }),
        }
    }

    /// Attach the initial run's plan-bound gridded-normal storage.
    pub fn with_planned_gridded_normal_binding(
        mut self,
        planned_gridded_normal: crate::PlannedGriddedNormalBinding,
    ) -> io::Result<Self> {
        if self.pass.phase() != crate::SpectralPassPhase::InitialMajor {
            return Err(io::Error::other(
                "only the initial major can compile gridded-normal replay",
            ));
        }
        let state = self
            .state
            .get_mut()
            .map_err(|_| io::Error::other("new spectral cycle executor mutex poisoned"))?;
        let (storage, maximum_bytes) =
            planned_gridded_normal.into_compilation().ok_or_else(|| {
                io::Error::other("initial major requires a plan-issued compilation binding")
            })?;
        state.gridded_storage = Some(storage);
        state.gridded_storage_ceiling = Some(maximum_bytes);
        Ok(self)
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
            .frozen_weighting = Some(artifact);
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

    /// Consume the sealed normal-operator capability after a successful pass.
    pub fn take_gridded_normal_replay(&self) -> Option<FrozenGriddedNormalReplay> {
        self.state.lock().ok()?.gridded_replay.take()
    }

    /// Consume the unchanged scientific completion after selected-output traversal.
    pub fn take_selected_output_completion(&self) -> Option<MajorCycleCompletion> {
        self.state.lock().ok()?.output_completion.take()
    }

    fn abort_final_visibility_replay(&self) -> io::Result<()> {
        let Some(sink) = &self.final_visibility_sink else {
            return Ok(());
        };
        sink.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .abort()
    }

    fn fragment(&self) -> Option<WeightingPlanFragment<'_>> {
        let source_resources = self.source_resources.clone()?;
        let mode = match self.mode {
            SpectralCycleExecutionMode::SelectedOutputOnly => {
                crate::WeightingStreamingMode::SelectedOutputOnly
            }
            SpectralCycleExecutionMode::Science => match self.pass.phase() {
                crate::SpectralPassPhase::FinalMajor => return None,
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
            },
        };
        Some(WeightingPlanFragment::streaming_for_pass(
            &self.weighting_plan,
            crate::spectral_cycle_plan::pass_node("transaction-read", self.pass),
            source_resources,
            self.id.clone(),
            self.pass,
            mode,
            crate::plan_continuum_transform_row(&self.problem)
                .expect("compiled transform row plan remains valid")
                .map(|plan| u64::try_from(plan.bytes()).expect("transform bytes fit u64")),
        ))
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
        if state.gridded_compilation.is_none()
            && let Some(storage) = state.gridded_storage.as_ref()
        {
            state.gridded_compilation = Some(GriddedNormalReplayCompilation::new(
                &self.problem,
                context,
                storage,
                self.weighting_plan.limits().max_block_samples(),
            )?);
        }
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
        state
            .weighting
            .authorize_imported_operator(&mut operator)
            .map_err(io::Error::other)?;
        state.operator = Some(operator);
        let SpectralCycleExecutorState {
            weighting,
            operator,
            gridded_compilation,
            complete_data_source_pass_count,
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
            if let Some(compilation) = gridded_compilation.as_mut() {
                compilation.consume_block(block)?;
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
            Some(crate::WeightingStreamingMode::SelectedOutputOnly) => Err(io::Error::other(
                "selected-output traversal cannot enter residual science streaming",
            )),
            None => Err(io::Error::other("streaming weighting mode missing")),
        };
        if result.is_ok() {
            *complete_data_source_pass_count = complete_data_source_pass_count
                .checked_add(
                    weighting
                        .latest_traversal_measurements()
                        .ok_or_else(|| io::Error::other("source-pass measurements missing"))?
                        .source_pass_count(),
                )
                .ok_or_else(|| io::Error::other("source-pass measurements overflow"))?;
            if let Some(compilation) = gridded_compilation.as_mut() {
                compilation.seal()?;
                self.log_gridded_write_measurements(compilation);
            }
            self.log_stream_measurements(weighting, "weighted-replay");
        }
        result
    }

    fn run_selected_output(
        &self,
        state: &mut SpectralCycleExecutorState,
        context: WorkExecutionContext<'_>,
        fragment: &WeightingPlanFragment<'_>,
        selected: BoundSelectedObservation,
    ) -> Result<(), io::Error> {
        let sink = self
            .final_visibility_sink
            .as_ref()
            .ok_or_else(|| io::Error::other("selected-output sink missing"))?;
        let prepared = state
            .prepared
            .take()
            .ok_or_else(|| io::Error::other("selected-output FFT preparation did not run"))?;
        let completion = state
            .output_completion
            .as_ref()
            .ok_or_else(|| io::Error::other("selected-output scientific completion missing"))?;
        {
            let mut sink = sink
                .lock()
                .map_err(|_| io::Error::other("final visibility sink poisoned"))?;
            sink.bind(
                self.problem.problem_id(),
                completion.final_model().generation_id(),
            )?;
            sink.begin_replay()?;
        }
        let mut operator = prepared
            .begin_streaming(context, &self.problem, &self.complete_data)
            .map_err(io::Error::other)?;
        operator
            .bind_selected_output_model(completion.final_model())
            .map_err(io::Error::other)?;
        state
            .weighting
            .authorize_imported_operator(&mut operator)
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
                .ok_or_else(|| io::Error::other("selected-output operator missing"))?
                .predict_final_visibility_chunk(block)
                .map_err(io::Error::other)?;
            sink.lock()
                .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                .consume(predicted)
        };
        let result = weighting
            .traverse_selected_output_bounded_stream(
                context,
                fragment,
                selected,
                &self.problem,
                &mut consume,
            )
            .map_err(io::Error::other);
        if result.is_ok() {
            self.log_stream_measurements(weighting, "selected-output");
        }
        result
    }

    fn run_gridded_replay(
        &self,
        state: &mut SpectralCycleExecutorState,
        context: WorkExecutionContext<'_>,
    ) -> Result<(), io::Error> {
        let prepared = state
            .prepared
            .take()
            .ok_or_else(|| io::Error::other("FFT preparation did not run"))?;
        let (preparation, prior_normal_state) = state
            .prepared_model
            .as_mut()
            .ok_or_else(|| io::Error::other("final-model preparation missing"))?
            .take_for_replay(context)?;
        let prior_normal_state = prior_normal_state.ok_or_else(|| {
            io::Error::other("gridded-normal replay requires the prior normal state")
        })?;
        let replay = state
            .gridded_replay
            .as_mut()
            .ok_or_else(|| io::Error::other("sealed gridded-normal replay missing"))?;
        let operator = self
            .complete_data
            .begin_gridded_replay(
                context,
                &self.problem,
                preparation,
                prior_normal_state,
                prepared,
                replay,
            )
            .map_err(io::Error::other)?;
        let route_capacity_bytes =
            u64::try_from(self.complete_data.residency().gridded_route_bytes())
                .map_err(|_| io::Error::other("gridded-normal route capacity overflow"))?;
        state.complete_data = Some(replay.execute_bounded(
            context,
            self.pass.ordinal(),
            operator,
            route_capacity_bytes,
        )?);
        self.log_gridded_replay_measurements(replay);
        Ok(())
    }

    fn log_gridded_replay_measurements(&self, replay: &FrozenGriddedNormalReplay) {
        let (Some(stream), Some(artifact)) = (
            replay.latest_stream_measurements(),
            replay.latest_read_measurements(),
        ) else {
            return;
        };
        eprintln!(
            "imaging_gridded_replay_summary ordinal={} blocks={} artifact_bytes={} payload_bytes={} read_bytes={} read_operations={} payload_copy_bytes={} payload_copy_operations={} buffer_allocations={} buffer_reuses={} source_slots={} workers={} worker_threads_started={} dispatch_waves={} active_worker_slots={} minimum_partitions_per_active_worker={} maximum_partitions_per_active_worker={} worker_slots={:?} partitions_executed={} commits_completed={} executed_work_identity={:x?} committed_work_identity={:x?} planned_source_capacity_bytes={} planned_kernel_window_capacity_bytes={} planned_gridded_route_maximum_frame_records={} planned_gridded_route_maximum_frame_groups={} planned_gridded_route_capacity_bytes={} peak_partial_dynamic_capacity_bytes={} peak_worker_stack_capacity_bytes={} peak_kernel_window_capacity_bytes={} peak_live_source_blocks={} peak_live_source_current_bytes={} peak_live_source_capacity_bytes={} ready_queue_high_water={} producer_wait_nanos={} consumer_wait_nanos={} source_starved_nanos={} overlap_nanos={} source_fill_nanos={} prepare_nanos={} execute_nanos={} commit_nanos={} wall_nanos={}",
            self.pass.ordinal(),
            stream.blocks_filled,
            artifact.artifact_bytes(),
            stream.logical_source_bytes,
            artifact.transferred_bytes(),
            artifact.operations(),
            artifact.payload_copy_bytes(),
            artifact.payload_copy_operations(),
            artifact.buffer_allocations(),
            artifact.buffer_reuses(),
            stream.source_slots,
            stream.workers,
            stream.worker_threads_started,
            stream.dispatch_waves,
            stream.workers_with_nonzero_partitions,
            stream.minimum_partitions_per_active_worker,
            stream.maximum_partitions_per_active_worker,
            stream.worker_slots,
            stream.partitions_executed,
            stream.commits_completed,
            stream.executed_work_identity_digest,
            stream.committed_work_identity_digest,
            stream.planned_source_capacity_bytes,
            stream.planned_kernel_window_capacity_bytes,
            self.complete_data
                .gridded_route_residency()
                .map(|residency| residency.maximum_frame_records())
                .unwrap_or(0),
            self.complete_data
                .gridded_route_residency()
                .map(|residency| residency.maximum_frame_groups())
                .unwrap_or(0),
            self.complete_data.residency().gridded_route_bytes(),
            stream.peak_partial_dynamic_capacity_bytes,
            stream.peak_worker_stack_capacity_bytes,
            stream.peak_kernel_window_capacity_bytes,
            stream.peak_live_source_blocks,
            stream.peak_live_source_current_bytes,
            stream.peak_live_source_capacity_bytes,
            stream.ready_queue_high_water,
            stream.producer_wait_nanos,
            stream.consumer_wait_nanos,
            stream.source_starved_nanos,
            stream.overlap_nanos,
            stream.source_fill_nanos,
            stream.prepare_nanos,
            stream.execute_nanos,
            stream.commit_nanos,
            stream.wall_nanos,
        );
    }

    fn log_gridded_write_measurements(&self, compilation: &GriddedNormalReplayCompilation) {
        let artifact = compilation.write_measurements();
        let allocations = compilation.compilation_measurements();
        eprintln!(
            "imaging_gridded_compile_summary ordinal={} blocks={} artifact_bytes={} payload_bytes={} write_bytes={} write_operations={} payload_copy_bytes={} payload_copy_operations={} buffer_allocations={} buffer_reuses={} source_group_vector_allocations={} source_group_capacity_growth_bytes={} reduction_map_entry_insertions={} multiplicity_vector_allocations={} multiplicity_capacity_growth_bytes={} encoded_buffer_allocations={} encoded_buffer_bytes={} descriptor_vector_allocations={} descriptor_capacity_growth_bytes={}",
            self.pass.ordinal(),
            allocations.blocks,
            artifact.artifact_bytes(),
            artifact.payload_bytes(),
            artifact.transferred_bytes(),
            artifact.operations(),
            artifact.payload_copy_bytes(),
            artifact.payload_copy_operations(),
            artifact.buffer_allocations(),
            artifact.buffer_reuses(),
            allocations.source_group_vector_allocations,
            allocations.source_group_capacity_growth_bytes,
            allocations.reduction_map_entry_insertions,
            allocations.multiplicity_vector_allocations,
            allocations.multiplicity_capacity_growth_bytes,
            allocations.encoded_buffer_allocations,
            allocations.encoded_buffer_bytes,
            allocations.descriptor_vector_allocations,
            allocations.descriptor_capacity_growth_bytes,
        );
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
            "imaging_source_read_ahead_summary mode=bounded_spectral stage={stage} phase={phase} ordinal={} enabled={} max_live_row_blocks={} queue_capacity={} live_row_block_high_water={} row_blocks={} pass_count={} stored_rows={} stored_samples={} selected_channel_runs={} streamed_samples={} source_bytes={} modeled_physical_read_bytes={} source_read_operations={} request_handoff_bytes={} selected_sample_handoff_bytes={} peak_consumer_scratch_current_bytes={} consumer_scratch_capacity_bytes={} allocated_storage_buffers={} reused_storage_buffers={} peak_live_current_bytes={} peak_live_capacity_bytes={} source_slots={} workers={} maximum_partitions_per_block={} planned_source_capacity_bytes={} ready_queue_high_water={} ready_queue_current_bytes_high_water={} ready_queue_capacity_bytes_high_water={} planned_kernel_window_capacity_bytes={} peak_kernel_window_capacity_bytes={} source_read_nanos={} source_fill_nanos={} source_arrangement_nanos={} stream_source_fill_nanos={} process_block_prepare_nanos={} process_block_execute_nanos={} route_consume_combined_nanos={} producer_wait_nanos={} source_starved_nanos={} terminal_wait_nanos={} consumer_wait_total_nanos={} lease_return_nanos={} producer_consumer_overlap_nanos={} wall_nanos={} consumer_recv_blocked_ms={:.3} producer_send_blocked_ms={:.3} producer_consumer_overlap_ms={:.3} source_read_ms={:.3} source_route_ms={:.3} consumer_ms={:.3} source_prepare_ms={:.3} effective_read_bandwidth_mib_s={:.3}",
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
            stream.source_starved_nanos,
            stream.terminal_wait_nanos,
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
        .then(|| state.weighting.latest_stream_measurements())
        .flatten();
        let gridded_write_measurements = if context.node().id == *fragment.streaming_node() {
            state
                .gridded_compilation
                .as_ref()
                .map(GriddedNormalReplayCompilation::write_measurements)
        } else {
            None
        };
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
                    (LeaseResource::IoBuffer(crate::IoBufferKind::SpillWrite), _)
                        if gridded_write_measurements.is_some() =>
                    {
                        gridded_write_measurements
                            .expect("gridded write measurements were checked")
                            .peak_buffer_bytes()
                    }
                    (
                        LeaseResource::Storage {
                            use_kind: crate::StorageUseKind::Temporary,
                            ..
                        },
                        _,
                    ) if gridded_write_measurements.is_some() => gridded_write_measurements
                        .expect("gridded write measurements were checked")
                        .artifact_bytes(),
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
                LeaseResource::IoBuffer(crate::IoBufferKind::SpillWrite) => {
                    Some(gridded_write_measurements.map_or_else(
                        || IoMeasurement::unobserved(crate::IoBufferKind::SpillWrite),
                        |measurements| measurements.io_measurement(),
                    ))
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

    fn gridded_node_measurements(
        &self,
        context: WorkExecutionContext<'_>,
        state: &SpectralCycleExecutorState,
    ) -> Result<WorkMeasurements, io::Error> {
        let final_model_preparation =
            crate::spectral_cycle_plan::pass_node("final-model-preparation", self.pass);
        let replay_measurements = (context.node().id == *self.complete_data.replay_node())
            .then(|| {
                state
                    .gridded_replay
                    .as_ref()
                    .and_then(FrozenGriddedNormalReplay::latest_read_measurements)
            })
            .flatten();
        let resources = context
            .node()
            .claims
            .iter()
            .map(|claim| {
                let peak = match (&claim.resource, replay_measurements) {
                    (
                        &LeaseResource::IoBuffer(crate::IoBufferKind::SpillRead),
                        Some(measurements),
                    ) => measurements.peak_buffer_bytes(),
                    (
                        &LeaseResource::Storage {
                            use_kind: crate::StorageUseKind::Temporary,
                            ..
                        },
                        Some(measurements),
                    ) => measurements.artifact_bytes(),
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
                LeaseResource::IoBuffer(crate::IoBufferKind::SpillRead) => {
                    Some(replay_measurements.map_or_else(
                        || IoMeasurement::unobserved(crate::IoBufferKind::SpillRead),
                        |measurements| measurements.io_measurement(),
                    ))
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
            .chain(
                self.gridded_input_artifact
                    .filter(|_| context.node().id == *self.complete_data.replay_node())
                    .map(|descriptor| {
                        let identity = descriptor.identity();
                        crate::ArtifactMeasurement::new(
                            identity,
                            Some(identity),
                            crate::ArtifactDisposition::Loaded,
                            descriptor.bytes(),
                            None,
                        )
                        .expect("planned gridded input evidence is implementation-owned")
                    }),
            )
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
                if self.mode == SpectralCycleExecutionMode::Science {
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
                }
            } else if context.node().id == *self.complete_data.preparation_node() {
                state.prepared = Some(
                    self.complete_data
                        .prepare(context)
                        .map_err(io::Error::other)?,
                );
            } else if self.mode == SpectralCycleExecutionMode::Science
                && self.pass.phase() == crate::SpectralPassPhase::FinalMajor
                && context.node().id == *self.complete_data.replay_node()
            {
                self.run_gridded_replay(&mut state, context)?;
            } else if fragment
                .as_ref()
                .is_some_and(|fragment| context.node().id == *fragment.source_read_node())
            {
                let fragment = fragment
                    .as_ref()
                    .expect("selected source branch checked the initial fragment");
                let selected = state
                    .selected
                    .take()
                    .ok_or_else(|| io::Error::other("selected observation already consumed"))?;
                match fragment.streaming_mode() {
                    Some(crate::WeightingStreamingMode::NaturalInitial) => {
                        self.run_stream(&mut state, context, fragment, Some(selected))?;
                    }
                    Some(crate::WeightingStreamingMode::DensityInitial) => {
                        state.selected_completion = Some(
                            state
                                .weighting
                                .traverse_density_source(context, fragment, selected, &self.problem)
                                .map_err(io::Error::other)?,
                        );
                        state.complete_data_source_pass_count = state
                            .complete_data_source_pass_count
                            .checked_add(
                                state
                                    .weighting
                                    .latest_traversal_measurements()
                                    .ok_or_else(|| {
                                        io::Error::other("density source-pass measurements missing")
                                    })?
                                    .source_pass_count(),
                            )
                            .ok_or_else(|| io::Error::other("source-pass measurements overflow"))?;
                        self.log_stream_measurements(&state.weighting, "density");
                    }
                    Some(crate::WeightingStreamingMode::SelectedOutputOnly) => {
                        self.run_selected_output(&mut state, context, fragment, selected)?;
                    }
                    None => return Err(io::Error::other("streaming weighting mode missing")),
                }
            } else if fragment.as_ref().is_some_and(|fragment| {
                context.node().id == *fragment.generation_node()
                    && fragment.streaming_mode()
                        == Some(crate::WeightingStreamingMode::DensityInitial)
            }) {
                self.run_stream(
                    &mut state,
                    context,
                    fragment
                        .as_ref()
                        .expect("density generation branch checked the initial fragment"),
                    None,
                )?;
            } else if self
                .complete_data
                .reconciliation_node()
                .is_some_and(|node| context.node().id == *node)
                && self.mode == SpectralCycleExecutionMode::Science
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
            } else if fragment
                .as_ref()
                .is_some_and(|fragment| context.node().id == *fragment.release_node())
            {
                let fragment = fragment
                    .as_ref()
                    .expect("release branch checked the initial fragment");
                state
                    .weighting
                    .release(context, fragment)
                    .map_err(io::Error::other)?;
            }
            match fragment.as_ref() {
                Some(fragment) => self.node_measurements(context, &state, fragment),
                None => self.gridded_node_measurements(context, &state),
            }
        })();
        result.map_err(|source| {
            let measurements = self.state.lock().ok().and_then(|state| {
                let fragment = self.fragment();
                match fragment.as_ref() {
                    Some(fragment) if state.weighting.latest_stream_measurements().is_some() => {
                        self.node_measurements(context, &state, fragment).ok()
                    }
                    None if state
                        .gridded_replay
                        .as_ref()
                        .and_then(FrozenGriddedNormalReplay::latest_read_measurements)
                        .is_some() =>
                    {
                        self.gridded_node_measurements(context, &state).ok()
                    }
                    Some(_) | None => None,
                }
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
        let fragment = self
            .fragment()
            .ok_or_else(|| io::Error::other("gridded replay has no observation-read completion"))?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("spectral cycle state poisoned"))?;
        if completion.owner_node() == fragment.streaming_node() {
            if self.mode == SpectralCycleExecutionMode::SelectedOutputOnly {
                let result = (|| {
                    let predecessor = state
                        .weighting
                        .complete_replay(completion)
                        .map_err(io::Error::other)?;
                    let _operator = state
                        .operator
                        .take()
                        .ok_or_else(|| io::Error::other("selected-output operator missing"))?;
                    let replay = state
                        .weighting
                        .replay_completion()
                        .ok_or_else(|| io::Error::other("selected-output completion missing"))?;
                    self.final_visibility_sink
                        .as_ref()
                        .ok_or_else(|| io::Error::other("selected-output sink missing"))?
                        .lock()
                        .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                        .finish(replay)?;
                    Ok(predecessor)
                })();
                if result.is_err() {
                    drop(state);
                    let _ = self.abort_final_visibility_replay();
                }
                return result;
            }
            let result = (|| {
                let predecessor = state
                    .weighting
                    .complete_replay(completion)
                    .map_err(io::Error::other)?;
                let operator = state
                    .operator
                    .take()
                    .ok_or_else(|| io::Error::other("complete-data operator missing"))?;
                let frozen_weighting = match state.weighting.frozen_artifact() {
                    Some(artifact) => {
                        if let Some(reservation) = state.pending_frozen_reservation.take() {
                            Some(
                                artifact
                                    .with_cross_plan_reservation(reservation)
                                    .map_err(io::Error::other)?,
                            )
                        } else {
                            artifact.has_cross_plan_reservation().then_some(artifact)
                        }
                    }
                    None => None,
                };
                let compilation = state.gridded_compilation.take();
                let replay = state
                    .weighting
                    .replay_completion()
                    .ok_or_else(|| io::Error::other("replay completion missing"))?;
                // All fallible scientific validation precedes the in-place
                // visibility writer's durable completion boundary.
                let complete_data = operator.complete(replay).map_err(io::Error::other)?;
                let gridded_replay = compilation
                    .map(|compilation| compilation.complete(replay))
                    .transpose()?;
                if let Some(sink) = &self.final_visibility_sink {
                    sink.lock()
                        .map_err(|_| io::Error::other("final visibility sink poisoned"))?
                        .finish(replay)?;
                }
                state.frozen_weighting = frozen_weighting;
                state.gridded_replay = gridded_replay;
                state.complete_data = Some(complete_data);
                Ok(predecessor)
            })();
            if result.is_err() {
                drop(state);
                let _ = self.abort_final_visibility_replay();
            }
            return result;
        }
        let selected = state
            .selected_completion
            .take()
            .ok_or_else(|| io::Error::other("selected-observation completion missing"))?;
        completion.bind(selected).map_err(io::Error::other)
    }

    fn retain_artifact_resources(
        &self,
        owner_node: &WorkNodeId,
        permit: crate::RetainedArtifactPermit,
    ) -> Result<bool, Self::Error> {
        let fragment = self
            .fragment()
            .ok_or_else(|| io::Error::other("artifact retention requires a streaming plan"))?;
        if owner_node != fragment.streaming_node() {
            return Err(io::Error::other(
                "artifact retention was issued to the wrong plan node",
            ));
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("spectral-cycle state poisoned"))?;
        let storage = state
            .gridded_storage
            .as_ref()
            .ok_or_else(|| io::Error::other("artifact retention lacks planned storage"))?
            .clone();
        let maximum_bytes = state
            .gridded_storage_ceiling
            .ok_or_else(|| io::Error::other("artifact retention lacks its planned ceiling"))?;
        let replay = state
            .gridded_replay
            .as_mut()
            .ok_or_else(|| io::Error::other("artifact retention precedes replay sealing"))?;
        let sealed_bytes = replay.descriptor().bytes();
        if sealed_bytes > maximum_bytes {
            return Err(io::Error::other(
                "sealed replay exceeds its admitted storage ceiling",
            ));
        }
        let permit = permit
            .narrow_temporary_storage(sealed_bytes)
            .map_err(io::Error::other)?;
        replay.retain_plan_storage(permit, &storage, sealed_bytes)?;
        Ok(true)
    }

    fn abort_observation_read(&self, owner_node: &WorkNodeId) -> Result<(), Self::Error> {
        if self
            .fragment()
            .as_ref()
            .is_some_and(|fragment| owner_node == fragment.streaming_node())
        {
            self.abort_final_visibility_replay()?;
        }
        Ok(())
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
