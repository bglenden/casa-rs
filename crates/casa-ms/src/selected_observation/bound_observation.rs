// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, MeasurementSetIdentity,
    ObservationProvenanceId, ObservationSnapshotId, ObservationSourceState,
    SelectedObservationCommitmentId, SelectedObservationGenerationId,
    SelectedObservationInspectionError, SelectedObservationPassError, SelectedObservationSample,
};
use std::{
    error::Error,
    fmt,
    mem::size_of,
    sync::atomic::{AtomicU64, Ordering},
};
use thiserror::Error;

use super::{
    BoundObservationSamples, BoundObservationSource, BoundObservationSourceError,
    SelectedObservationContentBudget, SelectedObservationMeasures,
    SelectedObservationMeasuresError, content_plan::SelectedObservationSharedBytes,
    spectral_contributions::SelectedObservationTraversalSample,
};

/// One current storage-owner state probe and bounded-content budget.
///
/// The content budget is the sole physical blocking authority. The current
/// source state is checked exactly against the compiler snapshot before the
/// retained MeasurementSet can be bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObservationSourceBinding {
    current_state: ObservationSourceState,
    content_budget: SelectedObservationContentBudget,
}

impl ObservationSourceBinding {
    /// Bind one freshly probed source state to an explicit content budget.
    #[must_use]
    pub const fn new(
        current_state: ObservationSourceState,
        content_budget: SelectedObservationContentBudget,
    ) -> Self {
        Self {
            current_state,
            content_budget,
        }
    }

    /// Return the canonical logical MeasurementSet identity.
    #[must_use]
    pub const fn measurement_set(&self) -> MeasurementSetIdentity {
        self.current_state.identity()
    }

    /// Return the explicit selected-content memory budget.
    #[must_use]
    pub const fn content_budget(&self) -> SelectedObservationContentBudget {
        self.content_budget
    }
}

/// Retained read-locked access to every source in one compiled selected observation.
///
/// Storage buffers and POINTING lookup primitives are owner-internal; callers
/// cannot use them to bypass this retained-access and terminal-completion path.
///
/// ```compile_fail
/// use casa_ms::{SelectedObservationBuffer, SelectedObservationBufferRequest};
///
/// let _ = std::mem::size_of::<SelectedObservationBuffer>();
/// let _ = std::mem::size_of::<SelectedObservationBufferRequest>();
/// ```
pub struct BoundSelectedObservation {
    identity: BoundSelectedObservationIdentity,
    measures: SelectedObservationMeasures,
    sources: Vec<BoundObservationSource>,
    access_binding: u64,
    next_traversal: u64,
}

impl BoundSelectedObservation {
    /// Open every compiled source under its fresh state probe and content budget.
    ///
    /// Caller plan order is irrelevant. Sources are retained and replayed only in the compiler's
    /// canonical read-set order.
    #[cfg(unix)]
    pub fn open(
        problem: &CompiledProblem,
        measures: SelectedObservationMeasures,
        mut bindings: Vec<ObservationSourceBinding>,
    ) -> Result<Self, BoundSelectedObservationError> {
        measures.validate_problem(problem)?;
        let expected = problem.inputs().observation_snapshot().sources();
        if bindings.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let binding_slot_bytes = bindings
            .capacity()
            .checked_mul(size_of::<ObservationSourceBinding>())
            .ok_or(BoundSelectedObservationError::BindingGraphByteOverflow)?;
        let binding_graph_initialization_bytes = bindings.iter().enumerate().try_fold(
            binding_slot_bytes,
            |bytes, (binding_index, binding)| {
                let already_accounted_rows = expected
                    .iter()
                    .map(|source| source.selection().rows())
                    .chain(
                        bindings[..binding_index]
                            .iter()
                            .map(|prior| prior.current_state.selected_rows()),
                    );
                binding
                    .current_state
                    .additional_retained_heap_bytes(already_accounted_rows)
                    .and_then(|additional| bytes.checked_add(additional))
                    .ok_or(BoundSelectedObservationError::BindingGraphByteOverflow)
            },
        )?;
        let mut sources = Vec::with_capacity(expected.len());
        let source_slots_retained_bytes = sources
            .capacity()
            .checked_mul(BoundObservationSource::retained_source_slot_bytes())
            .ok_or(BoundSelectedObservationError::SourceSlotByteOverflow)?;
        for (source_index, source) in expected.iter().enumerate() {
            let identity = source.identity();
            let Some(position) = bindings
                .iter()
                .position(|candidate| candidate.measurement_set() == identity)
            else {
                return Err(BoundSelectedObservationError::MissingSourceBinding {
                    measurement_set: identity,
                });
            };
            if bindings[position + 1..]
                .iter()
                .any(|candidate| candidate.measurement_set() == identity)
            {
                return Err(BoundSelectedObservationError::DuplicateSourceBinding {
                    measurement_set: identity,
                });
            }
            let binding = bindings.remove(position);
            let shared_bytes = if source_index == 0 {
                SelectedObservationSharedBytes::new(
                    measures.retained_bytes(),
                    source_slots_retained_bytes,
                    binding_graph_initialization_bytes,
                )
            } else {
                SelectedObservationSharedBytes::NONE
            };
            sources.push(
                BoundObservationSource::open_with_measures(
                    problem,
                    source,
                    &binding.current_state,
                    &measures,
                    shared_bytes,
                    binding.content_budget,
                )
                .map_err(|error| BoundSelectedObservationError::Source {
                    measurement_set: identity,
                    error: Box::new(error),
                })?,
            );
        }
        if !bindings.is_empty() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        measures.verify_state()?;
        let access_binding = NEXT_ACCESS_BINDING
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| BoundSelectedObservationError::AccessIdentityExhausted)?;
        Ok(Self {
            identity: BoundSelectedObservationIdentity::from_problem(problem),
            measures,
            sources,
            access_binding,
            next_traversal: 1,
        })
    }

    /// Return the compiled problem identity bound by this retained source set.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.identity.problem_id
    }

    /// Return whether this retained owner is poised for the traversal immediately after `prior`.
    ///
    /// This proves access-binding continuity without exposing the process-local binding identity.
    #[must_use]
    pub fn can_resume_after(&self, prior: &SelectedObservationCompletion) -> bool {
        self.access_binding == prior.access_binding
            && self.identity.problem_id == prior.problem_id
            && self.identity.observation_snapshot_id == prior.observation_snapshot_id
            && self.identity.observation_provenance_id == prior.observation_provenance_id
            && self.identity.commitment_id == prior.commitment_id
            && prior.traversal.checked_add(1) == Some(self.next_traversal)
    }

    #[cfg(test)]
    pub(crate) fn source_content_plan(
        &self,
        source_index: usize,
    ) -> Option<super::SelectedObservationContentPlan> {
        self.sources
            .get(source_index)
            .map(BoundObservationSource::content_plan)
    }

    #[cfg(test)]
    pub(crate) fn source_slot_allocation_bytes(&self) -> usize {
        self.sources.capacity() * BoundObservationSource::retained_source_slot_bytes()
    }

    /// Stream every source in canonical compiler order.
    pub(crate) fn selected_samples<'a>(
        &'a self,
        problem: &'a CompiledProblem,
    ) -> Result<BoundSelectedObservationSamples<'a>, BoundSelectedObservationError> {
        if !self.identity.matches(problem) {
            return Err(BoundSelectedObservationError::ProblemMismatch);
        }
        self.measures.verify_state()?;
        Ok(BoundSelectedObservationSamples {
            observation: self,
            problem,
            source_index: 0,
            current: None,
            finished: false,
        })
    }

    /// Traverse, validate, and consume every selected sample in one bounded pass.
    ///
    /// Each sample is validated before it reaches the consumer. Completion is
    /// minted only after the canonical stream's terminal poll succeeds and
    /// exhaustive coverage validation finishes.
    pub fn traverse<E>(
        &mut self,
        problem: &CompiledProblem,
        mut consume: impl FnMut(SelectedObservationTraversalSample) -> Result<(), E>,
    ) -> Result<SelectedObservationCompletion, SelectedObservationTraversalError<E>>
    where
        E: Error + 'static,
    {
        let traversal = self.next_traversal;
        let next_traversal = traversal
            .checked_add(1)
            .ok_or(SelectedObservationTraversalError::TraversalIdentityExhausted)?;
        let access_binding = self.access_binding;
        let samples = self
            .selected_samples(problem)
            .map_err(SelectedObservationTraversalError::Binding)?;
        let sources = &self.sources;
        let (generation_id, sample_count) = consume_projected_validated_stream(
            problem,
            samples,
            |sample| {
                let source = sources
                    .iter()
                    .find(|source| source.source_identity() == sample.address.measurement_set)
                    .ok_or(BoundObservationSourceError::ProblemSourceMismatch)?;
                SelectedObservationTraversalSample::from_owner(
                    problem,
                    sample,
                    source.geometry_engine(),
                )
            },
            &mut consume,
        )?;
        self.next_traversal = next_traversal;
        Ok(SelectedObservationCompletion {
            problem_id: problem.problem_id(),
            observation_snapshot_id: problem.inputs().observation_snapshot().snapshot_id(),
            observation_provenance_id: problem.inputs().observation_snapshot().provenance_id(),
            commitment_id: problem.selected_observation().commitment_id(),
            generation_id,
            sample_count,
            access_binding,
            traversal,
        })
    }
}

#[cfg(test)]
pub(super) fn consume_validated_stream<E>(
    problem: &CompiledProblem,
    samples: impl Iterator<Item = Result<SelectedObservationSample, BoundObservationSourceError>>,
    mut consume: impl FnMut(SelectedObservationSample) -> Result<(), E>,
) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationTraversalError<E>>
where
    E: Error + 'static,
{
    consume_projected_validated_stream(problem, samples, Ok, &mut consume)
}

fn consume_projected_validated_stream<E, T>(
    problem: &CompiledProblem,
    samples: impl Iterator<Item = Result<SelectedObservationSample, BoundObservationSourceError>>,
    mut project: impl FnMut(SelectedObservationSample) -> Result<T, BoundObservationSourceError>,
    mut consume: impl FnMut(T) -> Result<(), E>,
) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationTraversalError<E>>
where
    E: Error + 'static,
{
    // Iterator exhaustion performs the terminal poll before completion can be
    // minted; a terminal source error is therefore observed as an item.
    let samples = samples.map(|sample| sample.map_err(TraversalPassError::Source));
    match problem.inspect_selected_observation(samples, |sample| match project(sample) {
        Ok(projected) => consume(projected).map_err(TraversalPassError::Consumer),
        Err(error) => Err(TraversalPassError::Source(error)),
    }) {
        Ok(completion) => Ok(completion),
        Err(SelectedObservationPassError::Inspection(error)) => {
            Err(SelectedObservationTraversalError::Inspection(error))
        }
        Err(SelectedObservationPassError::External(TraversalPassError::Source(error))) => {
            Err(SelectedObservationTraversalError::Source(error))
        }
        Err(SelectedObservationPassError::External(TraversalPassError::Consumer(error))) => {
            Err(SelectedObservationTraversalError::Consumer(error))
        }
    }
}

enum TraversalPassError<E> {
    Source(BoundObservationSourceError),
    Consumer(E),
}

/// One fallible canonical sample stream over every retained source.
pub(crate) struct BoundSelectedObservationSamples<'a> {
    observation: &'a BoundSelectedObservation,
    problem: &'a CompiledProblem,
    source_index: usize,
    current: Option<BoundObservationSamples<'a>>,
    finished: bool,
}

static NEXT_ACCESS_BINDING: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BoundSelectedObservationIdentity {
    problem_id: CompiledProblemId,
    observation_snapshot_id: ObservationSnapshotId,
    observation_provenance_id: ObservationProvenanceId,
    geometry_id: CompiledGeometryId,
    commitment_id: SelectedObservationCommitmentId,
}

impl BoundSelectedObservationIdentity {
    fn from_problem(problem: &CompiledProblem) -> Self {
        Self {
            problem_id: problem.problem_id(),
            observation_snapshot_id: problem.inputs().observation_snapshot().snapshot_id(),
            observation_provenance_id: problem.inputs().observation_snapshot().provenance_id(),
            geometry_id: problem.geometry().geometry_id(),
            commitment_id: problem.selected_observation().commitment_id(),
        }
    }

    fn matches(self, problem: &CompiledProblem) -> bool {
        self == Self::from_problem(problem)
    }
}

/// Opaque owner-minted proof of one complete retained-access traversal.
///
/// This affine record is intentionally not cloneable. Content identity remains
/// separate from the logical snapshot, provenance, retained access binding,
/// and traversal attempt. It is the storage-owner half of final completion;
/// the execution adapter must combine it with the runtime's fresh
/// attempt/node/fence authority before dependent work may consume it.
#[derive(Debug)]
pub struct SelectedObservationCompletion {
    problem_id: CompiledProblemId,
    observation_snapshot_id: ObservationSnapshotId,
    observation_provenance_id: ObservationProvenanceId,
    commitment_id: SelectedObservationCommitmentId,
    generation_id: SelectedObservationGenerationId,
    sample_count: u64,
    access_binding: u64,
    traversal: u64,
}

impl SelectedObservationCompletion {
    /// Return the complete logical problem bound to this traversal.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Return the logical storage-owner snapshot commitment.
    #[must_use]
    pub const fn observation_snapshot_id(&self) -> ObservationSnapshotId {
        self.observation_snapshot_id
    }

    /// Return the locator/source provenance commitment.
    #[must_use]
    pub const fn observation_provenance_id(&self) -> ObservationProvenanceId {
        self.observation_provenance_id
    }

    /// Return the compiled selected-observation science commitment.
    #[must_use]
    pub const fn commitment_id(&self) -> SelectedObservationCommitmentId {
        self.commitment_id
    }

    /// Return the content-derived identity of the canonical selected values.
    #[must_use]
    pub const fn generation_id(&self) -> SelectedObservationGenerationId {
        self.generation_id
    }

    /// Return the exact number of validated and consumed samples.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return whether this completion came from the same retained access binding.
    #[must_use]
    pub fn same_access_binding(&self, other: &Self) -> bool {
        self.access_binding == other.access_binding
    }

    /// Return whether this completion came from an earlier traversal of the same binding.
    #[must_use]
    pub fn precedes(&self, other: &Self) -> bool {
        self.same_access_binding(other) && self.traversal < other.traversal
    }
}

/// Failure before a Selected Observation traversal could mint completion.
#[derive(Debug)]
pub enum SelectedObservationTraversalError<E> {
    /// Retained access is not bound to this compiled problem.
    Binding(BoundSelectedObservationError),
    /// The retained source failed, including on the terminal poll.
    Source(BoundObservationSourceError),
    /// One value or the final coverage proof contradicted the compiled commitment.
    Inspection(SelectedObservationInspectionError),
    /// The downstream bounded consumer rejected a validated sample.
    Consumer(E),
    /// The affine traversal identity domain was exhausted.
    TraversalIdentityExhausted,
}

impl<E> fmt::Display for SelectedObservationTraversalError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Binding(error) => error.fmt(formatter),
            Self::Source(error) => error.fmt(formatter),
            Self::Inspection(error) => error.fmt(formatter),
            Self::Consumer(_) => formatter.write_str("selected-observation consumer failed"),
            Self::TraversalIdentityExhausted => {
                formatter.write_str("selected-observation traversal identity exhausted")
            }
        }
    }
}

impl<E: Error + 'static> Error for SelectedObservationTraversalError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Binding(error) => Some(error),
            Self::Source(error) => Some(error),
            Self::Inspection(error) => Some(error),
            Self::Consumer(error) => Some(error),
            Self::TraversalIdentityExhausted => None,
        }
    }
}

impl Iterator for BoundSelectedObservationSamples<'_> {
    type Item = Result<SelectedObservationSample, BoundObservationSourceError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        loop {
            if let Some(current) = &mut self.current {
                if let Some(sample) = current.next() {
                    if sample.is_err() {
                        self.finished = true;
                    }
                    return Some(sample);
                }
                self.current = None;
                self.source_index += 1;
            }
            let Some(source) = self.observation.sources.get(self.source_index) else {
                self.finished = true;
                return None;
            };
            match source.selected_samples(self.problem) {
                Ok(samples) => self.current = Some(samples),
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

/// Failure to bind or replay a complete compiled selected observation.
#[derive(Debug, Error)]
pub enum BoundSelectedObservationError {
    /// The injected Measures provider is missing, stale, or unaccounted.
    #[error(transparent)]
    Measures(#[from] SelectedObservationMeasuresError),
    /// The supplied binding count or membership differs from the compiled source set.
    #[error("source binding set does not match the compiled selected observation")]
    BindingSetMismatch,
    /// One compiled source has no current state and budget binding.
    #[error("compiled source {measurement_set} has no retained-access binding")]
    MissingSourceBinding {
        /// Source missing a binding.
        measurement_set: MeasurementSetIdentity,
    },
    /// One compiled source was assigned more than one binding.
    #[error("compiled source {measurement_set} has duplicate retained-access bindings")]
    DuplicateSourceBinding {
        /// Source with duplicate bindings.
        measurement_set: MeasurementSetIdentity,
    },
    /// One retained source could not be bound under its state and budget.
    #[error("bind compiled source {measurement_set}: {error}")]
    Source {
        /// Source whose binding failed.
        measurement_set: MeasurementSetIdentity,
        /// Exact source-level failure.
        #[source]
        error: Box<BoundObservationSourceError>,
    },
    /// A different compiled problem was supplied for replay.
    #[error("retained selected observation belongs to a different compiled problem")]
    ProblemMismatch,
    /// The process-local retained-access identity domain was exhausted.
    #[error("selected-observation retained-access identity exhausted")]
    AccessIdentityExhausted,
    /// The retained source-slot allocation exceeded the host byte domain.
    #[error("selected-observation source-slot byte projection overflowed")]
    SourceSlotByteOverflow,
    /// The consumed source-binding graph exceeded the host byte domain.
    #[error("selected-observation binding-graph byte projection overflowed")]
    BindingGraphByteOverflow,
}
