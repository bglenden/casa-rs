// SPDX-License-Identifier: LGPL-3.0-or-later

use crate::MsReadPlan;
use casa_imaging_model::{
    CompiledProblem, CompiledProblemId, MeasurementSetIdentity, ObservationProvenanceId,
    ObservationSnapshotId, SelectedObservationCommitmentId, SelectedObservationGenerationId,
    SelectedObservationInspectionError, SelectedObservationPassError, SelectedObservationSample,
};
use std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};
use thiserror::Error;

use super::{
    BoundObservationSamples, BoundObservationSource, BoundObservationSourceError,
    SelectedObservationContentBudget,
};

/// One typed physical row plan for a compiled MeasurementSet source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObservationSourceReadPlan {
    measurement_set: MeasurementSetIdentity,
    row_plan: MsReadPlan,
    content_budget: SelectedObservationContentBudget,
}

impl ObservationSourceReadPlan {
    /// Bind a physical row plan to one canonical logical MeasurementSet identity.
    #[must_use]
    pub const fn new(
        measurement_set: MeasurementSetIdentity,
        row_plan: MsReadPlan,
        content_budget: SelectedObservationContentBudget,
    ) -> Self {
        Self {
            measurement_set,
            row_plan,
            content_budget,
        }
    }

    /// Return the canonical logical MeasurementSet identity.
    #[must_use]
    pub const fn measurement_set(self) -> MeasurementSetIdentity {
        self.measurement_set
    }

    /// Return the exact physical row plan.
    #[must_use]
    pub const fn row_plan(self) -> MsReadPlan {
        self.row_plan
    }

    /// Return the explicit selected-content memory budget.
    #[must_use]
    pub const fn content_budget(self) -> SelectedObservationContentBudget {
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
    problem_id: CompiledProblemId,
    sources: Vec<BoundObservationSource>,
    access_binding: u64,
    next_traversal: u64,
}

impl BoundSelectedObservation {
    /// Open every compiled source under its typed physical plan.
    ///
    /// Caller plan order is irrelevant. Sources are retained and replayed only in the compiler's
    /// canonical read-set order.
    #[cfg(unix)]
    pub fn open(
        problem: &CompiledProblem,
        mut plans: Vec<ObservationSourceReadPlan>,
    ) -> Result<Self, BoundSelectedObservationError> {
        let expected = problem.inputs().observation_snapshot().sources();
        if plans.len() != expected.len() {
            return Err(BoundSelectedObservationError::PlanSetMismatch);
        }
        let mut sources = Vec::with_capacity(expected.len());
        for source in expected {
            let identity = source.identity();
            let Some(position) = plans
                .iter()
                .position(|candidate| candidate.measurement_set == identity)
            else {
                return Err(BoundSelectedObservationError::MissingSourcePlan {
                    measurement_set: identity,
                });
            };
            if plans[position + 1..]
                .iter()
                .any(|candidate| candidate.measurement_set == identity)
            {
                return Err(BoundSelectedObservationError::DuplicateSourcePlan {
                    measurement_set: identity,
                });
            }
            let plan = plans.remove(position);
            sources.push(
                BoundObservationSource::open(source, plan.row_plan, plan.content_budget).map_err(
                    |error| BoundSelectedObservationError::Source {
                        measurement_set: identity,
                        error: Box::new(error),
                    },
                )?,
            );
        }
        if !plans.is_empty() {
            return Err(BoundSelectedObservationError::PlanSetMismatch);
        }
        let access_binding = NEXT_ACCESS_BINDING
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| BoundSelectedObservationError::AccessIdentityExhausted)?;
        Ok(Self {
            problem_id: problem.problem_id(),
            sources,
            access_binding,
            next_traversal: 1,
        })
    }

    /// Return the compiled problem identity bound by this retained source set.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem_id
    }

    /// Stream every source in canonical compiler order.
    pub(crate) fn selected_samples<'a>(
        &'a self,
        problem: &'a CompiledProblem,
    ) -> Result<BoundSelectedObservationSamples<'a>, BoundSelectedObservationError> {
        if problem.problem_id() != self.problem_id {
            return Err(BoundSelectedObservationError::ProblemMismatch);
        }
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
        mut consume: impl FnMut(SelectedObservationSample) -> Result<(), E>,
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
        let (generation_id, sample_count) =
            consume_validated_stream(problem, samples, &mut consume)?;
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

pub(super) fn consume_validated_stream<E>(
    problem: &CompiledProblem,
    samples: impl Iterator<Item = Result<SelectedObservationSample, BoundObservationSourceError>>,
    mut consume: impl FnMut(SelectedObservationSample) -> Result<(), E>,
) -> Result<(SelectedObservationGenerationId, u64), SelectedObservationTraversalError<E>>
where
    E: Error + 'static,
{
    // Iterator exhaustion performs the terminal poll before completion can be
    // minted; a terminal source error is therefore observed as an item.
    let samples = samples.map(|sample| sample.map_err(TraversalPassError::Source));
    match problem.inspect_selected_observation(samples, |sample| {
        consume(sample).map_err(TraversalPassError::Consumer)
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
    /// The supplied plan count or membership differs from the compiled source set.
    #[error("physical source-plan set does not match the compiled selected observation")]
    PlanSetMismatch,
    /// One compiled source has no physical plan.
    #[error("compiled source {measurement_set} has no physical row plan")]
    MissingSourcePlan {
        /// Source missing a plan.
        measurement_set: MeasurementSetIdentity,
    },
    /// One compiled source was assigned more than one physical plan.
    #[error("compiled source {measurement_set} has duplicate physical row plans")]
    DuplicateSourcePlan {
        /// Source with duplicate plans.
        measurement_set: MeasurementSetIdentity,
    },
    /// One retained source could not be bound under its plan.
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
}
