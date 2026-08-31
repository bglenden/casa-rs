// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, MeasurementSetIdentity,
    ObservationProvenanceId, ObservationSnapshotId, ObservationSourceState,
    SelectedInputWeightGroup, SelectedObservationCommitmentId, SelectedObservationGenerationId,
    SelectedObservationInspection, SelectedObservationInspectionError,
    SelectedObservationPassError, SelectedObservationRunChannel, SelectedObservationRunCorrelation,
    SelectedObservationRunRow, SelectedObservationSample, SelectedObservationSampleView,
    SelectedSpectralEvaluation,
};
use std::{
    cell::{Cell, RefCell},
    error::Error,
    fmt,
    mem::size_of,
    rc::Rc,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use thiserror::Error;

use crate::selected_observation_buffer::SelectedObservationBufferFillReport;

use super::access::{BlockVisitError, ProjectedSelectedObservationSample, SelectedRowReplay};
use super::{
    BoundObservationSamples, BoundObservationSource, BoundObservationSourceError,
    SelectedObservationBlock, SelectedObservationContentBudget, SelectedObservationMeasures,
    SelectedObservationMeasuresError,
    content_plan::SelectedObservationSharedBytes,
    maximum_selected_correlations,
    spectral_evaluation::{
        SelectedObservationTraversalRun, SelectedObservationTraversalSample,
        SpectralEvaluationProjector,
    },
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

/// Opaque storage-owner certificate for one complete selected-observation residency contract.
///
/// The certificate is derived only from the compiler's canonical source set and
/// every source binding supplied to [`BoundSelectedObservation::open`]. Callers
/// can inspect the aggregate hard bound and peak queue depth needed by a
/// scheduler, but cannot construct or alter the per-source facts that bind those
/// values to the retained owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedObservationResidencyCertificate {
    identity: BoundSelectedObservationIdentity,
    sources: Vec<SelectedObservationSourceResidency>,
    aggregate_resident_bytes: usize,
    peak_live_blocks: usize,
    maximum_pointing_polynomial_terms: usize,
}

/// Opaque owner-minted proof that one exact selected read set completed an
/// exhaustive canonical traversal.
///
/// The proof is cloneable because it contains only immutable selected-read
/// identities. It never contains or extends a retained MeasurementSet lock,
/// and it deliberately excludes the attempt-local access binding. Only
/// [`BoundSelectedObservation::rebind`] can authorize it under fresh locks.
#[derive(Clone, Debug)]
pub struct SelectedObservationReplayProof {
    inner: Arc<SelectedObservationReplayProofInner>,
}

#[derive(Debug)]
struct SelectedObservationReplayProofInner {
    identity: BoundSelectedObservationIdentity,
    sources: Vec<SelectedObservationReplaySource>,
    generation_id: SelectedObservationGenerationId,
    sample_count: u64,
}

#[derive(Clone, Debug)]
struct SelectedObservationReplaySource {
    state: ObservationSourceState,
}

impl SelectedObservationReplayProof {
    fn mint(
        identity: BoundSelectedObservationIdentity,
        sources: &[BoundObservationSource],
        generation_id: SelectedObservationGenerationId,
        sample_count: u64,
    ) -> Self {
        Self {
            inner: Arc::new(SelectedObservationReplayProofInner {
                identity,
                sources: sources
                    .iter()
                    .map(|source| SelectedObservationReplaySource {
                        state: source.selected_read_state().clone(),
                    })
                    .collect(),
                generation_id,
                sample_count,
            }),
        }
    }

    fn source_state(
        &self,
        measurement_set: MeasurementSetIdentity,
    ) -> Option<&ObservationSourceState> {
        self.inner.sources.iter().find_map(|source| {
            (source.state.identity() == measurement_set).then_some(&source.state)
        })
    }

    fn matches_problem(&self, problem: &CompiledProblem) -> bool {
        self.inner.identity.matches(problem)
    }

    fn generation_id(&self) -> SelectedObservationGenerationId {
        self.inner.generation_id
    }

    fn sample_count(&self) -> u64 {
        self.inner.sample_count
    }

    /// Return the exact additional heap retained by this shared proof graph.
    ///
    /// Selected-row manifests already owned by the compiled problem are not
    /// counted again. The shared Arc allocation, source slots, and uniquely
    /// owned generation vectors are included.
    pub fn retained_heap_bytes(&self, problem: &CompiledProblem) -> Option<usize> {
        if !self.matches_problem(problem) {
            return None;
        }
        let states = self
            .inner
            .sources
            .iter()
            .map(|source| &source.state)
            .collect::<Vec<_>>();
        Self::retained_heap_bytes_for_states(problem, &states, self.inner.sources.capacity())
    }

    fn retained_heap_bytes_for_states(
        problem: &CompiledProblem,
        states: &[&ObservationSourceState],
        source_capacity: usize,
    ) -> Option<usize> {
        let arc_header_bytes = size_of::<usize>().checked_mul(2)?;
        let mut bytes =
            arc_header_bytes.checked_add(size_of::<SelectedObservationReplayProofInner>())?;
        bytes = bytes.checked_add(
            source_capacity.checked_mul(size_of::<SelectedObservationReplaySource>())?,
        )?;
        for (source_index, state) in states.iter().enumerate() {
            let already_accounted_rows = problem
                .inputs()
                .observation_snapshot()
                .sources()
                .iter()
                .map(|source| source.selection().rows())
                .chain(
                    states[..source_index]
                        .iter()
                        .map(|prior| prior.selected_rows()),
                );
            bytes =
                bytes.checked_add(state.additional_retained_heap_bytes(already_accounted_rows)?)?;
        }
        Some(bytes)
    }
}

/// Current selected generation and count authorized only by a freshly rebound
/// exhaustive completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SelectedObservationReplayAuthorization {
    generation_id: SelectedObservationGenerationId,
    sample_count: u64,
}

impl SelectedObservationReplayAuthorization {
    /// Return the freshly rebound selected generation.
    #[must_use]
    pub const fn generation_id(self) -> SelectedObservationGenerationId {
        self.generation_id
    }

    /// Return the freshly rebound exhaustive selected sample count.
    #[must_use]
    pub const fn sample_count(self) -> u64 {
        self.sample_count
    }
}

#[derive(Clone, Debug)]
enum SelectedObservationReplayMode {
    Unproven,
    Proving,
    Rebound(SelectedObservationReplayProof),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SelectedObservationSourceResidency {
    measurement_set: MeasurementSetIdentity,
    content_budget: SelectedObservationContentBudget,
}

impl SelectedObservationResidencyCertificate {
    fn mint(
        problem: &CompiledProblem,
        bindings: &[ObservationSourceBinding],
    ) -> Result<Self, BoundSelectedObservationError> {
        let expected = problem.inputs().observation_snapshot().sources();
        if bindings.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let mut aggregate_resident_bytes = 0_usize;
        let mut peak_live_blocks = 0_usize;
        let mut maximum_pointing_polynomial_terms = 0_usize;
        let mut sources = Vec::with_capacity(expected.len());
        for source in expected {
            let measurement_set = source.identity();
            let mut matches = bindings
                .iter()
                .filter(|binding| binding.measurement_set() == measurement_set);
            let Some(binding) = matches.next() else {
                return Err(BoundSelectedObservationError::MissingSourceBinding {
                    measurement_set,
                });
            };
            if matches.next().is_some() {
                return Err(BoundSelectedObservationError::DuplicateSourceBinding {
                    measurement_set,
                });
            }
            let content_budget = binding.content_budget();
            aggregate_resident_bytes = aggregate_resident_bytes
                .checked_add(content_budget.available_bytes())
                .ok_or(BoundSelectedObservationError::ResidencyByteOverflow)?;
            peak_live_blocks = peak_live_blocks.max(content_budget.maximum_live_blocks());
            maximum_pointing_polynomial_terms = maximum_pointing_polynomial_terms
                .max(content_budget.maximum_pointing_polynomial_terms());
            sources.push(SelectedObservationSourceResidency {
                measurement_set,
                content_budget,
            });
        }
        Ok(Self {
            identity: BoundSelectedObservationIdentity::from_problem(problem),
            sources,
            aggregate_resident_bytes,
            peak_live_blocks,
            maximum_pointing_polynomial_terms,
        })
    }

    /// Return the aggregate hard byte ceiling across every retained source owner.
    #[must_use]
    pub const fn aggregate_resident_bytes(&self) -> usize {
        self.aggregate_resident_bytes
    }

    /// Return the peak simultaneously live selected-content block count.
    ///
    /// Sources are traversed serially in canonical order, so this is the maximum
    /// source-local queue depth rather than the sum of mutually exclusive depths.
    #[must_use]
    pub const fn peak_live_blocks(&self) -> usize {
        self.peak_live_blocks
    }

    /// Return the largest source-local POINTING polynomial term ceiling.
    #[must_use]
    pub const fn maximum_pointing_polynomial_terms(&self) -> usize {
        self.maximum_pointing_polynomial_terms
    }

    /// Return the exact source-local budget certified for one logical MeasurementSet.
    #[must_use]
    pub fn content_budget(
        &self,
        measurement_set: MeasurementSetIdentity,
    ) -> Option<SelectedObservationContentBudget> {
        self.sources.iter().find_map(|source| {
            (source.measurement_set == measurement_set).then_some(source.content_budget)
        })
    }

    /// Return whether this certificate belongs to the supplied compiled problem.
    #[must_use]
    pub fn matches_problem(&self, problem: &CompiledProblem) -> bool {
        self.identity.matches(problem)
    }
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

    /// Return the exact storage-owner state captured by this binding.
    #[must_use]
    pub const fn current_state(&self) -> &ObservationSourceState {
        &self.current_state
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
    residency: SelectedObservationResidencyCertificate,
    measures: SelectedObservationMeasures,
    sources: Vec<BoundObservationSource>,
    replay_mode: SelectedObservationReplayMode,
    access_binding: u64,
    next_traversal: u64,
}

impl BoundSelectedObservation {
    /// Mint the opaque aggregate residency contract for a complete source-binding set.
    ///
    /// The same canonical derivation is repeated and retained by [`Self::open`],
    /// allowing a scheduler to plan before opening while execution still fails
    /// closed if a different owner or budget set is later supplied.
    pub fn certify_residency(
        problem: &CompiledProblem,
        bindings: &[ObservationSourceBinding],
    ) -> Result<SelectedObservationResidencyCertificate, BoundSelectedObservationError> {
        SelectedObservationResidencyCertificate::mint(problem, bindings)
    }

    pub(crate) fn replay_proof_retained_heap_bytes(
        problem: &CompiledProblem,
        bindings: &[ObservationSourceBinding],
    ) -> Result<usize, BoundSelectedObservationError> {
        let expected = problem.inputs().observation_snapshot().sources();
        if bindings.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let mut states = Vec::with_capacity(expected.len());
        for source in expected {
            let mut matching = bindings
                .iter()
                .filter(|binding| binding.measurement_set() == source.identity());
            let Some(binding) = matching.next() else {
                return Err(BoundSelectedObservationError::MissingSourceBinding {
                    measurement_set: source.identity(),
                });
            };
            if matching.next().is_some() {
                return Err(BoundSelectedObservationError::DuplicateSourceBinding {
                    measurement_set: source.identity(),
                });
            }
            states.push(&binding.current_state);
        }
        SelectedObservationReplayProof::retained_heap_bytes_for_states(
            problem,
            &states,
            states.capacity(),
        )
        .ok_or(BoundSelectedObservationError::ReplayProofByteOverflow)
    }

    fn shared_bytes(
        problem: &CompiledProblem,
        measures: &SelectedObservationMeasures,
        bindings: &[ObservationSourceBinding],
        binding_capacity: usize,
        source_capacity: usize,
    ) -> Result<SelectedObservationSharedBytes, BoundSelectedObservationError> {
        let expected = problem.inputs().observation_snapshot().sources();
        let binding_slot_bytes = binding_capacity
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
        let source_slots_retained_bytes = source_capacity
            .checked_mul(BoundObservationSource::retained_source_slot_bytes())
            .ok_or(BoundSelectedObservationError::SourceSlotByteOverflow)?;
        Ok(SelectedObservationSharedBytes::new(
            measures.retained_bytes(),
            source_slots_retained_bytes,
            binding_graph_initialization_bytes,
        ))
    }

    /// Open every compiled source under its fresh state probe and content budget.
    ///
    /// Caller plan order is irrelevant. Sources are retained and replayed only in the compiler's
    /// canonical read-set order.
    #[cfg(unix)]
    pub fn open(
        problem: &CompiledProblem,
        measures: SelectedObservationMeasures,
        bindings: Vec<ObservationSourceBinding>,
    ) -> Result<Self, BoundSelectedObservationError> {
        Self::open_internal(problem, measures, bindings, false)
    }

    /// Open a proof-eligible owner after rederiving every source state under
    /// fresh retained locks.
    #[cfg(unix)]
    pub(crate) fn open_owner_validated(
        problem: &CompiledProblem,
        measures: SelectedObservationMeasures,
        bindings: Vec<ObservationSourceBinding>,
    ) -> Result<Self, BoundSelectedObservationError> {
        Self::open_internal(problem, measures, bindings, true)
    }

    #[cfg(unix)]
    fn open_internal(
        problem: &CompiledProblem,
        measures: SelectedObservationMeasures,
        mut bindings: Vec<ObservationSourceBinding>,
        owner_validated: bool,
    ) -> Result<Self, BoundSelectedObservationError> {
        measures.validate_problem(problem)?;
        let residency = SelectedObservationResidencyCertificate::mint(problem, &bindings)?;
        let expected = problem.inputs().observation_snapshot().sources();
        if bindings.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let mut sources = Vec::with_capacity(expected.len());
        let first_source_shared_bytes = Self::shared_bytes(
            problem,
            &measures,
            &bindings,
            bindings.capacity(),
            sources.capacity(),
        )?;
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
                first_source_shared_bytes
            } else {
                SelectedObservationSharedBytes::NONE
            };
            let opened = if owner_validated {
                BoundObservationSource::open_owner_validated_with_measures(
                    problem,
                    source,
                    &binding.current_state,
                    &measures,
                    shared_bytes,
                    binding.content_budget,
                )
            } else {
                BoundObservationSource::open_with_measures(
                    problem,
                    source,
                    &binding.current_state,
                    &measures,
                    shared_bytes,
                    binding.content_budget,
                )
            };
            sources.push(
                opened.map_err(|error| BoundSelectedObservationError::Source {
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
            residency,
            measures,
            sources,
            replay_mode: if owner_validated {
                SelectedObservationReplayMode::Proving
            } else {
                SelectedObservationReplayMode::Unproven
            },
            access_binding,
            next_traversal: 1,
        })
    }

    /// Reopen every compiled source under fresh retained locks and authorize a
    /// prior exhaustive replay proof for this new access binding.
    ///
    /// Owner-manifest, physical modification counters, selected physical rows,
    /// and selected read generations are rederived under the new locks before
    /// this function returns, so no block can be emitted on a mismatch.
    #[cfg(unix)]
    pub(crate) fn rebind(
        problem: &CompiledProblem,
        measures: SelectedObservationMeasures,
        mut bindings: Vec<ObservationSourceBinding>,
        proof: &SelectedObservationReplayProof,
    ) -> Result<Self, BoundSelectedObservationError> {
        measures.validate_problem(problem)?;
        if !proof.matches_problem(problem) {
            return Err(BoundSelectedObservationError::ReplayProofMismatch);
        }
        let expected = problem.inputs().observation_snapshot().sources();
        if bindings.len() != expected.len() || proof.inner.sources.len() != expected.len() {
            return Err(BoundSelectedObservationError::BindingSetMismatch);
        }
        let residency = SelectedObservationResidencyCertificate::mint(problem, &bindings)?;
        let mut sources = Vec::with_capacity(expected.len());
        let first_source_shared_bytes = Self::shared_bytes(
            problem,
            &measures,
            &bindings,
            bindings.capacity(),
            sources.capacity(),
        )?;
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
            let Some(prior_state) = proof.source_state(identity) else {
                return Err(BoundSelectedObservationError::ReplayProofMismatch);
            };
            let binding = bindings.remove(position);
            let shared_bytes = if source_index == 0 {
                first_source_shared_bytes
            } else {
                SelectedObservationSharedBytes::NONE
            };
            sources.push(
                BoundObservationSource::rebind_with_measures(
                    problem,
                    source,
                    &binding.current_state,
                    prior_state,
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
            residency,
            measures,
            sources,
            replay_mode: SelectedObservationReplayMode::Rebound(proof.clone()),
            access_binding,
            next_traversal: 1,
        })
    }

    /// Return the compiled problem identity bound by this retained source set.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.identity.problem_id
    }

    /// Return the exact aggregate residency certificate retained by this owner.
    #[must_use]
    pub const fn residency_certificate(&self) -> &SelectedObservationResidencyCertificate {
        &self.residency
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
        let measurements = Rc::new(RefCell::new(
            SelectedObservationTraversalMeasurementsBuilder::default(),
        ));
        Ok(BoundSelectedObservationSamples {
            observation: self,
            problem,
            source_index: 0,
            current: None,
            finished: false,
            measurements,
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
        mut consume: impl FnMut(SelectedObservationTraversalSample<'_>) -> Result<(), E>,
    ) -> Result<SelectedObservationCompletion, SelectedObservationTraversalError<E>>
    where
        E: Error + 'static,
    {
        let traversal = self.next_traversal;
        let next_traversal = traversal
            .checked_add(1)
            .ok_or(SelectedObservationTraversalError::TraversalIdentityExhausted)?;
        let access_binding = self.access_binding;
        let mut samples = self
            .selected_samples(problem)
            .map_err(SelectedObservationTraversalError::Binding)?;
        let measurements = Rc::clone(&samples.measurements);
        let sources = &self.sources;
        let mut spectral_evaluator = SpectralEvaluationProjector::new();
        let pending_weight_group = Cell::new(None);
        let selected = std::iter::from_fn(|| {
            samples.next_projected().map(|projected| {
                projected
                    .map(|projected| {
                        pending_weight_group.set(Some(projected.input_weight_group));
                        projected.selected
                    })
                    .map_err(TraversalPassError::Source)
            })
        });
        let (generation_id, sample_count) =
            match problem.inspect_selected_observation(selected, |sample| {
                let input_weight_group = pending_weight_group
                    .take()
                    .ok_or(BoundObservationSourceError::StoredSampleShapeMismatch)
                    .map_err(TraversalPassError::Source)?;
                let source = sources
                    .iter()
                    .find(|source| source.source_identity() == sample.address.measurement_set)
                    .ok_or(BoundObservationSourceError::ProblemSourceMismatch)
                    .map_err(TraversalPassError::Source)?;
                let projected = spectral_evaluator
                    .project(
                        problem,
                        sample.as_view().with_input_weight_group(input_weight_group),
                        source.geometry_engine(),
                    )
                    .map_err(TraversalPassError::Source)?;
                consume(projected).map_err(TraversalPassError::Consumer)
            }) {
                Ok(completion) => completion,
                Err(SelectedObservationPassError::Inspection(error)) => {
                    return Err(SelectedObservationTraversalError::Inspection(error));
                }
                Err(SelectedObservationPassError::External(TraversalPassError::Source(error))) => {
                    return Err(SelectedObservationTraversalError::Source(error));
                }
                Err(SelectedObservationPassError::External(TraversalPassError::Consumer(
                    error,
                ))) => {
                    return Err(SelectedObservationTraversalError::Consumer(error));
                }
            };
        let measurements = measurements
            .borrow()
            .finish(sample_count)
            .ok_or(SelectedObservationTraversalError::MeasurementOverflow)?;
        let (replay_proof, rebound) = match &self.replay_mode {
            SelectedObservationReplayMode::Unproven => (None, false),
            SelectedObservationReplayMode::Proving => (
                Some(SelectedObservationReplayProof::mint(
                    self.identity,
                    &self.sources,
                    generation_id,
                    sample_count,
                )),
                false,
            ),
            SelectedObservationReplayMode::Rebound(proof) => {
                if proof.generation_id() != generation_id || proof.sample_count() != sample_count {
                    return Err(SelectedObservationTraversalError::Binding(
                        BoundSelectedObservationError::ReplayProofMismatch,
                    ));
                }
                (Some(proof.clone()), true)
            }
        };
        self.next_traversal = next_traversal;
        Ok(SelectedObservationCompletion {
            problem_id: problem.problem_id(),
            observation_snapshot_id: problem.inputs().observation_snapshot().snapshot_id(),
            observation_provenance_id: problem.inputs().observation_snapshot().provenance_id(),
            commitment_id: problem.selected_observation().commitment_id(),
            generation_id,
            sample_count,
            measurements,
            access_binding,
            traversal,
            replay_proof,
            rebound,
        })
    }

    /// Split one retained traversal into an ordered refillable block source and
    /// its sole validating consumer.
    pub fn into_block_stream<'a>(
        self,
        problem: &'a CompiledProblem,
    ) -> Result<
        (
            SelectedObservationBlockSource<'a>,
            SelectedObservationBlockConsumer<'a>,
        ),
        BoundSelectedObservationError,
    > {
        if !self.identity.matches(problem) {
            return Err(BoundSelectedObservationError::ProblemMismatch);
        }
        self.measures.verify_state()?;
        let traversal = self.next_traversal;
        traversal
            .checked_add(1)
            .ok_or(BoundSelectedObservationError::TraversalIdentityExhausted)?;
        let maximum_rows = self
            .sources
            .iter()
            .map(BoundObservationSource::rows_per_block)
            .max()
            .unwrap_or(0);
        let identity = self.identity;
        let access_binding = self.access_binding;
        let next_traversal = traversal
            .checked_add(1)
            .ok_or(BoundSelectedObservationError::TraversalIdentityExhausted)?;
        let maximum_correlations = maximum_selected_correlations(problem);
        let replay_mode = self.replay_mode;
        let rebound = match &replay_mode {
            SelectedObservationReplayMode::Rebound(proof) => Some(proof.clone()),
            SelectedObservationReplayMode::Unproven | SelectedObservationReplayMode::Proving => {
                None
            }
        };
        Ok((
            SelectedObservationBlockSource {
                problem,
                residency: self.residency,
                measures: self.measures,
                sources: self.sources,
                source_index: 0,
                row_replay: None,
                source_pass_recorded: false,
                exhausted: false,
                maximum_rows,
                measurements: SelectedObservationTraversalMeasurementsBuilder::for_run_handoff(),
                identity,
                access_binding,
                traversal,
                next_traversal,
                replay_mode,
            },
            SelectedObservationBlockConsumer {
                problem,
                inspection: rebound
                    .is_none()
                    .then(|| problem.begin_selected_observation_inspection()),
                rebound,
                rebound_sample_count: 0,
                spectral_evaluator: SpectralEvaluationProjector::new(),
                correlations: Vec::with_capacity(maximum_correlations),
                evaluations: Vec::with_capacity(maximum_correlations),
                peak_scratch_current_bytes: 0,
            },
        ))
    }
}

/// Ordered refillable source for one retained selected-observation pass.
pub struct SelectedObservationBlockSource<'a> {
    problem: &'a CompiledProblem,
    residency: SelectedObservationResidencyCertificate,
    measures: SelectedObservationMeasures,
    sources: Vec<BoundObservationSource>,
    source_index: usize,
    row_replay: Option<SelectedRowReplay>,
    source_pass_recorded: bool,
    exhausted: bool,
    maximum_rows: usize,
    measurements: SelectedObservationTraversalMeasurementsBuilder,
    identity: BoundSelectedObservationIdentity,
    access_binding: u64,
    traversal: u64,
    next_traversal: u64,
    replay_mode: SelectedObservationReplayMode,
}

impl SelectedObservationBlockSource<'_> {
    /// Create one empty source-owned storage slot.
    #[must_use]
    pub fn create_storage(&self, slot: usize) -> SelectedObservationBlock {
        SelectedObservationBlock::new(slot, self.maximum_rows)
    }

    /// Fill the next canonical block, returning its source ordinal when ready.
    pub fn fill_next(
        &mut self,
        block: &mut SelectedObservationBlock,
    ) -> Result<Option<u32>, BoundObservationSourceError> {
        if self.exhausted {
            return Ok(None);
        }
        loop {
            let Some(source) = self.sources.get(self.source_index) else {
                self.exhausted = true;
                return Ok(None);
            };
            if !self.source_pass_recorded {
                self.measurements.record_source_pass()?;
                self.source_pass_recorded = true;
            }
            let logical_source = self
                .problem
                .selected_observation()
                .read_set()
                .sources()
                .get(self.source_index)
                .ok_or(BoundObservationSourceError::ProblemSourceMismatch)?;
            if self.row_replay.is_none() {
                self.row_replay = Some(source.selected_row_replay()?);
            }
            if source.fill_next_selected_block(
                self.problem,
                logical_source,
                self.row_replay
                    .as_mut()
                    .expect("selected-row replay initialized for current source"),
                block,
                &mut self.measurements,
            )? {
                return u32::try_from(self.source_index)
                    .map(Some)
                    .map_err(|_| BoundObservationSourceError::MeasurementOverflow);
            }
            self.source_index += 1;
            self.row_replay = None;
            self.source_pass_recorded = false;
        }
    }

    /// Mint terminal source proof only after the canonical terminal poll.
    pub fn complete(self) -> Result<SelectedObservationTerminal, BoundObservationSourceError> {
        if !self.exhausted {
            return Err(BoundObservationSourceError::IncompleteBlockTraversal);
        }
        Ok(SelectedObservationTerminal {
            identity: self.identity,
            residency: self.residency,
            measures: self.measures,
            sources: self.sources,
            access_binding: self.access_binding,
            traversal: self.traversal,
            next_traversal: self.next_traversal,
            measurements: self.measurements,
            replay_mode: self.replay_mode,
        })
    }
}

/// Sole incremental validator/projector for blocks from one source stream.
pub struct SelectedObservationBlockConsumer<'a> {
    problem: &'a CompiledProblem,
    inspection: Option<SelectedObservationInspection<'a>>,
    rebound: Option<SelectedObservationReplayProof>,
    rebound_sample_count: u64,
    spectral_evaluator: SpectralEvaluationProjector,
    correlations: Vec<SelectedObservationRunCorrelation>,
    evaluations: Vec<SelectedSpectralEvaluation>,
    peak_scratch_current_bytes: usize,
}

impl SelectedObservationBlockConsumer<'_> {
    /// Return bytes handed to the selected-generation hasher so far.
    #[must_use]
    pub const fn generation_proof_bytes(&self) -> u64 {
        match &self.inspection {
            Some(inspection) => inspection.generation_proof_bytes(),
            None => 0,
        }
    }

    /// Return selected-generation hasher update calls so far.
    #[must_use]
    pub const fn generation_proof_hash_calls(&self) -> u64 {
        match &self.inspection {
            Some(inspection) => inspection.generation_proof_hash_calls(),
            None => 0,
        }
    }

    /// Validate and consume every row/channel run in one opaque block.
    pub fn consume<E: Error + 'static>(
        &mut self,
        block: &SelectedObservationBlock,
        mut consume: impl FnMut(SelectedObservationTraversalRun<'_>) -> Result<(), E>,
    ) -> Result<(), SelectedObservationTraversalError<E>> {
        let inspection = &mut self.inspection;
        let rebound_sample_count = &mut self.rebound_sample_count;
        let spectral_evaluator = &mut self.spectral_evaluator;
        let correlations = &mut self.correlations;
        let evaluations = &mut self.evaluations;
        let peak_scratch_current_bytes = &mut self.peak_scratch_current_bytes;
        block
            .visit_selected_samples(
                self.problem,
                correlations,
                |row, channel, correlations, geometry_engine| {
                    evaluations.clear();
                    if evaluations.capacity() < correlations.len() {
                        return Err(SelectedObservationTraversalError::Source(
                            BoundObservationSourceError::StoredSampleShapeMismatch,
                        ));
                    }
                    if let Some(inspection) = inspection {
                        inspection
                            .push_run(row, &channel, correlations)
                            .map_err(SelectedObservationTraversalError::Inspection)?;
                    } else {
                        *rebound_sample_count = rebound_sample_count
                            .checked_add(u64::try_from(correlations.len()).map_err(|_| {
                                SelectedObservationTraversalError::MeasurementOverflow
                            })?)
                            .ok_or(SelectedObservationTraversalError::MeasurementOverflow)?;
                    }
                    for correlation in correlations {
                        let sample =
                            SelectedObservationSampleView::from_run(row, &channel, correlation);
                        evaluations.push(
                            spectral_evaluator
                                .project(self.problem, sample, geometry_engine)
                                .map_err(SelectedObservationTraversalError::Source)?
                                .spectral_evaluation(),
                        );
                    }
                    *peak_scratch_current_bytes = (*peak_scratch_current_bytes).max(
                        correlations
                            .len()
                            .checked_mul(size_of::<SelectedObservationRunCorrelation>())
                            .and_then(|bytes| {
                                evaluations
                                    .len()
                                    .checked_mul(size_of::<SelectedSpectralEvaluation>())
                                    .and_then(|evaluations| bytes.checked_add(evaluations))
                            })
                            .and_then(|bytes| {
                                bytes.checked_add(size_of::<SelectedInputWeightGroup>())
                            })
                            .ok_or(SelectedObservationTraversalError::MeasurementOverflow)?,
                    );
                    consume(SelectedObservationTraversalRun::new(
                        row,
                        channel,
                        correlations,
                        evaluations.as_slice(),
                    ))
                    .map_err(SelectedObservationTraversalError::Consumer)
                },
            )
            .map_err(|error| match error {
                BlockVisitError::Source(error) => SelectedObservationTraversalError::Source(error),
                BlockVisitError::Consumer(error) => error,
            })
    }

    /// Finish exhaustive inspection and combine it with terminal source proof.
    pub fn complete(
        self,
        terminal: SelectedObservationTerminal,
    ) -> Result<
        (BoundSelectedObservation, SelectedObservationCompletion),
        SelectedObservationTraversalError<std::convert::Infallible>,
    > {
        let scratch_capacity_bytes = self
            .correlations
            .capacity()
            .checked_mul(size_of::<SelectedObservationRunCorrelation>())
            .and_then(|bytes| {
                self.evaluations
                    .capacity()
                    .checked_mul(size_of::<SelectedSpectralEvaluation>())
                    .and_then(|evaluations| bytes.checked_add(evaluations))
            })
            .and_then(|bytes| bytes.checked_add(size_of::<SelectedInputWeightGroup>()))
            .ok_or(SelectedObservationTraversalError::MeasurementOverflow)?;
        let peak_scratch_current_bytes = self.peak_scratch_current_bytes;
        let rebound_sample_count = self.rebound_sample_count;
        let (generation_id, sample_count) = match (self.inspection, self.rebound.as_ref()) {
            (Some(inspection), None) => inspection
                .finish()
                .map_err(SelectedObservationTraversalError::Inspection)?,
            (None, Some(proof)) if rebound_sample_count == proof.sample_count() => {
                (proof.generation_id(), rebound_sample_count)
            }
            _ => {
                return Err(SelectedObservationTraversalError::Binding(
                    BoundSelectedObservationError::ReplayProofMismatch,
                ));
            }
        };
        let mut measurements = terminal.measurements;
        measurements
            .record_consumer_scratch(peak_scratch_current_bytes, scratch_capacity_bytes)
            .map_err(SelectedObservationTraversalError::Source)?;
        let measurements = measurements
            .finish(sample_count)
            .ok_or(SelectedObservationTraversalError::MeasurementOverflow)?;
        let (replay_proof, rebound) = match &terminal.replay_mode {
            SelectedObservationReplayMode::Unproven => (None, false),
            SelectedObservationReplayMode::Proving => (
                Some(SelectedObservationReplayProof::mint(
                    terminal.identity,
                    &terminal.sources,
                    generation_id,
                    sample_count,
                )),
                false,
            ),
            SelectedObservationReplayMode::Rebound(proof)
                if proof.generation_id() == generation_id
                    && proof.sample_count() == sample_count
                    && self
                        .rebound
                        .as_ref()
                        .is_some_and(|consumer| Arc::ptr_eq(&consumer.inner, &proof.inner)) =>
            {
                (Some(proof.clone()), true)
            }
            SelectedObservationReplayMode::Rebound(_) => {
                return Err(SelectedObservationTraversalError::Binding(
                    BoundSelectedObservationError::ReplayProofMismatch,
                ));
            }
        };
        let completion = SelectedObservationCompletion {
            problem_id: terminal.identity.problem_id,
            observation_snapshot_id: terminal.identity.observation_snapshot_id,
            observation_provenance_id: terminal.identity.observation_provenance_id,
            commitment_id: terminal.identity.commitment_id,
            generation_id,
            sample_count,
            measurements,
            access_binding: terminal.access_binding,
            traversal: terminal.traversal,
            replay_proof,
            rebound,
        };
        let selected = BoundSelectedObservation {
            identity: terminal.identity,
            residency: terminal.residency,
            measures: terminal.measures,
            sources: terminal.sources,
            replay_mode: terminal.replay_mode,
            access_binding: terminal.access_binding,
            next_traversal: terminal.next_traversal,
        };
        Ok((selected, completion))
    }
}

/// Source-owner proof that the ordered block source reached its terminal poll.
pub struct SelectedObservationTerminal {
    identity: BoundSelectedObservationIdentity,
    residency: SelectedObservationResidencyCertificate,
    measures: SelectedObservationMeasures,
    sources: Vec<BoundObservationSource>,
    access_binding: u64,
    traversal: u64,
    next_traversal: u64,
    measurements: SelectedObservationTraversalMeasurementsBuilder,
    replay_mode: SelectedObservationReplayMode,
}

impl SelectedObservationTerminal {
    /// Record runtime-observed simultaneous source-slot residency before validation closes.
    pub fn record_runtime_residency(
        &mut self,
        peak_live_blocks: usize,
        peak_live_current_bytes: u64,
        peak_live_capacity_bytes: u64,
    ) -> Result<(), BoundObservationSourceError> {
        self.measurements.record_live_blocks(
            u64::try_from(peak_live_blocks)
                .map_err(|_| BoundObservationSourceError::MeasurementOverflow)?,
            peak_live_current_bytes,
            peak_live_capacity_bytes,
        );
        Ok(())
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

#[cfg(test)]
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
    measurements: Rc<RefCell<SelectedObservationTraversalMeasurementsBuilder>>,
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

/// Immutable physical measurements for one completed selected-observation traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectedObservationTraversalMeasurements {
    source_pass_count: u64,
    block_count: u64,
    stored_row_count: u64,
    stored_sample_count: u64,
    logical_output_bytes: u64,
    modeled_physical_read_bytes: Option<u64>,
    source_read_operations: u64,
    request_handoff_bytes: u64,
    selected_sample_count: u64,
    selected_channel_run_count: u64,
    selected_sample_handoff_bytes: u64,
    peak_consumer_scratch_current_bytes: u64,
    consumer_scratch_capacity_bytes: u64,
    allocated_storage_buffers: u64,
    reused_storage_buffers: u64,
    peak_live_blocks: u64,
    peak_live_current_bytes: u64,
    peak_live_capacity_bytes: u64,
    source_read_nanos: u128,
    source_fill_nanos: u128,
    source_arrangement_nanos: u128,
}

macro_rules! traversal_measurement_getter {
    ($name:ident, $field:ident, $type:ty, $doc:literal) => {
        #[doc = $doc]
        #[must_use]
        pub const fn $name(&self) -> $type {
            self.$field
        }
    };
}

impl SelectedObservationTraversalMeasurements {
    traversal_measurement_getter!(
        source_pass_count,
        source_pass_count,
        u64,
        "Return the number of MeasurementSet sources traversed exactly once."
    );
    traversal_measurement_getter!(
        block_count,
        block_count,
        u64,
        "Return the number of bounded storage blocks filled."
    );
    traversal_measurement_getter!(
        stored_row_count,
        stored_row_count,
        u64,
        "Return the aggregate stored MAIN rows read across blocks."
    );
    traversal_measurement_getter!(
        stored_sample_count,
        stored_sample_count,
        u64,
        "Return packed stored channel-correlation samples read across blocks."
    );
    traversal_measurement_getter!(
        logical_output_bytes,
        logical_output_bytes,
        u64,
        "Return exact logical bytes produced by the closed storage-column reads."
    );
    traversal_measurement_getter!(
        modeled_physical_read_bytes,
        modeled_physical_read_bytes,
        Option<u64>,
        "Return modeled physical bytes only when every fill exposes a trustworthy model."
    );
    traversal_measurement_getter!(
        source_read_operations,
        source_read_operations,
        u64,
        "Return the exact closed-column read operation count."
    );
    traversal_measurement_getter!(
        request_handoff_bytes,
        request_handoff_bytes,
        u64,
        "Return bytes copied while handing requested physical rows into retained blocks."
    );
    traversal_measurement_getter!(
        selected_sample_count,
        selected_sample_count,
        u64,
        "Return the exact selected correlation-sample count handed downstream."
    );
    traversal_measurement_getter!(
        selected_channel_run_count,
        selected_channel_run_count,
        u64,
        "Return the exact selected row/channel run count handed downstream."
    );
    traversal_measurement_getter!(
        selected_sample_handoff_bytes,
        selected_sample_handoff_bytes,
        u64,
        "Return semantic bytes handed to the downstream sample consumer."
    );
    traversal_measurement_getter!(
        peak_consumer_scratch_current_bytes,
        peak_consumer_scratch_current_bytes,
        u64,
        "Return peak populated bytes in reusable run-correlation consumer scratch."
    );
    traversal_measurement_getter!(
        consumer_scratch_capacity_bytes,
        consumer_scratch_capacity_bytes,
        u64,
        "Return allocated capacity bytes in reusable run-correlation consumer scratch."
    );
    traversal_measurement_getter!(
        allocated_storage_buffers,
        allocated_storage_buffers,
        u64,
        "Return storage-backed vectors allocated by successful fills."
    );
    traversal_measurement_getter!(
        reused_storage_buffers,
        reused_storage_buffers,
        u64,
        "Return storage-backed vectors reused by successful fills."
    );
    traversal_measurement_getter!(
        peak_live_blocks,
        peak_live_blocks,
        u64,
        "Return the observed high-water count of simultaneously live blocks."
    );
    traversal_measurement_getter!(
        peak_live_current_bytes,
        peak_live_current_bytes,
        u64,
        "Return peak current bytes in simultaneously live block payloads."
    );
    traversal_measurement_getter!(
        peak_live_capacity_bytes,
        peak_live_capacity_bytes,
        u64,
        "Return peak allocated capacity bytes in simultaneously live block payloads."
    );
    traversal_measurement_getter!(
        source_read_nanos,
        source_read_nanos,
        u128,
        "Return wall nanoseconds spent in closed storage reads."
    );
    traversal_measurement_getter!(
        source_fill_nanos,
        source_fill_nanos,
        u128,
        "Return wall nanoseconds spent in complete selected-buffer fills."
    );
    traversal_measurement_getter!(
        source_arrangement_nanos,
        source_arrangement_nanos,
        u128,
        "Return wall nanoseconds spent arranging block-level source geometry."
    );
}

pub(super) struct SelectedObservationTraversalMeasurementsBuilder {
    measurements: SelectedObservationTraversalMeasurements,
    physical_model_complete: bool,
    selected_channel_run_count: Option<u64>,
}

impl Default for SelectedObservationTraversalMeasurementsBuilder {
    fn default() -> Self {
        Self {
            measurements: SelectedObservationTraversalMeasurements {
                source_pass_count: 0,
                block_count: 0,
                stored_row_count: 0,
                stored_sample_count: 0,
                logical_output_bytes: 0,
                modeled_physical_read_bytes: Some(0),
                source_read_operations: 0,
                request_handoff_bytes: 0,
                selected_sample_count: 0,
                selected_channel_run_count: 0,
                selected_sample_handoff_bytes: 0,
                peak_consumer_scratch_current_bytes: 0,
                consumer_scratch_capacity_bytes: 0,
                allocated_storage_buffers: 0,
                reused_storage_buffers: 0,
                peak_live_blocks: 0,
                peak_live_current_bytes: 0,
                peak_live_capacity_bytes: 0,
                source_read_nanos: 0,
                source_fill_nanos: 0,
                source_arrangement_nanos: 0,
            },
            physical_model_complete: true,
            selected_channel_run_count: None,
        }
    }
}

impl SelectedObservationTraversalMeasurementsBuilder {
    fn for_run_handoff() -> Self {
        Self {
            selected_channel_run_count: Some(0),
            ..Self::default()
        }
    }

    pub(super) fn record_source_pass(&mut self) -> Result<(), BoundObservationSourceError> {
        self.measurements.source_pass_count = self
            .measurements
            .source_pass_count
            .checked_add(1)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
        Ok(())
    }

    pub(super) fn record_fill(
        &mut self,
        report: SelectedObservationBufferFillReport,
    ) -> Result<(), BoundObservationSourceError> {
        macro_rules! add {
            ($field:ident, $value:expr) => {
                self.measurements.$field = self
                    .measurements
                    .$field
                    .checked_add($value)
                    .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
            };
        }
        add!(block_count, report.block_count);
        add!(stored_row_count, report.row_count);
        add!(stored_sample_count, report.sample_count);
        add!(logical_output_bytes, report.logical_output_bytes);
        add!(source_read_operations, report.read_operation_count);
        add!(request_handoff_bytes, report.request_handoff_bytes);
        add!(
            allocated_storage_buffers,
            report.allocation.allocated_storage_buffers
        );
        add!(
            reused_storage_buffers,
            report.allocation.reused_storage_buffers
        );
        add!(source_read_nanos, report.timings.storage_read_nanos());
        add!(source_fill_nanos, report.timings.total_fill_nanos);
        match (
            self.measurements.modeled_physical_read_bytes,
            report.modeled_physical_read_bytes,
        ) {
            (Some(total), Some(bytes)) if self.physical_model_complete => {
                self.measurements.modeled_physical_read_bytes = Some(
                    total
                        .checked_add(bytes)
                        .ok_or(BoundObservationSourceError::MeasurementOverflow)?,
                );
            }
            _ => {
                self.physical_model_complete = false;
                self.measurements.modeled_physical_read_bytes = None;
            }
        }
        Ok(())
    }

    pub(super) fn record_arrangement_nanos(
        &mut self,
        nanos: u128,
    ) -> Result<(), BoundObservationSourceError> {
        self.measurements.source_arrangement_nanos = self
            .measurements
            .source_arrangement_nanos
            .checked_add(nanos)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
        Ok(())
    }

    pub(super) fn record_selected_channel_runs(
        &mut self,
        row_count: u64,
        channel_count: usize,
    ) -> Result<(), BoundObservationSourceError> {
        let count = row_count
            .checked_mul(
                u64::try_from(channel_count)
                    .map_err(|_| BoundObservationSourceError::MeasurementOverflow)?,
            )
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
        let total = self
            .selected_channel_run_count
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?
            .checked_add(count)
            .ok_or(BoundObservationSourceError::MeasurementOverflow)?;
        self.selected_channel_run_count = Some(total);
        Ok(())
    }

    fn record_consumer_scratch(
        &mut self,
        peak_current_bytes: usize,
        capacity_bytes: usize,
    ) -> Result<(), BoundObservationSourceError> {
        self.measurements.peak_consumer_scratch_current_bytes =
            u64::try_from(peak_current_bytes)
                .map_err(|_| BoundObservationSourceError::MeasurementOverflow)?;
        self.measurements.consumer_scratch_capacity_bytes = u64::try_from(capacity_bytes)
            .map_err(|_| BoundObservationSourceError::MeasurementOverflow)?;
        Ok(())
    }

    pub(super) fn record_live_blocks(
        &mut self,
        blocks: u64,
        current_bytes: u64,
        capacity_bytes: u64,
    ) {
        self.measurements.peak_live_blocks = self.measurements.peak_live_blocks.max(blocks);
        self.measurements.peak_live_current_bytes =
            self.measurements.peak_live_current_bytes.max(current_bytes);
        self.measurements.peak_live_capacity_bytes = self
            .measurements
            .peak_live_capacity_bytes
            .max(capacity_bytes);
    }

    fn finish(&self, sample_count: u64) -> Option<SelectedObservationTraversalMeasurements> {
        let mut measurements = self.measurements;
        measurements.selected_sample_count = sample_count;
        measurements.selected_channel_run_count = self.selected_channel_run_count.unwrap_or(0);
        measurements.selected_sample_handoff_bytes =
            if let Some(channel_runs) = self.selected_channel_run_count {
                measurements
                    .stored_row_count
                    .checked_mul(u64::try_from(size_of::<SelectedObservationRunRow>()).ok()?)?
                    .checked_add(channel_runs.checked_mul(
                        u64::try_from(size_of::<SelectedObservationRunChannel>()).ok()?,
                    )?)?
                    .checked_add(
                        sample_count.checked_mul(
                            u64::try_from(
                                size_of::<SelectedObservationRunCorrelation>()
                                    .checked_add(size_of::<SelectedSpectralEvaluation>())?,
                            )
                            .ok()?,
                        )?,
                    )?
            } else {
                sample_count.checked_mul(
                    u64::try_from(size_of::<SelectedObservationTraversalSample<'static>>()).ok()?,
                )?
            };
        Some(measurements)
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
    measurements: SelectedObservationTraversalMeasurements,
    access_binding: u64,
    traversal: u64,
    replay_proof: Option<SelectedObservationReplayProof>,
    rebound: bool,
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

    /// Return immutable physical execution measurements for this completed pass.
    #[must_use]
    pub const fn measurements(&self) -> &SelectedObservationTraversalMeasurements {
        &self.measurements
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

    /// Clone the pass-back-only proof minted by an owner-validated exhaustive
    /// traversal, if this access was eligible to establish one.
    #[must_use]
    pub fn replay_proof(&self) -> Option<SelectedObservationReplayProof> {
        self.replay_proof.clone()
    }
}

impl SelectedObservationReplayProof {
    /// Authorize the current generation and count only after this same proof
    /// completed a freshly rebound exhaustive traversal.
    #[must_use]
    pub fn authorize_rebound_completion(
        &self,
        completion: &SelectedObservationCompletion,
    ) -> Option<SelectedObservationReplayAuthorization> {
        let rebound = completion.rebound
            && completion
                .replay_proof
                .as_ref()
                .is_some_and(|proof| Arc::ptr_eq(&self.inner, &proof.inner))
            && completion.problem_id == self.inner.identity.problem_id
            && completion.observation_snapshot_id == self.inner.identity.observation_snapshot_id
            && completion.observation_provenance_id
                == self.inner.identity.observation_provenance_id
            && completion.commitment_id == self.inner.identity.commitment_id
            && completion.generation_id == self.inner.generation_id
            && completion.sample_count == self.inner.sample_count;
        rebound.then_some(SelectedObservationReplayAuthorization {
            generation_id: completion.generation_id,
            sample_count: completion.sample_count,
        })
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
    /// Physical traversal counters exceeded their diagnostics domain.
    MeasurementOverflow,
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
            Self::MeasurementOverflow => {
                formatter.write_str("selected-observation traversal measurements overflowed")
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
            Self::TraversalIdentityExhausted | Self::MeasurementOverflow => None,
        }
    }
}

impl Iterator for BoundSelectedObservationSamples<'_> {
    type Item = Result<SelectedObservationSample, BoundObservationSourceError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_projected()
            .map(|result| result.map(|projected| projected.selected))
    }
}

impl BoundSelectedObservationSamples<'_> {
    fn next_projected(
        &mut self,
    ) -> Option<Result<ProjectedSelectedObservationSample, BoundObservationSourceError>> {
        if self.finished {
            return None;
        }
        loop {
            if let Some(current) = &mut self.current {
                if let Some(sample) = current.next_projected() {
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
            match source.selected_samples_measured(self.problem, Rc::clone(&self.measurements)) {
                Ok(samples) => {
                    if let Err(error) = self.measurements.borrow_mut().record_source_pass() {
                        self.finished = true;
                        return Some(Err(error));
                    }
                    self.current = Some(samples);
                }
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
    /// A prior replay proof does not authorize this compiled source set.
    #[error("selected-observation replay proof does not match the rebound source set")]
    ReplayProofMismatch,
    /// The affine traversal identity domain was exhausted.
    #[error("selected-observation traversal identity exhausted")]
    TraversalIdentityExhausted,
    /// The process-local retained-access identity domain was exhausted.
    #[error("selected-observation retained-access identity exhausted")]
    AccessIdentityExhausted,
    /// The retained source-slot allocation exceeded the host byte domain.
    #[error("selected-observation source-slot byte projection overflowed")]
    SourceSlotByteOverflow,
    /// The consumed source-binding graph exceeded the host byte domain.
    #[error("selected-observation binding-graph byte projection overflowed")]
    BindingGraphByteOverflow,
    /// Aggregate selected-source residency exceeded the host byte domain.
    #[error("selected-observation aggregate residency projection overflowed")]
    ResidencyByteOverflow,
    /// The retained replay-proof graph exceeded the host byte domain.
    #[error("selected-observation replay-proof byte projection overflowed")]
    ReplayProofByteOverflow,
}
