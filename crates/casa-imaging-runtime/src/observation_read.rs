// SPDX-License-Identifier: LGPL-3.0-or-later

//! Attempt-bound completion evidence for exact ObservationRead reports.

use std::{
    error::Error,
    fmt,
    sync::atomic::{AtomicU64, Ordering},
};

use casa_imaging_model::{
    CompiledProblem, ConsistencyToken, MeasurementSetIdentity, MetadataGeneration,
    ObservationReadSet, ObservationSelection, ObservationSnapshotId, SelectedColumns,
    SelectedObservationCommitmentId, SelectedObservationGenerationId,
    SelectedObservationInspectionError, SelectedObservationSample,
};

use crate::{
    execution::WorkNodeId,
    execution_bindings::{PhysicalWorkId, WorkCompletion},
    receipt::ExecutionAttemptId,
};

/// Runtime-observed exact read report for one canonical MeasurementSet source.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservationReadSourceReport {
    measurement_set: MeasurementSetIdentity,
    selection: ObservationSelection,
    selected_columns: SelectedColumns,
    metadata: Vec<MetadataGeneration>,
    consistency_token: ConsistencyToken,
}

impl ObservationReadSourceReport {
    /// Report every source read fact actually observed by the adapter.
    #[must_use]
    pub const fn new(
        measurement_set: MeasurementSetIdentity,
        selection: ObservationSelection,
        selected_columns: SelectedColumns,
        metadata: Vec<MetadataGeneration>,
        consistency_token: ConsistencyToken,
    ) -> Self {
        Self {
            measurement_set,
            selection,
            selected_columns,
            metadata,
            consistency_token,
        }
    }

    /// Return the canonical MeasurementSet source identity.
    #[must_use]
    pub const fn measurement_set(&self) -> MeasurementSetIdentity {
        self.measurement_set
    }

    /// Return the complete selected row, channel, and correlation semantics.
    #[must_use]
    pub const fn selection(&self) -> &ObservationSelection {
        &self.selection
    }

    /// Return exact visibility, flag, weight, and generated-column semantics.
    #[must_use]
    pub const fn selected_columns(&self) -> &SelectedColumns {
        &self.selected_columns
    }

    /// Return every observed metadata generation in canonical order.
    #[must_use]
    pub fn metadata(&self) -> &[MetadataGeneration] {
        &self.metadata
    }

    /// Return the source-owner consistency token observed at terminal completion.
    #[must_use]
    pub const fn consistency_token(&self) -> ConsistencyToken {
        self.consistency_token
    }
}

/// Opaque proof that one terminal ObservationRead exactly matched its bound read set.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservationReadCompletion {
    call: ObservationReadCompletionCall,
    observation_snapshot_id: ObservationSnapshotId,
    physical_work_id: PhysicalWorkId,
    terminal_owner: WorkNodeId,
    sources: Vec<ObservationReadSourceReport>,
    commitment_id: SelectedObservationCommitmentId,
    generation_id: SelectedObservationGenerationId,
    sample_count: u64,
}

impl ObservationReadCompletion {
    /// Return the immutable snapshot whose canonical source set was traversed.
    #[must_use]
    pub const fn observation_snapshot_id(&self) -> ObservationSnapshotId {
        self.observation_snapshot_id
    }

    /// Return the exact sealed physical work that owns this completion.
    #[must_use]
    pub const fn physical_work_id(&self) -> PhysicalWorkId {
        self.physical_work_id
    }

    /// Return the plan-bound ObservationRead node that reached terminal settlement.
    #[must_use]
    pub const fn terminal_owner(&self) -> &WorkNodeId {
        &self.terminal_owner
    }

    /// Return runtime-observed source reports in canonical read-set order.
    #[must_use]
    pub fn sources(&self) -> &[ObservationReadSourceReport] {
        &self.sources
    }

    /// Return the exact compiled selected-observation commitment.
    #[must_use]
    pub const fn commitment_id(&self) -> SelectedObservationCommitmentId {
        self.commitment_id
    }

    /// Return the content-only identity of the validated canonical sample stream.
    #[must_use]
    pub const fn generation_id(&self) -> SelectedObservationGenerationId {
        self.generation_id
    }

    /// Return the exact number of validated selected samples.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    pub(crate) fn matches_current(
        &self,
        attempt_id: ExecutionAttemptId,
        runtime_token: ObservationReadCompletionToken,
        problem: &CompiledProblem,
        physical_work_id: PhysicalWorkId,
        terminal_owner: &WorkNodeId,
    ) -> bool {
        self.call.matches(attempt_id, runtime_token)
            && self.observation_snapshot_id
                == problem.observation_transaction().observation_snapshot_id()
            && self.physical_work_id == physical_work_id
            && &self.terminal_owner == terminal_owner
            && self.commitment_id == problem.selected_observation().commitment_id()
            && observation_read_sources_match(
                problem.observation_transaction().read_set(),
                &self.sources,
            )
    }
}

/// Runtime source report did not match the plan-bound canonical read set.
#[derive(Debug)]
pub enum ObservationReadCompletionError<E> {
    /// Runtime source/generation facts differed from the bound read set.
    SourceReportMismatch,
    /// Selected samples did not realize the compiled scientific commitment.
    Inspection(SelectedObservationInspectionError),
    /// The selected-observation source failed while yielding a sample.
    Stream(E),
}

impl<E> fmt::Display for ObservationReadCompletionError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceReportMismatch => formatter
                .write_str("runtime ObservationRead report does not match its bound read set"),
            Self::Inspection(error) => error.fmt(formatter),
            Self::Stream(_) => formatter.write_str("selected-observation sample stream failed"),
        }
    }
}

impl<E: Error + 'static> Error for ObservationReadCompletionError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SourceReportMismatch => None,
            Self::Inspection(error) => Some(error),
            Self::Stream(error) => Some(error),
        }
    }
}

/// Capability available only to the adapter owning one plan-bound ObservationRead terminal.
#[derive(Debug)]
pub struct ObservationReadCompletionOwner<'a> {
    call: ObservationReadCompletionCall,
    problem: &'a CompiledProblem,
    physical_work_id: PhysicalWorkId,
    terminal_owner: &'a WorkNodeId,
}

impl<'a> ObservationReadCompletionOwner<'a> {
    pub(crate) const fn new(
        attempt_id: ExecutionAttemptId,
        runtime_token: ObservationReadCompletionToken,
        problem: &'a CompiledProblem,
        physical_work_id: PhysicalWorkId,
        terminal_owner: &'a WorkNodeId,
    ) -> Self {
        Self {
            call: ObservationReadCompletionCall::new(attempt_id, runtime_token),
            problem,
            physical_work_id,
            terminal_owner,
        }
    }

    /// Validate runtime-observed reports and mint opaque terminal evidence.
    pub fn complete<E>(
        self,
        sources: Vec<ObservationReadSourceReport>,
        samples: impl IntoIterator<Item = Result<SelectedObservationSample, E>>,
    ) -> Result<WorkCompletion, ObservationReadCompletionError<E>>
    where
        E: Error + 'static,
    {
        let expected = self.problem.observation_transaction().read_set();
        if !observation_read_sources_match(expected, &sources) {
            return Err(ObservationReadCompletionError::SourceReportMismatch);
        }
        let mut stream = FallibleSamples::new(samples.into_iter());
        let inspection = self.problem.inspect_selected_observation(&mut stream);
        if let Some(error) = stream.error {
            return Err(ObservationReadCompletionError::Stream(error));
        }
        let (generation_id, sample_count) =
            inspection.map_err(ObservationReadCompletionError::Inspection)?;
        Ok(WorkCompletion::from_observation_read(
            ObservationReadCompletion {
                call: self.call,
                observation_snapshot_id: self
                    .problem
                    .observation_transaction()
                    .observation_snapshot_id(),
                physical_work_id: self.physical_work_id,
                terminal_owner: self.terminal_owner.clone(),
                sources,
                commitment_id: self.problem.selected_observation().commitment_id(),
                generation_id,
                sample_count,
            },
        ))
    }
}

struct FallibleSamples<I, E> {
    samples: I,
    error: Option<E>,
}

impl<I, E> FallibleSamples<I, E> {
    fn new(samples: I) -> Self {
        Self {
            samples,
            error: None,
        }
    }
}

impl<I, E> Iterator for FallibleSamples<I, E>
where
    I: Iterator<Item = Result<SelectedObservationSample, E>>,
{
    type Item = SelectedObservationSample;

    fn next(&mut self) -> Option<Self::Item> {
        if self.error.is_some() {
            return None;
        }
        match self.samples.next() {
            Some(Ok(sample)) => Some(sample),
            Some(Err(error)) => {
                self.error = Some(error);
                None
            }
            None => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ObservationReadCompletionToken(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ObservationReadCompletionCall {
    attempt_id: ExecutionAttemptId,
    runtime_token: ObservationReadCompletionToken,
}

impl ObservationReadCompletionCall {
    const fn new(
        attempt_id: ExecutionAttemptId,
        runtime_token: ObservationReadCompletionToken,
    ) -> Self {
        Self {
            attempt_id,
            runtime_token,
        }
    }

    fn matches(
        self,
        attempt_id: ExecutionAttemptId,
        runtime_token: ObservationReadCompletionToken,
    ) -> bool {
        self.attempt_id == attempt_id && self.runtime_token == runtime_token
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ObservationReadCompletionTokenExhausted;

impl ObservationReadCompletionToken {
    pub(crate) fn fresh() -> Result<Self, ObservationReadCompletionTokenExhausted> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self::next(&NEXT)
    }

    fn next(counter: &AtomicU64) -> Result<Self, ObservationReadCompletionTokenExhausted> {
        let token = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .map_err(|_| ObservationReadCompletionTokenExhausted)?;
        Ok(Self(token))
    }
}

fn observation_read_sources_match(
    expected: &ObservationReadSet,
    actual: &[ObservationReadSourceReport],
) -> bool {
    expected.sources().len() == actual.len()
        && expected
            .sources()
            .iter()
            .zip(actual)
            .all(|(expected, actual)| {
                expected.measurement_set() == actual.measurement_set
                    && expected.selection() == &actual.selection
                    && expected.selected_columns() == &actual.selected_columns
                    && expected.metadata() == actual.metadata
                    && expected.consistency_token() == actual.consistency_token
            })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completion_token_exhaustion_is_typed() {
        let counter = AtomicU64::new(u64::MAX);

        assert_eq!(
            ObservationReadCompletionToken::next(&counter),
            Err(ObservationReadCompletionTokenExhausted)
        );
        assert_eq!(counter.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn completion_call_rejects_a_different_attempt_with_the_same_token() {
        let token = ObservationReadCompletionToken(7);
        let first_attempt = ExecutionAttemptId::from_sha256([1; 32]);
        let second_attempt = ExecutionAttemptId::from_sha256([2; 32]);
        let call = ObservationReadCompletionCall::new(first_attempt, token);

        assert!(call.matches(first_attempt, token));
        assert!(!call.matches(second_attempt, token));
    }
}
