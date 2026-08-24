// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime composition of reconstruction phases with opaque T17 traversal evidence.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledProblem, SelectedObservationGenerationId, SelectedObservationSample,
    SelectedSpectralContribution,
};
use casa_imaging_reconstruction::{
    WeightingAlgorithmState, WeightingError, WeightingGenerationId, WeightingPlan,
    WeightingReplayChunk as ReconstructionWeightedBlock, WeightingReplayCoverageId,
    WeightingReplayId, WeightingReplaySummary, WeightingResidency, begin_weighting_generation,
};
use casa_ms::{
    BoundSelectedObservation, SelectedObservationCompletion, SelectedObservationTraversalError,
};

use crate::{
    AttemptBoundObservationCompletion, ExecutionAttemptId, ObservationCompletionBindingError,
    ObservationReadCompletionContext, WorkNodeId,
};

/// One runtime-authorized output contribution carrying the frozen W generation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightedSpectralValue {
    contribution: SelectedSpectralContribution,
    imaging_weight: f64,
    generation: WeightingGenerationId,
}

impl WeightedSpectralValue {
    /// Return the storage-owner-reported output contribution.
    #[must_use]
    pub const fn contribution(self) -> SelectedSpectralContribution {
        self.contribution
    }

    /// Return the final non-negative diagonal metric value.
    #[must_use]
    pub const fn imaging_weight(self) -> f64 {
        self.imaging_weight
    }

    /// Return the sole frozen generation that supplied W.
    #[must_use]
    pub const fn weighting_generation(self) -> WeightingGenerationId {
        self.generation
    }
}

/// One runtime-authorized weighted sample carrying output-specific W values.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct WeightedObservationSample {
    sample: SelectedObservationSample,
    spectral_values: [Option<WeightedSpectralValue>; 2],
    generation: WeightingGenerationId,
}

impl WeightedObservationSample {
    /// Return the selected sample validated by T17 traversal.
    #[must_use]
    pub const fn selected(&self) -> &SelectedObservationSample {
        &self.sample
    }

    /// Iterate over output contributions and their final W values.
    pub fn spectral_values(&self) -> impl Iterator<Item = WeightedSpectralValue> + '_ {
        self.spectral_values.iter().flatten().copied()
    }

    /// Return the sole frozen generation that supplied W.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.generation
    }
}

/// One borrowed-consumption replay block branded only by runtime-held T17 evidence.
#[derive(Debug)]
pub struct WeightedObservationBlock {
    generation: WeightingGenerationId,
    sequence: u64,
    samples: Box<[WeightedObservationSample]>,
}

impl WeightedObservationBlock {
    fn authorize(generation: WeightingGenerationId, block: ReconstructionWeightedBlock) -> Self {
        let sequence = block.sequence();
        let samples = block
            .samples()
            .iter()
            .map(|sample| {
                let mut spectral_values = [None, None];
                for (slot, value) in sample.spectral_values().enumerate() {
                    spectral_values[slot] = Some(WeightedSpectralValue {
                        contribution: value.contribution(),
                        imaging_weight: value.imaging_weight(),
                        generation,
                    });
                }
                WeightedObservationSample {
                    sample: *sample.selected(),
                    spectral_values,
                    generation,
                }
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Self {
            generation,
            sequence,
            samples,
        }
    }

    /// Return the frozen W generation authorizing every sample.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.generation
    }

    /// Return the zero-based replay block sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }

    /// Borrow weighted samples for synchronous bounded consumption.
    #[must_use]
    pub const fn samples(&self) -> &[WeightedObservationSample] {
        &self.samples
    }
}

/// A frozen W whose reconstruction state is backed by two opaque T17 completions.
///
/// Callers cannot construct or rebind this evidence directly:
///
/// ```compile_fail
/// use casa_imaging_runtime::FrozenWeightingGeneration;
///
/// let _forged = FrozenWeightingGeneration {};
/// ```
#[derive(Debug)]
pub struct FrozenWeightingGeneration {
    state: WeightingAlgorithmState,
    density_completion: SelectedObservationCompletion,
    binding: WeightingGenerationBinding,
}

#[derive(Debug)]
struct WeightingGenerationBinding {
    attempt_id: ExecutionAttemptId,
    owner_node: WorkNodeId,
    lease_epoch: u64,
}

impl FrozenWeightingGeneration {
    /// Return the reconstruction-owned frozen W identity.
    #[must_use]
    pub const fn generation_id(&self) -> WeightingGenerationId {
        self.state.generation_id()
    }

    /// Return the exact selected-content generation proven by both T17 passes.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.density_completion.generation_id()
    }

    /// Return the exhaustive sample count proven by both T17 passes.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.state.sample_count()
    }

    /// Return frozen sum weights in global or output-channel order.
    #[must_use]
    pub const fn sum_weights(&self) -> &[f64] {
        self.state.sum_weights()
    }

    /// Return the generation pass's actual bounded residency.
    #[must_use]
    pub const fn generation_residency(&self) -> WeightingResidency {
        self.state.generation_residency()
    }

    /// Replay the same W through a third exhaustive T17 traversal.
    pub fn replay<E>(
        &self,
        predecessor: &AttemptBoundObservationCompletion,
        selected: &mut BoundSelectedObservation,
        problem: &CompiledProblem,
        plan: &WeightingPlan,
        mut emit: impl FnMut(&WeightedObservationBlock) -> Result<(), E>,
    ) -> Result<PendingWeightingReplay, WeightingReplayError<E>>
    where
        E: Error + 'static,
    {
        if predecessor.attempt_id() != self.binding.attempt_id
            || predecessor.owner_node() != &self.binding.owner_node
            || predecessor.lease_epoch() != self.binding.lease_epoch
            || validate_generation_completions(
                &self.density_completion,
                predecessor.owner_completion(),
            )
            .is_err()
        {
            return Err(WeightingReplayError::Evidence(WeightingEvidenceError));
        }
        let mut phase = self
            .state
            .begin_replay(problem, plan)
            .map_err(WeightingReplayError::Owner)?;
        let owner_completion = selected
            .traverse(problem, |reported| {
                if let Some(block) = phase
                    .consume(
                        problem,
                        *reported.selected(),
                        reported.spectral_contributions(),
                    )
                    .map_err(ReplayCallbackError::Owner)?
                {
                    let block = WeightedObservationBlock::authorize(self.generation_id(), block);
                    emit(&block).map_err(ReplayCallbackError::Consumer)?;
                }
                Ok(())
            })
            .map_err(WeightingReplayError::Traversal)?;
        validate_replay_completion(
            &self.density_completion,
            predecessor.owner_completion(),
            &owner_completion,
            &self.state,
        )
        .map_err(WeightingReplayError::Evidence)?;
        let (final_block, state) = phase.finish().map_err(WeightingReplayError::Owner)?;
        if let Some(block) = final_block {
            let block = WeightedObservationBlock::authorize(self.generation_id(), block);
            emit(&block).map_err(WeightingReplayError::Consumer)?;
        }
        Ok(PendingWeightingReplay {
            state,
            owner_completion,
        })
    }
}

/// Unbranded result of two exhaustive owner traversals.
///
/// Only [`traverse_weighting_generation`] constructs this value, and only a
/// scheduler-issued [`ObservationReadCompletionContext`] can turn it into a
/// consumable frozen generation.
///
/// ```compile_fail
/// use casa_imaging_runtime::PendingWeightingGeneration;
///
/// let _forged = PendingWeightingGeneration {};
/// ```
#[derive(Debug)]
pub struct PendingWeightingGeneration {
    state: WeightingAlgorithmState,
    density_completion: SelectedObservationCompletion,
    sum_weight_completion: SelectedObservationCompletion,
}

/// Drive both exhaustive owner traversals into an unbranded weighting generation.
pub fn traverse_weighting_generation(
    selected: &mut BoundSelectedObservation,
    problem: &CompiledProblem,
    plan: &WeightingPlan,
) -> Result<PendingWeightingGeneration, WeightingGenerationError> {
    let mut density =
        begin_weighting_generation(problem, plan).map_err(WeightingGenerationError::Owner)?;
    let density_completion = selected
        .traverse(problem, |reported| {
            density.consume(
                problem,
                *reported.selected(),
                reported.spectral_contributions(),
            )
        })
        .map_err(WeightingGenerationError::DensityTraversal)?;
    let sum_weight = density
        .finish(problem)
        .map_err(WeightingGenerationError::Owner)?;
    let mut sum_weight = sum_weight;
    let sum_weight_completion = selected
        .traverse(problem, |reported| {
            sum_weight.consume(
                problem,
                *reported.selected(),
                reported.spectral_contributions(),
            )
        })
        .map_err(WeightingGenerationError::SumWeightTraversal)?;
    let state = sum_weight
        .finish()
        .map_err(WeightingGenerationError::Owner)?;
    validate_generation_completions(&density_completion, &sum_weight_completion)
        .map_err(WeightingGenerationError::Evidence)?;
    if state.sample_count() != density_completion.sample_count() {
        return Err(WeightingGenerationError::Evidence(WeightingEvidenceError));
    }
    Ok(PendingWeightingGeneration {
        state,
        density_completion,
        sum_weight_completion,
    })
}

/// Bind an owner-traversed generation to one settled scheduler work node.
///
/// A caller-created reconstruction state cannot bypass owner traversal or
/// attempt binding:
///
/// ```compile_fail
/// use casa_imaging_reconstruction::WeightingAlgorithmState;
/// use casa_imaging_runtime::complete_weighting_generation;
///
/// fn bypass(state: WeightingAlgorithmState) {
///     let _ = complete_weighting_generation(state);
/// }
/// ```
pub fn complete_weighting_generation(
    pending: PendingWeightingGeneration,
    context: ObservationReadCompletionContext,
) -> Result<
    (FrozenWeightingGeneration, AttemptBoundObservationCompletion),
    WeightingGenerationCompletionError,
> {
    validate_generation_completions(&pending.density_completion, &pending.sum_weight_completion)
        .map_err(WeightingGenerationCompletionError::Evidence)?;
    if pending.state.sample_count() != pending.density_completion.sample_count() {
        return Err(WeightingGenerationCompletionError::Evidence(
            WeightingEvidenceError,
        ));
    }
    let binding = WeightingGenerationBinding {
        attempt_id: context.attempt_id(),
        owner_node: context.owner_node().clone(),
        lease_epoch: context.lease_epoch(),
    };
    let predecessor = context
        .bind(pending.sum_weight_completion)
        .map_err(WeightingGenerationCompletionError::Binding)?;
    Ok((
        FrozenWeightingGeneration {
            state: pending.state,
            density_completion: pending.density_completion,
            binding,
        },
        predecessor,
    ))
}

fn validate_generation_completions(
    density: &SelectedObservationCompletion,
    sum_weight: &SelectedObservationCompletion,
) -> Result<(), WeightingEvidenceError> {
    if !density.precedes(sum_weight)
        || density.problem_id() != sum_weight.problem_id()
        || density.commitment_id() != sum_weight.commitment_id()
        || density.generation_id() != sum_weight.generation_id()
        || density.sample_count() != sum_weight.sample_count()
    {
        return Err(WeightingEvidenceError);
    }
    Ok(())
}

fn validate_replay_completion(
    density: &SelectedObservationCompletion,
    prior: &SelectedObservationCompletion,
    replay: &SelectedObservationCompletion,
    state: &WeightingAlgorithmState,
) -> Result<(), WeightingEvidenceError> {
    if !density.precedes(prior)
        || !prior.precedes(replay)
        || replay.generation_id() != density.generation_id()
        || replay.sample_count() != state.sample_count()
    {
        return Err(WeightingEvidenceError);
    }
    Ok(())
}

/// Replay algorithm result awaiting scheduler-issued attempt authority.
#[derive(Debug)]
pub struct PendingWeightingReplay {
    state: WeightingReplaySummary,
    owner_completion: SelectedObservationCompletion,
}

impl PendingWeightingReplay {
    /// Bind the exhaustive replay to its owning ObservationRead work node.
    pub fn bind(
        self,
        context: ObservationReadCompletionContext,
    ) -> Result<WeightingReplayCompletion, ObservationCompletionBindingError> {
        let owner_completion = context.bind(self.owner_completion)?;
        Ok(WeightingReplayCompletion {
            state: self.state,
            owner_completion,
        })
    }
}

/// Distinct terminal proof of a weighted replay and its exhaustive T17 traversal.
#[derive(Debug)]
pub struct WeightingReplayCompletion {
    state: WeightingReplaySummary,
    owner_completion: AttemptBoundObservationCompletion,
}

impl WeightingReplayCompletion {
    /// Return the unique replay identity.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.state.replay_id()
    }

    /// Return the frozen W carried by every emitted block.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.state.weighting_generation()
    }

    /// Return the independently traversed T17 content generation.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.owner_completion.owner_completion().generation_id()
    }

    /// Return exact emitted weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.state.coverage()
    }

    /// Return the exhaustive emitted sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.owner_completion.owner_completion().sample_count()
    }

    /// Return emitted block count.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.state.block_count()
    }

    /// Return this generation's unique replay sequence.
    #[must_use]
    pub const fn replay_sequence(&self) -> u64 {
        self.state.replay_sequence()
    }

    /// Return actual bounded replay residency.
    #[must_use]
    pub const fn residency(&self) -> WeightingResidency {
        self.state.residency()
    }
}

/// Two T17 generation traversals or reconstruction reduction failed.
#[derive(Debug)]
pub enum WeightingGenerationError {
    /// Density traversal failed before opaque completion.
    DensityTraversal(SelectedObservationTraversalError<WeightingError>),
    /// Sum-weight traversal failed before opaque completion.
    SumWeightTraversal(SelectedObservationTraversalError<WeightingError>),
    /// Reconstruction rejected a plan, sample, or reduction.
    Owner(WeightingError),
    /// Opaque T17 completions did not prove the same ordered retained access.
    Evidence(WeightingEvidenceError),
}

impl fmt::Display for WeightingGenerationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DensityTraversal(error) => {
                write!(formatter, "weighting density traversal failed: {error}")
            }
            Self::SumWeightTraversal(error) => {
                write!(formatter, "weighting sum-weight traversal failed: {error}")
            }
            Self::Owner(error) => error.fmt(formatter),
            Self::Evidence(error) => error.fmt(formatter),
        }
    }
}

impl Error for WeightingGenerationError {}

/// Scheduler binding of an owner-traversed weighting generation failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WeightingGenerationCompletionError {
    /// Traversal evidence did not describe two ordered passes over one retained source.
    Evidence(WeightingEvidenceError),
    /// The scheduler completion context belongs to another compiled observation.
    Binding(ObservationCompletionBindingError),
}

impl fmt::Display for WeightingGenerationCompletionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Evidence(error) => error.fmt(formatter),
            Self::Binding(error) => error.fmt(formatter),
        }
    }
}

impl Error for WeightingGenerationCompletionError {}

/// Weighted replay traversal, reconstruction, or consumer failure.
#[derive(Debug)]
pub enum WeightingReplayError<E> {
    /// The exhaustive T17 traversal or an in-traversal callback failed.
    Traversal(SelectedObservationTraversalError<ReplayCallbackError<E>>),
    /// Reconstruction rejected the replay.
    Owner(WeightingError),
    /// Opaque replay completion did not follow the frozen generation passes.
    Evidence(WeightingEvidenceError),
    /// The consumer rejected the terminal partial block.
    Consumer(E),
}

impl<E: fmt::Display> fmt::Display for WeightingReplayError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Traversal(error) => error.fmt(formatter),
            Self::Owner(error) => error.fmt(formatter),
            Self::Evidence(error) => error.fmt(formatter),
            Self::Consumer(error) => write!(formatter, "weighted replay consumer failed: {error}"),
        }
    }
}

impl<E: Error + 'static> Error for WeightingReplayError<E> {}

/// Error raised inside the T17 replay callback.
#[derive(Debug)]
pub enum ReplayCallbackError<E> {
    /// Reconstruction rejected a validated sample.
    Owner(WeightingError),
    /// The downstream block consumer failed.
    Consumer(E),
}

impl<E: fmt::Display> fmt::Display for ReplayCallbackError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Owner(error) => error.fmt(formatter),
            Self::Consumer(error) => error.fmt(formatter),
        }
    }
}

impl<E: Error + 'static> Error for ReplayCallbackError<E> {}

/// Opaque traversal evidence did not bind the required ordered passes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WeightingEvidenceError;

impl fmt::Display for WeightingEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("weighting phases do not bind ordered exhaustive traversals of one retained selected observation")
    }
}

impl Error for WeightingEvidenceError {}
