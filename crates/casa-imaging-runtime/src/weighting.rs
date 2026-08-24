// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime composition of reconstruction phases with opaque T17 traversal evidence.

use std::{error::Error, fmt};

use casa_imaging_model::{CompiledProblem, SelectedObservationGenerationId};
use casa_imaging_reconstruction::{
    WeightedObservationBlock, WeightingError, WeightingGenerationId, WeightingGenerationState,
    WeightingPlan, WeightingReplayCoverageId, WeightingReplayId, WeightingReplayState,
    WeightingResidency, begin_weighting_generation,
};
use casa_ms::{
    BoundSelectedObservation, SelectedObservationCompletion, SelectedObservationTraversalError,
};

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
    state: WeightingGenerationState,
    density_completion: SelectedObservationCompletion,
    sum_weight_completion: SelectedObservationCompletion,
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
        selected: &mut BoundSelectedObservation,
        problem: &CompiledProblem,
        plan: &WeightingPlan,
        mut emit: impl FnMut(WeightedObservationBlock) -> Result<(), E>,
    ) -> Result<WeightingReplayCompletion, WeightingReplayError<E>>
    where
        E: Error + 'static,
    {
        let mut phase = self
            .state
            .begin_replay(problem, plan)
            .map_err(WeightingReplayError::Owner)?;
        let owner_completion = selected
            .traverse(problem, |sample| {
                if let Some(block) = phase
                    .consume(problem, sample)
                    .map_err(ReplayCallbackError::Owner)?
                {
                    emit(block).map_err(ReplayCallbackError::Consumer)?;
                }
                Ok(())
            })
            .map_err(WeightingReplayError::Traversal)?;
        validate_replay_completion(
            &self.density_completion,
            &self.sum_weight_completion,
            &owner_completion,
            &self.state,
        )
        .map_err(WeightingReplayError::Evidence)?;
        let (final_block, state) = phase.finish().map_err(WeightingReplayError::Owner)?;
        if let Some(block) = final_block {
            emit(block).map_err(WeightingReplayError::Consumer)?;
        }
        Ok(WeightingReplayCompletion {
            state,
            owner_completion,
        })
    }
}

/// Drive two exhaustive T17 traversals into reconstruction-owned density and sum-weight phases.
pub fn freeze_weighting_generation(
    selected: &mut BoundSelectedObservation,
    problem: &CompiledProblem,
    plan: &WeightingPlan,
) -> Result<FrozenWeightingGeneration, WeightingGenerationError> {
    let mut density =
        begin_weighting_generation(problem, plan).map_err(WeightingGenerationError::Owner)?;
    let density_completion = selected
        .traverse(problem, |sample| density.consume(problem, sample))
        .map_err(WeightingGenerationError::DensityTraversal)?;
    let mut sum_weight = density
        .finish(problem)
        .map_err(WeightingGenerationError::Owner)?;
    let sum_weight_completion = selected
        .traverse(problem, |sample| sum_weight.consume(problem, sample))
        .map_err(WeightingGenerationError::SumWeightTraversal)?;
    validate_generation_completions(&density_completion, &sum_weight_completion)
        .map_err(WeightingGenerationError::Evidence)?;
    let state = sum_weight
        .finish()
        .map_err(WeightingGenerationError::Owner)?;
    if state.sample_count() != density_completion.sample_count() {
        return Err(WeightingGenerationError::Evidence(WeightingEvidenceError));
    }
    Ok(FrozenWeightingGeneration {
        state,
        density_completion,
        sum_weight_completion,
    })
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
    state: &WeightingGenerationState,
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

/// Distinct terminal proof of a weighted replay and its exhaustive T17 traversal.
#[derive(Debug)]
pub struct WeightingReplayCompletion {
    state: WeightingReplayState,
    owner_completion: SelectedObservationCompletion,
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
        self.owner_completion.generation_id()
    }

    /// Return exact emitted weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.state.coverage()
    }

    /// Return the exhaustive emitted sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.owner_completion.sample_count()
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
