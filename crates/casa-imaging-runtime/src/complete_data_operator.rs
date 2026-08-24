// SPDX-License-Identifier: LGPL-3.0-or-later

//! Runtime ownership boundary for the first complete-data continuum operator.

use std::{error::Error, fmt};

use casa_imaging_model::{
    CompiledGeometryId, CompiledProblem, CompiledProblemId, InstrumentResponse, NumericsContractId,
    PolarizationCoordinate, ReconstructionBasis, SelectedObservationGenerationId,
    WeightingCommitmentId,
};
use casa_imaging_reconstruction::{
    WeightingGenerationId, WeightingReplayCoverageId, WeightingReplayId,
};

use crate::{ExecutionAttemptId, WeightedObservationBlock, WeightingReplayCompletion, WorkNodeId};

/// Versioned unnormalized primitive set produced by the nterms=1 continuum operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContinuumPrimitiveCatalog {
    /// Dirty, PSF, sensitivity, and sum-weight primitives under the v1 contract.
    UnnormalizedNterms1V1,
}

/// Opaque owner-minted proof that one complete weighted replay reached the operator.
///
/// The completion is deliberately not constructible from caller digests or a
/// generic runtime completion. It is minted only by consuming a
/// [`SerialMfsOperatorState`] after that state has accepted the complete ordered
/// stream of [`WeightedObservationBlock`] values and the terminal
/// [`WeightingReplayCompletion`].
///
/// A caller cannot substitute a generic scheduler completion:
///
/// ```compile_fail
/// use casa_imaging_runtime::{
///     AttemptBoundObservationCompletion, SerialMfsOperatorState,
/// };
///
/// fn substitute(
///     state: SerialMfsOperatorState,
///     generic: &AttemptBoundObservationCompletion,
/// ) {
///     let _ = state.complete(generic);
/// }
/// ```
///
/// Nor can a caller construct completion evidence from its own digest:
///
/// ```compile_fail
/// use casa_imaging_runtime::CompleteDataOperatorCompletion;
///
/// let _ = CompleteDataOperatorCompletion {};
/// ```
#[derive(Debug)]
pub struct CompleteDataOperatorCompletion {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: WeightingGenerationId,
    replay: WeightingReplayId,
    selected_generation: SelectedObservationGenerationId,
    coverage: WeightingReplayCoverageId,
    primitives: ContinuumPrimitiveCatalog,
    sample_count: u64,
    block_count: u64,
    attempt_id: ExecutionAttemptId,
    replay_node: WorkNodeId,
    lease_epoch: u64,
}

impl CompleteDataOperatorCompletion {
    /// Return the exact Compiled Problem executed by this operator.
    #[must_use]
    pub const fn problem_id(&self) -> CompiledProblemId {
        self.problem
    }

    /// Return the compiled geometry/operator coordinate commitment.
    #[must_use]
    pub const fn geometry_id(&self) -> CompiledGeometryId {
        self.geometry
    }

    /// Return the exact numerical contract.
    #[must_use]
    pub const fn numerics_id(&self) -> NumericsContractId {
        self.numerics
    }

    /// Return the compiler-owned weighting commitment used by T18.
    #[must_use]
    pub const fn weighting_commitment_id(&self) -> WeightingCommitmentId {
        self.weighting_commitment
    }

    /// Return the frozen W generation carried by every accepted block.
    #[must_use]
    pub const fn weighting_generation(&self) -> WeightingGenerationId {
        self.weighting_generation
    }

    /// Return the unique terminal replay identity.
    #[must_use]
    pub const fn replay_id(&self) -> WeightingReplayId {
        self.replay
    }

    /// Return the independently traversed selected-observation generation.
    #[must_use]
    pub const fn selected_generation(&self) -> SelectedObservationGenerationId {
        self.selected_generation
    }

    /// Return the exact T18 weighted-sample coverage.
    #[must_use]
    pub const fn coverage(&self) -> WeightingReplayCoverageId {
        self.coverage
    }

    /// Return the versioned primitive set produced by the operator.
    #[must_use]
    pub const fn primitive_catalog(&self) -> ContinuumPrimitiveCatalog {
        self.primitives
    }

    /// Return the exhaustive selected-sample count.
    #[must_use]
    pub const fn sample_count(&self) -> u64 {
        self.sample_count
    }

    /// Return the exhaustive replay block count.
    #[must_use]
    pub const fn block_count(&self) -> u64 {
        self.block_count
    }

    /// Return the execution attempt that authorized the replay.
    #[must_use]
    pub const fn attempt_id(&self) -> ExecutionAttemptId {
        self.attempt_id
    }

    /// Return the planned replay node that supplied terminal proof.
    #[must_use]
    pub const fn replay_node(&self) -> &WorkNodeId {
        &self.replay_node
    }

    /// Return the Resource Authority lease epoch held through replay.
    #[must_use]
    pub const fn lease_epoch(&self) -> u64 {
        self.lease_epoch
    }
}

/// Streaming owner for one serial CPU constant-basis MFS execution.
///
/// This boundary exposes no raw weighting configuration. Its only data input is
/// the T18-branded weighted block, and completion requires T18's terminal replay
/// proof.
///
/// Raw selected samples are not accepted at this boundary:
///
/// ```compile_fail
/// use casa_imaging_model::SelectedObservationSample;
/// use casa_imaging_runtime::SerialMfsOperatorState;
///
/// fn bypass(mut state: SerialMfsOperatorState, raw: &SelectedObservationSample) {
///     let _ = state.consume_weighted_block(raw);
/// }
/// ```
#[derive(Debug)]
pub struct SerialMfsOperatorState {
    problem: CompiledProblemId,
    geometry: CompiledGeometryId,
    numerics: NumericsContractId,
    weighting_commitment: WeightingCommitmentId,
    weighting_generation: Option<WeightingGenerationId>,
    next_block_sequence: u64,
    sample_count: u64,
}

impl SerialMfsOperatorState {
    /// Start an operator only for the T19 single-field Stokes-I nterms=1 surface.
    pub fn new(problem: &CompiledProblem) -> Result<Self, CompleteDataOperatorError> {
        if problem.geometry().domains().len() != 1
            || problem.reconstruction().basis() != ReconstructionBasis::Constant
            || problem.reconstruction().polarization().coordinates()
                != [PolarizationCoordinate::StokesI]
            || problem
                .science()
                .measurement_equation()
                .instrument_response()
                != InstrumentResponse::Scalar
        {
            return Err(CompleteDataOperatorError::UnsupportedProblem);
        }
        Ok(Self {
            problem: problem.problem_id(),
            geometry: problem.geometry().geometry_id(),
            numerics: problem.numerics_id(),
            weighting_commitment: problem.weighting().commitment_id(),
            weighting_generation: None,
            next_block_sequence: 0,
            sample_count: 0,
        })
    }

    /// Consume one ordered T18 weighted block synchronously.
    pub fn consume_weighted_block(
        &mut self,
        block: &WeightedObservationBlock,
    ) -> Result<(), CompleteDataOperatorError> {
        if block.sequence() != self.next_block_sequence {
            return Err(CompleteDataOperatorError::BlockSequence);
        }
        match self.weighting_generation {
            Some(generation) if generation != block.weighting_generation() => {
                return Err(CompleteDataOperatorError::WeightingGeneration);
            }
            None => self.weighting_generation = Some(block.weighting_generation()),
            Some(_) => {}
        }
        let block_samples = u64::try_from(block.samples().count())
            .map_err(|_| CompleteDataOperatorError::CoverageOverflow)?;
        self.sample_count = self
            .sample_count
            .checked_add(block_samples)
            .ok_or(CompleteDataOperatorError::CoverageOverflow)?;
        self.next_block_sequence = self
            .next_block_sequence
            .checked_add(1)
            .ok_or(CompleteDataOperatorError::CoverageOverflow)?;
        Ok(())
    }

    /// Consume terminal T18 proof and mint the complete-data operator completion.
    pub fn complete(
        self,
        replay: &WeightingReplayCompletion,
    ) -> Result<CompleteDataOperatorCompletion, CompleteDataOperatorError> {
        if self.weighting_generation != Some(replay.weighting_generation()) {
            return Err(CompleteDataOperatorError::WeightingGeneration);
        }
        if self.sample_count != replay.sample_count()
            || self.next_block_sequence != replay.block_count()
        {
            return Err(CompleteDataOperatorError::IncompleteCoverage);
        }
        Ok(CompleteDataOperatorCompletion {
            problem: self.problem,
            geometry: self.geometry,
            numerics: self.numerics,
            weighting_commitment: self.weighting_commitment,
            weighting_generation: replay.weighting_generation(),
            replay: replay.replay_id(),
            selected_generation: replay.selected_generation(),
            coverage: replay.coverage(),
            primitives: ContinuumPrimitiveCatalog::UnnormalizedNterms1V1,
            sample_count: replay.sample_count(),
            block_count: replay.block_count(),
            attempt_id: replay.attempt_id(),
            replay_node: replay.owner_node().clone(),
            lease_epoch: replay.lease_epoch(),
        })
    }
}

/// Exact reason T19 rejected an operator problem, block, or terminal proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompleteDataOperatorError {
    /// The Compiled Problem is outside single-field scalar Stokes-I nterms=1.
    UnsupportedProblem,
    /// Replay blocks were missing, repeated, or reordered.
    BlockSequence,
    /// Blocks or terminal proof disagree on the frozen W generation.
    WeightingGeneration,
    /// Sample or block counts exceeded the supported identity domain.
    CoverageOverflow,
    /// Terminal replay proof does not cover every block accepted by the operator.
    IncompleteCoverage,
}

impl fmt::Display for CompleteDataOperatorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::UnsupportedProblem => {
                "serial MFS requires one scalar-response Stokes-I constant-basis domain"
            }
            Self::BlockSequence => "weighted replay blocks are not exhaustive and ordered",
            Self::WeightingGeneration => "weighted replay generations do not match",
            Self::CoverageOverflow => "weighted replay coverage overflowed",
            Self::IncompleteCoverage => "terminal replay proof does not match consumed coverage",
        })
    }
}

impl Error for CompleteDataOperatorError {}
