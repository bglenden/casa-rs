// SPDX-License-Identifier: LGPL-3.0-or-later

//! Typed source catalog for continuum products.
//!
//! Every source role keeps its own typed owner record. The catalog is minted
//! only from a whole released Major-Cycle join, so one atomic reconciliation
//! result carries its multiple distinct completions together and no role's
//! evidence can be substituted for another.

use casa_imaging_model::{CompiledProblem, LogicalIdentity, ProductGraphId};
use casa_imaging_reconstruction::{
    FinalNormalState, FinalNormalStateCompletionId, MajorCycleCompletion, MajorCycleCompletionId,
    ModelGeneration, ModelGenerationId, NormalStateCatalog,
};

use crate::digest::{COMMITMENT_DOMAIN, COMMITMENT_VERSION, Encoder};
use crate::error::ProductsError;

/// Identity catalog of every source role behind one continuum generation.
///
/// Constructed only by [`ContinuumSourceCatalog::from_major_cycle`]; there is
/// no per-field public constructor, so a commitment set always describes one
/// inseparable reconciliation lineage.
#[derive(Debug, Clone)]
pub struct ContinuumSourceCatalog {
    problem: CompiledProblem,
    graph_id: ProductGraphId,
    major_cycle_completion: MajorCycleCompletionId,
    normal_state_completion: FinalNormalStateCompletionId,
    normal_state_content: LogicalIdentity,
    normal_state_catalog: NormalStateCatalog,
    input_model_generation: ModelGenerationId,
    final_model_generation: ModelGenerationId,
    weighting_generation: casa_imaging_reconstruction::WeightingGenerationId,
    replay: casa_imaging_reconstruction::WeightingReplayId,
    coverage: casa_imaging_reconstruction::WeightingReplayCoverageId,
    selected_generation: casa_imaging_model::SelectedObservationGenerationId,
    sample_count: u64,
    block_count: u64,
}

impl ContinuumSourceCatalog {
    /// Mint the catalog from one whole released Major-Cycle join.
    ///
    /// The problem must be the exact compiled problem the join reconciled.
    ///
    /// # Errors
    ///
    /// Rejects joins from another compiled problem and unsupported catalogs.
    pub fn from_major_cycle(
        problem: &CompiledProblem,
        join: &MajorCycleCompletion,
    ) -> Result<Self, ProductsError> {
        let normal_state = join.normal_state();
        if normal_state.problem_id() != problem.problem_id() {
            return Err(ProductsError::SourceLineageMismatch);
        }
        if join.model_completion().problem() != problem.problem_id() {
            return Err(ProductsError::SourceLineageMismatch);
        }
        if matches!(
            normal_state.catalog(),
            NormalStateCatalog::UnnormalizedNterms1V1
        ) {
            // The only supported continuum catalog.
        } else {
            return Err(ProductsError::UnsupportedProblem);
        }
        Ok(Self {
            graph_id: problem.product_graph().graph_id(),
            major_cycle_completion: join.completion_id(),
            normal_state_completion: normal_state.completion_id(),
            normal_state_content: normal_state.content_identity(),
            normal_state_catalog: normal_state.catalog(),
            input_model_generation: normal_state.input_model_generation(),
            final_model_generation: normal_state.final_model_generation(),
            weighting_generation: normal_state.weighting_generation(),
            replay: normal_state.replay_id(),
            coverage: normal_state.coverage(),
            selected_generation: normal_state.selected_generation(),
            sample_count: normal_state.sample_count(),
            block_count: normal_state.block_count(),
            problem: problem.clone(),
        })
    }

    /// Borrow the exact compiled problem this lineage reconciled.
    #[must_use]
    pub const fn problem(&self) -> &CompiledProblem {
        &self.problem
    }

    /// Return the compiler-owned Product Graph identity of this lineage.
    #[must_use]
    pub const fn graph_id(&self) -> ProductGraphId {
        self.graph_id
    }

    /// Return the Major-Cycle completion identity of this lineage.
    #[must_use]
    pub const fn major_cycle_completion(&self) -> MajorCycleCompletionId {
        self.major_cycle_completion
    }

    /// Return the final model generation named by this lineage.
    #[must_use]
    pub const fn final_model_generation(&self) -> ModelGenerationId {
        self.final_model_generation
    }

    /// Return the schema-versioned digest binding every role's evidence.
    #[must_use]
    pub fn commitment_id(&self) -> [u8; 32] {
        let mut encoder = Encoder::new(COMMITMENT_DOMAIN, COMMITMENT_VERSION);
        encoder.identity(self.problem.problem_id().as_bytes());
        encoder.identity(self.graph_id.as_bytes());
        encoder.identity(self.major_cycle_completion.as_bytes());
        encoder.identity(self.normal_state_completion.as_bytes());
        encoder.identity(self.normal_state_content.as_bytes());
        encoder.u8(match self.normal_state_catalog {
            NormalStateCatalog::UnnormalizedNterms1V1 => 0,
        });
        encoder.identity(self.input_model_generation.as_bytes());
        encoder.identity(self.final_model_generation.as_bytes());
        encoder.identity(self.weighting_generation.as_bytes());
        encoder.identity(self.replay.as_bytes());
        encoder.identity(self.coverage.as_bytes());
        encoder.identity(self.selected_generation.as_bytes());
        encoder.u64(self.sample_count);
        encoder.u64(self.block_count);
        encoder.finish()
    }
}

/// Borrowed scientific payloads for producing planned members.
///
/// Like the catalog, this value is minted only from a whole join plus its
/// exact compiled problem, so production always consumes the same atomic
/// reconciliation result the commitments name.
#[derive(Debug)]
pub struct ContinuumProductInputs<'a> {
    problem: &'a CompiledProblem,
    normal_state: &'a FinalNormalState,
    final_model: &'a ModelGeneration,
}

impl<'a> ContinuumProductInputs<'a> {
    /// Bind the borrowed payloads of one released join.
    ///
    /// # Errors
    ///
    /// Rejects joins whose normal state belongs to another compiled problem.
    pub fn from_major_cycle(
        problem: &'a CompiledProblem,
        join: &'a MajorCycleCompletion,
    ) -> Result<Self, ProductsError> {
        if join.normal_state().problem_id() != problem.problem_id() {
            return Err(ProductsError::SourceLineageMismatch);
        }
        Ok(Self {
            problem,
            normal_state: join.normal_state(),
            final_model: join.final_model(),
        })
    }

    /// Borrow the exact compiled problem behind these payloads.
    #[must_use]
    pub const fn problem(&self) -> &CompiledProblem {
        self.problem
    }

    /// Return radians-per-pixel on each direction axis of the main domain.
    #[must_use]
    pub fn cell_size_rad(&self) -> [f64; 2] {
        let domain = &self.problem.geometry().domains()[0];
        let increment = domain.direction().increment_rad();
        [increment[0].abs(), increment[1].abs()]
    }

    /// Borrow the authoritative normal state (residual, PSF, sensitivity).
    #[must_use]
    pub const fn normal_state(&self) -> &FinalNormalState {
        self.normal_state
    }

    /// Borrow the authoritative final model generation.
    #[must_use]
    pub const fn final_model(&self) -> &ModelGeneration {
        self.final_model
    }
}

/// One typed owner record retained in completions or seals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceRoleEvidence {
    /// The Final Normal State completion of the lineage.
    NormalState(FinalNormalStateCompletionId),
    /// The Major-Cycle completion of the lineage.
    MajorCycle(MajorCycleCompletionId),
    /// The frozen T18 weighting generation of the lineage.
    WeightingGeneration(casa_imaging_reconstruction::WeightingGenerationId),
    /// The authoritative T17 observation generation of the lineage.
    Observation(casa_imaging_model::SelectedObservationGenerationId),
}
