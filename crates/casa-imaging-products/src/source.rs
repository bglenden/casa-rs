// SPDX-License-Identifier: LGPL-3.0-or-later

//! Typed source catalog for continuum products.
//!
//! Every source role keeps its own typed owner record. The catalog is minted
//! only from a whole released Major-Cycle join, so one atomic reconciliation
//! result carries its multiple distinct completions together and no role's
//! evidence can be substituted for another.

use casa_imaging_model::{CompiledProblem, LogicalIdentity, ProductGraphId};
use casa_imaging_reconstruction::{
    CoupledReconstructionMask, FinalNormalState, FinalNormalStateCompletionId,
    MajorCycleCompletion, MajorCycleCompletionId, ModelGeneration, ModelGenerationId,
    NormalStateCatalog, ReconstructionMask, ReconstructionMaskGenerationId,
};

use crate::digest::{COMMITMENT_DOMAIN, COMMITMENT_VERSION, Encoder};
use crate::error::ProductsError;

#[derive(Debug, Clone, Copy)]
enum ContinuumNormalStateCatalog {
    PlaneV1,
    ChannelSlabV1,
    TaylorBlockV1,
    JointBlockV1,
}

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
    normal_state_catalog: ContinuumNormalStateCatalog,
    input_model_generation: ModelGenerationId,
    final_model_generation: ModelGenerationId,
    weighting_generation: casa_imaging_reconstruction::WeightingGenerationId,
    replay: casa_imaging_reconstruction::WeightingReplayId,
    coverage: casa_imaging_reconstruction::WeightingReplayCoverageId,
    selected_generation: casa_imaging_model::SelectedObservationGenerationId,
    continuum_transform_generation: Option<casa_imaging_model::ContinuumTransformGenerationId>,
    sample_count: u64,
    block_count: u64,
    reconstruction_mask: Option<ReconstructionMaskGenerationId>,
    line_reconstruction_mask: Option<ReconstructionMaskGenerationId>,
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
        Self::from_major_cycle_with_mask(problem, join, None)
    }

    /// Mint the catalog with the exact reconstruction mask used by the final
    /// bounded solve.
    ///
    /// The mask is a distinct scientific source from Final Normal State
    /// sensitivity/validity. Its immutable generation is therefore bound into
    /// the product commitment rather than inferred by the product owner.
    pub fn from_major_cycle_with_mask(
        problem: &CompiledProblem,
        join: &MajorCycleCompletion,
        mask: Option<&ReconstructionMask>,
    ) -> Result<Self, ProductsError> {
        let normal_state = join.normal_state();
        if normal_state.problem_id() != problem.problem_id() {
            return Err(ProductsError::SourceLineageMismatch);
        }
        if join.model_completion().problem() != problem.problem_id() {
            return Err(ProductsError::SourceLineageMismatch);
        }
        let normal_state_catalog = match normal_state.catalog() {
            NormalStateCatalog::UnnormalizedPlaneV1 => ContinuumNormalStateCatalog::PlaneV1,
            NormalStateCatalog::UnnormalizedChannelSlabV1 => {
                ContinuumNormalStateCatalog::ChannelSlabV1
            }
            NormalStateCatalog::UnnormalizedTaylorBlockV1 => {
                ContinuumNormalStateCatalog::TaylorBlockV1
            }
            NormalStateCatalog::UnnormalizedJointBlockV1 => {
                ContinuumNormalStateCatalog::JointBlockV1
            }
        };
        if mask.is_some_and(|mask| {
            mask.problem_id() != problem.problem_id() || mask.shape() != normal_state.shape()
        }) {
            return Err(ProductsError::SourceLineageMismatch);
        }
        Ok(Self {
            graph_id: problem.product_graph().graph_id(),
            major_cycle_completion: join.completion_id(),
            normal_state_completion: normal_state.completion_id(),
            normal_state_content: normal_state.content_identity(),
            normal_state_catalog,
            input_model_generation: normal_state.input_model_generation(),
            final_model_generation: normal_state.final_model_generation(),
            weighting_generation: normal_state.weighting_generation(),
            replay: normal_state.replay_id(),
            coverage: normal_state.coverage(),
            selected_generation: normal_state.selected_generation(),
            continuum_transform_generation: normal_state.continuum_transform_generation(),
            sample_count: normal_state.sample_count(),
            block_count: normal_state.block_count(),
            reconstruction_mask: mask.map(ReconstructionMask::generation_id),
            line_reconstruction_mask: None,
            problem: problem.clone(),
        })
    }

    /// Mint the catalog with both exact spatial supports of a joint solve.
    pub fn from_major_cycle_with_coupled_masks(
        problem: &CompiledProblem,
        join: &MajorCycleCompletion,
        masks: &CoupledReconstructionMask,
    ) -> Result<Self, ProductsError> {
        let mut catalog = Self::from_major_cycle_with_mask(problem, join, Some(masks.continuum()))?;
        if masks.line().problem_id() != problem.problem_id()
            || masks.line().shape() != join.normal_state().shape()
        {
            return Err(ProductsError::SourceLineageMismatch);
        }
        catalog.line_reconstruction_mask = Some(masks.line().generation_id());
        Ok(catalog)
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
            ContinuumNormalStateCatalog::PlaneV1 => 0,
            ContinuumNormalStateCatalog::ChannelSlabV1 => 1,
            ContinuumNormalStateCatalog::TaylorBlockV1 => 2,
            ContinuumNormalStateCatalog::JointBlockV1 => 3,
        });
        encoder.identity(self.input_model_generation.as_bytes());
        encoder.identity(self.final_model_generation.as_bytes());
        encoder.identity(self.weighting_generation.as_bytes());
        encoder.identity(self.replay.as_bytes());
        encoder.identity(self.coverage.as_bytes());
        encoder.identity(self.selected_generation.as_bytes());
        match self.continuum_transform_generation {
            Some(generation) => {
                encoder.u8(1);
                encoder.identity(generation.as_bytes());
            }
            None => encoder.u8(0),
        }
        encoder.u64(self.sample_count);
        encoder.u64(self.block_count);
        match self.reconstruction_mask {
            Some(generation) => {
                encoder.u8(1);
                encoder.identity(generation.as_bytes());
            }
            None => encoder.u8(0),
        }
        match self.line_reconstruction_mask {
            Some(generation) => {
                encoder.u8(1);
                encoder.identity(generation.as_bytes());
            }
            None => encoder.u8(0),
        }
        encoder.finish()
    }

    /// Return the immutable CLEAN-mask generation committed by this lineage.
    #[must_use]
    pub const fn reconstruction_mask_generation(&self) -> Option<ReconstructionMaskGenerationId> {
        self.reconstruction_mask
    }

    /// Return the line-mask generation committed by a joint lineage.
    #[must_use]
    pub const fn line_reconstruction_mask_generation(
        &self,
    ) -> Option<ReconstructionMaskGenerationId> {
        self.line_reconstruction_mask
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
    reconstruction_mask: Option<&'a ReconstructionMask>,
    coupled_masks: Option<&'a CoupledReconstructionMask>,
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
            reconstruction_mask: None,
            coupled_masks: None,
        })
    }

    /// Bind the exact reconstruction mask used by the final bounded solve.
    ///
    /// The mask must belong to this problem and model-grid shape.
    pub fn with_reconstruction_mask(
        mut self,
        mask: &'a ReconstructionMask,
    ) -> Result<Self, ProductsError> {
        if mask.problem_id() != self.problem.problem_id()
            || mask.shape() != self.normal_state.shape()
        {
            return Err(ProductsError::SourceLineageMismatch);
        }
        self.reconstruction_mask = Some(mask);
        self.coupled_masks = None;
        Ok(self)
    }

    /// Bind both exact spatial supports used by a joint solve.
    pub fn with_coupled_reconstruction_masks(
        mut self,
        masks: &'a CoupledReconstructionMask,
    ) -> Result<Self, ProductsError> {
        for mask in [masks.continuum(), masks.line()] {
            if mask.problem_id() != self.problem.problem_id()
                || mask.shape() != self.normal_state.shape()
            {
                return Err(ProductsError::SourceLineageMismatch);
            }
        }
        self.reconstruction_mask = Some(masks.continuum());
        self.coupled_masks = Some(masks);
        Ok(self)
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

    /// Borrow the exact CLEAN mask, when a bounded solve supplied one.
    #[must_use]
    pub const fn reconstruction_mask(&self) -> Option<&ReconstructionMask> {
        self.reconstruction_mask
    }

    /// Borrow both joint reconstruction masks, when supplied.
    #[must_use]
    pub const fn coupled_reconstruction_masks(&self) -> Option<&CoupledReconstructionMask> {
        self.coupled_masks
    }
}
