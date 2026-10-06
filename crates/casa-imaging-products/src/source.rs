// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct scientific inputs for continuum products.
//!
//! The inputs are minted only from a whole released Major-Cycle join, so one
//! atomic reconciliation result carries the scientific payloads together and
//! source/run association cannot be substituted independently.

use casa_imaging_model::{CompiledProblem, ImageDomainRole};
use casa_imaging_reconstruction::{
    CoupledReconstructionMask, FinalNormalState, FinalNormalStateCompletionId,
    ImageDomainReconstructionMasks, MajorCycleCompletion, MajorCycleCompletionId, ModelGeneration,
    ReconstructionMask, ReconstructionMaskGenerationId,
};

use crate::error::ProductsError;
/// Borrowed scientific payloads for producing planned members.
///
/// This value is minted only from a whole join plus its exact compiled
/// problem, so production always consumes the same atomic reconciliation
/// result named by the direct run associations.
#[derive(Debug)]
pub struct ContinuumProductInputs<'a> {
    problem: &'a CompiledProblem,
    major_cycle_completion: MajorCycleCompletionId,
    normal_state_completion: FinalNormalStateCompletionId,
    normal_state: &'a FinalNormalState,
    final_model: &'a ModelGeneration,
    reconstruction_mask: Option<&'a ReconstructionMask>,
    domain_reconstruction_masks: Option<&'a ImageDomainReconstructionMasks>,
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
            major_cycle_completion: join.completion_id(),
            normal_state_completion: join.normal_state().completion_id(),
            normal_state: join.normal_state(),
            final_model: join.final_model(),
            reconstruction_mask: None,
            domain_reconstruction_masks: None,
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
        self.domain_reconstruction_masks = None;
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
        self.domain_reconstruction_masks = None;
        self.coupled_masks = Some(masks);
        Ok(self)
    }

    /// Bind the exact spatial support for every canonical image domain.
    pub fn with_domain_reconstruction_masks(
        mut self,
        masks: &'a ImageDomainReconstructionMasks,
    ) -> Result<Self, ProductsError> {
        if masks.len() != self.normal_state.domain_count()
            || masks.len() != self.problem.geometry().domains().len()
            || self.normal_state.image_domain_mask_generation() != Some(masks.generation_id())
            || masks
                .iter()
                .zip(self.problem.geometry().domains())
                .any(|(mask, domain)| {
                    mask.problem_id() != self.problem.problem_id()
                        || mask.shape() != domain.shape().pixels()
                        || mask.coordinate() != domain.direction()
                })
        {
            return Err(ProductsError::SourceLineageMismatch);
        }
        self.reconstruction_mask = None;
        self.coupled_masks = None;
        self.domain_reconstruction_masks = Some(masks);
        Ok(self)
    }

    /// Borrow the exact compiled problem behind these payloads.
    #[must_use]
    pub const fn problem(&self) -> &CompiledProblem {
        self.problem
    }

    /// Return the released Major-Cycle run association for these payloads.
    #[must_use]
    pub const fn major_cycle_completion(&self) -> MajorCycleCompletionId {
        self.major_cycle_completion
    }

    /// Return the released Normal-State completion associated with this run.
    #[must_use]
    pub const fn normal_state_completion(&self) -> FinalNormalStateCompletionId {
        self.normal_state_completion
    }

    /// Return radians-per-pixel on each direction axis of the main domain.
    #[must_use]
    pub fn cell_size_rad(&self) -> [f64; 2] {
        self.cell_size_rad_for_domain(&ImageDomainRole::Main)
            .expect("compiled geometry contains exactly one main domain")
    }

    /// Return radians-per-pixel on each direction axis of one image domain.
    pub(crate) fn cell_size_rad_for_domain(
        &self,
        role: &ImageDomainRole,
    ) -> Result<[f64; 2], ProductsError> {
        let domain = self
            .problem
            .geometry()
            .domains()
            .iter()
            .find(|domain| domain.role() == role)
            .ok_or(ProductsError::SourceLineageMismatch)?;
        let increment = domain.direction().increment_rad();
        Ok([increment[0].abs(), increment[1].abs()])
    }

    /// Resolve the canonical model-domain ordinal from a compiler-owned role.
    pub(crate) fn model_domain_ordinal(
        &self,
        role: &ImageDomainRole,
    ) -> Result<usize, ProductsError> {
        let mut matches = self
            .final_model
            .shape()
            .domain_roles()
            .iter()
            .enumerate()
            .filter(|(_, candidate)| *candidate == role);
        let (ordinal, _) = matches.next().ok_or(ProductsError::SourceLineageMismatch)?;
        if matches.next().is_some() {
            return Err(ProductsError::SourceLineageMismatch);
        }
        Ok(ordinal)
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

    /// Borrow canonical per-domain CLEAN masks, when supplied.
    #[must_use]
    pub const fn domain_reconstruction_masks(&self) -> Option<&ImageDomainReconstructionMasks> {
        self.domain_reconstruction_masks
    }

    /// Return the one mask or per-domain mask-set generation bound to the inputs.
    #[must_use]
    pub fn reconstruction_mask_generation(&self) -> Option<ReconstructionMaskGenerationId> {
        self.domain_reconstruction_masks
            .map(ImageDomainReconstructionMasks::generation_id)
            .or_else(|| {
                self.reconstruction_mask
                    .map(ReconstructionMask::generation_id)
            })
    }

    /// Borrow both joint reconstruction masks, when supplied.
    #[must_use]
    pub const fn coupled_reconstruction_masks(&self) -> Option<&CoupledReconstructionMask> {
        self.coupled_masks
    }
}
