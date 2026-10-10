// SPDX-License-Identifier: LGPL-3.0-or-later

//! Direct scientific inputs for continuum products.
//!
//! The inputs borrow one whole Major-Cycle completion, so the final normal
//! state and model they carry come from the same reconciliation.

use casa_imaging_model::{CompiledProblem, ImageDomainRole};
use casa_imaging_reconstruction::{
    FinalNormalState, ImageDomainReconstructionMasks, MajorCycleCompletion, ModelGeneration,
    ReconstructionMask,
};

use crate::error::ProductsError;
/// Borrowed scientific payloads for producing planned members.
///
/// This value borrows one whole completion and its compiled problem, so the
/// final normal state and model it carries come from the same reconciliation.
#[derive(Debug)]
pub struct ContinuumProductInputs<'a> {
    problem: &'a CompiledProblem,
    normal_state: &'a FinalNormalState,
    final_model: &'a ModelGeneration,
    reconstruction_mask: Option<&'a ReconstructionMask>,
    domain_reconstruction_masks: Option<&'a ImageDomainReconstructionMasks>,
}

impl<'a> ContinuumProductInputs<'a> {
    /// Bind the borrowed payloads of one released join of `problem`.
    pub const fn from_major_cycle(
        problem: &'a CompiledProblem,
        join: &'a MajorCycleCompletion,
    ) -> Self {
        Self {
            problem,
            normal_state: join.normal_state(),
            final_model: join.final_model(),
            reconstruction_mask: None,
            domain_reconstruction_masks: None,
        }
    }

    /// Bind the exact reconstruction mask used by the final bounded solve.
    ///
    /// The mask must have the model-grid shape.
    pub fn with_reconstruction_mask(
        mut self,
        mask: &'a ReconstructionMask,
    ) -> Result<Self, ProductsError> {
        if mask.shape() != self.normal_state.shape() {
            return Err(ProductsError::ProblemShapeMismatch);
        }
        self.reconstruction_mask = Some(mask);
        self.domain_reconstruction_masks = None;
        Ok(self)
    }

    /// Bind the exact spatial support for every canonical image domain.
    pub fn with_domain_reconstruction_masks(
        mut self,
        masks: &'a ImageDomainReconstructionMasks,
    ) -> Result<Self, ProductsError> {
        if masks.len() != self.normal_state.domain_count()
            || masks.len() != self.problem.geometry().domains().len()
            || masks
                .iter()
                .zip(self.problem.geometry().domains())
                .any(|(mask, domain)| {
                    mask.shape() != domain.shape().pixels()
                        || mask.coordinate() != domain.direction()
                })
        {
            return Err(ProductsError::ProblemShapeMismatch);
        }
        self.reconstruction_mask = None;
        self.domain_reconstruction_masks = Some(masks);
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
            .ok_or(ProductsError::ProblemShapeMismatch)?;
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
        let (ordinal, _) = matches.next().ok_or(ProductsError::ProblemShapeMismatch)?;
        if matches.next().is_some() {
            return Err(ProductsError::ProblemShapeMismatch);
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
}
