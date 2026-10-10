// SPDX-License-Identifier: LGPL-3.0-or-later

//! The record of visibilities written back by the final major-cycle pass.

use casa_imaging_reconstruction::{ModelGenerationId, WeightingGenerationId};

/// Completed visibility write: its association and the number of cells
/// (row, channel, correlation) written; no content certificate.
///
/// Nothing checks that the written addresses are canonical or distinct: the
/// writing pass holds every plane in one traversal of the selection, which
/// visits each selected row once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityProductCompletion {
    final_model: ModelGenerationId,

    weighting_generation: WeightingGenerationId,
    sample_count: u64,
}

impl VisibilityProductCompletion {
    /// The record of `sample_count` visibilities written from `final_model`
    /// under `weighting_generation`.
    #[must_use]
    pub const fn new(
        final_model: ModelGenerationId,
        weighting_generation: WeightingGenerationId,
        sample_count: u64,
    ) -> Self {
        Self {
            final_model,
            weighting_generation,
            sample_count,
        }
    }

    /// Return the exact final model generation.
    #[must_use]
    pub const fn final_model(self) -> ModelGenerationId {
        self.final_model
    }
    /// Return the weighting generation of the pass that wrote them.
    #[must_use]
    pub const fn weighting_generation(self) -> WeightingGenerationId {
        self.weighting_generation
    }
    /// Return the number of cells written.
    #[must_use]
    pub const fn sample_count(self) -> u64 {
        self.sample_count
    }
}
