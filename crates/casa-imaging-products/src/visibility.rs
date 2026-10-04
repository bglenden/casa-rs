// SPDX-License-Identifier: LGPL-3.0-or-later

//! Ordering and count checks for owned paired-operator visibility streams.

use casa_imaging_model::{CompiledProblemId, MeasurementSetIdentity};
use casa_imaging_reconstruction::{
    ModelGenerationId, WeightingGenerationId, runtime_adapter::FinalVisibilitySample,
};
use thiserror::Error;

/// Ordering and count validation for bounded final-visibility blocks.
pub struct VisibilityProductProgress {
    problem: CompiledProblemId,
    final_model: ModelGenerationId,
    sample_count: u64,
    last_address: Option<(MeasurementSetIdentity, u64, u32, u32)>,
}

impl VisibilityProductProgress {
    /// Begin product projection for one exact problem and final model.
    #[must_use]
    pub fn new(problem: CompiledProblemId, final_model: ModelGenerationId) -> Self {
        Self {
            problem,
            final_model,
            sample_count: 0,
            last_address: None,
        }
    }

    /// Consume one bounded block in canonical selected-observation order.
    pub fn consume(
        &mut self,
        samples: &[FinalVisibilitySample],
    ) -> Result<(), VisibilityProductError> {
        for sample in samples {
            let address = sample.address();
            let order = (
                address.measurement_set,
                address.physical_row,
                address.channel_index,
                address.correlation_index,
            );
            if self.last_address.is_some_and(|previous| order <= previous) {
                return Err(VisibilityProductError::NoncanonicalAddress);
            }
            self.last_address = Some(order);
            self.sample_count = self
                .sample_count
                .checked_add(1)
                .ok_or(VisibilityProductError::SampleCountOverflow)?;
        }
        Ok(())
    }

    /// Close both products against the terminal selected/weighting generations.
    #[must_use]
    pub fn finish(
        self,

        weighting_generation: WeightingGenerationId,
    ) -> VisibilityProductCompletion {
        VisibilityProductCompletion {
            problem: self.problem,
            final_model: self.final_model,

            weighting_generation,
            sample_count: self.sample_count,
        }
    }
}

/// Completed stream association and count; no content certificate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityProductCompletion {
    problem: CompiledProblemId,
    final_model: ModelGenerationId,

    weighting_generation: WeightingGenerationId,
    sample_count: u64,
}

impl VisibilityProductCompletion {
    /// Return the compiled problem identity.
    #[must_use]
    pub const fn problem_id(self) -> CompiledProblemId {
        self.problem
    }
    /// Return the exact final model generation.
    #[must_use]
    pub const fn final_model(self) -> ModelGenerationId {
        self.final_model
    }
    /// Return the paired replay's weighting generation.
    #[must_use]
    pub const fn weighting_generation(self) -> WeightingGenerationId {
        self.weighting_generation
    }
    /// Return the selected sample count.
    #[must_use]
    pub const fn sample_count(self) -> u64 {
        self.sample_count
    }
}

/// Visibility projection failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum VisibilityProductError {
    /// Selected addresses were duplicated or arrived out of order.
    #[error("final visibility samples are not in canonical selected-observation order")]
    NoncanonicalAddress,
    /// Selected sample count overflowed.
    #[error("final visibility sample count overflowed")]
    SampleCountOverflow,
}
