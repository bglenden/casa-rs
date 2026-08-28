// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product authority for paired-operator visibility streams.

use std::fmt;

use casa_imaging_model::{
    CompiledProblemId, ContinuumTransformGenerationId, LogicalIdentity,
    SelectedObservationGenerationId, SelectedSampleAddress,
};
use casa_imaging_reconstruction::{
    ModelGenerationId, WeightingGenerationId, runtime_adapter::FinalVisibilitySample,
};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MODEL_VISIBILITY_DOMAIN: &[u8] = b"casa-rs-final-model-visibility-product";
const RESIDUAL_VISIBILITY_DOMAIN: &[u8] = b"casa-rs-final-residual-visibility-product";
const VISIBILITY_ADDRESS_DOMAIN: &[u8] = b"casa-rs-final-visibility-address-stream";
const MODEL_VALUE_DOMAIN: &[u8] = b"casa-rs-final-model-visibility-values";
const RESIDUAL_VALUE_DOMAIN: &[u8] = b"casa-rs-final-residual-visibility-values";
const VISIBILITY_PRODUCT_VERSION: u32 = 3;

macro_rules! visibility_identity {
    ($name:ident, $summary:literal) => {
        #[doc = $summary]
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(LogicalIdentity);

        impl $name {
            /// Return the exact SHA-256 digest.
            #[must_use]
            pub const fn as_bytes(self) -> [u8; 32] {
                self.0.as_bytes()
            }

            /// Return this identity for the persisted product owner.
            #[must_use]
            pub const fn identity(self) -> LogicalIdentity {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(concat!(stringify!($name), "("))?;
                for byte in self.as_bytes() {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str(")")
            }
        }
    };
}

visibility_identity!(
    ModelVisibilityProductId,
    "Content identity of final model visibilities over one exact selection."
);
visibility_identity!(
    ResidualVisibilityProductId,
    "Content identity of observed-minus-model visibilities over one exact selection."
);

/// Incremental product authority for bounded final-visibility blocks.
pub struct VisibilityProductAuthority {
    problem: CompiledProblemId,
    final_model: ModelGenerationId,
    address_hasher: Sha256,
    model_value_hasher: Sha256,
    residual_value_hasher: Sha256,
    sample_count: u64,
    last_address: Option<(u64, u32, u32)>,
}

impl VisibilityProductAuthority {
    /// Begin product projection for one exact problem and final model.
    #[must_use]
    pub fn new(problem: CompiledProblemId, final_model: ModelGenerationId) -> Self {
        let mut address_hasher = Sha256::new();
        address_hasher.update(VISIBILITY_ADDRESS_DOMAIN);
        address_hasher.update(VISIBILITY_PRODUCT_VERSION.to_le_bytes());
        let mut model_value_hasher = Sha256::new();
        model_value_hasher.update(MODEL_VALUE_DOMAIN);
        model_value_hasher.update(VISIBILITY_PRODUCT_VERSION.to_le_bytes());
        let mut residual_value_hasher = Sha256::new();
        residual_value_hasher.update(RESIDUAL_VALUE_DOMAIN);
        residual_value_hasher.update(VISIBILITY_PRODUCT_VERSION.to_le_bytes());
        Self {
            problem,
            final_model,
            address_hasher,
            model_value_hasher,
            residual_value_hasher,
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
                address.physical_row,
                address.channel_index,
                address.correlation_index,
            );
            if self.last_address.is_some_and(|previous| order <= previous) {
                return Err(VisibilityProductError::NoncanonicalAddress);
            }
            encode_address(&mut self.address_hasher, address);
            encode_complex(&mut self.model_value_hasher, sample.predicted());
            encode_complex(&mut self.residual_value_hasher, sample.residual());
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
        selected_generation: SelectedObservationGenerationId,
        continuum_transform_generation: Option<ContinuumTransformGenerationId>,
        weighting_generation: WeightingGenerationId,
    ) -> VisibilityProductCompletion {
        let address_digest: [u8; 32] = self.address_hasher.finalize().into();
        let model_value_digest: [u8; 32] = self.model_value_hasher.finalize().into();
        let residual_value_digest: [u8; 32] = self.residual_value_hasher.finalize().into();
        VisibilityProductCompletion {
            problem: self.problem,
            final_model: self.final_model,
            selected_generation,
            continuum_transform_generation,
            weighting_generation,
            sample_count: self.sample_count,
            model: ModelVisibilityProductId(product_identity(
                MODEL_VISIBILITY_DOMAIN,
                self.problem,
                self.final_model,
                selected_generation,
                continuum_transform_generation,
                weighting_generation,
                self.sample_count,
                address_digest,
                model_value_digest,
            )),
            residual: ResidualVisibilityProductId(product_identity(
                RESIDUAL_VISIBILITY_DOMAIN,
                self.problem,
                self.final_model,
                selected_generation,
                continuum_transform_generation,
                weighting_generation,
                self.sample_count,
                address_digest,
                residual_value_digest,
            )),
        }
    }
}

/// Closed product identities and exact operator provenance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityProductCompletion {
    problem: CompiledProblemId,
    final_model: ModelGenerationId,
    selected_generation: SelectedObservationGenerationId,
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    weighting_generation: WeightingGenerationId,
    sample_count: u64,
    model: ModelVisibilityProductId,
    residual: ResidualVisibilityProductId,
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
    /// Return the exact selected-observation generation.
    #[must_use]
    pub const fn selected_generation(self) -> SelectedObservationGenerationId {
        self.selected_generation
    }
    /// Return the sequential continuum-transform generation, when present.
    #[must_use]
    pub const fn continuum_transform_generation(self) -> Option<ContinuumTransformGenerationId> {
        self.continuum_transform_generation
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
    /// Return the model-visibility product identity.
    #[must_use]
    pub const fn model_product(self) -> ModelVisibilityProductId {
        self.model
    }
    /// Return the residual-visibility product identity.
    #[must_use]
    pub const fn residual_product(self) -> ResidualVisibilityProductId {
        self.residual
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

fn encode_address(hasher: &mut Sha256, address: SelectedSampleAddress) {
    hasher.update(address.measurement_set.identity().as_bytes());
    hasher.update(address.physical_row.to_le_bytes());
    hasher.update(address.data_description_id.to_le_bytes());
    hasher.update(address.spectral_window_id.to_le_bytes());
    hasher.update(address.channel_index.to_le_bytes());
    hasher.update(address.frequency_centre_hz.to_bits().to_le_bytes());
    hasher.update(address.frequency_lower_hz.to_bits().to_le_bytes());
    hasher.update(address.frequency_upper_hz.to_bits().to_le_bytes());
    hasher.update(address.channel_width_hz.to_bits().to_le_bytes());
    hasher.update((address.frequency_frame as u8).to_le_bytes());
    hasher.update(address.polarization_id.to_le_bytes());
    hasher.update(address.correlation_index.to_le_bytes());
    hasher.update((address.correlation_type as u8).to_le_bytes());
}

fn encode_complex(hasher: &mut Sha256, value: num_complex::Complex64) {
    hasher.update(value.re.to_bits().to_le_bytes());
    hasher.update(value.im.to_bits().to_le_bytes());
}

#[allow(clippy::too_many_arguments)]
fn product_identity(
    domain: &[u8],
    problem: CompiledProblemId,
    final_model: ModelGenerationId,
    selected_generation: SelectedObservationGenerationId,
    continuum_transform_generation: Option<ContinuumTransformGenerationId>,
    weighting_generation: WeightingGenerationId,
    sample_count: u64,
    address_digest: [u8; 32],
    value_digest: [u8; 32],
) -> LogicalIdentity {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(VISIBILITY_PRODUCT_VERSION.to_le_bytes());
    hasher.update(problem.as_bytes());
    hasher.update(final_model.as_bytes());
    hasher.update(selected_generation.as_bytes());
    match continuum_transform_generation {
        Some(generation) => {
            hasher.update([1]);
            hasher.update(generation.as_bytes());
        }
        None => hasher.update([0]),
    }
    hasher.update(weighting_generation.as_bytes());
    hasher.update(sample_count.to_le_bytes());
    hasher.update(address_digest);
    hasher.update(value_digest);
    LogicalIdentity::from_sha256(hasher.finalize().into())
}
