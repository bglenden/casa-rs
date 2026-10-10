// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Authoritative solver-independent model-state lifecycle.
//!
//! The model crate supplies only closed commitments and value schemas. This
//! crate owns the model, its updates and the major cycle that pairs it with
//! the normal state, without importing storage, execution, product, or solver
//! APIs.

mod block_normal;
mod continuum_transform;
mod image_response;
mod major_cycle;
mod normal_values;
pub use normal_values::{NormalValues, SensitivityValues};
mod mask;
mod model_lifecycle;
mod model_storage;
mod spectral_operator;

#[doc(hidden)]
pub use model_storage::{
    ModelSampleStorage, ModelSampleUpdate, ModelStorageFactory, ModelStoragePlan,
};

pub use image_response::{ImageResponseError, MinorCycleImageResponse, MosaicSensitivity};
pub use spectral_operator::{
    PassImages, PassNormalState, SpectralChannelValidity, SpectralOperatorError,
    SpectralOperatorPrimitives, SpectralPrimitiveCatalog, SpectralSlabPlan,
};

/// Internal composition surface used by `casa-imaging-runtime`.
///
/// These opaque operations are public only because Rust crates have no friend
/// visibility. Application code should use the runtime's plan-bound T19 API.
#[doc(hidden)]
pub mod runtime_adapter {
    pub use crate::spectral_operator::CompleteDataOwnerCompletion;
    pub use crate::spectral_operator::normal_storage::{
        CompleteDataNormalState, CompleteDataNormalWindow, NormalArrayStorage,
        NormalStorageFactory, NormalStoragePlan,
    };
}

pub use continuum_transform::{
    ContinuumFitError, ContinuumFitStatus, ContinuumRowInput, ContinuumRowResult, ContinuumSample,
    fit_and_subtract_continuum,
};
pub use major_cycle::{
    FinalNormalDomainState, FinalNormalState, FinalNormalStateCoefficientTerm,
    FinalNormalStateNormalMoment, FinalNormalStatePlane, FinalNormalStateWindow, MajorCycle,
    MajorCycleCompletion, MajorCycleError, NormalStateCatalog, normal_state_window_residency_bytes,
};
pub use mask::{
    AutoMaskBeam, AutoMultithreshControls, AutoMultithreshEvidence, ImageDomainMaskMaterialization,
    ImageDomainReconstructionMaskPlans, ImageDomainReconstructionMasks, MaskBox, MaskError,
    ReconstructionMask, ReconstructionMaskPlan, ReconstructionMaskSet, auto_multithresh,
    direction_world_to_pixel, reproject_mask_support,
};
pub(crate) use model_lifecycle::validate_model_value;
pub use model_lifecycle::{
    ModelGeneration, ModelLifecycle, ModelLifecycleError, PreparedFinalModel,
};
pub use spectral_operator::normal_storage::FinalNormalPlaneReader;
