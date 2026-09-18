// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Native continuum product algorithms and bounded write-only generation.
//!
//! This owner turns one authoritative Major-Cycle result into complete,
//! generation-consistent continuum products: restoring-beam fitting,
//! restoration, residual scaling, normalization, validity, metadata, and the
//! exact member set declared by the compiler-owned Product Graph. It owns the
//! generation-construction capability and transfers bounded windows directly
//! to a caller-owned write-only output.

mod beam;
mod demand;
mod error;
mod generation;
mod restore;
mod source;
mod storage;
mod taylor;
mod visibility;

pub use beam::{RestoringBeam, fit_restoring_beam};
pub use demand::ContinuumGenerationDemand;
pub use error::ProductsError;
pub use generation::{
    AnalyticPrimaryBeamModel, CONTINUUM_ALGORITHM_CATALOG_VERSION, ContinuumProductControls,
    DEFAULT_PSF_CUTOFF, PlannedContinuumGeneration, PlannedMember, ProductMemberContract,
    PublishedContinuumGeneration, PublishedMember, produce_continuum_members,
};
pub use restore::{
    MosaicSensitivity, ResidualBeamScaling, fft_convolve, gaussian_beam_image, normalize_plane,
    rescale_residual_to_beam,
};
pub use source::ContinuumProductInputs;
pub use storage::{
    ProductOutput, ProductStoragePlan, ProductWindow, ProductWindowLayout, ProductWriter,
};
pub use visibility::{
    ModelVisibilityProductId, ResidualVisibilityProductId, VisibilityProductAuthority,
    VisibilityProductCompletion, VisibilityProductError,
};

#[cfg(test)]
mod tests {
    #[test]
    fn algorithm_catalog_version_is_pinned() {
        // v9 separated cube pixels and CASA publication masks; bump this pin
        // only together with a reviewed product-algorithm identity change.
        assert_eq!(super::CONTINUUM_ALGORITHM_CATALOG_VERSION, 9);
        assert_eq!(super::DEFAULT_PSF_CUTOFF, 0.35);
    }
}
