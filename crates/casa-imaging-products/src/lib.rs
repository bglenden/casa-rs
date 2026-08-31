// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! Native continuum product algorithms and the Product Generation Authority.
//!
//! This owner turns one authoritative Major-Cycle result into complete,
//! generation-consistent continuum products: restoring-beam fitting,
//! restoration, residual scaling, normalization, validity, metadata, and the
//! exact member set declared by the compiler-owned Product Graph. It owns the
//! entire generation-construction capability behind one two-phase authority:
//!
//! 1. [`ProductGenerationAuthority::plan`] binds the exact Product Graph and
//!    closed typed source commitments into one schema-versioned planned
//!    generation; and
//! 2. [`ProductGenerationAuthority::authorize`] seals the produced artifacts
//!    only against the matching closed typed completions.
//!
//! There is no other construction path: planned generations, artifact
//! identities, and seals have no public constructors and cannot be
//! reconstructed from digest bytes. Publication consumes only the exact
//! sealed member set through private prepare → independently atomic member
//! replacement → per-member terminal receipt choreography.

mod authority;
mod beam;
mod digest;
mod error;
mod projection;
mod restore;
mod source;
mod taylor;
mod visibility;

pub use authority::{
    CONTINUUM_ALGORITHM_CATALOG_VERSION, ContinuumCommitmentId, ContinuumCompletionsId,
    ContinuumProducedMembers, ContinuumProductControls, ContinuumSealId, DEFAULT_PSF_CUTOFF,
    MemberArtifactId, PlannedContinuumGeneration, PlannedGenerationId, PlannedMember,
    ProductGenerationAuthority, SealedContinuumGeneration, SealedMember, SealedMemberContract,
    produce_continuum_members,
};
pub use beam::{RestoringBeam, fit_restoring_beam};
pub use error::ProductsError;
pub use projection::{PublicationMemberProjection, PublicationProjection};
pub use restore::{
    ResidualBeamScaling, fft_convolve, gaussian_beam_image, normalize_plane,
    rescale_residual_to_beam,
};
pub use source::{ContinuumProductInputs, ContinuumSourceCatalog};
pub use visibility::{
    ModelVisibilityProductId, ResidualVisibilityProductId, VisibilityProductAuthority,
    VisibilityProductCompletion, VisibilityProductError,
};

#[cfg(test)]
mod tests {
    #[test]
    fn algorithm_catalog_version_is_pinned() {
        assert_eq!(super::CONTINUUM_ALGORITHM_CATALOG_VERSION, 5);
        assert_eq!(super::DEFAULT_PSF_CUTOFF, 0.35);
    }
}
