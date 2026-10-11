// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The products of an imaging run (plan section 5.7).
//!
//! This crate turns a major cycle's final normal state and model into the
//! members of the problem's product list: restoring-beam fitting,
//! restoration, normalization, validity and metadata. It writes each member
//! in bounded windows straight to its output.

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
    AnalyticPrimaryBeamModel, ContinuumProductControls, DEFAULT_PSF_CUTOFF,
    PlannedContinuumGeneration, PlannedMember, PublishedContinuumGeneration, PublishedMember,
    produce_continuum_members,
};
pub use restore::{
    MosaicSensitivity, ResidualBeamScaling, fft_convolve, gaussian_beam_image, normalize_plane,
    rescale_residual_to_beam,
};
pub use source::ContinuumProductInputs;
pub use storage::{
    ProductOutput, ProductStoragePlan, ProductWindow, ProductWindowExecutor, ProductWindowLayout,
    ProductWriter,
};
pub use visibility::VisibilityProductCompletion;
