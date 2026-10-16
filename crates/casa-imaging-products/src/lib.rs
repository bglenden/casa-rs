// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The products of an imaging run (plan section 5.7).
//!
//! This crate turns a major cycle's final normal state and model into the
//! members of the problem's product list: normalization, restoring-beam
//! fitting, restoration, Taylor products, PB and pbcor, validity and
//! metadata. It writes each member in bounded windows to private staging
//! beside its CASA image target, then replaces each target atomically.

mod beam;
mod demand;
mod error;
mod generation;
mod normalization;
mod plan;
mod plane;
mod restore;
mod source;
mod storage;
mod taylor;
mod visibility;
mod writer;

pub use beam::{RestoringBeam, fit_restoring_beam};
pub use demand::ContinuumGenerationDemand;
pub use error::ProductsError;
pub use generation::produce_continuum_members;
pub use normalization::{
    MinorCycleImageResponse, MosaicSensitivity, ResidualFamily, normalize_plane,
};
pub use plan::{
    AnalyticPrimaryBeamModel, ContinuumProductControls, DEFAULT_PSF_CUTOFF,
    PlannedContinuumGeneration, PlannedMember, PublishedContinuumGeneration, PublishedMember,
};
pub use restore::{
    ResidualBeamScaling, fft_convolve, gaussian_beam_image, rescale_residual_to_beam,
};
pub use source::{ContinuumProductInputs, FacetSumWeights, PrimaryBeamCentres};
pub use storage::{ProductStoragePlan, ProductWindow, ProductWindowExecutor, ProductWindowLayout};
pub use visibility::VisibilityProductCompletion;
pub use writer::{CasaImageDomainOutput, CasaImageWriter};
