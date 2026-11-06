// SPDX-License-Identifier: LGPL-3.0-or-later
#![warn(missing_docs)]

//! The products of an imaging run.
//!
//! This crate turns a major cycle's final normal state and model into the
//! members of the problem's product list: normalization, restoring-beam
//! fitting, restoration, Taylor products, PB and pbcor, validity and
//! metadata. It writes each member in bounded windows to private staging
//! beside its CASA image target, then replaces each target atomically.

mod analytic_beam;
mod beam;
mod demand;
mod error;
mod generation;
mod mvc_cube;
mod mvc_normal;
mod mvc_reduction;
mod normalization;
mod plan;
mod plane;
mod restore;
mod source;
mod storage;
mod taylor;
mod taylor_beam;
mod taylor_moments;
mod visibility;
mod writer;

pub use analytic_beam::{ANALYTIC_PRIMARY_BEAM_BYTES_PER_CELL, AnalyticPrimaryBeam};
pub use beam::{RestoringBeam, fit_restoring_beam};
pub use demand::ContinuumGenerationDemand;
pub use error::ProductsError;
pub use generation::produce_continuum_members;
pub use mvc_cube::{
    MvcCubePlane, MvcCubePrimaryBeam, read_mvc_cube_plane, read_mvc_cube_primary_beam,
};
pub use mvc_normal::{encode_mvc_blank_taylor_planes, encode_mvc_taylor_planes};
pub use mvc_reduction::reduce_mvc_cube;
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
pub use taylor_beam::TaylorPrimaryBeamAccumulator;
pub use visibility::VisibilityProductCompletion;
pub use writer::{CasaImageDomainOutput, CasaImageWriter};

pub use taylor_moments::TaylorMomentAccumulator;
