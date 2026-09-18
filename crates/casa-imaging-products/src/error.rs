// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact failure reasons for continuum product construction and output.

use casa_imaging_model::{ProductRole, ProductTerm};
use thiserror::Error;

/// Exact reason product planning, production, or authorization failed closed.
#[derive(Debug, Clone, PartialEq, Error)]
pub enum ProductsError {
    /// A physical product output could not write or flush a window.
    #[error("product output failed: {0}")]
    Storage(String),
    /// A product window exceeded its shape or admitted capacity.
    #[error("product window exceeds its admitted shape or capacity")]
    InvalidWindow,
    /// Ordered product coverage was missing, repeated, or reordered.
    #[error("product window coverage requires channel {expected}, found {actual}")]
    WindowCoverage {
        /// Next required channel, or complete channel count at output finish.
        expected: usize,
        /// Actual start or completed channel count.
        actual: usize,
    },
    /// An authoritative Normal State window could not be read.
    #[error(transparent)]
    NormalAccess(#[from] casa_imaging_reconstruction::SpectralOperatorError),
    /// An authoritative model plane could not be read for product generation.
    #[error(transparent)]
    ModelAccess(#[from] casa_imaging_reconstruction::ModelLifecycleError),
    /// Shared reconstruction response normalization failed.
    #[error(transparent)]
    ImageResponse(#[from] casa_imaging_reconstruction::ImageResponseError),
    /// Production controls were outside their validated ranges.
    #[error("continuum production controls are invalid")]
    InvalidControls,
    /// The compiled problem is not a supported single-plane continuum problem.
    #[error("problem is not a supported single-plane constant-basis continuum problem")]
    UnsupportedProblem,
    /// The selected beam law cannot represent an output channel's frequency.
    #[error(
        "primary-beam model {model:?} does not support output channel {output_channel} at {frequency_hz} Hz"
    )]
    UnsupportedPrimaryBeamFrequency {
        /// Explicitly selected primary-beam law.
        model: crate::AnalyticPrimaryBeamModel,
        /// Zero-based output channel, after spectral coordinate conversion.
        output_channel: usize,
        /// Output channel centre, in Hz.
        frequency_hz: f64,
    },
    /// The Product Graph requested a role this catalog version cannot produce.
    #[error("product role {role:?} is not producible by algorithm catalog {catalog}")]
    UnsupportedProductRole {
        /// The first unsupported role encountered.
        role: ProductRole,
        /// Algorithm catalog version consulted.
        catalog: u32,
    },
    /// A Taylor term other than the zeroth coefficient was requested.
    #[error("Taylor term {term:?} exceeds the nterms=1 continuum algorithm catalog")]
    UnsupportedTaylorTerm {
        /// The offending Taylor term.
        term: ProductTerm,
    },
    /// The source evidence did not come from the same Major-Cycle result.
    #[error("source evidence does not match the Major-Cycle lineage")]
    SourceLineageMismatch,
    /// A produced payload length disagreed with its declared shape.
    #[error("member payload requires {expected} values but carries {actual}")]
    PayloadLengthMismatch {
        /// Shape-derived value count.
        expected: usize,
        /// Actual payload length.
        actual: usize,
    },
    /// Beam fitting failed for a graph that requires fitted beam metadata.
    #[error("restoring-beam fitting failed: {0}")]
    BeamFitFailed(String),
    /// Solver arithmetic produced a non-finite product value.
    #[error("product arithmetic generated a non-finite value")]
    GeneratedNonfinite,
    /// Checked product-generation resource arithmetic exceeded `u64`.
    #[error("continuum generation demand overflowed while calculating {0}")]
    ResourceDemandOverflow(&'static str),
}
