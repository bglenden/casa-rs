// SPDX-License-Identifier: LGPL-3.0-or-later

//! Exact failure reasons for continuum product construction and output.

use std::path::PathBuf;

use casa_imaging_model::{ProductRole, ProductTerm};
use thiserror::Error;

/// Why product planning, generation or writing failed.
#[derive(Debug, Error)]
pub enum ProductsError {
    /// A product image could not be created, written or saved.
    #[error("writing {}: {source}", path.display())]
    Image {
        /// The image's published path.
        path: PathBuf,
        /// What the image layer reported.
        #[source]
        source: casa_images::ImageError,
    },
    /// Staging or replacing a product image failed.
    #[error("{operation} {}: {source}", path.display())]
    Io {
        /// What the writer was doing.
        operation: &'static str,
        /// The directory or image it was doing it to.
        path: PathBuf,
        /// What the system reported.
        #[source]
        source: std::io::Error,
    },
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
    /// A direction-dependent sensitivity plane has no positive value: no
    /// weighted sample reached it.
    #[error("the sensitivity plane has no positive support")]
    UnsupportedSensitivity,
    /// Direction-dependent image units need flat-noise or flat-sky
    /// normalization.
    #[error("direction-dependent image units need flat-noise or flat-sky normalization")]
    UnsupportedNormalization,
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
    /// The product list publishes a role the products cannot produce.
    #[error("product role {role:?} cannot be produced")]
    UnsupportedProductRole {
        /// The first unsupported role encountered.
        role: ProductRole,
    },
    /// A Taylor term other than the zeroth coefficient was requested.
    #[error("Taylor term {term:?} exceeds the nterms=1 continuum algorithm catalog")]
    UnsupportedTaylorTerm {
        /// The offending Taylor term.
        term: ProductTerm,
    },
    /// A Taylor run's principal sum of weights, normal or published, is not
    /// positive and finite: no weighted sample reached the image.
    #[error("Taylor products need a positive principal sum of weights, not {sum_weight}")]
    PrincipalSumWeight {
        /// The principal sum of weights.
        sum_weight: f64,
    },
    /// A produced payload length disagreed with its declared shape.
    #[error("member payload requires {expected} values but carries {actual}")]
    PayloadLengthMismatch {
        /// Shape-derived value count.
        expected: usize,
        /// Actual payload length.
        actual: usize,
    },
    /// Beam fitting failed for products that carry fitted beam metadata.
    #[error("restoring-beam fitting failed: {0}")]
    BeamFitFailed(String),
    /// Solver arithmetic produced a non-finite product value.
    #[error("product arithmetic generated a non-finite value")]
    GeneratedNonfinite,
    /// Checked product-generation resource arithmetic exceeded `u64`.
    #[error("continuum generation demand overflowed while calculating {0}")]
    ResourceDemandOverflow(&'static str),
}
