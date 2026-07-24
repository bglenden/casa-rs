// SPDX-License-Identifier: LGPL-3.0-or-later
//! Imaging errors for CASA-compatible dirty imaging and CLEAN.

use thiserror::Error;

/// Errors returned by the pure imaging core.
#[derive(Debug, Error)]
pub enum ImagingError {
    /// The caller supplied an inconsistent or incomplete imaging request.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    /// The caller requested an imaging mode that this wave intentionally rejects.
    #[error("unsupported mode: {0}")]
    Unsupported(String),
    /// All samples were dropped by validation, flagging, or weight checks.
    #[error("no usable visibility samples remain after validation and flagging")]
    NoUsableSamples,
    /// A normalization or deapodization step produced an unusable result.
    #[error("FFT/grid normalization failed: {0}")]
    Normalization(String),
    /// A convolution-function cache was missing, corrupt, or incompatible.
    #[error("convolution-function cache failed validation: {0}")]
    ConvolutionFunctionCache(String),
}
