// SPDX-License-Identifier: LGPL-3.0-or-later
//! Typed failures of the measurement operator.

use thiserror::Error;

use crate::sample::CfKey;

/// A failure a caller of the measurement operator can act on.
///
/// Violated invariants between operator-owned values (an accumulator used
/// with a different operator, a sample placed outside its tile, a block
/// whose polarization count differs from the routing) are programmer errors
/// and panic with the invariant named.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum OperatorError {
    /// The image or padded grid cannot be gridded: an extent is zero or odd,
    /// an increment is zero, or the reference pixel lies outside the image.
    #[error("unsupported grid geometry: {reason}")]
    Geometry {
        /// Which rule failed.
        reason: &'static str,
    },
    /// The requested polarization coordinates have no CASA correlation
    /// representation for the selected correlations.
    #[error("polarization routing: {reason}")]
    Polarization {
        /// Which rule failed.
        reason: &'static str,
    },
    /// Weight-image gridding was requested from a kernel set without
    /// `FT[PB²]` taps.
    #[error("convolution function {key:?} has no weight taps")]
    WeightKernelUnavailable {
        /// The cell that was asked for weight taps.
        key: CfKey,
    },
    /// FFT planning failed for the padded grid shape.
    #[error("FFT plan: {0}")]
    Fft(#[from] casa_fft::FftError),
    /// An accumulator covering a tile was finished before being merged into
    /// a full-grid accumulator.
    #[error("cannot finish a tiled accumulator; merge it into a full-grid accumulator first")]
    TiledAccumulator,
    /// Two accumulators with different layouts cannot be merged.
    #[error("accumulator layouts differ: {reason}")]
    AccumulatorLayout {
        /// Which layout property differs.
        reason: &'static str,
    },
    /// Model images do not match the operator's image shape, plane range or
    /// polarization count.
    #[error("model images do not match the operator: {reason}")]
    ModelShape {
        /// Which property differs.
        reason: &'static str,
    },
    /// A native row has inconsistent channel, correlation or value counts.
    #[error("native row: {reason}")]
    NativeRow {
        /// Which count is inconsistent.
        reason: &'static str,
    },
    /// The output spectral axis cannot be resampled from the native axis.
    #[error("spectral resampling: {reason}")]
    SpectralAxis {
        /// Which rule failed.
        reason: &'static str,
    },
    /// The density grid shape or a weighting parameter is invalid.
    #[error("weighting: {reason}")]
    Weighting {
        /// Which rule failed.
        reason: &'static str,
    },
}
