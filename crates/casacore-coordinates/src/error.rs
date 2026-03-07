// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for the coordinate system module.
//!
//! [`CoordinateError`] covers axis-bounds violations, conversion failures,
//! unsupported projections, invalid serialised records, and dimension
//! mismatches. It is the unified error type returned by all coordinate
//! operations in this crate.

/// Errors arising from coordinate construction, conversion, and serialization.
///
/// This enum covers the full range of failures that coordinate operations can
/// encounter, from simple axis-index bounds checks to projection math failures.
/// It corresponds roughly to the AipsError exceptions thrown by C++ casacore's
/// Coordinates module.
#[derive(Debug, thiserror::Error)]
pub enum CoordinateError {
    /// An axis index exceeds the number of axes in the coordinate.
    #[error("axis index {index} out of range for {naxes} axes")]
    AxisOutOfRange {
        /// The requested axis index.
        index: usize,
        /// The total number of axes.
        naxes: usize,
    },

    /// A world-to-pixel or pixel-to-world conversion failed.
    #[error("conversion failed: {0}")]
    ConversionFailed(String),

    /// The requested projection type is not implemented.
    #[error("unsupported projection: {0}")]
    UnsupportedProjection(String),

    /// A serialised coordinate record could not be parsed.
    #[error("invalid coordinate record: {0}")]
    InvalidRecord(String),

    /// The number of axes or values supplied does not match expectations.
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch {
        /// The expected number of elements.
        expected: usize,
        /// The actual number of elements.
        got: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn axis_out_of_range_display() {
        let e = CoordinateError::AxisOutOfRange { index: 5, naxes: 3 };
        assert_eq!(e.to_string(), "axis index 5 out of range for 3 axes");
    }

    #[test]
    fn conversion_failed_display() {
        let e = CoordinateError::ConversionFailed("singular matrix".into());
        assert_eq!(e.to_string(), "conversion failed: singular matrix");
    }

    #[test]
    fn unsupported_projection_display() {
        let e = CoordinateError::UnsupportedProjection("BON".into());
        assert_eq!(e.to_string(), "unsupported projection: BON");
    }

    #[test]
    fn invalid_record_display() {
        let e = CoordinateError::InvalidRecord("missing crval".into());
        assert_eq!(e.to_string(), "invalid coordinate record: missing crval");
    }

    #[test]
    fn dimension_mismatch_display() {
        let e = CoordinateError::DimensionMismatch {
            expected: 2,
            got: 3,
        };
        assert_eq!(e.to_string(), "dimension mismatch: expected 2, got 3");
    }
}
