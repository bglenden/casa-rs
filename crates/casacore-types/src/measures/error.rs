// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for the measures module.

use std::fmt;

/// Errors that can occur during measure operations.
///
/// Corresponds to the various failure modes in C++ casacore measure
/// conversions: missing context data, unknown reference types, invalid
/// record format, and unimplemented conversion routes.
#[derive(Debug, Clone)]
pub enum MeasureError {
    /// A required piece of context data is missing from the [`MeasFrame`](super::MeasFrame).
    ///
    /// For example, converting between UT1 and UTC requires `dut1_seconds`
    /// to be set in the frame.
    MissingFrameData {
        /// Description of the missing data (e.g. "dUT1", "position").
        what: &'static str,
    },
    /// An unrecognised reference type string was encountered.
    UnknownRefType {
        /// The input string that could not be parsed.
        input: String,
    },
    /// A record could not be decoded as a valid measure.
    InvalidRecord {
        /// Human-readable reason for the failure.
        reason: String,
    },
    /// A unit does not match the expected dimension for a measure value.
    NonConformantUnit {
        /// The expected dimension (e.g. "time", "length").
        expected: &'static str,
        /// The unit string that was provided.
        got: String,
    },
    /// The requested conversion route is not yet implemented.
    ///
    /// Some time-scale conversions (UT2, GAST, LAST) require nutation
    /// models that are deferred to later waves.
    NotYetImplemented {
        /// Description of the unimplemented conversion route.
        route: String,
    },
    /// An error returned by a `sofars` function.
    SofarsError {
        /// The integer status code from sofars.
        code: i32,
    },
}

impl fmt::Display for MeasureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingFrameData { what } => {
                write!(f, "missing frame data: {what}")
            }
            Self::UnknownRefType { input } => {
                write!(f, "unknown reference type: {input:?}")
            }
            Self::InvalidRecord { reason } => {
                write!(f, "invalid measure record: {reason}")
            }
            Self::NonConformantUnit { expected, got } => {
                write!(f, "non-conformant unit: expected {expected}, got {got:?}")
            }
            Self::NotYetImplemented { route } => {
                write!(f, "conversion not yet implemented: {route}")
            }
            Self::SofarsError { code } => {
                write!(f, "sofars error: status code {code}")
            }
        }
    }
}

impl std::error::Error for MeasureError {}

#[cfg(test)]
mod tests {
    use super::MeasureError;

    #[test]
    fn display_covers_all_measure_error_variants() {
        let cases = [
            (
                MeasureError::MissingFrameData { what: "dUT1" },
                "missing frame data: dUT1",
            ),
            (
                MeasureError::UnknownRefType {
                    input: "BAD".to_string(),
                },
                "unknown reference type: \"BAD\"",
            ),
            (
                MeasureError::InvalidRecord {
                    reason: "missing value".to_string(),
                },
                "invalid measure record: missing value",
            ),
            (
                MeasureError::NonConformantUnit {
                    expected: "time",
                    got: "m".to_string(),
                },
                "non-conformant unit: expected time, got \"m\"",
            ),
            (
                MeasureError::NotYetImplemented {
                    route: "UT2 -> TDB".to_string(),
                },
                "conversion not yet implemented: UT2 -> TDB",
            ),
            (
                MeasureError::SofarsError { code: -42 },
                "sofars error: status code -42",
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(error.to_string(), expected);
            assert!(std::error::Error::source(&error).is_none());
        }
    }
}
