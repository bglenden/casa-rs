// SPDX-License-Identifier: LGPL-3.0-or-later
//! Error types for the unit/quantity system.
//!
//! [`UnitError`] covers all failure modes encountered during unit parsing,
//! dimensional analysis, and quantity conversion. It corresponds to the
//! runtime exceptions thrown by C++ `UnitVal::create()` and
//! `Quantum<T>::convert()`.

use std::fmt;

/// Errors that can occur during unit parsing, conversion, or arithmetic.
///
/// Corresponds to the runtime errors produced by C++ casacore's `UnitVal`,
/// `Unit`, and `Quantum<T>` classes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitError {
    /// The input string could not be parsed as a valid unit expression.
    ///
    /// Returned by the recursive descent parser when it encounters an
    /// unrecognised token, unbalanced parentheses, or other syntax errors.
    InvalidUnit {
        /// The original input string that failed to parse.
        input: String,
    },
    /// An operation required conformant (same-dimension) units but the
    /// operands had different dimensions.
    ///
    /// Returned by `Quantity` addition, subtraction, comparison, and
    /// explicit conversion when the source and target units are not
    /// dimensionally compatible.
    NonConformant {
        /// Human-readable description of the left-hand-side unit.
        lhs: String,
        /// Human-readable description of the right-hand-side unit.
        rhs: String,
    },
    /// A fractional root was requested but the resulting dimension exponents
    /// would not be integers.
    ///
    /// For example, taking the square root of a unit with an odd length
    /// exponent (e.g. `m^3`) is not representable.
    IndivisibleRoot,
    /// A root of degree zero was requested, which is undefined.
    ZeroRoot,
    /// A unit name was looked up in the registry but not found.
    UnknownUnit {
        /// The unrecognised unit name.
        name: String,
    },
}

impl fmt::Display for UnitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidUnit { input } => {
                write!(f, "invalid unit expression: {input:?}")
            }
            Self::NonConformant { lhs, rhs } => {
                write!(f, "non-conformant units: {lhs:?} vs {rhs:?}")
            }
            Self::IndivisibleRoot => {
                write!(f, "root would produce fractional dimension exponents")
            }
            Self::ZeroRoot => write!(f, "root of degree zero is undefined"),
            Self::UnknownUnit { name } => {
                write!(f, "unknown unit: {name:?}")
            }
        }
    }
}

impl std::error::Error for UnitError {}
