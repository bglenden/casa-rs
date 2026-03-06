// SPDX-License-Identifier: LGPL-3.0-or-later
//! Doppler measure: velocity/frequency ratio in various conventions.
//!
//! This module provides:
//!
//! - [`DopplerRef`] — the 5 Doppler reference conventions supported by casacore.
//! - [`MDoppler`] — a Doppler measure pairing a value with a convention, equivalent
//!   to C++ `MDoppler`.
//!
//! All conversions go through [`DopplerRef::RATIO`] as a hub (the frequency ratio
//! f/f₀), using pure algebra — no frame data is needed.
//!
//! # Conventions
//!
//! | Convention | Value meaning | Formula (to RATIO) |
//! |------------|---------------|-------------------|
//! | RADIO | v_radio/c | ratio = 1 − v |
//! | Z (OPTICAL)| z = Δf/f | ratio = 1/(1 + z) |
//! | RATIO | f/f₀ | identity |
//! | BETA (RELATIVISTIC) | v/c (relativistic) | ratio = √((1−β)/(1+β)) |
//! | GAMMA | Lorentz factor γ | β = √(1−1/γ²), then as BETA |

use std::fmt;
use std::str::FromStr;

use super::error::MeasureError;
use super::frame::MeasFrame;

// ---------------------------------------------------------------------------
// DopplerRef
// ---------------------------------------------------------------------------

/// Doppler reference conventions.
///
/// Corresponds to C++ `MDoppler::Types`. All five conventions describe the
/// same physical Doppler shift using different parameterizations.
///
/// # Aliases
///
/// When parsing from strings:
/// - `OPTICAL` → [`Z`](DopplerRef::Z)
/// - `RELATIVISTIC` → [`BETA`](DopplerRef::BETA)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DopplerRef {
    /// Radio convention: v = c(1 − f/f₀). Value is v/c.
    RADIO,
    /// Optical/redshift convention: z = f₀/f − 1. Value is z.
    ///
    /// Also known as `OPTICAL` in C++ casacore.
    Z,
    /// Frequency ratio: f/f₀. Dimensionless, in (0, ∞).
    RATIO,
    /// Relativistic velocity: v/c from the full Lorentz formula.
    ///
    /// Also known as `RELATIVISTIC` in C++ casacore.
    BETA,
    /// Lorentz factor: γ = 1/√(1 − β²). Value ≥ 1.
    GAMMA,
}

impl DopplerRef {
    /// Returns the C++ casacore integer code for this reference type.
    ///
    /// These codes match the `MDoppler::Types` enum values defined in C++
    /// `MDoppler.h`.
    pub fn casacore_code(self) -> i32 {
        match self {
            Self::RADIO => 0,
            Self::Z => 1, // C++ OPTICAL=1
            Self::RATIO => 2,
            Self::BETA => 3, // C++ RELATIVISTIC=3
            Self::GAMMA => 4,
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::RADIO),
            1 => Some(Self::Z),
            2 => Some(Self::RATIO),
            3 => Some(Self::BETA),
            4 => Some(Self::GAMMA),
            _ => None,
        }
    }

    /// All 5 reference types in canonical order.
    pub const ALL: [DopplerRef; 5] = [Self::RADIO, Self::Z, Self::RATIO, Self::BETA, Self::GAMMA];

    /// Returns the canonical string name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RADIO => "RADIO",
            Self::Z => "Z",
            Self::RATIO => "RATIO",
            Self::BETA => "BETA",
            Self::GAMMA => "GAMMA",
        }
    }
}

impl FromStr for DopplerRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "RADIO" => Ok(Self::RADIO),
            "Z" | "OPTICAL" => Ok(Self::Z),
            "RATIO" => Ok(Self::RATIO),
            "BETA" | "RELATIVISTIC" => Ok(Self::BETA),
            "GAMMA" => Ok(Self::GAMMA),
            _ => Err(MeasureError::UnknownRefType {
                input: s.to_owned(),
            }),
        }
    }
}

impl fmt::Display for DopplerRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ---------------------------------------------------------------------------
// MDoppler
// ---------------------------------------------------------------------------

/// A Doppler measure: a velocity/frequency ratio in a specified convention.
///
/// `MDoppler` stores a single `f64` value whose interpretation depends on the
/// [`DopplerRef`] convention. This is the Rust equivalent of C++ `casa::MDoppler`.
///
/// # Conversions
///
/// All conversions route through [`DopplerRef::RATIO`] (the frequency ratio)
/// as a hub. The math is pure algebra — no [`MeasFrame`] data is needed, but
/// the `frame` parameter is accepted for API consistency with other measures.
///
/// # Examples
///
/// ```
/// use casacore_types::measures::doppler::{MDoppler, DopplerRef};
/// use casacore_types::measures::MeasFrame;
///
/// // A source at z = 1 (redshift)
/// let z = MDoppler::new(1.0, DopplerRef::Z);
/// let frame = MeasFrame::new();
/// let radio = z.convert_to(DopplerRef::RADIO, &frame).unwrap();
/// // Radio velocity = 1 - 1/(1+z) = 0.5
/// assert!((radio.value() - 0.5).abs() < 1e-12);
/// ```
#[derive(Debug, Clone)]
pub struct MDoppler {
    value: f64,
    refer: DopplerRef,
}

impl MDoppler {
    /// Creates a new `MDoppler` from a value and convention.
    pub fn new(value: f64, refer: DopplerRef) -> Self {
        Self { value, refer }
    }

    /// Returns the stored value.
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Returns the reference convention.
    pub fn refer(&self) -> DopplerRef {
        self.refer
    }

    /// Converts this Doppler measure to a different convention.
    ///
    /// The `frame` parameter is unused (Doppler conversions are pure algebra)
    /// but accepted for API consistency with other measure types.
    pub fn convert_to(
        &self,
        target: DopplerRef,
        _frame: &MeasFrame,
    ) -> Result<MDoppler, MeasureError> {
        if self.refer == target {
            return Ok(self.clone());
        }

        // Convert to RATIO first, then to target.
        let ratio = to_ratio(self.value, self.refer);
        let out = from_ratio(ratio, target);

        Ok(MDoppler {
            value: out,
            refer: target,
        })
    }
}

impl fmt::Display for MDoppler {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Doppler: {} {}", self.value, self.refer)
    }
}

// ---------------------------------------------------------------------------
// Conversion formulas
// ---------------------------------------------------------------------------

/// Converts a value in the given convention to RATIO (f/f₀).
fn to_ratio(value: f64, from: DopplerRef) -> f64 {
    match from {
        DopplerRef::RATIO => value,
        DopplerRef::RADIO => 1.0 - value,
        DopplerRef::Z => 1.0 / (1.0 + value),
        DopplerRef::BETA => ((1.0 - value) / (1.0 + value)).sqrt(),
        DopplerRef::GAMMA => {
            let beta = (1.0 - 1.0 / (value * value)).sqrt();
            ((1.0 - beta) / (1.0 + beta)).sqrt()
        }
    }
}

/// Converts a RATIO value (f/f₀) to the target convention.
fn from_ratio(ratio: f64, to: DopplerRef) -> f64 {
    match to {
        DopplerRef::RATIO => ratio,
        DopplerRef::RADIO => 1.0 - ratio,
        DopplerRef::Z => 1.0 / ratio - 1.0,
        DopplerRef::BETA => {
            let r2 = ratio * ratio;
            (1.0 - r2) / (1.0 + r2)
        }
        DopplerRef::GAMMA => {
            let r2 = ratio * ratio;
            let beta = (1.0 - r2) / (1.0 + r2);
            1.0 / (1.0 - beta * beta).sqrt()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doppler_ref_parse_all() {
        for r in DopplerRef::ALL {
            let parsed: DopplerRef = r.as_str().parse().unwrap();
            assert_eq!(parsed, r);
        }
    }

    #[test]
    fn doppler_ref_parse_aliases() {
        assert_eq!("OPTICAL".parse::<DopplerRef>().unwrap(), DopplerRef::Z);
        assert_eq!(
            "RELATIVISTIC".parse::<DopplerRef>().unwrap(),
            DopplerRef::BETA
        );
    }

    #[test]
    fn radio_to_ratio_and_back() {
        let frame = MeasFrame::new();
        let radio = MDoppler::new(0.5, DopplerRef::RADIO);
        let ratio = radio.convert_to(DopplerRef::RATIO, &frame).unwrap();
        assert!((ratio.value() - 0.5).abs() < 1e-15);
        let back = ratio.convert_to(DopplerRef::RADIO, &frame).unwrap();
        assert!((back.value() - 0.5).abs() < 1e-15);
    }

    #[test]
    fn z_to_ratio() {
        let frame = MeasFrame::new();
        // z=1 → ratio = 1/(1+1) = 0.5
        let z = MDoppler::new(1.0, DopplerRef::Z);
        let ratio = z.convert_to(DopplerRef::RATIO, &frame).unwrap();
        assert!((ratio.value() - 0.5).abs() < 1e-15);
    }

    #[test]
    fn beta_to_ratio() {
        let frame = MeasFrame::new();
        // β=0 → ratio = 1 (no shift)
        let beta = MDoppler::new(0.0, DopplerRef::BETA);
        let ratio = beta.convert_to(DopplerRef::RATIO, &frame).unwrap();
        assert!((ratio.value() - 1.0).abs() < 1e-15);
    }

    #[test]
    fn gamma_to_ratio() {
        let frame = MeasFrame::new();
        // γ=1 → β=0 → ratio=1
        let gamma = MDoppler::new(1.0, DopplerRef::GAMMA);
        let ratio = gamma.convert_to(DopplerRef::RATIO, &frame).unwrap();
        assert!((ratio.value() - 1.0).abs() < 1e-15);
    }

    #[test]
    fn roundtrip_all_conventions() {
        let frame = MeasFrame::new();
        let original = MDoppler::new(0.3, DopplerRef::RADIO);

        for target in DopplerRef::ALL {
            let converted = original.convert_to(target, &frame).unwrap();
            let back = converted.convert_to(DopplerRef::RADIO, &frame).unwrap();
            assert!(
                (back.value() - 0.3).abs() < 1e-12,
                "Roundtrip via {target} failed: got {} expected 0.3",
                back.value()
            );
        }
    }

    #[test]
    fn casacore_code_roundtrip() {
        for r in DopplerRef::ALL {
            let code = r.casacore_code();
            assert_eq!(DopplerRef::from_casacore_code(code), Some(r));
        }
    }

    #[test]
    fn casacore_code_known_values() {
        assert_eq!(DopplerRef::RADIO.casacore_code(), 0);
        assert_eq!(DopplerRef::Z.casacore_code(), 1);
        assert_eq!(DopplerRef::GAMMA.casacore_code(), 4);
    }
}
