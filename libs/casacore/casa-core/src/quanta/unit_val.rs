// SPDX-License-Identifier: LGPL-3.0-or-later
//! A scale factor combined with SI dimensions.
//!
//! [`UnitVal`] is the core internal representation used by the unit system:
//! a positive `f64` scale factor paired with a [`UnitDim`] exponent vector.
//! It is the Rust counterpart of C++ `casa::UnitVal`.
//!
//! Two `UnitVal` values are *conformant* when their dimensions match,
//! regardless of scale.  Arithmetic on `UnitVal` follows dimensional
//! analysis rules.

use crate::quanta::dim::{Dimension, UnitDim};
use crate::quanta::error::UnitError;

/// A unit value: a scale factor relative to SI combined with dimensions.
///
/// The `factor` is always relative to the coherent SI unit for those
/// dimensions.  For example, `km` has factor `1000.0` with dimension
/// `[1,0,0,0,0,0,0,0,0,0]` (length).
///
/// Corresponds to C++ `casa::UnitVal`.
#[derive(Debug, Clone, Copy)]
pub struct UnitVal {
    /// Scale factor relative to the SI base for these dimensions.
    pub factor: f64,
    /// SI dimension exponents.
    pub dim: UnitDim,
}

impl UnitVal {
    /// Dimensionless unit with factor 1.
    pub const NODIM: Self = Self {
        factor: 1.0,
        dim: UnitDim::NODIM,
    };

    /// Explicitly undimensioned (the `_` unit in casacore).
    pub const UNDIM: Self = Self {
        factor: 1.0,
        dim: UnitDim::new([0, 0, 0, 0, 0, 0, 0, 0, 0, 1]),
    };

    /// Length (metre).
    pub const LENGTH: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::Length),
    };

    /// Mass (kilogram).
    pub const MASS: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::Mass),
    };

    /// Time (second).
    pub const TIME: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::Time),
    };

    /// Electric current (ampere).
    pub const CURRENT: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::Current),
    };

    /// Temperature (kelvin).
    pub const TEMPERATURE: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::Temperature),
    };

    /// Luminous intensity (candela).
    pub const LUMINOUS_INTENSITY: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::LuminousIntensity),
    };

    /// Amount of substance (mole).
    pub const AMOUNT: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::Amount),
    };

    /// Plane angle (radian).
    pub const ANGLE: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::Angle),
    };

    /// Solid angle (steradian).
    pub const SOLID_ANGLE: Self = Self {
        factor: 1.0,
        dim: UnitDim::basis(Dimension::SolidAngle),
    };

    /// Creates a new `UnitVal` from a factor and dimension.
    pub const fn new(factor: f64, dim: UnitDim) -> Self {
        Self { factor, dim }
    }

    /// Returns `true` if this unit is dimensionally conformant with `other`.
    ///
    /// Two units are conformant if and only if they have identical dimension
    /// exponents.  The scale factor is ignored.
    pub fn conformant(&self, other: &Self) -> bool {
        self.dim.conformant(&other.dim)
    }

    /// Raises the unit to an integer power.
    pub fn pow(&self, n: i32) -> Self {
        Self {
            factor: self.factor.powi(n),
            dim: self.dim.pow(n),
        }
    }

    /// Takes an integer root.
    pub fn root(&self, n: i32) -> Result<Self, UnitError> {
        Ok(Self {
            factor: self.factor.powf(1.0 / n as f64),
            dim: self.dim.root(n)?,
        })
    }

    /// Takes the square root.
    pub fn sqrt(&self) -> Result<Self, UnitError> {
        self.root(2)
    }
}

impl std::ops::Mul for UnitVal {
    type Output = Self;
    fn mul(self, rhs: Self) -> Self {
        Self {
            factor: self.factor * rhs.factor,
            dim: self.dim * rhs.dim,
        }
    }
}

impl std::ops::Div for UnitVal {
    type Output = Self;
    fn div(self, rhs: Self) -> Self {
        Self {
            factor: self.factor / rhs.factor,
            dim: self.dim / rhs.dim,
        }
    }
}

impl PartialEq for UnitVal {
    fn eq(&self, other: &Self) -> bool {
        self.dim == other.dim
            && (self.factor - other.factor).abs()
                < f64::EPSILON * self.factor.abs().max(other.factor.abs()).max(1.0)
    }
}

impl std::fmt::Display for UnitVal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}*{}", self.factor, self.dim)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodim_is_unity() {
        assert_eq!(UnitVal::NODIM.factor, 1.0);
        assert!(UnitVal::NODIM.dim.is_dimensionless());
    }

    #[test]
    fn length_has_correct_dim() {
        assert_eq!(UnitVal::LENGTH.dim.get(Dimension::Length), 1);
        assert_eq!(UnitVal::LENGTH.factor, 1.0);
    }

    #[test]
    fn mul_combines() {
        let km = UnitVal::new(1000.0, UnitDim::basis(Dimension::Length));
        let result = km * km;
        assert!((result.factor - 1e6).abs() < 1e-6);
        assert_eq!(result.dim.get(Dimension::Length), 2);
    }

    #[test]
    fn div_combines() {
        let m = UnitVal::LENGTH;
        let s = UnitVal::TIME;
        let v = m / s;
        assert_eq!(v.dim.get(Dimension::Length), 1);
        assert_eq!(v.dim.get(Dimension::Time), -1);
    }

    #[test]
    fn conformant_same_dims() {
        let km = UnitVal::new(1000.0, UnitDim::basis(Dimension::Length));
        assert!(km.conformant(&UnitVal::LENGTH));
    }

    #[test]
    fn not_conformant_different_dims() {
        assert!(!UnitVal::LENGTH.conformant(&UnitVal::TIME));
    }

    #[test]
    fn pow_and_root() {
        let m = UnitVal::LENGTH;
        let m2 = m.pow(2);
        assert_eq!(m2.dim.get(Dimension::Length), 2);
        let m_back = m2.sqrt().unwrap();
        assert_eq!(m_back.dim.get(Dimension::Length), 1);
        assert!((m_back.factor - 1.0).abs() < 1e-15);
    }
}
