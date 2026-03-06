// SPDX-License-Identifier: LGPL-3.0-or-later
//! Dimensional analysis types for SI base dimensions.
//!
//! [`UnitDim`] stores the exponents of the nine SI base dimensions (plus an
//! undimensioned marker) as a compact `[i8; 10]` array.  It is the Rust
//! counterpart of C++ `casa::UnitDim`.
//!
//! Arithmetic on dimensions follows the rules of dimensional analysis:
//! multiplication adds exponents, division subtracts them, and powers
//! scale them.

use crate::quanta::error::UnitError;

/// Number of tracked dimensions (9 SI + 1 undimensioned marker).
pub const NDIM: usize = 10;

/// Named indices into the [`UnitDim`] exponent array.
///
/// These match the C++ `UnitDim::Dim` enum values and identify which
/// position in the `[i8; 10]` array corresponds to each physical dimension.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum Dimension {
    /// Length (metre).
    Length = 0,
    /// Mass (kilogram).
    Mass = 1,
    /// Time (second).
    Time = 2,
    /// Electric current (ampere).
    Current = 3,
    /// Temperature (kelvin).
    Temperature = 4,
    /// Luminous intensity (candela).
    LuminousIntensity = 5,
    /// Amount of substance (mole).
    Amount = 6,
    /// Plane angle (radian).
    Angle = 7,
    /// Solid angle (steradian).
    SolidAngle = 8,
    /// Undimensioned / non-quantity marker.
    ///
    /// When this exponent is set to 1 the unit is explicitly dimensionless
    /// (like `_` in casacore), as opposed to a unit whose dimensions simply
    /// happen to cancel out.
    Undefined = 9,
}

/// A set of SI dimension exponents.
///
/// Each element of the inner `[i8; 10]` array records the exponent of one
/// SI base dimension (or the undimensioned marker at index 9).  For example,
/// velocity (m/s) is represented as `[1, 0, -1, 0, 0, 0, 0, 0, 0, 0]`.
///
/// Corresponds to C++ `casa::UnitDim`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct UnitDim {
    /// Exponent array indexed by [`Dimension`] variants.
    pub dims: [i8; NDIM],
}

impl UnitDim {
    /// Dimensionless (all exponents zero).
    pub const NODIM: Self = Self { dims: [0; NDIM] };

    /// Creates a `UnitDim` from a raw exponent array.
    pub const fn new(dims: [i8; NDIM]) -> Self {
        Self { dims }
    }

    /// Creates a `UnitDim` with a single dimension set to exponent 1.
    pub const fn basis(d: Dimension) -> Self {
        let mut dims = [0i8; NDIM];
        dims[d as usize] = 1;
        Self { dims }
    }

    /// Returns the exponent of the given dimension.
    pub const fn get(&self, d: Dimension) -> i8 {
        self.dims[d as usize]
    }

    /// Returns `true` if all exponents are zero (dimensionless).
    pub fn is_dimensionless(&self) -> bool {
        self.dims.iter().all(|&e| e == 0)
    }

    /// Raises all exponents to the given integer power.
    pub const fn pow(&self, n: i32) -> Self {
        let mut result = [0i8; NDIM];
        let mut i = 0;
        while i < NDIM {
            result[i] = (self.dims[i] as i32 * n) as i8;
            i += 1;
        }
        Self { dims: result }
    }

    /// Takes an integer root, returning an error if any exponent is not
    /// evenly divisible by `n`.
    pub fn root(&self, n: i32) -> Result<Self, UnitError> {
        if n == 0 {
            return Err(UnitError::ZeroRoot);
        }
        let mut result = [0i8; NDIM];
        for (r, &d) in result.iter_mut().zip(self.dims.iter()) {
            let e = d as i32;
            if e % n != 0 {
                return Err(UnitError::IndivisibleRoot);
            }
            *r = (e / n) as i8;
        }
        Ok(Self { dims: result })
    }

    /// Takes the square root, returning an error if any exponent is odd.
    pub fn sqrt(&self) -> Result<Self, UnitError> {
        self.root(2)
    }

    /// Returns `true` if two `UnitDim` values have identical exponents,
    /// meaning the units are dimensionally conformant.
    pub fn conformant(&self, other: &Self) -> bool {
        self == other
    }

    /// Combines dimensions by adding exponents (multiplication).
    pub const fn combine(&self, other: &Self) -> Self {
        let mut result = [0i8; NDIM];
        let mut i = 0;
        while i < NDIM {
            result[i] = self.dims[i] + other.dims[i];
            i += 1;
        }
        Self { dims: result }
    }

    /// Combines dimensions by subtracting exponents (division).
    pub const fn combine_inv(&self, other: &Self) -> Self {
        let mut result = [0i8; NDIM];
        let mut i = 0;
        while i < NDIM {
            result[i] = self.dims[i] - other.dims[i];
            i += 1;
        }
        Self { dims: result }
    }
}

impl std::ops::Mul for UnitDim {
    type Output = Self;
    /// Multiplying dimensions adds their exponents.
    fn mul(self, rhs: Self) -> Self {
        self.combine(&rhs)
    }
}

impl std::ops::Div for UnitDim {
    type Output = Self;
    /// Dividing dimensions subtracts exponents.
    fn div(self, rhs: Self) -> Self {
        self.combine_inv(&rhs)
    }
}

impl std::fmt::Display for UnitDim {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const LABELS: [&str; NDIM] = ["m", "kg", "s", "A", "K", "cd", "mol", "rad", "sr", "_"];
        let mut first = true;
        for (i, &exp) in self.dims.iter().enumerate() {
            if exp != 0 {
                if !first {
                    write!(f, ".")?;
                }
                write!(f, "{}", LABELS[i])?;
                if exp != 1 {
                    write!(f, "{exp}")?;
                }
                first = false;
            }
        }
        if first {
            write!(f, "1")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodim_is_all_zeros() {
        assert!(UnitDim::NODIM.is_dimensionless());
        assert_eq!(UnitDim::NODIM, UnitDim::default());
    }

    #[test]
    fn basis_sets_single_dimension() {
        let length = UnitDim::basis(Dimension::Length);
        assert_eq!(length.get(Dimension::Length), 1);
        assert_eq!(length.get(Dimension::Mass), 0);
        assert_eq!(length.get(Dimension::Time), 0);
    }

    #[test]
    fn mul_adds_exponents() {
        let m = UnitDim::basis(Dimension::Length);
        let s_inv = UnitDim::basis(Dimension::Time).pow(-1);
        let velocity = m * s_inv;
        assert_eq!(velocity.get(Dimension::Length), 1);
        assert_eq!(velocity.get(Dimension::Time), -1);
    }

    #[test]
    fn div_subtracts_exponents() {
        let m = UnitDim::basis(Dimension::Length);
        let s = UnitDim::basis(Dimension::Time);
        let velocity = m / s;
        assert_eq!(velocity.get(Dimension::Length), 1);
        assert_eq!(velocity.get(Dimension::Time), -1);
    }

    #[test]
    fn pow_scales_exponents() {
        let m = UnitDim::basis(Dimension::Length);
        let m3 = m.pow(3);
        assert_eq!(m3.get(Dimension::Length), 3);
    }

    #[test]
    fn root_divides_exponents() {
        let m2 = UnitDim::basis(Dimension::Length).pow(2);
        let m = m2.root(2).unwrap();
        assert_eq!(m.get(Dimension::Length), 1);
    }

    #[test]
    fn root_indivisible_error() {
        let m3 = UnitDim::basis(Dimension::Length).pow(3);
        assert_eq!(m3.root(2), Err(UnitError::IndivisibleRoot));
    }

    #[test]
    fn root_zero_error() {
        assert_eq!(UnitDim::NODIM.root(0), Err(UnitError::ZeroRoot));
    }

    #[test]
    fn conformant_check() {
        let m = UnitDim::basis(Dimension::Length);
        let s = UnitDim::basis(Dimension::Time);
        assert!(m.conformant(&m));
        assert!(!m.conformant(&s));
    }

    #[test]
    fn display_velocity() {
        let v = UnitDim::new([1, 0, -1, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(v.to_string(), "m.s-1");
    }

    #[test]
    fn display_dimensionless() {
        assert_eq!(UnitDim::NODIM.to_string(), "1");
    }
}
