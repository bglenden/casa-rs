// SPDX-License-Identifier: LGPL-3.0-or-later
//! Quantity: a value with a unit.
//!
//! [`Quantity`] pairs an `f64` value with a [`Unit`], providing arithmetic
//! and conversion operations.  It is the Rust counterpart of C++
//! `casa::Quantum<Double>` (a.k.a. `Quantity`).

use std::fmt;
use std::ops;
use std::str::FromStr;

use crate::quanta::error::UnitError;
use crate::quanta::unit::Unit;

/// A value with an associated unit.
///
/// `Quantity` is `Quantum<f64>` — the most common specialisation in casacore.
/// Arithmetic operations follow dimensional analysis rules:
///
/// - **Add/Sub** require conformant units and auto-convert to the left operand's unit.
/// - **Mul/Div** combine dimensions freely.
/// - Scalar `f64` multiplication/division preserves the unit.
///
/// # Examples
///
/// ```
/// use casacore_types::quanta::Quantity;
///
/// let d = Quantity::new(1.5, "km").unwrap();
/// let t = Quantity::new(3.0, "s").unwrap();
/// let v = &d / &t;
/// assert_eq!(v.unit().name(), "km/s");
/// ```
#[derive(Debug, Clone)]
pub struct Quantity {
    value: f64,
    unit: Unit,
}

impl Quantity {
    /// Creates a new `Quantity` from a value and a unit string.
    pub fn new(value: f64, unit: &str) -> Result<Self, UnitError> {
        Ok(Self {
            value,
            unit: Unit::new(unit)?,
        })
    }

    /// Creates a `Quantity` from a value and a pre-parsed [`Unit`].
    pub fn with_unit(value: f64, unit: Unit) -> Self {
        Self { value, unit }
    }

    /// Creates a dimensionless quantity.
    pub fn dimensionless(value: f64) -> Self {
        Self {
            value,
            unit: Unit::dimensionless(),
        }
    }

    /// Returns the numeric value in the current unit.
    pub fn value(&self) -> f64 {
        self.value
    }

    /// Returns a reference to the unit.
    pub fn unit(&self) -> &Unit {
        &self.unit
    }

    /// Converts this quantity to the target unit, returning a new `Quantity`.
    ///
    /// Returns `Err(UnitError::NonConformant)` if the units are not
    /// dimensionally compatible.
    pub fn convert(&self, target: &Unit) -> Result<Quantity, UnitError> {
        let v = self.get_value_in(target)?;
        Ok(Quantity {
            value: v,
            unit: target.clone(),
        })
    }

    /// Returns the numeric value expressed in the given target unit.
    ///
    /// Returns `Err(UnitError::NonConformant)` if the units are not
    /// dimensionally compatible.
    pub fn get_value_in(&self, target: &Unit) -> Result<f64, UnitError> {
        let src = self.unit.val();
        let dst = target.val();
        if !src.conformant(dst) {
            return Err(UnitError::NonConformant {
                lhs: self.unit.name().to_owned(),
                rhs: target.name().to_owned(),
            });
        }
        Ok(self.value * src.factor / dst.factor)
    }

    /// Returns the value in SI base units.
    pub fn get_si_value(&self) -> f64 {
        self.value * self.unit.val().factor
    }
}

impl fmt::Display for Quantity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.unit.name().is_empty() {
            write!(f, "{}", self.value)
        } else {
            write!(f, "{} {}", self.value, self.unit)
        }
    }
}

impl FromStr for Quantity {
    type Err = UnitError;
    /// Parses a string like `"2.73 km/s"` into a `Quantity`.
    ///
    /// The value and unit are separated by whitespace. If no unit is present,
    /// the quantity is dimensionless.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        // Find the split between numeric value and unit.
        let split = s
            .find(|c: char| {
                !c.is_ascii_digit() && c != '.' && c != '-' && c != '+' && c != 'e' && c != 'E'
            })
            .unwrap_or(s.len());

        let num_str = s[..split].trim();
        let unit_str = s[split..].trim();

        let value: f64 = num_str.parse().map_err(|_| UnitError::InvalidUnit {
            input: s.to_owned(),
        })?;

        if unit_str.is_empty() {
            Ok(Self::dimensionless(value))
        } else {
            Self::new(value, unit_str)
        }
    }
}

// ── Arithmetic with another Quantity ──

impl ops::Add for &Quantity {
    type Output = Quantity;
    /// Adds two quantities, converting the RHS to the LHS unit.
    ///
    /// # Panics
    ///
    /// Panics if the units are not conformant.
    fn add(self, rhs: &Quantity) -> Quantity {
        let rhs_val = rhs
            .get_value_in(&self.unit)
            .expect("non-conformant units in Quantity addition");
        Quantity {
            value: self.value + rhs_val,
            unit: self.unit.clone(),
        }
    }
}

impl ops::Sub for &Quantity {
    type Output = Quantity;
    /// Subtracts two quantities, converting the RHS to the LHS unit.
    ///
    /// # Panics
    ///
    /// Panics if the units are not conformant.
    fn sub(self, rhs: &Quantity) -> Quantity {
        let rhs_val = rhs
            .get_value_in(&self.unit)
            .expect("non-conformant units in Quantity subtraction");
        Quantity {
            value: self.value - rhs_val,
            unit: self.unit.clone(),
        }
    }
}

impl ops::Mul for &Quantity {
    type Output = Quantity;
    /// Multiplies two quantities, combining their units.
    fn mul(self, rhs: &Quantity) -> Quantity {
        let name = if rhs.unit.name().is_empty() {
            self.unit.name().to_owned()
        } else if self.unit.name().is_empty() {
            rhs.unit.name().to_owned()
        } else {
            format!("{}.{}", self.unit.name(), rhs.unit.name())
        };
        Quantity {
            value: self.value * rhs.value,
            unit: Unit::new(&name).unwrap_or_else(|_| {
                // If the combined name can't be parsed back, create from val directly
                Unit::dimensionless()
            }),
        }
    }
}

impl ops::Div for &Quantity {
    type Output = Quantity;
    /// Divides two quantities, combining their units.
    fn div(self, rhs: &Quantity) -> Quantity {
        let name = if rhs.unit.name().is_empty() {
            self.unit.name().to_owned()
        } else if self.unit.name().is_empty() {
            format!("({})-1", rhs.unit.name())
        } else {
            format!("{}/{}", self.unit.name(), rhs.unit.name())
        };
        Quantity {
            value: self.value / rhs.value,
            unit: Unit::new(&name).unwrap_or_else(|_| Unit::dimensionless()),
        }
    }
}

// ── Scalar arithmetic ──

impl ops::Mul<f64> for &Quantity {
    type Output = Quantity;
    fn mul(self, rhs: f64) -> Quantity {
        Quantity {
            value: self.value * rhs,
            unit: self.unit.clone(),
        }
    }
}

impl ops::Mul<&Quantity> for f64 {
    type Output = Quantity;
    fn mul(self, rhs: &Quantity) -> Quantity {
        rhs * self
    }
}

impl ops::Div<f64> for &Quantity {
    type Output = Quantity;
    fn div(self, rhs: f64) -> Quantity {
        Quantity {
            value: self.value / rhs,
            unit: self.unit.clone(),
        }
    }
}

impl ops::Neg for &Quantity {
    type Output = Quantity;
    fn neg(self) -> Quantity {
        Quantity {
            value: -self.value,
            unit: self.unit.clone(),
        }
    }
}

// ── Comparison (auto-converting) ──

impl PartialEq for Quantity {
    fn eq(&self, other: &Self) -> bool {
        if let Ok(other_val) = other.get_value_in(&self.unit) {
            (self.value - other_val).abs()
                < f64::EPSILON * self.value.abs().max(other_val.abs()).max(1.0)
        } else {
            false
        }
    }
}

impl PartialOrd for Quantity {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        other
            .get_value_in(&self.unit)
            .ok()
            .map(|other_val| self.value.total_cmp(&other_val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64, tol: f64) -> bool {
        let diff = (a - b).abs();
        let scale = a.abs().max(b.abs()).max(1e-30);
        diff / scale < tol
    }

    #[test]
    fn quantity_new() {
        let q = Quantity::new(2.73, "km").unwrap();
        assert_eq!(q.value(), 2.73);
        assert_eq!(q.unit().name(), "km");
    }

    #[test]
    fn quantity_convert() {
        let km = Quantity::new(1.0, "km").unwrap();
        let m_unit = Unit::new("m").unwrap();
        let m = km.convert(&m_unit).unwrap();
        assert!(close(m.value(), 1000.0, 1e-12));
    }

    #[test]
    fn quantity_convert_nonconformant() {
        let km = Quantity::new(1.0, "km").unwrap();
        let s_unit = Unit::new("s").unwrap();
        assert!(km.convert(&s_unit).is_err());
    }

    #[test]
    fn quantity_get_value_in() {
        let q = Quantity::new(2.5, "km").unwrap();
        let m_unit = Unit::new("m").unwrap();
        let val = q.get_value_in(&m_unit).unwrap();
        assert!(close(val, 2500.0, 1e-12));
    }

    #[test]
    fn quantity_add() {
        let a = Quantity::new(1.0, "km").unwrap();
        let b = Quantity::new(500.0, "m").unwrap();
        let c = &a + &b;
        assert!(close(c.value(), 1.5, 1e-12));
        assert_eq!(c.unit().name(), "km");
    }

    #[test]
    fn quantity_sub() {
        let a = Quantity::new(1.0, "km").unwrap();
        let b = Quantity::new(500.0, "m").unwrap();
        let c = &a - &b;
        assert!(close(c.value(), 0.5, 1e-12));
    }

    #[test]
    fn quantity_mul_scalar() {
        let a = Quantity::new(3.0, "m").unwrap();
        let b = &a * 2.0;
        assert!(close(b.value(), 6.0, 1e-12));
        assert_eq!(b.unit().name(), "m");
    }

    #[test]
    fn quantity_div_quantities() {
        let d = Quantity::new(1.5, "km").unwrap();
        let t = Quantity::new(3.0, "s").unwrap();
        let v = &d / &t;
        assert!(close(v.value(), 0.5, 1e-12));
        assert_eq!(v.unit().name(), "km/s");
    }

    #[test]
    fn quantity_from_str() {
        let q: Quantity = "2.73 km/s".parse().unwrap();
        assert!(close(q.value(), 2.73, 1e-12));
        assert_eq!(q.unit().name(), "km/s");
    }

    #[test]
    fn quantity_from_str_no_unit() {
        let q: Quantity = "42".parse().unwrap();
        assert!(close(q.value(), 42.0, 1e-12));
        assert!(q.unit().name().is_empty());
    }

    #[test]
    fn quantity_display() {
        let q = Quantity::new(2.73, "km/s").unwrap();
        assert_eq!(format!("{q}"), "2.73 km/s");
    }

    #[test]
    fn quantity_partial_ord() {
        let a = Quantity::new(1.0, "km").unwrap();
        let b = Quantity::new(500.0, "m").unwrap();
        assert!(a > b);
    }

    #[test]
    fn quantity_si_value() {
        let q = Quantity::new(3.0, "km").unwrap();
        assert!(close(q.get_si_value(), 3000.0, 1e-12));
    }
}
