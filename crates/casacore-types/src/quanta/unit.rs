// SPDX-License-Identifier: LGPL-3.0-or-later
//! The validated unit type.
//!
//! [`Unit`] wraps a unit name string with its pre-parsed [`UnitVal`],
//! providing a zero-cost way to carry unit metadata through computations.
//! It is the Rust counterpart of C++ `casa::Unit`.

use std::fmt;
use std::str::FromStr;

use crate::quanta::error::UnitError;
use crate::quanta::parser::parse_unit;
use crate::quanta::unit_val::UnitVal;

/// A validated unit: a name string paired with its resolved [`UnitVal`].
///
/// Parsing is performed once at construction time. The resolved value
/// (scale factor and dimensions) is cached for efficient arithmetic
/// and conversion.
///
/// Corresponds to C++ `casa::Unit`.
///
/// # Examples
///
/// ```
/// use casacore_types::quanta::Unit;
///
/// let km = Unit::new("km").unwrap();
/// assert_eq!(km.name(), "km");
/// assert!((km.val().factor - 1e3).abs() < 1e-10);
/// ```
#[derive(Debug, Clone)]
pub struct Unit {
    name: String,
    val: UnitVal,
}

impl Unit {
    /// Creates a new `Unit` by parsing the given unit string.
    ///
    /// Returns `Err(UnitError)` if the string is not a valid unit expression.
    pub fn new(s: &str) -> Result<Self, UnitError> {
        let val = parse_unit(s)?;
        Ok(Self {
            name: s.to_owned(),
            val,
        })
    }

    /// Returns the unit name as originally provided.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the resolved unit value (factor + dimensions).
    pub fn val(&self) -> &UnitVal {
        &self.val
    }

    /// Returns `true` if this unit is dimensionally conformant with `other`.
    pub fn conformant(&self, other: &Unit) -> bool {
        self.val.conformant(&other.val)
    }

    /// Creates a dimensionless unit (empty name, factor 1).
    pub fn dimensionless() -> Self {
        Self {
            name: String::new(),
            val: UnitVal::NODIM,
        }
    }
}

impl FromStr for Unit {
    type Err = UnitError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl fmt::Display for Unit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PartialEq for Unit {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for Unit {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quanta::dim::Dimension;

    #[test]
    fn unit_new_base() {
        let m = Unit::new("m").unwrap();
        assert_eq!(m.name(), "m");
        assert_eq!(m.val().factor, 1.0);
        assert_eq!(m.val().dim.get(Dimension::Length), 1);
    }

    #[test]
    fn unit_from_str() {
        let u: Unit = "km/s".parse().unwrap();
        assert_eq!(u.name(), "km/s");
    }

    #[test]
    fn unit_display() {
        let u = Unit::new("Jy/beam").unwrap();
        assert_eq!(format!("{u}"), "Jy/beam");
    }

    #[test]
    fn unit_conformant() {
        let m = Unit::new("m").unwrap();
        let km = Unit::new("km").unwrap();
        let s = Unit::new("s").unwrap();
        assert!(m.conformant(&km));
        assert!(!m.conformant(&s));
    }

    #[test]
    fn unit_invalid() {
        assert!(Unit::new("xyzzy").is_err());
    }
}
