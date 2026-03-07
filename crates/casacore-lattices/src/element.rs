// SPDX-License-Identifier: LGPL-3.0-or-later
//! Marker trait for types that can be stored in a lattice.

use casacore_types::PrimitiveType;
use num_complex::{Complex32, Complex64};

/// Marker trait for types that can be stored as elements of a `Lattice`.
///
/// This corresponds to the template constraint on C++ `Lattice<T>`: only
/// the 12 casacore-native types (bool, u8, i16, u16, i32, u32, i64, f32,
/// f64, Complex, DComplex, String) are valid lattice elements.
///
/// Each implementing type provides its [`PrimitiveType`] tag and a default
/// (zero/empty) value for initializing lattice storage.
///
/// # Examples
///
/// ```rust
/// use casacore_lattices::LatticeElement;
/// use casacore_types::PrimitiveType;
///
/// assert_eq!(f64::PRIMITIVE_TYPE, PrimitiveType::Float64);
/// assert_eq!(f64::default_value(), 0.0);
/// ```
pub trait LatticeElement: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// The [`PrimitiveType`] tag for this element type.
    const PRIMITIVE_TYPE: PrimitiveType;

    /// Returns the default (zero/empty) value for this element type.
    ///
    /// Used when initializing lattice storage. Mirrors C++ casacore's
    /// default-initialization for `Array<T>`.
    fn default_value() -> Self;
}

impl LatticeElement for bool {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Bool;
    fn default_value() -> Self {
        false
    }
}

impl LatticeElement for u8 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::UInt8;
    fn default_value() -> Self {
        0
    }
}

impl LatticeElement for i16 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Int16;
    fn default_value() -> Self {
        0
    }
}

impl LatticeElement for u16 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::UInt16;
    fn default_value() -> Self {
        0
    }
}

impl LatticeElement for i32 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Int32;
    fn default_value() -> Self {
        0
    }
}

impl LatticeElement for u32 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::UInt32;
    fn default_value() -> Self {
        0
    }
}

impl LatticeElement for i64 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Int64;
    fn default_value() -> Self {
        0
    }
}

impl LatticeElement for f32 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Float32;
    fn default_value() -> Self {
        0.0
    }
}

impl LatticeElement for f64 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Float64;
    fn default_value() -> Self {
        0.0
    }
}

impl LatticeElement for Complex32 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Complex32;
    fn default_value() -> Self {
        Complex32::new(0.0, 0.0)
    }
}

impl LatticeElement for Complex64 {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::Complex64;
    fn default_value() -> Self {
        Complex64::new(0.0, 0.0)
    }
}

impl LatticeElement for String {
    const PRIMITIVE_TYPE: PrimitiveType = PrimitiveType::String;
    fn default_value() -> Self {
        String::new()
    }
}
