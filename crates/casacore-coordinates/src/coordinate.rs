// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core coordinate trait and type enumeration.
//!
//! The [`Coordinate`] trait defines the interface that every concrete
//! coordinate type (linear, direction, spectral, Stokes, tabular) must
//! implement. [`CoordinateType`] is a simple discriminant used by
//! [`CoordinateSystem`](crate::CoordinateSystem) to locate coordinates
//! by kind without downcasting.

use std::fmt;

use casacore_types::RecordValue;

use crate::error::CoordinateError;

/// Discriminant for the five coordinate kinds supported by casacore.
///
/// This enum corresponds to C++ `Coordinate::Type` and is used by
/// [`CoordinateSystem::find_coordinate`](crate::CoordinateSystem::find_coordinate)
/// to locate a coordinate by kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CoordinateType {
    /// An N-axis linear coordinate.
    Linear,
    /// A 2-axis celestial (RA/Dec or lon/lat) coordinate.
    Direction,
    /// A 1-axis spectral (frequency/velocity) coordinate.
    Spectral,
    /// A 1-axis Stokes-parameter coordinate.
    Stokes,
    /// A 1-axis coordinate defined by a lookup table.
    Tabular,
}

impl fmt::Display for CoordinateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Linear => f.write_str("Linear"),
            Self::Direction => f.write_str("Direction"),
            Self::Spectral => f.write_str("Spectral"),
            Self::Stokes => f.write_str("Stokes"),
            Self::Tabular => f.write_str("Tabular"),
        }
    }
}

/// The interface shared by all coordinate types.
///
/// Every concrete coordinate (e.g. [`LinearCoordinate`](crate::LinearCoordinate),
/// [`DirectionCoordinate`](crate::DirectionCoordinate)) implements this trait.
/// The trait is object-safe (via `clone_box`) so that coordinates can be stored
/// as `Box<dyn Coordinate>` inside a [`CoordinateSystem`](crate::CoordinateSystem).
///
/// The pixel-to-world and world-to-pixel methods follow the conventions of
/// C++ `Coordinate::toWorld` and `Coordinate::toPixel`.
pub trait Coordinate: fmt::Debug + Send + Sync {
    /// Returns the kind of this coordinate.
    fn coordinate_type(&self) -> CoordinateType;

    /// Returns the number of pixel axes this coordinate spans.
    fn n_pixel_axes(&self) -> usize;

    /// Returns the number of world axes this coordinate spans.
    fn n_world_axes(&self) -> usize;

    /// Converts pixel coordinates to world coordinates.
    ///
    /// `pixel` must have length [`n_pixel_axes`](Coordinate::n_pixel_axes).
    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError>;

    /// Converts world coordinates to pixel coordinates.
    ///
    /// `world` must have length [`n_world_axes`](Coordinate::n_world_axes).
    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError>;

    /// Returns the reference world value for each world axis.
    fn reference_value(&self) -> Vec<f64>;

    /// Returns the reference pixel for each pixel axis.
    fn reference_pixel(&self) -> Vec<f64>;

    /// Returns the increment (world units per pixel) for each world axis.
    fn increment(&self) -> Vec<f64>;

    /// Returns the axis names (one per world axis).
    fn axis_names(&self) -> Vec<String>;

    /// Returns the axis unit strings (one per world axis).
    fn axis_units(&self) -> Vec<String>;

    /// Serializes this coordinate to a casacore-compatible record.
    fn to_record(&self) -> RecordValue;

    /// Returns a boxed clone of this coordinate.
    ///
    /// This method exists because `Clone` is not object-safe.
    fn clone_box(&self) -> Box<dyn Coordinate>;
}

impl Clone for Box<dyn Coordinate> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coordinate_type_display() {
        assert_eq!(CoordinateType::Linear.to_string(), "Linear");
        assert_eq!(CoordinateType::Direction.to_string(), "Direction");
        assert_eq!(CoordinateType::Spectral.to_string(), "Spectral");
        assert_eq!(CoordinateType::Stokes.to_string(), "Stokes");
        assert_eq!(CoordinateType::Tabular.to_string(), "Tabular");
    }

    #[test]
    fn coordinate_type_equality() {
        assert_eq!(CoordinateType::Linear, CoordinateType::Linear);
        assert_ne!(CoordinateType::Linear, CoordinateType::Direction);
    }
}
