// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core coordinate trait and type enumeration.
//!
//! The [`Coordinate`] trait defines the interface that every concrete
//! coordinate type (linear, direction, spectral, Stokes, tabular) must
//! implement. [`CoordinateType`] is a simple discriminant used by
//! [`CoordinateSystem`](crate::CoordinateSystem) to locate coordinates
//! by kind without downcasting.

use std::fmt;

use casa_types::RecordValue;

use crate::error::CoordinateError;
use crate::{
    DirectionCoordinate, LinearCoordinate, SpectralCoordinate, StokesCoordinate, TabularCoordinate,
};

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

/// Closed model for every coordinate kind supported by casacore images.
#[derive(Debug, Clone)]
pub enum CoordinateModel {
    Linear(LinearCoordinate),
    Direction(DirectionCoordinate),
    Spectral(Box<SpectralCoordinate>),
    Stokes(StokesCoordinate),
    Tabular(TabularCoordinate),
}

macro_rules! dispatch_coordinate {
    ($coordinate:expr, $method:ident $(, $argument:expr)*) => {
        match $coordinate {
            CoordinateModel::Linear(value) => value.$method($($argument),*),
            CoordinateModel::Direction(value) => value.$method($($argument),*),
            CoordinateModel::Spectral(value) => value.$method($($argument),*),
            CoordinateModel::Stokes(value) => value.$method($($argument),*),
            CoordinateModel::Tabular(value) => value.$method($($argument),*),
        }
    };
}

impl CoordinateModel {
    pub fn coordinate_type(&self) -> CoordinateType {
        dispatch_coordinate!(self, coordinate_type)
    }

    pub fn n_pixel_axes(&self) -> usize {
        dispatch_coordinate!(self, n_pixel_axes)
    }

    pub fn n_world_axes(&self) -> usize {
        dispatch_coordinate!(self, n_world_axes)
    }

    pub fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        dispatch_coordinate!(self, to_world, pixel)
    }

    pub fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        dispatch_coordinate!(self, to_pixel, world)
    }

    pub fn reference_value(&self) -> Vec<f64> {
        dispatch_coordinate!(self, reference_value)
    }

    pub fn reference_pixel(&self) -> Vec<f64> {
        dispatch_coordinate!(self, reference_pixel)
    }

    pub fn increment(&self) -> Vec<f64> {
        dispatch_coordinate!(self, increment)
    }

    pub fn axis_names(&self) -> Vec<String> {
        dispatch_coordinate!(self, axis_names)
    }

    pub fn axis_units(&self) -> Vec<String> {
        dispatch_coordinate!(self, axis_units)
    }

    /// Serializes this coordinate in the canonical casacore field layout.
    pub fn to_record(&self) -> RecordValue {
        dispatch_coordinate!(self, to_record)
    }
}

macro_rules! coordinate_model_from {
    ($type:ty, $variant:ident) => {
        impl From<$type> for CoordinateModel {
            fn from(value: $type) -> Self {
                Self::$variant(value)
            }
        }
    };
}

coordinate_model_from!(LinearCoordinate, Linear);
coordinate_model_from!(DirectionCoordinate, Direction);
coordinate_model_from!(StokesCoordinate, Stokes);
coordinate_model_from!(TabularCoordinate, Tabular);

impl From<SpectralCoordinate> for CoordinateModel {
    fn from(value: SpectralCoordinate) -> Self {
        Self::Spectral(Box::new(value))
    }
}

mod sealed {
    pub trait Sealed {}
}

impl sealed::Sealed for LinearCoordinate {}
impl sealed::Sealed for DirectionCoordinate {}
impl sealed::Sealed for SpectralCoordinate {}
impl sealed::Sealed for StokesCoordinate {}
impl sealed::Sealed for TabularCoordinate {}

/// The interface shared by all coordinate types.
///
/// Every concrete coordinate (e.g. [`LinearCoordinate`](crate::LinearCoordinate),
/// [`DirectionCoordinate`](crate::DirectionCoordinate)) implements this trait.
/// [`CoordinateSystem`](crate::CoordinateSystem) stores the closed
/// [`CoordinateModel`] enum; this trait only shares behavior among the five
/// concrete coordinate types.
///
/// The pixel-to-world and world-to-pixel methods follow the conventions of
/// C++ `Coordinate::toWorld` and `Coordinate::toPixel`.
pub trait Coordinate: sealed::Sealed + fmt::Debug + Send + Sync {
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
