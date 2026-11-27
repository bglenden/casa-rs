// SPDX-License-Identifier: LGPL-3.0-or-later
//! Core coordinate trait and type enumeration.
//!
//! The [`Coordinate`] trait defines the interface that every concrete
//! coordinate type (linear, direction, spectral, Stokes, tabular, quality) must
//! implement. [`CoordinateType`] is a simple discriminant used by
//! [`CoordinateSystem`](crate::CoordinateSystem) to locate coordinates
//! by kind without downcasting.

use std::fmt;

use casa_core::RecordValue;

use crate::error::CoordinateError;
use crate::{
    CoordinateSystem, DirectionCoordinate, LinearCoordinate, QualityCoordinate, SpectralCoordinate,
    StokesCoordinate, TabularCoordinate,
};

/// Discriminant for the coordinate kinds supported by casacore.
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
    /// A 1-axis data/error quality coordinate.
    Quality,
    /// A nested coordinate system.
    System,
}

impl CoordinateType {
    /// Prefix of this coordinate's numbered field in a native casacore record.
    pub fn record_prefix(self) -> &'static str {
        match self {
            Self::Linear => "linear",
            Self::Direction => "direction",
            Self::Spectral => "spectral",
            Self::Stokes => "stokes",
            Self::Tabular => "tabular",
            Self::Quality => "quality",
            Self::System => "coordsys",
        }
    }
}

impl fmt::Display for CoordinateType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Linear => f.write_str("Linear"),
            Self::Direction => f.write_str("Direction"),
            Self::Spectral => f.write_str("Spectral"),
            Self::Stokes => f.write_str("Stokes"),
            Self::Tabular => f.write_str("Tabular"),
            Self::Quality => f.write_str("Quality"),
            Self::System => f.write_str("System"),
        }
    }
}

/// Closed model for every coordinate kind supported by casacore images.
#[derive(Debug, Clone)]
pub enum CoordinateModel {
    /// A linear mapping between pixels and world axes.
    Linear(LinearCoordinate),
    /// Two celestial axes with a spherical projection and reference frame.
    Direction(DirectionCoordinate),
    /// A frequency or velocity axis with its spectral reference frame.
    Spectral(Box<SpectralCoordinate>),
    /// An axis of discrete Stokes or correlation products.
    Stokes(StokesCoordinate),
    /// World-coordinate samples indexed and interpolated by pixel.
    Tabular(TabularCoordinate),
    /// A discrete axis describing the role of each plane, such as data or error.
    Quality(QualityCoordinate),
    /// An ordered composition of coordinate models.
    System(Box<CoordinateSystem>),
}

macro_rules! dispatch_coordinate {
    ($coordinate:expr, $method:ident $(, $argument:expr)*) => {
        match $coordinate {
            CoordinateModel::Linear(value) => value.$method($($argument),*),
            CoordinateModel::Direction(value) => value.$method($($argument),*),
            CoordinateModel::Spectral(value) => value.$method($($argument),*),
            CoordinateModel::Stokes(value) => value.$method($($argument),*),
            CoordinateModel::Tabular(value) => value.$method($($argument),*),
            CoordinateModel::Quality(value) => value.$method($($argument),*),
            CoordinateModel::System(value) => value.$method($($argument),*),
        }
    };
}

impl CoordinateModel {
    /// Return the kind of coordinate model held by this value.
    pub fn coordinate_type(&self) -> CoordinateType {
        dispatch_coordinate!(self, coordinate_type)
    }

    /// Number of pixel axes consumed by the model.
    pub fn n_pixel_axes(&self) -> usize {
        dispatch_coordinate!(self, n_pixel_axes)
    }

    /// Number of world axes produced by the model.
    pub fn n_world_axes(&self) -> usize {
        dispatch_coordinate!(self, n_world_axes)
    }

    /// Convert zero-based pixel coordinates to world values in the model's axis units.
    ///
    /// Returns an error for the wrong dimensionality or an invalid conversion.
    pub fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        dispatch_coordinate!(self, to_world, pixel)
    }

    /// Convert world values in the model's axis units to zero-based pixel coordinates.
    ///
    /// Returns an error for incompatible values or an invalid inverse conversion.
    pub fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        dispatch_coordinate!(self, to_pixel, world)
    }

    /// World values at the model reference point, in world-axis order.
    pub fn reference_value(&self) -> Vec<f64> {
        dispatch_coordinate!(self, reference_value)
    }

    /// Zero-based pixel coordinates of the model reference point.
    pub fn reference_pixel(&self) -> Vec<f64> {
        dispatch_coordinate!(self, reference_pixel)
    }

    /// World-axis increments per pixel in the units returned by `axis_units`.
    pub fn increment(&self) -> Vec<f64> {
        dispatch_coordinate!(self, increment)
    }

    /// Return the names of the world axes in output order.
    pub fn axis_names(&self) -> Vec<String> {
        dispatch_coordinate!(self, axis_names)
    }

    /// Return the unit strings for the world axes in output order.
    pub fn axis_units(&self) -> Vec<String> {
        dispatch_coordinate!(self, axis_units)
    }

    /// Units recorded for each world axis, before converting direction angles to radians.
    pub fn declared_axis_units(&self) -> Vec<String> {
        match self {
            Self::Direction(direction) => direction.visible_world_units(),
            Self::System(system) => system.world_axis_values(Self::declared_axis_units),
            _ => self.axis_units(),
        }
    }

    /// Concrete coordinate and local axis for a world axis of this model.
    pub fn world_axis_leaf(&self, axis: usize) -> Option<(&Self, usize)> {
        if let Self::System(system) = self {
            let (index, local) = system.world_axis_owner(axis)?;
            system.coordinate(index).world_axis_leaf(local)
        } else {
            (axis < self.n_world_axes()).then_some((self, axis))
        }
    }

    /// Reference values and increments expressed in the units in the coordinate record.
    pub fn reference_and_increment_in_declared_units(&self) -> (Vec<f64>, Vec<f64>) {
        match self {
            Self::Direction(direction) => direction.reference_and_increment_in_world_units(),
            Self::System(system) => system
                .world_axis_values(|model| {
                    let (reference, increment) = model.reference_and_increment_in_declared_units();
                    reference.into_iter().zip(increment).collect()
                })
                .into_iter()
                .unzip(),
            _ => (self.reference_value(), self.increment()),
        }
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
coordinate_model_from!(QualityCoordinate, Quality);

impl From<SpectralCoordinate> for CoordinateModel {
    fn from(value: SpectralCoordinate) -> Self {
        Self::Spectral(Box::new(value))
    }
}

impl From<CoordinateSystem> for CoordinateModel {
    fn from(value: CoordinateSystem) -> Self {
        Self::System(Box::new(value))
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
impl sealed::Sealed for QualityCoordinate {}
impl sealed::Sealed for CoordinateSystem {}

/// The interface shared by all coordinate types.
///
/// Every concrete coordinate (e.g. [`LinearCoordinate`](crate::LinearCoordinate),
/// [`DirectionCoordinate`](crate::DirectionCoordinate)) implements this trait.
/// [`CoordinateSystem`](crate::CoordinateSystem) stores the closed
/// [`CoordinateModel`] enum; this trait only shares behavior among the
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

impl Coordinate for CoordinateSystem {
    fn coordinate_type(&self) -> CoordinateType {
        CoordinateType::System
    }

    fn n_pixel_axes(&self) -> usize {
        self.n_pixel_axes()
    }

    fn n_world_axes(&self) -> usize {
        self.n_world_axes()
    }

    fn to_world(&self, pixel: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        self.to_world(pixel)
    }

    fn to_pixel(&self, world: &[f64]) -> Result<Vec<f64>, CoordinateError> {
        self.to_pixel(world)
    }

    fn reference_value(&self) -> Vec<f64> {
        self.reference_value()
    }

    fn reference_pixel(&self) -> Vec<f64> {
        self.reference_pixel()
    }

    fn increment(&self) -> Vec<f64> {
        self.world_axis_values(CoordinateModel::increment)
    }

    fn axis_names(&self) -> Vec<String> {
        self.world_axis_values(CoordinateModel::axis_names)
    }

    fn axis_units(&self) -> Vec<String> {
        self.world_axis_units()
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
