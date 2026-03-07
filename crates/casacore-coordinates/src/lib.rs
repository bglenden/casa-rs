// SPDX-License-Identifier: LGPL-3.0-or-later
//! Coordinate systems for astronomical images.
//!
//! This crate implements the coordinate system machinery used to map between
//! pixel positions and world (physical) coordinates in multi-dimensional
//! astronomical image cubes. It is the Rust equivalent of the C++ casacore
//! `Coordinates` module.
//!
//! # Architecture
//!
//! A [`CoordinateSystem`] is a container holding one or more [`Coordinate`]
//! objects, each of which owns a contiguous set of pixel and world axes:
//!
//! - [`DirectionCoordinate`] — 2-axis celestial (RA/Dec or lon/lat) with
//!   spherical projection.
//! - [`SpectralCoordinate`] — 1-axis frequency or velocity.
//! - [`StokesCoordinate`] — 1-axis Stokes parameter (I, Q, U, V, etc.).
//! - [`LinearCoordinate`] — N-axis general linear mapping.
//! - [`TabularCoordinate`] — 1-axis with a lookup table and interpolation.
//!
//! The [`Projection`] type provides the spherical map projections (SIN, TAN,
//! ARC, CAR, SFL, etc.) used by [`DirectionCoordinate`].
//!
//! [`ObsInfo`] carries observation metadata (telescope, observer, date, position).
//!
//! # Examples
//!
//! ```rust
//! use casacore_coordinates::{
//!     CoordinateSystem, DirectionCoordinate, SpectralCoordinate,
//!     StokesCoordinate, StokesType, Projection, ProjectionType,
//! };
//! use casacore_types::measures::direction::DirectionRef;
//! use casacore_types::measures::frequency::FrequencyRef;
//!
//! let mut cs = CoordinateSystem::new();
//!
//! // Direction coordinate: SIN projection centered at RA=0, Dec=+45deg
//! let proj = Projection::new(ProjectionType::SIN);
//! let dir = DirectionCoordinate::new(
//!     DirectionRef::J2000,
//!     proj,
//!     [0.0, std::f64::consts::FRAC_PI_4],
//!     [-1e-4, 1e-4],
//!     [512.0, 512.0],
//! );
//! cs.add_coordinate(Box::new(dir));
//!
//! // Spectral coordinate
//! let spec = SpectralCoordinate::new(
//!     FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42040575e9,
//! );
//! cs.add_coordinate(Box::new(spec));
//!
//! // Stokes coordinate
//! let stokes = StokesCoordinate::new(vec![
//!     StokesType::I, StokesType::Q, StokesType::U, StokesType::V,
//! ]);
//! cs.add_coordinate(Box::new(stokes));
//!
//! assert_eq!(cs.n_pixel_axes(), 4); // 2 (direction) + 1 (spectral) + 1 (stokes)
//! ```

pub mod coordinate;
pub mod coordinate_system;
pub mod direction;
pub mod error;
pub mod fits;
pub mod linear;
pub mod obs_info;
pub mod projection;
pub mod spectral;
pub mod stokes;
pub mod tabular;

// Re-export public types at crate root for convenience.
pub use coordinate::{Coordinate, CoordinateType};
pub use coordinate_system::CoordinateSystem;
pub use direction::DirectionCoordinate;
pub use error::CoordinateError;
pub use fits::{FitsHeader, FitsKeyword, FitsValue};
pub use linear::LinearCoordinate;
pub use obs_info::ObsInfo;
pub use projection::{Projection, ProjectionType};
pub use spectral::SpectralCoordinate;
pub use stokes::{StokesCoordinate, StokesType};
pub use tabular::TabularCoordinate;
