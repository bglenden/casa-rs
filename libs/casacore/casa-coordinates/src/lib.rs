// SPDX-License-Identifier: LGPL-3.0-or-later
#![doc = include_str!("../README.md")]
//!
//! <details>
//! <summary>API details</summary>
//!
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
//! use casa_coordinates::{
//!     CoordinateSystem, DirectionCoordinate, SpectralCoordinate,
//!     StokesCoordinate, StokesType, Projection, ProjectionType,
//! };
//! use casa_core::measures::direction::DirectionRef;
//! use casa_core::measures::frequency::FrequencyRef;
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
//! cs.add_coordinate(dir);
//!
//! // Spectral coordinate
//! let spec = SpectralCoordinate::new(
//!     FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42040575e9,
//! );
//! cs.add_coordinate(spec);
//!
//! // Stokes coordinate
//! let stokes = StokesCoordinate::new(vec![
//!     StokesType::I, StokesType::Q, StokesType::U, StokesType::V,
//! ]);
//! cs.add_coordinate(stokes);
//!
//! assert_eq!(cs.n_pixel_axes(), 4); // 2 (direction) + 1 (spectral) + 1 (stokes)
//! ```
//!
//! </details>
#![warn(missing_docs)]

mod coordinate;
mod coordinate_system;
pub mod defaults;
mod direction;
pub mod display;
mod error;
pub mod fits;
mod linear;
mod obs_info;
pub mod projection;
mod quality;
mod record_utils;
mod spectral;
mod stokes;
mod tabular;

pub use coordinate::{Coordinate, CoordinateModel, CoordinateType};
pub use coordinate_system::{CoordinateSystem, RecordCoordinate};
pub use direction::DirectionCoordinate;
pub use display::format_world_display;
pub use error::CoordinateError;
pub use fits::{FitsHeader, FitsKeyword, FitsValue};
pub use linear::LinearCoordinate;
pub use obs_info::ObsInfo;
pub use projection::{Projection, ProjectionType};
pub use quality::{QualityCoordinate, QualityType};
pub use spectral::{SpectralCoordinate, SpectralNativeType};
pub use stokes::{StokesCoordinate, StokesType};
pub use tabular::{TabularCoordinate, TabularEdit};
