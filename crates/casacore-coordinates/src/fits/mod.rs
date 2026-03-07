// SPDX-License-Identifier: LGPL-3.0-or-later
//! FITS/WCS coordinate interoperability.
//!
//! This module provides conversion between FITS World Coordinate System (WCS)
//! headers and the coordinate types defined in this crate. It can parse
//! standard FITS WCS keywords (CTYPEn, CRVALn, CRPIXn, CDELTn, PCi_j, etc.)
//! into a [`CoordinateSystem`](crate::CoordinateSystem), and emit those
//! keywords from an existing coordinate system.
//!
//! The implementation follows:
//! - Greisen & Calabretta (2002), "Representations of world coordinates in FITS"
//! - Calabretta & Greisen (2002), "Representations of celestial coordinates in FITS"
//! - Greisen et al. (2006), "Representations of spectral coordinates in FITS"
//!
//! # Examples
//!
//! ```rust
//! use casacore_coordinates::fits::{FitsHeader, to_fits_header, from_fits_header};
//! use casacore_coordinates::{
//!     CoordinateSystem, DirectionCoordinate, SpectralCoordinate,
//!     StokesCoordinate, StokesType, Projection, ProjectionType,
//! };
//! use casacore_types::measures::direction::DirectionRef;
//! use casacore_types::measures::frequency::FrequencyRef;
//!
//! // Build a coordinate system
//! let mut cs = CoordinateSystem::new();
//! let proj = Projection::new(ProjectionType::SIN);
//! let dir = DirectionCoordinate::new(
//!     DirectionRef::J2000, proj,
//!     [0.0, std::f64::consts::FRAC_PI_4],
//!     [-1e-4, 1e-4], [512.0, 512.0],
//! );
//! cs.add_coordinate(Box::new(dir));
//! let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42040575e9);
//! cs.add_coordinate(Box::new(spec));
//! let stokes = StokesCoordinate::new(vec![StokesType::I, StokesType::V]);
//! cs.add_coordinate(Box::new(stokes));
//!
//! // Convert to FITS header
//! let shape = [1024, 1024, 256, 2];
//! let header = to_fits_header(&cs, &shape);
//!
//! // Round-trip back
//! let cs2 = from_fits_header(&header, &shape).unwrap();
//! assert_eq!(cs2.n_pixel_axes(), cs.n_pixel_axes());
//! ```

pub mod coordinate_util;
pub mod header;

pub use coordinate_util::{from_fits_header, to_fits_header};
pub use header::{FitsHeader, FitsKeyword, FitsValue};
