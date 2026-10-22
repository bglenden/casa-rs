// SPDX-License-Identifier: LGPL-3.0-or-later
//! Derived quantities computed from MS metadata: hour angle, parallactic
//! angle, azimuth/elevation, local apparent sidereal time, and J2000 UVW.
//!
//! The [`engine::MsCalEngine`] caches antenna positions, field directions, and
//! the observatory position extracted from an MS, then computes derived
//! quantities on demand using the `casa_types::measures` conversion
//! machinery.
//!
//! Cf. C++ `DerivedMC::MSCalEngine`, `DerivedColumn`.

pub mod columns;
pub mod engine;
mod gridft;

pub use gridft::{GridFtProjection, gridft_uvw_between_j2000_directions};
