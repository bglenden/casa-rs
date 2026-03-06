// SPDX-License-Identifier: LGPL-3.0-or-later
//! Derived quantities computed from MS metadata: hour angle, parallactic
//! angle, azimuth/elevation, local apparent sidereal time, and J2000 UVW.
//!
//! The [`engine::MsCalEngine`] caches antenna positions, field directions, and
//! the observatory position extracted from an MS, then computes derived
//! quantities on demand using the `casacore_types::measures` conversion
//! machinery.
//!
//! Cf. C++ `DerivedMC::MSCalEngine`, `DerivedColumn`.

pub mod columns;
pub mod engine;
