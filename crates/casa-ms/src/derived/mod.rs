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

use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::quanta::{Quantity, Unit};

/// Parse two angular quantities with an optional direction frame, as CASA
/// tasks accept for an explicit phase center.
pub(crate) fn parse_phase_direction(value: &str) -> Option<MDirection> {
    let mut parts = value.split_whitespace();
    let (frame, longitude, latitude) =
        match (parts.next(), parts.next(), parts.next(), parts.next()) {
            (Some(longitude), Some(latitude), None, None) => {
                (DirectionRef::J2000, longitude, latitude)
            }
            (Some(frame), Some(longitude), Some(latitude), None) => {
                (frame.parse().ok()?, longitude, latitude)
            }
            _ => return None,
        };
    let rad = Unit::new("rad").expect("known angular unit");
    let angle = |text| {
        Quantity::read(text)
            .ok()?
            .get_value_in(&rad)
            .ok()
            .filter(|r| r.is_finite())
    };
    Some(MDirection::from_angles(
        angle(longitude)?,
        angle(latitude)?,
        frame,
    ))
}
