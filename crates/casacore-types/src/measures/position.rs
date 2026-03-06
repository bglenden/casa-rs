// SPDX-License-Identifier: LGPL-3.0-or-later
//! Position measure: 3D spatial positions in different reference frames.
//!
//! This module provides:
//!
//! - [`PositionRef`] — supported position reference types (ITRF, WGS84).
//! - [`MPosition`] — a 3D position in a specified reference frame, equivalent
//!   to C++ `MPosition`.
//!
//! Conversions between ITRF (geocentric Cartesian) and WGS84 (geodetic) use
//! the [`sofars`] crate's `gd2gc`/`gc2gd` functions with the WGS84 ellipsoid.

use std::fmt;
use std::str::FromStr;

use super::error::MeasureError;

/// WGS84 ellipsoid identifier for sofars (n=1).
const WGS84_ELLIPSOID: i32 = 1;

/// Position reference frame types.
///
/// Corresponds to the subset of C++ `MPosition::Types` needed for ITRF and
/// WGS84 conversions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PositionRef {
    /// International Terrestrial Reference Frame — geocentric Cartesian (x, y, z) in metres.
    ITRF,
    /// World Geodetic System 1984 — geodetic (longitude, latitude, height) with
    /// longitude and latitude in radians, height in metres above the ellipsoid.
    WGS84,
}

impl PositionRef {
    /// Returns the C++ casacore integer code for this reference type.
    ///
    /// These codes match the `MPosition::Types` enum values defined in C++
    /// `MPosition.h`.
    pub fn casacore_code(self) -> i32 {
        match self {
            Self::ITRF => 0,
            Self::WGS84 => 1,
        }
    }

    /// Converts a C++ casacore integer code to the corresponding reference type.
    pub fn from_casacore_code(code: i32) -> Option<Self> {
        match code {
            0 => Some(Self::ITRF),
            1 => Some(Self::WGS84),
            _ => None,
        }
    }

    /// Returns the canonical string name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ITRF => "ITRF",
            Self::WGS84 => "WGS84",
        }
    }
}

impl FromStr for PositionRef {
    type Err = MeasureError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ITRF" => Ok(Self::ITRF),
            "WGS84" => Ok(Self::WGS84),
            _ => Err(MeasureError::UnknownRefType {
                input: s.to_owned(),
            }),
        }
    }
}

impl fmt::Display for PositionRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A 3D position in a specified reference frame.
///
/// `MPosition` stores three coordinate values whose meaning depends on the
/// reference type:
///
/// - **ITRF**: `(x, y, z)` in metres (geocentric Cartesian).
/// - **WGS84**: `(longitude_rad, latitude_rad, height_m)` (geodetic).
///
/// This is the Rust equivalent of C++ `casa::MPosition`.
///
/// # Conversions
///
/// Use [`convert_to`](MPosition::convert_to) to transform between reference
/// frames. The conversion uses the WGS84 reference ellipsoid.
///
/// # Examples
///
/// ```
/// use casacore_types::measures::{MPosition, PositionRef};
///
/// // VLA position in ITRF
/// let vla = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
/// let wgs = vla.convert_to(PositionRef::WGS84).unwrap();
/// assert!((wgs.latitude_rad().to_degrees() - 34.08).abs() < 0.1);
/// ```
#[derive(Debug, Clone)]
pub struct MPosition {
    values: [f64; 3],
    refer: PositionRef,
}

impl MPosition {
    /// Creates an ITRF position from geocentric Cartesian coordinates in metres.
    pub fn new_itrf(x: f64, y: f64, z: f64) -> Self {
        Self {
            values: [x, y, z],
            refer: PositionRef::ITRF,
        }
    }

    /// Creates a WGS84 position from geodetic coordinates.
    ///
    /// - `longitude_rad`: east-positive longitude in radians.
    /// - `latitude_rad`: latitude in radians.
    /// - `height_m`: height above the WGS84 ellipsoid in metres.
    pub fn new_wgs84(longitude_rad: f64, latitude_rad: f64, height_m: f64) -> Self {
        Self {
            values: [longitude_rad, latitude_rad, height_m],
            refer: PositionRef::WGS84,
        }
    }

    /// Returns the raw coordinate triplet.
    pub fn values(&self) -> [f64; 3] {
        self.values
    }

    /// Returns the reference type.
    pub fn refer(&self) -> PositionRef {
        self.refer
    }

    /// Returns the longitude in radians, regardless of reference frame.
    ///
    /// For ITRF, computes `atan2(y, x)`. For WGS84, returns the stored value.
    pub fn longitude_rad(&self) -> f64 {
        match self.refer {
            PositionRef::ITRF => self.values[1].atan2(self.values[0]),
            PositionRef::WGS84 => self.values[0],
        }
    }

    /// Returns the latitude in radians, regardless of reference frame.
    ///
    /// For ITRF, computes the geodetic latitude via WGS84 conversion.
    /// For WGS84, returns the stored value.
    pub fn latitude_rad(&self) -> f64 {
        match self.refer {
            PositionRef::ITRF => {
                // Convert to WGS84 to get geodetic latitude.
                match sofars::coords::gc2gd(WGS84_ELLIPSOID, self.values) {
                    Ok((_lon, lat, _h)) => lat,
                    Err(_) => {
                        // Fallback: geocentric latitude
                        let r = (self.values[0].powi(2) + self.values[1].powi(2)).sqrt();
                        self.values[2].atan2(r)
                    }
                }
            }
            PositionRef::WGS84 => self.values[1],
        }
    }

    /// Returns the geocentric latitude in radians: `asin(z / r)`.
    ///
    /// This is the angle from the equatorial plane to the position vector,
    /// as opposed to the geodetic latitude (normal to the ellipsoid surface).
    /// C++ casacore uses geocentric latitude for `AZEL` (and geodetic for `AZELGEO`).
    pub fn geocentric_latitude_rad(&self) -> f64 {
        let xyz = self.as_itrf();
        let r = (xyz[0] * xyz[0] + xyz[1] * xyz[1] + xyz[2] * xyz[2]).sqrt();
        if r == 0.0 { 0.0 } else { (xyz[2] / r).asin() }
    }

    /// Returns the ITRF (x, y, z) coordinates in metres.
    ///
    /// If this position is already ITRF, returns the stored values directly.
    /// If WGS84, converts to geocentric Cartesian.
    pub fn as_itrf(&self) -> [f64; 3] {
        match self.refer {
            PositionRef::ITRF => self.values,
            PositionRef::WGS84 => sofars::coords::gd2gc(
                WGS84_ELLIPSOID,
                self.values[0],
                self.values[1],
                self.values[2],
            )
            .unwrap_or(self.values),
        }
    }

    /// Returns the position as geocentric spherical coordinates
    /// `(longitude_rad, latitude_rad, radius_m)`.
    ///
    /// This matches the format used by C++ `MVPosition::getRecordValue()`,
    /// which stores positions in geocentric (not geodetic) spherical form.
    pub fn as_spherical(&self) -> (f64, f64, f64) {
        let [x, y, z] = self.as_itrf();
        let r = (x * x + y * y + z * z).sqrt();
        let lon = y.atan2(x);
        let rxy = (x * x + y * y).sqrt();
        let lat = z.atan2(rxy);
        (lon, lat, r)
    }

    /// Converts this position to a different reference frame.
    pub fn convert_to(&self, target: PositionRef) -> Result<MPosition, MeasureError> {
        if self.refer == target {
            return Ok(self.clone());
        }

        match (self.refer, target) {
            (PositionRef::ITRF, PositionRef::WGS84) => {
                let (lon, lat, h) = sofars::coords::gc2gd(WGS84_ELLIPSOID, self.values)
                    .map_err(|code| MeasureError::SofarsError { code })?;
                Ok(MPosition {
                    values: [lon, lat, h],
                    refer: PositionRef::WGS84,
                })
            }
            (PositionRef::WGS84, PositionRef::ITRF) => {
                let xyz = sofars::coords::gd2gc(
                    WGS84_ELLIPSOID,
                    self.values[0],
                    self.values[1],
                    self.values[2],
                )
                .map_err(|code| MeasureError::SofarsError { code })?;
                Ok(MPosition {
                    values: xyz,
                    refer: PositionRef::ITRF,
                })
            }
            _ => unreachable!("only two position reference types exist"),
        }
    }
}

impl fmt::Display for MPosition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.refer {
            PositionRef::ITRF => write!(
                f,
                "Position ITRF: [{:.3}, {:.3}, {:.3}] m",
                self.values[0], self.values[1], self.values[2]
            ),
            PositionRef::WGS84 => write!(
                f,
                "Position WGS84: lon={:.6}°, lat={:.6}°, h={:.3} m",
                self.values[0].to_degrees(),
                self.values[1].to_degrees(),
                self.values[2]
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn position_ref_parse() {
        assert_eq!("ITRF".parse::<PositionRef>().unwrap(), PositionRef::ITRF);
        assert_eq!("WGS84".parse::<PositionRef>().unwrap(), PositionRef::WGS84);
        assert_eq!("itrf".parse::<PositionRef>().unwrap(), PositionRef::ITRF);
        assert!("GALACTIC".parse::<PositionRef>().is_err());
    }

    #[test]
    fn itrf_to_wgs84_roundtrip() {
        // VLA coordinates
        let itrf = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
        let wgs = itrf.convert_to(PositionRef::WGS84).unwrap();
        let back = wgs.convert_to(PositionRef::ITRF).unwrap();

        for i in 0..3 {
            assert!(
                (back.values[i] - itrf.values[i]).abs() < 1e-3,
                "coordinate {i} mismatch: {} vs {}",
                back.values[i],
                itrf.values[i]
            );
        }
    }

    #[test]
    fn longitude_latitude_consistency() {
        let itrf = MPosition::new_itrf(-1601185.4, -5041977.5, 3554875.9);
        let wgs = itrf.convert_to(PositionRef::WGS84).unwrap();

        // Longitude should match
        let lon_diff = (itrf.longitude_rad() - wgs.longitude_rad()).abs();
        assert!(lon_diff < 1e-10, "longitude mismatch: {lon_diff}");

        // Latitude should match
        let lat_diff = (itrf.latitude_rad() - wgs.latitude_rad()).abs();
        assert!(lat_diff < 1e-10, "latitude mismatch: {lat_diff}");
    }

    #[test]
    fn casacore_code_roundtrip() {
        for &r in &[PositionRef::ITRF, PositionRef::WGS84] {
            let code = r.casacore_code();
            assert_eq!(PositionRef::from_casacore_code(code), Some(r));
        }
    }

    #[test]
    fn casacore_code_known_values() {
        assert_eq!(PositionRef::ITRF.casacore_code(), 0);
        assert_eq!(PositionRef::WGS84.casacore_code(), 1);
    }
}
