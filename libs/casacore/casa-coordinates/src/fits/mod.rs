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
//! use casa_coordinates::fits::{FitsHeader, to_fits_header, from_fits_header};
//! use casa_coordinates::{
//!     CoordinateSystem, DirectionCoordinate, SpectralCoordinate,
//!     StokesCoordinate, StokesType, Projection, ProjectionType,
//! };
//! use casa_core::measures::direction::DirectionRef;
//! use casa_core::measures::frequency::FrequencyRef;
//!
//! // Build a coordinate system
//! let mut cs = CoordinateSystem::new();
//! let proj = Projection::new(ProjectionType::SIN);
//! let dir = DirectionCoordinate::new(
//!     DirectionRef::J2000, proj,
//!     [0.0, std::f64::consts::FRAC_PI_4],
//!     [-1e-4, 1e-4], [512.0, 512.0],
//! );
//! cs.add_coordinate(dir);
//! let spec = SpectralCoordinate::new(FrequencyRef::LSRK, 1.42e9, 1e6, 0.0, 1.42040575e9);
//! cs.add_coordinate(spec);
//! let stokes = StokesCoordinate::new(vec![StokesType::I, StokesType::V]);
//! cs.add_coordinate(stokes);
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
mod obs_date;

pub use coordinate_util::{from_fits_header, to_fits_header, try_to_fits_header};
pub use header::{FitsHeader, FitsKeyword, FitsValue};

/// Convert a FITS unit expression to the canonical casacore unit spelling.
pub fn canonical_unit_name(input: &str) -> String {
    map_unit_symbols(input, |token| match token {
        "BEAM" => "beam",
        "DAYS" => "d",
        "DEGREES" | "DEG" => "deg",
        "HZ" => "Hz",
        "JY" => "Jy",
        "KELVINS" | "KELVIN" => "K",
        "KM" => "km",
        "METERS" | "M" => "m",
        "PASCAL" => "Pa",
        "PIXEL" => "pixel",
        "SECONDS" | "SEC" | "S" => "s",
        "VOLTS" => "V",
        "YEARS" | "YEAR" => "a",
        _ => token,
    })
}

// CUNIT aliases before casacore's unit parser, matching WCSLIB UNITFIX
// (wcsutrn with ctrl=7), including S, H, and D in astronomical headers.
fn wcs_unit_name(input: &str) -> String {
    let translated = map_unit_symbols(input, |token| match token {
        "Angstroms" | "angstrom" | "angstroms" => "Angstrom",
        "arcmins" | "ARCMIN" | "ARCMINS" => "arcmin",
        "arcsecs" | "ARCSEC" | "ARCSECS" => "arcsec",
        "Byte" => "byte",
        "day" | "days" | "DAY" | "D" => "d",
        "degree" | "degrees" | "Deg" | "Degree" | "Degrees" | "DEGREE" => "deg",
        "GHZ" => "GHz",
        "hr" | "HR" | "H" => "h",
        "hz" => "Hz",
        "KHZ" => "kHz",
        "kelvin" | "kelvins" | "Kelvin" | "Kelvins" => "K",
        "meter" | "meters" | "metre" | "metres" | "METER" | "METRE" | "METRES" => "m",
        "MIN" => "min",
        "MHZ" => "MHz",
        "Ohm" => "ohm",
        "pascal" | "pascals" | "Pascal" | "Pascals" | "PASCALS" => "Pa",
        "pixels" | "PIXELS" => "pixel",
        "radian" | "radians" | "RAD" | "RADIAN" | "RADIANS" => "rad",
        "sec" | "second" | "seconds" | "SECOND" => "s",
        "volt" | "volts" | "Volt" | "Volts" | "VOLT" => "V",
        "year" | "years" | "YR" => "a",
        _ => token,
    });
    canonical_unit_name(&translated)
}

fn map_unit_symbols(input: &str, translate: impl Fn(&str) -> &str) -> String {
    let mut output = String::new();
    let mut remaining = input;
    while !remaining.is_empty() {
        let alphabetic = remaining.as_bytes()[0].is_ascii_alphabetic();
        let end = remaining
            .find(|c: char| c.is_ascii_alphabetic() != alphabetic)
            .unwrap_or(remaining.len());
        let token = &remaining[..end];
        output.push_str(translate(token));
        remaining = &remaining[end..];
    }
    output
}
