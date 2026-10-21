// SPDX-License-Identifier: LGPL-3.0-or-later
//! Antenna configuration files: CASA's `simutil.readantenna`.
//!
//! `simobserve` and `simalma` read the array from a text file such as
//! CASA's `alma.cycle10.3.cfg`:
//!
//! ```text
//! # observatory=ALMA
//! # coordsys=LOC (local tangent plane)
//! # x             y               z             diam  station  ant
//! -5.850273514   -125.9985379    -1.590364043   12.   A058     DA41
//! -19.90369337    52.82680653    -1.892119601   12.   A023     DA42
//! ```
//!
//! [`read_antenna_config`] reads one and returns what CASA's
//! `readantenna` returns: Earth-centred (ITRF) antenna positions, dish
//! diameters, pad and antenna names, the telescope name and the observatory
//! position.
//!
//! # The format
//!
//! - **Header lines** start with `#`. When the text after the `#` holds
//!   exactly one `=`, the two sides, stripped of every `#` and of
//!   surrounding white space, are a `key=value` parameter; the key is all
//!   the text before the `=`. Other header lines are comments. Headers may
//!   appear anywhere, and a later duplicate wins.
//! - **Antenna lines** lose everything from their first `#`. Their
//!   white-space-separated columns are x, y, z, diameter (metres, parsed as
//!   Python's `float`), the pad (station) name and the antenna name. With
//!   five columns the pad is also the antenna name; with four, both are
//!   `A%02d` for the antenna's index.
//! - **`coordsys`** (required) says what x, y, z are. Its value is
//!   upper-cased; `XYZ` and `UTM` must match exactly and `LOC` matches its
//!   first three characters (CASA's own files say
//!   `coordsys=LOC (local tangent plane)`).
//!   - `XYZ`: ITRF x, y, z, used as they are.
//!   - `UTM`: easting, northing and elevation, with the required headers
//!     `zone`, `datum` (a code of [`geodesy::DATUMS`], case-sensitive) and
//!     `hemisphere` (south when its first letter is `S` or `s`), converted
//!     with [`geodesy::utm_to_xyz`].
//!   - `LOC`: east, north and up offsets from the observatory, converted
//!     with [`geodesy::local_to_itrf`] at the observatory's geodetic WGS 84
//!     latitude, longitude and height.
//! - **`observatory`** names the telescope (`SIMULATED` without one).
//! - **`COFA=lon,lat`** (degrees) sets the observatory position, at height
//!   zero; it overrides a known observatory, with a warning.
//!
//! # The observatory position
//!
//! In CASA's order:
//! 1. `COFA`, when given.
//! 2. A known observatory: the measures provider's observatory table
//!    (CASA's `me.observatory`), converted to WGS 84. The name matches as
//!    casacore's `MeasTable::Observatory` matches it: a case-insensitive
//!    exact match, else the one table name that is a prefix of the
//!    telescope name (CASA's `aca.tp.cfg` says `observatory=ALMASD`, which
//!    is ALMA). Two such names (`GB` and `GBT` for `GBTX`) match none.
//! 3. Otherwise a `LOC` file is an error, and for absolute coordinates the
//!    observatory is the mean antenna position, converted to WGS 84 with
//!    [`geodesy::xyz_to_geodetic`].
//!
//! The position is returned in WGS 84, as CASA returns it.
//!
//! # Where this differs from CASA
//!
//! CASA's `readantenna` has bugs on some paths. These follow its evident
//! intent instead. Each was confirmed by running CASA 6.7.6 unless it says
//! otherwise.
//! - **Blank and short lines.** Lines that are neither headers nor
//!   antennas (blank lines, lines with fewer than four columns) are
//!   skipped. CASA means to skip them (`pass  # ignore empty rows`) but
//!   appends a default `A%02d` pad and antenna name for each, numbered by
//!   line. Its name lists then outgrow the positions and pair with the
//!   wrong antennas: for CASA's `WSRT.cfg`, whose header has two blank
//!   lines, it returns 16 names for 14 antennas, starting `A00`, `A01`,
//!   `RT0`.
//! - **The mean position.** For an unknown observatory without `COFA`, CASA
//!   averages the loop variable `x` left over from the coordinate
//!   conversion. For `XYZ` files that variable does not exist and CASA
//!   raises `UnboundLocalError`; for `UTM` files it is the last antenna's
//!   x, y, z. CASA also unpacks `xyz2long`'s longitude, latitude, height as
//!   latitude, longitude, height, so its position has the two angles
//!   swapped. Here the observatory is `xyz2long` of the mean of the ITRF
//!   antenna positions, as CASA's comment says.
//! - **Errors.**
//!   - A `coordsys` that is none of `XYZ`, `UTM`, `LOC…` is an error. CASA
//!     returns no positions (and, through the blank-line bug, one name per
//!     antenna line).
//!   - A non-integer `zone`, an empty `hemisphere` and a malformed `COFA`
//!     are errors with messages in CASA's style, where CASA lets Python's
//!     `ValueError` or `IndexError` escape.
//!   - A number that does not parse is CASA's `Could not read file` error,
//!     with the line and the text added.
//!   - `zone` and `datum` are checked even when there are no antennas,
//!     where CASA never converts them (from CASA's source).
//! - **Unresolvable names.** CASA counts a telescope as known when any
//!   table name is a substring of it, then looks it up with
//!   `me.observatory`, which can fail (`xALMAx`). CASA then raises
//!   `KeyError` for a `LOC` file and takes the mean-position path otherwise;
//!   here such a name is simply unknown.
//! - **Prefix ambiguity.** casacore also counts a longer table name that
//!   starts with the telescope name as a candidate (`MWA3` matches both
//!   `MWA` and `MWA32T`, so neither; from casacore's `MUString::minimaxNC`).
//!   The measures provider only answers exact lookups and cannot list its
//!   table, so that case resolves to the shorter name here.
//! - **`COFA` height.** The observatory is at height 0. casacore stores a
//!   zero-height WGS 84 position as a 1e-6 m vector, so CASA's position, and
//!   the height its `LOC` conversion uses, is 1e-6 m.
//! - **Conversion to WGS 84** of an observatory tabulated in ITRF uses
//!   [`MPosition::convert_to`] (SOFA's `gc2gd`) rather than casacore's
//!   iteration. For real sites they agree to about 1e-14 rad and 1e-9 m.
//!   CASA's table also has placeholder entries at or near the geocentre
//!   (`VLBA` and `EVN` at ITRF (1e-6, 0, 0) m; `LOFAR`, `OVRO_MMA` and
//!   `SUNRISE` within a kilometre of it), whose geodetic positions are
//!   meaningless either way: for CASA's `vlba.cfg` CASA's observatory is
//!   (0, 0, 742.93 m) and this one (0, 0, −6378137 m). `XYZ` antenna
//!   positions do not depend on it.
//! - **Numbers** with non-ASCII decimal digits, which Python's `float`
//!   accepts, are rejected (from Python's documentation).

pub mod geodesy;
mod parse;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use casa_types::measures::{MPosition, MeasureError, MeasuresProvider, PositionRef};
use thiserror::Error;

/// The telescope name CASA uses when the file has no `observatory` header.
pub const SIMULATED_TELESCOPE: &str = "SIMULATED";

/// How a configuration file gives its antenna positions (`coordsys`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigCoordinates {
    /// `XYZ`: Earth-centred ITRF x, y, z.
    Xyz,
    /// `UTM`: easting, northing and elevation in one UTM zone.
    Utm,
    /// `LOC…`: east, north, up offsets from the observatory.
    Local,
}

/// Where [`AntennaConfigFile::observatory`] comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservatorySource {
    /// The measures provider's observatory table.
    Catalog,
    /// The `COFA` header.
    Cofa,
    /// The mean antenna position (an unknown observatory with absolute
    /// coordinates).
    MeanAntennaPosition,
}

/// An antenna configuration file as CASA's `readantenna` returns it.
///
/// The vectors run over antennas in file order and have equal lengths.
/// CASA's return tuple is `(x, y, z, dish_diameter, pad_names,
/// antenna_names, telescope, observatory)`.
#[derive(Debug, Clone)]
pub struct AntennaConfigFile {
    /// ITRF x of each antenna, metres.
    pub x: Vec<f64>,
    /// ITRF y of each antenna, metres.
    pub y: Vec<f64>,
    /// ITRF z of each antenna, metres.
    pub z: Vec<f64>,
    /// Dish diameters, metres.
    pub dish_diameter: Vec<f64>,
    /// Pad (station) names: the fifth column.
    pub pad_names: Vec<String>,
    /// Antenna names: the sixth column, else the pad name.
    pub antenna_names: Vec<String>,
    /// The `observatory` header, or [`SIMULATED_TELESCOPE`].
    pub telescope: String,
    /// The observatory position in WGS 84 (CASA's `posobs`).
    pub observatory: MPosition,
    /// How [`Self::observatory`] was found.
    pub observatory_source: ObservatorySource,
    /// The file's coordinate system.
    pub coordinates: ConfigCoordinates,
    /// Every `key=value` header parameter, values as written.
    pub header: BTreeMap<String, String>,
}

/// A configuration file that cannot be read. Where CASA reports the same
/// failure, the message is CASA's.
#[derive(Debug, Error)]
pub enum ReadAntennaError {
    /// The file could not be read.
    #[error("Could not read file: '{}': {source}", path.display())]
    Io {
        /// The file.
        path: PathBuf,
        /// The I/O error.
        source: std::io::Error,
    },
    /// An antenna column that should be a number is not one.
    #[error("{}", invalid_number_message(.path.as_deref(), .line, .value))]
    InvalidNumber {
        /// The file, when the configuration was read from one.
        path: Option<PathBuf>,
        /// The 1-based line number.
        line: usize,
        /// The column's text.
        value: String,
    },
    /// The header has no `coordsys`.
    #[error("Must specify XYZ, UTM or LOC coordinate system in antenna file header")]
    MissingCoordinateSystem,
    /// `coordsys` is none of `XYZ`, `UTM`, `LOC…`.
    #[error(
        "Must specify XYZ, UTM or LOC coordinate system in antenna file header (coordsys={value})"
    )]
    UnknownCoordinateSystem {
        /// The `coordsys` value.
        value: String,
    },
    /// Local coordinates with neither a known observatory nor `COFA`.
    #[error(
        "To use local coordinates in the antenna position file, you must either use a known \
         observatory name, or provide the COFA explicitly"
    )]
    LocalNeedsObservatory,
    /// UTM without `zone`.
    #[error("You must specify zone=NN in your antenna file")]
    MissingZone,
    /// A `zone` that is not an integer.
    #[error("You must specify zone=NN in your antenna file (zone={value})")]
    InvalidZone {
        /// The `zone` value.
        value: String,
    },
    /// UTM without `datum`.
    #[error("You must specify datum in your antenna file")]
    MissingDatum,
    /// A datum code that is not in [`geodesy::DATUMS`].
    #[error("unknown datum {code}")]
    UnknownDatum {
        /// The `datum` value.
        code: String,
    },
    /// UTM without `hemisphere`, or with an empty one.
    #[error("You must specify hemisphere=N|S in your antenna file")]
    MissingHemisphere,
    /// A `COFA` that is not `longitude,latitude`.
    #[error("COFA must be longitude,latitude in degrees (COFA={value})")]
    InvalidCofa {
        /// The `COFA` value.
        value: String,
    },
    /// An unknown observatory with no antennas to place it by.
    #[error("Cannot place an unknown observatory without COFA: the file has no antennas")]
    NoAntennas,
    /// The measures provider failed.
    #[error("Could not look up or convert observatory '{telescope}': {source}")]
    Measures {
        /// The telescope name.
        telescope: String,
        /// The provider's error.
        source: MeasureError,
    },
}

/// Read an antenna configuration file (CASA's `simutil.readantenna`).
///
/// `measures` supplies the observatory table. See the [module
/// docs](self) for the format, the rules and the differences from CASA.
///
/// # Errors
///
/// [`ReadAntennaError::Io`] when the file cannot be read as UTF-8 text, and
/// otherwise as [`read_antenna_config_str`].
pub fn read_antenna_config(
    path: &Path,
    measures: &dyn MeasuresProvider,
) -> Result<AntennaConfigFile, ReadAntennaError> {
    tracing::info!("Reading antenna positions from '{}'", path.display());
    let contents = std::fs::read_to_string(path).map_err(|source| ReadAntennaError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    read_antenna_config_str(&contents, measures).map_err(|error| match error {
        ReadAntennaError::InvalidNumber {
            path: None,
            line,
            value,
        } => ReadAntennaError::InvalidNumber {
            path: Some(path.to_path_buf()),
            line,
            value,
        },
        error => error,
    })
}

/// CASA's `Could not read file: '<path>'`, with the line and the text.
fn invalid_number_message(path: Option<&Path>, line: &usize, value: &str) -> String {
    let file = path.map_or_else(String::new, |path| format!(" '{}':", path.display()));
    format!("Could not read file:{file} line {line}: could not convert string to float: '{value}'")
}

/// Read an antenna configuration from its text; [`read_antenna_config`]
/// without the file.
///
/// # Errors
///
/// - [`ReadAntennaError::InvalidNumber`] for an antenna column that is not
///   a number;
/// - [`ReadAntennaError::MissingCoordinateSystem`] and
///   [`ReadAntennaError::UnknownCoordinateSystem`] for `coordsys`;
/// - [`ReadAntennaError::InvalidCofa`] for `COFA`;
/// - [`ReadAntennaError::LocalNeedsObservatory`] for local coordinates
///   without an observatory position;
/// - the `Missing…`/`Invalid…`/`UnknownDatum` variants for the UTM headers;
/// - [`ReadAntennaError::NoAntennas`] when an unknown observatory must be
///   placed by antennas the file does not have;
/// - [`ReadAntennaError::Measures`] when the provider fails.
pub fn read_antenna_config_str(
    contents: &str,
    measures: &dyn MeasuresProvider,
) -> Result<AntennaConfigFile, ReadAntennaError> {
    let parsed = parse::parse_config(contents)?;
    let coordinates = coordinate_system(&parsed.header)?;
    let telescope = parsed
        .header
        .get("observatory")
        .cloned()
        .unwrap_or_else(|| SIMULATED_TELESCOPE.to_string());
    let known = catalog_observatory(&telescope, measures)?;

    let placed = if let Some(cofa) = parsed.header.get("COFA") {
        if known.is_some() {
            tracing::warn!(
                "antenna config file specifies COFA for a known observatory {telescope}, \
                 overriding with specified COFA."
            );
        }
        Some((cofa_position(cofa)?, ObservatorySource::Cofa))
    } else if let Some(position) = known {
        Some((position, ObservatorySource::Catalog))
    } else if coordinates == ConfigCoordinates::Local {
        return Err(ReadAntennaError::LocalNeedsObservatory);
    } else {
        None
    };

    let (x, y, z) = match (coordinates, &placed) {
        (ConfigCoordinates::Xyz, _) => (parsed.x, parsed.y, parsed.z),
        (ConfigCoordinates::Utm, _) => utm_positions(&parsed)?,
        (ConfigCoordinates::Local, Some((observatory, _))) => local_positions(&parsed, observatory),
        (ConfigCoordinates::Local, None) => return Err(ReadAntennaError::LocalNeedsObservatory),
    };
    let (observatory, observatory_source) = match placed {
        Some(placed) => placed,
        None => (
            mean_position(&x, &y, &z)?,
            ObservatorySource::MeanAntennaPosition,
        ),
    };

    Ok(AntennaConfigFile {
        x,
        y,
        z,
        dish_diameter: parsed.diameter,
        pad_names: parsed.pad_names,
        antenna_names: parsed.antenna_names,
        telescope,
        observatory,
        observatory_source,
        coordinates,
        header: parsed.header,
    })
}

/// `coordsys`, upper-cased: `XYZ` or `UTM` exactly, or starting `LOC`.
fn coordinate_system(
    header: &BTreeMap<String, String>,
) -> Result<ConfigCoordinates, ReadAntennaError> {
    let value = header
        .get("coordsys")
        .ok_or(ReadAntennaError::MissingCoordinateSystem)?;
    let upper = value.to_uppercase();
    if upper == "XYZ" {
        Ok(ConfigCoordinates::Xyz)
    } else if upper == "UTM" {
        Ok(ConfigCoordinates::Utm)
    } else if upper.chars().take(3).eq("LOC".chars()) {
        Ok(ConfigCoordinates::Local)
    } else {
        Err(ReadAntennaError::UnknownCoordinateSystem {
            value: value.clone(),
        })
    }
}

/// The telescope's position from the provider's table, in WGS 84, as
/// casacore's `MeasTable::Observatory` finds it: an exact
/// (case-insensitive) match, else the single table name that is a proper
/// prefix of the telescope name.
fn catalog_observatory(
    telescope: &str,
    measures: &dyn MeasuresProvider,
) -> Result<Option<MPosition>, ReadAntennaError> {
    let failed = |source| ReadAntennaError::Measures {
        telescope: telescope.to_string(),
        source,
    };
    let lookup = |name: &str| MPosition::from_observatory_name(name, measures).map_err(failed);
    let position = match lookup(telescope)? {
        Some(position) => Some(position),
        None => {
            let mut matches = Vec::new();
            for (end, _) in telescope.char_indices().skip(1) {
                let prefix = &telescope[..end];
                // Providers trim their queries; a prefix ending in white
                // space would match the shorter name a second time.
                if prefix.trim() != prefix {
                    continue;
                }
                if let Some(position) = lookup(prefix)? {
                    matches.push(position);
                }
            }
            if matches.len() == 1 {
                matches.pop()
            } else {
                None
            }
        }
    };
    position
        .map(|position| position.convert_to(PositionRef::WGS84).map_err(failed))
        .transpose()
}

/// `COFA=lon,lat` in degrees, at height zero.
fn cofa_position(value: &str) -> Result<MPosition, ReadAntennaError> {
    let invalid = || ReadAntennaError::InvalidCofa {
        value: value.to_string(),
    };
    let mut parts = value.split(',');
    let longitude = parts
        .next()
        .and_then(parse::python_float)
        .ok_or_else(invalid)?;
    let latitude = parts
        .next()
        .and_then(parse::python_float)
        .ok_or_else(invalid)?;
    Ok(MPosition::new_wgs84(
        longitude.to_radians(),
        latitude.to_radians(),
        0.0,
    ))
}

type Positions = (Vec<f64>, Vec<f64>, Vec<f64>);

/// UTM easting, northing, elevation to ITRF.
fn utm_positions(parsed: &parse::ParsedConfig) -> Result<Positions, ReadAntennaError> {
    let header = &parsed.header;
    let zone = header.get("zone").ok_or(ReadAntennaError::MissingZone)?;
    let datum = header.get("datum").ok_or(ReadAntennaError::MissingDatum)?;
    let hemisphere = header
        .get("hemisphere")
        .and_then(|value| value.chars().next())
        .ok_or(ReadAntennaError::MissingHemisphere)?;
    let south = hemisphere.to_uppercase().eq(['S']);
    let zone = parse::python_int(zone).ok_or_else(|| ReadAntennaError::InvalidZone {
        value: zone.clone(),
    })?;
    let datum = geodesy::datum(datum).ok_or_else(|| ReadAntennaError::UnknownDatum {
        code: datum.clone(),
    })?;
    tracing::info!(
        "Antenna locations in UTM; will read from file easting, northing, elevation in m"
    );
    Ok(convert_rows(parsed, |row| {
        geodesy::utm_to_xyz(row[0], row[1], row[2], zone, datum, south)
    }))
}

/// Local east, north, up offsets to ITRF about the observatory.
fn local_positions(parsed: &parse::ParsedConfig, observatory: &MPosition) -> Positions {
    // CASA converts the observatory's WGS 84 angles to degrees for
    // locxyz2itrf, which converts them back.
    let [longitude, latitude, height] = observatory.values();
    let (latitude_deg, longitude_deg) = (latitude.to_degrees(), longitude.to_degrees());
    convert_rows(parsed, |row| {
        geodesy::local_to_itrf(latitude_deg, longitude_deg, height, row)
    })
}

fn convert_rows(parsed: &parse::ParsedConfig, convert: impl Fn([f64; 3]) -> [f64; 3]) -> Positions {
    let mut positions = (
        Vec::with_capacity(parsed.x.len()),
        Vec::with_capacity(parsed.x.len()),
        Vec::with_capacity(parsed.x.len()),
    );
    for ((&x, &y), &z) in parsed.x.iter().zip(&parsed.y).zip(&parsed.z) {
        let [x, y, z] = convert([x, y, z]);
        positions.0.push(x);
        positions.1.push(y);
        positions.2.push(z);
    }
    positions
}

/// The mean ITRF antenna position in WGS 84 (CASA's evident intent).
fn mean_position(x: &[f64], y: &[f64], z: &[f64]) -> Result<MPosition, ReadAntennaError> {
    if x.is_empty() {
        return Err(ReadAntennaError::NoAntennas);
    }
    let count = x.len() as f64;
    let mean = |values: &[f64]| values.iter().sum::<f64>() / count;
    let (longitude, latitude, height) =
        geodesy::xyz_to_geodetic(mean(x), mean(y), mean(z), &geodesy::WGS84)
            .expect("the WGS 84 ellipsoid is valid");
    Ok(MPosition::new_wgs84(longitude, latitude, height))
}

#[cfg(test)]
mod tests;
