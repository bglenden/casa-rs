// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA-style display formatting of world coordinates.
//!
//! casacore's `CoordinateSystem::format` chain renders each world axis
//! for display, and the statistics surfaces join one field per world
//! axis with `", "`. This module reproduces that rendering for the
//! forms those surfaces reach:
//!
//! - equatorial directions as sexagesimal coordinates and Galactic,
//!   ecliptic, and horizontal directions as fixed decimal degrees,
//! - Stokes and Quality planes by name,
//! - every other axis as C++'s default `ostream <<` rendering of the
//!   world value with its unit appended directly.

use casa_core::measures::direction::DirectionRef;

use crate::coordinate::CoordinateModel;
use crate::coordinate_system::CoordinateSystem;
use crate::error::CoordinateError;

/// Format the world coordinates of `pixel` the way CASA's statistics
/// surfaces render them: one field per world axis, comma-separated.
///
/// The pixel vector has one value per pixel axis, in axis order. The
/// returned string is casacore's `CoordinateSystem::format` rendering
/// of every world axis, joined with `", "` as `ia.statistics` builds
/// its `blcf`, `trcf`, `minposf` and `maxposf` strings.
///
/// # Errors
///
/// A pixel the projection cannot place or whose length is not the
/// coordinate system's pixel-axis count.
///
/// # Examples
///
/// ```
/// use casa_coordinates::format_world_display;
/// use casa_coordinates::{
///     DirectionCoordinate, Projection, ProjectionType, StokesCoordinate, StokesType,
/// };
/// use casa_core::measures::direction::DirectionRef;
///
/// let arcmin = std::f64::consts::PI / 180.0 / 60.0;
/// let mut coords = casa_coordinates::CoordinateSystem::new();
/// coords.add_coordinate(
///     DirectionCoordinate::new(
///         DirectionRef::J2000,
///         Projection::new(ProjectionType::SIN),
///         [0.0, 0.0],
///         [-arcmin, arcmin],
///         [4.0, 4.0],
///     )
///     .with_world_units(["'", "'"])
///     .expect("arcminutes"),
/// );
/// coords.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
/// let text = format_world_display(&coords, &[0.0, 0.0, 0.0]).expect("inside the projection");
/// assert_eq!(text, "00:00:16.000, -00.04.00.000, I");
/// ```
pub fn format_world_display(
    coords: &CoordinateSystem,
    pixel: &[f64],
) -> Result<String, CoordinateError> {
    let world = coords.to_world(pixel)?;
    let fields: Vec<_> = world
        .into_iter()
        .enumerate()
        .map(|(axis, value)| {
            let (index, local) = coords.world_axis_owner(axis).expect("public world axis");
            let coord = coords.coordinate(index);
            match coord {
                CoordinateModel::Direction(direction) => direction_field(
                    direction.direction_ref(),
                    direction.longitude_axis() == Some(local),
                    value,
                ),
                CoordinateModel::Stokes(_) => {
                    let code = value as i32;
                    crate::stokes::StokesType::from_code(code)
                        .map_or_else(|| code.to_string(), |kind| kind.name().into())
                }
                CoordinateModel::Quality(_) => crate::QualityType::from_world(value).name().into(),
                _ => format!(
                    "{}{}",
                    format_stream_value(value),
                    coord.axis_units()[local]
                ),
            }
        })
        .collect();
    Ok(fields.join(", "))
}

fn direction_field(frame: DirectionRef, longitude: bool, radians: f64) -> String {
    let signed = matches!(
        frame,
        DirectionRef::ECLIPTIC | DirectionRef::MECLIPTIC | DirectionRef::TECLIPTIC
    );
    let positive = matches!(
        frame,
        DirectionRef::GALACTIC
            | DirectionRef::SUPERGAL
            | DirectionRef::AZEL
            | DirectionRef::AZELSW
            | DirectionRef::AZELGEO
            | DirectionRef::AZELSWGEO
    );
    if !(signed || positive) {
        return if longitude {
            format_longitude_hours(radians)
        } else {
            format_latitude_dms(radians)
        };
    }
    let degrees = radians.to_degrees();
    let wrapped = if !longitude {
        degrees
    } else if signed {
        (degrees + 180.0).rem_euclid(360.0) - 180.0
    } else {
        degrees.rem_euclid(360.0)
    };
    format!("{wrapped:.6}deg")
}

/// Format a longitude in radians as hours `HH:MM:SS.sss`, wrapped into
/// one revolution, with the seconds rounded to three decimals.
pub fn format_longitude_hours(radians: f64) -> String {
    format_longitude_hours_with_precision(radians, 3)
}

/// Format a longitude in hours, with the requested number of second decimals.
pub fn format_longitude_hours_with_precision(radians: f64, decimals: usize) -> String {
    let hours = (radians * 12.0 / std::f64::consts::PI).rem_euclid(24.0);
    let mut hour = hours.floor() as u32;
    let mut minute = ((hours - hour as f64) * 60.0).floor() as u32;
    let seconds = (hours - hour as f64) * 3600.0 - minute as f64 * 60.0;
    let scale = 10.0_f64.powi(decimals as i32);
    let seconds = (seconds * scale).round() / scale;
    if seconds >= 60.0 {
        minute += 1;
        if minute >= 60 {
            minute = 0;
            hour = (hour + 1) % 24;
        }
        return format!(
            "{hour:02}:{minute:02}:{:0width$.decimals$}",
            0.0,
            width = decimals + 3
        );
    }
    format!(
        "{hour:02}:{minute:02}:{seconds:0width$.decimals$}",
        width = decimals + 3
    )
}

/// Format a latitude in radians as signed `DDD.MM.SS.sss` with the
/// seconds rounded to three decimals and the sign carried at zero
/// degrees, as casacore's ANGLE rendering gives it.
pub fn format_latitude_dms(radians: f64) -> String {
    format_latitude_dms_with_precision(radians, 3)
}

/// Format a signed angle in degrees, with the requested number of second decimals.
pub fn format_latitude_dms_with_precision(radians: f64, decimals: usize) -> String {
    let degrees = radians * 180.0 / std::f64::consts::PI;
    let sign = if degrees.is_sign_negative() { "-" } else { "+" };
    let absolute = degrees.abs();
    let mut degree = absolute.floor() as u32;
    let mut minute = ((absolute - degree as f64) * 60.0).floor() as u32;
    let seconds = (absolute - degree as f64) * 3600.0 - minute as f64 * 60.0;
    let scale = 10.0_f64.powi(decimals as i32);
    let seconds = (seconds * scale).round() / scale;
    if seconds >= 60.0 {
        minute += 1;
        if minute >= 60 {
            minute = 0;
            degree += 1;
        }
        return format!(
            "{sign}{degree:02}.{minute:02}.{:0width$.decimals$}",
            0.0,
            width = decimals + 3
        );
    }
    format!(
        "{sign}{degree:02}.{minute:02}.{seconds:0width$.decimals$}",
        width = decimals + 3
    )
}

/// Format a world value as C++'s default `ostream <<` rendering: `%g`
/// with six significant digits, trailing zeros stripped.
pub fn format_stream_value(value: f64) -> String {
    format_stream_value_with_precision(value, 6)
}

/// Format a world value with the given number of significant digits.
pub fn format_stream_value_with_precision(value: f64, precision: usize) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "nan".to_string()
        } else if value > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }

    // In scientific form the mantissa carries
    // the digits, and the exponent decides %g's fixed-versus-scientific
    // choice.
    let precision = precision.max(1);
    let fractional = precision - 1;
    let scientific = format!("{value:.fractional$e}");
    let (mantissa, exponent) = scientific
        .split_once('e')
        .expect("scientific formatting carries an exponent");
    let exponent: i32 = exponent.parse().expect("a decimal exponent");
    let digits: String = mantissa.chars().filter(|c| c.is_ascii_digit()).collect();
    let sign = if mantissa.starts_with('-') { "-" } else { "" };

    if !(-4..precision as i32).contains(&exponent) {
        let mut text = format!("{}.{}", &digits[..1], &digits[1..]);
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
        let sign_char = if exponent < 0 { '-' } else { '+' };
        let magnitude = exponent.unsigned_abs();
        format!("{sign}{text}e{sign_char}{magnitude:02}")
    } else {
        let decimals = (fractional as i32 - exponent).max(0) as usize;
        let mut text = format!("{:.decimals$}", value.abs());
        if text.contains('.') {
            while text.ends_with('0') {
                text.pop();
            }
            if text.ends_with('.') {
                text.pop();
            }
        }
        format!("{sign}{text}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hours_wrap_and_round() {
        let to_hours = |deg: f64| deg * std::f64::consts::PI / 180.0;
        assert_eq!(format_longitude_hours(to_hours(0.0)), "00:00:00.000");
        assert_eq!(
            format_longitude_hours(to_hours(-1.0 / 60.0)),
            "23:59:56.000"
        );
        assert_eq!(format_longitude_hours(to_hours(180.0)), "12:00:00.000");
        // Four arcminutes of sky is sixteen seconds of time.
        let arcmin = to_hours(1.0 / 60.0);
        assert_eq!(format_longitude_hours(4.0 * arcmin), "00:00:16.000");
        // Rounding past a whole second carries into the minute.
        let nearly = to_hours(10.0) + 59.9996 * std::f64::consts::PI / 12.0 / 3600.0;
        assert_eq!(format_longitude_hours(nearly), "00:41:00.000");
    }

    #[test]
    fn latitude_carries_the_sign_at_every_magnitude() {
        let to_deg = |deg: f64| deg * std::f64::consts::PI / 180.0;
        assert_eq!(format_latitude_dms(to_deg(0.0)), "+00.00.00.000");
        assert_eq!(format_latitude_dms(to_deg(-4.0 / 60.0)), "-00.04.00.000");
        assert_eq!(format_latitude_dms(to_deg(-90.0)), "-90.00.00.000");
        assert_eq!(format_latitude_dms(to_deg(45.5)), "+45.30.00.000");
        assert_eq!(format_latitude_dms(to_deg(3.0 / 60.0)), "+00.03.00.000");
        // Rounding past a whole second carries into the minute.
        let nearly = to_deg(45.0) + 59.9996 * std::f64::consts::PI / 180.0 / 3600.0;
        assert_eq!(format_latitude_dms(nearly), "+45.01.00.000");
    }

    #[test]
    fn stream_values_match_cxx_default_formatting() {
        assert_eq!(format_stream_value(0.0), "0");
        assert_eq!(format_stream_value(1.415002e9), "1.415e+09");
        assert_eq!(format_stream_value(-2.5), "-2.5");
        assert_eq!(format_stream_value(0.25), "0.25");
        assert_eq!(format_stream_value(1e-7), "1e-07");
        assert_eq!(format_stream_value(123456.0), "123456");
        assert_eq!(format_stream_value(1234567.0), "1.23457e+06");
        assert_eq!(format_stream_value(999999.5), "1e+06");
        assert_eq!(format_stream_value(-1e-5), "-1e-05");
    }
}
