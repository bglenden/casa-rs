// SPDX-License-Identifier: LGPL-3.0-or-later
//! Angle formatting helpers for radian values.
//!
//! This module provides [`MvAngle`], a lightweight Rust counterpart to C++
//! `casacore::MVAngle`. Like `MVAngle`, it stores an angle as radians and
//! supports normalization plus sexagesimal formatting. The current Rust
//! implementation covers the subset needed by CASA-style summary/reporting
//! code: fixed-width angle formatting, two-digit declination-style formatting,
//! and time-style formatting for right ascension.

/// An angle value stored in radians.
///
/// This is the Rust counterpart to C++ `casacore::MVAngle`. It is intended for
/// angle normalization and human-readable formatting. For directional
/// reference-frame conversion use the measure-layer types such as
/// [`crate::measures::direction::MDirection`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MvAngle {
    radians: f64,
}

impl MvAngle {
    /// Creates an `MvAngle` from radians.
    pub const fn from_radians(radians: f64) -> Self {
        Self { radians }
    }

    /// Returns the stored angle in radians.
    pub const fn as_radians(self) -> f64 {
        self.radians
    }

    /// Returns the stored angle in degrees.
    pub fn as_degrees(self) -> f64 {
        self.radians.to_degrees()
    }

    /// Normalizes the angle into the interval `[lower_turns, lower_turns + 1)`.
    ///
    /// This mirrors the common C++ `MVAngle::operator(Double)` usage where the
    /// bound is expressed in turns rather than radians. For example,
    /// `normalized(0.0)` yields the `0..360 deg` range typically used for right
    /// ascension display.
    pub fn normalized(self, lower_turns: f64) -> Self {
        let lower = lower_turns * std::f64::consts::TAU;
        let radians = (self.radians - lower).rem_euclid(std::f64::consts::TAU) + lower;
        Self { radians }
    }

    /// Formats the angle as `+ddd.mm.ss[.f...]`.
    pub fn format_angle(self, second_decimals: usize) -> String {
        format_sexagesimal(
            self.as_degrees(),
            3,
            '.',
            '.',
            second_decimals,
            SignStyle::Always,
        )
    }

    /// Formats the angle as `+dd.mm.ss[.f...]`.
    ///
    /// This matches the common `MVAngle::DIG2` usage for declination- and
    /// latitude-style values.
    pub fn format_angle_dig2(self, second_decimals: usize) -> String {
        format_sexagesimal(
            self.as_degrees(),
            2,
            '.',
            '.',
            second_decimals,
            SignStyle::Always,
        )
    }

    /// Formats the angle as `hh:mm:ss[.f...]`.
    ///
    /// Positive values omit the sign, matching the common `MVAngle::TIME`
    /// display used for right ascension. Negative values are prefixed with `-`.
    pub fn format_time(self, second_decimals: usize) -> String {
        format_sexagesimal(
            self.radians * (24.0 / std::f64::consts::TAU),
            2,
            ':',
            '.',
            second_decimals,
            SignStyle::NegativeOnly,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SignStyle {
    Always,
    NegativeOnly,
}

fn format_sexagesimal(
    primary_units: f64,
    primary_width: usize,
    field_sep: char,
    fraction_sep: char,
    second_decimals: usize,
    sign_style: SignStyle,
) -> String {
    let sign_char = if primary_units.is_sign_negative() {
        '-'
    } else {
        '+'
    };
    let abs_primary = primary_units.abs();
    let scale = 10_i64.pow(second_decimals as u32);
    let total_scaled_seconds = (abs_primary * 3_600.0 * scale as f64).round() as i64;
    let seconds_per_primary = 3_600_i64 * scale;
    let primary = total_scaled_seconds / seconds_per_primary;
    let minutes = (total_scaled_seconds / (60 * scale)) % 60;
    let seconds = (total_scaled_seconds / scale) % 60;
    let fraction = total_scaled_seconds % scale;

    let sign = match sign_style {
        SignStyle::Always => sign_char.to_string(),
        SignStyle::NegativeOnly if sign_char == '-' => "-".to_string(),
        SignStyle::NegativeOnly => String::new(),
    };

    if second_decimals == 0 {
        return format!(
            "{sign}{primary:0primary_width$}{field_sep}{minutes:02}{field_sep}{seconds:02}"
        );
    }

    format!(
        "{sign}{primary:0primary_width$}{field_sep}{minutes:02}{field_sep}{seconds:02}{fraction_sep}{fraction:0second_decimals$}"
    )
}

#[cfg(test)]
mod tests {
    use super::MvAngle;

    #[test]
    fn right_ascension_time_wraps_to_zero_to_twenty_four_hours() {
        let ra = MvAngle::from_radians(-0.25 * std::f64::consts::TAU).normalized(0.0);
        assert_eq!(ra.format_time(6), "18:00:00.000000");
    }

    #[test]
    fn declination_uses_two_digit_signed_format() {
        let dec = MvAngle::from_radians((-12.5_f64).to_radians());
        assert_eq!(dec.format_angle_dig2(5), "-12.30.00.00000");
    }

    #[test]
    fn longitude_uses_three_digit_signed_format() {
        let lon = MvAngle::from_radians(123.5_f64.to_radians());
        assert_eq!(lon.format_angle(1), "+123.30.00.0");
    }
}
