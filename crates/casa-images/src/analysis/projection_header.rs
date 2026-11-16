// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's FITS-like projection header string.

use casa_coordinates::{Coordinate, DirectionCoordinate, ProjectionType};

/// `ImageMetaDataBase::_getProjection` names slant SIN's parameters and
/// recognizes its special NCP form; other projections use their name.
pub(super) fn projection_header(direction: &DirectionCoordinate) -> String {
    let projection = direction.projection();
    let parameters = projection.parameters();
    if projection.projection_type() != ProjectionType::SIN
        || parameters.len() != 2
        || parameters.iter().all(|&value| value == 0.0)
    {
        return projection.name().to_string();
    }
    let mut result = format!(
        "SIN ([{}, {}])",
        cpp_number(parameters[0]),
        cpp_number(parameters[1])
    );
    let declination = direction.reference_value()[1];
    let ncp_parameter = 1.0 / declination.tan();
    if parameters[0] == 0.0
        && declination != 0.0
        && (parameters[1] - ncp_parameter).abs()
            <= 1e-13 * parameters[1].abs().max(ncp_parameter.abs())
    {
        result.push_str(": NCP");
    }
    result
}

/// C++'s default stream precision is six significant digits (`defaultfloat`).
fn cpp_number(value: f64) -> String {
    cpp_number_precision(value, 6)
}

/// C++ defaultfloat with the supplied significant-digit precision.
pub(super) fn cpp_number_precision(value: f64, precision: usize) -> String {
    if value.is_nan() {
        return if value.is_sign_negative() {
            "-nan"
        } else {
            "nan"
        }
        .into();
    }
    if value.is_infinite() {
        return if value.is_sign_negative() {
            "-inf"
        } else {
            "inf"
        }
        .into();
    }
    let places = precision.saturating_sub(1);
    let scientific = format!("{value:.places$e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("scientific format");
    let exponent: i32 = exponent.parse().expect("scientific exponent");
    if exponent < -4 || exponent >= i32::try_from(precision).unwrap_or(i32::MAX) {
        let mantissa = mantissa.trim_end_matches('0').trim_end_matches('.');
        format!(
            "{mantissa}e{}{number:02}",
            if exponent < 0 { '-' } else { '+' },
            number = exponent.abs()
        )
    } else {
        let places = usize::try_from(i32::try_from(places).unwrap_or(i32::MAX) - exponent)
            .expect("nonnegative precision");
        let fixed = format!("{value:.places$}");
        if fixed.contains('.') {
            fixed
                .trim_end_matches('0')
                .trim_end_matches('.')
                .to_string()
        } else {
            fixed
        }
    }
}
