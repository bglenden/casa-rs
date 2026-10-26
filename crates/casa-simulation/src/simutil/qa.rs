// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `qa` calls `simutil.py` makes, as casatools computes them: values
//! read as `casaQuantity` reads strings, casacore's `Quantum` arithmetic,
//! and the strings `qa.formxxx` writes. Also Python's own number formats,
//! where CASA writes a number into a string it reads back or into a file.

use casa_types::quanta::casacore::{self, mvangle_radians};
use casa_types::quanta::{Dimension, Quantity, Unit, UnitDim};

use super::SimutilError;

/// `qa.quantity(text)`: the string read as casacore reads quantities.
pub(crate) fn read(text: &str) -> Result<Quantity, SimutilError> {
    Quantity::read(text).map_err(|_| SimutilError::Quantity(text.to_string()))
}

/// A number with no unit, as `qa` takes a Python number.
pub(crate) fn number(value: f64) -> Quantity {
    Quantity::dimensionless(value)
}

/// `qa.convert(value, unit)['value']`.
pub(crate) fn value_in(value: &Quantity, unit: &str) -> Result<f64, SimutilError> {
    let target = if unit.is_empty() {
        Unit::dimensionless()
    } else {
        casacore::casacore_unit(unit)?
    };
    Ok(casacore::convert(value, Some(&target))?.value())
}

/// `qa.convert(value, unit)`.
pub(crate) fn convert(value: &Quantity, unit: &str) -> Result<Quantity, SimutilError> {
    Ok(casacore::convert(
        value,
        Some(&casacore::casacore_unit(unit)?),
    )?)
}

/// `qa.add`.
pub(crate) fn add(left: &Quantity, right: &Quantity) -> Result<Quantity, SimutilError> {
    Ok(casacore::add(left, right)?)
}

/// `qa.sub`.
pub(crate) fn sub(left: &Quantity, right: &Quantity) -> Result<Quantity, SimutilError> {
    Ok(casacore::sub(left, right)?)
}

/// `qa.mul`.
pub(crate) fn mul(left: &Quantity, right: &Quantity) -> Result<Quantity, SimutilError> {
    Ok(casacore::mul(left, right)?)
}

/// `qa.div`.
pub(crate) fn div(left: &Quantity, right: &Quantity) -> Result<Quantity, SimutilError> {
    Ok(casacore::div(left, right)?)
}

/// `qa.toangle`: an angle as it is, a time as radians at 360 degrees a day.
pub(crate) fn to_angle(value: &Quantity) -> Result<Quantity, SimutilError> {
    if value.unit().val().dim == UnitDim::basis(Dimension::Angle) {
        return Ok(value.clone());
    }
    let radians = mvangle_radians(value)?;
    Ok(Quantity::with_unit(
        radians,
        casacore::casacore_unit("rad")?,
    ))
}

/// `qa.formxxx(value, format='hms', prec=5)`.
pub(crate) fn hms(value: &Quantity) -> Result<String, SimutilError> {
    formxxx(value, "hms")
}

/// `qa.formxxx(value, format='dms', prec=5)`.
pub(crate) fn dms(value: &Quantity) -> Result<String, SimutilError> {
    formxxx(value, "dms")
}

fn formxxx(value: &Quantity, format: &str) -> Result<String, SimutilError> {
    Ok(casacore::formxxx(value, format, 5)?.expect("hms and dms are formats casacore writes"))
}

/// Python's `str(x)` of a float: the shortest digits that read back to
/// `x`, in fixed notation from 1e-4 up to 1e16 (with at least one decimal,
/// `10.0`) and in scientific notation outside it (`1e-05`, `1e+16`).
pub fn python_float(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_string();
    }
    if value.is_infinite() {
        return if value > 0.0 { "inf" } else { "-inf" }.to_string();
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0"
        } else {
            "0.0"
        }
        .to_string();
    }
    // Rust's `{:e}` writes the shortest round-trip digits.
    let scientific = format!("{value:e}");
    let (mantissa, exponent) = scientific.split_once('e').expect("Rust writes an exponent");
    let exponent: i32 = exponent.parse().expect("Rust writes an integer exponent");
    let (sign, mantissa) = mantissa
        .strip_prefix('-')
        .map_or(("", mantissa), |rest| ("-", rest));
    let digits: String = mantissa.chars().filter(|c| *c != '.').collect();
    if !(-4..16).contains(&exponent) {
        let tail = &digits[1..];
        let sign_e = if exponent < 0 { '-' } else { '+' };
        let fraction = if tail.is_empty() {
            String::new()
        } else {
            format!(".{tail}")
        };
        return format!(
            "{sign}{}{fraction}e{sign_e}{:02}",
            &digits[..1],
            exponent.abs()
        );
    }
    let point = exponent + 1;
    let fixed = if point <= 0 {
        format!("0.{}{digits}", "0".repeat(point.unsigned_abs() as usize))
    } else {
        let point = point as usize;
        if digits.len() <= point {
            format!("{digits}{}.0", "0".repeat(point - digits.len()))
        } else {
            format!("{}.{}", &digits[..point], &digits[point..])
        }
    };
    format!("{sign}{fixed}")
}

#[cfg(test)]
mod tests {
    use super::python_float;

    #[test]
    fn floats_print_as_python_prints_them() {
        for (value, written) in [
            (10.0, "10.0"),
            (12.5, "12.5"),
            (1e-05, "1e-05"),
            (0.0001, "0.0001"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1e16, "1e+16"),
            (1e15, "1000000000000000.0"),
            (123_456_789.0, "123456789.0"),
            (-0.5, "-0.5"),
            (-0.0, "-0.0"),
            (1.5e-7, "1.5e-07"),
            (2.5e20, "2.5e+20"),
            (-12.345_678_9, "-12.3456789"),
        ] {
            assert_eq!(python_float(value), written, "{value}");
        }
    }
}
