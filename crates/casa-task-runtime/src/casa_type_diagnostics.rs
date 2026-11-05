// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's recorded direct scalar parameter refusals.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use casa_provider_contracts::ParameterValue;

/// A message only when the recorded CASA scalar validator would also reject
/// the value. CASA coerces integers and booleans to floats and accepts
/// booleans as integers; native refusals of those values keep their own text.
pub(crate) fn scalar_refusal(surface: &str, name: &str, value: &ParameterValue) -> Option<String> {
    let kind = scalar_type(surface, name)?;
    let accepted = match kind {
        "cInt" => matches!(value, ParameterValue::Integer(_) | ParameterValue::Bool(_)),
        "cFloat" => matches!(
            value,
            ParameterValue::Integer(_) | ParameterValue::Float(_) | ParameterValue::Bool(_)
        ),
        "cStr" | "cPath" => matches!(value, ParameterValue::String(_)),
        "cBool" => matches!(value, ParameterValue::Bool(_)),
        _ => return None,
    };
    (!accepted).then(|| format!("{{'{name}': ['must be of {kind} type']}}"))
}

/// Whether the pinned CASA schema declares a parameter as a single `cInt`.
pub(crate) fn integer_parameter(surface: &str, name: &str) -> bool {
    scalar_type(surface, name) == Some("cInt")
}

/// The direct scalar type recorded for this CASA parameter.
fn scalar_type(surface: &str, name: &str) -> Option<&'static str> {
    static PARAMETERS: OnceLock<BTreeMap<String, BTreeMap<String, String>>> = OnceLock::new();
    let parameters = PARAMETERS.get_or_init(|| {
        serde_json::from_str(include_str!("../resources/casa-scalar-types.json"))
            .expect("the checked CASA scalar-parameter inventory")
    });
    parameters.get(surface)?.get(name).map(String::as_str)
}

/// A JSON integer literal that cannot fit the native signed representation.
/// Decimal and exponent forms are floats in CASA even when mathematically whole.
pub(crate) fn integer_literal_outside_i64(raw: &str) -> bool {
    let digits = raw.strip_prefix('-').unwrap_or(raw);
    !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && raw.parse::<i64>().is_err()
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::ParameterValue;

    use super::{integer_literal_outside_i64, scalar_refusal};

    #[test]
    fn required_paths_and_union_types_do_not_get_guessed_scalar_errors() {
        let value = ParameterValue::Integer(42);
        assert_eq!(scalar_refusal("tclean", "vis", &value), None);
        assert_eq!(scalar_refusal("tclean", "mask", &value), None);
        assert_eq!(scalar_refusal("calibrate", "vis", &value), None);
    }

    #[test]
    fn json_integer_bounds_are_distinct_from_float_spellings() {
        for valid in [
            "9223372036854775807",
            "-9223372036854775808",
            "9.223372036854776e18",
        ] {
            assert!(!integer_literal_outside_i64(valid), "{valid}");
        }
        for overflow in ["9223372036854775808", "-9223372036854775809"] {
            assert!(integer_literal_outside_i64(overflow), "{overflow}");
        }
    }
}
