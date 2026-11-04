// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's recorded single-type integer parameter refusals.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use casa_provider_contracts::ParameterValue;

/// Message for a value CASA's `cInt` validator refuses, where the pinned
/// task schema has that single type. Python booleans count as integers in
/// CASA; casa-rs currently refuses them, so they retain its own diagnostic.
pub(crate) fn integer_refusal(surface: &str, name: &str, value: &ParameterValue) -> Option<String> {
    if matches!(value, ParameterValue::Integer(_) | ParameterValue::Bool(_)) {
        return None;
    }
    integer_parameter(surface, name).then(|| format!("{{'{name}': ['must be of cInt type']}}"))
}

/// Whether the pinned CASA schema declares a parameter as a single `cInt`.
pub(crate) fn integer_parameter(surface: &str, name: &str) -> bool {
    static PARAMETERS: OnceLock<BTreeMap<String, Vec<String>>> = OnceLock::new();
    let parameters = PARAMETERS.get_or_init(|| {
        serde_json::from_str(include_str!("../resources/casa-integer-parameters.json"))
            .expect("the checked CASA integer-parameter inventory")
    });
    parameters
        .get(surface)
        .is_some_and(|names| names.iter().any(|item| item == name))
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
    use super::integer_literal_outside_i64;

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
