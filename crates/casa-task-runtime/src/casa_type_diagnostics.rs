// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's recorded direct parameter type refusals.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use casa_provider_contracts::ParameterValue;

/// A message only when the recorded CASA type validator would also reject
/// the value. CASA coerces integers and booleans to floats and accepts
/// booleans as integers; native refusals of those values keep their own text.
pub(crate) fn direct_type_refusal(
    surface: &str,
    name: &str,
    value: &ParameterValue,
) -> Option<String> {
    let kind = direct_type(surface, name)?;
    // JSON integers beyond i64 arrive here as floats. CASA accepts them in
    // cIntVec, so do not claim a refusal when the original spelling is lost.
    if kind == "cIntVec"
        && match value {
            ParameterValue::Float(_) => true,
            ParameterValue::Array(items) => items
                .iter()
                .any(|item| matches!(item, ParameterValue::Float(_))),
            _ => false,
        }
    {
        return None;
    }
    let accepted = match kind {
        "cInt" => matches!(value, ParameterValue::Integer(_) | ParameterValue::Bool(_)),
        "cFloat" => matches!(
            value,
            ParameterValue::Integer(_) | ParameterValue::Float(_) | ParameterValue::Bool(_)
        ),
        "cStr" | "cPath" => matches!(value, ParameterValue::String(_)),
        "cBool" => matches!(value, ParameterValue::Bool(_)),
        "cStrVec" | "cPathVec" => vector_accepts(
            value,
            |item| matches!(item, ParameterValue::String(_)),
            |item| matches!(item, ParameterValue::String(_)),
        ),
        "cIntVec" => vector_accepts(
            value,
            |item| matches!(item, ParameterValue::Integer(_) | ParameterValue::Bool(_)),
            |item| matches!(item, ParameterValue::Integer(_) | ParameterValue::Bool(_)),
        ),
        "cFloatVec" => vector_accepts(
            value,
            |item| {
                matches!(
                    item,
                    ParameterValue::Integer(_) | ParameterValue::Float(_) | ParameterValue::Bool(_)
                )
            },
            |item| matches!(item, ParameterValue::Integer(_) | ParameterValue::Float(_)),
        ),
        "cBoolVec" => vector_accepts(
            value,
            |item| matches!(item, ParameterValue::Bool(_)),
            |item| matches!(item, ParameterValue::Bool(_)),
        ),
        _ => return None,
    };
    (!accepted).then(|| format!("{{'{name}': ['must be of {kind} type']}}"))
}

/// `to_list` turns strings into one-element lists and mappings into their
/// string keys. Other scalars may be promoted by the specific vector coercer.
fn vector_accepts(
    value: &ParameterValue,
    element: impl Fn(&ParameterValue) -> bool,
    scalar: impl Fn(&ParameterValue) -> bool,
) -> bool {
    match value {
        ParameterValue::Array(items) => items.iter().all(element),
        ParameterValue::Table(items) => {
            items.is_empty() || element(&ParameterValue::String(String::new()))
        }
        _ => scalar(value),
    }
}

/// Whether the pinned CASA schema declares a parameter as a single `cInt`.
pub(crate) fn integer_parameter(surface: &str, name: &str) -> bool {
    direct_type(surface, name) == Some("cInt")
}

/// The direct type recorded for this CASA parameter.
fn direct_type(surface: &str, name: &str) -> Option<&'static str> {
    static PARAMETERS: OnceLock<BTreeMap<String, BTreeMap<String, String>>> = OnceLock::new();
    let parameters = PARAMETERS.get_or_init(|| {
        serde_json::from_str(include_str!("../resources/casa-direct-types.json"))
            .expect("the checked CASA direct-type inventory")
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

    use super::{direct_type_refusal, integer_literal_outside_i64};

    #[test]
    fn required_paths_and_union_types_do_not_get_guessed_scalar_errors() {
        let value = ParameterValue::Integer(42);
        assert_eq!(direct_type_refusal("tclean", "vis", &value), None);
        assert_eq!(direct_type_refusal("tclean", "mask", &value), None);
        assert_eq!(direct_type_refusal("calibrate", "vis", &value), None);
    }

    #[test]
    fn vector_coercions_do_not_claim_casa_refused_accepted_values() {
        use ParameterValue::{Array, Bool, Float, Integer, String as Text, Table};
        use std::collections::BTreeMap;

        for (task, name, value) in [
            ("applycal", "gainfield", Text("field".into())),
            (
                "applycal",
                "gainfield",
                Table(BTreeMap::from([("field".into(), Integer(1))])),
            ),
            ("flagdata", "clipminmax", Integer(42)),
            ("flagdata", "clipminmax", Float(42.0)),
            ("flagdata", "clipminmax", Array(vec![Bool(true)])),
            ("flagdata", "clipminmax", Table(BTreeMap::new())),
            ("immoments", "moments", Bool(true)),
            ("applycal", "calwt", Bool(true)),
        ] {
            assert_eq!(
                direct_type_refusal(task, name, &value),
                None,
                "{task}.{name}={value:?}"
            );
        }
        assert_eq!(
            direct_type_refusal("flagdata", "clipminmax", &Bool(true)),
            Some("{'clipminmax': ['must be of cFloatVec type']}".into())
        );
        assert_eq!(
            direct_type_refusal("immoments", "moments", &Float(1.5)),
            None
        );
        assert_eq!(
            direct_type_refusal("immoments", "moments", &Array(vec![Float(1.5)])),
            None
        );
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
