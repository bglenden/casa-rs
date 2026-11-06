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

/// A task with one direct required-path validator in its recorded schema.
pub(crate) fn single_required_path(surface: &str, name: &str) -> bool {
    static PARAMETERS: OnceLock<BTreeMap<String, String>> = OnceLock::new();
    let parameters = PARAMETERS.get_or_init(|| {
        serde_json::from_str(include_str!("../resources/casa-single-required-paths.json"))
            .expect("the checked CASA single-required-path inventory")
    });
    parameters.get(surface).is_some_and(|value| value == name)
}

/// A JSON integer literal that cannot fit the native signed representation.
/// Decimal and exponent forms are floats in CASA even when mathematically whole.
pub(crate) fn integer_literal_outside_i64(raw: &str) -> bool {
    let digits = raw.strip_prefix('-').unwrap_or(raw);
    !digits.is_empty()
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && raw.parse::<i64>().is_err()
}

/// Decode a variant from its JSON spelling before integer type information is lost.
pub(crate) fn parse_variant_json(raw: &str) -> Result<ParameterValue, String> {
    use serde_json::value::RawValue;
    let raw = raw.trim();
    match raw.as_bytes().first() {
        Some(b'[') => serde_json::from_str::<Vec<&RawValue>>(raw)
            .map_err(|error| error.to_string())?
            .iter()
            .map(|value| parse_variant_json(value.get()))
            .collect::<Result<Vec<_>, _>>()
            .map(ParameterValue::Array),
        Some(b'{') => serde_json::from_str::<BTreeMap<String, &RawValue>>(raw)
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|(key, value)| Ok((key, parse_variant_json(value.get())?)))
            .collect::<Result<BTreeMap<_, _>, String>>()
            .map(ParameterValue::Table),
        Some(b'"') => serde_json::from_str(raw)
            .map(ParameterValue::String)
            .map_err(|error| format!("invalid string: {error}")),
        Some(b'-' | b'0'..=b'9')
            if raw
                .strip_prefix('-')
                .unwrap_or(raw)
                .bytes()
                .all(|ch| ch.is_ascii_digit()) =>
        {
            raw.parse::<i64>()
                .map(ParameterValue::Integer)
                .map_err(|_| "integer is outside casa-rs's signed 64-bit range".into())
        }
        Some(b'-' | b'0'..=b'9') => {
            let number = raw.parse::<f64>().map_err(|error| error.to_string())?;
            if !number.is_finite() {
                return Err("non-finite numbers are forbidden".into());
            }
            Ok(ParameterValue::Float(number))
        }
        Some(b't') if raw == "true" => Ok(ParameterValue::Bool(true)),
        Some(b'f') if raw == "false" => Ok(ParameterValue::Bool(false)),
        _ => Err("null is not a parameter value".into()),
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::ParameterValue;

    use super::{direct_type_refusal, integer_literal_outside_i64, single_required_path};

    #[test]
    fn variant_json_keeps_integer_negative_zero_and_rejects_nested_overflow() {
        let value = super::parse_variant_json("[0, -0, -0.0]").unwrap();
        assert_eq!(
            value,
            ParameterValue::Array(vec![
                ParameterValue::Integer(0),
                ParameterValue::Integer(0),
                ParameterValue::Float(-0.0)
            ])
        );
        assert!(
            super::parse_variant_json("{\"nested\":[9223372036854775808]}")
                .unwrap_err()
                .contains("signed 64-bit")
        );
    }

    #[test]
    fn required_paths_and_union_types_do_not_get_guessed_scalar_errors() {
        let value = ParameterValue::Integer(42);
        assert_eq!(direct_type_refusal("tclean", "vis", &value), None);
        assert_eq!(direct_type_refusal("tclean", "mask", &value), None);
        assert_eq!(direct_type_refusal("calibrate", "vis", &value), None);
    }

    #[test]
    fn only_tasks_with_one_direct_required_path_are_selected() {
        assert!(single_required_path("imstat", "imagename"));
        assert!(single_required_path("split", "vis"));
        assert!(!single_required_path("feather", "highres"));
        assert!(!single_required_path("feather", "lowres"));
        assert!(!single_required_path("tclean", "vis"));
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
