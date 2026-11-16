// SPDX-License-Identifier: LGPL-3.0-or-later
//! Verify that build and runtime JSON parsers agree on catalog numbers.

use std::collections::BTreeSet;

/// One JSON number and the representations produced by the parser.
pub type JsonNumberFingerprint = (String, u64, u64, u64);

/// Fingerprint the bundled catalog and a generated provider document.
///
/// Build scripts can embed the result to check whether the target uses an
/// equivalent JSON parser before trusting host-generated metadata.
#[doc(hidden)]
pub fn builtin_provider_fingerprint(document: &str) -> Option<Vec<JsonNumberFingerprint>> {
    let catalog = include_str!("../resources/parameter-catalog.json");
    let surfaces = include_str!("../resources/parameter-surfaces.json");
    number_tokens(&[catalog, surfaces, document])
        .into_iter()
        .map(|token| {
            let (direct, value, casa) = number_bits(&token)?;
            Some((token, direct, value, casa))
        })
        .collect()
}

/// Check an embedded build-host fingerprint against this target's parser.
#[doc(hidden)]
pub fn builtin_provider_parser_matches(probes: &[(&str, u64, u64, u64)]) -> bool {
    matches(probes)
}

/// Verify that parsing and reserializing a generated provider is lossless.
#[doc(hidden)]
pub fn provider_document_roundtrips(document: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(document)
        .and_then(|value| serde_json::to_string(&value))
        .is_ok_and(|restored| restored == document)
}

/// Collect JSON number tokens, ignoring quoted text and preserving spelling.
pub(crate) fn number_tokens(inputs: &[&str]) -> BTreeSet<String> {
    let mut numbers = BTreeSet::new();
    for input in inputs {
        let bytes = input.as_bytes();
        let mut index = 0;
        while index < bytes.len() {
            match bytes[index] {
                b'"' => {
                    index += 1;
                    while index < bytes.len() {
                        match bytes[index] {
                            b'\\' => index += 2,
                            b'"' => {
                                index += 1;
                                break;
                            }
                            _ => index += 1,
                        }
                    }
                }
                b'-' | b'0'..=b'9' => {
                    let start = index;
                    index += 1;
                    while index < bytes.len()
                        && matches!(bytes[index], b'0'..=b'9' | b'.' | b'e' | b'E' | b'+' | b'-')
                    {
                        index += 1;
                    }
                    numbers.insert(input[start..index].to_owned());
                }
                _ => index += 1,
            }
        }
    }
    numbers
}

pub(crate) fn number_bits(token: &str) -> Option<(u64, u64, u64)> {
    let direct: f64 = serde_json::from_str(token).ok()?;
    let value: serde_json::Value = serde_json::from_str(token).ok()?;
    let casa: crate::CasaFloat = serde_json::from_str(token).ok()?;
    Some((
        direct.to_bits(),
        value.as_f64()?.to_bits(),
        casa.0.to_bits(),
    ))
}

pub(crate) fn matches(probes: &[(&str, u64, u64, u64)]) -> bool {
    probes
        .iter()
        .all(|(token, direct, value, casa)| number_bits(token) == Some((*direct, *value, *casa)))
}

#[cfg(test)]
mod tests {
    use super::{builtin_provider_fingerprint, matches, number_bits, provider_document_roundtrips};

    #[test]
    fn different_parser_results_require_runtime_validation() {
        let token = "1.0000000000000002";
        let (direct, value, casa) = number_bits(token).unwrap();
        assert!(matches(&[(token, direct, value, casa)]));
        assert!(!matches(&[(token, direct ^ 1, value, casa)]));
        assert!(!matches(&[(token, direct, value ^ 1, casa)]));
        assert!(!matches(&[(token, direct, value, casa ^ 1)]));
    }

    #[test]
    fn detects_a_number_affected_by_float_roundtrip() {
        // This decimal rounds differently with serde_json's default parser
        // and its float_roundtrip feature. This test crate enables the latter.
        let token = "2.56742423927834415e219";
        let (direct, value, casa) = number_bits(token).unwrap();
        assert_eq!(direct, 0x6d7d_17ac_e8d6_e1c8);
        assert!(!matches(&[(token, 0x6d7d_17ac_e8d6_e1c7, value, casa)]));
    }

    #[test]
    fn generated_documents_must_round_trip_without_changing_numbers() {
        assert!(provider_document_roundtrips(r#"{"value":1.0}"#));
        assert!(!provider_document_roundtrips(r#"{"value":1e0}"#));
        assert!(!provider_document_roundtrips("not json"));
        let probes = builtin_provider_fingerprint(r#"{"value":2.56742423927834415e219}"#).unwrap();
        assert!(
            probes
                .iter()
                .any(|(token, _, _, _)| token == "2.56742423927834415e219")
        );
    }
}
