// SPDX-License-Identifier: LGPL-3.0-or-later
//! Verify that build and runtime JSON parsers agree on catalog numbers.

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
    use super::{matches, number_bits};

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
}
