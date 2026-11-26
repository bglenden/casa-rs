// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA uv-distance grammar and squared bounds in meters.

use std::collections::BTreeMap;

use super::UvUnit;
use super::parser::ddid_to_lambda_map;
use super::syntax::trim_row_id_space;
use crate::{MeasurementSet, MsError, MsResult};

#[derive(Clone, Debug, Default)]
/// Compiled baseline-length constraints for a flag agent, with optional spectral-window scaling.
pub struct UvDistanceSelection {
    meters: Vec<(f64, f64)>,
    by_ddid: BTreeMap<i32, Vec<(f64, f64)>>,
}

impl UvDistanceSelection {
    /// Compile the UV-distance expression against the MeasurementSet’s spectral metadata.
    pub fn parse(ms: &MeasurementSet, text: &str) -> MsResult<Self> {
        let mut selection = Self::default();
        let mut wavelengths = Vec::new();
        for term in text.split(',') {
            let ((lower, upper), unit) = parse_term(term)?;
            match unit {
                UvUnit::Meters(scale) => {
                    selection
                        .meters
                        .push(((lower * scale).powi(2), (upper * scale).powi(2)));
                }
                UvUnit::Lambda(scale) => wavelengths.push((lower * scale, upper * scale)),
            }
        }
        if !wavelengths.is_empty() {
            for (ddid, lambda) in ddid_to_lambda_map(ms)? {
                let square = |value: f64| f64::from(((value * lambda) as f32).powi(2));
                selection.by_ddid.insert(
                    ddid,
                    wavelengths
                        .iter()
                        .map(|&(lower, upper)| (square(lower), square(upper)))
                        .collect(),
                );
            }
        }
        Ok(selection)
    }

    pub(super) fn needs_data_desc_id(&self) -> bool {
        !self.by_ddid.is_empty()
    }

    /// Test whether this baseline and data-description ID satisfy the compiled UV selection.
    pub fn holds(&self, uvw: [f64; 3], ddid: Option<i32>) -> bool {
        let distance = uvw[0].powi(2) + uvw[1].powi(2);
        let matches = |&(lower, upper): &(f64, f64)| (lower..=upper).contains(&distance);
        self.meters.iter().any(matches)
            || ddid
                .and_then(|ddid| self.by_ddid.get(&ddid))
                .is_some_and(|bounds| bounds.iter().any(matches))
    }

    pub(super) fn to_taql(&self) -> String {
        let mut terms = self
            .meters
            .iter()
            .filter_map(distance_taql)
            .collect::<Vec<_>>();
        for (ddid, bounds) in &self.by_ddid {
            let distances = bounds.iter().filter_map(distance_taql).collect::<Vec<_>>();
            if !distances.is_empty() {
                terms.push(format!(
                    "(DATA_DESC_ID=={ddid} AND ({}))",
                    distances.join(" OR ")
                ));
            }
        }
        if terms.is_empty() {
            "FALSE".into()
        } else {
            terms.join(" OR ")
        }
    }
}

fn distance_taql(&(lower, upper): &(f64, f64)) -> Option<String> {
    const DISTANCE: &str = "(SQUARE(UVW[1]) + SQUARE(UVW[2]))";
    if !lower.is_finite() || upper.is_nan() || lower > upper {
        return None;
    }
    Some(if upper.is_infinite() {
        format!("{DISTANCE}>={lower:e}")
    } else {
        format!("({DISTANCE}>={lower:e} AND {DISTANCE}<={upper:e})")
    })
}

fn parse_term(text: &str) -> MsResult<((f64, f64), UvUnit)> {
    let mut parts = text.split(':');
    let expression = trim_row_id_space(parts.next().unwrap_or_default());
    let (lower, upper, unit) = if let Some(text) = expression.strip_prefix('<') {
        let (upper, unit) = number_with_unit(text)?;
        (0.0, upper, unit)
    } else if let Some(text) = expression.strip_prefix('>') {
        let (lower, unit) = number_with_unit(text)?;
        (lower + 1e-10, f64::from(f32::MAX), unit)
    } else if let Some((left, right)) = expression.split_once('~') {
        let (lower, rest) = number(left)?;
        if !rest.is_empty() {
            return Err(invalid_uv("~"));
        }
        let (upper, unit) = number_with_unit(right)?;
        (lower, upper, unit)
    } else {
        let (lower, unit) = number_with_unit(expression)?;
        // In MSUvDistGram's single-value production, the second slot
        // of the bound pair is zero. Keep both slots for percent forms.
        (lower, 0.0, unit)
    };
    let mut bounds = (lower, upper);
    for part in parts {
        let (percent, rest) = number(part)?;
        if rest != "%" {
            return Err(invalid_uv(rest));
        }
        bounds.0 *= 1.0 - percent * 0.01;
        bounds.1 *= 1.0 + percent * 0.01;
    }
    Ok((bounds, unit))
}

fn number_with_unit(text: &str) -> MsResult<(f64, UvUnit)> {
    let (value, token) = number(text)?;
    let unit = match token {
        "" | "m" | "M" => UvUnit::Meters(1.0),
        "km" | "Km" => UvUnit::Meters(1_000.0),
        "lambda" | "LAMBDA" => UvUnit::Lambda(1.0),
        "klambda" | "Klambda" | "kLAMBDA" | "KLAMBDA" => UvUnit::Lambda(1_000.0),
        "Mlambda" | "MLAMBDA" => UvUnit::Lambda(1_000_000.0),
        "k" | "K" | "kM" | "KM" | "kLambda" | "KLambda" => {
            return Err(MsError::InvalidInput(
                "Unrecognized units k found. Possible units are [KM]LAMBDA for wavelengths or [K]M for distance.".into(),
            ));
        }
        _ => return Err(invalid_uv(token)),
    };
    Ok((value, unit))
}

fn number(text: &str) -> MsResult<(f64, &str)> {
    let text = trim_row_id_space(text);
    let digits = |data: &[u8]| data.iter().take_while(|byte| byte.is_ascii_digit()).count();
    let bytes = text.as_bytes();
    let mut end = digits(bytes);
    if end > 0 && bytes.get(end) == Some(&b'.') {
        end += 1 + digits(&bytes[end + 1..]);
    }
    let mut numeric_end = end;
    if let Some(kind @ (b'd' | b'D' | b'e' | b'E')) = bytes.get(end).copied() {
        let start = end + 1 + usize::from(matches!(bytes.get(end + 1), Some(b'+' | b'-')));
        let count = digits(&bytes[start..]);
        if count > 0 {
            end = start + count;
            // The lexer accepts D exponents and converts through atof.
            if numeric_end > 0 && matches!(kind, b'e' | b'E') {
                numeric_end = end;
            }
        }
    }
    let value = if end == 0 {
        return Err(invalid_uv(
            &text[..text.chars().next().map_or(0, char::len_utf8)],
        ));
    } else if numeric_end == 0 {
        // An exponent without a mantissa is accepted by the lexer and
        // converted to zero by atof.
        0.0
    } else {
        text[..numeric_end].parse().map_err(|_| invalid_uv(text))?
    };
    Ok((value, trim_row_id_space(&text[end..])))
}

fn invalid_uv(token: &str) -> MsError {
    MsError::InvalidInput(format!(
        "UV Distribution Expression: Parse error at or near {token:?}"
    ))
}
