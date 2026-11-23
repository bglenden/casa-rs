// SPDX-License-Identifier: LGPL-3.0-or-later
//! MSStateGram / MSStateIndex intent expressions over STATE rows.

use super::antenna_expr::regex::FieldRegex;
use crate::{MsError, MsResult};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MatchMode {
    Task,
    FlagData,
    Taql,
}

pub(super) fn resolve(text: &str, rows: &[(String, bool)], mode: MatchMode) -> MsResult<Vec<i32>> {
    let casa_lexer = mode != MatchMode::Task;
    let text = if casa_lexer { lexer_prefix(text) } else { text };
    let text = if let Some(inner) = text.strip_prefix('(') {
        inner.strip_suffix(')').ok_or_else(|| invalid(text))?
    } else {
        text
    };
    let mut selected = vec![false; rows.len()];
    for term in split(text, ',', casa_lexer)? {
        let mut matching = vec![true; rows.len()];
        let atoms = split(term, '&', casa_lexer)?;
        let bounded = atoms.len() == 2 && atoms[0].starts_with('>') && atoms[1].starts_with('<');
        if atoms.len() > 1
            && !bounded
            && atoms
                .iter()
                .any(|atom| atom.starts_with(['<', '>']) || numeric_reference(atom))
        {
            return Err(invalid(term));
        }
        for &atom in &atoms {
            let mask = atom_mask(atom, rows, casa_lexer)?;
            if !mask.iter().any(|value| *value) {
                if mode == MatchMode::FlagData && !bounded {
                    warn_unmatched_atom(atom);
                } else if mode == MatchMode::Task && !numeric_reference(atom) {
                    return Err(MsError::VersionError(format!(
                        "intent selector {atom:?} did not match any STATE rows"
                    )));
                }
            }
            for (value, included) in matching.iter_mut().zip(mask) {
                *value &= included;
            }
        }
        // MSStateGram checks combined bounds before adding other union terms;
        // numeric ID lists alone may ignore unavailable IDs.
        if bounded && !matching.iter().any(|value| *value) {
            if mode == MatchMode::FlagData {
                let lower = atoms[0][1..].trim();
                let upper = atoms[1][1..].trim();
                tracing::warn!("State Expression: No state found in the range [{lower},{upper}]");
            } else if mode == MatchMode::Task {
                return Err(MsError::VersionError(format!(
                    "intent bounds {term:?} did not match any STATE rows"
                )));
            }
        }
        for (value, included) in selected.iter_mut().zip(matching) {
            *value |= included;
        }
    }
    if mode != MatchMode::Taql && !selected.iter().any(|value| *value) {
        if mode == MatchMode::FlagData {
            return Err(MsError::VersionError(format!(
                "No match found for state expression: {text}"
            )));
        }
        return Err(MsError::VersionError(format!(
            "intent selector {text:?} did not match any STATE rows"
        )));
    }
    Ok(selected
        .into_iter()
        .enumerate()
        .filter_map(|(index, selected)| selected.then_some(index as i32))
        .collect())
}

fn warn_unmatched_atom(atom: &str) {
    if let Some(bound @ ('<' | '>')) = atom.chars().next() {
        let number = atom[1..].trim();
        tracing::warn!("State Expression: No state ID found {bound}{number}");
    } else if numeric_reference(atom) {
        tracing::warn!(
            "State Expression: Partial or no match for State ID list [{}]",
            atom.trim()
        );
    } else {
        // Keep punctuation after the quoted selector for the task logger.
        tracing::warn!("State Expression: No match found for {atom:?}.");
    }
}

/// The STATE lexer accepts UTF-8 bytes inside quoted patterns and regexes.
fn lexer_prefix(text: &str) -> &str {
    let mut delimiter = None;
    for (index, character) in text.char_indices() {
        match delimiter {
            Some(close) if character == close => delimiter = None,
            None if matches!(character, '"' | '/') => delimiter = Some(character),
            None if !character.is_ascii() => return &text[..index],
            _ => {}
        }
    }
    text
}

/// List and intersection separators inside quoted patterns/regexes are data.
fn split(text: &str, separator: char, flagdata: bool) -> MsResult<Vec<&str>> {
    // The flagdata lexer treats a space as a literal STATE identifier.
    let missing = |part: &str| part.trim().is_empty() && !(flagdata && part.contains(' '));
    let (mut delimiter, mut start) = (None, 0);
    let mut parts = Vec::new();
    for (index, character) in text.char_indices() {
        match delimiter {
            Some(close) if character == close => delimiter = None,
            None if matches!(character, '"' | '/') => delimiter = Some(character),
            None if character == separator => {
                let part = &text[start..index];
                if missing(part) {
                    return Err(invalid(text));
                }
                parts.push(part);
                start = index + character.len_utf8();
            }
            _ => {}
        }
    }
    let last = &text[start..];
    if delimiter.is_some() || missing(last) {
        return Err(invalid(text));
    }
    parts.push(last);
    Ok(parts)
}

fn atom_mask(text: &str, rows: &[(String, bool)], flagdata: bool) -> MsResult<Vec<bool>> {
    let decimal = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    let number = |text: &str| -> MsResult<i64> {
        if !decimal(text.trim()) {
            return Err(invalid(text));
        }
        text.trim().parse().map_err(|_| invalid(text))
    };
    if let Some((first, last)) = text.split_once('~')
        && decimal(first.trim())
    {
        let (first, last) = (number(first)?, number(last)?);
        if first > last {
            return Err(invalid(text));
        }
        // Enumerate the table, never the potentially enormous user range.
        return Ok((0..rows.len())
            .map(|index| first <= index as i64 && index as i64 <= last)
            .collect());
    }
    if let Some(bound) = text.strip_prefix('<') {
        let bound = number(bound)?;
        return Ok(rows
            .iter()
            .enumerate()
            .map(|(index, (_, flagged))| !*flagged && (index as i64) < bound)
            .collect());
    }
    if let Some(bound) = text.strip_prefix('>') {
        let bound = number(bound)?;
        return Ok(rows
            .iter()
            .enumerate()
            .map(|(index, (_, flagged))| !*flagged && index as i64 > bound)
            .collect());
    }
    if decimal(text.trim()) {
        let id = number(text)?;
        return Ok((0..rows.len()).map(|index| index as i64 == id).collect());
    }
    let matcher = if let Some(delimiter @ ('"' | '/')) = text.chars().next() {
        let body = text
            .strip_prefix(delimiter)
            .and_then(|text| text.strip_suffix(delimiter))
            .filter(|text| !text.contains(delimiter))
            .ok_or_else(|| invalid(text))?;
        // MSStateGram's QS/RS default rules consume backslashes/newlines.
        let body = body.replace(['\\', '\n'], "");
        Some(FieldRegex::compile(&body, delimiter == '/').map_err(MsError::InvalidInput)?)
    } else if text.bytes().all(name_byte)
        || (flagdata && text.contains(' ') && text.bytes().all(|byte| matches!(byte, b' ' | b'\t')))
    {
        None
    } else {
        if !text
            .bytes()
            .all(|byte| byte != b'.' && (name_byte(byte) || b"$'*?".contains(&byte)))
        {
            return Err(invalid(text));
        }
        Some(FieldRegex::compile(text.trim_matches(' '), false).map_err(MsError::InvalidInput)?)
    };
    rows.iter()
        .map(|(mode, flagged)| {
            if *flagged {
                return Ok(false);
            }
            let mode = if matcher.is_some() {
                mode.trim_matches(' ')
            } else {
                mode.as_str()
            };
            let mut components = mode.split(',').peekable();
            while let Some(component) = components.next() {
                // CASA's getline loop yields no component for an empty mode
                // or the empty suffix after a final comma.
                if flagdata && component.is_empty() && components.peek().is_none() {
                    break;
                }
                if let Some(matcher) = &matcher {
                    if matcher.matches(component).map_err(|error| {
                        MsError::InvalidInput(format!("invalid intent regex: {error}"))
                    })? {
                        return Ok(true);
                    }
                } else if component == text {
                    return Ok(true);
                }
            }
            Ok(false)
        })
        .collect()
}

fn name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || b"_{}+-.=;@#%:! ".contains(&byte)
}

fn numeric_reference(text: &str) -> bool {
    let decimal = |text: &str| !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit());
    decimal(text.trim())
        || text
            .split_once('~')
            .is_some_and(|(first, _)| decimal(first.trim()))
}

fn invalid(text: &str) -> MsError {
    MsError::InvalidInput(format!("invalid STATE intent expression {text:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_grammar_matches_components_masks_flags_and_bounds_numeric_ranges() {
        let rows = vec![
            (
                "CALIBRATE_PHASE#ON_SOURCE,CALIBRATE_WVR#ON_SOURCE".into(),
                false,
            ),
            ("OBSERVE_TARGET#ON_SOURCE".into(), false),
            ("CALIBRATE_PHASE#ON_SOURCE".into(), true),
        ];
        for text in [
            "CALIBRATE_PHASE#ON_SOURCE",
            "*PHASE*",
            "\"CALIBRATE_{PHASE,WVR}*\"",
            "/CALIBRATE_(PHASE|WVR)#ON_SOURCE/",
            "CALIBRATE_PHASE*&*WVR*",
        ] {
            assert_eq!(
                resolve(text, &rows, MatchMode::Task).unwrap(),
                [0],
                "{text}"
            );
        }
        for (text, expected) in [
            ("2", vec![2]),
            ("0~2147483647", vec![0, 1, 2]),
            (">0&<2", vec![1]),
            ("(0,1)", vec![0, 1]),
            ("0,999", vec![0]),
            ("<3", vec![0, 1]),
            (">0", vec![1]),
        ] {
            assert_eq!(
                resolve(text, &rows, MatchMode::Task).unwrap(),
                expected,
                "{text}"
            );
        }
        for text in [
            "NO_SUCH_INTENT",
            "0~",
            "0~1,",
            "\"unclosed",
            "999",
            "CALIBRATE_PHASE#ON_SOURCE, OBSERVE_TARGET#ON_SOURCE",
        ] {
            assert!(resolve(text, &rows, MatchMode::Task).is_err(), "{text}");
        }
    }

    #[test]
    fn empty_combined_bound_rejects_even_with_a_matching_union_member() {
        let rows = vec![("A".into(), false), ("B".into(), false)];
        assert!(resolve(">0&<1,0", &rows, MatchMode::Task).is_err());
        assert_eq!(resolve(">0&<2,0", &rows, MatchMode::Task).unwrap(), [0, 1]);
    }
}
