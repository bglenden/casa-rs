// SPDX-License-Identifier: LGPL-3.0-or-later
//! Reusable CASA-style selector parsing for MeasurementSet-facing APIs.
//!
//! CASA keeps generic selector parsing near `MSSelection`, while spectral cube
//! regridding and axis construction live lower in `SubMS` /
//! `MSTransformRegridder`. This module covers the former: reusable text
//! parsing for numeric-id selectors and SPW+channel selectors.

use crate::error::{MsError, MsResult};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One `spw[:channel-selection]` clause from a CASA spectral-window selector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SpwSelector {
    /// Spectral-window id.
    pub spw_id: i32,
    /// Optional channel-selection clause following `:`.
    pub channels: Option<ChannelSelection>,
}

/// Parsed CASA channel selector such as `4~9;12~14` or `0~10^2`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChannelSelection {
    /// Ordered selector segments.
    pub segments: Vec<ChannelSelectionSegment>,
}

/// One contiguous channel-selection segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ChannelSelectionSegment {
    /// Inclusive starting channel.
    pub start: i32,
    /// Inclusive ending channel.
    pub end: i32,
    /// Positive stride between selected channels.
    pub stride: usize,
}

impl ChannelSelection {
    /// Expand the selection into concrete channel indices for a spectral window
    /// with `total_channels` channels.
    pub fn indices(&self, total_channels: usize) -> MsResult<Vec<usize>> {
        let mut indices = Vec::<usize>::new();
        for segment in &self.segments {
            if segment.stride == 0 {
                return Err(MsError::VersionError(
                    "channel selector stride must be positive".to_string(),
                ));
            }
            if segment.start < 0 || segment.end < 0 {
                return Err(MsError::VersionError(format!(
                    "channel selector segment {:?} uses a negative channel index",
                    segment
                )));
            }

            let stride = segment.stride as i32;
            let mut current = segment.start;
            let step = if segment.start <= segment.end {
                stride
            } else {
                -stride
            };
            loop {
                let index = usize::try_from(current).map_err(|_| {
                    MsError::VersionError(format!(
                        "channel selector segment {:?} uses an invalid channel index",
                        segment
                    ))
                })?;
                if index >= total_channels {
                    return Err(MsError::VersionError(format!(
                        "channel selector segment {:?} exceeds spectral window with {total_channels} channels",
                        segment
                    )));
                }
                if !indices.contains(&index) {
                    indices.push(index);
                }
                if current == segment.end {
                    break;
                }
                current += step;
                if step > 0 && current > segment.end {
                    break;
                }
                if step < 0 && current < segment.end {
                    break;
                }
            }
        }
        Ok(indices)
    }
}

/// Parse a CASA-style numeric id selector such as `0,2~4`.
pub fn parse_numeric_id_selector(value: &str, label: &str) -> MsResult<Vec<i32>> {
    Ok(parse_numeric_id_ranges(value, label)?
        .into_iter()
        .flat_map(|(start, end)| start..=end)
        .collect())
}

/// Raw numeric terms preserve the distinction between a literal and a range,
/// including endpoint order, until each selector applies its range semantics.
enum NumericIdTerm {
    Id(i32),
    Range(i32, i32),
}

/// Legacy APIs permit empty list terms and Rust's explicit positive sign;
/// the scan/observation grammar preserves CASA's stricter lexical rules.
#[derive(Clone, Copy)]
enum NumericIdSyntax {
    Legacy,
    Row,
}

fn parse_numeric_id_terms(
    value: &str,
    label: &str,
    syntax: NumericIdSyntax,
) -> MsResult<Vec<NumericIdTerm>> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| matches!(syntax, NumericIdSyntax::Row) || !part.is_empty())
        .map(|part| {
            if matches!(syntax, NumericIdSyntax::Row) && (part.is_empty() || part.contains('+')) {
                return Err(MsError::InvalidInput(format!(
                    "{label} selector {part:?} is not a supported numeric id or range"
                )));
            }
            if let Some((start, end)) = parse_numeric_range_endpoints(part) {
                return Ok(NumericIdTerm::Range(start, end));
            }
            part.parse::<i32>().map(NumericIdTerm::Id).map_err(|_| {
                MsError::VersionError(format!(
                    "{label} selector {part:?} is not a supported numeric id or range"
                ))
            })
        })
        .collect()
}

/// Legacy numeric selectors normalize range direction before expansion.
fn parse_numeric_id_ranges(value: &str, label: &str) -> MsResult<Vec<(i32, i32)>> {
    let ranges = parse_numeric_id_terms(value, label, NumericIdSyntax::Legacy)?
        .into_iter()
        .map(|term| match term {
            NumericIdTerm::Id(id) => (id, id),
            NumericIdTerm::Range(start, end) => (start.min(end), start.max(end)),
        })
        .collect();
    Ok(canonical_numeric_ranges(ranges))
}

/// Parse the numeric scan/observation subset into compact inclusive intervals.
/// CASA's MSScanParse and MSObservationParse reject negative, equal and reversed
/// range bounds; a single nonnegative literal remains valid. Storage is bounded
/// by the expression's term count, independently of interval width.
///
/// # Errors
///
/// An empty term, an explicit `+` sign, a term that is not a number or
/// `a~b` range, a negative ID, or range bounds that are equal or reversed.
pub fn parse_numeric_row_id_ranges(value: &str, label: &str) -> MsResult<Vec<(i32, i32)>> {
    let ranges = parse_numeric_id_terms(value, label, NumericIdSyntax::Row)?
        .into_iter()
        .map(|term| match term {
            NumericIdTerm::Id(id) if id >= 0 => Ok((id, id)),
            NumericIdTerm::Range(start, end) if start >= 0 && end > start => Ok((start, end)),
            _ => Err(MsError::InvalidInput(format!(
                "{label} selector requires nonnegative IDs and strictly increasing range bounds"
            ))),
        })
        .collect::<MsResult<Vec<_>>>()?;
    Ok(canonical_numeric_ranges(ranges))
}

pub(super) fn canonical_numeric_ranges(mut values: Vec<(i32, i32)>) -> Vec<(i32, i32)> {
    values.sort_unstable();
    let mut count = 0;
    for index in 0..values.len() {
        let (start, end) = values[index];
        if count > 0 && start <= values[count - 1].1.saturating_add(1) {
            values[count - 1].1 = values[count - 1].1.max(end);
        } else {
            values[count] = (start, end);
            count += 1;
        }
    }
    values.truncate(count);
    values
}

/// Parse a CASA-style spectral-window selector.
///
/// Supported forms include:
/// - `0`
/// - `2~17`
/// - `0:4~13`
/// - `2~17:4~13`
/// - `0:4~9;12~14`
/// - `0:0~10^2`
pub fn parse_spw_selector(value: &str) -> MsResult<Vec<SpwSelector>> {
    let mut selectors = Vec::<SpwSelector>::new();
    for raw_part in value.split(',') {
        let part = raw_part.trim();
        if part.is_empty() {
            continue;
        }
        let (spw_text, channel_text) = match part.split_once(':') {
            Some((spw, channels)) => (spw.trim(), Some(channels.trim())),
            None => (part, None),
        };
        let spw_ids = parse_numeric_id_selector(spw_text, "spw")?;
        if spw_ids.is_empty() {
            return Err(MsError::VersionError(format!(
                "spw selector {part:?} does not start with a numeric spectral-window id"
            )));
        }
        let channels = match channel_text {
            Some(text) if !text.is_empty() => Some(parse_channel_selection(text)?),
            _ => None,
        };
        selectors.extend(spw_ids.into_iter().map(|spw_id| SpwSelector {
            spw_id,
            channels: channels.clone(),
        }));
    }
    if selectors.is_empty() {
        return Err(MsError::VersionError("spw selector was empty".to_string()));
    }
    Ok(selectors)
}

fn parse_channel_selection(value: &str) -> MsResult<ChannelSelection> {
    let mut segments = Vec::<ChannelSelectionSegment>::new();
    for raw_segment in value.split(';') {
        let segment = raw_segment.trim();
        if segment.is_empty() {
            continue;
        }
        let (base, stride) = match segment.split_once('^') {
            Some((base, stride_text)) => {
                let stride = stride_text.trim().parse::<usize>().map_err(|_| {
                    MsError::VersionError(format!(
                        "channel selector segment {segment:?} has an invalid stride"
                    ))
                })?;
                if stride == 0 {
                    return Err(MsError::VersionError(format!(
                        "channel selector segment {segment:?} has a zero stride"
                    )));
                }
                (base.trim(), stride)
            }
            None => (segment, 1usize),
        };
        let (start, end) = match base.split_once('~') {
            Some((start, end)) => {
                let start = start.trim().parse::<i32>().map_err(|_| {
                    MsError::VersionError(format!(
                        "channel selector segment {segment:?} has an invalid start channel"
                    ))
                })?;
                let end = end.trim().parse::<i32>().map_err(|_| {
                    MsError::VersionError(format!(
                        "channel selector segment {segment:?} has an invalid end channel"
                    ))
                })?;
                (start, end)
            }
            None => {
                let channel = base.parse::<i32>().map_err(|_| {
                    MsError::VersionError(format!(
                        "channel selector segment {segment:?} is not a supported channel id or range"
                    ))
                })?;
                (channel, channel)
            }
        };
        segments.push(ChannelSelectionSegment { start, end, stride });
    }
    if segments.is_empty() {
        return Err(MsError::VersionError(
            "channel selector was empty".to_string(),
        ));
    }
    Ok(ChannelSelection { segments })
}

/// One element of a CASA `intent` selector: a `STATE_ID` (from a number or
/// a range `a~b`), or an `OBS_MODE` pattern, matched against a whole
/// `OBS_MODE`, with `*` matching any run of characters
/// ([`IntentSelector::matches`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IntentSelector {
    /// A state number.
    State(i32),
    /// An `OBS_MODE` pattern.
    Mode(String),
}

impl IntentSelector {
    /// Whether this element selects `obs_mode` as a pattern does; a state
    /// number matches no mode.
    #[must_use]
    pub fn matches(&self, obs_mode: &str) -> bool {
        match self {
            Self::State(_) => false,
            Self::Mode(pattern) => super::parser::matches_simple_glob(pattern, obs_mode),
        }
    }
}

/// The elements of the CASA `intent` selector `text`: comma-separated state
/// numbers, state ranges and `OBS_MODE` patterns, in order, empty elements
/// skipped.
#[must_use]
pub fn parse_intent_selector(text: &str) -> Vec<IntentSelector> {
    let mut selectors = Vec::new();
    for part in text
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        if let Some((start, end)) = parse_numeric_range(part) {
            selectors.extend((start..=end).map(IntentSelector::State));
        } else if let Ok(id) = part.parse::<i32>() {
            selectors.push(IntentSelector::State(id));
        } else {
            selectors.push(IntentSelector::Mode(part.to_string()));
        }
    }
    selectors
}

pub(super) fn parse_numeric_range_endpoints(value: &str) -> Option<(i32, i32)> {
    let (start, end) = value.split_once('~')?;
    Some((
        start.trim().parse::<i32>().ok()?,
        end.trim().parse::<i32>().ok()?,
    ))
}

pub(super) fn parse_numeric_range(value: &str) -> Option<(i32, i32)> {
    let (start, end) = parse_numeric_range_endpoints(value)?;
    let (start, end) = if start <= end {
        (start, end)
    } else {
        (end, start)
    };
    Some((start, end))
}

pub(super) fn dedup_i32(values: Vec<i32>) -> Vec<i32> {
    values
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_intervals_are_bounded_by_terms_and_preserve_the_grammar() {
        let ranges = parse_numeric_id_ranges("0~2147483647", "scan").unwrap();
        assert_eq!(ranges, [(0, i32::MAX)]);
        assert_eq!(
            ranges.capacity(),
            parse_numeric_id_ranges("0~1", "scan").unwrap().capacity()
        );
        assert_eq!(
            parse_numeric_id_ranges("9, 3~1, 2~5,, 8,2147483647~2147483646", "scan").unwrap(),
            [(1, 5), (8, 9), (i32::MAX - 1, i32::MAX)]
        );
        assert_eq!(
            parse_numeric_id_selector("3~1,2,,7", "scan").unwrap(),
            [1, 2, 3, 7]
        );
        assert_eq!(
            parse_numeric_id_ranges("-2147483648~2147483647", "scan").unwrap(),
            [(i32::MIN, i32::MAX)]
        );
        for text in [">1", "*", "1~2~3", "2147483648", "x"] {
            assert!(parse_numeric_id_ranges(text, "scan").is_err());
        }
    }

    #[test]
    fn row_id_ranges_apply_casas_strict_bounds_without_changing_legacy_ids() {
        assert_eq!(
            parse_numeric_row_id_ranges("1,3~5,4", "scan").unwrap(),
            [(1, 1), (3, 5)]
        );
        assert_eq!(
            parse_numeric_row_id_ranges("0~2147483647", "observation").unwrap(),
            [(0, i32::MAX)]
        );
        for value in ["1~1", "3~1", "-1", "-1~3", "0~0"] {
            assert!(
                parse_numeric_row_id_ranges(value, "scan").is_err(),
                "{value}"
            );
            assert!(
                parse_numeric_row_id_ranges(value, "observation").is_err(),
                "{value}"
            );
        }
        assert_eq!(
            parse_numeric_id_selector("1~1,3~1", "legacy").unwrap(),
            [1, 2, 3]
        );
        assert_eq!(parse_numeric_range("1~0"), Some((0, 1)));
        assert_eq!(parse_numeric_range("0~0"), Some((0, 0)));
    }

    #[test]
    fn row_id_lexical_rules_preserve_legacy_numeric_syntax() {
        for text in [
            "", " ", ",1", "1,", "1,,2", ",", "+1", "+0~1", "0~+1", "none", "None",
        ] {
            assert!(
                parse_numeric_row_id_ranges(text, "scan").is_err(),
                "{text:?}"
            );
            assert!(
                parse_numeric_row_id_ranges(text, "observation").is_err(),
                "{text:?}"
            );
        }
        assert_eq!(
            parse_numeric_row_id_ranges(" 1 , 3 ~ 5 ", "scan").unwrap(),
            [(1, 1), (3, 5)]
        );
        assert_eq!(
            parse_numeric_id_selector(",+1,1,,+2~+3,", "legacy").unwrap(),
            [1, 2, 3]
        );
        assert!(parse_numeric_id_selector(" ", "legacy").unwrap().is_empty());
    }

    #[test]
    fn parse_numeric_id_selector_deduplicates_ranges() {
        let parsed = parse_numeric_id_selector("0,2~4,3", "spw").unwrap();
        assert_eq!(parsed, vec![0, 2, 3, 4]);
    }

    #[test]
    fn parse_spw_selector_with_channel_range() {
        let parsed = parse_spw_selector("0:4~13").unwrap();
        assert_eq!(
            parsed,
            vec![SpwSelector {
                spw_id: 0,
                channels: Some(ChannelSelection {
                    segments: vec![ChannelSelectionSegment {
                        start: 4,
                        end: 13,
                        stride: 1,
                    }],
                }),
            }]
        );
    }

    #[test]
    fn parse_spw_selector_expands_spw_range_with_shared_channel_selection() {
        let parsed = parse_spw_selector("2~4:4~6").unwrap();
        assert_eq!(
            parsed
                .iter()
                .map(|selector| selector.spw_id)
                .collect::<Vec<_>>(),
            vec![2, 3, 4]
        );
        for selector in parsed {
            assert_eq!(
                selector.channels.unwrap().indices(8).unwrap(),
                vec![4, 5, 6]
            );
        }
    }

    #[test]
    fn parse_spw_selector_with_gap_and_stride() {
        let parsed = parse_spw_selector("0:4~9;12~14,1:0~10^2").unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].spw_id, 0);
        assert_eq!(
            parsed[0].channels.as_ref().unwrap().indices(20).unwrap(),
            vec![4, 5, 6, 7, 8, 9, 12, 13, 14]
        );
        assert_eq!(parsed[1].spw_id, 1);
        assert_eq!(
            parsed[1].channels.as_ref().unwrap().indices(20).unwrap(),
            vec![0, 2, 4, 6, 8, 10]
        );
    }

    #[test]
    fn channel_selection_rejects_out_of_bounds_index() {
        let parsed = parse_spw_selector("0:4~13").unwrap();
        let error = parsed[0]
            .channels
            .as_ref()
            .unwrap()
            .indices(10)
            .unwrap_err();
        assert!(error.to_string().contains("exceeds spectral window"));
    }
}
