// SPDX-License-Identifier: LGPL-3.0-or-later
//! Canonical syntax and dataset-context validation for MeasurementSet selectors.

use casa_provider_contracts::SelectorGrammar;

use crate::selection::syntax::{parse_numeric_id_selector, parse_spw_selector};
use crate::{MeasurementSet, MsError, MsResult, MsSelection};

/// Dataset context needed by selectors that are edited separately from SPW.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MsSelectorEditContext {
    /// Selected spectral-window id for a channel-only edit.
    pub spectral_window_id: Option<i32>,
}

/// Validate one selector with the same parser used by MeasurementSet execution.
///
/// This does not scan MAIN rows. It parses the full selector and resolves
/// subtable-owned names, units, correlation types, intents, and channel bounds.
pub fn validate_ms_selector_edit(
    ms: &MeasurementSet,
    grammar: SelectorGrammar,
    value: &str,
    context: MsSelectorEditContext,
) -> MsResult<()> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(());
    }

    let mut request = MsSelection::default();
    match grammar {
        SelectorGrammar::Field => request.field = Some(value.to_string()),
        SelectorGrammar::SpectralWindow => request.spw = Some(value.to_string()),
        SelectorGrammar::TimeRange => request.timerange = Some(value.to_string()),
        SelectorGrammar::UvRange => request.uvrange = Some(value.to_string()),
        SelectorGrammar::Antenna => request.antenna = Some(value.to_string()),
        SelectorGrammar::Scan => request.scan = Some(value.to_string()),
        SelectorGrammar::Correlation => request.correlation = Some(value.to_string()),
        SelectorGrammar::Array => request.array = Some(value.to_string()),
        SelectorGrammar::Observation => request.observation = Some(value.to_string()),
        SelectorGrammar::Intent => request.intent = Some(value.to_string()),
        SelectorGrammar::Feed => {
            validate_numeric_membership(value, "feed", feed_ids(ms)?.as_slice())?;
            return Ok(());
        }
        SelectorGrammar::MsSelect => request.msselect = Some(value.to_string()),
        SelectorGrammar::ImageChannels => {
            let spw_id = context.spectral_window_id.ok_or_else(|| {
                MsError::InvalidInput(
                    "channel edit validation requires a selected spectral window".to_string(),
                )
            })?;
            request.spw = Some(format!("{spw_id}:{value}"));
        }
        SelectorGrammar::ImageBox | SelectorGrammar::ImageRegion | SelectorGrammar::Stokes => {
            return Err(MsError::InvalidInput(format!(
                "{grammar:?} is not a MeasurementSet selector grammar"
            )));
        }
    }

    request.compile(ms)?;
    validate_dataset_bounds(ms, grammar, value, context)
}

fn validate_dataset_bounds(
    ms: &MeasurementSet,
    grammar: SelectorGrammar,
    value: &str,
    context: MsSelectorEditContext,
) -> MsResult<()> {
    match grammar {
        SelectorGrammar::Field => {
            let numeric = numeric_parts(value);
            validate_ids_below(&numeric, "field", ms.field()?.row_count())
        }
        SelectorGrammar::SpectralWindow => validate_spw_bounds(ms, value),
        SelectorGrammar::ImageChannels => {
            let spw_id = context.spectral_window_id.expect("checked by caller");
            validate_spw_bounds(ms, &format!("{spw_id}:{value}"))
        }
        SelectorGrammar::Antenna => {
            let ids = antenna_numeric_parts(value);
            validate_ids_below(&ids, "antenna", ms.antenna()?.row_count())
        }
        SelectorGrammar::Observation => validate_numeric_membership(
            value,
            "observation",
            &(0..ms.observation()?.row_count() as i32).collect::<Vec<_>>(),
        ),
        SelectorGrammar::Scan | SelectorGrammar::Array => {
            parse_numeric_id_selector(value, selector_label(grammar)).map(|_| ())
        }
        SelectorGrammar::TimeRange
        | SelectorGrammar::UvRange
        | SelectorGrammar::Correlation
        | SelectorGrammar::Intent
        | SelectorGrammar::Feed
        | SelectorGrammar::MsSelect => Ok(()),
        SelectorGrammar::ImageBox | SelectorGrammar::ImageRegion | SelectorGrammar::Stokes => {
            unreachable!("rejected by caller")
        }
    }
}

fn validate_spw_bounds(ms: &MeasurementSet, value: &str) -> MsResult<()> {
    let spectral_window = ms.spectral_window()?;
    for selector in parse_spw_selector(value)? {
        let spw_id = usize::try_from(selector.spw_id).map_err(|_| {
            MsError::InvalidInput(format!(
                "spectral-window id {} is negative",
                selector.spw_id
            ))
        })?;
        if spw_id >= spectral_window.row_count() {
            return Err(MsError::InvalidInput(format!(
                "spectral-window id {spw_id} is outside 0..{}",
                spectral_window.row_count()
            )));
        }
        if let Some(channels) = selector.channels {
            let total = usize::try_from(spectral_window.num_chan(spw_id)?).map_err(|_| {
                MsError::InvalidInput(format!(
                    "spectral-window {spw_id} has a negative channel count"
                ))
            })?;
            channels.indices(total)?;
        }
    }
    Ok(())
}

fn numeric_parts(value: &str) -> Vec<i32> {
    value
        .split(',')
        .map(str::trim)
        .filter(|part| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|character| character.is_ascii_digit() || character == '~')
        })
        .flat_map(|part| parse_numeric_id_selector(part, "selector").unwrap_or_default())
        .collect()
}

fn antenna_numeric_parts(value: &str) -> Vec<i32> {
    value
        .split(',')
        .flat_map(|part| part.split("&&"))
        .filter_map(|part| part.trim().parse::<i32>().ok())
        .collect()
}

fn validate_ids_below(ids: &[i32], label: &str, upper: usize) -> MsResult<()> {
    for &id in ids {
        let id = usize::try_from(id)
            .map_err(|_| MsError::InvalidInput(format!("{label} id {id} is negative")))?;
        if id >= upper {
            return Err(MsError::InvalidInput(format!(
                "{label} id {id} is outside 0..{upper}"
            )));
        }
    }
    Ok(())
}

fn validate_numeric_membership(value: &str, label: &str, allowed: &[i32]) -> MsResult<()> {
    for id in parse_numeric_id_selector(value, label)? {
        if !allowed.contains(&id) {
            return Err(MsError::InvalidInput(format!(
                "{label} id {id} is not present in this MeasurementSet"
            )));
        }
    }
    Ok(())
}

fn feed_ids(ms: &MeasurementSet) -> MsResult<Vec<i32>> {
    let feed = ms.feed()?;
    let mut ids = Vec::new();
    for row in 0..feed.row_count() {
        let id = feed.i32(row, "FEED_ID")?;
        if !ids.contains(&id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn selector_label(grammar: SelectorGrammar) -> &'static str {
    match grammar {
        SelectorGrammar::Scan => "scan",
        SelectorGrammar::Array => "array",
        _ => "selector",
    }
}
