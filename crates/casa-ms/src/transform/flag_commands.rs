// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rewrite the copied FLAG_CMD spectral selectors for the product MS.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::LazyLock;

use casa_tables::TableOptions;
use casa_types::{ScalarValue, Value};
use regex::Regex;

use super::{
    MsTransformError, TransformChannelSelection, open_staged_subtable_for_write,
    resolve_transform_channels,
};
use crate::{MeasurementSet, SubtableId};

static SPW_ARGUMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"spw\s*=\s*(\S+)").expect("valid SPW argument pattern"));

pub(super) fn rewrite(
    input: &MeasurementSet,
    output: &Path,
    selector: &str,
    selected: &BTreeMap<i32, TransformChannelSelection>,
    width: usize,
    widths: Option<&[usize]>,
) -> Result<(), MsTransformError> {
    let path = output.join(SubtableId::FlagCmd.name());
    let fail = |source| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut table = open_staged_subtable_for_write(&path).map_err(fail)?;
    let commands = table
        .column_accessor("COMMAND")
        .and_then(|column| column.scalar_cells_owned())
        .map_err(fail)?;
    // CASA's initial guard checks only the first saved command. A table
    // headed by a named selector is left as written.
    if !matches!(commands.first(), Some(Some(ScalarValue::String(command))) if has_numeric_spw(command))
    {
        return Ok(());
    }
    let mut changed = false;
    let mut output_commands = table.column_accessor_mut("COMMAND").map_err(fail)?;
    let selector = if selector.is_empty() { "*" } else { selector };
    let selected_channels = selected
        .iter()
        .map(|(&spw, window)| {
            let channels = window.channels.iter().copied().collect::<BTreeSet<_>>();
            (spw, channels.into_iter().collect::<Vec<_>>())
        })
        .collect::<BTreeMap<_, _>>();
    for (row, command) in commands.into_iter().enumerate() {
        let Some(ScalarValue::String(command)) = command else {
            continue;
        };
        let Some(fields) = SPW_ARGUMENT.captures(&command) else {
            continue;
        };
        let argument = fields[1].replace(['\'', '"'], "");
        let new_selector = remap(
            input,
            selector,
            &argument,
            &selected_channels,
            width,
            widths,
        );
        let updated = match new_selector.as_str() {
            "" => String::new(),
            "*" => command.replace(&fields[0], ""),
            selector => command.replace(&fields[0], &format!("spw='{selector}'")),
        };
        if updated != command {
            output_commands
                .set(row, Value::Scalar(ScalarValue::String(updated)))
                .map_err(fail)?;
            changed = true;
        }
    }
    if changed {
        table
            .prepare_write()
            .save(TableOptions::new(&path))
            .map_err(fail)?;
    }
    Ok(())
}

fn has_numeric_spw(command: &str) -> bool {
    command.split_whitespace().any(|word| {
        let value = word.trim_matches(['s', 'p', 'w', '=']);
        word.contains("spw")
            && !value.is_empty()
            && !value.bytes().any(|letter| letter.is_ascii_alphabetic())
    })
}

fn remap(
    input: &MeasurementSet,
    selection: &str,
    original: &str,
    selected: &BTreeMap<i32, Vec<usize>>,
    width: usize,
    widths: Option<&[usize]>,
) -> String {
    if original.is_empty() || original == "*" || original == selection {
        return "*".into();
    }
    let Ok(windows) = input.resolve_spw_selector(original).and_then(|spw| {
        resolve_transform_channels(input, &spw, original)
            .map_err(|error| crate::MsError::InvalidInput(error.to_string()))
    }) else {
        return String::new();
    };
    let mut by_channel_label = BTreeMap::<String, Vec<usize>>::new();
    for (out_spw, (&spw, channels)) in selected.iter().enumerate() {
        let Some(command) = windows.get(&spw) else {
            continue;
        };
        let width = widths
            .filter(|widths| widths.len() > 1)
            .map_or(width, |widths| {
                widths.get(spw as usize).copied().unwrap_or(1)
            });
        let included = command
            .channels
            .iter()
            .filter_map(|channel| {
                channels
                    .binary_search(channel)
                    .ok()
                    .map(|index| index / width)
            })
            .collect::<BTreeSet<_>>();
        if included.is_empty() {
            return String::new();
        }
        let label = channel_label(&included.into_iter().collect::<Vec<_>>(), channels.len());
        by_channel_label.entry(label).or_default().push(out_spw);
    }
    let mut groups = by_channel_label
        .into_iter()
        .map(|(channels, spws)| {
            let range = consecutive_ranges(&spws);
            if channels.is_empty() {
                range
            } else {
                format!("{range}:{channels}")
            }
        })
        .collect::<Vec<_>>();
    groups.sort();
    groups.join(",")
}

fn channel_label(channels: &[usize], total: usize) -> String {
    if channels.len() == total {
        return String::new();
    }
    if channels.len() > 1 {
        let step = channels[1] - channels[0];
        if step > 1 && channels.windows(2).all(|pair| pair[1] - pair[0] == step) {
            return format!("{}~{}^{step}", channels[0], channels[channels.len() - 1]);
        }
    }
    consecutive_ranges(channels)
}

fn consecutive_ranges(values: &[usize]) -> String {
    let mut ranges = Vec::new();
    let mut remaining = values;
    while let Some((&first, rest)) = remaining.split_first() {
        let count = rest
            .iter()
            .zip(first + 1..)
            .take_while(|&(actual, expected)| *actual == expected)
            .count();
        ranges.push(if count == 0 {
            first.to_string()
        } else {
            format!("{first}~{}", first + count)
        });
        remaining = &rest[count..];
    }
    ranges.join(";")
}
