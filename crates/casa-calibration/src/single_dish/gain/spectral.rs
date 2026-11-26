// SPDX-License-Identifier: LGPL-3.0-or-later
//! Spectral metadata of scalar double-circle gain solutions.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::{MeasurementSet, ResolvedMsSelection};
use casa_tables::{Table, TableOptions};
use ndarray::{ArrayD, IxDyn};

use crate::single_dish::edges::Integration;

use super::error_text;

pub(super) fn channels(
    ms: &MeasurementSet,
    selected: &ResolvedMsSelection,
    integrations: &[&Integration],
) -> Result<BTreeMap<usize, usize>, String> {
    let spectral = ms.spectral_window().map_err(error_text)?;
    let windows = integrations
        .iter()
        .map(|row| row.window)
        .collect::<BTreeSet<_>>();
    windows
        .into_iter()
        .map(|window| {
            let channels = spectral.num_chan(window).map_err(error_text)? as usize;
            let channel = if let Some(selection) = selected.channel_selections.get(&(window as i32))
            {
                let picked = selection.indices(channels).map_err(error_text)?;
                let [index] = picked.as_slice() else {
                    return Err(
                        "Double-circle gain needs one selected channel per spectral window.".into(),
                    );
                };
                *index
            } else if channels == 1 {
                0
            } else {
                return Err(
                    "Double-circle gain needs one selected channel per spectral window.".into(),
                );
            };
            Ok((window, channel))
        })
        .collect()
}

pub(super) fn write_axis(
    path: &Path,
    windows: &BTreeMap<usize, usize>,
    present: &BTreeSet<usize>,
) -> Result<(), String> {
    let options = TableOptions::new(path);
    let mut table = Table::open(options.clone()).map_err(error_text)?;
    for row in 0..table.row_count() {
        put(
            &mut table,
            row,
            "FLAG_ROW",
            Value::Scalar(ScalarValue::Bool(!present.contains(&row))),
        )?;
        let Some(&channel) = windows.get(&row) else {
            continue;
        };
        let frequency = read_channel(&table, row, "CHAN_FREQ", channel)?;
        let width = read_channel(&table, row, "CHAN_WIDTH", channel)?;
        let effective = read_channel(&table, row, "EFFECTIVE_BW", channel)?;
        // SolvableVisCal::setCTFrequencies uses its selected frequency as
        // RESOLUTION and sums this axis for TOTAL_BANDWIDTH.
        for (name, value) in [
            ("CHAN_FREQ", frequency),
            ("CHAN_WIDTH", width),
            ("EFFECTIVE_BW", effective),
            ("RESOLUTION", frequency),
        ] {
            put(
                &mut table,
                row,
                name,
                Value::Array(ArrayValue::Float64(ArrayD::from_elem(IxDyn(&[1]), value))),
            )?;
        }
        put(
            &mut table,
            row,
            "TOTAL_BANDWIDTH",
            Value::Scalar(ScalarValue::Float64(frequency)),
        )?;
        put(
            &mut table,
            row,
            "NUM_CHAN",
            Value::Scalar(ScalarValue::Int32(1)),
        )?;
    }
    table.save(options).map_err(error_text)?;
    Ok(())
}

fn read_channel(table: &Table, row: usize, column: &str, channel: usize) -> Result<f64, String> {
    let ArrayValue::Float64(values) = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(error_text)?
    else {
        return Err(format!("Invalid spectral axis {column}."));
    };
    values
        .get(IxDyn(&[channel]))
        .copied()
        .ok_or_else(|| format!("Missing channel {channel} in {column}."))
}

fn put(table: &mut Table, row: usize, name: &str, value: Value) -> Result<(), String> {
    table
        .cell_accessor_mut(row, name)
        .and_then(|mut cell| cell.set(value))
        .map_err(error_text)
}
