// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected B-table channelization, including CASA's persisted width defects.
use super::{BandpassFrequencyInterval, BandpassSolutionRow, BandpassSolveError};
use crate::solve::grouping::SelectedSolveRow;
use casa_ms::ms::MeasurementSet;
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use std::{collections::BTreeMap, path::Path};

pub(super) fn bin_widths(
    ms: &MeasurementSet,
    rows: &[SelectedSolveRow],
    interval: Option<BandpassFrequencyInterval>,
) -> Result<BTreeMap<i32, usize>, BandpassSolveError> {
    match interval {
        Some(BandpassFrequencyInterval::Channels(count)) if count > 1.0 => Ok(rows
            .iter()
            .map(|row| (row.data_spw_id, count as usize))
            .collect()),
        Some(BandpassFrequencyInterval::Hertz(hertz)) => bandwidth_bin_widths(ms, hertz),
        _ => Ok(BTreeMap::new()),
    }
}

fn bandwidth_bin_widths(
    ms: &MeasurementSet,
    hertz: f64,
) -> Result<BTreeMap<i32, usize>, BandpassSolveError> {
    let fail = |reason: String| BandpassSolveError::UnsupportedConfiguration { reason };
    let window = ms
        .spectral_window()
        .map_err(|error| fail(error.to_string()))?;
    let mut widths = BTreeMap::new();
    for spw in 0..window.row_count() {
        let channels = window
            .chan_width(spw)
            .map_err(|error| fail(error.to_string()))?;
        let width = channels
            .first()
            .filter(|width| width.is_finite() && **width != 0.0)
            .ok_or_else(|| fail(format!("SPW {spw} has no valid first channel width")))?;
        widths.insert(spw as i32, (hertz / width.abs()).floor().max(1.0) as usize);
    }
    // Once the layer is installed, even windows with a width of one use
    // the frequency averager's scalar weights for the time centroid.
    if widths.values().all(|&width| width == 1) {
        widths.clear();
    }
    Ok(widths)
}

pub(super) fn write(
    path: &Path,
    rows: &[BandpassSolutionRow],
    channel_bin_widths: &BTreeMap<i32, usize>,
    selected_rows: &[SelectedSolveRow],
) -> Result<(), BandpassSolveError> {
    let fail = |reason: String| BandpassSolveError::UnsupportedConfiguration { reason };
    let mut table = Table::open(TableOptions::new(path)).map_err(|e| fail(e.to_string()))?;
    let selections = selected_rows
        .iter()
        .filter_map(|row| {
            row.channel_selection
                .as_ref()
                .map(|channels| (row.data_spw_id, channels))
        })
        .collect::<BTreeMap<_, _>>();
    for row in 0..table.row_count() {
        let present = rows.iter().any(|r| r.spw_id == row as i32);
        table
            .cell_accessor_mut(row, "FLAG_ROW")
            .and_then(|mut c| c.set(Value::Scalar(ScalarValue::Bool(!present))))
            .map_err(|e| fail(e.to_string()))?;
        if !present {
            continue;
        }
        let array = |column| -> Result<Vec<f64>, BandpassSolveError> {
            let ArrayValue::Float64(values) = table
                .cell_accessor(row, column)
                .and_then(|c| c.array())
                .map_err(|e| fail(e.to_string()))?
            else {
                return Err(fail(format!("SPW {row} {column} is not Float64")));
            };
            Ok(values.iter().copied().collect())
        };
        let frequencies = array("CHAN_FREQ")?;
        let widths = array("CHAN_WIDTH")?;
        let selection = selections.get(&(row as i32)).copied();
        let channels = crate::solve::grouping::selected_channels(selection, frequencies.len())?;
        let mut selected_frequencies = Vec::new();
        let mut selected_widths = Vec::new();
        for channel in channels {
            selected_frequencies.push(frequencies[channel]);
            selected_widths.push(
                widths
                    .get(channel)
                    .ok_or_else(|| fail("SPW channel width shape mismatch".to_string()))?
                    .abs(),
            );
        }
        if let Some(&width) = channel_bin_widths.get(&(row as i32)) {
            selected_frequencies = selected_frequencies
                .chunks(width)
                .map(|bin| bin.iter().sum::<f64>() / bin.len() as f64)
                .collect();
            selected_widths = selected_widths
                .chunks(width)
                .map(|bin| bin.iter().sum())
                .collect();
        }
        let count = selected_frequencies.len();
        let total: f64 = selected_frequencies.iter().sum();
        // SolvableVisCal::setCTFrequencies aliases resolution to frequency
        // and sums frequencies as bandwidth. CASA requires exact agreement
        // when appending; these fields are not used to apply the solutions.
        for (column, values) in [
            ("CHAN_FREQ", selected_frequencies.clone()),
            ("RESOLUTION", selected_frequencies),
            ("CHAN_WIDTH", selected_widths.clone()),
            ("EFFECTIVE_BW", selected_widths),
        ] {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut c| c.set(Value::Array(ArrayValue::from_f64_vec(values))))
                .map_err(|e| fail(e.to_string()))?;
        }
        for (column, value) in [
            ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(count as i32))),
            (
                "TOTAL_BANDWIDTH",
                Value::Scalar(ScalarValue::Float64(total)),
            ),
        ] {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut c| c.set(value))
                .map_err(|e| fail(e.to_string()))?;
        }
    }
    table
        .save(TableOptions::new(path))
        .map_err(|e| fail(e.to_string()))
}
