// SPDX-License-Identifier: LGPL-3.0-or-later
//! Selected B-table channelization, including CASA's persisted width defects.
use super::{BandpassSolutionRow, BandpassSolveError, BandpassSolveRequest};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, ScalarValue, Value};
use std::path::Path;

pub(super) fn write(
    path: &Path,
    request: &BandpassSolveRequest,
    rows: &[BandpassSolutionRow],
) -> Result<(), BandpassSolveError> {
    let fail = |reason: String| BandpassSolveError::UnsupportedConfiguration { reason };
    let mut table = Table::open(TableOptions::new(path)).map_err(|e| fail(e.to_string()))?;
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
        let selection = request.selection.channel_selection_for_spw(row as i32);
        let channels =
            crate::solve::grouping::selected_channels(selection.as_ref(), frequencies.len())?;
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
