// SPDX-License-Identifier: LGPL-3.0-or-later
//! Chunk geometry and baseline identity used by the legacy visibility iterator.

use std::collections::{BTreeMap, HashSet};
use std::path::Path;

use casa_tables::{RequiredScalarColumnValues, TableError};

use crate::MeasurementSet;
use crate::selection::ResolvedMsSelectionRow;

use super::time_average::{TimeChunk, TimeGroup, TimeInputRow, TimeSample};
use super::{LegacyTimeOptions, MsTransformError, TimeAverageOptions};

struct Sample {
    time: TimeSample,
    interval: f64,
    baseline: [i32; 5],
}

pub(super) fn plan(
    input: &MeasurementSet,
    rows: &[ResolvedMsSelectionRow],
    valid: Vec<usize>,
    time: TimeAverageOptions,
    legacy: LegacyTimeOptions,
) -> Result<Vec<TimeGroup>, MsTransformError> {
    let path = input.path().unwrap_or_else(|| Path::new("<memory>"));
    let error = |source| MsTransformError::MutateMeasurementSet {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let physical = rows.iter().map(|row| row.row_index).collect::<Vec<_>>();
    let values = input
        .main_table()
        .required_scalar_columns_owned_for_rows(
            &[
                "ARRAY_ID",
                "SCAN_NUMBER",
                "STATE_ID",
                "OBSERVATION_ID",
                "PROCESSOR_ID",
                "FEED1",
                "FEED2",
                "INTERVAL",
            ],
            &physical,
        )
        .map_err(error)?;
    let integer = |name| match &values[name] {
        RequiredScalarColumnValues::Int32(values) => Ok(values.as_slice()),
        _ => Err(error(TableError::Schema(format!("{name} must hold Int32")))),
    };
    let RequiredScalarColumnValues::Float64(interval) = &values["INTERVAL"] else {
        return Err(error(TableError::Schema(
            "INTERVAL must hold Float64".into(),
        )));
    };
    let arrays = integer("ARRAY_ID")?;
    let scans = integer("SCAN_NUMBER")?;
    let states = integer("STATE_ID")?;
    let observations = integer("OBSERVATION_ID")?;
    let proc = integer("PROCESSOR_ID")?;
    let feed1 = integer("FEED1")?;
    let feed2 = integer("FEED2")?;
    let mut samples = rows
        .iter()
        .enumerate()
        .map(|(index, row)| Sample {
            time: TimeSample {
                row: row.row_index,
                description: row.data_desc_id,
                time: row.time_mjd_seconds,
                chunk: TimeChunk([
                    arrays[index],
                    if time.span_scans { 0 } else { scans[index] },
                    if time.span_states { 0 } else { states[index] },
                    row.field_id,
                    row.data_desc_id,
                    if legacy.combine_observations {
                        0
                    } else {
                        observations[index]
                    },
                    0,
                    0,
                ]),
                antenna_pair: [row.antenna1_id, row.antenna2_id],
                uvw: [0.0; 3],
            },
            interval: interval[index],
            baseline: [
                proc[index],
                row.antenna1_id,
                row.antenna2_id,
                feed1[index],
                feed2[index],
            ],
        })
        .collect::<Vec<_>>();
    samples.sort_unstable_by(|left, right| {
        left.time
            .chunk
            .cmp(&right.time.chunk)
            .then_with(|| left.time.time.total_cmp(&right.time.time))
            .then_with(|| left.time.row.cmp(&right.time.row))
    });
    let valid = valid.into_iter().collect::<HashSet<_>>();
    let mut pending = samples.as_slice();
    let mut output = Vec::new();
    while let Some(first) = pending.first() {
        let offset = first.time.time - 0.01;
        let end = pending
            .partition_point(|sample| {
                sample.time.chunk == first.time.chunk
                    && (sample.time.time == first.time.time
                        || sample.time.time - offset < time.interval_seconds)
            })
            .max(1);
        append_chunk(&pending[..end], &valid, &mut output);
        pending = &pending[end..];
    }
    Ok(output)
}

fn append_chunk(samples: &[Sample], valid: &HashSet<usize>, output: &mut Vec<TimeGroup>) {
    let Some(first) = samples
        .iter()
        .find(|sample| valid.contains(&sample.time.row))
    else {
        return;
    };
    let last = samples
        .iter()
        .rev()
        .find(|sample| valid.contains(&sample.time.row))
        .expect("at least one valid integration");
    let elapsed = last.time.time - first.time.time;
    let geometry = (
        first.time.time + 0.5 * elapsed,
        elapsed + 0.5 * (first.interval + last.interval),
    );
    let mut baselines = BTreeMap::new();
    let mut previous = None;
    let mut buffer_first = first.time.row;
    for sample in samples {
        if previous != Some(sample.time.time) {
            buffer_first = sample.time.row;
            previous = Some(sample.time.time);
        }
        if !valid.contains(&sample.time.row) {
            continue;
        }
        let (_, rows) = baselines
            .entry(sample.baseline)
            .or_insert_with(|| (sample.time, Vec::new()));
        rows.push(TimeInputRow {
            physical: sample.time.row,
            buffer_first,
        });
    }
    output.extend(
        baselines
            .into_values()
            .enumerate()
            .map(|(index, (first, rows))| TimeGroup::legacy(first, rows, index, geometry)),
    );
}
