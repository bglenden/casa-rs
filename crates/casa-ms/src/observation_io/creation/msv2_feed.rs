// SPDX-License-Identifier: LGPL-3.0-or-later
//! Preserve the validity of retained static MSv2 feeds. The catalogue
//! matches receptors; their original validity must not become the MAIN span.
use super::{combine::Combined, inputs::Input, msv2_tables::Layout};
use crate::observation_io::update::{request_error, storage};
use crate::{MeasurementSet, SubtableId};
use casa_core::{RecordValue, ScalarValue, Value};
use casa_observation::ObservationError;

fn integer(row: &RecordValue, name: &str) -> Result<i32, ObservationError> {
    match row.get(name) {
        Some(Value::Scalar(ScalarValue::Int32(value))) => Ok(*value),
        _ => Err(request_error(format!("FEED {name} is not int32"))),
    }
}

fn real(row: &RecordValue, name: &str) -> Result<f64, ObservationError> {
    match row.get(name) {
        Some(Value::Scalar(ScalarValue::Float64(value))) => Ok(*value),
        _ => Err(request_error(format!("FEED {name} is not float64"))),
    }
}

/// MSFeedColumns::matchFeed extends a matching row when its old validity
/// does not cover the incoming one. An old zero interval means unlimited.
fn merge_validity(
    output: &mut casa_tables::Table,
    target: usize,
    old: &RecordValue,
    incoming: &RecordValue,
) -> Result<(), ObservationError> {
    let old_time = real(old, "TIME")?;
    let new_time = real(incoming, "TIME")?;
    let old_interval = real(old, "INTERVAL")?;
    let old_half = if old_interval == 0.0 {
        5.0e17
    } else {
        old_interval / 2.0
    };
    let new_half = real(incoming, "INTERVAL")? / 2.0;
    let offset = new_time - old_time;
    if -old_half <= offset - new_half && old_half >= offset + new_half {
        return Ok(());
    }
    let centre = old_time + offset / 2.0;
    let interval = 2.0 * (old_half.max(offset + new_half) - offset / 2.0);
    for (column, value) in [("TIME", centre), ("INTERVAL", interval)] {
        output
            .row_accessor_mut()
            .set_cell(target, column, Value::Scalar(ScalarValue::Float64(value)))
            .map_err(storage)?;
    }
    Ok(())
}

pub(super) fn preserve(
    ms: &mut MeasurementSet,
    inputs: &[Input],
    combined: &Combined,
    layout: &Layout,
) -> Result<(), ObservationError> {
    let Some(output) = ms.subtable_mut(SubtableId::Feed) else {
        return Ok(());
    };
    let mut written = vec![false; output.row_count()];
    for (input, map) in inputs.iter().zip(&combined.rows) {
        let Some(source) = input
            .msv2()
            .and_then(|input| input.measurement_set().subtable(SubtableId::Feed))
        else {
            continue;
        };
        let catalogue = input.observation().catalogue();
        for row in source.rows().map_err(storage)? {
            let antenna = integer(row, "ANTENNA_ID")?;
            let antenna = *usize::try_from(antenna)
                .ok()
                .and_then(|id| map.antennas.get(id))
                .ok_or_else(|| request_error("FEED names an unknown antenna"))?;
            let feed = integer(row, "FEED_ID")?;
            let window = integer(row, "SPECTRAL_WINDOW_ID")?;
            let windows: Vec<i32> = catalogue
                .partitions
                .iter()
                .zip(&map.partitions)
                .filter(|(partition, _)| window < 0 || partition.window.0 as i32 == window)
                .map(|(_, partition)| layout.spectral_window[partition.output])
                .collect();
            let mut matched = false;
            for (target, assigned) in written.iter_mut().enumerate() {
                let target_row = output.row_accessor().row(target).map_err(storage)?.clone();
                let target_window = integer(&target_row, "SPECTRAL_WINDOW_ID")?;
                if integer(&target_row, "ANTENNA_ID")? != antenna
                    || integer(&target_row, "FEED_ID")? != feed
                    || (target_window >= 0 && !windows.contains(&target_window))
                {
                    continue;
                }
                matched = true;
                if *assigned {
                    merge_validity(output, target, &target_row, row)?;
                    continue;
                }
                for column in ["TIME", "INTERVAL"] {
                    let value = row
                        .get(column)
                        .ok_or_else(|| request_error(format!("FEED lacks {column}")))?;
                    output
                        .row_accessor_mut()
                        .set_cell(target, column, value.clone())
                        .map_err(storage)?;
                }
                if window < 0 {
                    output
                        .row_accessor_mut()
                        .set_cell(
                            target,
                            "SPECTRAL_WINDOW_ID",
                            Value::Scalar(ScalarValue::Int32(-1)),
                        )
                        .map_err(storage)?;
                }
                *assigned = true;
            }
            if !matched {
                // An unused antenna still owns its FEED rows. CASA keeps
                // them even though the scientific batches never name it.
                let mut row = row.clone();
                *row.get_mut("ANTENNA_ID").expect("checked antenna") =
                    Value::Scalar(ScalarValue::Int32(antenna));
                let window = if window < 0 {
                    -1
                } else {
                    *windows.first().ok_or_else(|| {
                        request_error("FEED of an unused spectral window is outside #939")
                    })?
                };
                *row.get_mut("SPECTRAL_WINDOW_ID").expect("checked window") =
                    Value::Scalar(ScalarValue::Int32(window));
                output.add_row(row).map_err(storage)?;
                written.push(true);
            }
        }
    }
    let mut duplicates = Vec::new();
    let rows = output.rows().map_err(storage)?;
    for row in 0..rows.len() {
        if rows[..row].contains(&rows[row]) {
            duplicates.push(row);
        }
    }
    output.remove_rows(&duplicates).map_err(storage)?;
    Ok(())
}
