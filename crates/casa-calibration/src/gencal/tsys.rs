// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert MS SYSCAL measurements into CASA's B TSYS calibration table.

use std::collections::HashMap;

use casa_core::{ArrayValue, ScalarValue};
use casa_ms::{MeasurementSet, schema::SubtableId};
use casa_tables::Table;
use ndarray::{ArrayD, IxDyn};

use super::{GencalError, PriorCalRow, get_f64};
use crate::constants::{COL_INTERVAL, COL_TIME};
use crate::session::scan_timing::ScanTiming;

const COL_SYSCAL_SPW: &str = "SPECTRAL_WINDOW_ID";
const COL_SYSCAL_ANTENNA: &str = "ANTENNA_ID";
const COL_TSYS: &str = "TSYS";
const COL_TSYS_SPECTRUM: &str = "TSYS_SPECTRUM";

struct Sample {
    row: usize,
    window: usize,
    antenna: usize,
    time: f64,
    interval: f64,
}

pub(super) fn rows(
    ms: &MeasurementSet,
    uniform: bool,
) -> Result<(Vec<PriorCalRow>, bool), GencalError> {
    let syscal = ms
        .subtable(SubtableId::SysCal)
        .ok_or_else(|| invalid("The SYSCAL subtable is not present in the specified MS."))?;
    let spectral = has_measurement(syscal, COL_TSYS_SPECTRUM)?;
    let column = if spectral {
        COL_TSYS_SPECTRUM
    } else if has_measurement(syscal, COL_TSYS)? {
        COL_TSYS
    } else {
        return Err(invalid("SYSCAL table has no Tsys measurements."));
    };
    let antennas = ms.antenna().map_err(invalid)?.row_count();
    let windows = ms.spectral_window().map_err(invalid)?;
    let scans = ScanTiming::read(ms.main_table()).map_err(invalid)?;
    let mut samples = (0..syscal.row_count())
        .map(|row| sample(syscal, row, windows.row_count(), antennas))
        .collect::<Result<Vec<_>, _>>()?;
    samples.sort_by(|left, right| {
        left.time
            .total_cmp(&right.time)
            .then(left.window.cmp(&right.window))
    });

    let mut output = Vec::new();
    for group in
        samples.chunk_by(|left, right| left.time == right.time && left.window == right.window)
    {
        let first = &group[0];
        let time = first.time - first.interval / 2.0;
        let scan = scans.at(time);
        let channels = if spectral {
            usize::try_from(windows.num_chan(first.window).map_err(invalid)?).map_err(invalid)?
        } else {
            1
        };
        let mut values = HashMap::new();
        let mut group_receptors = None;
        for entry in group {
            let (parameters, receptors) =
                temperatures(syscal, entry.row, column, channels, spectral)?;
            if group_receptors
                .replace(receptors)
                .is_some_and(|previous| previous != receptors)
            {
                return Err(invalid(
                    "SYSCAL group has inconsistent Tsys receptor shapes.",
                ));
            }
            values.insert(entry.antenna, parameters);
        }
        let selected: Vec<_> = if uniform {
            (0..antennas).collect()
        } else {
            group.iter().map(|entry| entry.antenna).collect()
        };
        let zeros = ArrayD::zeros(IxDyn(&[2, channels]));
        for antenna in selected {
            output.push(PriorCalRow {
                time_seconds: time,
                interval_seconds: 0.0,
                field_id: scan.field,
                spw_id: first.window as i32,
                antenna_id: antenna as i32,
                observation_id: scan.observation,
                scan_number: scan.number,
                parameters: ArrayValue::Float32(values.get(&antenna).unwrap_or(&zeros).clone()),
                snr_value: if uniform { 1.0 } else { 0.1 },
                preserved: None,
            });
        }
    }
    Ok((output, spectral))
}

fn sample(
    table: &Table,
    row: usize,
    windows: usize,
    antennas: usize,
) -> Result<Sample, GencalError> {
    let id = |column: &str, count: usize| {
        let cell = table
            .cell_accessor(row, column)
            .and_then(|cell| cell.scalar())
            .map_err(invalid)?;
        match cell {
            ScalarValue::Int32(value) if *value >= 0 && (*value as usize) < count => {
                Ok(*value as usize)
            }
            _ => Err(invalid(format!("Invalid {column} in SYSCAL row {row}."))),
        }
    };
    let time = get_f64(table, row, COL_TIME)?;
    let interval = get_f64(table, row, COL_INTERVAL)?;
    if !time.is_finite() || !interval.is_finite() {
        return Err(invalid(format!("Invalid timestamp in SYSCAL row {row}.")));
    }
    Ok(Sample {
        row,
        window: id(COL_SYSCAL_SPW, windows)?,
        antenna: id(COL_SYSCAL_ANTENNA, antennas)?,
        time,
        interval,
    })
}

fn has_measurement(table: &Table, column: &str) -> Result<bool, GencalError> {
    if table.row_count() == 0
        || !table
            .schema()
            .is_some_and(|schema| schema.contains_column(column))
    {
        return Ok(false);
    }
    table
        .cell_accessor(0, column)
        .and_then(|cell| cell.array_shape())
        .map(|shape| shape.is_some())
        .map_err(invalid)
}

fn temperatures(
    table: &Table,
    row: usize,
    column: &str,
    channels: usize,
    spectral: bool,
) -> Result<(ArrayD<f32>, usize), GencalError> {
    let value = table
        .cell_accessor(row, column)
        .and_then(|cell| cell.array())
        .map_err(invalid)?;
    let ArrayValue::Float32(source) = value else {
        return Err(invalid(format!(
            "{column} in SYSCAL row {row} is not Float32."
        )));
    };
    let receptors = match source.shape() {
        [receptors, count] if spectral && *count == channels => *receptors,
        [receptors] if !spectral => *receptors,
        _ => {
            return Err(invalid(
                "SYSCAL Tsys Spectrum channel axis shape doesn't match data.",
            ));
        }
    };
    if !(1..=2).contains(&receptors) {
        return Err(invalid("Tsys pol axis is implausible."));
    }
    let mut values = ArrayD::zeros(IxDyn(&[2, channels]));
    for receptor in 0..receptors {
        if spectral {
            for channel in 0..channels {
                values[[receptor, channel]] = source[[receptor, channel]];
            }
        } else {
            values[[receptor, 0]] = source[[receptor]];
        }
    }
    Ok((values, receptors))
}

fn invalid(detail: impl std::fmt::Display) -> GencalError {
    GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {detail}"))
}
