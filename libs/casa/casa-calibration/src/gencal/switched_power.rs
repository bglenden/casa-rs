// SPDX-License-Identifier: LGPL-3.0-or-later
//! EVLA switched-power priors generated from SYSPOWER and CALDEVICE.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_tables::{ColumnType, RequiredScalarColumnValues, Table, TableOptions};
use ndarray::{ArrayD, IxDyn};

use super::{GencalError, GencalType, PriorCalRow, get_f64};
use crate::constants::{
    COL_ARRAY_ID, COL_FIELD_ID, COL_FLAG, COL_OBSERVATION_ID, COL_SCAN_NUMBER, COL_TIME,
};

pub(crate) const CORRELATOR_EFFICIENCY: f32 = 0.932;
const FRINGE_ROTATION_SCALE: f32 = 1.176;
const FIELD_SCAN_TOLERANCE_SECONDS: f64 = 0.1;
const MISSING_NOISE: [f32; 2] = [-1.0; 2];

struct VisibilityLabels {
    fields_by_time: Vec<(f64, i32)>,
    scan_limits: Vec<(i32, (f64, f64))>,
}

pub(crate) fn input_paths(
    ms: &MeasurementSet,
    path: &Path,
) -> Result<(PathBuf, PathBuf), GencalError> {
    let resolve = |name| {
        ms.main_table()
            .keywords()
            .get(name)
            .and_then(Value::as_table_ref)
            .map(|reference| casa_tables::add_directory(reference, path))
            .ok_or_else(|| invalid(format!("RecordInterface: field {name} is unknown")))
    };
    Ok((resolve("SYSPOWER")?, resolve("CALDEVICE")?))
}

pub(super) fn rows(
    ms: &MeasurementSet,
    power_path: &Path,
    noise_path: &Path,
    caltype: GencalType,
    selection: Option<&casa_ms::MsSelection>,
) -> Result<Vec<PriorCalRow>, GencalError> {
    let power = open(power_path, "SYSPOWER")?;
    let noise = open(noise_path, "CALDEVICE")?;
    validate_scalar_columns(&power, &[("INTERVAL", PrimitiveType::Float64)])?;
    validate_scalar_columns(
        &noise,
        &[
            (COL_TIME, PrimitiveType::Float64),
            ("INTERVAL", PrimitiveType::Float64),
            ("NUM_CAL_LOAD", PrimitiveType::Int32),
        ],
    )?;
    let antennas = ms.antenna().map_err(invalid)?.row_count();
    let windows = ms.spectral_window().map_err(invalid)?.row_count();
    let labels = visibility_labels(ms, selection)?;
    let noise = noise_temperatures(&noise, antennas, windows)?;
    let mut samples = (0..power.row_count())
        .map(|row| {
            let time = get_f64(&power, row, COL_TIME)?;
            if !time.is_finite() {
                return Err(invalid(format!("Invalid TIME in SYSPOWER row {row}")));
            }
            let (window, antenna) = ids(&power, row, windows, antennas)?;
            Ok((time, window, antenna, row))
        })
        .collect::<Result<Vec<_>, _>>()?;
    samples.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));

    let mut output = Vec::new();
    let mut gains = vec![([1.0; 4], false); antennas];
    for group in samples.chunk_by(|a, b| a.0 == b.0 && a.1 == b.1) {
        let (time, window, _, _) = group[0];
        gains.fill(([1.0; 4], false));
        for &(_, _, antenna, row) in group {
            let temperature = noise
                .get(&(window, antenna))
                .copied()
                .unwrap_or(MISSING_NOISE);
            gains[antenna] = power_parameters(&power, row, temperature, caltype)?;
        }
        let (field, scan) = labels_at(&labels, time);
        for (antenna, &(values, good)) in gains.iter().enumerate() {
            let flags = ArrayD::from_elem(IxDyn(&[4, 1]), !good);
            output.push(PriorCalRow {
                time_seconds: time,
                interval_seconds: 0.0,
                field_id: field,
                spw_id: window as i32,
                antenna_id: antenna as i32,
                observation_id: -1,
                scan_number: scan,
                parameters: ArrayValue::Float32(
                    ArrayD::from_shape_vec(IxDyn(&[4, 1]), values.to_vec())
                        .expect("four parameters"),
                ),
                snr_value: 1.0,
                preserved: Some(RecordValue::new(vec![RecordField::new(
                    COL_FLAG,
                    Value::Array(ArrayValue::Bool(flags)),
                )])),
            });
        }
    }
    if output.is_empty() {
        return Err(invalid("SYSPOWER contains no switched-power samples"));
    }
    Ok(output)
}

fn open(path: &Path, name: &str) -> Result<Table, GencalError> {
    Table::open(TableOptions::new(path)).map_err(|source| {
        invalid(format!(
            "The {name} subtable is not readable at {}: {source}",
            path.display()
        ))
    })
}

fn validate_scalar_columns(
    table: &Table,
    columns: &[(&str, PrimitiveType)],
) -> Result<(), GencalError> {
    for &(name, expected) in columns {
        let column = table
            .schema()
            .and_then(|schema| schema.column(name))
            .ok_or_else(|| invalid(format!("Table column {name} is unknown")))?;
        if *column.column_type() != ColumnType::Scalar || column.data_type() != Some(expected) {
            return Err(invalid(format!(
                "Table column {name} has the wrong scalar type"
            )));
        }
    }
    Ok(())
}

fn ids(
    table: &Table,
    row: usize,
    windows: usize,
    antennas: usize,
) -> Result<(usize, usize), GencalError> {
    let id = |name, count| {
        let value = table
            .cell_accessor(row, name)
            .and_then(|cell| cell.scalar())
            .map_err(invalid)?;
        match value {
            ScalarValue::Int32(value) if *value >= 0 && (*value as usize) < count => {
                Ok(*value as usize)
            }
            _ => Err(invalid(format!("Invalid {name} in row {row}"))),
        }
    };
    Ok((
        id("SPECTRAL_WINDOW_ID", windows)?,
        id("ANTENNA_ID", antennas)?,
    ))
}

fn float_array<'a>(
    table: &'a Table,
    row: usize,
    name: &str,
) -> Result<&'a ArrayD<f32>, GencalError> {
    match table
        .cell_accessor(row, name)
        .and_then(|cell| cell.array())
        .map_err(invalid)?
    {
        ArrayValue::Float32(array) => Ok(array),
        _ => Err(invalid(format!(
            "{name} in row {row} must be a Float32 array"
        ))),
    }
}

fn noise_temperatures(
    table: &Table,
    antennas: usize,
    windows: usize,
) -> Result<HashMap<(usize, usize), [f32; 2]>, GencalError> {
    let mut noise = HashMap::new();
    for row in 0..table.row_count() {
        let key = ids(table, row, windows, antennas)?;
        if noise.contains_key(&key) {
            continue;
        }
        let values = float_array(table, row, "NOISE_CAL")?;
        let nominal = match values.len() {
            1 => [*values.first().expect("one value"); 2],
            2 => {
                let mut values = values.iter();
                [
                    *values.next().expect("two values"),
                    *values.next().expect("two values"),
                ]
            }
            4 if values.shape() == [2, 2] => [values[[0, 0]], values[[0, 1]]],
            4 => {
                return Err(invalid(
                    "NOISE_CAL with four entries must have shape [2, 2]",
                ));
            }
            _ => MISSING_NOISE,
        };
        noise.insert(key, nominal);
    }
    Ok(noise)
}

fn power_parameters(
    table: &Table,
    row: usize,
    noise: [f32; 2],
    caltype: GencalType,
) -> Result<([f32; 4], bool), GencalError> {
    let receptors = |name| {
        let values = float_array(table, row, name)?;
        if values.shape() != [2] {
            return Err(invalid(format!(
                "{name} in row {row} must have two receptors"
            )));
        }
        Ok([values[[0]], values[[1]]])
    };
    let sum = receptors("SWITCHED_SUM")?;
    let diff = receptors("SWITCHED_DIFF")?;
    let rq = receptors("REQUANTIZER_GAIN")?;
    let good = |values: [f32; 2]| values.iter().all(|&value| value > f32::EPSILON);
    // CASA's SWPOW validity check also requires the RQ plane, including for
    // the evlagain alias, while the gain formula itself uses diff and noise.
    let valid = match caltype {
        GencalType::Rq => good(rq),
        GencalType::Swpwts => good(sum) && good(rq),
        _ => good(sum) && good(diff) && good(noise) && good(rq),
    };
    let mut parameters = [1.0; 4];
    if valid {
        let digital = (CORRELATOR_EFFICIENCY * FRINGE_ROTATION_SCALE).sqrt();
        for receptor in 0..2 {
            let (sum, diff, rq, noise) =
                (sum[receptor], diff[receptor], rq[receptor], noise[receptor]);
            let (gain, tsys) = match caltype {
                GencalType::Rq => (rq, 1.0 / (rq * rq)),
                GencalType::Swpwts => (rq, (sum / 2.0) / (rq * rq)),
                GencalType::SwpOverRq => ((diff / noise).sqrt() / rq, noise * sum / diff / 2.0),
                _ => ((diff / noise).sqrt(), noise * sum / diff / 2.0),
            };
            parameters[receptor * 2] = gain * digital;
            parameters[receptor * 2 + 1] = tsys;
        }
    }
    Ok((parameters, valid))
}

fn visibility_labels(
    ms: &MeasurementSet,
    selection: Option<&casa_ms::MsSelection>,
) -> Result<VisibilityLabels, GencalError> {
    let columns = ms
        .main_table()
        .required_scalar_columns_owned(&[
            COL_TIME,
            COL_FIELD_ID,
            COL_SCAN_NUMBER,
            COL_OBSERVATION_ID,
            COL_ARRAY_ID,
        ])
        .map_err(invalid)?;
    let RequiredScalarColumnValues::Float64(times) = &columns[COL_TIME] else {
        return Err(invalid("MeasurementSet TIME is not Float64"));
    };
    let ids = |name| match &columns[name] {
        RequiredScalarColumnValues::Int32(values) => Ok(values.as_slice()),
        _ => Err(invalid(format!("MeasurementSet {name} is not Int32"))),
    };
    let fields = ids(COL_FIELD_ID)?;
    let scans = ids(COL_SCAN_NUMBER)?;
    let observations = ids(COL_OBSERVATION_ID)?;
    let arrays = ids(COL_ARRAY_ID)?;
    let mut fields_by_time = HashMap::new();
    let mut scan_limits = HashMap::new();
    let observe = |row: usize| {
        let time = times[row];
        if !time.is_finite() {
            return;
        }
        fields_by_time
            .entry(time.to_bits())
            .and_modify(|field: &mut i32| *field = (*field).min(fields[row]))
            .or_insert(fields[row]);
        let limits = scan_limits
            .entry((observations[row], arrays[row], scans[row]))
            .or_insert((time, time));
        limits.0 = limits.0.min(time);
        limits.1 = limits.1.max(time);
    };
    if let Some(selection) = selection {
        crate::session::resolve_casa_calibration_selection(ms, selection)
            .map_err(invalid)?
            .row_indices()
            .for_each(observe);
    } else {
        (0..times.len()).for_each(observe);
    }
    // At any visibility time CASA's set reports the smallest field ID.
    let mut fields_by_time = fields_by_time
        .into_iter()
        .map(|(time, field)| (f64::from_bits(time), field))
        .collect::<Vec<_>>();
    fields_by_time.sort_unstable_by(|a, b| a.0.total_cmp(&b.0));
    Ok(VisibilityLabels {
        fields_by_time,
        scan_limits: scan_limits
            .into_iter()
            .map(|((_, _, scan), limits)| (scan, limits))
            .collect(),
    })
}

fn labels_at(labels: &VisibilityLabels, time: f64) -> (i32, i32) {
    let (low, high) = (
        time - FIELD_SCAN_TOLERANCE_SECONDS,
        time + FIELD_SCAN_TOLERANCE_SECONDS,
    );
    let fields = &labels.fields_by_time;
    let first = fields.partition_point(|&(timestamp, _)| timestamp < low);
    // MSMetadata inserts fields at the first time above the high endpoint
    // before exiting its search. Scan matching instead tests the full span.
    let after = fields.partition_point(|&(timestamp, _)| timestamp <= high);
    let end = (after + 1).min(fields.len());
    let field = fields[first..end]
        .iter()
        .map(|&(_, field)| field)
        .min()
        .unwrap_or(-1);
    let scan = labels
        .scan_limits
        .iter()
        .filter(|&&(_, (start, end))| start <= high && end >= low)
        .map(|&(scan, _)| scan)
        .min()
        .unwrap_or(-1);
    (field, scan)
}

fn invalid(detail: impl std::fmt::Display) -> GencalError {
    GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {detail}"))
}
