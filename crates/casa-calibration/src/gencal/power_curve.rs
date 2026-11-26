// SPDX-License-Identifier: LGPL-3.0-or-later
//! Convert the embedded FITS gain-curve extension as CASA's EPowerCurve does.

use std::path::{Path, PathBuf};

use casa_core::{ArrayValue, ScalarValue, Value};
use casa_ms::MeasurementSet;
use casa_tables::{Table, TableOptions};

use super::{
    COL_GAIN, COL_TIME, GencalError, POWER_CURVE_TERMS, PriorCalRow, f32_column,
    get_f32_array_fortran, get_f64, get_string,
};

pub(super) fn source_path(owner: &Path, table: &Table) -> Result<Option<PathBuf>, GencalError> {
    match table.keywords().get("GAIN_CURVE") {
        None => Ok(None),
        Some(Value::TableRef(reference)) => Ok(Some(casa_tables::add_directory(reference, owner))),
        Some(_) => Err(GencalError::InvalidRequest(
            "Error in Calibrater::specifycal. GAIN_CURVE keyword does not name a table".into(),
        )),
    }
}

pub(super) fn rows(ms: &MeasurementSet, path: &Path) -> Result<Vec<PriorCalRow>, GencalError> {
    let invalid = |message: String| {
        GencalError::InvalidRequest(format!("Error in Calibrater::specifycal. {message}"))
    };
    let table =
        Table::open(TableOptions::new(path)).map_err(|source| GencalError::OpenGainCurves {
            path: path.display().to_string(),
            source: Box::new(source),
        })?;
    let antenna_count = ms
        .antenna()
        .map_err(|error| invalid(error.to_string()))?
        .row_count();
    let window_count = ms
        .spectral_window()
        .map_err(|error| invalid(error.to_string()))?
        .row_count();
    let mut curves = Vec::new();
    for row in 0..table.row_count() {
        let integer = |column: &str| {
            let value = table
                .cell_accessor(row, column)
                .and_then(|cell| cell.scalar())
                .map_err(|error| invalid(error.to_string()))?;
            match value {
                ScalarValue::Int32(value) => Ok(*value),
                _ => Err(invalid(format!(
                    "GAIN_CURVE {column} row {row} is not an integer"
                ))),
            }
        };
        let window = integer("SPECTRAL_WINDOW_ID")?;
        if usize::try_from(window).map_or(true, |id| id >= window_count) {
            continue;
        }
        let antenna = integer("ANTENNA_ID")?;
        if usize::try_from(antenna).map_or(true, |id| id >= antenna_count) {
            return Err(invalid(format!(
                "GAIN_CURVE row {row} has invalid antenna {antenna}"
            )));
        }
        let terms = (integer("NUM_POLY")?.max(0) as usize).min(POWER_CURVE_TERMS);
        let coordinate = get_string(&table, row, "TYPE")?;
        let (elevation, voltage) = match coordinate.as_str() {
            "POWER(ZA)" => (false, false),
            "POWER(EL)" => (true, false),
            "VOLTAGE(ZA)" => (false, true),
            "VOLTAGE(EL)" => (true, true),
            _ => {
                return Err(invalid(format!(
                    "unsupported gain-curve type {coordinate:?}"
                )));
            }
        };
        let array = table
            .cell_accessor(row, COL_GAIN)
            .and_then(|cell| cell.array())
            .map_err(|error| invalid(error.to_string()))?;
        let ArrayValue::Float32(raw) = array else {
            return Err(invalid(format!("GAIN_CURVE row {row} has non-float GAIN")));
        };
        if raw.ndim() != 2 || raw.shape()[0] != 2 || raw.shape()[1] < terms {
            return Err(invalid(format!(
                "GAIN_CURVE row {row} has an incompatible GAIN shape"
            )));
        }
        let sensitivity = get_f32_array_fortran(&table, row, "SENSITIVITY")?;
        let [sensitivity_r, sensitivity_l] = sensitivity.as_slice() else {
            return Err(invalid(format!(
                "GAIN_CURVE row {row} needs two sensitivity values"
            )));
        };
        let mut gain = [0.0; 2 * POWER_CURVE_TERMS];
        for receptor in 0..2 {
            let start = receptor * POWER_CURVE_TERMS;
            let polynomial = &mut gain[start..start + POWER_CURVE_TERMS];
            for (degree, term) in polynomial.iter_mut().enumerate().take(terms) {
                *term = raw[[receptor, degree]];
            }
            if elevation {
                let mut zenith = [0.0; POWER_CURVE_TERMS];
                for (degree, value) in zenith.iter_mut().enumerate() {
                    for (input_degree, coefficient) in polynomial.iter().enumerate().skip(degree) {
                        let scale = choose(input_degree, degree) as f64
                            * (-1.0_f64).powi(input_degree as i32)
                            * (-90.0_f64).powi((input_degree - degree) as i32);
                        *value += *coefficient * scale as f32;
                    }
                }
                polynomial.copy_from_slice(&zenith);
            }
        }
        if voltage {
            // CASA's EPowerCurve squares the first receptor into a cleared
            // vector. The second receptor remains zero in that legacy path.
            let mut power = [0.0; 2 * POWER_CURVE_TERMS];
            for (left, coefficient) in gain[..POWER_CURVE_TERMS].iter().enumerate() {
                for (right, other) in gain[..POWER_CURVE_TERMS - left].iter().enumerate() {
                    power[left + right] += coefficient * other;
                }
            }
            gain = power;
        }
        for (coefficients, scale) in gain
            .as_chunks_mut::<POWER_CURVE_TERMS>()
            .0
            .iter_mut()
            .zip([*sensitivity_r, *sensitivity_l])
        {
            for coefficient in coefficients {
                *coefficient *= scale;
            }
        }
        let time = get_f64(&table, row, COL_TIME)?;
        let half = get_f64(&table, row, "INTERVAL")? / 2.0;
        let begin = time - half;
        let end = time + half;
        curves.push(PriorCalRow {
            time_seconds: (begin + end) / 2.0,
            interval_seconds: (end - begin) / 2.0,
            field_id: -1,
            spw_id: window,
            antenna_id: antenna,
            observation_id: -1,
            scan_number: -1,
            parameters: f32_column(&gain),
            snr_value: 0.0,
            preserved: None,
        });
    }
    Ok(curves)
}

fn choose(n: usize, k: usize) -> usize {
    (0..k).fold(1, |value, step| value * (n - step) / (step + 1))
}
