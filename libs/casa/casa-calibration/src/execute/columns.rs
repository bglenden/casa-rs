// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CorrectedDataCreation {
    pub(super) created: bool,
}

pub(super) fn ensure_corrected_data_column(
    ms: &mut MeasurementSet,
    observed_column: &str,
) -> Result<CorrectedDataCreation, TableError> {
    if ms
        .main_table()
        .schema()
        .is_some_and(|schema| schema.contains_column(VisibilityDataColumn::CorrectedData.name()))
    {
        return Ok(CorrectedDataCreation { created: false });
    }
    if observed_column == "FLOAT_DATA"
        && let Some(path) = ms.path()
    {
        let lock = casa_tables::TableWriteLock::acquire_to_reread(path, 1)?;
        crate::model_columns::initialize_calibrater_columns(ms, false, true, false, lock).map_err(
            |error| match error {
                MsError::Table(error) => error,
                other => TableError::Storage(other.to_string()),
            },
        )?;
        return Ok(CorrectedDataCreation { created: true });
    }

    // CASA's `addScrCols` clears the cached sort keywords first.
    casa_ms::prepare_write(ms, VisibilityDataColumn::CorrectedData, true).map_err(|error| {
        match error {
            MsError::Table(error) => error,
            other => TableError::Storage(other.to_string()),
        }
    })?;
    let column_def = *VisibilityDataColumn::CorrectedData
        .optional_column()
        .column_def();
    let schema = build_table_schema(&[column_def]).expect("single optional column schema");
    let column: ColumnSchema = schema
        .column(VisibilityDataColumn::CorrectedData.name())
        .expect("corrected data column present")
        .clone();
    ms.main_table_mut().add_column(column, None)?;
    if let Some(data_keywords) = ms.main_table().column_keywords(observed_column).cloned() {
        ms.main_table_mut()
            .set_column_keywords(VisibilityDataColumn::CorrectedData.name(), data_keywords);
    }

    if ms.path().is_none() {
        let data_rows = (0..ms.row_count())
            .map(|row_index| {
                let data = ms
                    .main_table()
                    .column_accessor(observed_column)?
                    .array_cell(row_index)?
                    .clone();
                Ok(match data {
                    ArrayValue::Float32(values) => complex_visibility_from_float(&values),
                    data => data,
                })
            })
            .collect::<Result<Vec<_>, casa_tables::TableError>>()?;
        let mut writer = ms
            .main_table_mut()
            .row_accessor_mut()
            .prepare(&[VisibilityDataColumn::CorrectedData.name()])?;
        let slot = writer
            .column_index(VisibilityDataColumn::CorrectedData.name())
            .expect("prepared CORRECTED_DATA slot");
        for (row_index, data) in data_rows.into_iter().enumerate() {
            writer.seek(row_index)?;
            writer.set_value_at(slot, Value::Array(data))?;
        }
    }

    Ok(CorrectedDataCreation { created: true })
}

pub(super) fn apply_observed_column(ms: &MeasurementSet, prefer_float: bool) -> &'static str {
    if prefer_float || !crate::model_columns::has_column(ms, VisibilityDataColumn::Data) {
        crate::model_columns::observed_column(ms)
    } else {
        VisibilityDataColumn::Data.name()
    }
}

pub(super) fn complex_visibility_from_float(values: &ArrayD<f32>) -> ArrayValue {
    ArrayValue::Complex32(values.mapv(|value| Complex32::new(value, 0.)))
}

pub(super) fn display_ms_path(ms: &MeasurementSet) -> String {
    ms.path()
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "<in-memory>".to_string())
}

pub(super) fn open_append_file(path: &Path) -> Option<File> {
    OpenOptions::new().create(true).append(true).open(path).ok()
}

pub(super) fn expand_weight_to_spectrum(weight: &ArrayD<f32>, channel_count: usize) -> ArrayD<f32> {
    let mut expanded = ArrayD::from_elem(IxDyn(&[weight.shape()[0], channel_count]).f(), 0.0_f32);
    for corr_index in 0..weight.shape()[0] {
        for chan_index in 0..channel_count {
            expanded[[corr_index, chan_index]] = weight[[corr_index]];
        }
    }
    expanded
}

pub(super) fn stokes_name(code: i32) -> &'static str {
    match code {
        5 => "RR",
        6 => "RL",
        7 => "LR",
        8 => "LL",
        9 => "XX",
        10 => "XY",
        11 => "YX",
        12 => "YY",
        _ => "??",
    }
}

pub(super) fn correlation_receptors(code: i32) -> Option<(usize, usize)> {
    match code {
        5 | 9 => Some((0, 0)),
        6 | 10 => Some((0, 1)),
        7 | 11 => Some((1, 0)),
        8 | 12 => Some((1, 1)),
        _ => None,
    }
}

pub(super) fn get_i32(
    table: &Table,
    row_index: usize,
    column: &str,
) -> Result<i32, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::Int32(value) => Ok(*value),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

pub(super) fn get_f64(
    table: &Table,
    row_index: usize,
    column: &str,
) -> Result<f64, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::Float64(value) => Ok(*value),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

pub(super) fn get_complex32(
    table: &Table,
    row_index: usize,
    column: &str,
) -> Result<Complex32, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::Complex32(value) => Ok(*value),
        ScalarValue::Complex64(value) => Ok(Complex32::new(value.re as f32, value.im as f32)),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

pub(super) fn get_string(
    table: &Table,
    row_index: usize,
    column: &str,
) -> Result<String, ApplyExecutionError> {
    match table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.scalar())
        .map_err(|source| ApplyExecutionError::MutateMeasurementSet {
            path: "<table>".to_string(),
            source: MsError::from(source),
        })? {
        ScalarValue::String(value) => Ok(value.clone()),
        other => Err(ApplyExecutionError::UnsupportedParameterShape {
            path: format!("{column}:{:?}", other.primitive_type()),
            shape: vec![],
        }),
    }
}

pub(super) fn get_numeric_array(
    table: &Table,
    row_index: usize,
    column: &str,
    path: &Path,
) -> Result<Vec<f64>, ApplyExecutionError> {
    let values = table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.array())
        .map_err(|source| ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("failed to read {column} row {row_index}: {source}"),
        })?;
    match values {
        ArrayValue::Float32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        ArrayValue::Int32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        other => Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "{column} row {row_index} had unexpected array type {:?}",
                other.primitive_type()
            ),
        }),
    }
}

pub(super) fn get_f64_array(
    table: &Table,
    row_index: usize,
    column: &str,
    path: &Path,
) -> Result<Vec<f64>, ApplyExecutionError> {
    let values = table
        .cell_accessor(row_index, column)
        .and_then(|cell| cell.array())
        .map_err(|source| ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!("failed to read {column} row {row_index}: {source}"),
        })?;
    match values {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        ArrayValue::Float32(values) => Ok(values.iter().map(|value| f64::from(*value)).collect()),
        other => Err(ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason: format!(
                "{column} row {row_index} had unexpected array type {:?}",
                other.primitive_type()
            ),
        }),
    }
}
