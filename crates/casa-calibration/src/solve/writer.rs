// SPDX-License-Identifier: LGPL-3.0-or-later
//! On-disk caltable writing for limited `gaincal`.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use casacore_ms::MsError;
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::SubtableId;
use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableInfo, TableOptions, TableSchema};
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use super::{GainSolveError, GainSolveReport, GainSolveRequest};
use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    COL_WEIGHT, KEY_CASA_VERSION, KEY_MS_NAME, KEY_PAR_TYPE, KEY_POL_BASIS, KEY_VIS_CAL,
    STANDARD_SUBTABLE_KEYWORDS, TABLE_INFO_TYPE,
};
use crate::solve::kernel::SolutionRow;

const COL_ARRAY_ID: &str = "ARRAY_ID";
const COL_TIME_EXTRA_PREC: &str = "TIME_EXTRA_PREC";

pub(crate) fn write_gain_caltable(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    refant_id: i32,
    rows: &[SolutionRow],
) -> Result<GainSolveReport, GainSolveError> {
    prepare_output_root(&request.output_table)?;

    let schema = TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, casacore_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, casacore_types::PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ARRAY_ID, casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar(COL_TIME_EXTRA_PREC, casacore_types::PrimitiveType::Float64),
        ColumnSchema::array_variable(
            COL_CPARAM,
            casacore_types::PrimitiveType::Complex32,
            Some(2),
        ),
        ColumnSchema::array_variable(
            COL_PARAMERR,
            casacore_types::PrimitiveType::Float32,
            Some(2),
        ),
        ColumnSchema::array_variable(COL_FLAG, casacore_types::PrimitiveType::Bool, Some(2)),
        ColumnSchema::array_variable(COL_SNR, casacore_types::PrimitiveType::Float32, Some(2)),
        ColumnSchema::array_variable(COL_WEIGHT, casacore_types::PrimitiveType::Float32, Some(2)),
    ])
    .expect("valid gain caltable schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: TABLE_INFO_TYPE.to_string(),
        sub_type: request.gain_type.vis_cal().to_string(),
    });
    table.keywords_mut().upsert(
        KEY_PAR_TYPE,
        Value::Scalar(ScalarValue::String("Complex".to_string())),
    );
    table.keywords_mut().upsert(
        KEY_VIS_CAL,
        Value::Scalar(ScalarValue::String(request.gain_type.vis_cal().to_string())),
    );
    table.keywords_mut().upsert(
        KEY_MS_NAME,
        Value::Scalar(ScalarValue::String(
            ms.path()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "<in-memory>".to_string()),
        )),
    );
    table.keywords_mut().upsert(
        KEY_POL_BASIS,
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        KEY_CASA_VERSION,
        Value::Scalar(ScalarValue::String("casa-rs".to_string())),
    );
    set_fixed_unit_keyword(&mut table, COL_TIME, &["s"]);
    set_measinfo_keyword(&mut table, COL_TIME, "epoch", Some("UTC"));
    set_fixed_unit_keyword(&mut table, COL_INTERVAL, &["s"]);
    set_fixed_unit_keyword(&mut table, COL_TIME_EXTRA_PREC, &["s"]);
    for name in STANDARD_SUBTABLE_KEYWORDS {
        table.keywords_mut().upsert(
            *name,
            Value::table_ref(subtable_keyword_value(
                &request.output_table,
                &request.output_table.join(name),
            )),
        );
    }

    for row in rows {
        table
            .add_row(RecordValue::new(vec![
                RecordField::new(
                    COL_TIME,
                    Value::Scalar(ScalarValue::Float64(row.time_seconds)),
                ),
                RecordField::new(
                    COL_FIELD_ID,
                    Value::Scalar(ScalarValue::Int32(row.field_id)),
                ),
                RecordField::new(
                    COL_SPECTRAL_WINDOW_ID,
                    Value::Scalar(ScalarValue::Int32(row.spw_id)),
                ),
                RecordField::new(
                    COL_ANTENNA1,
                    Value::Scalar(ScalarValue::Int32(row.antenna_id)),
                ),
                RecordField::new(COL_ANTENNA2, Value::Scalar(ScalarValue::Int32(-1))),
                RecordField::new(
                    COL_INTERVAL,
                    Value::Scalar(ScalarValue::Float64(row.interval_seconds)),
                ),
                RecordField::new(
                    COL_SCAN_NUMBER,
                    Value::Scalar(ScalarValue::Int32(row.scan_number)),
                ),
                RecordField::new(
                    COL_OBSERVATION_ID,
                    Value::Scalar(ScalarValue::Int32(row.observation_id)),
                ),
                RecordField::new(COL_ARRAY_ID, Value::Scalar(ScalarValue::Int32(0))),
                RecordField::new(
                    COL_TIME_EXTRA_PREC,
                    Value::Scalar(ScalarValue::Float64(0.0)),
                ),
                RecordField::new(
                    COL_CPARAM,
                    Value::Array(ArrayValue::Complex32(
                        ArrayD::from_shape_vec(IxDyn(&[row.gains.len(), 1]).f(), row.gains.clone())
                            .expect("gain vector should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_PARAMERR,
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[row.gains.len(), 1]).f(),
                            vec![0.0; row.gains.len()],
                        )
                        .expect("paramerr vector should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_FLAG,
                    Value::Array(ArrayValue::Bool(
                        ArrayD::from_shape_vec(IxDyn(&[row.flags.len(), 1]).f(), row.flags.clone())
                            .expect("flag vector should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_SNR,
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[row.gains.len(), 1]).f(),
                            vec![1.0; row.gains.len()],
                        )
                        .expect("snr vector should reshape to receptor x channel"),
                    )),
                ),
                RecordField::new(
                    COL_WEIGHT,
                    Value::Array(ArrayValue::Float32(
                        ArrayD::from_shape_vec(
                            IxDyn(&[row.gains.len(), 1]).f(),
                            vec![1.0; row.gains.len()],
                        )
                        .expect("weight vector should reshape to receptor x channel"),
                    )),
                ),
            ]))
            .expect("insert gain solution row");
    }

    table
        .save(
            TableOptions::new(&request.output_table)
                .with_data_manager(DataManagerKind::StandardStMan),
        )
        .map_err(|source| GainSolveError::SaveCalibrationTable {
            path: request.output_table.display().to_string(),
            source: Box::new(source),
        })?;

    for (id, name) in [
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
    ] {
        ms.subtable(id)
            .expect("required subtable available")
            .save(TableOptions::new(request.output_table.join(name)))
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: name.to_string(),
                path: request.output_table.display().to_string(),
                source: Box::new(source),
            })?;
    }
    write_gain_spectral_window_subtable(ms, &request.output_table, rows)?;

    let field_ids = rows.iter().map(|row| row.field_id).collect::<BTreeSet<_>>();
    let spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    Ok(GainSolveReport {
        output_table: request.output_table.clone(),
        gain_type: request.gain_type,
        refant_antenna_id: refant_id,
        field_ids: field_ids.iter().copied().collect(),
        spectral_window_ids: spw_ids.iter().copied().collect(),
        solution_row_count: rows.len(),
    })
}

pub(crate) fn prepare_output_root(path: &Path) -> Result<(), GainSolveError> {
    if path.exists() {
        fs::remove_dir_all(path)
            .or_else(|_| fs::remove_file(path))
            .map_err(|error| GainSolveError::PrepareOutput {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| GainSolveError::PrepareOutput {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    Ok(())
}

pub(crate) fn set_fixed_unit_keyword(table: &mut Table, column: &str, units: &[&str]) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    keywords.upsert(
        "QuantumUnits",
        Value::Array(ArrayValue::from_string_vec(
            units.iter().map(|unit| (*unit).to_string()).collect(),
        )),
    );
    table.set_column_keywords(column, keywords);
}

pub(crate) fn set_measinfo_keyword(
    table: &mut Table,
    column: &str,
    measure_type: &str,
    measure_ref: Option<&str>,
) {
    let mut keywords = table.column_keywords(column).cloned().unwrap_or_default();
    let mut fields = vec![RecordField::new(
        "type",
        Value::Scalar(ScalarValue::String(measure_type.to_string())),
    )];
    if let Some(measure_ref) = measure_ref {
        fields.push(RecordField::new(
            "Ref",
            Value::Scalar(ScalarValue::String(measure_ref.to_string())),
        ));
    }
    keywords.upsert("MEASINFO", Value::Record(RecordValue::new(fields)));
    table.set_column_keywords(column, keywords);
}

fn write_gain_spectral_window_subtable(
    ms: &MeasurementSet,
    output_root: &Path,
    rows: &[SolutionRow],
) -> Result<(), GainSolveError> {
    let destination = output_root.join("SPECTRAL_WINDOW");
    ms.subtable(SubtableId::SpectralWindow)
        .expect("required subtable available")
        .save(TableOptions::new(&destination))
        .map_err(|source| GainSolveError::CopySubtable {
            subtable: "SPECTRAL_WINDOW".to_string(),
            path: output_root.display().to_string(),
            source: Box::new(source),
        })?;

    let solved_spw_ids = rows.iter().map(|row| row.spw_id).collect::<BTreeSet<_>>();
    let mut table = Table::open(TableOptions::new(&destination)).map_err(|source| {
        GainSolveError::CopySubtable {
            subtable: "SPECTRAL_WINDOW".to_string(),
            path: output_root.display().to_string(),
            source: Box::new(source),
        }
    })?;

    for spw_id in solved_spw_ids {
        let row =
            usize::try_from(spw_id).map_err(|_| GainSolveError::UnsupportedParameterShape {
                path: "SPECTRAL_WINDOW row id".to_string(),
                shape: vec![spw_id as usize],
            })?;
        let chan_freq = get_f64_array(&table, row, "CHAN_FREQ")?;
        let chan_width = get_f64_array(&table, row, "CHAN_WIDTH")?;
        let effective_bw = get_f64_array(&table, row, "EFFECTIVE_BW")?;
        let resolution = get_f64_array(&table, row, "RESOLUTION")?;

        if chan_freq.len() <= 1 {
            continue;
        }

        let center_index = chan_freq.len() / 2;
        table
            .set_cell(row, "NUM_CHAN", Value::Scalar(ScalarValue::Int32(1)))
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .set_cell(
                row,
                "CHAN_FREQ",
                Value::Array(f64_array(&[chan_freq[center_index]])),
            )
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .set_cell(
                row,
                "CHAN_WIDTH",
                Value::Array(f64_array(&[chan_width.iter().copied().sum()])),
            )
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .set_cell(
                row,
                "EFFECTIVE_BW",
                Value::Array(f64_array(&[effective_bw.iter().copied().sum()])),
            )
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
        table
            .set_cell(
                row,
                "RESOLUTION",
                Value::Array(f64_array(&[resolution.iter().copied().sum()])),
            )
            .map_err(|source| GainSolveError::CopySubtable {
                subtable: "SPECTRAL_WINDOW".to_string(),
                path: output_root.display().to_string(),
                source: Box::new(source),
            })?;
    }

    table
        .save(TableOptions::new(&destination))
        .map_err(|source| GainSolveError::CopySubtable {
            subtable: "SPECTRAL_WINDOW".to_string(),
            path: output_root.display().to_string(),
            source: Box::new(source),
        })?;

    Ok(())
}

fn get_f64_array(table: &Table, row: usize, column: &str) -> Result<Vec<f64>, GainSolveError> {
    match table
        .get_array_cell(row, column)
        .map_err(MsError::from)
        .map_err(|source| GainSolveError::OpenMeasurementSet {
            path: column.to_string(),
            source,
        })? {
        ArrayValue::Float64(values) => Ok(values.iter().copied().collect()),
        other => Err(GainSolveError::UnsupportedParameterShape {
            path: format!("{column} array"),
            shape: other.shape().to_vec(),
        }),
    }
}

fn f64_array(values: &[f64]) -> ArrayValue {
    ArrayValue::Float64(
        ArrayD::from_shape_vec(IxDyn(&[values.len()]).f(), values.to_vec())
            .expect("f64 vector should reshape to 1-D array"),
    )
}

pub(crate) fn subtable_keyword_value(base_path: &Path, subtable_path: &Path) -> String {
    if let Ok(relative) = subtable_path.strip_prefix(base_path) {
        let rel = relative.to_string_lossy();
        return format!("././{rel}");
    }
    if let Some(parent) = base_path.parent()
        && let Ok(relative) = subtable_path.strip_prefix(parent)
    {
        let rel = relative.to_string_lossy();
        return format!("./{rel}");
    }
    if subtable_path.is_relative() {
        let rel = subtable_path.to_string_lossy();
        return format!("././{}", rel.trim_start_matches("./"));
    }
    subtable_path.to_string_lossy().to_string()
}
