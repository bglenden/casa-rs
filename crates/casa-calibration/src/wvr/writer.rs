// SPDX-License-Identifier: LGPL-3.0-or-later
//! Publish per-antenna path solutions as CASA T Jones tables.

use std::path::Path;

use casa_core::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use casa_ms::schema::{SubtableId, history};
use casa_ms::{MeasurementSet, MsError, build_table_schema};
use casa_tables::{Table, TableOptions};
use ndarray::{ArrayD, IxDyn};

use super::paths::gain;
use super::{WvrError, WvrRequest};
use crate::CalibrationOutputMode;
use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
};
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriteError, CalibrationTableWriter,
    SingleChannelFrequency, make_windows_single_channel, new_calibration_table_schema,
};

pub(super) struct AntennaPaths {
    pub time_seconds: f64,
    pub field_id: i32,
    pub meters: Vec<f64>,
}

pub(super) struct PhaseWindow {
    pub id: i32,
    pub frequency_hz: f64,
    pub reverse: bool,
    pub dispersion: f64,
}

pub(super) fn publish(
    ms: &MeasurementSet,
    request: &WvrRequest,
    integrations: &[AntennaPaths],
    windows: &[PhaseWindow],
    mode: CalibrationOutputMode,
) -> Result<(), WvrError> {
    let subtables = [
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
    ];
    for &(id, name) in &subtables {
        if ms.subtable(id).is_none() {
            return Err(MsError::MissingSubtable(name.into()).into());
        }
    }
    let output = &request.output_table;
    let ms_name = ms
        .path()
        .and_then(Path::file_name)
        .map(|part| part.to_string_lossy())
        .unwrap_or_else(|| "<in-memory>".into());
    let mut writer = CalibrationTableWriter::with_mode(
        ms,
        CalibrationTableDescriptor {
            output,
            schema: new_calibration_table_schema(true),
            subtype: "T Jones",
            parameter_type: Some("Complex"),
            measurement_set_name: ms_name.into_owned(),
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
        mode,
    )?;
    let interval = integrations.get(1).map_or(0.0, |second| {
        second.time_seconds - integrations[0].time_seconds
    });
    for integration in integrations {
        for window in windows {
            for (antenna, &path) in integration.meters.iter().enumerate() {
                writer.append(solution_row(
                    integration,
                    window,
                    interval,
                    antenna as i32,
                    path,
                ))?;
            }
        }
    }
    writer.copy_subtables(&subtables)?;
    make_windows_single_channel(
        &writer.output_path().join("SPECTRAL_WINDOW"),
        SingleChannelFrequency::MiddleChannel,
    )?;
    write_history(&writer.output_path().join("HISTORY"), request)?;
    Ok(writer.save_main()?)
}

fn write_history(path: &Path, request: &WvrRequest) -> Result<(), CalibrationTableWriteError> {
    let options = TableOptions::new(path);
    let failure = |source| CalibrationTableWriteError::Save {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let schema = build_table_schema(history::REQUIRED_COLUMNS).expect("valid WVR history schema");
    let mut table = Table::with_schema(schema);
    crate::writer::set_fixed_unit_keyword(&mut table, COL_TIME, &["s"]);
    crate::writer::set_measinfo_keyword(&mut table, COL_TIME, "epoch", Some("UTC"));
    let mut row = RecordValue::new(Vec::new());
    let parameters = serde_json::to_string(request).expect("WVR requests are serializable");
    for (column, text) in [
        ("APPLICATION", "Produced with casa-rs WVR calibration"),
        ("MESSAGE", &parameters),
        ("ORIGIN", "wvrgcal"),
        ("PRIORITY", "INFO"),
    ] {
        row.upsert(column, Value::Scalar(ScalarValue::String(text.into())));
    }
    for name in ["OBSERVATION_ID", "OBJECT_ID"] {
        row.upsert(name, Value::Scalar(ScalarValue::Int32(-1)));
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0.0, |duration| duration.as_secs_f64());
    row.upsert(
        "TIME",
        Value::Scalar(ScalarValue::Float64(now + 40_587.0 * 86_400.0)),
    );
    table.add_row(row).map_err(failure)?;
    table.save(options).map_err(failure)
}

fn solution_row(
    integration: &AntennaPaths,
    window: &PhaseWindow,
    interval: f64,
    antenna: i32,
    path: f64,
) -> RecordValue {
    let (correction, flagged) = gain(path, window.frequency_hz, window.reverse, window.dispersion);
    let scalar = |name, value| RecordField::new(name, Value::Scalar(value));
    let array = |name, value| RecordField::new(name, Value::Array(value));
    let shape = || IxDyn(&[1, 1]);
    RecordValue::new(vec![
        scalar(COL_TIME, ScalarValue::Float64(integration.time_seconds)),
        scalar(COL_FIELD_ID, ScalarValue::Int32(integration.field_id)),
        scalar(COL_SPECTRAL_WINDOW_ID, ScalarValue::Int32(window.id)),
        scalar(COL_ANTENNA1, ScalarValue::Int32(antenna)),
        scalar(COL_ANTENNA2, ScalarValue::Int32(-1)),
        scalar(COL_INTERVAL, ScalarValue::Float64(interval)),
        scalar(COL_SCAN_NUMBER, ScalarValue::Int32(-1)),
        scalar(COL_OBSERVATION_ID, ScalarValue::Int32(0)),
        array(
            COL_CPARAM,
            ArrayValue::Complex32(ArrayD::from_elem(shape(), correction)),
        ),
        array(
            COL_FLAG,
            ArrayValue::Bool(ArrayD::from_elem(shape(), flagged)),
        ),
        array(
            COL_PARAMERR,
            ArrayValue::Float32(ArrayD::from_elem(shape(), 0.0)),
        ),
        array(
            COL_SNR,
            ArrayValue::Float32(ArrayD::from_elem(shape(), 3.0)),
        ),
    ])
}
