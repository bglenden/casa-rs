// SPDX-License-Identifier: LGPL-3.0-or-later
//! Make the sources-and-lines fixture's MSv2 (#976,
//! `tools/ms-trial/make_lines_fixtures.sh`): a simulated observation of one
//! field in two spectral windows whose `SOURCE` states the field's source
//! with one line in the first window and two in the second, each with its
//! transition, rest frequency (LSRK) and systemic velocity (LSRK). As for
//! the trial's own MSv2 (`tools/ms-trial/make_fixtures.py`), `UVW`'s
//! `MEASINFO` names ITRF, as CASA writes it, which XRADIO needs to give
//! UVW the frame its schema requires.
//!
//! ```text
//! cargo run -p casa-ms --example ms_trial_lines -- MS
//! ```

use std::path::PathBuf;

use casa_ms::{
    MeasurementSet, SubtableId, SyntheticAntenna, SyntheticObservationRequest,
    SyntheticSpectralSetup, generate_synthetic_observation_ms,
};
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_tables::{ColumnSchema, RowRange};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};

/// The field's phase centre and its source's direction, radians (J2000).
const DIRECTION_RAD: [f64; 2] = [1.23, -0.45];

/// Start of the observation, MJD seconds, and its length, seconds.
const START_S: f64 = 59_000.25 * 86_400.0;
const DURATION_S: f64 = 65.0;

/// Each window's lines: transition, rest frequency (Hz) and systemic
/// velocity (m/s).
const LINES: [&[(&str, f64, f64)]; 2] = [
    &[("CO_6-5", 691.473_076_3e9, 1_500.0)],
    &[
        ("H2O_658", 658.006_55e9, 1_500.0),
        ("HCN_8-7", 708.877_21e9, 1_520.0),
    ],
];

fn floats(values: &[f64]) -> Value {
    Value::Array(ArrayValue::Float64(
        ndarray::ArrayD::from_shape_vec(vec![values.len()], values.to_vec())
            .expect("a vector's shape"),
    ))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .ok_or("usage: ms_trial_lines MS")?;
    let antennas = vec![
        SyntheticAntenna::vla("VLA01", "N01", [-1_601_185.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA02", "N02", [-1_601_085.4, -5_041_977.5, 3_554_875.9]),
        SyntheticAntenna::vla("VLA03", "N03", [-1_601_185.4, -5_041_877.5, 3_554_875.9]),
    ];
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        path.with_extension("unused.fits"),
        &path,
        antennas,
    );
    request.predict_model = false;
    request.phase_center_rad = DIRECTION_RAD;
    request.start_time_mjd_seconds = START_S;
    request.duration_seconds = DURATION_S;
    request.integration_seconds = 10.0;
    request.spectral_windows = vec![
        SyntheticSpectralSetup {
            name: "low".to_string(),
            start_frequency_hz: 672.0e9,
            channel_width_hz: 2.0e6,
            channel_count: 4,
        },
        SyntheticSpectralSetup {
            name: "high".to_string(),
            start_frequency_hz: 676.0e9,
            // Ascending: XRADIO's converter writes a descending window in
            // ascending order, and its partition would not bind.
            channel_width_hz: 1.0e6,
            channel_count: 3,
        },
    ];
    generate_synthetic_observation_ms(&request)?;

    let mut ms = MeasurementSet::open(&path)?;
    let main = ms.main_table_mut();
    let mut keywords = main.column_keywords("UVW").cloned().unwrap_or_default();
    match keywords.get_mut("MEASINFO") {
        Some(Value::Record(measinfo)) => match measinfo.get_mut("Ref") {
            Some(reference) => *reference = Value::Scalar(ScalarValue::String("ITRF".into())),
            None => measinfo.push(RecordField::new(
                "Ref",
                Value::Scalar(ScalarValue::String("ITRF".into())),
            )),
        },
        _ => return Err("UVW has no MEASINFO".into()),
    }
    main.set_column_keywords("UVW", keywords);
    ms.field_mut()?.table_mut().fill_column_range(
        "SOURCE_ID",
        RowRange::new(0, 1),
        Value::Scalar(ScalarValue::Int32(0)),
    )?;
    let source = ms.add_optional_subtable(SubtableId::Source)?;
    for (name, data_type) in [
        ("REST_FREQUENCY", PrimitiveType::Float64),
        ("SYSVEL", PrimitiveType::Float64),
        ("TRANSITION", PrimitiveType::String),
    ] {
        source.add_column(ColumnSchema::array_variable(name, data_type, Some(1)), None)?;
    }
    TableMeasDesc::new_fixed("REST_FREQUENCY", MeasureType::Frequency, "LSRK").write(source)?;
    TableMeasDesc::new_fixed("SYSVEL", MeasureType::RadialVelocity, "LSRK").write(source)?;
    for (window, lines) in LINES.iter().enumerate() {
        let int = |value: i32| Value::Scalar(ScalarValue::Int32(value));
        let float = |value: f64| Value::Scalar(ScalarValue::Float64(value));
        let transitions: Vec<String> = lines.iter().map(|line| line.0.to_string()).collect();
        source.add_row(RecordValue::new(vec![
            RecordField::new("CALIBRATION_GROUP", int(-1)),
            RecordField::new("CODE", Value::Scalar(ScalarValue::String(String::new()))),
            RecordField::new("DIRECTION", floats(&DIRECTION_RAD)),
            RecordField::new("INTERVAL", float(DURATION_S)),
            RecordField::new("NAME", Value::Scalar(ScalarValue::String("Lines".into()))),
            RecordField::new("NUM_LINES", int(i32::try_from(lines.len())?)),
            RecordField::new("PROPER_MOTION", floats(&[0.0, 0.0])),
            RecordField::new("SOURCE_ID", int(0)),
            RecordField::new("SPECTRAL_WINDOW_ID", int(i32::try_from(window)?)),
            RecordField::new("TIME", float(START_S + DURATION_S / 2.0)),
            RecordField::new(
                "REST_FREQUENCY",
                floats(&lines.iter().map(|line| line.1).collect::<Vec<_>>()),
            ),
            RecordField::new(
                "SYSVEL",
                floats(&lines.iter().map(|line| line.2).collect::<Vec<_>>()),
            ),
            RecordField::new(
                "TRANSITION",
                Value::Array(ArrayValue::String(ndarray::ArrayD::from_shape_vec(
                    vec![transitions.len()],
                    transitions,
                )?)),
            ),
        ]))?;
    }
    ms.save()?;
    Ok(())
}
