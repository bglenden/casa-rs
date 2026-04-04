// SPDX-License-Identifier: LGPL-3.0-or-later
#![allow(dead_code)]

use std::path::{Path, PathBuf};
#[cfg(feature = "slow-tests")]
use std::process::Command;

use casacore_tables::{ColumnSchema, DataManagerKind, Table, TableInfo, TableOptions, TableSchema};
#[cfg(feature = "slow-tests")]
use casacore_test_support::casatestdata_path;
use casacore_types::{ArrayValue, Complex32, RecordField, RecordValue, ScalarValue, Value};

pub fn create_minimal_complex_caltable(root: &Path) -> PathBuf {
    let schema = TableSchema::new(vec![
        ColumnSchema::scalar("TIME", casacore_types::PrimitiveType::Float64),
        ColumnSchema::scalar("FIELD_ID", casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar("SPECTRAL_WINDOW_ID", casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA1", casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar("ANTENNA2", casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar("INTERVAL", casacore_types::PrimitiveType::Float64),
        ColumnSchema::scalar("SCAN_NUMBER", casacore_types::PrimitiveType::Int32),
        ColumnSchema::scalar("OBSERVATION_ID", casacore_types::PrimitiveType::Int32),
        ColumnSchema::array_variable("CPARAM", casacore_types::PrimitiveType::Complex32, Some(1)),
        ColumnSchema::array_variable("PARAMERR", casacore_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("FLAG", casacore_types::PrimitiveType::Bool, Some(1)),
        ColumnSchema::array_variable("SNR", casacore_types::PrimitiveType::Float32, Some(1)),
        ColumnSchema::array_variable("WEIGHT", casacore_types::PrimitiveType::Float32, Some(1)),
    ])
    .expect("valid schema");
    let mut table = Table::with_schema(schema);
    table.set_info(TableInfo {
        table_type: "Calibration".to_string(),
        sub_type: "G Jones".to_string(),
    });
    table.keywords_mut().upsert(
        "ParType",
        Value::Scalar(ScalarValue::String("Complex".to_string())),
    );
    table.keywords_mut().upsert(
        "VisCal",
        Value::Scalar(ScalarValue::String("G Jones".to_string())),
    );
    table.keywords_mut().upsert(
        "MSName",
        Value::Scalar(ScalarValue::String("synthetic.ms".to_string())),
    );
    table.keywords_mut().upsert(
        "PolBasis",
        Value::Scalar(ScalarValue::String("unknown".to_string())),
    );
    table.keywords_mut().upsert(
        "CASA_Version",
        Value::Scalar(ScalarValue::String("test".to_string())),
    );
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        table.keywords_mut().upsert(name, Value::table_ref(name));
    }
    table
        .add_row(RecordValue::new(vec![
            RecordField::new("TIME", Value::Scalar(ScalarValue::Float64(1.0))),
            RecordField::new("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(3))),
            RecordField::new("ANTENNA1", Value::Scalar(ScalarValue::Int32(1))),
            RecordField::new("ANTENNA2", Value::Scalar(ScalarValue::Int32(-1))),
            RecordField::new("INTERVAL", Value::Scalar(ScalarValue::Float64(30.0))),
            RecordField::new("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(7))),
            RecordField::new("OBSERVATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new(
                "CPARAM",
                Value::Array(ArrayValue::from_complex32_vec(vec![
                    Complex32 { re: 1.0, im: 0.0 },
                    Complex32 { re: 0.0, im: 1.0 },
                ])),
            ),
            RecordField::new(
                "PARAMERR",
                Value::Array(ArrayValue::from_f32_vec(vec![0.1, 0.1])),
            ),
            RecordField::new(
                "FLAG",
                Value::Array(ArrayValue::from_bool_vec(vec![false, false])),
            ),
            RecordField::new(
                "SNR",
                Value::Array(ArrayValue::from_f32_vec(vec![10.0, 11.0])),
            ),
            RecordField::new(
                "WEIGHT",
                Value::Array(ArrayValue::from_f32_vec(vec![1.0, 1.0])),
            ),
        ]))
        .expect("row insert");
    table
        .save(TableOptions::new(root).with_data_manager(DataManagerKind::StandardStMan))
        .expect("save synthetic caltable");

    let empty_schema = TableSchema::new(vec![]).expect("empty schema");
    for name in [
        "OBSERVATION",
        "ANTENNA",
        "FIELD",
        "SPECTRAL_WINDOW",
        "HISTORY",
    ] {
        Table::with_schema(empty_schema.clone())
            .save(TableOptions::new(root.join(name)))
            .expect("save subtable");
    }
    root.to_path_buf()
}

#[cfg(feature = "slow-tests")]
pub fn discover_casa_python() -> Option<PathBuf> {
    let mut candidates = Vec::new();
    for key in ["CASA_RS_CASA_PYTHON", "CASA_PYTHON"] {
        if let Some(value) = std::env::var_os(key) {
            candidates.push(PathBuf::from(value));
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        candidates.push(
            PathBuf::from(home)
                .join("SoftwareProjects")
                .join("casa-build")
                .join("venv")
                .join("bin")
                .join("python"),
        );
    }
    candidates.push(PathBuf::from("python3"));
    candidates.push(PathBuf::from("python"));

    candidates.into_iter().find(|program| {
        Command::new(program)
            .arg("-c")
            .arg("import casatasks")
            .output()
            .is_ok_and(|output| output.status.success())
    })
}

#[cfg(feature = "slow-tests")]
pub fn ngc5921_ms_path() -> Option<PathBuf> {
    casatestdata_path("measurementset/vla/ngc5921.ms").filter(|path| path.exists())
}

#[cfg(feature = "slow-tests")]
pub fn casa_skip_reason() -> String {
    match (discover_casa_python(), ngc5921_ms_path()) {
        (None, _) => {
            "CASA calibration parity skipped: no CASA-capable python found via CASA_RS_CASA_PYTHON, CASA_PYTHON, python3, or python".to_string()
        }
        (_, None) => {
            "CASA calibration parity skipped: missing ngc5921.ms under CASA_RS_TESTDATA_ROOT, ../casatestdata, or ~/SoftwareProjects/casatestdata".to_string()
        }
        _ => "CASA calibration parity skipped".to_string(),
    }
}

#[cfg(feature = "slow-tests")]
pub fn generate_casa_exemplars(output_dir: &Path) -> Result<(PathBuf, PathBuf, PathBuf), String> {
    let python = discover_casa_python().ok_or_else(casa_skip_reason)?;
    let ms_path = ngc5921_ms_path().ok_or_else(casa_skip_reason)?;
    let script = r#"
import os
from casatasks import bandpass, gaincal

vis = os.environ["CASA_RS_CAL_MS"]
out = os.environ["CASA_RS_CAL_OUT"]
phase = os.path.join(out, "phase.gcal")
tsolve = os.path.join(out, "t.gcal")
bp = os.path.join(out, "b.bcal")

gaincal(
    vis=vis,
    caltable=phase,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    minsnr=0.0,
)
gaincal(
    vis=vis,
    caltable=tsolve,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    calmode="p",
    gaintype="T",
    minsnr=0.0,
)
bandpass(
    vis=vis,
    caltable=bp,
    field="0",
    spw="0",
    solint="inf",
    refant="VA15",
    bandtype="B",
    gaintable=[phase],
    minsnr=0.0,
)
"#;
    let output = Command::new(&python)
        .env("CASA_RS_CAL_MS", &ms_path)
        .env("CASA_RS_CAL_OUT", output_dir)
        .arg("-c")
        .arg(script)
        .output()
        .map_err(|error| format!("spawn {}: {error}", python.display()))?;
    if !output.status.success() {
        return Err(format!(
            "CASA exemplar generation failed:\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    Ok((
        output_dir.join("phase.gcal"),
        output_dir.join("t.gcal"),
        output_dir.join("b.bcal"),
    ))
}
