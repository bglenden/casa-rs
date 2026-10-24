// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration tables of simulated antenna-based terms: what CASA's
//! simulator writes for `sm.setgain(table=...)` and
//! `sm.setleakage(table=...)` (`SolvableVisCal::setSimulate` and
//! `storeNCT`), written here for `casa-simulation`.
//!
//! The layout is CASA 6.7.6's for a simulated `G Jones` or `D Jones` term
//! (measured): one row per solution and antenna, ANTENNA2 −1, INTERVAL 0,
//! SCAN_NUMBER −1, OBSERVATION_ID 0, CPARAM, PARAMERR, FLAG and SNR of one
//! channel per receptor (`[2, 1]`), PARAMERR and SNR zero, FLAG false, and
//! WEIGHT left undefined; the OBSERVATION, ANTENNA, FIELD, HISTORY and
//! SPECTRAL_WINDOW subtables are the MeasurementSet's. As CASA's
//! `NewCalTable::writeToDisk`, a table already at the output is replaced.

use std::fs;
use std::path::Path;

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{ColumnSchema, TableSchema};
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

use crate::constants::{
    COL_ANTENNA1, COL_ANTENNA2, COL_CPARAM, COL_FIELD_ID, COL_FLAG, COL_INTERVAL,
    COL_OBSERVATION_ID, COL_PARAMERR, COL_SCAN_NUMBER, COL_SNR, COL_SPECTRAL_WINDOW_ID, COL_TIME,
    COL_WEIGHT,
};
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriteError, CalibrationTableWriter,
};

/// Which simulated term a table holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SimulatedJones {
    /// `sm.setgain`: antenna gains, `G Jones`.
    Gain,
    /// `sm.setleakage`: polarization leakage, `D Jones`.
    Leakage,
}

impl SimulatedJones {
    /// The table's `VisCal` keyword and TableInfo subtype.
    #[must_use]
    pub fn vis_cal(self) -> &'static str {
        match self {
            Self::Gain => "G Jones",
            Self::Leakage => "D Jones",
        }
    }
}

/// One antenna's solution: a row of the table.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SimulatedSolution {
    /// TIME, seconds (MJD): the solution's time.
    pub time_s: f64,
    /// FIELD_ID of the solution.
    pub field_id: i32,
    /// SPECTRAL_WINDOW_ID of the solution.
    pub spectral_window_id: i32,
    /// ANTENNA1.
    pub antenna: i32,
    /// The receptors' values, CPARAM.
    pub receptors: [Complex32; 2],
}

/// Write `solutions` of `kind` as the calibration table `output` of the
/// MeasurementSet `ms`, replacing a table already there.
///
/// # Errors
///
/// `output` exists and is not a table, or the table cannot be written.
pub fn write_simulated_jones_table(
    ms: &MeasurementSet,
    output: &Path,
    kind: SimulatedJones,
    solutions: &[SimulatedSolution],
) -> Result<(), CalibrationTableWriteError> {
    replace_existing_table(output)?;
    let measurement_set_name = ms.path().and_then(Path::file_name).map_or_else(
        || "<in-memory>".to_string(),
        |name| name.to_string_lossy().into_owned(),
    );
    let mut writer = CalibrationTableWriter::create(
        ms,
        CalibrationTableDescriptor {
            output,
            schema: schema(),
            subtype: kind.vis_cal(),
            parameter_type: Some("Complex"),
            measurement_set_name,
            include_polarization_basis: true,
            time_extra_precision_column: None,
        },
    )?;
    for solution in solutions {
        writer.append(row(solution))?;
    }
    writer.finish(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
    ])
}

/// Remove the table at `output`, as CASA's `writeToDisk` replaces it;
/// refuse anything there that is not a table.
fn replace_existing_table(output: &Path) -> Result<(), CalibrationTableWriteError> {
    if !output.exists() {
        return Ok(());
    }
    let refuse = |reason: String| CalibrationTableWriteError::Prepare {
        path: output.display().to_string(),
        reason,
    };
    if !output.join("table.dat").is_file() {
        return Err(refuse("exists and is not a table".to_string()));
    }
    fs::remove_dir_all(output).map_err(|error| refuse(format!("cannot replace it: {error}")))
}

fn schema() -> TableSchema {
    TableSchema::new(vec![
        ColumnSchema::scalar(COL_TIME, PrimitiveType::Float64),
        ColumnSchema::scalar(COL_FIELD_ID, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_SPECTRAL_WINDOW_ID, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA1, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_ANTENNA2, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_INTERVAL, PrimitiveType::Float64),
        ColumnSchema::scalar(COL_SCAN_NUMBER, PrimitiveType::Int32),
        ColumnSchema::scalar(COL_OBSERVATION_ID, PrimitiveType::Int32),
        ColumnSchema::array_variable(COL_CPARAM, PrimitiveType::Complex32, None),
        ColumnSchema::array_variable(COL_PARAMERR, PrimitiveType::Float32, None),
        ColumnSchema::array_variable(COL_FLAG, PrimitiveType::Bool, None),
        ColumnSchema::array_variable(COL_SNR, PrimitiveType::Float32, None),
        ColumnSchema::array_variable(COL_WEIGHT, PrimitiveType::Float32, None),
    ])
    .expect("valid simulated calibration schema")
}

/// A `[receptor, channel]` cell of two receptors and one channel.
fn cell<T>(values: [T; 2]) -> ArrayD<T> {
    ArrayD::from_shape_vec(IxDyn(&[2, 1]).f(), Vec::from(values))
        .expect("two receptors by one channel")
}

fn row(solution: &SimulatedSolution) -> RecordValue {
    let int =
        |name: &str, value: i32| RecordField::new(name, Value::Scalar(ScalarValue::Int32(value)));
    let double =
        |name: &str, value: f64| RecordField::new(name, Value::Scalar(ScalarValue::Float64(value)));
    RecordValue::new(vec![
        double(COL_TIME, solution.time_s),
        int(COL_FIELD_ID, solution.field_id),
        int(COL_SPECTRAL_WINDOW_ID, solution.spectral_window_id),
        int(COL_ANTENNA1, solution.antenna),
        int(COL_ANTENNA2, -1),
        double(COL_INTERVAL, 0.0),
        int(COL_SCAN_NUMBER, -1),
        int(COL_OBSERVATION_ID, 0),
        RecordField::new(
            COL_CPARAM,
            Value::Array(ArrayValue::Complex32(cell(solution.receptors))),
        ),
        RecordField::new(
            COL_PARAMERR,
            Value::Array(ArrayValue::Float32(cell([0.0; 2]))),
        ),
        RecordField::new(COL_FLAG, Value::Array(ArrayValue::Bool(cell([false; 2])))),
        RecordField::new(COL_SNR, Value::Array(ArrayValue::Float32(cell([0.0; 2])))),
    ])
}
