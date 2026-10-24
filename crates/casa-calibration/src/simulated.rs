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
//! WEIGHT left undefined; the OBSERVATION, ANTENNA, FIELD and HISTORY
//! subtables are the MeasurementSet's, and its SPECTRAL_WINDOW with each
//! window reduced to one channel. As CASA's `NewCalTable::writeToDisk`, a
//! table already at the output is replaced; the new table is built beside
//! it and the old one removed only once the new one is in place. An output
//! that is the MeasurementSet, lies inside it, or contains it is refused
//! before anything is touched (#891 review).
//!
//! The table is built in memory and saved: [`simulated_table_bytes`] is
//! what that holds, for the caller to admit.

use std::path::{Component, Path, PathBuf};

use casa_ms::ms::MeasurementSet;
use casa_ms::schema::SubtableId;
use casa_tables::{ColumnSchema, Table, TableError, TableOptions, TableSchema};
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

/// Heap a simulated table holds for each row while it is built in memory
/// and saved: the row's record (twelve named cells, four of them arrays)
/// and its share of the columns the save forms. Measured at about 2,050
/// bytes (the #891 review's 36,000-row probe, and the table law in
/// `casa-simulation`'s `session_memory/jones.rs`); a quarter more is kept
/// for the allocator.
pub const SIMULATED_TABLE_ROW_BYTES: u64 = 2_560;

/// Heap writing a simulated table of `rows` rows holds beyond its fixed
/// metadata.
#[must_use]
pub const fn simulated_table_bytes(rows: u64) -> u64 {
    rows * SIMULATED_TABLE_ROW_BYTES
}

/// Refuse `output` as a simulated table of the MeasurementSet at
/// `measurement_set` when it is that MeasurementSet, lies inside it (a
/// subtable), or contains it: replacing it would remove the data the table
/// is written from. Paths are compared after resolving symbolic links and
/// `..` in the part of each that exists, so an alias is caught as well.
///
/// # Errors
///
/// [`CalibrationTableWriteError::Prepare`] naming the conflict.
pub fn check_simulated_table_destination(
    measurement_set: &Path,
    output: &Path,
) -> Result<(), CalibrationTableWriteError> {
    let (table, ms) = (resolved(output), resolved(measurement_set));
    let conflict = if table == ms {
        "is the MeasurementSet it is written from"
    } else if table.starts_with(&ms) {
        "lies inside the MeasurementSet it is written from"
    } else if ms.starts_with(&table) {
        "contains the MeasurementSet it is written from"
    } else {
        return Ok(());
    };
    Err(CalibrationTableWriteError::Prepare {
        path: output.display().to_string(),
        reason: format!("{conflict} ({})", measurement_set.display()),
    })
}

/// `path` made absolute, with the longest part of it that exists resolved
/// (symbolic links and `..`) and the rest normalised lexically.
fn resolved(path: &Path) -> PathBuf {
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut base = PathBuf::new();
    let mut exists = true;
    for component in absolute.components() {
        match component {
            Component::CurDir => continue,
            Component::ParentDir if !exists => {
                base.pop();
                continue;
            }
            other => base.push(other),
        }
        if exists {
            match base.canonicalize() {
                Ok(canonical) => base = canonical,
                Err(_) => exists = false,
            }
        }
    }
    base
}

/// Write `solutions` of `kind` as the calibration table `output` of the
/// MeasurementSet `ms`, replacing a table already there once the new one is
/// saved. The rows are formed one at a time from `solutions`.
///
/// # Errors
///
/// `output` is, lies inside or contains the MeasurementSet
/// ([`check_simulated_table_destination`]); `output` exists and is not a
/// table; or the table cannot be written.
pub fn write_simulated_jones_table(
    ms: &MeasurementSet,
    output: &Path,
    kind: SimulatedJones,
    solutions: impl IntoIterator<Item = SimulatedSolution>,
) -> Result<(), CalibrationTableWriteError> {
    if let Some(path) = ms.path() {
        check_simulated_table_destination(path, output)?;
    }
    let measurement_set_name = ms.path().and_then(Path::file_name).map_or_else(
        || "<in-memory>".to_string(),
        |name| name.to_string_lossy().into_owned(),
    );
    let mut writer = CalibrationTableWriter::create_replacing(
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
        writer.append(row(&solution))?;
    }
    writer.copy_subtables(&[
        (SubtableId::Observation, "OBSERVATION"),
        (SubtableId::Antenna, "ANTENNA"),
        (SubtableId::Field, "FIELD"),
        (SubtableId::History, "HISTORY"),
        (SubtableId::SpectralWindow, "SPECTRAL_WINDOW"),
    ])?;
    make_windows_single_channel(&writer.output_path().join("SPECTRAL_WINDOW"))?;
    writer.save_main()
}

/// Reduce every spectral window of the SPECTRAL_WINDOW subtable at `path`
/// to one channel, as CASA's `NewCalTable::makeSpwSingleChan` does for a
/// term that does not depend on frequency: the middle channel's frequency
/// (`CHAN_FREQ[n/2]`), and CHAN_WIDTH, EFFECTIVE_BW and RESOLUTION the
/// window's TOTAL_BANDWIDTH. CASA's `applycal` refuses a table whose
/// windows keep the MeasurementSet's channels.
fn make_windows_single_channel(path: &Path) -> Result<(), CalibrationTableWriteError> {
    let failed = |source: TableError| CalibrationTableWriteError::Save {
        path: path.display().to_string(),
        source: Box::new(source),
    };
    let mut table = Table::open(TableOptions::new(path)).map_err(failed)?;
    for row in 0..table.row_count() {
        let value = |column: &str| -> Result<Value, TableError> {
            Ok(table
                .cell_accessor(row, column)?
                .value()?
                .cloned()
                .unwrap_or_else(|| Value::Scalar(ScalarValue::Int32(0))))
        };
        let frequencies = match value("CHAN_FREQ").map_err(failed)? {
            Value::Array(ArrayValue::Float64(frequencies)) => frequencies,
            _ => continue,
        };
        let channels = frequencies.len();
        if channels <= 1 {
            continue;
        }
        let bandwidth = match value("TOTAL_BANDWIDTH").map_err(failed)? {
            Value::Scalar(ScalarValue::Float64(bandwidth)) => bandwidth,
            _ => continue,
        };
        let one = |value: f64| {
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(IxDyn(&[1]), vec![value]).expect("one channel"),
            ))
        };
        let middle = frequencies
            .iter()
            .nth(channels / 2)
            .copied()
            .unwrap_or_default();
        for (column, cell) in [
            ("NUM_CHAN", Value::Scalar(ScalarValue::Int32(1))),
            ("CHAN_FREQ", one(middle)),
            ("CHAN_WIDTH", one(bandwidth)),
            ("EFFECTIVE_BW", one(bandwidth)),
            ("RESOLUTION", one(bandwidth)),
        ] {
            table
                .cell_accessor_mut(row, column)
                .and_then(|mut accessor| accessor.set(cell))
                .map_err(failed)?;
        }
    }
    table.flush().map_err(failed)
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
