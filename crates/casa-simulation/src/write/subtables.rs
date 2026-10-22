// SPDX-License-Identifier: LGPL-3.0-or-later
//! The subtable rows of a simulated MeasurementSet, as `NewMSSimulator`
//! writes them.
//!
//! Each row gives every scalar and fixed-shape column a value; a
//! variable-shape array CASA does not write (OBSERVATION's LOG and SCHEDULE,
//! SOURCE's POSITION) is left undefined. POINTING, which grows with the
//! integrations, is generated as the MeasurementSet is saved instead.

mod pointing;

use casa_ms::column_def::{ColumnDef, ColumnKind, column_schema};
use casa_ms::schema::{self, SubtableId};
use casa_ms::{MeasurementSet, SubtableColumnOverrides};
use casa_tables::ColumnSchema;
use casa_tables::table_measures::{MeasureType, TableMeasDesc};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{
    ArrayValue, Complex32, PrimitiveType, RecordField, RecordValue, ScalarValue, Value,
};
use ndarray::ArrayD;

use crate::session::Simulator;
use crate::session::error::{SimulatorError, SimulatorResult};

/// A scalar or fixed-shape cell value.
pub(crate) fn scalar_i32(value: i32) -> Value {
    Value::Scalar(ScalarValue::Int32(value))
}

pub(crate) fn scalar_f64(value: f64) -> Value {
    Value::Scalar(ScalarValue::Float64(value))
}

fn scalar_bool(value: bool) -> Value {
    Value::Scalar(ScalarValue::Bool(value))
}

fn scalar_string(value: &str) -> Value {
    Value::Scalar(ScalarValue::String(value.to_string()))
}

fn f64_array(shape: &[usize], values: Vec<f64>) -> Value {
    Value::Array(ArrayValue::Float64(
        ArrayD::from_shape_vec(shape.to_vec(), values).expect("shape matches values"),
    ))
}

fn i32_array(shape: &[usize], values: Vec<i32>) -> Value {
    Value::Array(ArrayValue::Int32(
        ArrayD::from_shape_vec(shape.to_vec(), values).expect("shape matches values"),
    ))
}

fn string_array(values: &[&str]) -> Value {
    Value::Array(ArrayValue::String(
        ArrayD::from_shape_vec(
            vec![values.len()],
            values.iter().map(|value| (*value).to_string()).collect(),
        )
        .expect("shape matches values"),
    ))
}

/// A direction cell (`[2, 1]`): one polynomial term of (longitude,
/// latitude).
pub(crate) fn direction_cell(direction_rad: [f64; 2]) -> Value {
    f64_array(&[2, 1], direction_rad.to_vec())
}

/// The default of a column CASA leaves unwritten: zero, false or empty for
/// scalars and fixed shapes; `None` (an undefined cell) for variable
/// shapes.
fn default_value(definition: &ColumnDef) -> Option<Value> {
    let zero = |data_type: PrimitiveType| match data_type {
        PrimitiveType::Bool => ScalarValue::Bool(false),
        PrimitiveType::Int32 => ScalarValue::Int32(0),
        PrimitiveType::Float32 => ScalarValue::Float32(0.0),
        PrimitiveType::String => ScalarValue::String(String::new()),
        PrimitiveType::Complex32 => ScalarValue::Complex32(Complex32::new(0.0, 0.0)),
        _ => ScalarValue::Float64(0.0),
    };
    match definition.column_kind {
        ColumnKind::Scalar => Some(Value::Scalar(zero(definition.data_type))),
        ColumnKind::FixedArray { shape } => {
            let count = shape.iter().product();
            Some(match definition.data_type {
                PrimitiveType::Int32 => i32_array(shape, vec![0; count]),
                PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape.to_vec(), vec![false; count])
                        .expect("shape matches values"),
                )),
                _ => f64_array(shape, vec![0.0; count]),
            })
        }
        ColumnKind::VariableArray { .. } => None,
    }
}

/// A row of a subtable: `values` for the columns given, and CASA's defaults
/// for `table`'s other required columns. Optional columns appear only when
/// given.
fn row(table: SubtableId, values: Vec<(&str, Value)>) -> RecordValue {
    let given = |name: &str| values.iter().find(|(given, _)| *given == name);
    let mut fields = Vec::new();
    for definition in schema::required_columns(table) {
        let value = match given(definition.name) {
            Some((_, value)) => Some(value.clone()),
            None => default_value(definition),
        };
        if let Some(value) = value {
            fields.push(RecordField::new(definition.name, value));
        }
    }
    for definition in schema::optional_columns(table) {
        if let Some((_, value)) = given(definition.name) {
            fields.push(RecordField::new(definition.name, value.clone()));
        }
    }
    RecordValue::new(fields)
}

fn add_rows(
    ms: &mut MeasurementSet,
    table: SubtableId,
    rows: Vec<RecordValue>,
) -> SimulatorResult<()> {
    let subtable = ms
        .subtable_mut(table)
        .ok_or_else(|| SimulatorError::raised(format!("{} subtable missing", table.name())))?;
    for record in rows {
        subtable
            .add_row(record)
            .map_err(|error| SimulatorError::raised(format!("{} row: {error}", table.name())))?;
    }
    Ok(())
}

/// Add the optional columns of `table` that CASA's simulator creates. As
/// casacore's `addColumnToDesc` with its default dimensionality makes them,
/// an array column has no fixed shape or dimensionality, so a cell CASA
/// does not write (SOURCE.POSITION) stays undefined. The units and measure
/// keywords come from the schema at save.
fn add_optional_columns(
    ms: &mut MeasurementSet,
    table: SubtableId,
    names: &[&str],
) -> SimulatorResult<()> {
    let subtable = ms
        .subtable_mut(table)
        .ok_or_else(|| SimulatorError::raised(format!("{} subtable missing", table.name())))?;
    for name in names {
        let definition = schema::optional_columns(table)
            .iter()
            .find(|definition| definition.name == *name)
            .ok_or_else(|| {
                SimulatorError::raised(format!("{} has no column {name}", table.name()))
            })?;
        let (column, default) = match definition.column_kind {
            ColumnKind::Scalar => (column_schema(definition), default_value(definition)),
            ColumnKind::FixedArray { .. } | ColumnKind::VariableArray { .. } => (
                ColumnSchema::array_variable(definition.name, definition.data_type, None),
                None,
            ),
        };
        subtable
            .add_column(column, default)
            .map_err(|error| SimulatorError::raised(format!("{}.{name}: {error}", table.name())))?;
    }
    Ok(())
}

/// Fill every subtable but POINTING from the session.
pub(crate) fn write_subtables(ms: &mut MeasurementSet, session: &Simulator) -> SimulatorResult<()> {
    write_antennas(ms, session)?;
    write_feeds(ms, session)?;
    write_spectral_windows(ms, session)?;
    write_fields_and_sources(ms, session)?;
    write_observations(ms, session)?;
    write_states(ms, session)
}

fn write_antennas(ms: &mut MeasurementSet, session: &Simulator) -> SimulatorResult<()> {
    let mut table = ms
        .antenna_mut()
        .map_err(|error| SimulatorError::raised(format!("ANTENNA: {error}")))?;
    for antenna in session.antennas() {
        table
            .add_antenna(
                &antenna.name,
                &antenna.station,
                "GROUND-BASED",
                &antenna.mount,
                antenna.position_itrf_m,
                [0.0; 3],
                antenna.diameter_m,
            )
            .map_err(|error| SimulatorError::raised(format!("ANTENNA row: {error}")))?;
    }
    Ok(())
}

fn write_feeds(ms: &mut MeasurementSet, session: &Simulator) -> SimulatorResult<()> {
    let identity = Value::Array(ArrayValue::Complex32(
        ArrayD::from_shape_vec(
            vec![2, 2],
            vec![
                Complex32::new(1.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(0.0, 0.0),
                Complex32::new(1.0, 0.0),
            ],
        )
        .expect("2x2"),
    ));
    let rows = session
        .feeds()
        .iter()
        .map(|feed| {
            row(
                SubtableId::Feed,
                vec![
                    ("ANTENNA_ID", scalar_i32(feed.antenna_id)),
                    ("BEAM_ID", scalar_i32(-1)),
                    ("BEAM_OFFSET", f64_array(&[2, 2], vec![0.0; 4])),
                    ("FEED_ID", scalar_i32(feed.feed_id)),
                    ("INTERVAL", scalar_f64(1e30)),
                    ("NUM_RECEPTORS", scalar_i32(2)),
                    ("POLARIZATION_TYPE", string_array(&feed.polarization)),
                    ("POL_RESPONSE", identity.clone()),
                    ("POSITION", f64_array(&[3], vec![0.0; 3])),
                    ("RECEPTOR_ANGLE", f64_array(&[2], vec![0.0; 2])),
                    ("SPECTRAL_WINDOW_ID", scalar_i32(-1)),
                    ("TIME", scalar_f64(0.0)),
                ],
            )
        })
        .collect();
    add_rows(ms, SubtableId::Feed, rows)
}

fn write_spectral_windows(ms: &mut MeasurementSet, session: &Simulator) -> SimulatorResult<()> {
    let mut spws = Vec::new();
    let mut polarizations = Vec::new();
    let mut descriptions = Vec::new();
    for (index, window) in session.spectral_windows().iter().enumerate() {
        let channels = window.channel_count;
        let widths = vec![window.channel_width_hz; channels];
        spws.push(row(
            SubtableId::SpectralWindow,
            vec![
                (
                    "CHAN_FREQ",
                    f64_array(&[channels], window.channel_frequencies_hz()),
                ),
                ("CHAN_WIDTH", f64_array(&[channels], widths.clone())),
                ("EFFECTIVE_BW", f64_array(&[channels], widths.clone())),
                ("FLAG_ROW", scalar_bool(false)),
                ("FREQ_GROUP", scalar_i32(0)),
                ("FREQ_GROUP_NAME", scalar_string("Group 1")),
                ("IF_CONV_CHAIN", scalar_i32(0)),
                ("MEAS_FREQ_REF", scalar_i32(window.frame.casacore_code())),
                ("NAME", scalar_string(&window.name)),
                ("NET_SIDEBAND", scalar_i32(1)),
                ("NUM_CHAN", scalar_i32(channels as i32)),
                ("REF_FREQUENCY", scalar_f64(window.start_frequency_hz)),
                ("RESOLUTION", f64_array(&[channels], widths)),
                (
                    "TOTAL_BANDWIDTH",
                    scalar_f64(channels as f64 * window.channel_width_hz),
                ),
            ],
        ));
        let correlations = window.correlations.len();
        let products = window.correlation_products();
        let mut product_cells = vec![0; 2 * correlations];
        for (correlation, pair) in products.iter().enumerate() {
            product_cells[correlation] = pair[0];
            product_cells[correlations + correlation] = pair[1];
        }
        polarizations.push(row(
            SubtableId::Polarization,
            vec![
                ("CORR_PRODUCT", i32_array(&[2, correlations], product_cells)),
                (
                    "CORR_TYPE",
                    i32_array(&[correlations], window.correlations.clone()),
                ),
                ("FLAG_ROW", scalar_bool(false)),
                ("NUM_CORR", scalar_i32(correlations as i32)),
            ],
        ));
        descriptions.push(row(
            SubtableId::DataDescription,
            vec![
                ("FLAG_ROW", scalar_bool(false)),
                ("POLARIZATION_ID", scalar_i32(index as i32)),
                ("SPECTRAL_WINDOW_ID", scalar_i32(index as i32)),
            ],
        ));
    }
    add_rows(ms, SubtableId::SpectralWindow, spws)?;
    add_rows(ms, SubtableId::Polarization, polarizations)?;
    add_rows(ms, SubtableId::DataDescription, descriptions)?;
    // Each window's frequencies are in its MEAS_FREQ_REF frame. casacore's
    // table of codes ends with Undefined (64).
    let (mut types, mut codes): (Vec<String>, Vec<i32>) = FrequencyRef::ALL
        .iter()
        .map(|frame| (frame.as_str().to_string(), frame.casacore_code()))
        .unzip();
    types.push("Undefined".to_string());
    codes.push(64);
    let table = ms
        .subtable_mut(SubtableId::SpectralWindow)
        .ok_or_else(|| SimulatorError::raised("SPECTRAL_WINDOW subtable missing"))?;
    for column in ["CHAN_FREQ", "REF_FREQUENCY"] {
        TableMeasDesc::new_variable_int(
            column,
            MeasureType::Frequency,
            "MEAS_FREQ_REF",
            types.clone(),
            codes.clone(),
        )
        .and_then(|description| description.write(table))
        .map_err(|error| SimulatorError::raised(format!("SPECTRAL_WINDOW.{column}: {error}")))?;
    }
    Ok(())
}

/// The units and measure references of the SOURCE columns CASA's
/// simulator adds, as it writes them.
fn write_source_measure_keywords(ms: &mut MeasurementSet) -> SimulatorResult<()> {
    let table = ms
        .subtable_mut(SubtableId::Source)
        .ok_or_else(|| SimulatorError::raised("SOURCE subtable missing"))?;
    for (column, measure, refer) in [
        ("POSITION", MeasureType::Position, "ITRF"),
        ("REST_FREQUENCY", MeasureType::Frequency, "LSRK"),
        ("SYSVEL", MeasureType::RadialVelocity, "LSRK"),
    ] {
        TableMeasDesc::new_fixed(column, measure, refer)
            .write(table)
            .map_err(|error| SimulatorError::raised(format!("SOURCE.{column}: {error}")))?;
    }
    Ok(())
}

fn write_fields_and_sources(ms: &mut MeasurementSet, session: &Simulator) -> SimulatorResult<()> {
    add_optional_columns(
        ms,
        SubtableId::Source,
        &[
            "POSITION",
            "PULSAR_ID",
            "REST_FREQUENCY",
            "SYSVEL",
            "TRANSITION",
        ],
    )?;
    write_source_measure_keywords(ms)?;
    let mut fields = Vec::new();
    let mut sources = Vec::new();
    for (index, field) in session.fields().iter().enumerate() {
        let direction = direction_cell(field.direction_rad);
        fields.push(row(
            SubtableId::Field,
            vec![
                ("CODE", scalar_string(&field.code)),
                ("DELAY_DIR", direction.clone()),
                ("FLAG_ROW", scalar_bool(false)),
                ("NAME", scalar_string(&field.name)),
                ("NUM_POLY", scalar_i32(0)),
                ("PHASE_DIR", direction.clone()),
                ("REFERENCE_DIR", direction),
                ("SOURCE_ID", scalar_i32(index as i32)),
                ("TIME", scalar_f64(0.0)),
            ],
        ));
        // With a spectral window defined by then, the source has one line,
        // at the first window's reference frequency.
        let (line_count, line_window) = match field.rest_frequency_hz {
            Some(_) => (1, 0),
            None => (0, -1),
        };
        let mut source = vec![
            ("CALIBRATION_GROUP", scalar_i32(0)),
            ("CODE", scalar_string(&field.code)),
            ("DIRECTION", f64_array(&[2], field.direction_rad.to_vec())),
            ("INTERVAL", scalar_f64(1e30)),
            ("NAME", scalar_string(&field.name)),
            ("NUM_LINES", scalar_i32(line_count)),
            ("PROPER_MOTION", f64_array(&[2], vec![0.0; 2])),
            ("PULSAR_ID", scalar_i32(0)),
            ("SOURCE_ID", scalar_i32(index as i32)),
            ("SPECTRAL_WINDOW_ID", scalar_i32(line_window)),
            ("TIME", scalar_f64(field.source_time_s)),
        ];
        if let Some(rest_frequency_hz) = field.rest_frequency_hz {
            source.extend([
                ("REST_FREQUENCY", f64_array(&[1], vec![rest_frequency_hz])),
                ("TRANSITION", string_array(&["X"])),
                ("SYSVEL", f64_array(&[1], vec![0.0])),
            ]);
        }
        sources.push(row(SubtableId::Source, source));
    }
    add_rows(ms, SubtableId::Field, fields)?;
    add_rows(ms, SubtableId::Source, sources)
}

fn write_observations(ms: &mut MeasurementSet, session: &Simulator) -> SimulatorResult<()> {
    let rows = session
        .observations()
        .iter()
        .map(|observation| {
            row(
                SubtableId::Observation,
                vec![
                    ("FLAG_ROW", scalar_bool(false)),
                    ("OBSERVER", scalar_string(&observation.observer)),
                    ("PROJECT", scalar_string(&observation.project)),
                    ("RELEASE_DATE", scalar_f64(0.0)),
                    ("SCHEDULE_TYPE", scalar_string("")),
                    ("TELESCOPE_NAME", scalar_string(&observation.telescope)),
                    (
                        "TIME_RANGE",
                        f64_array(&[2], observation.time_range_s.to_vec()),
                    ),
                ],
            )
        })
        .collect();
    add_rows(ms, SubtableId::Observation, rows)
}

fn write_states(ms: &mut MeasurementSet, session: &Simulator) -> SimulatorResult<()> {
    let rows = session
        .states()
        .iter()
        .map(|state| {
            row(
                SubtableId::State,
                vec![
                    ("CAL", scalar_f64(state.cal)),
                    ("FLAG_ROW", scalar_bool(false)),
                    ("LOAD", scalar_f64(state.load)),
                    ("OBS_MODE", scalar_string(&state.obs_mode)),
                    ("REF", scalar_bool(state.reference)),
                    ("SIG", scalar_bool(state.sig)),
                    ("SUB_SCAN", scalar_i32(state.sub_scan)),
                ],
            )
        })
        .collect();
    add_rows(ms, SubtableId::State, rows)
}

/// The subtables the MeasurementSet's save writes from generated columns:
/// POINTING, one row per antenna per integration, which is not held as rows
/// (see [`pointing`]).
pub(crate) fn subtable_overrides(session: &Simulator) -> SimulatorResult<SubtableColumnOverrides> {
    let scans = session
        .scans()
        .iter()
        .map(pointing::PointingScan::of)
        .collect::<Vec<_>>();
    let pointing = pointing::pointing_overrides(&scans)
        .map_err(|error| SimulatorError::raised(format!("POINTING columns: {error}")))?;
    let mut overrides = SubtableColumnOverrides::new();
    overrides.insert(SubtableId::Pointing, pointing);
    Ok(overrides)
}

/// Heap the [`subtable_overrides`] of `session` hold while the save runs:
/// POINTING's, which grows with the scans.
pub(crate) fn generated_bytes(session: &Simulator) -> u64 {
    pointing::overrides_bytes(session.scans())
}
