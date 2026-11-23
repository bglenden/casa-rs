// SPDX-License-Identifier: LGPL-3.0-or-later
//! Persist CASA's legacy GSPLINE coefficient, knot, and reference arrays.

use std::collections::{BTreeMap, BTreeSet};

use casa_ms::MeasurementSet;
use casa_tables::{ColumnSchema, TableSchema};
use casa_types::{ArrayValue, PrimitiveType, RecordField, RecordValue, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use num_complex::Complex32;

use super::SplineCoefficients;
use crate::bandpass::bpoly::{
    legacy_polynomial_columns, write_legacy_cal_desc, write_legacy_history,
};
use crate::output::CalibrationOutput;
use crate::writer::{
    CalibrationTableDescriptor, CalibrationTableWriter, set_fixed_unit_keyword,
    set_measinfo_keyword,
};
use crate::{CalibrationOutputMode, GainSolveError, GainSolveRequest};

pub(super) fn write(
    ms: &MeasurementSet,
    request: &GainSolveRequest,
    coefficients: &SplineCoefficients,
    (mode, refresh_existing): (CalibrationOutputMode, bool),
    fields: &BTreeSet<i32>,
    (reference, antennas, frequency): (i32, &BTreeSet<usize>, f64),
    poly_mode: &str,
) -> Result<usize, GainSolveError> {
    let mut columns = legacy_polynomial_columns(true);
    columns.retain(|column| column.name() != "SIDEBAND_REF");
    columns.extend(
        ["N_KNOTS_AMP", "N_KNOTS_PHASE"]
            .map(|name| ColumnSchema::scalar(name, PrimitiveType::Int32)),
    );
    columns.extend(
        ["SPLINE_KNOTS_AMP", "SPLINE_KNOTS_PHASE"]
            .map(|name| ColumnSchema::array_variable(name, PrimitiveType::Float64, Some(4))),
    );
    let ms_name = ms
        .path()
        .and_then(|path| path.file_name())
        .and_then(|name| name.to_str())
        .unwrap_or_default();
    let output = CalibrationOutput::prepare(&request.output_table, mode)?;
    let table_mode = if refresh_existing && mode == CalibrationOutputMode::Append {
        if output.destination.exists() {
            let existing = crate::output_merge::open(&output.destination)?;
            if existing.info().sub_type != "GSPLINE" {
                return Err(GainSolveError::Baseline(
                    "append destination is not a GSPLINE table".into(),
                ));
            }
        }
        // Generic GSPLINE refreshes existing antenna/field entries.
        CalibrationOutputMode::Replace
    } else {
        mode
    };
    let mut writer = CalibrationTableWriter::staged(
        ms,
        CalibrationTableDescriptor {
            output: &request.output_table,
            schema: TableSchema::new(columns).expect("GSPLINE schema"),
            subtype: "GSPLINE",
            parameter_type: None,
            measurement_set_name: ms_name.to_owned(),
            include_polarization_basis: false,
            time_extra_precision_column: Some("TIME_EXTRA_PREC"),
        },
        output,
        table_mode,
    );
    writer.preserve_row_order();
    let table = writer.table_mut();
    *table.keywords_mut() = RecordValue::new(vec![
        RecordField::new("CAL_DESC", Value::table_ref("././CAL_DESC")),
        RecordField::new("CAL_HISTORY", Value::table_ref("././CAL_HISTORY")),
    ]);
    set_fixed_unit_keyword(table, "REF_FREQUENCY", &["Hz"]);
    set_measinfo_keyword(table, "REF_FREQUENCY", "frequency", Some("LSRK"));
    set_fixed_unit_keyword(table, "REF_DIRECTION", &["rad", "rad"]);
    set_measinfo_keyword(table, "REF_DIRECTION", "direction", Some("J2000"));
    let row = SplineRow {
        coefficients,
        time: coefficients.knots.iter().sum::<f64>() / coefficients.knots.len() as f64,
        reference,
        frequency,
        mode: poly_mode,
        write_amplitude: poly_mode.contains("AMP") || poly_mode.contains("A&P"),
        write_phase: poly_mode.contains("PHAS") || poly_mode.contains("A&P"),
    };
    for &field in fields {
        for &antenna in antennas {
            writer.append(row.values(field, antenna))?;
        }
    }
    let description = writer.output_path().join("CAL_DESC");
    write_legacy_cal_desc(
        description.clone(),
        &BTreeMap::from([(-1, 0)]),
        |_| 0,
        true,
        ms_name,
    )
    .map_err(|source| GainSolveError::SaveCalibrationTable {
        path: description.display().to_string(),
        source: Box::new(source),
    })?;
    let history = writer.output_path().join("CAL_HISTORY");
    write_legacy_history(&history).map_err(|source| GainSolveError::SaveCalibrationTable {
        path: history.display().to_string(),
        source: Box::new(source),
    })?;
    writer.save_main()?;
    Ok(fields.len() * antennas.len())
}

struct SplineRow<'a> {
    coefficients: &'a SplineCoefficients,
    time: f64,
    reference: i32,
    frequency: f64,
    mode: &'a str,
    write_amplitude: bool,
    write_phase: bool,
}

impl SplineRow<'_> {
    fn values(&self, field: i32, antenna: usize) -> RecordValue {
        let scalar = |name, value| RecordField::new(name, Value::Scalar(value));
        let int = |name, value| scalar(name, ScalarValue::Int32(value));
        let number = |name, value| scalar(name, ScalarValue::Float64(value));
        let text = |name, value: &str| scalar(name, ScalarValue::String(value.to_owned()));
        let mut fields = vec![
            number("TIME", self.time),
            number("TIME_EXTRA_PREC", 0.),
            number("INTERVAL", 0.),
            int("ANTENNA1", antenna as i32),
            int("FIELD_ID", field),
            int("CAL_DESC_ID", 0),
            text("POLY_TYPE", "SPLINE"),
            text("POLY_MODE", self.mode),
            text("PHASE_UNITS", if self.write_phase { "RADIANS" } else { "" }),
            scalar(
                "SCALE_FACTOR",
                ScalarValue::Complex32(Complex32::new(1., 0.)),
            ),
            scalar("TOTAL_SOLUTION_OK", ScalarValue::Bool(false)),
            scalar("TOTAL_FIT", ScalarValue::Float32(0.)),
            scalar("TOTAL_FIT_WEIGHT", ScalarValue::Float32(0.)),
            int("MEAS_FREQ_REF", 1),
            int("MEAS_DIR_REF", 0),
        ];
        for name in [
            "FEED1",
            "ARRAY_ID",
            "OBSERVATION_ID",
            "SCAN_NUMBER",
            "PROCESSOR_ID",
            "STATE_ID",
            "PHASE_ID",
            "PULSAR_BIN",
            "PULSAR_GATE_ID",
            "FREQ_GROUP",
            "CALIBRATION_GROUP",
            "CAL_HISTORY_ID",
        ] {
            fields.push(int(name, -1));
        }
        for name in [
            "FREQ_GROUP_NAME",
            "FIELD_NAME",
            "FIELD_CODE",
            "SOURCE_NAME",
            "SOURCE_CODE",
        ] {
            fields.push(text(name, ""));
        }
        for (name, value) in [
            ("REF_ANT", if self.write_phase { self.reference } else { 0 }),
            ("REF_FEED", 0),
            ("REF_RECEPTOR", 0),
        ] {
            fields.push(RecordField::new(
                name,
                Value::Array(ArrayValue::Int32(array(&[1, 1, 1], vec![value]))),
            ));
        }
        fields.extend([
            f64_field(
                "REF_FREQUENCY",
                &[1, 1, 1],
                vec![if self.write_phase { self.frequency } else { 0. }],
            ),
            f64_field(
                "REF_DIRECTION",
                &[2, 1, 1, 1],
                vec![0., std::f64::consts::FRAC_PI_2],
            ),
            RecordField::new(
                "GAIN",
                Value::Array(ArrayValue::Complex32(array(
                    &[1, 1, 1, 1],
                    vec![Complex32::default()],
                ))),
            ),
        ]);
        for (name, value) in [("FLAG", true), ("SOLUTION_OK", false)] {
            fields.push(RecordField::new(
                name,
                Value::Array(ArrayValue::Bool(array(&[1, 1, 1, 1], vec![value]))),
            ));
        }
        for name in ["FIT", "FIT_WEIGHT", "SNR"] {
            fields.push(RecordField::new(
                name,
                Value::Array(ArrayValue::Float32(array(&[1, 1, 1, 1], vec![0.]))),
            ));
        }
        for (suffix, part) in [
            (
                "AMP",
                self.write_amplitude
                    .then_some(self.coefficients.log_amplitude.as_ref())
                    .flatten(),
            ),
            (
                "PHASE",
                self.write_phase
                    .then_some(self.coefficients.phase.as_ref())
                    .flatten(),
            ),
        ] {
            let count = part.map_or(0, |_| self.coefficients.knots.len());
            let knots = if count == 0 {
                vec![0.]
            } else {
                self.coefficients.knots.clone()
            };
            let values = part.map_or_else(
                || vec![0.],
                |hands| {
                    hands
                        .iter()
                        .flat_map(|hand| hand[antenna].iter().copied().chain([0.; 4]))
                        .collect()
                },
            );
            fields.extend([
                RecordField::new(
                    format!("N_KNOTS_{suffix}"),
                    Value::Scalar(ScalarValue::Int32(count as i32)),
                ),
                RecordField::new(
                    format!("N_POLY_{suffix}"),
                    Value::Scalar(ScalarValue::Int32(count as i32)),
                ),
                f64_field(
                    &format!("SPLINE_KNOTS_{suffix}"),
                    &[1, 1, 1, knots.len()],
                    knots,
                ),
                f64_field(
                    &format!("POLY_COEFF_{suffix}"),
                    &[1, 1, 1, values.len()],
                    values,
                ),
            ]);
        }
        RecordValue::new(fields)
    }
}

fn f64_field(name: &str, shape: &[usize], values: Vec<f64>) -> RecordField {
    RecordField::new(
        name,
        Value::Array(ArrayValue::Float64(array(shape, values))),
    )
}

fn array<T>(shape: &[usize], values: Vec<T>) -> ArrayD<T> {
    ArrayD::from_shape_vec(IxDyn(shape).f(), values).expect("GSPLINE legacy array shape")
}
