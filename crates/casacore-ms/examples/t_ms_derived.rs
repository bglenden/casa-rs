// SPDX-License-Identifier: LGPL-3.0-or-later
//! Derived quantities demo.
//!
//! Creates an MS with VLA-like antenna positions and a field at known RA/Dec,
//! then computes and prints: hour angle, parallactic angle, azimuth/elevation,
//! and LAST for several timestamps. Shows per-row derived column access and
//! compares antenna 1 vs antenna 2 values.
//!
//! Cf. C++ `tMSDerivedValues`.

use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::derived::columns::DerivedColumns;
use casacore_ms::derived::engine::MsCalEngine;
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::{self, SubtableId};
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use num_complex::Complex32;

/// VLA approximate ITRF position (meters).
const VLA_X: f64 = -1601185.4;
const VLA_Y: f64 = -5041977.5;
const VLA_Z: f64 = 3554875.9;

fn main() {
    println!("=== t_ms_derived: Derived quantities demo ===\n");

    let mut ms = MeasurementSet::create_memory(MeasurementSetBuilder::new()).expect("create MS");

    // ---- Add 2 antennas (VLA-like, ~100m apart) ----
    {
        let mut ant = ms.antenna_mut().expect("antenna");
        ant.add_antenna(
            "VLA01",
            "N01",
            "GROUND-BASED",
            "ALT-AZ",
            [VLA_X, VLA_Y, VLA_Z],
            [0.0; 3],
            25.0,
        )
        .expect("add antenna");
        ant.add_antenna(
            "VLA02",
            "E01",
            "GROUND-BASED",
            "ALT-AZ",
            [VLA_X + 100.0, VLA_Y, VLA_Z],
            [0.0; 3],
            25.0,
        )
        .expect("add antenna");
    }
    println!("Antennas: VLA01, VLA02 (~100m baseline)");

    // ---- Add a field at RA=12h, Dec=+30deg ----
    let ra = std::f64::consts::PI; // 12h = pi radians
    let dec = 30.0_f64.to_radians();
    {
        let field_table = ms.subtable_mut(SubtableId::Field).expect("field subtable");
        let dir = ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![ra, dec]).unwrap());
        let row = make_subtable_row(
            schema::field::REQUIRED_COLUMNS,
            &[
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("DemoField".to_string())),
                ),
                ("CODE", Value::Scalar(ScalarValue::String("T".to_string()))),
                ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                ("DELAY_DIR", Value::Array(dir.clone())),
                ("PHASE_DIR", Value::Array(dir.clone())),
                ("REFERENCE_DIR", Value::Array(dir)),
                ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                ("TIME", Value::Scalar(ScalarValue::Float64(0.0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        field_table.add_row(row).expect("add field");
    }
    println!(
        "Field: DemoField at RA={:.1}h Dec={:+.1}deg\n",
        ra * 12.0 / std::f64::consts::PI,
        dec.to_degrees()
    );

    // ---- Add main-table rows at different times ----
    let base_mjd = 59000.0; // MJD 59000
    let hours: Vec<f64> = vec![0.0, 4.0, 8.0, 12.0, 16.0, 20.0];

    for (i, &hour) in hours.iter().enumerate() {
        let time_mjd_sec = (base_mjd + hour / 24.0) * 86400.0;
        add_main_row(
            &mut ms,
            &[
                ("TIME", Value::Scalar(ScalarValue::Float64(time_mjd_sec))),
                ("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("ANTENNA1", Value::Scalar(ScalarValue::Int32(0))),
                ("ANTENNA2", Value::Scalar(ScalarValue::Int32(1))),
                (
                    "SCAN_NUMBER",
                    Value::Scalar(ScalarValue::Int32(i as i32 + 1)),
                ),
            ],
        );
    }
    println!("Added {} rows at UT hours: {:?}\n", hours.len(), hours);

    // ---- Create engine and derived columns ----
    let engine = MsCalEngine::new(&ms).expect("create engine");
    let derived = DerivedColumns::new(ms.main_table(), engine);

    println!("Observatory position: antenna 0 (VLA)");
    println!(
        "Engine: {} antennas, {} fields\n",
        derived.engine().num_antennas(),
        derived.engine().num_fields()
    );

    // ---- Print derived quantities per row ----
    println!(
        "{:<6} {:<8} {:>10} {:>10} {:>10} {:>10} {:>10}",
        "Row", "UT(h)", "HA(h)", "PA(deg)", "Az(deg)", "El(deg)", "LAST(h)"
    );
    println!("{}", "-".repeat(70));

    for (row, &ut_hour) in hours.iter().enumerate().take(ms.row_count()) {
        let ha_rad = derived.ha1(row).expect("HA");
        let ha_hours = ha_rad * 12.0 / std::f64::consts::PI;

        let pa_rad = derived.pa1(row).expect("PA");
        let pa_deg = pa_rad.to_degrees();

        let (az_rad, el_rad) = derived.azel1(row).expect("AzEl");
        let az_deg = az_rad.to_degrees();
        let el_deg = el_rad.to_degrees();

        let last_rad = derived.last1(row).expect("LAST");
        let last_hours = last_rad * 12.0 / std::f64::consts::PI;

        println!(
            "{row:<6} {ut_hour:<8.1} {ha_hours:>10.4} {pa_deg:>10.4} {az_deg:>10.4} {el_deg:>10.4} {last_hours:>10.4}"
        );
    }

    // ---- Compare antenna 1 vs antenna 2 ----
    println!("\n--- Antenna comparison (row 2, UT={:.1}h) ---", hours[2]);
    let row = 2;

    let ha1 = derived.ha1(row).expect("HA1");
    let ha2 = derived.ha2(row).expect("HA2");
    println!(
        "  HA:  ant1={:+.6}h  ant2={:+.6}h  diff={:.9}h",
        ha1 * 12.0 / std::f64::consts::PI,
        ha2 * 12.0 / std::f64::consts::PI,
        (ha1 - ha2) * 12.0 / std::f64::consts::PI
    );

    let pa1 = derived.pa1(row).expect("PA1");
    let pa2 = derived.pa2(row).expect("PA2");
    println!(
        "  PA:  ant1={:+.6}deg  ant2={:+.6}deg  diff={:.9}deg",
        pa1.to_degrees(),
        pa2.to_degrees(),
        (pa1 - pa2).to_degrees()
    );

    let (az1, el1) = derived.azel1(row).expect("AzEl1");
    let (az2, el2) = derived.azel2(row).expect("AzEl2");
    println!(
        "  Az:  ant1={:.6}deg  ant2={:.6}deg  diff={:.9}deg",
        az1.to_degrees(),
        az2.to_degrees(),
        (az1 - az2).to_degrees()
    );
    println!(
        "  El:  ant1={:.6}deg  ant2={:.6}deg  diff={:.9}deg",
        el1.to_degrees(),
        el2.to_degrees(),
        (el1 - el2).to_degrees()
    );

    let last1 = derived.last1(row).expect("LAST1");
    let last2 = derived.last2(row).expect("LAST2");
    println!(
        "  LAST: ant1={:.6}h  ant2={:.6}h  diff={:.9}h",
        last1 * 12.0 / std::f64::consts::PI,
        last2 * 12.0 / std::f64::consts::PI,
        (last1 - last2) * 12.0 / std::f64::consts::PI
    );

    // ---- UVW ----
    println!("\n--- J2000 UVW for all rows ---");
    println!("{:<6} {:>12} {:>12} {:>12}", "Row", "U(m)", "V(m)", "W(m)");
    println!("{}", "-".repeat(48));
    for row in 0..ms.row_count() {
        let uvw = derived.uvw_j2000(row).expect("UVW");
        println!(
            "{row:<6} {:>12.4} {:>12.4} {:>12.4}",
            uvw[0], uvw[1], uvw[2]
        );
    }

    // ---- HADEC ----
    println!("\n--- HADEC for row 0 ---");
    let (ha, dec_out) = derived.hadec1(0).expect("HADEC");
    println!(
        "  HA={:.6}h  Dec={:+.6}deg (input Dec={:+.1}deg)",
        ha * 12.0 / std::f64::consts::PI,
        dec_out.to_degrees(),
        dec.to_degrees()
    );

    println!("\n=== t_ms_derived complete ===");
}

fn make_subtable_row(defs: &[ColumnDef], overrides: &[(&str, Value)]) -> RecordValue {
    let fields: Vec<RecordField> = defs
        .iter()
        .map(|c| {
            if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == c.name) {
                RecordField::new(c.name, v.clone())
            } else {
                RecordField::new(c.name, default_value_for_def(c))
            }
        })
        .collect();
    RecordValue::new(fields)
}

fn add_main_row(ms: &mut MeasurementSet, overrides: &[(&str, Value)]) {
    let schema = ms.main_table().schema().unwrap().clone();
    let all_cols: Vec<&ColumnDef> = schema::main_table::REQUIRED_COLUMNS
        .iter()
        .chain(schema::main_table::OPTIONAL_COLUMNS.iter())
        .collect();

    let fields: Vec<RecordField> = schema
        .columns()
        .iter()
        .map(|col| {
            if let Some((_, v)) = overrides.iter().find(|(n, _)| *n == col.name()) {
                return RecordField::new(col.name(), v.clone());
            }
            if let Some(c) = all_cols.iter().find(|c| c.name == col.name()) {
                RecordField::new(col.name(), default_value_for_def(c))
            } else {
                RecordField::new(col.name(), Value::Scalar(ScalarValue::Int32(0)))
            }
        })
        .collect();
    ms.main_table_mut()
        .add_row(RecordValue::new(fields))
        .unwrap();
}

fn default_value_for_def(c: &ColumnDef) -> Value {
    match c.column_kind {
        ColumnKind::Scalar => match c.data_type {
            casacore_types::PrimitiveType::Int32 => Value::Scalar(ScalarValue::Int32(0)),
            casacore_types::PrimitiveType::Float64 => Value::Scalar(ScalarValue::Float64(0.0)),
            casacore_types::PrimitiveType::Bool => Value::Scalar(ScalarValue::Bool(false)),
            casacore_types::PrimitiveType::String => {
                Value::Scalar(ScalarValue::String(String::new()))
            }
            _ => Value::Scalar(ScalarValue::Float64(0.0)),
        },
        ColumnKind::FixedArray { shape } => {
            let total: usize = shape.iter().product();
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(shape.to_vec(), vec![0.0; total]).unwrap(),
            ))
        }
        ColumnKind::VariableArray { ndim } => {
            let shape: Vec<usize> = vec![1; ndim];
            let total: usize = shape.iter().product();
            match c.data_type {
                casacore_types::PrimitiveType::Bool => Value::Array(ArrayValue::Bool(
                    ArrayD::from_shape_vec(shape, vec![false; total]).unwrap(),
                )),
                casacore_types::PrimitiveType::Float32 => Value::Array(ArrayValue::Float32(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
                casacore_types::PrimitiveType::Complex32 => Value::Array(ArrayValue::Complex32(
                    ArrayD::from_shape_vec(shape, vec![Complex32::new(0.0, 0.0); total]).unwrap(),
                )),
                casacore_types::PrimitiveType::String => Value::Array(ArrayValue::String(
                    ArrayD::from_shape_vec(shape, vec![String::new(); total]).unwrap(),
                )),
                _ => Value::Array(ArrayValue::Float64(
                    ArrayD::from_shape_vec(shape, vec![0.0; total]).unwrap(),
                )),
            }
        }
    }
}
