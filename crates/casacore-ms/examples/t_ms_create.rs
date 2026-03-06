// SPDX-License-Identifier: LGPL-3.0-or-later
//! MS creation and subtable population demo.
//!
//! Creates a MeasurementSet with 3 antennas, 2 fields, 1 spectral window,
//! 2 polarization setups, populates all required subtables, adds main table
//! rows with realistic Complex32 visibility data, saves to disk, and
//! prints a summary.
//!
//! Cf. C++ `tMeasurementSet`.

use casacore_ms::builder::MeasurementSetBuilder;
use casacore_ms::column_def::{ColumnDef, ColumnKind};
use casacore_ms::ms::MeasurementSet;
use casacore_ms::schema::{self, SubtableId};
use casacore_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use ndarray::ArrayD;
use num_complex::Complex32;

fn main() {
    let dir = tempfile::tempdir().expect("tempdir");
    let ms_path = dir.path().join("demo.ms");

    println!("=== t_ms_create: MeasurementSet creation demo ===\n");

    // ---- Create the MS with DATA column ----
    let builder = MeasurementSetBuilder::new().with_main_column("DATA");
    let mut ms = MeasurementSet::create(&ms_path, builder).expect("create MS");

    // ---- Populate ANTENNA subtable (3 antennas, VLA-like) ----
    let vla_positions: [(f64, f64, f64); 3] = [
        (-1601185.4, -5041977.5, 3554875.9),
        (-1601085.4, -5041977.5, 3554875.9),
        (-1601185.4, -5041877.5, 3554875.9),
    ];
    {
        let mut ant = ms.antenna_mut().expect("antenna subtable");
        for (i, (x, y, z)) in vla_positions.iter().enumerate() {
            ant.add_antenna(
                &format!("VLA{i:02}"),
                &format!("N{i:02}"),
                "GROUND-BASED",
                "ALT-AZ",
                [*x, *y, *z],
                [0.0; 3],
                25.0,
            )
            .expect("add antenna");
        }
    }
    println!("Added 3 antennas");

    // ---- Populate FIELD subtable (2 fields) ----
    let fields_data: [(&str, &str, f64, f64); 2] = [
        ("3C286", "C", 3.539_257_8, 0.532_880_4), // RA ~202.8 deg, Dec ~30.5 deg
        ("CygA", "T", 5.233_686_6, 0.710_940_2),  // RA ~299.9 deg, Dec ~40.7 deg
    ];
    {
        let field_table = ms.subtable_mut(SubtableId::Field).expect("field subtable");
        for (name, code, ra, dec) in &fields_data {
            let dir_array =
                ArrayValue::Float64(ArrayD::from_shape_vec(vec![2, 1], vec![*ra, *dec]).unwrap());
            let row = make_subtable_row(
                schema::field::REQUIRED_COLUMNS,
                &[
                    ("NAME", Value::Scalar(ScalarValue::String(name.to_string()))),
                    ("CODE", Value::Scalar(ScalarValue::String(code.to_string()))),
                    ("NUM_POLY", Value::Scalar(ScalarValue::Int32(0))),
                    ("DELAY_DIR", Value::Array(dir_array.clone())),
                    ("PHASE_DIR", Value::Array(dir_array.clone())),
                    ("REFERENCE_DIR", Value::Array(dir_array)),
                    ("SOURCE_ID", Value::Scalar(ScalarValue::Int32(-1))),
                    ("TIME", Value::Scalar(ScalarValue::Float64(0.0))),
                    ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ],
            );
            field_table.add_row(row).expect("add field");
        }
    }
    println!("Added 2 fields: 3C286 (calibrator), CygA (target)");

    // ---- Populate POLARIZATION subtable (2 setups) ----
    {
        let pol_table = ms
            .subtable_mut(SubtableId::Polarization)
            .expect("polarization subtable");

        // Setup 0: full Stokes (RR, RL, LR, LL)
        let corr_type_full =
            ArrayValue::Int32(ArrayD::from_shape_vec(vec![4], vec![5, 6, 7, 8]).unwrap());
        let corr_product_full = ArrayValue::Int32(
            ArrayD::from_shape_vec(vec![2, 4], vec![0, 0, 1, 1, 0, 1, 0, 1]).unwrap(),
        );
        let row0 = make_subtable_row(
            schema::polarization::REQUIRED_COLUMNS,
            &[
                ("NUM_CORR", Value::Scalar(ScalarValue::Int32(4))),
                ("CORR_TYPE", Value::Array(corr_type_full)),
                ("CORR_PRODUCT", Value::Array(corr_product_full)),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        pol_table.add_row(row0).expect("add pol setup 0");

        // Setup 1: parallel hand only (RR, LL)
        let corr_type_rr_ll =
            ArrayValue::Int32(ArrayD::from_shape_vec(vec![2], vec![5, 8]).unwrap());
        let corr_product_rr_ll =
            ArrayValue::Int32(ArrayD::from_shape_vec(vec![2, 2], vec![0, 1, 0, 1]).unwrap());
        let row1 = make_subtable_row(
            schema::polarization::REQUIRED_COLUMNS,
            &[
                ("NUM_CORR", Value::Scalar(ScalarValue::Int32(2))),
                ("CORR_TYPE", Value::Array(corr_type_rr_ll)),
                ("CORR_PRODUCT", Value::Array(corr_product_rr_ll)),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        pol_table.add_row(row1).expect("add pol setup 1");
    }
    println!("Added 2 polarization setups: RR/RL/LR/LL and RR/LL");

    // ---- Populate SPECTRAL_WINDOW subtable (1 SPW, 16 channels) ----
    let num_chan: usize = 16;
    let ref_freq: f64 = 1.4e9; // L-band
    let chan_width: f64 = 1.0e6; // 1 MHz channels
    {
        let spw_table = ms
            .subtable_mut(SubtableId::SpectralWindow)
            .expect("spw subtable");
        let chan_freqs: Vec<f64> = (0..num_chan)
            .map(|i| ref_freq + i as f64 * chan_width)
            .collect();
        let widths = vec![chan_width; num_chan];
        let f64_arr = |v: &[f64]| {
            Value::Array(ArrayValue::Float64(
                ArrayD::from_shape_vec(vec![v.len()], v.to_vec()).unwrap(),
            ))
        };
        let row = make_subtable_row(
            schema::spectral_window::REQUIRED_COLUMNS,
            &[
                (
                    "NUM_CHAN",
                    Value::Scalar(ScalarValue::Int32(num_chan as i32)),
                ),
                (
                    "NAME",
                    Value::Scalar(ScalarValue::String("L-band".to_string())),
                ),
                (
                    "REF_FREQUENCY",
                    Value::Scalar(ScalarValue::Float64(ref_freq)),
                ),
                (
                    "TOTAL_BANDWIDTH",
                    Value::Scalar(ScalarValue::Float64(num_chan as f64 * chan_width)),
                ),
                ("CHAN_FREQ", f64_arr(&chan_freqs)),
                ("CHAN_WIDTH", f64_arr(&widths)),
                ("EFFECTIVE_BW", f64_arr(&widths)),
                ("RESOLUTION", f64_arr(&widths)),
                ("MEAS_FREQ_REF", Value::Scalar(ScalarValue::Int32(5))), // TOPO
                ("NET_SIDEBAND", Value::Scalar(ScalarValue::Int32(1))),
                ("FREQ_GROUP", Value::Scalar(ScalarValue::Int32(0))),
                (
                    "FREQ_GROUP_NAME",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                ("IF_CONV_CHAIN", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        spw_table.add_row(row).expect("add spw");
    }
    println!(
        "Added 1 spectral window: L-band, {num_chan} x {:.1} MHz channels",
        chan_width / 1e6
    );

    // ---- Populate DATA_DESCRIPTION subtable ----
    {
        let dd_table = ms
            .subtable_mut(SubtableId::DataDescription)
            .expect("dd subtable");
        let row = make_subtable_row(
            schema::data_description::REQUIRED_COLUMNS,
            &[
                ("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        dd_table.add_row(row).expect("add dd");
    }
    println!("Added 1 data description (SPW 0, Pol 0)");

    // ---- Populate OBSERVATION subtable ----
    {
        let obs_table = ms
            .subtable_mut(SubtableId::Observation)
            .expect("obs subtable");
        let time_range =
            ArrayValue::Float64(ArrayD::from_shape_vec(vec![2], vec![5.0976e9, 5.0977e9]).unwrap());
        let empty_string_arr =
            ArrayValue::String(ArrayD::from_shape_vec(vec![1], vec!["".to_string()]).unwrap());
        let row = make_subtable_row(
            schema::observation::REQUIRED_COLUMNS,
            &[
                (
                    "TELESCOPE_NAME",
                    Value::Scalar(ScalarValue::String("VLA".to_string())),
                ),
                (
                    "OBSERVER",
                    Value::Scalar(ScalarValue::String("casa-rs-demo".to_string())),
                ),
                (
                    "PROJECT",
                    Value::Scalar(ScalarValue::String("DEMO001".to_string())),
                ),
                ("RELEASE_DATE", Value::Scalar(ScalarValue::Float64(0.0))),
                (
                    "SCHEDULE_TYPE",
                    Value::Scalar(ScalarValue::String(String::new())),
                ),
                ("TIME_RANGE", Value::Array(time_range)),
                ("LOG", Value::Array(empty_string_arr.clone())),
                ("SCHEDULE", Value::Array(empty_string_arr)),
                ("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
            ],
        );
        obs_table.add_row(row).expect("add obs");
    }
    println!("Added 1 observation row (VLA)");

    // ---- Add main-table rows with visibility data ----
    let num_corr: usize = 4;
    let time_mjd_sec = 59000.5 * 86400.0; // MJD 59000.5 = noon UT
    let integration = 10.0; // 10 second integrations
    let baselines: Vec<(i32, i32)> = vec![(0, 1), (0, 2), (1, 2)];
    let num_times = 3;

    for t_idx in 0..num_times {
        let time = time_mjd_sec + t_idx as f64 * integration;
        for &(ant1, ant2) in &baselines {
            // Synthesize visibility data: amplitude decreases with baseline, phase rotates
            let vis: Vec<Complex32> = (0..num_corr * num_chan)
                .map(|i| {
                    let amp = 1.0 / (1.0 + (ant2 - ant1) as f32);
                    let phase = (i as f32 * 0.1) + (t_idx as f32 * 0.3);
                    Complex32::new(amp * phase.cos(), amp * phase.sin())
                })
                .collect();
            let data = ArrayValue::Complex32(
                ArrayD::from_shape_vec(vec![num_corr, num_chan], vis).unwrap(),
            );

            add_main_row(
                &mut ms,
                &[
                    ("ANTENNA1", Value::Scalar(ScalarValue::Int32(ant1))),
                    ("ANTENNA2", Value::Scalar(ScalarValue::Int32(ant2))),
                    ("FIELD_ID", Value::Scalar(ScalarValue::Int32(0))),
                    ("DATA_DESC_ID", Value::Scalar(ScalarValue::Int32(0))),
                    ("SCAN_NUMBER", Value::Scalar(ScalarValue::Int32(1))),
                    ("TIME", Value::Scalar(ScalarValue::Float64(time))),
                    ("TIME_CENTROID", Value::Scalar(ScalarValue::Float64(time))),
                    ("EXPOSURE", Value::Scalar(ScalarValue::Float64(integration))),
                    ("INTERVAL", Value::Scalar(ScalarValue::Float64(integration))),
                    ("DATA", Value::Array(data)),
                ],
            );
        }
    }
    println!(
        "Added {} main-table rows ({} baselines x {} times)",
        baselines.len() * num_times,
        baselines.len(),
        num_times
    );

    // ---- Save ----
    ms.save().expect("save MS");
    println!("\nSaved to: {}\n", ms_path.display());

    // ---- Validate ----
    let issues = ms.validate().expect("validate");
    if issues.is_empty() {
        println!("Validation: PASS (no issues)");
    } else {
        println!("Validation issues:");
        for issue in &issues {
            println!("  {issue:?}");
        }
    }

    // ---- Print summary ----
    println!("\n--- Summary ---");
    println!("MS version: {:?}", ms.ms_version());
    println!("Main table rows: {}", ms.row_count());

    let ant = ms.antenna().expect("antenna");
    println!("Antennas ({}):", ant.row_count());
    for i in 0..ant.row_count() {
        println!(
            "  {}: {} station={} diam={:.1}m",
            i,
            ant.name(i).unwrap(),
            ant.station(i).unwrap(),
            ant.dish_diameter(i).unwrap()
        );
    }

    let field = ms.field().expect("field");
    println!("Fields ({}):", field.row_count());
    for i in 0..field.row_count() {
        println!(
            "  {}: {} code={}",
            i,
            field.name(i).unwrap(),
            field.code(i).unwrap()
        );
    }

    let spw = ms.spectral_window().expect("spw");
    println!("Spectral windows ({}):", spw.row_count());
    for i in 0..spw.row_count() {
        println!(
            "  {}: {} nchan={} ref_freq={:.3} MHz bw={:.3} MHz",
            i,
            spw.name(i).unwrap(),
            spw.num_chan(i).unwrap(),
            spw.ref_frequency(i).unwrap() / 1e6,
            spw.total_bandwidth(i).unwrap() / 1e6,
        );
    }

    let pol = ms.polarization().expect("pol");
    println!("Polarization setups ({}):", pol.row_count());
    for i in 0..pol.row_count() {
        let types = pol.corr_type(i).unwrap();
        let names: Vec<&str> = types.iter().map(|&c| stokes_name(c)).collect();
        println!(
            "  {}: ncorr={} types=[{}]",
            i,
            pol.num_corr(i).unwrap(),
            names.join(", ")
        );
    }

    // Verify DATA column
    let data_col = ms.data_column("DATA").expect("DATA column");
    let shape = data_col.shape(0).expect("shape");
    println!(
        "\nDATA column shape (row 0): {:?} = [num_corr, num_chan]",
        shape
    );

    println!("\n=== t_ms_create complete ===");
}

/// Build a RecordValue for a subtable row, using column defs for defaults.
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

/// Add a main-table row with overrides.
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

fn stokes_name(code: i32) -> &'static str {
    match code {
        1 => "I",
        2 => "Q",
        3 => "U",
        4 => "V",
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
