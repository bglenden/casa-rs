// SPDX-License-Identifier: LGPL-3.0-or-later

//! The CASA-named calibration surfaces return what CASA 6.7.6.14's tasks
//! return (#931): each key in CASA's order, with CASA's type and NumPy
//! dtype, as `tools/casa-compat/record_casa_returns.py` recorded them in
//! `resources/casa-compat/casa-6.7.6.14/returns.json`. An array's length
//! and the antennas and windows `solvestats` keys follow the data, and the
//! fixtures here are not CASA's MeasurementSet, so arrays are compared by
//! dtype and number of axes and each run of `ant<id>` or `spw<id>` keys as
//! one. A CASA parameter casa-rs takes only at its default is refused at
//! any other value, naming #979.

mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_provider_contracts::{CasaKey, CasaValue, FloatDtype, IntDtype, TaskReply};
use casa_types::Complex32;
use serde_json::{Value, json};
use tempfile::TempDir;

/// `calibrate SURFACE --json-run -` with `values`, in `workspace`.
fn json_run(workspace: &Path, surface: &str, values: Value) -> (Output, Option<TaskReply>) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_calibrate"))
        .args([surface, "--json-run", "-"])
        .current_dir(workspace)
        .env_remove("CASA_RS_STATE_DIR")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start calibrate");
    let request = json!({"surface": surface, "values": values}).to_string();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.as_bytes())
        .unwrap();
    let output = child.wait_with_output().expect("run calibrate");
    let reply = serde_json::from_slice(&output.stdout).ok();
    (output, reply)
}

fn returned(workspace: &Path, surface: &str, values: Value) -> CasaValue {
    let (output, reply) = json_run(workspace, surface, values);
    match reply {
        Some(TaskReply::Succeeded { value, .. }) => value,
        other => panic!(
            "{surface}: {other:?}\n{}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

/// `values` on `surface` are refused naming `parameter` and #979.
fn refused_naming_979(workspace: &Path, surface: &str, values: Value, parameter: &str) {
    let (output, reply) = json_run(workspace, surface, values);
    match reply {
        Some(TaskReply::Failed {
            parameter: Some(name),
            message,
            ..
        }) => {
            assert_eq!(name, parameter, "{message}");
            assert!(message.contains("#979"), "{message}");
        }
        other => panic!(
            "{surface} {parameter}: {other:?}\n{}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

/// `value`'s structure as the CASA recording describes Python values, an
/// array by its dtype and number of axes.
fn describe(value: &CasaValue) -> Value {
    let array = |dtype: &str, shape: &[usize]| json!({"type": "ndarray", "dtype": dtype, "ndim": shape.len()});
    match value {
        CasaValue::None => json!({"type": "None"}),
        CasaValue::Bool { .. } => json!({"type": "bool"}),
        CasaValue::Int { .. } => json!({"type": "int"}),
        CasaValue::Float { .. } => json!({"type": "float"}),
        CasaValue::String { .. } => json!({"type": "str"}),
        CasaValue::List { items } => {
            json!({"type": "list", "items": items.iter().map(describe).collect::<Vec<_>>()})
        }
        CasaValue::Dict { entries } => json!({
            "type": "dict",
            "items": entries
                .iter()
                .map(|entry| match &entry.key {
                    CasaKey::String(key) => json!([key, "str", describe(&entry.value)]),
                    CasaKey::Int(key) => json!([key, "int", describe(&entry.value)]),
                })
                .collect::<Vec<_>>(),
        }),
        CasaValue::BoolArray { shape, .. } => array("bool", shape),
        CasaValue::IntArray { dtype, shape, .. } => array(
            match dtype {
                IntDtype::Int32 => "int32",
                IntDtype::Int64 => "int64",
            },
            shape,
        ),
        CasaValue::FloatArray { dtype, shape, .. } => array(
            match dtype {
                FloatDtype::Float32 => "float32",
                FloatDtype::Float64 => "float64",
            },
            shape,
        ),
        CasaValue::ComplexArray { shape, .. } => array("complex", shape),
        CasaValue::StrArray { shape, .. } => array("str", shape),
    }
}

/// A recorded CASA structure as [`describe`] gives it: NumPy's sized
/// string dtypes (`<U15`) read as `str`, an array's shape as its number of
/// axes.
fn comparable(value: &Value) -> Value {
    match value {
        Value::Object(fields) if fields.get("type") == Some(&json!("ndarray")) => {
            let dtype = fields["dtype"].as_str().unwrap();
            let dtype = if dtype.starts_with("<U") {
                "str"
            } else {
                dtype
            };
            json!({"type": "ndarray", "dtype": dtype, "ndim": fields["shape"].as_array().unwrap().len()})
        }
        Value::Object(fields) => Value::Object(
            fields
                .iter()
                .map(|(key, value)| (key.clone(), comparable(value)))
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(comparable).collect()),
        other => other.clone(),
    }
}

/// `structure` with each run of dictionary items keyed `ant<id>` or
/// `spw<id>` as one item keyed `ant#` or `spw#`, which must all have the
/// first one's structure.
fn collapse_ids(structure: &Value) -> Value {
    let indexed = |key: &str| {
        ["ant", "spw"].into_iter().find(|prefix| {
            key.strip_prefix(prefix)
                .is_some_and(|id| !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
        })
    };
    match structure {
        Value::Object(fields) if fields.get("type") == Some(&json!("dict")) => {
            let mut items = Vec::<Value>::new();
            for item in fields["items"].as_array().unwrap() {
                let key = item[0].as_str().unwrap();
                let value = collapse_ids(&item[2]);
                let Some(prefix) = indexed(key) else {
                    items.push(json!([key, item[1], value]));
                    continue;
                };
                let collapsed = format!("{prefix}#");
                match items.last() {
                    Some(last) if last[0] == json!(collapsed) => {
                        assert_eq!(last[2], value, "{key} differs from the {prefix}s before it");
                    }
                    _ => items.push(json!([collapsed, item[1], value])),
                }
            }
            json!({"type": "dict", "items": items})
        }
        other => other.clone(),
    }
}

/// CASA's recorded return for `case`, comparable with [`describe`].
fn casa(case: &str) -> Value {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../resources/casa-compat/casa-6.7.6.14/returns.json"
    );
    let document: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let recorded = &document["cases"][case]["returns"];
    assert!(!recorded.is_null(), "no recorded return for {case}");
    collapse_ids(&comparable(recorded))
}

/// `value`'s structure, comparable with [`casa`].
fn structure(value: &CasaValue) -> Value {
    collapse_ids(&describe(value))
}

/// `value`'s entry `key`.
fn entry<'a>(value: &'a CasaValue, key: &str) -> &'a CasaValue {
    let CasaValue::Dict { entries } = value else {
        panic!("not a dictionary: {value:?}");
    };
    &entries
        .iter()
        .find(|entry| entry.key == CasaKey::String(key.into()))
        .unwrap_or_else(|| panic!("no {key} in {value:?}"))
        .value
}

fn ints(value: &CasaValue) -> Vec<i64> {
    match value {
        CasaValue::IntArray { data, .. } => data.clone(),
        other => panic!("not integers: {other:?}"),
    }
}

fn strs(value: &CasaValue) -> Vec<String> {
    match value {
        CasaValue::StrArray { data, .. } => data.clone(),
        other => panic!("not strings: {other:?}"),
    }
}

#[test]
fn gaincal_returns_its_tables_and_selected_data() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let ms = ms.to_str().unwrap();
    let solve = |caltable: &str, gaintable: Value| {
        returned(
            dir.path(),
            "gaincal",
            json!({
                "vis": ms, "caltable": caltable, "refant": "0", "minblperant": 2,
                "gaintable": gaintable,
            }),
        )
    };
    let value = solve("g.gcal", json!([]));
    assert_eq!(structure(&value), casa("gaincal"));
    assert!(strs(entry(&value, "apply_tables")).is_empty());
    assert_eq!(strs(entry(&value, "solve_tables")), ["g.gcal"]);
    let selected = entry(&value, "selectvis");
    assert_eq!(ints(entry(selected, "antennas")), [0, 1, 2]);
    assert_eq!(ints(entry(selected, "field")), [0]);
    assert_eq!(ints(entry(selected, "observation")), [0]);
    assert_eq!(ints(entry(selected, "scan")), [1]);
    assert_eq!(ints(entry(selected, "spw")), [0]);

    // One interval (solint='inf'), solved for every antenna against
    // antenna 0, in window 0 only.
    let stats = entry(&value, "solvestats");
    let window = entry(stats, "spw0");
    for stage in [
        "expected",
        "data_unflagged",
        "above_minblperant",
        "above_minsnr",
    ] {
        assert_eq!(ints(entry(stats, stage)), [1, 1], "{stage}");
        assert_eq!(ints(entry(window, stage)), [1, 1], "{stage}");
        for antenna in ["ant0", "ant1", "ant2"] {
            assert_eq!(
                ints(entry(entry(window, antenna), stage)),
                [1, 1],
                "{stage}"
            );
        }
    }
    let refant = |antenna: &str| ints(entry(entry(window, antenna), "used_as_refant"));
    assert_eq!([refant("ant0"), refant("ant1")], [[1], [0]]);

    // CASA lists the pre-applied tables sorted.
    std::fs::rename(dir.path().join("g.gcal"), dir.path().join("z.gcal")).unwrap();
    solve("a.gcal", json!([])); // a second table, named to sort first
    let value = solve("g2.gcal", json!(["z.gcal", "a.gcal"]));
    assert_eq!(structure(&value), casa("gaincal pre-applied"));
    assert_eq!(strs(entry(&value, "apply_tables")), ["a.gcal", "z.gcal"]);
}

#[test]
fn solvestats_counts_the_reference_antenna_flex_hands_on_to() {
    // Antenna 0, the preferred reference, has no data in the first of three
    // integrations. CASA's flex mode refers that interval to an alternate
    // and keeps it while antenna 0 lacks a solution in the interval before.
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    for row in [0, 1] {
        common::make_row_flag_only_outlier(&ms, row, Complex32::new(1.0, 0.0));
    }
    let value = returned(
        dir.path(),
        "gaincal",
        json!({
            "vis": ms.to_str().unwrap(), "caltable": "g.gcal", "refant": "0",
            "solint": "int", "minblperant": 1,
        }),
    );
    let window = entry(entry(&value, "solvestats"), "spw0");
    let antenna = |id: usize| entry(window, &format!("ant{id}"));
    assert_eq!(ints(entry(antenna(0), "expected")), [3, 3]);
    assert_eq!(ints(entry(antenna(0), "data_unflagged")), [2, 2]);
    assert_eq!(ints(entry(antenna(0), "above_minsnr")), [2, 2]);
    let used = (0..3)
        .map(|id| ints(entry(antenna(id), "used_as_refant"))[0])
        .collect::<Vec<_>>();
    // The first two intervals go to one alternate; antenna 0 takes the third.
    assert_eq!(used[0], 1, "{used:?}");
    assert!(
        used[1..].contains(&2) && used.iter().sum::<i64>() == 3,
        "{used:?}"
    );
}

/// A `solint` interval starts at its group's earliest time, whatever the
/// order of the fields `combine` joins: CASA sorts the combined axes out
/// before time.
#[test]
fn combined_fields_keep_casas_interval_boundaries() {
    let dir = TempDir::new().unwrap();
    let cluster = |time_seconds, scan_number| common::SyntheticGainTimeCluster {
        time_seconds,
        scan_number,
        gains: [[Complex32::new(1.0, 0.0); 2]; 3],
    };
    // Field 1 is observed first, in scan 1; field 0 thirty seconds later.
    let ms = common::create_gain_solve_fixture_ms_from_clusters(
        dir.path(),
        &[cluster(130.0, 2), cluster(131.0, 2), cluster(132.0, 2)],
    );
    let mut opened = casa_ms::ms::MeasurementSet::open(&ms).expect("open");
    for time_seconds in [100.0, 101.0, 102.0] {
        common::append_gain_solve_cluster_for_field(&mut opened, 1, &cluster(time_seconds, 1));
    }
    opened.save().expect("save");
    let value = returned(
        dir.path(),
        "gaincal",
        json!({
            "vis": ms.to_str().unwrap(), "caltable": "g.gcal", "refant": "0",
            "minblperant": 2, "combine": "scan,field", "solint": "20s",
        }),
    );
    assert_eq!(ints(entry(entry(&value, "solvestats"), "expected")), [2, 2]);
}

/// Two data descriptions on one window: CASA solves each apart, which
/// casa-rs does not yet, so the solve is refused naming #979, whether the
/// second repeats the first's correlations or holds the cross hands.
#[test]
fn data_descriptions_sharing_a_window_are_refused_naming_979() {
    for cross_hands in [false, true] {
        let dir = TempDir::new().unwrap();
        let ms =
            common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
        share_window_zero(&ms, cross_hands);
        for (surface, caltable) in [("gaincal", "g.gcal"), ("bandpass", "b.bcal")] {
            let (output, reply) = json_run(
                dir.path(),
                surface,
                json!({"vis": ms.to_str().unwrap(), "caltable": caltable, "refant": "0",
                       "minblperant": 2, "smodel": [1, 0, 0, 0]}),
            );
            match reply {
                Some(TaskReply::Failed { message, .. }) => assert!(
                    message.contains("share spectral window 0") && message.contains("#979"),
                    "{message}"
                ),
                other => panic!(
                    "{surface} {cross_hands}: {other:?}\n{}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            }
        }
    }
}

/// Puts every other MAIN row of `ms` on a new data description of window
/// 0, with polarization 0's correlations or a new setup of the cross hands.
fn share_window_zero(ms: &Path, cross_hands: bool) {
    use casa_ms::schema::{self as ms_schema, SubtableId};
    use casa_types::{ArrayValue, ScalarValue, Value as CellValue};
    let int = |value| CellValue::Scalar(ScalarValue::Int32(value));
    let mut opened = casa_ms::ms::MeasurementSet::open(ms).expect("open");
    let polarization_id = if cross_hands {
        let products = ndarray::ArrayD::from_shape_vec(vec![2, 2], vec![0, 1, 1, 0]).unwrap();
        let types = ndarray::ArrayD::from_shape_vec(vec![2], vec![6, 7]).unwrap();
        opened
            .subtable_mut(SubtableId::Polarization)
            .expect("POLARIZATION")
            .add_row(common::make_subtable_row(
                ms_schema::polarization::REQUIRED_COLUMNS,
                &[
                    ("NUM_CORR", int(2)),
                    ("CORR_TYPE", CellValue::Array(ArrayValue::Int32(types))),
                    (
                        "CORR_PRODUCT",
                        CellValue::Array(ArrayValue::Int32(products)),
                    ),
                ],
            ))
            .expect("add POLARIZATION row");
        1
    } else {
        0
    };
    let descriptions = opened
        .subtable_mut(SubtableId::DataDescription)
        .expect("DATA_DESCRIPTION");
    let description = i32::try_from(descriptions.row_count()).unwrap();
    descriptions
        .add_row(common::make_subtable_row(
            ms_schema::data_description::REQUIRED_COLUMNS,
            &[
                ("SPECTRAL_WINDOW_ID", int(0)),
                ("POLARIZATION_ID", int(polarization_id)),
            ],
        ))
        .expect("add DATA_DESCRIPTION row");
    let main = opened.main_table_mut();
    for row in (1..main.row_count()).step_by(2) {
        main.column_accessor_mut("DATA_DESC_ID")
            .and_then(|mut column| column.set(row, int(description)))
            .expect("set DATA_DESC_ID");
    }
    opened.save().expect("save");
}

#[test]
fn bandpass_returns_its_tables_and_selected_data() {
    let dir = TempDir::new().unwrap();
    let unit = [Complex32::new(1.0, 0.0); 2];
    let ms = common::create_bandpass_solve_fixture_ms(
        dir.path(),
        &[100.0, 101.0].map(|time_seconds| common::SyntheticBandpassTimeCluster {
            field_id: 0,
            time_seconds,
            scan_number: 1,
            prior_gains: [unit; 3],
            bandpass_gains: [[unit; 2]; 3],
        }),
    );
    // The fixture's three antennas have two baselines each, which CASA's
    // default minblperant of 4 does not solve; CASA writes no table.
    let (output, reply) = json_run(
        dir.path(),
        "bandpass",
        json!({
            "vis": ms.to_str().unwrap(), "caltable": "none.bcal", "refant": "0",
            "smodel": [1, 0, 0, 0],
        }),
    );
    match reply {
        Some(TaskReply::Failed { message, .. }) => {
            assert!(message.contains("no solution"), "{message}");
        }
        other => panic!("{other:?}\n{}", String::from_utf8_lossy(&output.stderr)),
    }

    let value = returned(
        dir.path(),
        "bandpass",
        json!({
            "vis": ms.to_str().unwrap(), "caltable": "b.bcal", "refant": "0",
            "smodel": [1, 0, 0, 0], "minblperant": 2,
        }),
    );
    assert_eq!(structure(&value), casa("bandpass"));
    assert_eq!(strs(entry(&value, "solve_tables")), ["b.bcal"]);
    // CASA counts a bandpass solve by interval, not by channel.
    let window = entry(entry(&value, "solvestats"), "spw0");
    assert_eq!(ints(entry(window, "expected")), [1, 1]);
    assert_eq!(ints(entry(window, "above_minblperant")), [1, 1]);
    assert_eq!(ints(entry(entry(window, "ant2"), "above_minsnr")), [1, 1]);
    assert_eq!(ints(entry(entry(window, "ant0"), "used_as_refant")), [1]);
}

#[test]
fn fluxscale_returns_flux_densities_per_field_and_window() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let row = |time_seconds, field_id, gain| common::SyntheticGainSolutionRow {
        time_seconds,
        field_id,
        spectral_window_id: 0,
        antenna_id: 0,
        gains: vec![Complex32::new(gain, 0.0); 2],
        flags: vec![false, false],
    };
    let gains = common::create_apply_gain_caltable(
        &dir.path().join("gain.gcal"),
        &["REF0", "TARGET0"],
        &[row(10.0, 0, 1.0), row(20.0, 1, 2.0)],
    );
    let value = returned(
        dir.path(),
        "fluxscale",
        json!({
            "vis": ms.to_str().unwrap(),
            "caltable": gains.to_str().unwrap(),
            "fluxtable": "flux.gcal",
            "reference": ["REF0"],
            "transfer": ["TARGET0"],
        }),
    );
    assert_eq!(structure(&value), casa("fluxscale"));
    let field = entry(&value, "1");
    assert_eq!(entry(field, "fieldName"), &CasaValue::string("TARGET0"));
    let CasaValue::FloatArray { shape, .. } = entry(entry(field, "0"), "fluxd") else {
        panic!("fluxd");
    };
    assert_eq!(shape, &[4]);
}

/// Two windows are fitted as CASA fits them, about their geometric mean
/// frequency (`fluxscale/fit.rs` holds CASA's probed values).
#[test]
fn fluxscale_fits_the_spectrum_across_windows() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let mut rows = Vec::new();
    for (spectral_window_id, transfer) in [(0, 2.0_f32), (1, 2.5)] {
        for antenna_id in 0..3 {
            let scatter = 1.0 + 0.02 * antenna_id as f32;
            for (time_seconds, field_id, gain) in
                [(10.0, 0, scatter), (20.0, 1, transfer * scatter)]
            {
                rows.push(common::SyntheticGainSolutionRow {
                    time_seconds,
                    field_id,
                    spectral_window_id,
                    antenna_id,
                    gains: vec![Complex32::new(gain, 0.0); 2],
                    flags: vec![false, false],
                });
            }
        }
    }
    let gains = common::create_apply_gain_caltable(
        &dir.path().join("gain.gcal"),
        &["REF0", "TARGET0"],
        &rows,
    );
    let value = returned(
        dir.path(),
        "fluxscale",
        json!({
            "vis": ms.to_str().unwrap(),
            "caltable": gains.to_str().unwrap(),
            "fluxtable": "flux.gcal",
            "reference": ["REF0"],
            "transfer": ["TARGET0"],
        }),
    );
    let float = |value: &CasaValue| match value {
        CasaValue::Float { value } => value.0,
        other => panic!("{other:?}"),
    };
    let floats = |value: &CasaValue| match value {
        CasaValue::FloatArray { shape, data, .. } => (
            shape.clone(),
            data.iter().map(|value| value.0).collect::<Vec<_>>(),
        ),
        other => panic!("{other:?}"),
    };
    // Each window's frequency is its middle channel, CASA's solFreq.
    let (_, frequencies) = floats(entry(&value, "freq"));
    assert_eq!(frequencies, [1.001e9, 1.101e9]);
    let field = entry(&value, "1");
    let fluxes = ["0", "1"].map(|window| floats(entry(entry(field, window), "fluxd")).1[0]);
    let reference_frequency = float(entry(field, "fitRefFreq"));
    assert!((reference_frequency - (1.001e9_f64 * 1.101e9).sqrt()).abs() < 1.0);
    let fitted = float(entry(field, "fitFluxd"));
    assert!(
        fluxes[0] < fitted && fitted < fluxes[1],
        "{fitted} {fluxes:?}"
    );
    let (_, spidx) = floats(entry(field, "spidx"));
    assert!(
        (spidx[0] - fitted.log10()).abs() < 1e-12 && spidx[1] > 0.0,
        "{spidx:?}"
    );
    // Two windows leave the fit no degree of freedom: no errors, but the
    // covariance of the two coefficients.
    assert_eq!(floats(entry(field, "spidxerr")).1, [0.0, 0.0]);
    assert_eq!(floats(entry(field, "covarMat")).0, [2, 2]);
}

#[test]
fn applycal_and_gencal_return_none() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let ms = ms.to_str().unwrap();
    returned(
        dir.path(),
        "gaincal",
        json!({"vis": ms, "caltable": "g.gcal", "refant": "0", "minblperant": 2}),
    );
    let value = returned(
        dir.path(),
        "applycal",
        json!({"vis": ms, "gaintable": ["g.gcal"], "flagbackup": false}),
    );
    assert_eq!(structure(&value), casa("applycal"));
    let value = returned(
        dir.path(),
        "gencal",
        json!({"vis": ms, "caltable": "o.cal", "caltype": "opac", "spw": "0", "parameter": [0.05]}),
    );
    assert_eq!(structure(&value), casa("gencal"));
}

#[test]
fn casa_parameters_not_implemented_yet_are_refused_naming_979() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let ms = ms.to_str().unwrap();
    let solve = json!({"vis": ms, "caltable": "g.gcal", "refant": "0"});
    let with = |base: &Value, name: &str, value: Value| {
        let mut values = base.clone();
        values[name] = value;
        values
    };
    for (surface, name, value) in [
        ("gaincal", "preavg", json!(10.0)),
        ("gaincal", "refantmode", json!("strict")),
        ("gaincal", "solmode", json!("L1")),
        ("gaincal", "uvrange", json!(">1klambda")),
        ("gaincal", "intent", json!("*PHASE*")),
        ("bandpass", "solint", json!("int")),
        ("bandpass", "fillgaps", json!(2)),
    ] {
        refused_naming_979(dir.path(), surface, with(&solve, name, value), name);
    }
    let flux = json!({
        "vis": ms, "caltable": "g.gcal", "fluxtable": "f.gcal", "reference": ["REF0"],
    });
    refused_naming_979(
        dir.path(),
        "fluxscale",
        with(&flux, "fitorder", json!(2)),
        "fitorder",
    );
    // CASA's fluxscale passes timerange and scan to the solve whatever
    // antenna is, though its task XML shows them under antenna.
    for (name, value) in [
        ("timerange", json!("2030/01/01~2030/01/02")),
        ("scan", json!("1")),
    ] {
        refused_naming_979(dir.path(), "fluxscale", with(&flux, name, value), name);
    }
    // gencal reads infile only for the caltypes CASA's task XML shows it
    // under, antpos among them.
    let gencal = json!({"vis": ms, "caltable": "o.cal", "caltype": "antpos"});
    refused_naming_979(
        dir.path(),
        "gencal",
        with(&gencal, "infile", json!("w.txt")),
        "infile",
    );
}

/// A sub-parameter CASA reads only under another parameter's value takes
/// any value elsewhere, and is ignored there, as CASA ignores it: gaincal's
/// GSPLINE controls under gaintype G, bandpass's BPOLY ones under B,
/// gencal's infile for an opacity table.
#[test]
fn a_sub_parameter_casa_ignores_takes_any_value() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    let ms = ms.to_str().unwrap();
    for (surface, values) in [
        (
            "gaincal",
            json!({"vis": ms, "caltable": "g.gcal", "refant": "0", "minblperant": 2,
                   "splinetime": 1800.0, "phasewrap": 90.0, "normtype": "median"}),
        ),
        (
            "bandpass",
            json!({"vis": ms, "caltable": "b.bcal", "refant": "0", "minblperant": 2,
                   "maskedge": 60}),
        ),
        (
            "gencal",
            json!({"vis": ms, "caltable": "o.cal", "caltype": "opac", "spw": "0",
                   "parameter": [0.05], "infile": "w.txt"}),
        ),
    ] {
        let (output, reply) = json_run(dir.path(), surface, values);
        assert!(
            matches!(reply, Some(TaskReply::Succeeded { .. })),
            "{surface}: {reply:?}\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

/// CASA's empty `smodel` reads a field's virtual model (`setjy` with
/// `usescratch=False`) before `MODEL_DATA`. casa-rs does not implement the
/// virtual model yet, so a solve that would read one is refused, naming
/// #979, rather than run against another model.
#[test]
fn an_empty_smodel_is_refused_where_casa_would_read_a_virtual_model() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    {
        let mut opened = casa_ms::ms::MeasurementSet::open(&ms).expect("open");
        let keywords = opened.main_table_mut().keywords_mut();
        keywords.upsert(
            "definedmodel_field_0",
            casa_types::Value::Scalar(casa_types::ScalarValue::String("model_0".into())),
        );
        keywords.upsert(
            "model_0",
            casa_types::Value::Record(casa_types::RecordValue::default()),
        );
        opened.save().expect("save");
    }
    let ms = ms.to_str().unwrap();
    for (surface, caltable) in [("gaincal", "g.gcal"), ("bandpass", "b.bcal")] {
        refused_naming_979(
            dir.path(),
            surface,
            json!({"vis": ms, "caltable": caltable, "refant": "0"}),
            "smodel",
        );
    }
    // An explicit point model is CASA's too, whatever the MeasurementSet holds.
    let (output, reply) = json_run(
        dir.path(),
        "gaincal",
        json!({"vis": ms, "caltable": "p.gcal", "refant": "0", "minblperant": 2,
               "smodel": [1, 0, 0, 0]}),
    );
    assert!(
        matches!(reply, Some(TaskReply::Succeeded { .. })),
        "{reply:?}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
