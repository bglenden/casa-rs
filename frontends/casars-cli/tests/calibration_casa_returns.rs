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

#[path = "../../../libs/casa/casa-calibration/tests/common/mod.rs"]
mod common;
#[path = "../../../libs/casacore/casa-ms/tests/common/mod.rs"]
mod ms_common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use casa_contracts::{CasaKey, CasaValue, FloatDtype, IntDtype, TaskReply};
use casa_core::Complex32;
use serde_json::{Value, json};
use tempfile::TempDir;

/// `calibrate SURFACE --json-run -` with `values`, in `workspace`.
fn json_run(workspace: &Path, surface: &str, values: Value) -> (Output, Option<TaskReply>) {
    json_run_binary(env!("CARGO_BIN_EXE_calibrate"), workspace, surface, values)
}

fn json_run_binary(
    binary: &str,
    workspace: &Path,
    surface: &str,
    values: Value,
) -> (Output, Option<TaskReply>) {
    let mut child = Command::new(binary)
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

/// `value`'s structure as the CASA recording describes Python values, an
/// array by its dtype and number of axes.
fn describe(value: &CasaValue) -> Value {
    let array = |dtype: &str, shape: &[usize]| json!({"type": "ndarray", "dtype": dtype, "ndim": shape.len()});
    match value {
        CasaValue::None => json!({"type": "None"}),
        CasaValue::Bool { .. } => json!({"type": "bool"}),
        CasaValue::Int { .. } => json!({"type": "int"}),
        CasaValue::Float { .. } => json!({"type": "float"}),
        CasaValue::Complex { .. } => json!({"type": "complex"}),
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
        CasaValue::UInt64Array { shape, .. } => array("uint64", shape),
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
                "gaintable": gaintable, "preavg": 10.0, "refantmode": "strict",
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
    let mut opened = casa_ms::MeasurementSet::open(&ms).expect("open");
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

/// Gaincal accepts either correlation layout on descriptions of the same
/// window. Bandpass support is tracked on its own calibration ticket.
#[test]
fn gaincal_and_bandpass_accept_data_descriptions_sharing_a_window() {
    for cross_hands in [false, true] {
        let dir = TempDir::new().unwrap();
        let ms =
            common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
        share_window_zero(&ms, cross_hands);
        let parameters = |caltable| {
            json!({"vis": ms.to_str().unwrap(), "caltable": caltable, "refant": "0",
                   "minblperant": 2, "smodel": [1, 0, 0, 0]})
        };
        let value = returned(dir.path(), "gaincal", parameters("g.gcal"));
        assert_eq!(ints(entry(entry(&value, "selectvis"), "spw")), [0]);
        let value = returned(dir.path(), "bandpass", parameters("b.bcal"));
        assert_eq!(ints(entry(entry(&value, "selectvis"), "spw")), [0]);
    }
}

/// Puts every other MAIN row of `ms` on a new data description of window
/// 0, with polarization 0's correlations or a new setup of the cross hands.
fn share_window_zero(ms: &Path, cross_hands: bool) {
    use casa_core::{ArrayValue, ScalarValue, Value as CellValue};
    use casa_ms::schema::{self as ms_schema, SubtableId};
    let int = |value| CellValue::Scalar(ScalarValue::Int32(value));
    let mut opened = casa_ms::MeasurementSet::open(ms).expect("open");
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
    // Pinned CASA returns the statistics even when no channel solves (#945).
    assert!(output.status.success());
    let Some(TaskReply::Succeeded { value, .. }) = reply else {
        panic!("no-solution solve must succeed")
    };
    assert_eq!(
        entry(&value, "solve_tables"),
        &CasaValue::StrArray {
            shape: vec![1],
            data: vec!["none.bcal".to_string()]
        }
    );
    assert!(!dir.path().join("none.bcal").exists());

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
    let ms = fluxscale_ms(dir.path());
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
    let ms = fluxscale_ms(dir.path());
    let mut rows = Vec::new();
    for (spectral_window_id, transfer) in [(0, 2.0_f32), (1, 2.5)] {
        for antenna_id in 0..3 {
            let scatter = 1.0 + 0.02 * antenna_id as f32;
            // Common antenna scatter cancels in CASA's paired means. Give
            // the transfer independent scatter so both flux errors are
            // positive and this tests a determined, weighted spectral fit.
            let transfer_gain = transfer * scatter * (1.0 + 0.01 * (antenna_id as f32 - 1.0));
            for (time_seconds, field_id, gain) in [(10.0, 0, scatter), (20.0, 1, transfer_gain)] {
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

/// Fluxscale resolves names against vis.FIELD. Match the synthetic gains'
/// names rather than relying on the former implementation ignoring vis.
fn fluxscale_ms(root: &Path) -> std::path::PathBuf {
    let path = common::create_apply_fixture_ms(root, false);
    let mut ms = casa_ms::MeasurementSet::open(&path).unwrap();
    let fields = ms.subtable_mut(casa_ms::schema::SubtableId::Field).unwrap();
    for (row, name) in ["REF0", "TARGET0"].into_iter().enumerate() {
        fields
            .cell_accessor_mut(row, "NAME")
            .unwrap()
            .set(casa_core::Value::Scalar(casa_core::ScalarValue::String(
                name.into(),
            )))
            .unwrap();
    }
    ms.save().unwrap();
    path
}

#[test]
fn implemented_fluxscale_options_select_samples_and_set_the_fit_order() {
    let dir = TempDir::new().unwrap();
    let ms = fluxscale_ms(dir.path());
    let samples = [
        (10., 0, 1., 1),
        (11., 0, 3., 2),
        (20., 1, 2., 1),
        (21., 1, 10., 2),
        (30., 1, 12., 1),
    ];
    let rows = samples.map(
        |(time_seconds, field_id, gain, _)| common::SyntheticGainSolutionRow {
            time_seconds,
            field_id,
            spectral_window_id: 0,
            antenna_id: 0,
            gains: vec![Complex32::new(gain, 0.); 2],
            flags: vec![false; 2],
        },
    );
    let gain = common::create_apply_gain_caltable(
        &dir.path().join("gain.gcal"),
        &["REF0", "TARGET0"],
        &rows,
    );
    let mut table = casa_tables::Table::open(casa_tables::TableOptions::new(&gain)).unwrap();
    for (row, (_, _, _, scan)) in samples.into_iter().enumerate() {
        table
            .cell_accessor_mut(row, "SCAN_NUMBER")
            .unwrap()
            .set(casa_core::Value::Scalar(casa_core::ScalarValue::Int32(
                scan,
            )))
            .unwrap();
    }
    table.save(casa_tables::TableOptions::new(&gain)).unwrap();
    let value = returned(
        dir.path(),
        "fluxscale",
        json!({
            "vis": ms.to_str().unwrap(), "caltable": gain.to_str().unwrap(),
            "fluxtable": "flux.gcal", "reference": "REF0", "transfer": "TARGET0",
            "fitorder": 2, "scan": "1",
            "timerange": "1858/11/17/00:00:09~1858/11/17/00:00:25",
        }),
    );
    let field = entry(&value, "1");
    let CasaValue::FloatArray { data, .. } = entry(entry(field, "0"), "fluxd") else {
        panic!("fluxd")
    };
    // Time excludes gain 12; scan excludes gains 3 and 10. The retained
    // transfer/reference amplitude ratio is 2, independently giving 4 Jy.
    assert_eq!(data[0].0, 4.);
    let CasaValue::FloatArray { shape, data, .. } = entry(field, "spidx") else {
        panic!("spidx")
    };
    assert_eq!(shape, &[3]);
    assert!(data.iter().all(|value| value.0 == 0.));
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
fn gencal_refuses_a_missing_tec_image_without_publishing_a_table() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    // TEC calibration is implemented; an absent input is still a refusal,
    // naming the input parameter before any calibration table is published.
    let (output, reply) = json_run(
        dir.path(),
        "gencal",
        json!({"vis": ms, "caltable": "tec.cal", "caltype": "tecim", "infile": "iono.im"}),
    );
    assert!(!output.status.success());
    match reply {
        Some(TaskReply::Failed {
            parameter: Some(name),
            message,
            ..
        }) => {
            assert_eq!(name, "infile");
            assert_eq!(message, "An existing tec map must be specified in infile");
        }
        other => panic!("expected missing TEC input refusal: {other:?}"),
    }
    assert!(!dir.path().join("tec.cal").exists());
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
                   "splinetime": 1800.0, "phasewrap": 90.0, "normtype": "median", "rmsthresh": [0.1]}),
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

/// A field with a virtual record and no matching entries uses the unit model.
#[test]
fn gaincal_and_bandpass_use_the_unit_model_for_an_empty_virtual_record() {
    let dir = TempDir::new().unwrap();
    let ms = common::create_gain_solve_fixture_ms(dir.path(), common::SyntheticGainFixtureKind::G);
    {
        let mut opened = casa_ms::MeasurementSet::open(&ms).expect("open");
        let keywords = opened.main_table_mut().keywords_mut();
        keywords.upsert(
            "definedmodel_field_0",
            casa_core::Value::Scalar(casa_core::ScalarValue::String("model_0".into())),
        );
        keywords.upsert(
            "model_0",
            casa_core::Value::Record(casa_core::RecordValue::new(vec![
                casa_core::RecordField::new(
                    "version",
                    casa_core::Value::Scalar(casa_core::ScalarValue::Int32(2)),
                ),
            ])),
        );
        opened.save().expect("save");
    }
    let ms = ms.to_str().unwrap();
    for surface in ["gaincal", "bandpass"] {
        for (label, smodel) in [("virtual", json!([])), ("point", json!([1, 0, 0, 0]))] {
            let caltable = format!("{surface}-{label}.cal");
            let (output, reply) = json_run(
                dir.path(),
                surface,
                json!({"vis": ms, "caltable": caltable, "refant": "0", "minblperant": 2,
                       "minsnr": 0.0, "smodel": smodel}),
            );
            assert!(
                matches!(reply, Some(TaskReply::Succeeded { .. })),
                "{reply:?}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(dir.path().join(caltable).is_dir());
        }
    }
}

const MSTRANSFORM: &str = env!("CARGO_BIN_EXE_mstransform");

#[test]
fn split_and_mstransform_return_none() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = ms_common::create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    // The fixture has no CORRECTED_DATA, CASA's default column.
    for (surface, extra) in [
        ("split", json!({"width": 2})),
        ("mstransform", json!({"chanaverage": true, "chanbin": 2})),
    ] {
        let outputvis = workspace.path().join(format!("{surface}.ms"));
        let mut values = json!({"vis": vis, "outputvis": outputvis, "datacolumn": "data"});
        values
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let (_, reply) = json_run_binary(MSTRANSFORM, workspace.path(), surface, values);
        let Some(TaskReply::Succeeded { value, .. }) = reply else {
            panic!("{surface} returned {reply:?}");
        };
        assert_eq!(describe(&value), casa(surface), "{surface}");
        assert!(outputvis.exists(), "{surface}");
    }
}

#[test]
fn transform_refuses_channel_averaging_by_one_before_creating_output() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = ms_common::create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let outputvis = workspace.path().join("out.ms");
    // CASA's mstransform refuses to average by one channel, as casa-rs now
    // does, rather than copy the data unaveraged (Astra's #997 F8).
    let values = json!({"vis": vis, "outputvis": outputvis, "datacolumn": "data",
                        "chanaverage": true});
    let Some(TaskReply::Failed {
        message, parameter, ..
    }) = json_run_binary(MSTRANSFORM, workspace.path(), "mstransform", values).1
    else {
        panic!("chanaverage with chanbin=1 was not refused");
    };
    assert_eq!(parameter.as_deref(), Some("chanbin"), "{message}");
    assert!(message.contains("must be > 1"), "{message}");
    assert!(!outputvis.exists());
}

#[test]
fn ignored_transform_sub_parameters_do_not_refuse() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = ms_common::create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let outputvis = workspace.path().join("out.ms");
    let values = json!({"vis": vis, "outputvis": outputvis, "datacolumn": "data",
                        "nchan": 3, "outframe": "LSRK", "timebin": "10s", "fitorder": 2});
    let (_, reply) = json_run_binary(MSTRANSFORM, workspace.path(), "mstransform", values);
    assert!(
        matches!(reply, Some(TaskReply::Succeeded { .. })),
        "{reply:?}"
    );
    assert!(outputvis.exists());
}
