// SPDX-License-Identifier: LGPL-3.0-or-later

#[path = "../../casa-ms/tests/common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use casa_logging::validate_log_table;
use casa_ms::{MeasurementSet, MeasurementSetBuilder};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Complex32, ScalarValue, Value};
use ndarray::{ArrayD, IxDyn};
use tempfile::tempdir;

use common::{NUM_CHAN, NUM_CORR, create_msexplore_fixture_ms};

#[test]
fn clip_with_its_defaults_flags_nans_as_casa_does() {
    // Astra's #997 F4: CASA's clip mode with clipminmax=[] and
    // clipzeros=False flags NaNs; zeros stay.
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    seed_constant_data_with_outliers(&ms_path, &[(0, 0, 0, f32::NAN), (1, 2, 3, 0.0)]);
    let reply = flagdata_json_run(serde_json::json!({
        "vis": ms_path, "mode": "clip", "flagbackup": false,
    }));
    assert!(
        matches!(reply, casa_contracts::TaskReply::Succeeded { .. }),
        "{reply:?}"
    );
    let ms = MeasurementSet::open(&ms_path).expect("open MS");
    assert_eq!(flag_count(&ms), 1);
    assert_flag(&ms, 0, 0, 0);
    assert_not_flag(&ms, 1, 2, 3);
}

#[test]
fn the_flagdata_binary_applies_explicit_rflag_thresholds() {
    // Astra's #962 P2: on the front end, every explicit timedev or freqdev
    // was refused as not text, on the command line and on --json-run alike.
    let flagdata_bin = env!("CARGO_BIN_EXE_flagdata");
    for route in ["command line", "--json-run"] {
        let temp = tempdir().expect("tempdir");
        let ms_path = create_msexplore_fixture_ms(temp.path());
        seed_constant_data_with_outliers(&ms_path, &[(0, 0, 5, 100.0), (3, 1, 7, 120.0)]);
        let vis = ms_path.to_str().expect("UTF-8 MS path");
        let output = if route == "--json-run" {
            let request = temp.path().join("request.json");
            let values = serde_json::json!({
                "surface": "flagdata",
                "values": {
                    "vis": vis, "mode": "rflag", "flagbackup": false,
                    // A fractional and an integer threshold.
                    "timedev": 1.0, "freqdev": 10,
                },
            });
            std::fs::write(&request, values.to_string()).expect("write request");
            Command::new(flagdata_bin)
                .args(["--json-run", request.to_str().expect("UTF-8 path")])
                .output()
        } else {
            Command::new(flagdata_bin)
                .args(["--vis", vis, "--mode", "rflag", "--no-flagbackup"])
                .args(["--timedev", "1.0", "--freqdev", "10", "--no-save-last"])
                .output()
        }
        .expect("run flagdata");
        assert!(
            output.status.success(),
            "{route}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        let ms = MeasurementSet::open(&ms_path).expect("open MS");
        assert_flag(&ms, 0, 0, 5);
        assert_flag(&ms, 3, 1, 7);
    }
}

/// The `flagdata` binary's reply to a `--json-run` request of `values`.
fn flagdata_json_run(values: serde_json::Value) -> casa_contracts::TaskReply {
    let temp = tempdir().expect("tempdir");
    let request = temp.path().join("request.json");
    let request_text = serde_json::json!({"surface": "flagdata", "values": values}).to_string();
    std::fs::write(&request, request_text).expect("write request");
    let output = Command::new(env!("CARGO_BIN_EXE_flagdata"))
        .args(["--json-run", request.to_str().expect("UTF-8 path")])
        .output()
        .expect("run flagdata");
    serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("{error}: {}", String::from_utf8_lossy(&output.stderr)))
}

/// The flags an rflag run of `values` leaves on a fresh fixture whose DATA
/// is `data(row, corr, chan)`, with `rows` as (row, scan, time offset).
fn rflag_flags(
    rows: &[(usize, i32, f64)],
    data: impl Fn(usize, usize, usize) -> f32,
    values: serde_json::Value,
) -> usize {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());
    set_scan_and_time(&ms_path, rows);
    seed_data(&ms_path, data);
    let mut values = values;
    values["vis"] = serde_json::json!(ms_path);
    values["mode"] = serde_json::json!("rflag");
    values["extendflags"] = serde_json::json!(false);
    values["flagbackup"] = serde_json::json!(false);
    let reply = flagdata_json_run(values);
    assert!(
        matches!(reply, casa_contracts::TaskReply::Succeeded { .. }),
        "{reply:?}"
    );
    flag_count(&MeasurementSet::open(&ms_path).expect("open MS"))
}

#[test]
fn rflag_scales_a_given_threshold_as_it_scales_a_computed_one() {
    // Astra's #997 F2: CASA multiplies a given timedev or freqdev by its
    // scale too. Each row a scan of its own, every spectrum zero but for 2
    // in its last channel: a spectral deviation of 2 from the median.
    let separate = [(0, 1, 0.0), (1, 2, 1.0), (2, 3, 2.0), (3, 4, 3.0)];
    let spike = |_: usize, _: usize, chan: usize| if chan == NUM_CHAN - 1 { 2.0 } else { 0.0 };
    let spectral = |scale: f64| {
        rflag_flags(
            &separate,
            spike,
            serde_json::json!({"timedev": 1e9, "freqdev": 1.0, "freqdevscale": scale}),
        )
    };
    assert_eq!(spectral(1.0), 4 * NUM_CORR, "one channel of each spectrum");
    assert_eq!(spectral(5.0), 0, "CASA flags none at freqdev 1.0 times 5");

    // One scan of four times; correlation 0 of channel 5 is 3 at the third:
    // each three-time window of it has a deviation of sqrt(2).
    let scan = [(0, 1, 0.0), (1, 1, 1.0), (2, 1, 2.0), (3, 1, 3.0)];
    let step = |row: usize, corr: usize, chan: usize| {
        if (row, corr, chan) == (2, 0, 5) {
            3.0
        } else {
            0.0
        }
    };
    let temporal = |scale: f64| {
        rflag_flags(
            &scan,
            step,
            serde_json::json!({"timedev": 1.0, "freqdev": 1e9, "timedevscale": scale}),
        )
    };
    assert_eq!(temporal(1.0), 4, "channel 5 of correlation 0 at every time");
    assert_eq!(temporal(5.0), 0);
}

#[test]
fn rflag_accepts_computed_matrix_and_file_thresholds() {
    // Empty lists and strings request automatic thresholds. Per-field/SPW
    // values from a matrix and a CASA threshold file use the same cutoff.
    for name in ["timedev", "freqdev"] {
        let fixture = tempdir().expect("tempdir");
        let threshold_file = fixture.path().join("thresholds.txt");
        std::fs::write(&threshold_file, format!("{{'{name}': [[0, 0, 1.5]]}}\n"))
            .expect("write CASA threshold file");
        let threshold = |value: serde_json::Value| {
            let temp = tempdir().expect("tempdir");
            let ms_path = create_msexplore_fixture_ms(temp.path());
            let reply = flagdata_json_run(serde_json::json!({
                "vis": ms_path, "mode": "rflag", "flagbackup": false, name: value,
            }));
            let casa_contracts::TaskReply::Succeeded { result, .. } = reply else {
                panic!("{name}={value}: {reply:?}");
            };
            result[name].as_f64().expect("reported RFlag cutoff")
        };
        assert_eq!(
            threshold(serde_json::json!([])),
            threshold(serde_json::json!(""))
        );
        assert_eq!(threshold(serde_json::json!([[0, 0, 1.5]])), 7.5);
        assert_eq!(threshold(serde_json::json!(threshold_file)), 7.5);
    }
}

#[test]
fn extend_grows_each_correlation_on_its_own_and_reads_no_data_column() {
    // Astra's #997 F3 and F10, through the binary: half of correlation 0's
    // channels flagged on row 0 grow to all of them with growfreq=50, and to
    // every correlation only with extendpols. CASA's extension agent reads
    // flags alone, so CORRECTED on a MeasurementSet without the column is
    // no failure.
    for (extendpols, expected) in [(false, NUM_CHAN), (true, NUM_CORR * NUM_CHAN)] {
        let temp = tempdir().expect("tempdir");
        let ms_path = create_msexplore_fixture_ms(temp.path());
        for chan in 0..NUM_CHAN / 2 {
            set_flag(&ms_path, 0, 0, chan, true);
        }
        let reply = flagdata_json_run(serde_json::json!({
            "vis": ms_path, "mode": "extend", "datacolumn": "CORRECTED",
            "growtime": 0.0, "growfreq": 50.0, "extendpols": extendpols,
            "flagbackup": false,
        }));
        assert!(
            matches!(reply, casa_contracts::TaskReply::Succeeded { .. }),
            "{reply:?}"
        );
        let ms = MeasurementSet::open(&ms_path).expect("open MS");
        assert_eq!(flag_count(&ms), expected, "extendpols={extendpols}");
        assert_flag(&ms, 0, 0, NUM_CHAN - 1);
    }
}

#[test]
fn flagdata_binary_emits_json() {
    let temp = tempdir().expect("tempdir");
    let ms_path = create_msexplore_fixture_ms(temp.path());

    let flagdata_output = Command::new(env!("CARGO_BIN_EXE_flagdata"))
        .args([
            "--vis",
            ms_path.to_str().expect("UTF-8 MS path"),
            "--mode",
            "summary",
            "--no-flagbackup",
        ])
        .output()
        .expect("run flagdata");
    assert!(
        flagdata_output.status.success(),
        "flagdata stderr: {}",
        String::from_utf8_lossy(&flagdata_output.stderr)
    );
    let flagdata_json: serde_json::Value =
        serde_json::from_slice(&flagdata_output.stdout).expect("flagdata JSON");
    assert_eq!(flagdata_json["mode"], "summary");
    assert_eq!(flagdata_json["flagged_samples"], 0);
}

/// Set every row's DATA to `data(row, corr, chan)`.
fn seed_data(ms_path: &std::path::Path, data: impl Fn(usize, usize, usize) -> f32) {
    let mut ms = MeasurementSet::open(ms_path).expect("open MS for seed");
    for row in 0..ms.row_count() {
        let values = ArrayD::from_shape_fn(IxDyn(&[NUM_CORR, NUM_CHAN]), |index| {
            Complex32::new(data(row, index[0], index[1]), 0.0)
        });
        ms.main_table_mut()
            .cell_accessor_mut(row, "DATA")
            .expect("DATA cell")
            .set(Value::Array(ArrayValue::Complex32(values)))
            .expect("set DATA");
    }
    ms.save_main_table_only().expect("save seeded DATA");
}

fn seed_constant_data_with_outliers(
    ms_path: &std::path::Path,
    outliers: &[(usize, usize, usize, f32)],
) {
    let mut ms = MeasurementSet::open(ms_path).expect("open MS for seed");
    for row in 0..ms.row_count() {
        let mut values = ArrayD::from_elem(IxDyn(&[NUM_CORR, NUM_CHAN]), Complex32::new(1.0, 0.0));
        for &(outlier_row, corr, chan, amp) in outliers {
            if outlier_row == row {
                values[IxDyn(&[corr, chan])] = Complex32::new(amp, 0.0);
            }
        }
        ms.main_table_mut()
            .cell_accessor_mut(row, "DATA")
            .expect("DATA cell")
            .set(Value::Array(ArrayValue::Complex32(values)))
            .expect("set DATA");
    }
    ms.save_main_table_only().expect("save seeded DATA");
}

fn assert_flag(ms: &MeasurementSet, row: usize, corr: usize, chan: usize) {
    let flags = match ms
        .main_table()
        .cell_accessor(row, "FLAG")
        .expect("FLAG cell")
        .value()
        .expect("FLAG value")
        .expect("defined FLAG")
    {
        Value::Array(ArrayValue::Bool(flags)) => flags,
        other => panic!("unexpected FLAG value {other:?}"),
    };
    assert!(flags[IxDyn(&[corr, chan])]);
}

fn assert_not_flag(ms: &MeasurementSet, row: usize, corr: usize, chan: usize) {
    let flags = match ms
        .main_table()
        .cell_accessor(row, "FLAG")
        .expect("FLAG cell")
        .value()
        .expect("FLAG value")
        .expect("defined FLAG")
    {
        Value::Array(ArrayValue::Bool(flags)) => flags,
        other => panic!("unexpected FLAG value {other:?}"),
    };
    assert!(!flags[IxDyn(&[corr, chan])]);
}

fn flag_count(ms: &MeasurementSet) -> usize {
    (0..ms.row_count())
        .map(|row| {
            let flags = match ms
                .main_table()
                .cell_accessor(row, "FLAG")
                .expect("FLAG cell")
                .value()
                .expect("FLAG value")
                .expect("defined FLAG")
            {
                Value::Array(ArrayValue::Bool(flags)) => flags,
                other => panic!("unexpected FLAG value {other:?}"),
            };
            flags.iter().filter(|flag| **flag).count()
        })
        .sum()
}

fn set_scan_and_time(ms_path: &std::path::Path, rows: &[(usize, i32, f64)]) {
    let mut ms = MeasurementSet::open(ms_path).expect("open MS for scan/time seed");
    for &(row, scan, offset) in rows {
        ms.main_table_mut()
            .cell_accessor_mut(row, "SCAN_NUMBER")
            .expect("SCAN_NUMBER cell")
            .set(Value::Scalar(ScalarValue::Int32(scan)))
            .expect("set SCAN_NUMBER");
        ms.main_table_mut()
            .cell_accessor_mut(row, "TIME")
            .expect("TIME cell")
            .set(Value::Scalar(ScalarValue::Float64(
                common::TIME_BASE_SECONDS + offset,
            )))
            .expect("set TIME");
    }
    ms.save_main_table_only()
        .expect("save scan/time seeded MAIN");
}

fn set_flag(ms_path: &std::path::Path, row: usize, corr: usize, chan: usize, value: bool) {
    let mut ms = MeasurementSet::open(ms_path).expect("open MS for flag seed");
    set_flag_on_open_ms(&mut ms, row, corr, chan, value);
    ms.save_main_table_only().expect("save flag seeded MAIN");
}

fn set_flag_on_open_ms(ms: &mut MeasurementSet, row: usize, corr: usize, chan: usize, value: bool) {
    let mut flags = match ms
        .main_table()
        .cell_accessor(row, "FLAG")
        .expect("FLAG cell")
        .value()
        .expect("FLAG value")
        .expect("defined FLAG")
    {
        Value::Array(ArrayValue::Bool(flags)) => flags.clone(),
        other => panic!("unexpected FLAG value {other:?}"),
    };
    flags[IxDyn(&[corr, chan])] = value;
    let flag_row = flags.iter().all(|flag| *flag);
    ms.main_table_mut()
        .cell_accessor_mut(row, "FLAG")
        .expect("FLAG cell")
        .set(Value::Array(ArrayValue::Bool(flags)))
        .expect("set FLAG");
    ms.main_table_mut()
        .cell_accessor_mut(row, "FLAG_ROW")
        .expect("FLAG_ROW cell")
        .set(Value::Scalar(ScalarValue::Bool(flag_row)))
        .expect("set FLAG_ROW");
}

#[test]
fn flagdata_empty_selection_writes_warn_log_row() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let ms_path = tempdir.path().join("empty.ms");
    let mut ms = MeasurementSet::create(&ms_path, MeasurementSetBuilder::new()).expect("create MS");
    common::populate_subtables(&mut ms);
    ms.save().expect("save empty MS");
    drop(ms);

    let log_table = tempdir.path().join("flagdata.log");
    let output = Command::new(env!("CARGO_BIN_EXE_flagdata"))
        .arg("--log-table")
        .arg(&log_table)
        .arg("--log-stderr-priority")
        .arg("off")
        .arg("--vis")
        .arg(&ms_path)
        .arg("--mode")
        .arg("summary")
        .output()
        .expect("run flagdata summary with empty selection");

    assert!(
        output.status.success(),
        "flagdata failed: stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stderr), "");

    let table = Table::open(TableOptions::new(&log_table)).expect("open log table");
    validate_log_table(&table).expect("validate log table");
    assert_log_contains(&table, "WARN", "flagdata selected no rows");
}

#[test]
fn flagdata_unknown_field_writes_severe_log_row() {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let ms_path = common::create_msexplore_fixture_ms(tempdir.path());
    let log_table = tempdir.path().join("flagdata.log");
    let output = Command::new(env!("CARGO_BIN_EXE_flagdata"))
        .arg("--log-table")
        .arg(&log_table)
        .arg("--log-stderr-priority")
        .arg("off")
        .arg("--vis")
        .arg(&ms_path)
        .args(["--mode", "summary", "--field", "999"])
        .output()
        .expect("run flagdata summary with an unknown field");

    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("did not match any existing fields"));
    let table = Table::open(TableOptions::new(&log_table)).expect("open log table");
    validate_log_table(&table).expect("validate log table");
    assert_log_contains(&table, "SEVERE", "flagdata failed");
}

fn assert_log_contains(table: &Table, priority: &str, message_fragment: &str) {
    let priorities = table
        .column_accessor("PRIORITY")
        .expect("PRIORITY column")
        .scalar_cells_owned()
        .expect("PRIORITY cells");
    let messages = table
        .column_accessor("MESSAGE")
        .expect("MESSAGE column")
        .scalar_cells_owned()
        .expect("MESSAGE cells");
    let found = priorities
        .iter()
        .zip(messages.iter())
        .any(|(row_priority, row_message)| {
            matches!(row_priority, Some(ScalarValue::String(value)) if value == priority)
                && matches!(row_message, Some(ScalarValue::String(value)) if value.contains(message_fragment))
        });
    assert!(
        found,
        "expected {priority} log row containing {message_fragment:?}"
    );
}

/// Every file under `path`, by relative path, with its bytes.
fn snapshot(path: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    fn walk(root: &Path, path: &Path, files: &mut BTreeMap<PathBuf, Vec<u8>>) {
        if path.is_dir() {
            for entry in std::fs::read_dir(path).unwrap() {
                walk(root, &entry.unwrap().path(), files);
            }
        } else {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            files.insert(relative, std::fs::read(path).unwrap());
        }
    }
    let mut files = BTreeMap::new();
    walk(path, path, &mut files);
    files
}

#[test]
fn summary_and_refused_selection_preserve_the_measurement_set() {
    let temp = tempdir().expect("tempdir");
    let vis = create_msexplore_fixture_ms(temp.path());
    let before = snapshot(&vis);
    for (options, success) in [
        (vec!["--mode", "summary"], true),
        (
            vec!["--mode", "manual", "--spw", "0:never", "--no-flagbackup"],
            false,
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_flagdata"))
            .arg("--vis")
            .arg(&vis)
            .args(options)
            .arg("--no-save-last")
            .output()
            .expect("run flagdata");
        assert_eq!(output.status.success(), success, "{output:?}");
        assert_eq!(snapshot(&vis), before);
        assert!(
            !temp
                .path()
                .join("msexplore_fixture.ms.flagversions")
                .exists()
        );
    }
}
