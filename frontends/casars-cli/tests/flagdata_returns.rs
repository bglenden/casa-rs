// SPDX-License-Identifier: LGPL-3.0-or-later

//! The MeasurementSet task binaries return what CASA 6.7.6's tasks return
//! (#931): the structure of each return is compared with CASA's own,
//! recorded in `resources/casa-compat/casa-6.7.6.14/returns.json` by
//! `tools/casa-compat/record_casa_returns.py`. The CASA values casa-rs does
//! not implement yet are refused before the task starts, naming #978.

#[path = "../../../libs/casacore/casa-ms/tests/common/mod.rs"]
mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_contracts::{CasaKey, CasaValue, TaskReply};
use common::{NUM_CHAN, NUM_CORR, create_msexplore_fixture_ms_with_flags};
use serde_json::{Value, json};

const FLAGDATA: &str = env!("CARGO_BIN_EXE_flagdata");

/// `binary`'s reply to a `--json-run` request of `values` on `surface`.
fn json_run(binary: &str, surface: &str, values: Value, workspace: &Path) -> TaskReply {
    let mut child = Command::new(binary)
        .args(["--json-run", "-"])
        .current_dir(workspace)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start task binary");
    let request = json!({"surface": surface, "values": values}).to_string();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(request.as_bytes())
        .unwrap();
    let output = child.wait_with_output().expect("run task binary");
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "{surface}: {error}\n{}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

/// What `binary` returns for `values` on `surface`, which must succeed.
fn returned(binary: &str, surface: &str, values: Value, workspace: &Path) -> CasaValue {
    match json_run(binary, surface, values, workspace) {
        TaskReply::Succeeded { value, .. } => value,
        other => panic!("{surface}: {other:?}"),
    }
}

/// `value`'s structure, as the CASA recording describes Python values.
fn describe(value: &CasaValue) -> Value {
    match value {
        CasaValue::None => json!({"type": "None"}),
        CasaValue::Bool { .. } => json!({"type": "bool"}),
        CasaValue::Int { .. } => json!({"type": "int"}),
        CasaValue::Float { .. } => json!({"type": "float"}),
        CasaValue::String { .. } => json!({"type": "str"}),
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
        other => panic!("no MeasurementSet task returns {other:?}"),
    }
}

/// CASA's recorded return value for `case`.
fn casa(case: &str) -> Value {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../resources/casa-compat/casa-6.7.6.14/returns.json");
    let document: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    assert_eq!(document["casa_version"], "6.7.6.14");
    document["cases"][case]["returns"].clone()
}

/// A summary's structure with each breakdown's keys (antenna names, scan
/// numbers...) replaced by the one structure its entries share: the
/// recording's MeasurementSet is not this one.
fn summary_shape(structure: &Value) -> Value {
    let items = structure["items"].as_array().expect("a dictionary");
    let items = items
        .iter()
        .map(|item| {
            let value = &item[2];
            let Some(entries) = value["items"].as_array() else {
                return item.clone();
            };
            let shapes = entries.iter().map(|entry| &entry[2]).collect::<Vec<_>>();
            assert!(!shapes.is_empty(), "{}: no entries", item[0]);
            assert!(shapes.iter().all(|shape| shape == &shapes[0]), "{item}");
            json!([item[0], item[1], {"type": "dict", "each": shapes[0]}])
        })
        .collect::<Vec<_>>();
    json!({"type": structure["type"], "items": items})
}

/// The entry `key` of the dictionary `value`.
fn entry<'a>(value: &'a CasaValue, key: &str) -> &'a CasaValue {
    let CasaValue::Dict { entries } = value else {
        panic!("{value:?}");
    };
    &entries
        .iter()
        .find(|entry| entry.key == CasaKey::String(key.to_string()))
        .unwrap_or_else(|| panic!("no {key:?} in {value:?}"))
        .value
}

#[test]
fn flagdata_summary_returns_casas_dictionary() {
    let workspace = tempfile::tempdir().unwrap();
    let flagged = [(0, 0, 0), (1, 1, 2), (3, 3, 15)];
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &flagged);
    let summary = returned(
        FLAGDATA,
        "flagdata",
        json!({"vis": vis, "mode": "summary"}),
        workspace.path(),
    );
    assert_eq!(
        summary_shape(&describe(&summary)),
        summary_shape(&casa("flagdata summary"))
    );
    let total = (4 * NUM_CORR * NUM_CHAN) as f64;
    assert_eq!(entry(&summary, "flagged"), &CasaValue::float(3.0));
    assert_eq!(entry(&summary, "total"), &CasaValue::float(total));
    assert_eq!(entry(&summary, "name"), &CasaValue::string("Summary"));
    assert_eq!(entry(&summary, "type"), &CasaValue::string("summary"));
}

/// The flagged and total counts of `key` in the summary's `axis`.
fn counts(summary: &CasaValue, axis: &str, key: &str) -> (CasaValue, CasaValue) {
    let counts = entry(entry(summary, axis), key);
    (
        entry(counts, "flagged").clone(),
        entry(counts, "total").clone(),
    )
}

#[test]
fn flagdata_summary_counts_each_row_by_its_own_data_description() {
    // Astra's #997 review ran CASA 6.7.6.14 on two DATA_DESC_IDs sharing a
    // window, XX/YY and RR/LL, in two observations and arrays and one scan:
    // each row counts under its own data description's correlations and
    // its own observation and array, the window and scan once for all.
    let workspace = tempfile::tempdir().unwrap();
    let flagged = [(0, 0, 0), (2, 3, 0), (2, 3, 1), (2, 3, 2), (2, 3, 3)];
    let vis =
        common::create_two_polarization_fixture_ms(workspace.path(), [9, 10, 11, 12], &flagged);
    let summary = returned(
        FLAGDATA,
        "flagdata",
        json!({"vis": vis, "mode": "summary"}),
        workspace.path(),
    );
    let float = |value: usize| CasaValue::float(value as f64);
    let row = NUM_CORR * NUM_CHAN;
    assert_eq!(counts(&summary, "spw", "0"), (float(5), float(4 * row)));
    assert_eq!(counts(&summary, "scan", "1"), (float(5), float(4 * row)));
    for axis in ["observation", "array"] {
        assert_eq!(counts(&summary, axis, "0"), (float(1), float(2 * row)));
        assert_eq!(counts(&summary, axis, "1"), (float(4), float(2 * row)));
    }
    let CasaValue::Dict { entries } = entry(&summary, "correlation") else {
        panic!("{summary:?}")
    };
    let names = entries
        .iter()
        .map(|entry| entry.key.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["LL", "LR", "RL", "RR", "XX", "XY", "YX", "YY"].map(|name| CasaKey::String(name.into()))
    );
    let per_correlation = 2 * NUM_CHAN;
    assert_eq!(
        counts(&summary, "correlation", "RR"),
        (float(1), float(per_correlation))
    );
    assert_eq!(
        counts(&summary, "correlation", "YY"),
        (float(4), float(per_correlation))
    );
    assert_eq!(
        counts(&summary, "correlation", "XX"),
        (float(0), float(per_correlation))
    );
}

#[test]
fn flagdata_summary_names_every_casacore_correlation() {
    // CASA 6.7.6.14 aborts on RX/RY (Astra's #997 review; see
    // docs/CASA (C++) bugs.md); casa-rs keys them by casacore's names.
    let workspace = tempfile::tempdir().unwrap();
    let vis = common::create_two_polarization_fixture_ms(workspace.path(), [13, 14, 15, 16], &[]);
    let summary = returned(
        FLAGDATA,
        "flagdata",
        json!({"vis": vis, "mode": "summary"}),
        workspace.path(),
    );
    for name in ["RX", "RY", "LX", "LY"] {
        assert_eq!(
            counts(&summary, "correlation", name),
            (
                CasaValue::float(0.0),
                CasaValue::float((2 * NUM_CHAN) as f64)
            )
        );
    }
}

#[test]
fn flagdata_returns_an_empty_dictionary_in_its_other_modes() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let flags = || {
        returned(
            FLAGDATA,
            "flagdata",
            json!({"vis": vis, "mode": "summary"}),
            workspace.path(),
        )
    };
    let flagged = |summary: &CasaValue| entry(summary, "flagged").clone();
    let total = (4 * NUM_CORR * NUM_CHAN) as f64;
    for (case, values, after) in [
        // CASA's action='' runs nothing, saving no flag version.
        (
            "flagdata action none",
            json!({"mode": "manual", "action": ""}),
            0.0,
        ),
        (
            "flagdata manual",
            json!({"mode": "manual", "antenna": "0"}),
            total,
        ),
        ("flagdata unflag", json!({"mode": "unflag"}), 0.0),
    ] {
        let mut values = values;
        values["vis"] = json!(vis);
        let value = returned(FLAGDATA, "flagdata", values, workspace.path());
        assert_eq!(describe(&value), casa(case), "{case}");
        assert_eq!(flagged(&flags()), CasaValue::float(after), "{case}");
    }
    let versions = vis.with_file_name("msexplore_fixture.ms.flagversions");
    let saved = std::fs::read_dir(&versions)
        .unwrap()
        .filter(|entry| {
            let name = entry.as_ref().unwrap().file_name();
            name.to_string_lossy().starts_with("flags.flagdata_")
        })
        .count();
    assert_eq!(saved, 2, "one backup each for manual and unflag");
}

#[test]
fn flagdata_list_and_rflag_return_casa_shapes() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let empty = returned(
        FLAGDATA,
        "flagdata",
        json!({
            "vis": vis,
            "mode": "list",
            "action": "calculate",
            "inpfile": ["mode='manual' antenna='0'"],
            "flagbackup": false,
        }),
        workspace.path(),
    );
    assert_eq!(describe(&empty), casa("flagdata manual"));

    let summary = returned(
        FLAGDATA,
        "flagdata",
        json!({
            "vis": vis,
            "mode": "list",
            "action": "calculate",
            "inpfile": ["mode='manual' antenna='0'", "mode='summary'"],
            "flagbackup": false,
        }),
        workspace.path(),
    );
    let direct_summary = returned(
        FLAGDATA,
        "flagdata",
        json!({"vis": vis, "mode": "summary"}),
        workspace.path(),
    );
    assert_eq!(describe(&summary), describe(&direct_summary));
    assert_eq!(
        entry(&summary, "flagged"),
        &CasaValue::float((4 * NUM_CORR * NUM_CHAN) as f64)
    );

    let rflag = returned(
        FLAGDATA,
        "flagdata",
        json!({
            "vis": vis,
            "mode": "rflag",
            "action": "calculate",
            "channelavg": true,
            "chanbin": 2,
            "flagbackup": false,
        }),
        workspace.path(),
    );
    assert_eq!(entry(&rflag, "type"), &CasaValue::string("list"));
    assert_eq!(entry(&rflag, "nreport"), &CasaValue::Int { value: 1 });
}

#[test]
fn a_sub_parameter_casa_ignores_takes_any_value() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let total = (4 * NUM_CORR * NUM_CHAN) as f64;
    // Manual flagging with clip's, tfcrop's and summary's controls set.
    let values = json!({"vis": vis, "mode": "manual", "antenna": "0", "clipminmax": [0.0, 1.0],
                        "timefit": "poly", "spwchan": true, "cmdreason": "ignored"});
    returned(FLAGDATA, "flagdata", values, workspace.path());
    let summary = returned(
        FLAGDATA,
        "flagdata",
        json!({"vis": vis, "mode": "summary"}),
        workspace.path(),
    );
    assert_eq!(entry(&summary, "flagged"), &CasaValue::float(total));
}
