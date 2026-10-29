// SPDX-License-Identifier: LGPL-3.0-or-later

//! The MeasurementSet task binaries return what CASA 6.7.6's tasks return
//! (#931): the structure of each return is compared with CASA's own,
//! recorded in `resources/casa-compat/casa-6.7.6.14/returns.json` by
//! `tools/casa-compat/record_casa_returns.py`. The CASA values casa-rs does
//! not implement yet are refused before the task starts, naming #978.

mod common;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

use casa_provider_contracts::{CasaKey, CasaValue, TaskReply};
use common::{NUM_CHAN, NUM_CORR, create_msexplore_fixture_ms_with_flags};
use serde_json::{Value, json};

const FLAGDATA: &str = env!("CARGO_BIN_EXE_flagdata");
const FLAGMANAGER: &str = env!("CARGO_BIN_EXE_flagmanager");
const MSTRANSFORM: &str = env!("CARGO_BIN_EXE_mstransform");

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
fn flagmanager_returns_casas_version_list() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let run = |binary, surface, values: Value| {
        let mut values = values;
        values["vis"] = json!(vis);
        returned(binary, surface, values, workspace.path())
    };
    // The recording's sequence: two flagdata backups, then a saved version.
    run(
        FLAGDATA,
        "flagdata",
        json!({"mode": "manual", "antenna": "0"}),
    );
    run(FLAGDATA, "flagdata", json!({"mode": "unflag"}));
    let saved = run(
        FLAGMANAGER,
        "flagmanager",
        json!({"mode": "save", "versionname": "v1"}),
    );
    assert_eq!(describe(&saved), casa("flagmanager save"));

    let listed = run(FLAGMANAGER, "flagmanager", json!({"mode": "list"}));
    assert_eq!(describe(&listed), casa("flagmanager list"));
    let CasaValue::Dict { entries } = &listed else {
        unreachable!()
    };
    let names = entries[..3]
        .iter()
        .map(|version| entry(&version.value, "name").clone())
        .collect::<Vec<_>>();
    assert_eq!(
        names,
        ["flagdata_1", "flagdata_2", "v1"].map(CasaValue::string)
    );
    assert_eq!(
        entry(&listed, "MS"),
        &CasaValue::string(vis.display().to_string())
    );
}

#[test]
fn split_and_mstransform_return_none() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
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
        let value = returned(MSTRANSFORM, surface, values, workspace.path());
        assert_eq!(describe(&value), casa(surface), "{surface}");
        assert!(outputvis.exists(), "{surface}");
    }
}

#[test]
fn casa_values_not_implemented_yet_are_refused_naming_978() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let outputvis = workspace.path().join("out.ms");
    for (binary, surface, values, parameter) in [
        (FLAGDATA, "flagdata", json!({"mode": "shadow"}), "mode"),
        (
            FLAGDATA,
            "flagdata",
            json!({"action": "calculate"}),
            "action",
        ),
        (
            FLAGDATA,
            "flagdata",
            json!({"quackmode": "tail"}),
            "quackmode",
        ),
        // An unimplemented CASA parameter, refused at any value but its default.
        (
            FLAGDATA,
            "flagdata",
            json!({"clipminmax": [0.0, 1.0]}),
            "clipminmax",
        ),
        (
            MSTRANSFORM,
            "mstransform",
            json!({"outputvis": outputvis, "datacolumn": "model"}),
            "datacolumn",
        ),
        (
            MSTRANSFORM,
            "mstransform",
            json!({"outputvis": outputvis, "regridms": true}),
            "regridms",
        ),
    ] {
        let mut values = values;
        values["vis"] = json!(vis);
        let TaskReply::Failed {
            message,
            parameter: named,
            ..
        } = json_run(binary, surface, values, workspace.path())
        else {
            panic!("{surface} {parameter}: not refused");
        };
        assert_eq!(named.as_deref(), Some(parameter), "{message}");
        assert!(message.contains("#978"), "{message}");
    }
    assert!(!outputvis.exists());
}
