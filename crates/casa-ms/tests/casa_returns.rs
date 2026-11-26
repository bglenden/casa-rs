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

use casa_contracts::{CasaKey, CasaValue, TaskReply};
use common::create_msexplore_fixture_ms_with_flags;
use serde_json::{Value, json};

const FLAGMANAGER: &str = env!("CARGO_BIN_EXE_flagmanager");

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
fn flagmanager_returns_casas_version_list() {
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let run = |binary, surface, values: Value| {
        let mut values = values;
        values["vis"] = json!(vis);
        returned(binary, surface, values, workspace.path())
    };
    // The recording's sequence: two flagdata backups, then a saved version.
    for action in [
        casa_ms::FlagDataAction::Flag,
        casa_ms::FlagDataAction::Unflag,
    ] {
        casa_ms::flagdata_path(
            &vis,
            &casa_ms::FlagDataRequest {
                action,
                flagbackup: true,
                ..Default::default()
            },
        )
        .unwrap();
    }
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
fn saving_a_version_name_again_keeps_the_old_version() {
    // Astra's #997 F1: CASA's task renames the version a save would replace
    // to `<name>.old.<seconds>`, so both generations stay restorable.
    let workspace = tempfile::tempdir().unwrap();
    let vis = create_msexplore_fixture_ms_with_flags(workspace.path(), &[]);
    let run = |binary, surface, values: Value| {
        let mut values = values;
        values["vis"] = json!(vis);
        returned(binary, surface, values, workspace.path())
    };
    let flagged = || {
        let summary = casa_ms::flagdata_path(
            &vis,
            &casa_ms::FlagDataRequest {
                mode: casa_ms::FlagDataMode::Summary,
                ..Default::default()
            },
        )
        .unwrap();
        CasaValue::float(summary.flagged_samples as f64)
    };
    let save = |comment| {
        run(
            FLAGMANAGER,
            "flagmanager",
            json!({"mode": "save", "versionname": "v1", "comment": comment}),
        )
    };
    save("first");
    casa_ms::flagdata_path(
        &vis,
        &casa_ms::FlagDataRequest {
            flagbackup: false,
            ..Default::default()
        },
    )
    .unwrap();
    let after_manual = flagged();
    assert_ne!(after_manual, CasaValue::float(0.0));
    save("second");

    let listed = run(FLAGMANAGER, "flagmanager", json!({"mode": "list"}));
    let CasaValue::Dict { entries } = &listed else {
        unreachable!()
    };
    assert_eq!(entries.len(), 3, "two versions and the MS: {listed:?}");
    let CasaValue::String { value: kept } = entry(&entries[0].value, "name") else {
        panic!("{listed:?}")
    };
    let seconds = kept.strip_prefix("v1.old.").expect("the renamed version");
    assert!(seconds.parse::<u64>().is_ok(), "{kept}");
    // casa-rs keeps the old version's own comment; CASA overwrites it.
    assert_eq!(
        entry(&entries[0].value, "comment"),
        &CasaValue::string("first")
    );
    assert_eq!(entry(&entries[1].value, "name"), &CasaValue::string("v1"));
    assert_eq!(
        entry(&entries[1].value, "comment"),
        &CasaValue::string("second")
    );

    let restore = |name: &str| {
        run(
            FLAGMANAGER,
            "flagmanager",
            json!({"mode": "restore", "versionname": name}),
        )
    };
    restore(kept);
    assert_eq!(flagged(), CasaValue::float(0.0));
    restore("v1");
    assert_eq!(flagged(), after_manual);
}
