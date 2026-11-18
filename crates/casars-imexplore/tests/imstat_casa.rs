// SPDX-License-Identifier: LGPL-3.0-or-later

//! casa-rs `imstat` against CASA 6.7.6.14's recorded outputs (#977).
//!
//! `tools/casa-compat/imstat_oracle.py` records CASA's complete return
//! record on the two CASA-written images committed beside this test.
//! This runs the same calls through the native task surface and
//! compares every key: strings and positions exactly, statistics
//! within the 1e-3 normalized agreement the acceptance allows. The
//! fixture also records the robust-algorithm cases; those cases land
//! with their parameters.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use casa_provider_contracts::TaskReply;
use serde_json::{Value, json};

/// The path of a fixture under `tests/data/imstat`.
fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/imstat")
        .join(name)
}

/// CASA's recorded statistics, as JSON.
fn expected() -> Value {
    serde_json::from_str(
        &std::fs::read_to_string(fixture("expected.json")).expect("read expected.json"),
    )
    .expect("parse expected.json")
}

/// `imexplore imstat --json-run -` with `values`, on the fixture image.
fn run_imstat(image: &Path, extra: Value) -> Value {
    let mut values = json!({"imagename": image});
    if let (Value::Object(ours), Value::Object(more)) = (&mut values, &extra) {
        for (key, value) in more {
            ours.insert(key.clone(), value.clone());
        }
    }
    let request = json!({"surface": "imstat", "values": values}).to_string();
    let mut child = Command::new(env!("CARGO_BIN_EXE_imexplore"))
        .args(["imstat", "--json-run", "-"])
        .current_dir(image.parent().expect("a fixture directory"))
        .env_remove("CASA_RS_STATE_DIR")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("start imexplore");
    child
        .stdin
        .take()
        .expect("imexplore reads a request")
        .write_all(request.as_bytes())
        .expect("write the request");
    let output = child.wait_with_output().expect("run imstat");
    let reply: Option<TaskReply> = serde_json::from_slice(&output.stdout).ok();
    match reply {
        Some(TaskReply::Succeeded { value, .. }) => value.to_plain_json(),
        other => panic!(
            "imstat: {other:?}\n{}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

/// Every recorded key agrees with casa-rs: strings and positions
/// exactly, statistics within the normalized agreement.
fn assert_record_matches(ours: &Value, casa: &Value, case: &str) {
    let ours = ours
        .as_object()
        .unwrap_or_else(|| panic!("{case}: imstat returned {ours}"));
    let casa = casa
        .as_object()
        .unwrap_or_else(|| panic!("{case}: expected.json holds {casa}"));
    assert_eq!(
        ours.len(),
        casa.len(),
        "{case}: casa-rs returns {} keys, CASA {}",
        ours.len(),
        casa.len()
    );
    for (key, reference) in casa {
        let ours_value = ours
            .get(key)
            .unwrap_or_else(|| panic!("{case}: casa-rs returns no {key}"));
        match reference {
            Value::String(expected) => assert_eq!(
                ours_value, expected,
                "{case}: {key} is casa-rs {ours_value}, CASA {expected}"
            ),
            Value::Array(items) if !items.is_empty() && items[0].is_string() => assert_eq!(
                ours_value, reference,
                "{case}: {key} is casa-rs {ours_value}, CASA {reference}"
            ),
            Value::Array(items) if !items.is_empty() && items[0].is_number() => {
                let numbers = ours_value
                    .as_array()
                    .unwrap_or_else(|| panic!("{case}: {key} is {ours_value}"));
                assert_eq!(numbers.len(), items.len(), "{case}: {key} shape");
                for (mine, theirs) in numbers.iter().zip(items) {
                    let (mine, theirs) = (mine.as_f64().unwrap(), theirs.as_f64().unwrap());
                    let allowed = 1.0e-3 * theirs.abs().max(1.0);
                    assert!(
                        (mine - theirs).abs() <= allowed,
                        "{case}: {key} is casa-rs {mine}, CASA {theirs}"
                    );
                }
            }
            other => panic!("{case}: {key} records {other}"),
        }
    }
}

#[test]
fn imstat_classic_statistics_match_casa_on_the_casa_written_image() {
    let expected = expected();
    let values = fixture("values.im");
    for (case, call) in [
        ("classic", json!({})),
        ("classic chans", json!({"chans": "1~2"})),
        ("classic box", json!({"box": "2,2,5,5"})),
    ] {
        let ours = run_imstat(&values, call);
        assert_record_matches(&ours, &expected["values"][case], case);
    }
}

#[test]
fn imstat_classic_statistics_match_casa_without_a_beam_and_with_a_box() {
    let expected = expected();
    let unbeamed = fixture("unbeamed.im");
    let ours = run_imstat(&unbeamed, json!({}));
    assert_record_matches(&ours, &expected["unbeamed"]["classic"], "unbeamed classic");
    assert!(
        ours.get("flux").is_none(),
        "an image without a beam returns no flux"
    );
    let ours = run_imstat(&unbeamed, json!({"box": "2,2,5,5"}));
    assert_record_matches(
        &ours,
        &expected["unbeamed"]["classic box"],
        "unbeamed classic box",
    );
}
