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
/// The reply, successful or failed.
fn imstat_reply(image: &Path, extra: Value) -> TaskReply {
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
    serde_json::from_slice(&output.stdout).expect("imexplore replies with one document")
}

/// `imexplore imstat --json-run -` with `values`, on the fixture image.
fn run_imstat(image: &Path, extra: Value) -> Value {
    match imstat_reply(image, extra) {
        TaskReply::Succeeded { value, .. } => value.to_plain_json(),
        other => panic!("imstat failed: {other:?}"),
    }
}

/// The failure message the task reports.
fn run_imstat_failure(image: &Path, extra: Value) -> String {
    match imstat_reply(image, extra) {
        TaskReply::Failed { message, .. } => message,
        other => panic!("imstat unexpectedly succeeded: {other:?}"),
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
                    let (mine, theirs) = (
                        mine.as_f64()
                            .unwrap_or_else(|| panic!("{case}: {key} holds {mine}")),
                        theirs
                            .as_f64()
                            .unwrap_or_else(|| panic!("{case}: {key} records {theirs}")),
                    );
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
        ("classic clmethod tiled", json!({"clmethod": "tiled"})),
        (
            "classic clmethod framework",
            json!({"clmethod": "framework"}),
        ),
        ("classic chans", json!({"chans": "1~2"})),
        ("classic box", json!({"box": "2,2,5,5"})),
    ] {
        let ours = run_imstat(&values, call);
        assert_record_matches(&ours, &expected["values"][case], case);
    }
}

/// Every robust-algorithm case the oracle recorded: the record's keys and
/// values, or CASA's refusal wording.
#[test]
fn imstat_robust_algorithms_match_casa_on_the_casa_written_image() {
    let expected = expected();
    let values = fixture("values.im");
    for (case, call) in [
        (
            "hinges-fences fence -1",
            json!({"algorithm": "hinges-fences", "fence": -1.0}),
        ),
        (
            "hinges-fences fence 0.5",
            json!({"algorithm": "hinges-fences", "fence": 0.5}),
        ),
        (
            "hinges-fences fence 1",
            json!({"algorithm": "hinges-fences", "fence": 1.0}),
        ),
        (
            "hinges-fences fence 2",
            json!({"algorithm": "hinges-fences", "fence": 2.0}),
        ),
        (
            "hinges-fences fence 0",
            json!({"algorithm": "hinges-fences", "fence": 0.0}),
        ),
        (
            "hinges-fences minimum h",
            json!({"algorithm": "h", "fence": 1.0}),
        ),
        (
            "hinges-fences upper case",
            json!({"algorithm": "Hinges-Fences", "fence": 1.0}),
        ),
        ("fit-half", json!({"algorithm": "fit-half"})),
        (
            "fit-half median",
            json!({"algorithm": "fit-half", "center": "median"}),
        ),
        (
            "fit-half zero",
            json!({"algorithm": "fit-half", "center": "zero"}),
        ),
        (
            "fit-half center med",
            json!({"algorithm": "fit-half", "center": "med"}),
        ),
        (
            "fit-half lside false",
            json!({"algorithm": "fit-half", "lside": false}),
        ),
        (
            "fit-half median lside false",
            json!({"algorithm": "fit-half", "center": "median", "lside": false}),
        ),
        (
            "fit-half zero lside false",
            json!({"algorithm": "fit-half", "center": "zero", "lside": false}),
        ),
        ("chauvenet", json!({"algorithm": "chauvenet"})),
        (
            "chauvenet zscore 1.5",
            json!({"algorithm": "chauvenet", "zscore": 1.5}),
        ),
        (
            "chauvenet zscore 2",
            json!({"algorithm": "chauvenet", "zscore": 2.0}),
        ),
        (
            "chauvenet zscore 3",
            json!({"algorithm": "chauvenet", "zscore": 3.0}),
        ),
        (
            "chauvenet maxiter 1",
            json!({"algorithm": "chauvenet", "maxiter": 1}),
        ),
        (
            "chauvenet maxiter 3",
            json!({"algorithm": "chauvenet", "maxiter": 3}),
        ),
        (
            "chauvenet maxiter 0",
            json!({"algorithm": "chauvenet", "maxiter": 0}),
        ),
        (
            "chauvenet maxiter -5",
            json!({"algorithm": "chauvenet", "maxiter": -5}),
        ),
        ("biweight", json!({"algorithm": "biweight"})),
        (
            "biweight niter 1",
            json!({"algorithm": "biweight", "niter": 1}),
        ),
        (
            "biweight niter 2",
            json!({"algorithm": "biweight", "niter": 2}),
        ),
        (
            "biweight niter -1",
            json!({"algorithm": "biweight", "niter": -1}),
        ),
        (
            "biweight niter 0",
            json!({"algorithm": "biweight", "niter": 0}),
        ),
        ("biweight upper case", json!({"algorithm": "BIWEIGHT"})),
        (
            "biweight box",
            json!({"algorithm": "biweight", "box": "2,2,5,5"}),
        ),
        (
            "classic single pixel",
            json!({"box": "0,0,0,0", "chans": "0"}),
        ),
        (
            "fit-half single pixel",
            json!({"algorithm": "fit-half", "center": "zero", "box": "0,0,0,0", "chans": "0"}),
        ),
        ("minimum-match cla", json!({"algorithm": "cla"})),
    ] {
        let ours = run_imstat(&values, call);
        assert_record_matches(&ours, &expected["values"][case], case);
    }
    for (case, call) in [
        (
            "biweight single pixel",
            json!({"algorithm": "biweight", "box": "0,0,0,0", "chans": "0"}),
        ),
        ("minimum-match c", json!({"algorithm": "c"})),
        ("minimum-match x", json!({"algorithm": "x"})),
        ("minimum-match empty", json!({"algorithm": ""})),
        ("center m", json!({"algorithm": "fit-half", "center": "m"})),
        (
            "center bad",
            json!({"algorithm": "fit-half", "center": "bad"}),
        ),
        ("clmethod bad", json!({"clmethod": "bad"})),
    ] {
        let message = run_imstat_failure(&values, call);
        assert_eq!(
            message.trim_end(),
            expected["values"][case]["error"]
                .as_str()
                .expect("the recorded refusal"),
            "{case}"
        );
    }
}

#[test]
fn imstat_robust_algorithms_match_casa_without_a_beam() {
    let expected = expected();
    let unbeamed = fixture("unbeamed.im");
    for (case, call) in [
        (
            "hinges-fences fence 1",
            json!({"algorithm": "hinges-fences", "fence": 1.0}),
        ),
        (
            "chauvenet zscore 1.5",
            json!({"algorithm": "chauvenet", "zscore": 1.5}),
        ),
        (
            "fit-half median",
            json!({"algorithm": "fit-half", "center": "median"}),
        ),
        ("biweight", json!({"algorithm": "biweight"})),
    ] {
        let ours = run_imstat(&unbeamed, call);
        assert_record_matches(&ours, &expected["unbeamed"][case], case);
        assert!(
            ours.get("flux").is_none(),
            "{case}: an image without a beam returns no flux"
        );
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
