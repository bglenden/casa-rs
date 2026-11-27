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

use casa_contracts::TaskReply;
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
/// exactly, statistics within the normalized agreement, at any nesting.
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
            other => assert_numbers_match(ours_value, other, case, key),
        }
    }
}

/// Statistics agree within the normalized agreement, at any nesting.
fn assert_numbers_match(ours: &Value, casa: &Value, case: &str, key: &str) {
    match (ours, casa) {
        (Value::Array(ours), Value::Array(casa)) => {
            assert_eq!(ours.len(), casa.len(), "{case}: {key} shape");
            for (mine, theirs) in ours.iter().zip(casa) {
                assert_numbers_match(mine, theirs, case, key);
            }
        }
        (Value::Number(mine), Value::Number(theirs)) => {
            let (mine, theirs) = (mine.as_f64().unwrap(), theirs.as_f64().unwrap());
            let allowed = 1.0e-3 * theirs.abs().max(1.0);
            assert!(
                (mine - theirs).abs() <= allowed,
                "{case}: {key} is casa-rs {mine}, CASA {theirs}"
            );
        }
        other => panic!("{case}: {key} records {other:?}",),
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

/// Every `axes` form the oracle recorded: the per-plane records, with
/// CASA's degenerate display axes dropped, and the invalid-axes
/// refusals.
#[test]
fn imstat_axes_match_casas_per_plane_records() {
    let expected = expected();
    let values = fixture("values.im");
    for (case, call) in [
        ("axes 0 1", json!({"axes": [0, 1]})),
        ("axes 1 0", json!({"axes": [1, 0]})),
        ("axes scalar 0", json!({"axes": 0})),
        ("axes scalar 3", json!({"axes": 3})),
        ("axes scalar -1", json!({"axes": -1})),
        ("axes all four", json!({"axes": [0, 1, 2, 3]})),
        ("axes 3", json!({"axes": [3]})),
        ("axes 2", json!({"axes": [2]})),
        ("axes empty list", json!({"axes": []})),
        ("axes minus one list", json!({"axes": [-1]})),
        (
            "axes 0 1 with box",
            json!({"axes": [0, 1], "box": "2,2,5,5"}),
        ),
        (
            "axes 0 1 with chans",
            json!({"axes": [0, 1], "chans": "1~2"}),
        ),
        (
            "biweight with axes",
            json!({"algorithm": "biweight", "axes": [0, 1]}),
        ),
        (
            "fit-half with axes",
            json!({"algorithm": "fit-half", "axes": [0, 1]}),
        ),
        (
            "chauvenet with axes",
            json!({"algorithm": "chauvenet", "axes": [0, 1]}),
        ),
        (
            "hinges-fences with axes",
            json!({"algorithm": "hinges-fences", "fence": 1.0, "axes": [0, 1]}),
        ),
    ] {
        let ours = run_imstat(&values, call);
        assert_record_matches(&ours, &expected["values"][case], case);
    }
    for (case, call) in [
        ("axes minus one twice", json!({"axes": [-1, -1]})),
        ("axes negative -4 -3", json!({"axes": [-4, -3]})),
        ("axes out of range 4", json!({"axes": [4]})),
        ("axes out of range -5", json!({"axes": -5})),
    ] {
        let message = run_imstat_failure(&values, call);
        assert_eq!(
            message.trim_end().trim_end_matches('.'),
            expected["values"][case]["error"]
                .as_str()
                .expect("the recorded refusal")
                .trim_end()
                .trim_end_matches('.'),
            "{case}"
        );
    }
}

/// Every `stokes` form the oracle recorded on the four-Stokes image,
/// with CASA's refusal for the forms that are not plane names. A name
/// the image does not carry is a recorded CASA deviation: casa-rs
/// refuses it clearly where CASA reports an internal bounds error.
#[test]
fn imstat_stokes_match_casas_selections() {
    let expected = expected();
    let stokes4 = fixture("stokes4.im");
    for (case, call) in [
        ("classic", json!({})),
        ("stokes I", json!({"stokes": "I"})),
        ("stokes IQUV", json!({"stokes": "IQUV"})),
        ("stokes QU", json!({"stokes": "QU"})),
        ("stokes reverse IU", json!({"stokes": "IU"})),
        ("stokes comma Q,U", json!({"stokes": "Q,U"})),
        ("stokes empty", json!({"stokes": ""})),
        ("stokes with axes", json!({"stokes": "QU", "axes": [0, 1]})),
        ("stokes with chans", json!({"stokes": "QU", "chans": "1"})),
    ] {
        let ours = run_imstat(&stokes4, call);
        assert_record_matches(&ours, &expected["stokes4"][case], case);
    }
    for (case, call) in [
        ("stokes numeric", json!({"stokes": "0"})),
        ("stokes numeric range", json!({"stokes": "0~2"})),
        ("stokes unknown", json!({"stokes": "Z"})),
        ("stokes out of range", json!({"stokes": "5"})),
    ] {
        let message = run_imstat_failure(&stokes4, call);
        assert_eq!(
            message.trim_end().trim_end_matches('.'),
            expected["stokes4"][case]["error"]
                .as_str()
                .expect("the recorded refusal")
                .trim_end()
                .trim_end_matches('.'),
            "{case}"
        );
    }
    // CASA reports an internal LCBox bounds error for a plane the image
    // does not carry; casa-rs refuses the name clearly instead.
    let message = run_imstat_failure(&fixture("values.im"), json!({"stokes": "Q"}));
    assert_eq!(message, "Stokes Q is not one of this image's Stokes planes");
}

/// Every `mask` form the oracle recorded, with `stretch` extending
/// conforming shapes as CASA does; the record is the same whatever
/// `listit` and `verbose` hold, as CASA returns it.
#[test]
fn imstat_mask_and_logging_match_casa() {
    let expected = expected();
    let values = fixture("values.im");
    let mask = format!("\"{}\" > 100", values.display());
    for (case, call) in [
        ("mask threshold", json!({"mask": mask})),
        (
            "mask threshold with stretch",
            json!({"mask": mask, "stretch": true}),
        ),
        (
            "mask boolean and",
            json!({"mask": format!(
                "\"{}\" > 3 && \"{}\" < 10",
                values.display(),
                values.display()
            )}),
        ),
        (
            "mask boolean or",
            json!({"mask": format!(
                "\"{}\" > 3 || \"{}\" > 400",
                values.display(),
                values.display()
            )}),
        ),
        (
            "mask boolean not",
            json!({"mask": format!("!(\"{}\" > 3)", values.display())}),
        ),
        (
            "mask isnan nan image",
            json!({"mask": format!("isnan(\"{}\")", fixture("nan.im").display())}),
        ),
        (
            "mask isnan no nan",
            json!({"mask": format!("isnan(\"{}\")", values.display())}),
        ),
        (
            "mask iif",
            json!({"mask": format!(
                "iif(\"{}\" > 100, \"{}\", 0) > 50",
                values.display(),
                values.display()
            )}),
        ),
        (
            "mask sqrt threshold",
            json!({"mask": format!("\"{}\" > sqrt(100)", values.display())}),
        ),
        (
            "mask two images",
            json!({"mask": format!(
                "\"{}\" > \"{}\"",
                values.display(),
                fixture("other.im").display()
            )}),
        ),
        (
            "mask single quotes",
            json!({"mask": format!("'{}' > 100", values.display())}),
        ),
        (
            "mask pi comparison",
            json!({"mask": format!("\"{}\" > 2 * pi()", values.display())}),
        ),
        (
            "mask 2d stretch",
            json!({
                "mask": format!("\"{}\" > 0.5", fixture("mask2d.im").display()),
                "stretch": true,
            }),
        ),
        (
            "mask 3d stretch",
            json!({
                "mask": format!("\"{}\" > 0.5", fixture("mask3d.im").display()),
                "stretch": true,
            }),
        ),
        ("listit false", json!({"listit": false})),
        ("verbose false", json!({"verbose": false})),
        (
            "listit and verbose false",
            json!({"listit": false, "verbose": false}),
        ),
    ] {
        let ours = run_imstat(&values, call);
        assert_record_matches(&ours, &expected["values"][case], case);
    }
    let ours = run_imstat(
        &values,
        json!({"mask": format!("\"{}\" < -1000", values.display())}),
    );
    assert_record_matches(
        &ours,
        &expected["values"]["mask empty selection"],
        "mask empty",
    );
    // The refusals casa-rs words as CASA does.
    for (case, call) in [
        (
            "mask 2d no stretch",
            json!({
                "mask": format!("\"{}\" > 0.5", fixture("mask2d.im").display()),
                "stretch": false,
            }),
        ),
        (
            "mask unextendable",
            json!({
                "mask": format!("\"{}\" > 0.5", fixture("small.im").display()),
                "stretch": true,
            }),
        ),
        (
            "mask bad name",
            json!({"mask": "\"does-not-exist.im\" > 3"}),
        ),
    ] {
        let message = run_imstat_failure(&values, call);
        assert_eq!(
            message.trim_end().trim_end_matches('.'),
            expected["values"][case]["error"]
                .as_str()
                .expect("the recorded refusal")
                .trim_end()
                .trim_end_matches('.'),
            "{case}"
        );
    }
    // A malformed expression is refused with the engine's own wording;
    // CASA's ImageExprParse message stays in the fixture as reference.
    for call in [
        json!({"mask": "not an expression"}),
        json!({"mask": format!("\"{}\" > ", values.display())}),
    ] {
        let message = run_imstat_failure(&values, call);
        assert!(
            message.contains("Input mask specification is incorrect"),
            "{message}"
        );
    }
}

/// CASA's logfile: the statistics block is written whatever `listit`
/// holds, appended or overwritten as `append` selects; the presentation
/// is each surface's own, the information the same.
#[test]
fn imstat_logfile_records_the_block_and_appends() {
    let workspace = tempfile::tempdir().expect("scratch");
    let values = fixture("values.im");
    let log = workspace.path().join("stats.log");

    run_imstat(&values, json!({"logfile": log.clone()}));
    let first = std::fs::read_to_string(&log).expect("the logfile is written");
    assert!(first.contains("npts: 256"), "{first}");
    assert!(first.contains("min: -400"), "{first}");

    run_imstat(&values, json!({"logfile": log.clone(), "append": true}));
    let appended = std::fs::read_to_string(&log).expect("the logfile is appended");
    assert_eq!(appended.matches("npts: 256").count(), 2, "{appended}");

    run_imstat(&values, json!({"logfile": log.clone(), "append": false}));
    let overwritten = std::fs::read_to_string(&log).expect("the logfile is overwritten");
    assert_eq!(overwritten.matches("npts: 256").count(), 1, "{overwritten}");

    // CASA writes the logfile even with the logger block suppressed.
    let log2 = workspace.path().join("stats2.log");
    run_imstat(&values, json!({"logfile": log2.clone(), "listit": false}));
    let text = std::fs::read_to_string(&log2).expect("listit does not gate the logfile");
    assert!(text.contains("npts: 256"), "{text}");
}

#[test]
fn imstat_plane_log_has_velocity_and_iterations_and_omits_masked_planes() {
    let workspace = tempfile::tempdir().unwrap();
    let image = fixture("fourdim.im");
    let logfile = workspace.path().join("planes.log");
    run_imstat(
        &image,
        json!({"axes": [0, 1], "logfile": logfile, "append": false}),
    );
    let text = std::fs::read_to_string(&logfile).unwrap();
    assert!(text.contains("#VELOCITY column unit = km/s"), "{text}");
    assert!(
        text.contains("1141.36626") && text.contains("1141.1552"),
        "{text}"
    );

    let mask = format!("\"{}\" > 115", image.display());
    run_imstat(
        &image,
        json!({"axes": [0, 1], "mask": mask, "logfile": logfile, "append": false}),
    );
    let text = std::fs::read_to_string(&logfile).unwrap();
    let rows = text
        .lines()
        .skip_while(|line| !line.contains("VELOCITY(Plane)"))
        .skip(1);
    assert_eq!(rows.count(), 1, "{text}");
    assert!(text.contains("4.000000e0"), "{text}");
    assert!(!text.contains("NaN"), "{text}");

    run_imstat(
        &fixture("values.im"),
        json!({"axes": [0, 1], "algorithm": "chauvenet", "zscore": 1.5, "logfile": logfile, "append": false}),
    );
    let text = std::fs::read_to_string(&logfile).unwrap();
    assert!(text.contains("N Iter"), "{text}");
    assert!(
        text.lines().any(|line| line
            .split_whitespace()
            .last()
            .and_then(|n| n.parse::<usize>().ok())
            .is_some_and(|n| n > 0)),
        "{text}"
    );
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

/// CASA's own imstat test suite — the scenarios upstream
/// `test_task_imstat.py` holds that the parameter cases above did not
/// already cover — replayed from the recorded fixture: several boxes in
/// one `box` spec and a `box` with spaces, the channel comparisons
/// CASA's `chans` accepts, the upstream axes quartet on a four-axis
/// ramp, a moment map's flux, the `;` channel separator, and imstat on
/// a linear-coordinate FITS image.
#[test]
fn imstat_upstream_suite_matches_casa() {
    let expected = expected();
    let upstream = &expected["upstream"];
    let multi = fixture("multi.im");
    let fourdim = fixture("fourdim.im");
    let moment = fixture("moment.im");
    let excl = fixture("excl.im");
    let linear = fixture("linear_coords.fits");
    for (case, image, call) in [
        ("box double", &multi, json!({"box": "0,0,2,2,4,4,6,6"})),
        ("box overlap", &multi, json!({"box": "0,0,2,2,0,0,2,2"})),
        (
            "box triple",
            &multi,
            json!({"box": "0,0,1,1,4,4,5,5,8,8,9,9"}),
        ),
        ("box spaces", &multi, json!({"box": "0, 0,  1 ,   1"})),
        (
            "box double with chans",
            &multi,
            json!({"box": "0,0,2,2,4,4,6,6", "chans": "0~4, 6, >8"}),
        ),
        ("chans gt", &multi, json!({"chans": ">8"})),
        ("chans lt", &multi, json!({"chans": "<2"})),
        ("chans ge", &multi, json!({"chans": ">=8"})),
        ("chans le", &multi, json!({"chans": "<=2"})),
        ("chans both filters", &multi, json!({"chans": ">8,<2"})),
        ("fourdim axes minus one", &fourdim, json!({"axes": -1})),
        ("fourdim axes 0 1 2", &fourdim, json!({"axes": [0, 1, 2]})),
        ("fourdim axes 0 1", &fourdim, json!({"axes": [0, 1]})),
        ("fourdim axes 3", &fourdim, json!({"axes": 3})),
        ("moment map flux", &moment, json!({})),
        (
            "exclusion chans semicolon",
            &excl,
            json!({"axes": [0, 1], "chans": "10~20;60~90", "stokes": "IV"}),
        ),
        ("linear coordinates", &linear, json!({})),
    ] {
        let ours = run_imstat(image, call);
        assert_record_matches(&ours, &upstream[case], case);
    }
    // The refusals casa-rs words as CASA does.
    for (case, call) in [
        ("box seven values", json!({"box": "0,0,2,2,4,4,6"})),
        ("box inverted", json!({"box": "5,5,2,2"})),
        ("box out of range", json!({"box": "0,0,20,20"})),
        ("chans bad comparison", json!({"chans": ">abc"})),
        ("chans float comparison", json!({"chans": ">2.5"})),
    ] {
        let message = run_imstat_failure(&multi, call);
        assert_eq!(
            message.trim_end().trim_end_matches('.'),
            upstream[case]["error"]
                .as_str()
                .expect("the recorded refusal")
                .trim_end()
                .trim_end_matches('.'),
            "{case}"
        );
    }
}

/// CASA's subimage of a several-box `box` spec is the bounding box with
/// the union masked in: the cut keeps the outside pixels' values and
/// drops their validity, as `test_multiple_region_support`'s region
/// holds.
#[test]
fn imsubimage_multi_box_cuts_the_bounding_box_with_a_union_mask() {
    let scratch = tempfile::tempdir().expect("scratch");
    let outfile = scratch.path().join("cut.image");
    casa_imageanalysis::imsubimage(&casa_imageanalysis::ImsubimageRequest {
        imagename: fixture("multi.im"),
        outfile: outfile.clone(),
        box_pixels: Some("0,0,2,2,4,4,6,6".into()),
        ..casa_imageanalysis::ImsubimageRequest::default()
    })
    .expect("cut the multi-box region");
    let cut = casa_images::PagedImage::<f32>::open(&outfile).expect("open the cut");
    assert_eq!(cut.shape(), [7, 7, 10]);
    // The gap pixel between the two boxes keeps its value and loses its
    // validity.
    assert_eq!(f64::from(cut.get_at(&[3, 3, 0]).expect("a pixel")), 33.0);
    let mask = cut
        .get_mask_slice(&[0, 0, 0], &[7, 7, 10], &[1, 1, 1])
        .expect("the mask")
        .expect("the region mask");
    assert_eq!(
        mask.iter().filter(|valid| **valid).count(),
        18 * 10,
        "the union holds 18 pixels per channel"
    );
}
