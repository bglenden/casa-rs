// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;
use std::fs;

use super::*;

#[test]
fn the_application_catalog_lists_every_launch_entry() {
    let applications = applications().expect("application catalog");
    assert_eq!(applications.len(), 26);
    assert!(
        applications
            .iter()
            .all(|application| application.kind == "task" && application.id != "casars")
    );
    assert!(applications.iter().any(|application| {
        application.id == "tclean"
            && application.display_name == "Tclean"
            && application.executable == "casars-imager"
            && application.override_env == "CASARS_IMAGER_BIN"
    }));
}

#[test]
fn a_task_runs_its_override_or_names_what_is_missing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let imager = dir.path().join("imager");
    fs::write(&imager, "").expect("fake imager");
    let environment = HashMap::from([(
        "CASARS_IMAGER_BIN".to_string(),
        imager.display().to_string(),
    )]);
    assert_eq!(
        executable("tclean", &environment, dir.path()).unwrap(),
        imager
    );
    let error = executable("tablebrowser", &environment, dir.path()).unwrap_err();
    assert!(error.contains("no task \"tablebrowser\""), "{error}");

    // An empty suite root is searched alone, and the error says so.
    let environment = HashMap::from([(
        "CASARS_SUITE_ROOT".to_string(),
        dir.path().display().to_string(),
    )]);
    let error = executable("tclean", &environment, dir.path()).unwrap_err();
    assert!(error.contains("CASARS_SUITE_ROOT"), "{error}");
}

/// The outcome of an `imstat` process that exited with `returncode` after
/// replying `stdout`.
fn imstat_outcome(returncode: i32, stdout: &str) -> OutcomeView {
    let dir = tempfile::tempdir().expect("tempdir");
    outcome("imstat", returncode, stdout, dir.path(), Vec::new()).expect("outcome")
}

#[test]
fn an_outcome_keeps_each_way_a_task_can_end() {
    let succeeded = imstat_outcome(
        0,
        r#"{"status":"succeeded","surface":"imstat","value":{"kind":"dict","entries":[{"key":"npts","value":{"kind":"float_array","dtype":"float64","shape":[1],"data":[12.0]}}]},"result":{}}"#,
    );
    let OutcomeView::Succeeded { value, completion } = succeeded else {
        panic!("{succeeded:?}");
    };
    assert_eq!(
        value,
        CasaValue::dict([("npts", CasaValue::float64_vector([12.0]))])
    );
    assert_eq!(completion.surface_id, "imstat");

    assert_eq!(
        imstat_outcome(
            2,
            r#"{"status":"failed","surface":"imstat","kind":"validation","message":"no image","parameter":"imagename"}"#,
        ),
        OutcomeView::Refused {
            message: "no image".to_string(),
            parameter: Some("imagename".to_string()),
        }
    );
    assert_eq!(
        imstat_outcome(
            1,
            r#"{"status":"failed","surface":"imstat","kind":"execution","message":"cannot read"}"#,
        ),
        OutcomeView::Failed {
            message: "cannot read".to_string(),
            parameter: None,
        }
    );
    // Python's return code for a signal is the negated signal number.
    assert_eq!(imstat_outcome(-9, ""), OutcomeView::Signalled { signal: 9 });
    let OutcomeView::MalformedReply { exit_code, reason } = imstat_outcome(0, "npts: 12") else {
        panic!("a text reply is malformed");
    };
    assert_eq!(exit_code, Some(0));
    assert!(reason.contains("not valid"), "{reason}");
}

#[test]
fn a_reply_for_another_surface_is_malformed() {
    let reply = imstat_outcome(
        0,
        r#"{"status":"succeeded","surface":"imhead","value":{"kind":"none"},"result":{}}"#,
    );
    let OutcomeView::MalformedReply { reason, .. } = reply else {
        panic!("{reply:?}");
    };
    assert!(reason.contains("\"imhead\", not \"imstat\""), "{reason}");
}

#[test]
fn a_completion_reports_the_declared_product() {
    let dir = tempfile::tempdir().expect("tempdir");
    let table_path = dir.path().join("gain.cal");
    fs::create_dir(&table_path).expect("product directory");
    let completion = completion(
        "gaincal",
        r#"{"kind":"solve_gain","report":{}}"#,
        dir.path(),
        vec![("output".to_string(), "gain.cal".to_string())],
    )
    .expect("typed completion");

    assert_eq!(completion.products.len(), 1);
    let product = &completion.products[0];
    assert_eq!(product.path, table_path.display().to_string());
    assert_eq!(product.resource_kind, "CASA_TABLE");
    assert_eq!(product.role, "PRIMARY");
    assert!(product.exists);
    assert!(product.diagnostic.is_none());
}

#[test]
fn a_malformed_result_names_the_surface() {
    let dir = tempfile::tempdir().expect("tempdir");
    let error = completion("tclean", "runner stdout", dir.path(), Vec::new())
        .expect_err("malformed result");
    assert!(
        error.starts_with("unknown managed result for \"tclean\""),
        "{error}"
    );
}
