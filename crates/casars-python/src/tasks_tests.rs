// SPDX-License-Identifier: LGPL-3.0-or-later

use std::fs;

use super::*;

#[test]
fn the_application_catalog_lists_every_launch_entry() {
    let applications = applications().expect("application catalog");
    assert_eq!(applications.len(), 28);
    assert!(
        applications
            .iter()
            .all(|application| application.id != "tablebrowser")
    );
    assert!(applications.iter().any(|application| {
        application.id == "imager"
            && application.kind == "task"
            && application.display_name == "Imager"
            && application.executable == "casars-imager"
    }));
    assert!(applications.iter().any(|application| {
        application.id == "casars" && application.kind == "launcher" && application.include_in_suite
    }));
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
    let error = completion("imager", "runner stdout", dir.path(), Vec::new())
        .expect_err("malformed result");
    assert!(
        error.starts_with("unknown managed result for \"imager\""),
        "{error}"
    );
}
