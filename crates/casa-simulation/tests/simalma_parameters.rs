// SPDX-License-Identifier: LGPL-3.0-or-later
//! Observable exposure allocation, dry runs and project input protection.
use casa_simulation::{ExecutionControls, SessionResources, simalma::simalma};
use serde_json::json;
#[path = "common/simalma_fixture.rs"]
mod fixture;

#[test]
fn scalar_and_vector_times_are_allocated_and_dryrun_publishes_no_data() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, mut request, _) = fixture::fixture(resources);
    request.parameters.insert("dryrun".into(), json!(true));
    let scalar = simalma(&request).unwrap();
    assert!(scalar.warnings[0].contains("1.000000 minutes"));
    assert!(scalar.warnings[1].contains("2.000000 minutes"));
    assert!(scalar.warnings[2].contains("2 TP antennas"));
    assert!(scalar.products.is_empty());
    assert!(!scratch.path().join("combo").exists());
    request
        .parameters
        .insert("totaltime".into(), json!(["20min", "30min"]));
    let vector = simalma(&request).unwrap();
    assert!(vector.warnings[0].contains("20.000000 minutes"));
    assert!(vector.warnings[1].contains("30.000000 minutes"));
    assert!(!scratch.path().join("combo").exists());
}

#[test]
fn invalid_time_vector_and_project_owned_inputs_preserve_existing_project() {
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    let (scratch, mut request, _) = fixture::fixture(resources);
    let project = scratch.path().join("combo");
    std::fs::create_dir(&project).unwrap();
    let canary = project.join("caller-data");
    std::fs::write(&canary, b"preserve").unwrap();
    request
        .parameters
        .insert("totaltime".into(), json!(["1min", "2min", "3min"]));
    let error = simalma(&request).err().unwrap().to_string();
    assert!(error.contains("one per configuration"), "{error}");
    assert_eq!(std::fs::read(&canary).unwrap(), b"preserve");
    request.parameters.insert("totaltime".into(), json!("1min"));
    request.parameters.insert("skymodel".into(), json!(canary));
    let error = simalma(&request).err().unwrap().to_string();
    assert!(error.contains("skymodel would be deleted"), "{error}");
    assert_eq!(std::fs::read(&canary).unwrap(), b"preserve");
    request.parameters.insert(
        "skymodel".into(),
        json!(scratch.path().join("inputs/model.image")),
    );
    let config = project.join("alma.test.cfg");
    std::fs::copy(scratch.path().join("inputs/alma.test.cfg"), &config).unwrap();
    request
        .parameters
        .insert("antennalist".into(), json!([config]));
    let error = simalma(&request).err().unwrap().to_string();
    assert!(error.contains("configuration would be deleted"), "{error}");
    assert_eq!(std::fs::read(&canary).unwrap(), b"preserve");
}
