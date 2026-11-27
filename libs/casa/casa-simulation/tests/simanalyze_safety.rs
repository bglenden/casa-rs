// SPDX-License-Identifier: LGPL-3.0-or-later
//! A composed run must preserve every selected input and honor overwrite.
use casa_simulation::{
    ExecutionControls, SessionResources,
    simanalyze::{SimanalyzeRequest, simanalyze},
};
use flate2::read::GzDecoder;
use serde_json::{Map, Value, json};
use std::path::Path;

fn fixture(case: &str) -> (tempfile::TempDir, Map<String, Value>) {
    let scratch = tempfile::tempdir().unwrap();
    tar::Archive::new(GzDecoder::new(
        &include_bytes!("data/simanalyze_multi_inputs.tar.gz")[..],
    ))
    .unpack(scratch.path())
    .unwrap();
    let oracle: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/simanalyze_multi_casa.json.gz")[..],
    ))
    .unwrap();
    let mut parameters = oracle["cases"][case]["parameters"]
        .as_object()
        .unwrap()
        .clone();
    parameters.insert("project".into(), json!(scratch.path().join(case)));
    (scratch, parameters)
}

fn request(parameters: Map<String, Value>) -> SimanalyzeRequest {
    SimanalyzeRequest {
        workspace: None,
        parameters,
        resources: SessionResources::from_controls(&ExecutionControls::default()).unwrap(),
    }
}

fn canary(directory: &Path) {
    std::fs::create_dir_all(directory).unwrap();
    std::fs::write(directory.join("caller-data"), b"preserve this input").unwrap();
}

fn preserved(directory: &Path) {
    assert_eq!(
        std::fs::read(directory.join("caller-data")).unwrap(),
        b"preserve this input"
    );
}

#[test]
fn selected_concat_target_is_refused_before_publication() {
    let (scratch, mut p) = fixture("multi");
    let project = scratch.path().join("multi");
    let input = project.join("multi.concat.ms");
    std::fs::rename(project.join("multi.compact.ms"), &input).unwrap();
    canary(&input);
    p.insert(
        "vis".into(),
        json!("multi.concat.ms/../multi.concat.ms,multi.small.ms"),
    );
    let error = simanalyze(&request(p)).err().unwrap().to_string();
    assert!(error.contains("would be overwritten"), "{error}");
    preserved(&input);
    assert!(!project.join("multi.concat.image").exists());
}

#[test]
fn feather_input_is_protected_before_synthesis_overwrites_it() {
    let (scratch, mut p) = fixture("feather");
    let project = scratch.path().join("feather");
    let input = project.join("feather.small.image");
    canary(&input);
    p.insert("featherimage".into(), json!(input));
    let error = simanalyze(&request(p)).err().unwrap().to_string();
    assert!(error.contains("would be overwritten"), "{error}");
    preserved(&input);
    assert!(!project.join("feather.small.psf").exists());
}

#[test]
fn every_total_power_intermediate_honors_no_overwrite() {
    let (scratch, mut p) = fixture("tp");
    let project = scratch.path().join("tp");
    p.insert("overwrite".into(), json!(false));
    for name in [
        "tp.sd.image0",
        "tp.sd.image0.scaled",
        "tp.sd.image0.image",
        "tp.sd.image0.weight",
    ] {
        let output = project.join(name);
        canary(&output);
        let error = simanalyze(&request(p.clone())).err().unwrap().to_string();
        assert!(error.contains("already exists"), "{error}");
        preserved(&output);
        assert!(!project.join("tp.sd.image").exists());
        std::fs::remove_dir_all(output).unwrap();
    }
}

#[test]
fn absolute_project_can_feather_its_new_total_power_image() {
    let (scratch, mut p) = fixture("seed");
    p.insert("modelimage".into(), json!(""));
    p.insert("featherimage".into(), json!("$project.sd.image"));
    let outcome = simanalyze(&request(p)).unwrap();
    assert!(outcome.stem.ends_with(".feather"));
    assert!(outcome.fidelity.is_some());
    assert!(scratch.path().join("seed/seed.sd.image").exists());
    assert!(
        scratch
            .path()
            .join("seed/seed.small.feather.image")
            .exists()
    );
}
