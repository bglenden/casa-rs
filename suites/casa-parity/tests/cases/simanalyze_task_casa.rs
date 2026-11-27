// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native simobserve → simanalyze against 13 actual CASA 6.7.6.14 images.
use casa_core::measures::{EpochRef, MEpoch};
use casa_images::PagedImage;
use casa_simulation::simanalyze::{SimanalyzeRequest, simanalyze};
use casa_simulation::simobserve::{SimobserveParameters, SimobserveRequest, simobserve};
use casa_simulation::{ExecutionControls, SessionResources, SimulatorOptions};
use flate2::read::GzDecoder;
use serde_json::{Value, json};
use std::path::Path;

#[path = "../../../../libs/casa/casa-simulation/tests/support/simulation_plot_inputs.rs"]
mod simulation_plot_inputs;

fn flattened(value: &Value, output: &mut Vec<f64>) {
    if let Some(values) = value.as_array() {
        for value in values {
            flattened(value, output);
        }
    } else {
        output.push(value.as_f64().unwrap());
    }
}

fn compare(path: &Path, expected: &Value) {
    let image = PagedImage::<f32>::open(path).unwrap();
    assert_eq!(json!(image.shape()), expected["shape"]);
    let mut truth = Vec::new();
    flattened(&expected["pixels"], &mut truth);
    let pixels = image.get().unwrap();
    assert_eq!(pixels.len(), truth.len());
    let (mut error, mut power) = (0., 0.);
    for (&actual, expected) in pixels.iter().zip(truth) {
        assert!(actual.is_finite() && expected.is_finite());
        error += (f64::from(actual) - expected).powi(2);
        power += expected.powi(2);
    }
    let nrms = (error / power.max(f64::MIN_POSITIVE)).sqrt();
    println!("{} NRMS {nrms:e}", path.display());
    assert!(nrms <= 1e-3, "{} NRMS {nrms:e}", path.display());
    // Fidelity is a ratio; CASA's copied Jy/beam label is a documented bug.
    if !path.to_string_lossy().ends_with(".fidelity") {
        assert_eq!(image.units(), expected["unit"].as_str().unwrap());
    }
}

#[test]
fn single_array_imaging_analysis_and_complete_plot_payload_match_casa() {
    let oracle: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!(
            "../../../../libs/casa/casa-simulation/tests/data/simanalyze_task_casa.json.gz"
        )[..],
    ))
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6.14");
    let scratch = tempfile::tempdir().unwrap();
    let root = scratch.path();
    let (model, _, config) = simulation_plot_inputs::inputs(root);
    let project = root.join("analyze");
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    simobserve(&SimobserveRequest {
        workspace: None,
        parameters: SimobserveParameters {
            project: project.display().to_string(),
            skymodel: model.display().to_string(),
            antennalist: config.display().to_string(),
            mapsize: vec!["1arcsec".into()],
            integration: "10s".into(),
            totaltime: "40s".into(),
            thermalnoise: String::new(),
            graphics: "none".into(),
            hourangle: "35s".into(),
            ..SimobserveParameters::default()
        },
        data_roots: Vec::new(),
        options: SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: MEpoch::from_mjd(60_000., EpochRef::UTC),
        },
    })
    .unwrap();
    let request = SimanalyzeRequest {
        workspace: None,
        resources,
        parameters: json!({
            "project": project, "imsize": [128,128], "niter": 20,
            "threshold": "1e-7Jy", "analyze": true, "graphics": "none",
        })
        .as_object()
        .unwrap()
        .clone(),
    };
    let result = simanalyze(&request).unwrap();
    for (suffix, expected) in oracle["images"].as_object().unwrap() {
        compare(&project.join(format!("analyze.small.{suffix}")), expected);
    }
    assert!(result.fidelity.unwrap().medabsdevmed > 0.);
    assert!(
        result
            .products
            .iter()
            .all(|p| p.is_dir() && !p.to_string_lossy().ends_with(".ms"))
    );
    assert!(!project.join("analyze.small.residual.flat").exists());
    repeat_fresh_against_casa(&request, &project, &oracle);
    safety_cases(&request, &project, &model);
    // Refusing overwrite must leave the successful image untouched.
    let mut refusal = request;
    refusal.parameters.insert("overwrite".into(), json!(false));
    assert!(
        casa_simulation::simanalyze::simanalyze(&refusal)
            .err()
            .expect("overwrite refused")
            .to_string()
            .contains("already exists")
    );
}

fn repeat_fresh_against_casa(request: &SimanalyzeRequest, project: &Path, oracle: &Value) {
    let mut repeated = SimanalyzeRequest {
        workspace: None,
        resources: request.resources,
        parameters: request.parameters.clone(),
    };
    repeated.parameters.insert("graphics".into(), json!("none"));
    casa_simulation::simanalyze::simanalyze(&repeated).unwrap();
    for (suffix, expected) in oracle["images"].as_object().unwrap() {
        compare(&project.join(format!("analyze.small.{suffix}")), expected);
    }
}

fn safety_cases(request: &SimanalyzeRequest, project: &Path, model: &Path) {
    let read = |suffix: &str| {
        PagedImage::<f32>::open(project.join(format!("analyze.small.{suffix}")))
            .unwrap()
            .get()
            .unwrap()
    };
    let previous = (read("image"), read("model"));
    let mut run = SimanalyzeRequest {
        workspace: None,
        resources: request.resources,
        parameters: request.parameters.clone(),
    };
    run.parameters.insert("graphics".into(), json!("none"));
    run.parameters.insert("dryrun".into(), json!(true));
    run.parameters.insert("imsize".into(), json!([0, 0]));
    run.parameters.insert("cell".into(), json!("2e-6rad"));
    let positive = casa_simulation::simanalyze::simanalyze(&run)
        .unwrap()
        .imaging_parameters;
    run.parameters.insert("cell".into(), json!("-2e-6rad"));
    let negative = casa_simulation::simanalyze::simanalyze(&run)
        .unwrap()
        .imaging_parameters;
    assert_eq!(positive, negative);
    assert_eq!(positive["imsize"], json!([64, 64]));
    for cell in ["0rad", "1e-300rad"] {
        run.parameters.insert("cell".into(), json!(cell));
        assert!(casa_simulation::simanalyze::simanalyze(&run).is_err());
    }
    run.parameters = request.parameters.clone();
    run.parameters.insert("graphics".into(), json!("none"));
    run.parameters.insert(
        "modelimage".into(),
        json!(project.join("analyze.small.model")),
    );
    assert!(
        casa_simulation::simanalyze::simanalyze(&run)
            .err()
            .unwrap()
            .to_string()
            .contains("would be overwritten")
    );
    assert_eq!((read("image"), read("model")), previous);
    // The old wrapper deleted both products before this native preparation
    // error. An external start model in K cannot initialize a Jy/pixel model.
    let mut invalid = PagedImage::<f32>::open(model).unwrap();
    invalid.set_units("K").unwrap();
    invalid.save().unwrap();
    run.parameters.insert("modelimage".into(), json!(model));
    assert!(
        casa_simulation::simanalyze::simanalyze(&run)
            .err()
            .unwrap()
            .to_string()
            .contains("unit")
    );
    assert_eq!((read("image"), read("model")), previous);
}
