// SPDX-License-Identifier: LGPL-3.0-or-later
//! Native simobserve → simanalyze against 13 actual CASA 6.7.6.14 images.
use casa_images::PagedImage;
use casa_simulation::simanalyze::{SimanalyzeRequest, simanalyze_with_plots};
use casa_simulation::simobserve::{SimobserveParameters, SimobserveRequest, simobserve};
use casa_simulation::{ExecutionControls, SessionResources, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use flate2::read::GzDecoder;
use serde_json::{Value, json};
use std::path::Path;

#[path = "support/simulation_plot_inputs.rs"]
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
        &include_bytes!("data/simanalyze_task_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6.14");
    let scratch = tempfile::tempdir().unwrap();
    let root = scratch.path();
    let (model, _, config) = simulation_plot_inputs::inputs(root);
    let project = root.join("analyze");
    let resources = SessionResources::from_controls(&ExecutionControls::default()).unwrap();
    simobserve(&SimobserveRequest {
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
    let payload = root.join("arrays.bin");
    std::fs::File::create(&payload).unwrap();
    let request = SimanalyzeRequest {
        resources,
        parameters: json!({
            "project": project, "imsize": [128,128], "niter": 20,
            "threshold": "1e-7Jy", "analyze": true, "graphics": "file",
        })
        .as_object()
        .unwrap()
        .clone(),
    };
    let result = simanalyze_with_plots(&request, &payload).unwrap();
    for (suffix, expected) in oracle["images"].as_object().unwrap() {
        compare(&project.join(format!("analyze.small.{suffix}")), expected);
    }
    assert!(result.fidelity.unwrap().medabsdevmed > 0.);
    assert_eq!(result.plots.len(), 2);
    let bytes = std::fs::metadata(payload).unwrap().len();
    assert!(bytes > 128 * 128 * 4);
    for plot in result.plots {
        plot.document.validate_payload(bytes).unwrap();
    }
    assert!(
        result
            .products
            .iter()
            .all(|p| p.is_dir() && !p.to_string_lossy().ends_with(".ms"))
    );
    assert!(!project.join("analyze.small.residual.flat").exists());
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
