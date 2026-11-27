// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calculation products from actual CASA multi-MS and total-power simanalyze.
use casa_images::{ImageInterface, PagedImage};
use casa_simulation::{
    ExecutionControls, SessionResources,
    session::BackendRequest,
    simanalyze::{SimanalyzeRequest, simanalyze},
};
use flate2::read::GzDecoder;
use serde_json::{Value, json};
use std::path::Path;

fn flatten(value: &Value, output: &mut Vec<f64>) {
    if let Some(values) = value.as_array() {
        for value in values {
            flatten(value, output);
        }
    } else {
        output.push(
            value
                .as_f64()
                .or_else(|| value.as_bool().map(|value| if value { 1. } else { 0. }))
                .expect("CASA pixel or mask"),
        );
    }
}

fn compare(path: &Path, expected: &Value) -> bool {
    let image =
        PagedImage::<f32>::open(path).unwrap_or_else(|error| panic!("{}: {error}", path.display()));
    assert_eq!(
        json!(image.shape()),
        expected["shape"],
        "{}",
        path.display()
    );
    let mut truth = Vec::new();
    flatten(&expected["pixels"], &mut truth);
    let actual = image.get().unwrap();
    let mut mask = Vec::new();
    flatten(&expected["mask"], &mut mask);
    let native_mask = image
        .default_mask()
        .unwrap()
        .unwrap_or_else(|| ndarray::ArrayD::from_elem(ndarray::IxDyn(image.shape()), true));
    assert_eq!(native_mask.len(), mask.len());
    assert!(
        native_mask
            .iter()
            .zip(mask)
            .all(|(&native, expected)| native == (expected != 0.)),
        "{} mask",
        path.display()
    );
    let (mut error, mut power) = (0., 0.);
    // Masked pixels are undefined scientific samples; CASA's Fourier
    // expression leaves different raw values there. Match masks exactly
    // above and keep the 1e-3 NRMS bound over every defined sample.
    for ((&actual, expected), &valid) in actual.iter().zip(truth).zip(native_mask.iter()) {
        assert!(actual.is_finite() && expected.is_finite());
        if valid {
            error += (f64::from(actual) - expected).powi(2);
            power += expected.powi(2);
        }
    }
    let nrms = (error / power.max(f64::MIN_POSITIVE)).sqrt();
    println!("{} NRMS {nrms:e}", path.display());
    for (name, actual) in [
        (
            "reference",
            (0..image.coordinates().n_coordinates())
                .flat_map(|index| image.coordinates().coordinate(index).reference_value())
                .collect::<Vec<_>>(),
        ),
        (
            "increment",
            (0..image.coordinates().n_coordinates())
                .flat_map(|index| image.coordinates().coordinate(index).increment())
                .collect::<Vec<_>>(),
        ),
    ] {
        let expected = expected[name].as_array().unwrap();
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.into_iter().zip(expected) {
            let expected = expected.as_f64().unwrap();
            assert!(
                (actual - expected).abs() <= 1e-12 * expected.abs().max(1.),
                "{} {name}",
                path.display()
            );
        }
    }
    if path.to_string_lossy().ends_with(".image") {
        assert_eq!(
            image.coordinates().obs_info().telescope,
            expected["telescope"].as_str().unwrap()
        );
        assert_eq!(
            image.coordinates().obs_info().observer,
            expected["observer"].as_str().unwrap()
        );
    }
    if !path.to_string_lossy().ends_with(".fidelity") {
        assert_eq!(image.units(), expected["unit"].as_str().unwrap());
    }
    nrms <= 1e-3
}

#[test]
fn weighted_multi_array_and_total_power_products_match_casa() {
    let oracle: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!(
            "../../../../libs/casa/casa-simulation/tests/data/simanalyze_multi_casa.json.gz"
        )[..],
    ))
    .unwrap();
    assert_eq!(oracle["casa_version"], "6.7.6.14");
    let scratch = tempfile::tempdir().unwrap();
    tar::Archive::new(GzDecoder::new(
        &include_bytes!(
            "../../../../libs/casa/casa-simulation/tests/data/simanalyze_multi_inputs.tar.gz"
        )[..],
    ))
    .unpack(scratch.path())
    .unwrap();
    let resources = SessionResources::from_controls(&ExecutionControls {
        backend: BackendRequest::Cpu,
        ..ExecutionControls::default()
    })
    .unwrap();
    let mut passed = true;
    for case in ["multi", "tp", "seed", "feather"] {
        let reference = &oracle["cases"][case];
        let project = scratch.path().join(case);
        let mut parameters = reference["parameters"].as_object().unwrap().clone();
        parameters.insert("project".into(), json!(case));
        if case == "feather" {
            parameters.insert("featherimage".into(), json!("tp/tp.sd.image"));
        }
        let outcome = simanalyze(&SimanalyzeRequest {
            workspace: Some(scratch.path().to_path_buf()),
            parameters,
            resources,
        })
        .unwrap();
        assert!(outcome.fidelity.is_some());
        for (name, expected) in reference["images"].as_object().unwrap() {
            passed &= compare(&project.join(name), expected);
        }
    }
    assert!(
        passed,
        "CASA numerical comparison failed; see per-product NRMS above"
    );
}
