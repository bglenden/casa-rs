// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sky overrides, prepared pixels/coordinates and selected predicted DATA.
mod existing_ms_calculations;
use casa_images::PagedImage;
use casa_simulation::simutil::skymodel::{ModelOverrides, modify_model};
use casa_simulation::{BackendRequest, PredictSpec, SessionResources, Simulator, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

fn check_prepared(path: &Path, expected: &Value) {
    let image = PagedImage::<f32>::open(path).unwrap();
    let shape: Vec<usize> = expected["shape"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_u64().unwrap() as usize)
        .collect();
    assert_eq!(image.shape(), shape);
    assert_eq!(image.units(), expected["unit"].as_str().unwrap());
    let coordinates = image.coordinates();
    let units: Vec<String> = (0..coordinates.n_coordinates())
        .flat_map(|index| coordinates.coordinate(index).axis_units())
        .collect();
    let expected_units: Vec<&str> = expected["units"]
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value.as_str().unwrap())
        .collect();
    assert_eq!(units, expected_units);
    let pixels = image.get().unwrap();
    let reference = expected["pixels"].as_array().unwrap();
    let peak = reference
        .iter()
        .map(|v| v.as_f64().unwrap().abs())
        .fold(0., f64::max);
    assert_eq!(pixels.len(), reference.len());
    for (value, reference) in pixels.t().iter().zip(reference) {
        assert!((f64::from(*value) - reference.as_f64().unwrap()).abs() <= 1e-6 * peak);
    }
    for point in expected["worlds"].as_array().unwrap() {
        let pixel: Vec<f64> = point["pixel"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap())
            .collect();
        let world = coordinates.to_world(&pixel).unwrap();
        for (value, reference) in world.iter().zip(point["world"].as_array().unwrap()) {
            let reference = reference.as_f64().unwrap();
            assert!(
                (value - reference).abs() <= 1e-12 * reference.abs().max(1.),
                "world at {pixel:?}: {value} vs {reference}"
            );
        }
    }
}

#[test]
fn overridden_multi_channel_skies_and_combined_inputs_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_prepared_sky_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
    let mut policies = vec![
        ("w1", support::resources(1, 2 << 30)),
        ("w3", support::resources(3, 2 << 30)),
    ];
    if host.metal {
        policies.push((
            "metal3",
            SessionResources::pinned(
                host,
                casa_imaging_runtime::ResourcePolicy::Explicit {
                    workers: 3,
                    memory: 2 << 30,
                },
                BackendRequest::Metal,
            )
            .unwrap(),
        ));
    }
    for (prefix, resources) in policies {
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            let directory = tempfile::tempdir().unwrap();
            let model = directory.path().join("prepared.image");
            let overrides = &case["overrides"];
            let text = |key: &str| overrides[key].as_str().unwrap().to_string();
            modify_model(
                &data.join("sm_image_layouts").join(format!(
                    "{}.image",
                    case["spec"]["source"].as_str().unwrap()
                )),
                &model,
                &ModelOverrides {
                    inbright: text("inbright"),
                    indirection: vec![text("indirection")],
                    incell: vec![text("incell")],
                    incenter: text("incenter"),
                    inwidth: text("inwidth"),
                },
            )
            .unwrap();
            check_prepared(&model, &case["prepared"]);
            let path = directory.path().join("predicted.ms");
            copy(&data.join("sm_prepared_sky/base.ms"), &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_data(&[], &[0], "").unwrap();
            let mut component_lists = Vec::new();
            if case["spec"]["components"] == true {
                let component = directory.path().join("point.cl");
                support::write_stokes_point_list(
                    &component,
                    support::CENTRE_RAD,
                    [0.25, 0.05, -0.02, 0.01],
                );
                component_lists.push(component);
            }
            sm.predict(&PredictSpec {
                images: vec![model.clone()],
                component_lists,
                incremental: false,
            })
            .unwrap();
            sm.close().unwrap();
            let nrms = existing_ms_calculations::compare(&path, case);
            println!("prepared sky {name} {prefix}: NRMS {nrms}");
            assert!(nrms < 1e-3, "{name} {prefix}: {nrms}");
            if let Some(export) = std::env::var_os("CASA_RS_PREPARED_SKY_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
                copy(
                    &model,
                    &Path::new(&export).join(format!("{prefix}-{name}.image")),
                );
            }
        }
    }
}
