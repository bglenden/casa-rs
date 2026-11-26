// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulation plot documents and payload validation against CASA 6.7.6.14 oracles.

use std::io::Read;
use std::path::Path;

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_plot_data::plot_document::{
    PlotArrayDtype, PlotBinaryArray, PlotDocumentPanel, PlotLayerData,
};
use casa_plot_data::simulation::{simanalyze_with_plots, simobserve_with_plots};
use casa_simulation::simanalyze::SimanalyzeRequest;
use casa_simulation::simobserve::{SimobserveParameters, SimobserveRequest, simobserve};
use casa_simulation::{BackendRequest, ExecutionControls, SessionResources, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use flate2::read::GzDecoder;
use serde_json::{Value, json};

#[path = "../../../crates/casa-simulation/tests/support/simulation_plot_inputs.rs"]
mod simulation_plot_inputs;

fn simobserve_fixture() -> Value {
    let mut text = String::new();
    GzDecoder::new(
        &include_bytes!(
            "../../../crates/casa-simulation/tests/data/simobserve_graphics_casa.json.gz"
        )[..],
    )
    .read_to_string(&mut text)
    .expect("CASA fixture");
    let value: Value = serde_json::from_str(&text).expect("CASA JSON");
    assert_eq!(value["casa_version"], "6.7.6.14");
    value
}

fn simobserve_request(
    root: &Path,
    name: &str,
    workers: usize,
    graphics: &str,
) -> SimobserveRequest {
    let (image, components, config) = simulation_plot_inputs::inputs(root);
    let project = root.join(name);
    let mut parameters = SimobserveParameters {
        project: project.display().to_string(),
        skymodel: image.display().to_string(),
        antennalist: config.display().to_string(),
        mapsize: vec!["1arcsec".into()],
        integration: "10s".into(),
        totaltime: "40s".into(),
        thermalnoise: String::new(),
        graphics: graphics.into(),
        hourangle: "35s".into(),
        ..SimobserveParameters::default()
    };
    if name.starts_with("int_components") {
        parameters.skymodel.clear();
        parameters.complist = components.display().to_string();
        parameters.compwidth = "1GHz".into();
    }
    if name == "sd_image" {
        parameters.obsmode = "sd".into();
        parameters.sdantlist.clone_from(&parameters.antennalist);
        parameters.antennalist.clear();
        parameters.maptype = "square".into();
        parameters.incell = "2arcsec".into();
    }
    if name == "int_dense_pointings" {
        parameters.mapsize = vec!["16arcsec".into()];
        parameters.pointingspacing = "1arcsec".into();
        parameters.maptype = "hexagonal".into();
    }
    let resources = SessionResources::pinned(
        HostResources {
            threads: 8,
            performance_cores: 8,
            available_memory: 8 << 30,
            metal: false,
        },
        ResourcePolicy::Explicit {
            workers,
            memory: 2 << 30,
        },
        BackendRequest::Cpu,
    )
    .expect("resources");
    SimobserveRequest {
        workspace: None,
        parameters,
        data_roots: Vec::new(),
        options: SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources,
            now: MEpoch::from_mjd(60_000.0, EpochRef::UTC),
        },
    }
}

#[test]
fn complete_native_figures_match_casas_independent_plot_data() {
    let oracle = simobserve_fixture();
    for case in oracle["cases"].as_array().expect("cases") {
        let name = case["name"].as_str().expect("name");
        for workers in [1, 3] {
            let directory = tempfile::tempdir().expect("directory");
            let payload = directory.path().join("arrays.bin");
            std::fs::write(&payload, []).expect("payload");
            let (outcome, plots) = simobserve_with_plots(
                &simobserve_request(directory.path(), name, workers, "file"),
                &payload,
            )
            .expect("native figures");
            assert!(outcome.warnings.is_empty(), "{:?}", outcome.warnings);
            assert_eq!(plots.len(), 2);
            let bytes = std::fs::read(&payload).expect("bytes");
            for plot in &plots {
                plot.document
                    .validate_payload(bytes.len() as u64)
                    .expect("valid views");
                let file = plot
                    .output_file
                    .as_ref()
                    .expect("file mode")
                    .file_name()
                    .expect("name")
                    .to_string_lossy();
                let reference = &case["figures"][file.as_ref()];
                assert!(reference.is_array(), "{file} has no CASA reference");
                if name == "int_dense_pointings" && plot.document.id == "skymodel" {
                    assert!(
                        reference[0]["lines"][0]["x"]
                            .as_array()
                            .expect("dense map")
                            .len()
                            > 19
                    );
                }
                for (index, panel) in plot.document.panels.iter().enumerate() {
                    compare_panel(panel, &reference[index], &bytes);
                }
            }
        }
    }
}

#[test]
fn simanalyze_complete_plot_payload_validates() {
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
    let payload = root.join("arrays.bin");
    std::fs::File::create(&payload).unwrap();
    let request = SimanalyzeRequest {
        workspace: None,
        resources,
        parameters: json!({
            "project": project, "imsize": [128,128], "niter": 20,
            "threshold": "1e-7Jy", "analyze": true, "graphics": "file",
        })
        .as_object()
        .unwrap()
        .clone(),
    };
    let (result, plots) = simanalyze_with_plots(&request, &payload).unwrap();
    assert!(result.fidelity.unwrap().medabsdevmed > 0.);
    assert_eq!(plots.len(), 2);
    let bytes = std::fs::metadata(payload).unwrap().len();
    assert!(bytes > 128 * 128 * 4);
    for plot in plots {
        plot.document.validate_payload(bytes).unwrap();
    }
}

fn flatten(value: &Value, out: &mut Vec<f64>) {
    if let Some(array) = value.as_array() {
        for value in array {
            flatten(value, out);
        }
    } else {
        out.push(value.as_f64().expect("CASA pixel"));
    }
}

fn binary_values(array: &PlotBinaryArray, bytes: &[u8]) -> Vec<f64> {
    let size = array.dtype.bytes();
    let strides = if array.strides_bytes.is_empty() {
        if array.shape.len() == 2 {
            vec![array.shape[1] * size, size]
        } else {
            vec![size]
        }
    } else {
        array.strides_bytes.clone()
    };
    let read = |offset: u64| {
        let offset = offset as usize;
        match array.dtype {
            PlotArrayDtype::F32Le => f64::from(f32::from_le_bytes(
                bytes[offset..offset + 4].try_into().expect("f32"),
            )),
            PlotArrayDtype::F64Le => {
                f64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("f64"))
            }
        }
    };
    let mut output = Vec::new();
    for i in 0..array.shape[0] {
        if array.shape.len() == 2 {
            for j in 0..array.shape[1] {
                output.push(read(array.offset_bytes + i * strides[0] + j * strides[1]));
            }
        } else {
            output.push(read(array.offset_bytes + i * strides[0]));
        }
    }
    output
}

fn close_values(actual: &[f64], reference: &Value, allowance: f64, context: &str) {
    let mut expected = Vec::new();
    flatten(reference, &mut expected);
    assert_eq!(actual.len(), expected.len(), "{context}: full sample count");
    let mut maximum = 0.0_f64;
    for (&actual, expected) in actual.iter().zip(expected) {
        assert!(
            actual.is_finite() && expected.is_finite(),
            "{context}: finite samples"
        );
        maximum = maximum.max((actual - expected).abs());
    }
    assert!(
        maximum <= allowance,
        "{context}: {maximum} exceeds {allowance}"
    );
}

fn compare_panel(panel: &PlotDocumentPanel, reference: &Value, bytes: &[u8]) {
    assert_eq!(panel.title, reference["title"].as_str().expect("title"));
    assert_eq!(
        panel.axes[0].label,
        reference["xlabel"].as_str().expect("xlabel")
    );
    assert_eq!(
        panel.axes[1].label,
        reference["ylabel"].as_str().expect("ylabel")
    );
    close_values(
        &[panel.axes[0].lower, panel.axes[0].upper],
        &reference["xlim"],
        1e-3,
        &format!("{} x limits", panel.id),
    );
    close_values(
        &[panel.axes[1].lower, panel.axes[1].upper],
        &reference["ylim"],
        1e-3,
        &format!("{} y limits", panel.id),
    );
    let (mut line_index, mut image_index, mut circle_index, mut anchored_index) = (0, 0, 0, 0);
    for layer in &panel.layers {
        match layer.data.as_ref().expect("binary layer") {
            PlotLayerData::Points { x, y } => {
                let expected = &reference["lines"][line_index];
                line_index += 1;
                close_values(
                    &binary_values(x, bytes),
                    &expected["x"],
                    1e-3,
                    &format!("{} {} x", panel.id, layer.id),
                );
                close_values(
                    &binary_values(y, bytes),
                    &expected["y"],
                    1e-3,
                    &format!("{} {} y", panel.id, layer.id),
                );
            }
            PlotLayerData::Image {
                pixels,
                extent,
                range,
                ..
            } => {
                let expected = &reference["images"][image_index];
                image_index += 1;
                let actual = binary_values(pixels, bytes);
                let mut expected_pixels = Vec::new();
                flatten(&expected["pixels"], &mut expected_pixels);
                assert_eq!(actual.len(), expected_pixels.len());
                let error = actual
                    .iter()
                    .zip(&expected_pixels)
                    .map(|(a, e)| (a - e).powi(2))
                    .sum::<f64>();
                let power = expected_pixels.iter().map(|e| e * e).sum::<f64>();
                assert!(
                    power > 0.0 && (error / power).sqrt() <= 1e-3,
                    "{} full image NRMS",
                    panel.id
                );
                close_values(extent, &expected["extent"], 1e-9, "image extent");
                close_values(range, &expected["clim"], 1e-5, "image colour limits");
            }
            PlotLayerData::Circles {
                x,
                y,
                radii,
                anchored,
                ..
            } => {
                let (x, y, radii) = (
                    binary_values(x, bytes),
                    binary_values(y, bytes),
                    binary_values(radii, bytes),
                );
                for ((x, y), radius) in x.into_iter().zip(y).zip(radii) {
                    let expected = if *anchored {
                        let value = &reference["anchored_circles"][anchored_index];
                        anchored_index += 1;
                        value
                    } else {
                        let value = &reference["circles"][circle_index];
                        circle_index += 1;
                        value
                    };
                    close_values(&[x, y], &expected["center"], 1e-3, "circle centre");
                    assert!((radius - expected["radius"].as_f64().expect("radius")).abs() <= 1e-9);
                }
            }
        }
    }
    assert_eq!(
        line_index,
        reference["lines"].as_array().expect("lines").len()
    );
    assert_eq!(
        image_index,
        reference["images"].as_array().expect("images").len()
    );
    assert_eq!(
        circle_index,
        reference["circles"].as_array().expect("circles").len()
    );
    assert_eq!(
        anchored_index,
        reference
            .get("anchored_circles")
            .and_then(Value::as_array)
            .map_or(0, Vec::len)
    );
    assert_eq!(
        panel.annotations.len(),
        reference["texts"].as_array().expect("texts").len()
    );
    for (actual, expected) in panel
        .annotations
        .iter()
        .zip(reference["texts"].as_array().expect("texts"))
    {
        assert_eq!(actual.text, expected["text"].as_str().expect("text"));
        close_values(
            &[actual.x, actual.y],
            &expected["position"],
            1e-3,
            "text position",
        );
    }
}
