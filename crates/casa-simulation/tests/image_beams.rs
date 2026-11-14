// SPDX-License-Identifier: LGPL-3.0-or-later
//! Saved Jones-image inputs modify simulated image and component signals.
mod existing_ms_calculations;

use casa_simulation::{
    BackendRequest, PredictSpec, PredictionOptions, SessionResources, Simulator, SimulatorOptions,
    VoltagePatternSpec,
};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

fn run(operation: &str) {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_image_beams_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let inputs = data.join("sm_image_beams");
    let sky = data.join("sm_voltage_patterns");
    let mut policies = vec![
        ("w1", support::resources(1, 2 << 30)),
        ("w3", support::resources(3, 2 << 30)),
    ];
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
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
    let mut failures = Vec::new();
    for (prefix, resources) in policies {
        for case in fixture["cases"].as_array().unwrap() {
            if case["operation"] != operation {
                continue;
            }
            let name = case["name"].as_str().unwrap();
            let inputs = case["inputs"]
                .as_str()
                .map_or_else(|| inputs.clone(), |name| inputs.join(name));
            let temporary = tempfile::tempdir().unwrap();
            let path = temporary.path().join("signals.ms");
            let base = case["base_ms"].as_str().map_or_else(
                || data.join("sm_image_layouts/base.ms"),
                |name| inputs.join(name),
            );
            copy(&base, &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            if operation.starts_with("mosaic") {
                sm.set_options(&PredictionOptions {
                    ftmachine: "mosaic".into(),
                    ..PredictionOptions::default()
                })
                .unwrap();
            }
            let beam = case["beam"].as_str().unwrap();
            let beam_table = temporary.path().join("beam.vp");
            copy(&inputs.join(format!("{beam}.vp")), &beam_table);
            let mut table =
                casa_tables::Table::open(casa_tables::TableOptions::new(&beam_table)).unwrap();
            let mut record = table
                .cell_accessor(0, "pbdescription")
                .unwrap()
                .record()
                .unwrap();
            for key in ["realimage", "imagimage", "compleximage"] {
                if let Some(casa_types::Value::Scalar(casa_types::ScalarValue::String(path))) =
                    record.get_mut(key)
                    && !path.is_empty()
                {
                    let name = Path::new(path).file_name().unwrap().to_owned();
                    *path = inputs.join(name).to_string_lossy().into_owned();
                }
            }
            table
                .cell_accessor_mut(0, "pbdescription")
                .unwrap()
                .set_record(record)
                .unwrap();
            table.flush().unwrap();
            drop(table);
            sm.set_vp(&VoltagePatternSpec {
                dovp: case["dovp"].as_bool().unwrap_or(true),
                use_default_vp: false,
                vp_table: beam_table.to_string_lossy().into_owned(),
                do_squint: false,
                ..VoltagePatternSpec::default()
            })
            .unwrap();
            sm.set_data(
                &[],
                if case["all_fields_selected"] == true {
                    &[]
                } else {
                    &[0]
                },
                case["query"].as_str().unwrap_or(""),
            )
            .unwrap();
            let mut prediction = PredictSpec::default();
            if case["operation"] != "components" {
                prediction.images.push(
                    case["sky_image"]
                        .as_str()
                        .map_or_else(|| sky.join("sky.image"), |name| inputs.join(name)),
                );
            } else {
                prediction.component_lists.push(sky.join("sky.cl"));
            }
            sm.predict(&prediction)
                .unwrap_or_else(|error| panic!("{name}: {error}"));
            sm.close().unwrap();
            let mut reference = case.clone();
            if let Some(rows) = case.get("corrected_rows") {
                reference["after"]["rows"] = rows.clone();
            }
            let nrms = existing_ms_calculations::compare(&path, &reference);
            println!("image beam {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_IMAGE_BEAMS_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn saved_jones_images_modify_component_signals_like_casa() {
    run("components");
}

#[test]
fn saved_jones_images_modify_model_images_like_casa() {
    run("image");
}

#[test]
fn saved_jones_images_modify_intrinsic_mosaics_like_casa() {
    run("mosaic");
}

#[test]
fn saved_jones_mosaics_use_each_windows_initial_orientation() {
    run("mosaic_window_visits");
}
