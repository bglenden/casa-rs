// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA simulator image-layout outcomes and polarized visibilities.
mod existing_ms_calculations;
use casa_simulation::{BackendRequest, CasaOutcome, PredictSpec, Simulator, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn direct_image_layouts_and_stokes_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_image_layouts_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_layouts");
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7);
    let host = casa_imaging_runtime::HostResources::detect().unwrap();
    let mut policies = vec![
        ("w1", predict_support::resources(1, 2 << 30)),
        ("w3", predict_support::resources(3, 2 << 30)),
    ];
    if host.metal {
        policies.push((
            "metal3",
            casa_simulation::SessionResources::pinned(
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
        for case in cases {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("predicted.ms");
            copy(&inputs.join("base.ms"), &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_data(&[], &[0], "").unwrap();
            if case["beam"] == true {
                sm.set_vp(&casa_simulation::VoltagePatternSpec {
                    do_squint: false,
                    ..casa_simulation::VoltagePatternSpec::default()
                })
                .unwrap();
            }
            let result = sm.predict(&PredictSpec {
                images: vec![inputs.join(format!("{}.image", case["name"].as_str().unwrap()))],
                component_lists: Vec::new(),
                incremental: false,
            });
            if case["returned"] == true {
                result.unwrap();
            } else {
                assert_eq!(
                    result.unwrap_err().outcome(),
                    CasaOutcome::ReturnsFalse,
                    "{}",
                    case["name"]
                );
            }
            sm.close().unwrap();
            let nrms = existing_ms_calculations::compare(&path, case);
            assert!(nrms < 1e-3, "{} {prefix}: NRMS {nrms}", case["name"]);
            println!("image {} {prefix}: NRMS {nrms}", case["name"]);
            if let Some(export) = std::env::var_os("CASA_RS_IMAGE_LAYOUTS_OUT") {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("{prefix}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
        }
    }
}

#[test]
fn polarized_total_power_refuses_before_testing_spectral_overlap() {
    use casa_tables::{Table, TableOptions};
    use casa_types::{ArrayValue, Value as CellValue};
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_image_layouts_casa.json.gz")[..],
    ))
    .unwrap();
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_layouts");
    for overlap in [true, false] {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unsupported.ms");
        copy(&inputs.join("base.ms"), &path);
        if !overlap {
            let spectral = path.join("SPECTRAL_WINDOW");
            let mut table = Table::open(TableOptions::new(&spectral)).unwrap();
            for row in 0..table.row_count() {
                table
                    .cell_accessor_mut(row, "CHAN_FREQ")
                    .unwrap()
                    .set(CellValue::Array(ArrayValue::Float64(
                        ndarray::arr1(&[1e12, 1.1e12, 1.2e12, 1.3e12]).into_dyn(),
                    )))
                    .unwrap();
            }
            table.save(TableOptions::new(&spectral)).unwrap();
        }
        let before = std::fs::read(path.join("table.dat")).unwrap();
        let mut sm = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: casa_test_support::deterministic_measures_provider(),
                resources: predict_support::resources(1, 2 << 30),
                now: MEpoch::from_mjd(predict_support::REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .unwrap();
        sm.set_data(&[], &[0], "").unwrap();
        sm.set_options(&casa_simulation::PredictionOptions {
            ftmachine: "sd".into(),
            grid_function: "BOX".into(),
            ..casa_simulation::PredictionOptions::default()
        })
        .unwrap();
        let error = sm
            .predict(&PredictSpec {
                images: vec![inputs.join("iquv.image")],
                component_lists: Vec::new(),
                incremental: false,
            })
            .unwrap_err();
        assert_eq!(error.outcome(), CasaOutcome::NotYetImplemented);
        assert!(error.to_string().contains("#1299"));
        sm.close().unwrap();
        assert_eq!(std::fs::read(path.join("table.dat")).unwrap(), before);
        let unchanged = serde_json::json!({"after": fixture["cases"][0]["before"]});
        assert_eq!(existing_ms_calculations::compare(&path, &unchanged), 0.);
    }
}
