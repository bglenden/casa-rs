// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA image prediction over windows with different correlation setups.
mod existing_ms_calculations;
#[path = "mixed_correlations/review.rs"]
mod review;
use casa_simulation::{BackendRequest, PredictSpec, SessionResources, Simulator, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn mixed_window_correlations_match_casa() {
    run(include_bytes!("data/sm_mixed_correlations_casa.json.gz"));
}

#[test]
fn mixed_polarization_aliases_match_casa() {
    run(include_bytes!("data/sm_mixed_aliases_casa.json.gz"));
}

fn run(bytes: &[u8]) {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(bytes)).unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let inputs = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_mixed_correlations");
    let image = inputs.with_file_name("sm_image_layouts").join("iquv.image");
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
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("mixed.ms");
            copy(
                &inputs.join(format!("{}.ms", case["layout"].as_str().unwrap())),
                &path,
            );
            if case["spec"]["aliases"] == true {
                use casa_tables::{Table, TableOptions};
                use casa_types::{ScalarValue, Value};
                let target = path.join("DATA_DESCRIPTION");
                let mut table = Table::open(TableOptions::new(&target)).unwrap();
                table
                    .cell_accessor_mut(1, "SPECTRAL_WINDOW_ID")
                    .unwrap()
                    .set(Value::Scalar(ScalarValue::Int32(0)))
                    .unwrap();
                table.save(TableOptions::new(&target)).unwrap();
            }
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            if case["spec"]["selected"] == true {
                if case["spec"]["aliases"] == true {
                    sm.set_data(&[], &[], "DATA_DESC_ID == 1").unwrap();
                } else {
                    sm.set_data(&[1], &[], "").unwrap();
                }
            }
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
                images: vec![image.clone()],
                component_lists,
                incremental: false,
            })
            .unwrap_or_else(|error| panic!("{}: {error}", case["name"]));
            sm.close().unwrap();
            if case["spec"]["selected"] == true {
                assert_unselected_window(&path, case);
            }
            let nrms = existing_ms_calculations::compare(&path, case);
            println!("mixed correlations {} {prefix}: NRMS {nrms}", case["name"]);
            if let Some(export) = std::env::var_os("CASA_RS_MIXED_CORRELATIONS_OUT") {
                copy(
                    &path,
                    &Path::new(&export)
                        .join(format!("{prefix}-{}.ms", case["name"].as_str().unwrap())),
                );
            }
            assert!(nrms < 1e-3, "{} {prefix}: {nrms}", case["name"]);
        }
    }
}

fn assert_unselected_window(path: &Path, case: &Value) {
    use casa_tables::{Table, TableOptions};
    use casa_types::ArrayValue;
    let table = Table::open(TableOptions::new(path)).unwrap();
    for (row, before) in case["before"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
    {
        if before["data_desc_id"] != 0 {
            continue;
        }
        for name in ["DATA", "MODEL_DATA", "CORRECTED_DATA"] {
            let ArrayValue::Complex32(values) =
                table.cell_accessor(row, name).unwrap().array().unwrap()
            else {
                panic!("complex science");
            };
            let correlations = values.shape()[0];
            for (index, reference) in before[name.to_lowercase()]
                .as_array()
                .unwrap()
                .iter()
                .enumerate()
            {
                let value = values[[index % correlations, index / correlations]];
                assert_eq!(
                    [f64::from(value.re), f64::from(value.im)],
                    [
                        reference[0].as_f64().unwrap(),
                        reference[1].as_f64().unwrap()
                    ],
                    "unchanged {name} row{row}"
                );
            }
        }
    }
}
