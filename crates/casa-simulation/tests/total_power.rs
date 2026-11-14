// SPDX-License-Identifier: LGPL-3.0-or-later
//! Actual CASA total-power sky inputs and telescope kernels.
#[path = "image_frame_combinations/eop.rs"]
mod eop;
mod existing_ms_calculations;
use casa_simulation::{
    BackendRequest, CasaOutcome, PredictSpec, PredictionOptions, SessionResources, Simulator,
    SimulatorOptions, VoltagePatternSpec,
};
use casa_types::measures::{EpochRef, MEpoch};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

#[test]
fn total_power_sky_inputs_and_kernels_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_total_power_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    let inputs = data.join("sm_mosaic_coverage");
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
            let name = case["name"].as_str().unwrap();
            let pb = name.starts_with("pb_") || name == "moved_pb";
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("signals.ms");
            copy(
                &if name == "mixed" {
                    data.join("sm_total_power/mixed.ms")
                } else {
                    inputs.join("outside.ms")
                },
                &path,
            );
            let image_beam = configure_input(&path, &data, name);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: if case.get("eop").is_some() {
                        eop::provider(&case["eop"])
                    } else {
                        casa_test_support::deterministic_measures_provider()
                    },
                    resources,
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_options(&PredictionOptions {
                ftmachine: if name == "saved_jones_ft" { "ft" } else { "sd" }.into(),
                grid_function: if pb { "PB" } else { "BOX" }.into(),
                w_planes: 16,
                ..PredictionOptions::default()
            })
            .unwrap();
            sm.set_vp(&VoltagePatternSpec {
                dovp: matches!(
                    name,
                    "moved_pb" | "saved_numeric" | "saved_jones" | "saved_jones_ft"
                ),
                use_default_vp: !name.starts_with("saved_"),
                vp_table: if name == "saved_numeric" {
                    data.join("sm_saved_curves/numeric_power.vp")
                        .to_string_lossy()
                        .into_owned()
                } else {
                    image_beam
                },
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
                "",
            )
            .unwrap();
            let prediction = PredictSpec {
                images: if name == "components" {
                    vec![]
                } else {
                    vec![data.join(format!("sm_total_power/{name}.image"))]
                },
                component_lists: if matches!(name, "components" | "image_components") {
                    vec![data.join("sm_total_power/point.cl")]
                } else {
                    vec![]
                },
                ..PredictSpec::default()
            };
            let result = sm.predict(&prediction);
            if case["returned"] == true {
                if let Err(error) = result {
                    failures.push(format!("{name} {prefix}: {error}"));
                    sm.close().unwrap();
                    continue;
                }
            } else {
                if result.is_ok() {
                    failures.push(format!("{name}: unexpectedly succeeded"));
                    sm.close().unwrap();
                    continue;
                }
                assert_eq!(
                    result.unwrap_err().outcome(),
                    CasaOutcome::ReturnsFalse,
                    "{name}"
                );
            }
            sm.close().unwrap();
            let mut reference = case.clone();
            if let Some(rows) = case.get("corrected_rows") {
                reference["after"]["rows"] = rows.clone();
            }
            let nrms = existing_ms_calculations::compare(&path, &reference);
            println!("total power {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_TOTAL_POWER_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

fn configure_input(path: &Path, data: &Path, name: &str) -> String {
    use casa_tables::{Table, TableOptions};
    use casa_types::{ArrayValue, ScalarValue, Value};
    if let Some(telescope) = name.strip_prefix("pb_") {
        let mut table = Table::open(TableOptions::new(path.join("OBSERVATION"))).unwrap();
        table
            .row_accessor_mut()
            .set_cell(
                0,
                "TELESCOPE_NAME",
                Value::Scalar(ScalarValue::String(if name == "pb_unknown" {
                    "NO_SUCH_TELESCOPE".into()
                } else {
                    telescope.to_uppercase()
                })),
            )
            .unwrap();
        table.flush().unwrap();
    }
    if matches!(
        name,
        "pb_vla" | "pb_evla" | "pb_ata" | "pb_ngvla" | "pb_unknown"
    ) {
        let mut table = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW"))).unwrap();
        let ArrayValue::Float64(mut frequencies) = table
            .cell_accessor(0, "CHAN_FREQ")
            .unwrap()
            .array()
            .unwrap()
            .clone()
        else {
            panic!("frequencies")
        };
        frequencies.mapv_inplace(|value| value - 230e9 + 8e9);
        table
            .row_accessor_mut()
            .set_cell(
                0,
                "CHAN_FREQ",
                Value::Array(ArrayValue::Float64(frequencies)),
            )
            .unwrap();
        table
            .row_accessor_mut()
            .set_cell(0, "REF_FREQUENCY", Value::Scalar(ScalarValue::Float64(8e9)))
            .unwrap();
        table.flush().unwrap();
    }
    if name == "moved_pb" {
        let mut table = Table::open(TableOptions::new(path.join("POINTING"))).unwrap();
        for row in 0..table.row_count() {
            let ArrayValue::Float64(mut direction) = table
                .cell_accessor(row, "DIRECTION")
                .unwrap()
                .array()
                .unwrap()
                .clone()
            else {
                panic!("pointing direction")
            };
            direction[[0, 0]] += 5e-6;
            table
                .row_accessor_mut()
                .set_cell(
                    row,
                    "DIRECTION",
                    Value::Array(ArrayValue::Float64(direction)),
                )
                .unwrap();
        }
        table.flush().unwrap();
    }
    if name.starts_with("saved_jones") {
        let beam = path.parent().unwrap().join("beam.vp");
        copy(&data.join("sm_total_power/saved_jones.vp"), &beam);
        let mut table = Table::open(TableOptions::new(&beam)).unwrap();
        let mut record = table
            .cell_accessor(0, "pbdescription")
            .unwrap()
            .record()
            .unwrap();
        *record.get_mut("compleximage").unwrap() = Value::Scalar(ScalarValue::String(
            data.join("sm_image_beams/jones.image")
                .to_string_lossy()
                .into_owned(),
        ));
        table
            .cell_accessor_mut(0, "pbdescription")
            .unwrap()
            .set_record(record)
            .unwrap();
        table.flush().unwrap();
        beam.to_string_lossy().into_owned()
    } else {
        String::new()
    }
}
