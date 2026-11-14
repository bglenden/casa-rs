// SPDX-License-Identifier: LGPL-3.0-or-later
//! Beam sampling on CASA simulator mosaics with irregular channel frequencies.
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

#[test]
fn irregular_mosaic_beam_channels_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_mosaic_irregular_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
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
            if case["profile"] != "fused" {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("signals.ms");
            copy(&data.join("sm_image_layouts/base.ms"), &path);
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: casa_test_support::deterministic_measures_provider(),
                    resources,
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            sm.set_options(&PredictionOptions {
                ftmachine: "mosaic".into(),
                ..PredictionOptions::default()
            })
            .unwrap();
            sm.set_vp(&VoltagePatternSpec {
                dovp: false,
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
            sm.predict(&PredictSpec {
                images: vec![data.join("sm_voltage_patterns/sky.image")],
                ..PredictSpec::default()
            })
            .unwrap();
            sm.close().unwrap();
            let mut reference = case.clone();
            if let Some(rows) = case.get("corrected_rows") {
                reference["after"]["rows"] = rows.clone();
            }
            let nrms = existing_ms_calculations::compare(&path, &reference);
            let table = casa_tables::Table::open(casa_tables::TableOptions::new(&path)).unwrap();
            let casa_types::ArrayValue::Complex32(values) =
                table.cell_accessor(0, "DATA").unwrap().array().unwrap()
            else {
                panic!("DATA")
            };
            for channel in 0..4 {
                println!(
                    "{prefix} channel {channel}: native {:?}, CASA {}",
                    values[[0, channel]],
                    reference["after"]["rows"][0]["data"][channel * 4]
                );
            }
            println!("irregular mosaic {name} {prefix}: DATA NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} {prefix}: {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_MOSAIC_IRREGULAR_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("{prefix}-{name}.ms")),
                );
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
