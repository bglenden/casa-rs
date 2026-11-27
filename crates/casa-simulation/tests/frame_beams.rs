// SPDX-License-Identifier: LGPL-3.0-or-later
//! Separate array/FT observer contexts and cached beam epochs against CASA.
#[path = "image_frame_combinations/eop.rs"]
mod eop;
mod existing_ms_calculations;
use casa_core::measures::{EpochRef, MEpoch};
use casa_simulation::{
    PredictSpec, PredictionOptions, Simulator, SimulatorOptions, VoltagePatternSpec,
};
use existing_ms_calculations::{copy, predict_support as support};
use flate2::read::GzDecoder;
use serde_json::Value;
use std::path::Path;

fn beam_spec(case: &Value, data: &Path, temporary: &Path) -> VoltagePatternSpec {
    let component = case["operation"] == "component";
    let increment = if component {
        &case["spec"]["beam_increment_deg"]
    } else {
        &case["beam_increment_deg"]
    };
    let mut spec = VoltagePatternSpec {
        do_squint: case["squint"].as_bool().unwrap_or(false),
        parallactic_angle_increment_deg: increment.as_f64().unwrap(),
        ..VoltagePatternSpec::default()
    };
    if let Some(input) = case["vp_table"].as_str() {
        let source = data.join(input);
        let path = temporary.join("beam.vp");
        copy(&source, &path);
        let mut table = casa_tables::Table::open(casa_tables::TableOptions::new(&path)).unwrap();
        let mut record = table
            .cell_accessor(0, "pbdescription")
            .unwrap()
            .record()
            .unwrap();
        for key in ["realimage", "imagimage", "compleximage"] {
            if let Some(casa_core::Value::Scalar(casa_core::ScalarValue::String(value))) =
                record.get_mut(key)
                && !value.is_empty()
            {
                let name = Path::new(value).file_name().unwrap().to_owned();
                *value = source
                    .parent()
                    .unwrap()
                    .join(name)
                    .to_string_lossy()
                    .into_owned();
            }
        }
        table
            .cell_accessor_mut(0, "pbdescription")
            .unwrap()
            .set_record(record)
            .unwrap();
        table.flush().unwrap();
        spec.use_default_vp = false;
        spec.vp_table = path.to_string_lossy().into_owned();
    }
    spec
}

#[test]
fn time_dependent_beam_frames_match_casa() {
    let fixture: Value = serde_json::from_reader(GzDecoder::new(
        &include_bytes!("data/sm_frame_beams_casa.json.gz")[..],
    ))
    .unwrap();
    assert_eq!(fixture["casa_version"], "6.7.6.14");
    let data = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data");
    for workers in [1, 3] {
        let mut failures = Vec::new();
        for case in fixture["cases"].as_array().unwrap() {
            let name = case["name"].as_str().unwrap();
            if std::env::var("CASA_RS_FRAME_BEAM_CASE")
                .is_ok_and(|selected| !selected.split(',').any(|value| value == name))
            {
                continue;
            }
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("beam.ms");
            copy(
                &data.join(case["base"].as_str().unwrap_or("sm_image_layouts/base.ms")),
                &path,
            );
            let mut sm = Simulator::open_from_ms(
                &path,
                SimulatorOptions {
                    measures: eop::provider(&case["eop"]),
                    resources: support::resources(workers, 2 << 30),
                    now: MEpoch::from_mjd(support::REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap();
            if let Some(location) = case.get("location_itrf_m") {
                sm.set_options(&PredictionOptions {
                    location_itrf_m: std::array::from_fn(|axis| location[axis].as_f64().unwrap()),
                    ..PredictionOptions::default()
                })
                .unwrap();
            }
            sm.set_data(&[], &[0], "").unwrap();
            let component = case["operation"] == "component";
            sm.set_vp(&beam_spec(case, &data, directory.path()))
                .unwrap();
            let input = data.join(case["input"].as_str().unwrap());
            let spec = if component {
                PredictSpec {
                    component_lists: vec![input],
                    ..PredictSpec::default()
                }
            } else {
                PredictSpec {
                    images: vec![input],
                    ..PredictSpec::default()
                }
            };
            sm.predict(&spec)
                .unwrap_or_else(|error| panic!("{name} W{workers}: {error}"));
            sm.close().unwrap();
            let nrms = existing_ms_calculations::compare(&path, case);
            println!("frame beam {name} w{workers}: NRMS {nrms}");
            if !nrms.is_finite() || nrms >= 1e-3 {
                failures.push(format!("{name} w{workers}: NRMS {nrms}"));
            }
            if let Some(export) = std::env::var_os("CASA_RS_FRAME_BEAM_OUT") {
                copy(
                    &path,
                    &Path::new(&export).join(format!("w{workers}-{name}.ms")),
                );
            }
        }
        assert!(failures.is_empty(), "{}", failures.join("\n"));
    }
}
