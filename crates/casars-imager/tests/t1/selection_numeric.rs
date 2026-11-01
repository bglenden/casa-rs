// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unactivated scan/observation preview: only catalog refusal is bypassed.
use std::path::Path;

use casa_imaging_application::{ImagingRequest, route_law};
use casa_ms::{MeasurementSet, SubtableId};
use casa_types::{ScalarValue, Value};
use casars_imager::{ImagerRunTaskResult, ImagingParameters, resolve_request, run};
use serde_json::json;

use super::fixture::{Component, Observation, PHASE_CENTER_RAD, Setup};

fn numeric_fixture() -> Observation {
    let mut setup = Setup::vla_q_band(2);
    setup.fields = vec![PHASE_CENTER_RAD; 4];
    let observation = Observation::synthesise_setup(
        setup,
        &[(
            "point",
            Component {
                offset_px: [12, 8],
                flux_jy: 1.0,
                fwhm_px: None,
                spectral_index: 0.0,
            },
        )],
    );
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    let row = ms
        .subtable(SubtableId::Observation)
        .unwrap()
        .rows()
        .unwrap()[0]
        .clone();
    ms.subtable_mut(SubtableId::Observation)
        .unwrap()
        .add_row(row)
        .unwrap();
    for row in 0..ms.row_count() {
        let field = match ms
            .main_table()
            .cell_accessor(row, "FIELD_ID")
            .unwrap()
            .scalar()
            .unwrap()
        {
            ScalarValue::Int32(value) => *value,
            other => panic!("unexpected FIELD_ID {other:?}"),
        };
        // Each observation partition contains two scan numbers.
        for (column, value) in [
            ("SCAN_NUMBER", field % 2 + 1),
            ("OBSERVATION_ID", field / 2),
        ] {
            ms.main_table_mut()
                .row_accessor_mut()
                .set_cell(row, column, Value::Scalar(ScalarValue::Int32(value)))
                .unwrap();
        }
    }
    ms.save().unwrap();
    drop(ms);
    observation
}

fn preview(
    observation: &Observation,
    name: &str,
    scan: &str,
    obs: &str,
    selectdata: bool,
) -> (ImagingParameters, ImagingRequest) {
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": observation.scratch(name),
        "backend": "cpu", "niter": 0, "write_pb": true, "selectdata": selectdata,
    });
    for (name, value) in observation.geometry().parameters() {
        values[name] = value;
    }
    let (mut parameters, _) =
        resolve_request(&ImagingParameters(values.as_object().unwrap().clone())).unwrap();
    // Preview only: retain the production catalog's two unimplemented markers.
    parameters.0.insert("scan".to_string(), json!(scan));
    parameters.0.insert("observation".to_string(), json!(obs));
    let request = ImagingRequest::from_tclean(&parameters.0).unwrap();
    (parameters, request)
}

fn compare_products(result: &ImagerRunTaskResult, prefix: &Path, reference: &Path) {
    let prefix = prefix.to_string_lossy();
    for artifact in result.artifacts.iter().filter(|artifact| artifact.exists) {
        let suffix = artifact.path.strip_prefix(prefix.as_ref()).unwrap();
        let expected = format!("{}{suffix}", reference.display());
        let nrms = route_law::image_nrms(Path::new(&expected), Path::new(&artifact.path)).unwrap();
        assert!(nrms <= route_law::TOLERANCE, "{suffix}: {nrms:e}");
    }
}

#[test]
fn preview_scan_observation_reaches_survey_and_both_imaging_routes() {
    let observation = numeric_fixture();
    for (index, scan, obs, fields) in [
        (0, "2", "", "1,3"),
        (1, "", "1", "2,3"),
        (2, "2", "0", "1"),
        (3, "1~2,2", "1", "2,3"),
        (4, "2~2147483647", "0~2147483647", "1,3"),
    ] {
        let reference_name = format!("numeric-reference-{index}");
        let (reference, products) = observation.image(
            &reference_name,
            json!({"field": fields, "niter": 0, "write_pb": true}),
        );
        for selectdata in [false, true] {
            let name = format!("numeric-preview-{index}-{selectdata}");
            let (parameters, request) = preview(&observation, &name, scan, obs, selectdata);
            let legacy = run(parameters.clone(), &request).unwrap();
            assert_eq!(legacy.run.gridded_samples, reference.gridded_samples);
            assert_eq!(
                legacy
                    .artifacts
                    .iter()
                    .filter(|artifact| artifact.exists)
                    .count(),
                products.suffixes().len()
            );
            compare_products(
                &legacy,
                &request.imagename,
                &observation.scratch(&reference_name),
            );
            let batches = route_law::on_batches(&request);
            let mut batch_parameters = parameters;
            batch_parameters
                .0
                .insert("imagename".to_string(), json!(batches.imagename));
            let result = run(batch_parameters, &batches).unwrap();
            assert_eq!(result.run.gridded_samples, reference.gridded_samples);
            assert_eq!(result.artifacts.len(), legacy.artifacts.len());
            compare_products(&result, &batches.imagename, &request.imagename);
        }
    }
    for (index, scan, obs) in [
        (0, "99", ""),
        (1, ">1", ""),
        (2, "1~1", ""),
        (3, "2~1", ""),
        (4, "", "0~0"),
        (5, "", "1~0"),
    ] {
        let name = format!("numeric-refused-{index}");
        let (parameters, request) = preview(&observation, &name, scan, obs, true);
        assert!(run(parameters, &request).is_err());
        assert!(!observation.scratch(&format!("{name}.psf")).exists());
    }
}
