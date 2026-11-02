// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unactivated scan/observation preview: only catalog refusal is bypassed.
use std::path::Path;

use casa_imaging_application::{ImagingRequest, product_law};
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
        let nrms =
            product_law::image_nrms(Path::new(&expected), Path::new(&artifact.path)).unwrap();
        assert!(nrms <= product_law::TOLERANCE, "{suffix}: {nrms:e}");
    }
}

#[test]
fn preview_scan_observation_reaches_survey_and_imaging() {
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
            let result = run(parameters, &request).unwrap();
            assert_eq!(result.run.gridded_samples, reference.gridded_samples);
            assert_eq!(
                result
                    .artifacts
                    .iter()
                    .filter(|artifact| artifact.exists)
                    .count(),
                products.suffixes().len()
            );
            compare_products(
                &result,
                &request.imagename,
                &observation.scratch(&reference_name),
            );
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

#[test]
fn single_term_controller_peak_matches_published_residual_units() {
    use casa_images::PagedImage;
    use casa_imaging_application::{Cancel, HostResources, RunContext, execute};
    use casars_imager::resolve_request;

    let observation = numeric_fixture();
    let prefix = observation.scratch("residual-units");
    let mut values = json!({
        "vis": observation.measurement_set(), "imagename": prefix,
        "backend": "cpu", "gridprecision": "f32", "parallel": false,
        "field": "2", "datacolumn": "data", "gridder": "standard",
        "deconvolver": "hogbom", "nterms": 1,
        "niter": 10, "gain": 0.1, "threshold": "0Jy", "write_pb": false,
    });
    for (name, value) in observation.geometry().parameters() {
        values[name] = value;
    }
    let (_, request) = resolve_request(&ImagingParameters(values.as_object().unwrap().clone()))
        .expect("production field-selection request");
    let outcome = execute(
        &request,
        RunContext {
            host: HostResources::detect().unwrap(),
            policy: request.resource_policy(),
            cancel: Cancel::default(),
            summary: None,
        },
    )
    .expect("single-term clean");
    assert_eq!(outcome.total_minor_iterations, 10);
    let controller_peak = outcome
        .final_residual_statistics
        .expect("final convergence measurement")
        .peak;

    let residual = PagedImage::<f32>::open(prefix.with_extension("residual")).unwrap();
    let pixels = residual.get().unwrap();
    let shape = residual.shape();
    let valid = residual
        .get_mask_slice(&vec![0; shape.len()], shape, &vec![1; shape.len()])
        .unwrap();
    let mut published_peak = 0.0_f64;
    for (index, &pixel) in pixels.indexed_iter() {
        if valid.as_ref().is_none_or(|mask| mask[index]) {
            assert!(pixel.is_finite(), "nonfinite valid residual");
            published_peak = published_peak.max(f64::from(pixel).abs());
        }
    }
    assert!(published_peak > 0.0 && controller_peak.is_finite());
    // Both paths measure the same final residual in Jy/beam. Publishing a
    // Float image rounds each value; taking a maximum does not accumulate
    // those errors. Two Float epsilons allow the final cast and arithmetic
    // ordering, but not a distinct PSF-peak normalization of the controller.
    let budget = 2.0 * f64::from(f32::EPSILON) * published_peak;
    assert!(
        (controller_peak - published_peak).abs() <= budget,
        "controller peak {controller_peak:.17e}, published peak {published_peak:.17e}, \
         Float rounding budget {budget:.3e}"
    );
}
