// SPDX-License-Identifier: LGPL-3.0-or-later
//! Unactivated scan/observation preview: only catalog refusal is bypassed.
use std::path::Path;

use casa_imaging_application::{ImagingRequest, route_law};
use casa_ms::{MeasurementSet, SubtableId};
use casa_types::{ScalarValue, Value};
use casars_imager::{ImagerRunTaskResult, ImagingParameters, run};
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
    // Preview only: change two refusal markers in an owned bundle, preserving
    // the real resolver and the production catalog's unimplemented markers.
    let mut bundle = casa_provider_contracts::builtin_surface_bundle("tclean").unwrap();
    let casa_provider_contracts::SurfaceDefinition::Task(definition) = &mut bundle.surface;
    let mut cleared = 0;
    for binding in &mut definition.bindings {
        if matches!(binding.name.as_str(), "scan" | "observation") {
            assert_eq!(binding.unimplemented.take().unwrap().issue, 980);
            cleared += 1;
        }
    }
    assert_eq!(cleared, 2);
    values["scan"] = json!(scan);
    values["observation"] = json!(obs);
    let workspace = std::env::current_dir().unwrap();
    let mut open = casa_task_runtime::OpenSessionRequest::defaults(bundle, &workspace);
    open.managed_save = false;
    open.override_patch.values = values
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| {
            (
                name.clone(),
                casa_provider_contracts::ParameterValue::from_plain_json(value).unwrap(),
            )
        })
        .collect();
    let session = casa_task_runtime::ParameterRuntime::default()
        .open_session(open)
        .unwrap();
    assert!(
        session
            .diagnostics()
            .iter()
            .all(|diagnostic| diagnostic.level != casa_task_runtime::DiagnosticLevel::Error)
    );
    let (parameters, request) = casars_imager::task_request(
        &casa_task_runtime::TaskInvocation::new(&session, &workspace),
    )
    .unwrap();
    assert_eq!(parameters.0["scan"], json!(scan));
    assert_eq!(parameters.0["observation"], json!(obs));
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
        (5, " 2 ", " 0 ", "1"),
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
}

#[test]
fn preview_numeric_selectors_refuse_malformed_text_without_images() {
    let observation = numeric_fixture();
    for (index, scan, obs) in [
        (0, "99", ""),
        (1, ">1", ""),
        (2, "1~1", ""),
        (3, "2~1", ""),
        (4, "", "0~0"),
        (5, "", "1~0"),
        (6, ",1", ""),
        (7, "1,", ""),
        (8, "1,,2", ""),
        (9, ",", ""),
        (10, "+1", ""),
        (11, "  ", ""),
        (12, "none", ""),
        (13, "None", ""),
        (14, "", ",0"),
        (15, "", "0,"),
        (16, "", "0,,1"),
        (17, "", "+0"),
        (18, "", "  "),
        (19, "", "none"),
        (20, "", "None"),
    ] {
        let name = format!("numeric-refused-{index}");
        let (parameters, request) = preview(&observation, &name, scan, obs, true);
        assert!(run(parameters, &request).is_err());
        for suffix in [
            "psf",
            "residual",
            "image",
            "image.pbcor",
            "model",
            "sumwt",
            "pb",
            "weight",
            "mask",
        ] {
            assert!(
                !observation.scratch(&format!("{name}.{suffix}")).exists(),
                "{name}.{suffix}"
            );
        }
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
