// SPDX-License-Identifier: LGPL-3.0-or-later

//! Repository-level inventory proving every built-in provider bundle uses the
//! shared envelope validator. Keep this list explicit so adding a provider
//! without conformance evidence fails review visibly.

#[test]
fn every_builtin_provider_validates_through_the_shared_envelope() {
    assert_task_provider(
        "calibration",
        casa_calibration::calibration_task_schema_bundle(),
    );
    assert_task_provider("importvla", casa_vla::importvla_task_schema_bundle());
    assert_task_provider(
        "msexplore",
        casa_ms::msexplore::task_contract::msexplore_task_schema_bundle(),
    );
    assert_task_provider(
        "simobserve",
        casars_simulation::simobserve_task_schema_bundle(),
    );
    assert_task_provider("mstransform", casa_ms::mstransform_task_schema_bundle());
    assert_task_provider("flagdata", casa_ms::flagdata_task_schema_bundle());
    assert_task_provider("flagmanager", casa_ms::flagmanager_task_schema_bundle());
    assert_task_provider(
        "msconvert",
        casars_msconvert::msconvert_task_schema_bundle(),
    );
    assert_task_provider(
        "image-analysis",
        casa_images::analysis::image_analysis_task_schema_bundle(),
    );
    assert_task_provider("imager", casars_imager::imager_task_schema_bundle());
}

fn assert_task_provider<E>(
    name: &str,
    mut bundle: casa_provider_contracts::TaskProviderContract<E>,
) {
    bundle
        .validate()
        .unwrap_or_else(|errors| panic!("{name} provider contract: {errors:?}"));

    let protocol_version = std::mem::replace(&mut bundle.protocol.protocol_version, 0);
    assert_rejected(name, &bundle.validate(), "invalid_protocol_version");
    bundle.protocol.protocol_version = protocol_version;

    let actions = &mut bundle.projections.cli.as_mut().unwrap().machine_actions;
    let json_run = actions.json_run.replace("--wrong-run".to_string());
    assert_rejected(name, &bundle.validate(), "task_actions");
    bundle
        .projections
        .cli
        .as_mut()
        .unwrap()
        .machine_actions
        .json_run = json_run;

    let casa_provider_contracts::SurfaceDefinition::Task(definition) =
        &mut bundle.parameter_surfaces[0].surface;
    let surface_id = std::mem::take(&mut definition.id);
    assert_rejected(name, &bundle.validate(), "parameter_surface");
    let casa_provider_contracts::SurfaceDefinition::Task(definition) =
        &mut bundle.parameter_surfaces[0].surface;
    definition.id = surface_id;

    bundle.domain_schemas.request_schema = schemars::schema_for!(bool);
    assert_rejected(name, &bundle.validate(), "request_schema_mismatch");
}

fn assert_rejected(
    name: &str,
    result: &Result<(), Vec<casa_provider_contracts::ProviderContractValidationError>>,
    code: &str,
) {
    assert!(
        result
            .as_ref()
            .is_err_and(|errors| errors.iter().any(|error| error.code == code)),
        "{name} did not reject {code}: {result:?}"
    );
}
