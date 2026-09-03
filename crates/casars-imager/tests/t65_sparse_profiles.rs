// SPDX-License-Identifier: LGPL-3.0-or-later

//! T65 contract gate for canonical current-version sparse imager profiles.

use std::{collections::BTreeSet, error::Error, path::PathBuf};

use casa_provider_contracts::{ParameterValue, PersistenceClass, builtin_surface_bundle};
use casa_task_runtime::{
    BaseSource, DiagnosticCode, ParameterSession, ProfileError, parse_profile,
    project_provider_invocation, render_sparse_profile, resolve_profile,
};
use casars_imager::{
    ImagerCubeAxisValue, ImagerCubeInterpolation, ImagerDeconvolver, ImagerPlaneSelection,
    ImagerRestoringBeamMode, ImagerSpectralMode, ImagerTaskRequest, ImagerWeighting,
    imager_provider_invocation,
};

const VLASS_SINGLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/test-profiles/vlass-single-field-awproject.toml"
));
const VLASS_ALL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/test-profiles/vlass-all-fields-awproject.toml"
));
const CONTINUUM: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/test-profiles/imager-standard-continuum.toml"
));
const CUBE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../resources/test-profiles/imager-standard-cube.toml"
));

#[test]
fn current_sparse_profiles_round_trip_through_one_canonical_request() -> Result<(), Box<dyn Error>>
{
    let bundle = builtin_surface_bundle("imager")?;
    assert!(bundle.surface.migrations().is_empty());
    assert!(
        bundle
            .surface
            .bindings()
            .iter()
            .all(|binding| binding.aliases.is_empty())
    );

    for (name, source, measurement_set) in [
        (
            "vlass-single",
            VLASS_SINGLE,
            "VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms",
        ),
        (
            "vlass-all",
            VLASS_ALL,
            "VLASS1.2.sb36484946.eb36542800.58574.4235612037_ptgfix_split_bright_source.ms",
        ),
        (
            "standard-continuum",
            CONTINUUM,
            "representative-continuum.ms",
        ),
        ("standard-cube", CUBE, "representative-cube.ms"),
    ] {
        let parsed = parse_profile(source)?;
        assert_eq!(
            parsed.header.contract,
            bundle.surface.contract_version(),
            "{name}"
        );
        let resolved = resolve_profile(&parsed, &bundle)?;
        assert!(resolved.diagnostics.is_empty(), "{name}");
        assert_eq!(
            resolved.explicit_overrides.keys().collect::<BTreeSet<_>>(),
            parsed.parameters.keys().collect::<BTreeSet<_>>(),
            "{name} must preserve exactly its explicit parameter set"
        );
        assert_eq!(
            render_sparse_profile(&bundle, &resolved.values)?,
            source,
            "{name} fixture must contain only required values and non-default overrides"
        );
        for parameter in parsed.parameters.keys() {
            let binding = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| &binding.name == parameter)
                .ok_or_else(|| format!("missing canonical binding for {name}.{parameter}"))?;
            let concept = bundle
                .catalog
                .concept(&binding.concept)
                .ok_or_else(|| format!("missing concept for {name}.{parameter}"))?;
            assert_eq!(
                concept.persistence_class,
                PersistenceClass::Profile,
                "{name}.{parameter} is not profile-owned"
            );
        }

        let request = request_from_profile(name, source)?;
        assert_eq!(request.measurement_set, PathBuf::from(measurement_set));
        match name {
            "vlass-single" => {
                assert_eq!(request.field_ids.as_deref(), Some(&[1525][..]));
                assert_eq!(request.spw_selector.as_deref(), Some("2~17"));
                assert_eq!(request.w_project_planes, Some(32));
                assert!(request.use_pointing);
                assert_eq!(request.parallel, Some(false));
                assert_eq!(
                    request
                        .aw_project
                        .as_ref()
                        .map(|config| config.cf_cache.as_path()),
                    Some(std::path::Path::new("cf-cache/vlass-spw2-17"))
                );
            }
            "vlass-all" => {
                let fields = request.field_ids.as_deref().expect("expanded FIELD_IDs");
                assert_eq!(fields.len(), 63);
                assert_eq!(fields.first(), Some(&1107));
                assert_eq!(fields.last(), Some(&1562));
                assert!(request.aw_project.is_some());
            }
            "standard-continuum" => {
                assert_eq!(request.image_size, 1024);
                assert_eq!(request.cell_arcsec, 0.25);
                assert_eq!(request.field_ids.as_deref(), Some(&[0, 1, 2][..]));
                assert_eq!(request.correlation, Some(ImagerPlaneSelection::StokesQ));
                assert_eq!(request.deconvolver, ImagerDeconvolver::Multiscale);
                assert_eq!(request.multiscale_scales, vec![0.0, 4.0, 12.0]);
                assert_eq!(request.weighting, ImagerWeighting::Briggs { robust: -0.25 });
                assert!(request.write_pb);
                assert!(request.pbcor);
                assert_eq!(request.parallel, Some(true));
            }
            "standard-cube" => {
                assert_eq!(request.image_size, 768);
                assert_eq!(request.channel_start, Some(10));
                assert_eq!(request.channel_count, Some(24));
                assert_eq!(request.correlation, Some(ImagerPlaneSelection::CorrXX));
                assert_eq!(request.spectral_mode, ImagerSpectralMode::Cube);
                assert_eq!(request.cube_axis.outframe, "BARY");
                assert_eq!(request.cube_axis.veltype, "Z");
                assert_eq!(
                    request.cube_axis.interpolation,
                    ImagerCubeInterpolation::Nearest
                );
                assert_eq!(request.cube_axis.rest_frequency_hz, Some(1.42e9));
                assert_eq!(
                    request.cube_axis.start,
                    Some(ImagerCubeAxisValue::FrequencyHz {
                        hz: 1.1e9,
                        frame: None,
                    })
                );
                assert_eq!(
                    request.cube_axis.width,
                    Some(ImagerCubeAxisValue::Channel { channel: 1 })
                );
                assert_eq!(request.weighting, ImagerWeighting::Uniform);
                assert_eq!(request.restoring_beam_mode, ImagerRestoringBeamMode::Common);
                assert_eq!(request.parallel, Some(false));
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}

#[test]
fn minimal_profile_adds_defaults_but_serializes_only_required_values() -> Result<(), Box<dyn Error>>
{
    let bundle = builtin_surface_bundle("imager")?;
    let source = format!(
        "[casars]\nformat = 1\nsurface = \"imager\"\nkind = \"task\"\ncontract = {}\n\n[parameters]\nvis = [\"minimal.ms\"]\nimagename = \"products/minimal\"\n",
        bundle.surface.contract_version()
    );
    let parsed = parse_profile(&source)?;
    let resolved = resolve_profile(&parsed, &bundle)?;
    assert_eq!(
        resolved
            .explicit_overrides
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["imagename".to_string(), "vis".to_string()])
    );
    assert_eq!(
        resolved.values["imsize"],
        ParameterValue::Array(vec![ParameterValue::Integer(512); 2])
    );
    assert_eq!(
        resolved.values["cell"],
        ParameterValue::Array(vec![ParameterValue::String("1arcsec".into()); 2])
    );
    assert_eq!(
        resolved.values["stokes"],
        ParameterValue::String("I".into())
    );
    assert_eq!(render_sparse_profile(&bundle, &resolved.values)?, source);

    let request = request_from_profile("minimal", &source)?;
    assert_eq!(request.image_size, 512);
    assert_eq!(request.cell_arcsec, 1.0);
    assert_eq!(request.correlation, Some(ImagerPlaneSelection::StokesI));
    assert_eq!(request.spectral_mode, ImagerSpectralMode::Mfs);
    assert_eq!(request.parallel, None);
    Ok(())
}

#[test]
fn imager_profiles_reject_stale_contract_aliases_and_non_profile_authority() {
    let bundle = builtin_surface_bundle("imager").unwrap();
    let current = bundle.surface.contract_version();
    let stale = profile_source(current - 1, "");
    assert_diagnostic(&stale, &bundle, DiagnosticCode::UnsupportedContract);

    let alias = profile_source(current, "polarization = \"Q\"\n");
    assert_diagnostic(&alias, &bundle, DiagnosticCode::UnknownParameter);

    for parameter in [
        "runtime_resource_inventory",
        "prepared_cf_source",
        "provider_executable",
        "publication_root",
    ] {
        let source = profile_source(current, &format!("{parameter} = \"forbidden\"\n"));
        assert_diagnostic(&source, &bundle, DiagnosticCode::UnknownParameter);
    }
}

fn request_from_profile(
    name: &str,
    source: &str,
) -> Result<casars_imager::ImagerRunTaskRequest, Box<dyn Error>> {
    let bundle = builtin_surface_bundle("imager")?;
    let profile = parse_profile(source)?;
    let session = ParameterSession::from_profile(
        bundle,
        BaseSource::File(PathBuf::from(format!("{name}.toml"))),
        &profile,
    )?;
    let invocation = project_provider_invocation(&session, |family, values, direct| {
        assert_eq!(family, "imager");
        imager_provider_invocation(values, direct.args)
    })?;
    let request: ImagerTaskRequest = serde_json::from_str(
        invocation
            .stdin
            .as_deref()
            .ok_or("missing canonical provider request")?,
    )?;
    let ImagerTaskRequest::Run(request) = request;
    Ok(request)
}

fn profile_source(contract: u32, extra: &str) -> String {
    format!(
        "[casars]\nformat = 1\nsurface = \"imager\"\nkind = \"task\"\ncontract = {contract}\n\n[parameters]\nvis = [\"example.ms\"]\nimagename = \"products/example\"\n{extra}"
    )
}

fn assert_diagnostic(
    source: &str,
    bundle: &casa_provider_contracts::SurfaceContractBundle,
    expected: DiagnosticCode,
) {
    let profile = parse_profile(source).unwrap();
    let ProfileError::Diagnostics(diagnostics) = resolve_profile(&profile, bundle).unwrap_err()
    else {
        panic!("expected profile diagnostic")
    };
    assert_eq!(diagnostics.len(), 1);
    assert_eq!(diagnostics[0].code, expected);
}
