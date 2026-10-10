// SPDX-License-Identifier: LGPL-3.0-or-later

//! T65 contract gate for canonical current-version sparse imager profiles.

use std::{collections::BTreeSet, error::Error, path::PathBuf};

use casa_imaging_application::{
    AwCfSource, AwProjection, Deconvolver, Gridder, ImagingRequest, PolarizationCoordinate,
    RestoringBeamPolicy, SpecMode, Weighting,
};
use casa_ms::CubeInterpolation;
use casa_provider_contracts::{ParameterValue, PersistenceClass, builtin_surface_bundle};
use casa_task_runtime::{
    BaseSource, DiagnosticCode, ParameterSession, ProfileError, parse_profile,
    project_provider_invocation, render_sparse_profile, resolve_profile,
};
use casa_types::measures::{doppler::DopplerRef, frequency::FrequencyRef};
use casars_imager::{ImagerTaskRequest, imager_provider_invocation, resolve_request};

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
        request.validate()?;
        assert_eq!(request.vis, PathBuf::from(measurement_set));
        match name {
            "vlass-single" => {
                assert_eq!(request.field.as_deref(), Some(&[1525][..]));
                assert_eq!(request.spw.as_deref(), Some("2~17"));
                assert!(!request.parallel);
                let Gridder::Awproject(AwProjection {
                    wprojplanes,
                    usepointing,
                    cf_source: AwCfSource::CasaImport { cfcache },
                    ..
                }) = &request.gridder
                else {
                    panic!("an imported-cache AW request: {request:?}");
                };
                assert_eq!(wprojplanes.map(usize::from), Some(32));
                assert!(*usepointing);
                assert_eq!(cfcache, &PathBuf::from("cf-cache/vlass-spw2-17"));
            }
            "vlass-all" => {
                let fields = request.field.as_deref().expect("expanded FIELD_IDs");
                assert_eq!(fields.len(), 63);
                assert_eq!(fields.first(), Some(&1107));
                assert_eq!(fields.last(), Some(&1562));
                assert!(matches!(request.gridder, Gridder::Awproject(_)));
            }
            "standard-continuum" => {
                assert_eq!(request.imsize, 1024);
                assert_eq!(request.cell, 0.25);
                assert_eq!(request.field.as_deref(), Some(&[0, 1, 2][..]));
                assert_eq!(request.stokes, [PolarizationCoordinate::StokesQ]);
                assert_eq!(request.deconvolver, Deconvolver::Multiscale);
                assert_eq!(request.scales, [0.0, 4.0, 12.0]);
                assert_eq!(request.weighting, Weighting::Briggs);
                assert_eq!(request.robust, -0.25);
                assert!(request.write_pb);
                assert!(request.pbcor);
                assert!(request.parallel);
            }
            "standard-cube" => {
                assert_eq!(request.imsize, 768);
                assert_eq!(request.channel_start, Some(10));
                assert_eq!(request.channel_count, Some(24));
                assert_eq!(request.stokes, [PolarizationCoordinate::LinearXx]);
                assert_eq!(request.specmode, SpecMode::Cube);
                assert_eq!(request.outframe, FrequencyRef::BARY);
                assert_eq!(request.veltype, DopplerRef::Z);
                assert_eq!(request.interpolation, CubeInterpolation::Nearest);
                assert_eq!(request.restfreq, Some(1.42e9));
                assert_eq!(request.start.as_deref(), Some("1.1GHz"));
                assert_eq!(request.width.as_deref(), Some("1"));
                assert_eq!(request.weighting, Weighting::Uniform);
                assert_eq!(request.restoringbeam, RestoringBeamPolicy::Common);
                assert!(!request.parallel);
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
    assert_eq!(request.imsize, 512);
    assert_eq!(request.cell, 1.0);
    assert_eq!(request.stokes, [PolarizationCoordinate::StokesI]);
    assert_eq!(request.specmode, SpecMode::Mfs);
    assert!(!request.parallel);
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

/// The request the profile `source` projects through the provider
/// invocation and the imager resolves.
fn request_from_profile(name: &str, source: &str) -> Result<ImagingRequest, Box<dyn Error>> {
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
    let ImagerTaskRequest::Run(parameters) = request;
    Ok(resolve_request(&parameters)?.1)
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
