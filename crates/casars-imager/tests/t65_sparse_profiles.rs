// SPDX-License-Identifier: LGPL-3.0-or-later

//! T65 contract gate for canonical current-version sparse tclean profiles.

use std::{collections::BTreeSet, error::Error, path::PathBuf};

use casa_imaging_application::{
    AwCfSource, AwProjection, Deconvolver, Gridder, ImagingRequest, PolarizationCoordinate,
    RestoringBeamPolicy, SpecMode, Weighting,
};
use casa_ms::CubeInterpolation;
use casa_provider_contracts::{
    ParameterValue, PersistenceClass, TaskRunRequest, builtin_surface_bundle,
};
use casa_task_runtime::{
    BaseSource, DiagnosticCode, ParameterSession, ProfileError, parse_profile,
    project_provider_invocation, render_sparse_profile, resolve_profile,
};
use casa_types::measures::{doppler::DopplerRef, frequency::FrequencyRef};
use casars_imager::{ImagingParameters, resolve_request};

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
    let bundle = builtin_surface_bundle("tclean")?;
    // The imager's contract-28 profiles migrate; none older is read.
    assert_eq!(bundle.surface.migrations().len(), 4);
    assert_eq!(bundle.surface.migrations()[0].from_contract, 28);
    assert_eq!(bundle.surface.migrations()[1].from_contract, 29);
    assert_eq!(bundle.surface.migrations()[2].from_contract, 30);
    assert_eq!(bundle.surface.migrations()[3].from_contract, 31);
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
                assert_eq!(request.field.as_deref(), Some("1525"));
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
                assert_eq!(
                    request.field.as_deref(),
                    Some("1107~1127,1512~1532,1542~1562")
                );
                assert!(matches!(request.gridder, Gridder::Awproject(_)));
            }
            "standard-continuum" => {
                assert_eq!(request.imsize, [1024; 2]);
                assert_eq!(request.cell, [0.25; 2]);
                assert_eq!(request.field.as_deref(), Some("0~2"));
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
                assert_eq!(request.imsize, [768; 2]);
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
    let bundle = builtin_surface_bundle("tclean")?;
    // CASA's tclean needs a cell; everything else takes its default.
    let source = format!(
        "[casars]\nformat = 1\nsurface = \"tclean\"\nkind = \"task\"\ncontract = {}\n\n[parameters]\nvis = [\"minimal.ms\"]\nimagename = \"products/minimal\"\ncell = [\"1arcsec\"]\n",
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
        BTreeSet::from([
            "cell".to_string(),
            "imagename".to_string(),
            "vis".to_string()
        ])
    );
    assert_eq!(
        resolved.values["imsize"],
        ParameterValue::Array(vec![ParameterValue::Integer(100)])
    );
    assert_eq!(
        resolved.values["stokes"],
        ParameterValue::String("I".into())
    );
    assert_eq!(render_sparse_profile(&bundle, &resolved.values)?, source);

    let request = request_from_profile("minimal", &source)?;
    assert_eq!(request.imsize, [100; 2]);
    assert_eq!(request.cell, [1.0; 2]);
    assert_eq!(request.stokes, [PolarizationCoordinate::StokesI]);
    assert_eq!(request.specmode, SpecMode::Mfs);
    assert!(!request.parallel);
    Ok(())
}

#[test]
fn imager_profiles_reject_stale_contract_aliases_and_non_profile_authority() {
    let bundle = builtin_surface_bundle("tclean").unwrap();
    let current = bundle.surface.contract_version();
    // Contract 28, the imager's last, migrates; older ones are refused.
    let stale = profile_source(27, "");
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
    let bundle = builtin_surface_bundle("tclean")?;
    let profile = parse_profile(source)?;
    let session = ParameterSession::from_profile(
        bundle,
        BaseSource::File(PathBuf::from(format!("{name}.toml"))),
        &profile,
    )?;
    let invocation = project_provider_invocation(&session)?;
    let request: TaskRunRequest = serde_json::from_str(
        invocation
            .stdin
            .as_deref()
            .ok_or("missing canonical task request")?,
    )?;
    assert_eq!(request.surface, "tclean");
    Ok(resolve_request(&ImagingParameters(request.values))?.1)
}

fn profile_source(contract: u32, extra: &str) -> String {
    format!(
        "[casars]\nformat = 1\nsurface = \"tclean\"\nkind = \"task\"\ncontract = {contract}\n\n[parameters]\nvis = [\"example.ms\"]\nimagename = \"products/example\"\n{extra}"
    )
}

/// An imager profile of contract 28 runs as tclean: its casa-rs names move
/// to CASA's, its forms to CASA's, and an omitted changed default is named.
#[test]
fn an_imager_profile_migrates_to_tclean() -> Result<(), Box<dyn Error>> {
    let source = "[casars]\nformat = 1\nsurface = \"imager\"\nkind = \"task\"\ncontract = 28\n\n\
        [parameters]\nvis = [\"old.ms\"]\nimagename = \"products/old\"\ncell = [\"2arcsec\"]\n\
        imsize = [64]\nphasecenter_field = \"3\"\nmask_box = \"1,2,10,12\"\n\
        minor_cycle_length = 7\ndirty_only = true\nniter = 100\nveltype = \"OPTICAL\"\n\
        scales = \"0,4\"\nrestoringbeam = \"common\"\ndatacolumn = \"none\"\n\
        deconvolver = \"multiscale\"\n";
    let bundle = builtin_surface_bundle("imager")?;
    let parsed = parse_profile(source)?;
    let resolved = resolve_profile(&parsed, &bundle)?;
    let value = |name: &str| resolved.values[name].clone();
    assert_eq!(value("phasecenter"), ParameterValue::String("3".into()));
    assert_eq!(value("cycleniter"), ParameterValue::Integer(7));
    // dirty_only=true was a dirty run: CASA's niter=0.
    assert_eq!(value("niter"), ParameterValue::Integer(0));
    assert_eq!(value("veltype"), ParameterValue::String("optical".into()));
    assert_eq!(
        value("scales"),
        ParameterValue::Array(vec![ParameterValue::Float(0.0), ParameterValue::Float(4.0)])
    );
    assert_eq!(
        value("datacolumn"),
        ParameterValue::String("corrected".into())
    );
    // Of the changed defaults, the profile set imsize and cell, and
    // cycleniter by its old name: only the others are warned.
    let warned = resolved
        .diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.code == DiagnosticCode::DefaultChanged)
        .filter_map(|diagnostic| diagnostic.parameter.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        warned,
        [
            "nterms",
            "perchanweightdensity",
            "write_pb",
            "hogbom_iteration_mode"
        ]
    );
    let request = request_from_profile("old", source)?;
    assert_eq!(request.phasecenter_field, Some(3));
    assert_eq!(request.mask_box, [[1, 2, 10, 12]]);
    // The profile keeps its cycleniter of 7, which CASA's tclean takes as
    // niter when it is more: a dirty run's request holds one.
    assert_eq!(request.minor_cycle_length, 1);
    assert_eq!(request.niter, 0);
    assert_eq!(request.scales, [0.0, 4.0]);
    assert_eq!(request.restoringbeam, RestoringBeamPolicy::Common);

    // An old explicit "none" beside the value renamed onto it is the
    // absence it was, not a second value.
    let source = "[casars]\nformat = 1\nsurface = \"imager\"\nkind = \"task\"\ncontract = 28\n\n\
        [parameters]\nvis = [\"old.ms\"]\nimagename = \"products/old\"\ncell = [\"2arcsec\"]\n\
        phasecenter_field = \"3\"\nphasecenter = \"none\"\nmask_box = \"1,2,10,12\"\n\
        mask_image = \"none\"\n";
    let resolved = resolve_profile(&parse_profile(source)?, &bundle)?;
    assert_eq!(
        resolved.values["phasecenter"],
        ParameterValue::String("3".into())
    );
    let request = request_from_profile("none", source)?;
    assert_eq!(
        (request.phasecenter_field, request.mask_box),
        (Some(3), vec![[1, 2, 10, 12]])
    );
    Ok(())
}

/// An imager profile that left `wprojplanes` out had its planes derived,
/// with W or AW projection; tclean's default is one plane, so the
/// migration keeps the derivation (CASA's -1). `perchanweightdensity`
/// left out now takes tclean's `True`, with the changed-default warning.
#[test]
fn an_imager_profile_keeps_its_derived_w_planes() -> Result<(), Box<dyn Error>> {
    for (gridder, extra) in [("wproject", ""), ("awproject", "cfcache = \"cf-cache\"\n")] {
        let source = format!(
            "[casars]\nformat = 1\nsurface = \"imager\"\nkind = \"task\"\ncontract = 28\n\n\
             [parameters]\nvis = [\"old.ms\"]\nimagename = \"products/old\"\n\
             gridder = \"{gridder}\"\n{extra}"
        );
        let bundle = builtin_surface_bundle("tclean")?;
        let resolved = resolve_profile(&parse_profile(&source)?, &bundle)?;
        assert_eq!(
            resolved.values["wprojplanes"],
            ParameterValue::Integer(-1),
            "{gridder}"
        );
        assert!(
            resolved.diagnostics.iter().any(|diagnostic| diagnostic.code
                == DiagnosticCode::DefaultChanged
                && diagnostic.parameter.as_deref() == Some("perchanweightdensity")),
            "{gridder}"
        );
    }
    // A profile that named its planes keeps them.
    let source = "[casars]\nformat = 1\nsurface = \"imager\"\nkind = \"task\"\ncontract = 28\n\n\
        [parameters]\nvis = [\"old.ms\"]\nimagename = \"products/old\"\ngridder = \"wproject\"\n\
        wprojplanes = 16\n";
    let resolved = resolve_profile(&parse_profile(source)?, &builtin_surface_bundle("tclean")?)?;
    assert_eq!(resolved.values["wprojplanes"], ParameterValue::Integer(16));
    Ok(())
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
