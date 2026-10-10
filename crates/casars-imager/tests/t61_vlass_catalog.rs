// SPDX-License-Identifier: LGPL-3.0-or-later

//! T61 real-data gate for the catalog-to-request seam: the VLASS AW
//! projection controls reach the typed request through the provider
//! invocation, and a standard request reaches the real snapshot.

use std::{collections::BTreeMap, error::Error, fs, path::Path};

use casa_imaging_application::{AwCfSource, AwProjection, Gridder, ProductNormalization};
use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{
    OpenSessionRequest, ParameterRuntime, ResolutionPatch, project_provider_invocation,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casars_imager::{ImagerTaskRequest, ImagerTaskResult, imager_provider_invocation};

const DATASET: &str = "measurementset/vla/ref_vlass_wtsp_creation.ms";
const FIXTURE_SPW_SELECTOR: &str = "0:0~15";

#[test]
#[ignore = "requires slow-parity casatestdata"]
fn t61_vlass_controls_reach_the_request_and_the_real_snapshot() -> Result<(), Box<dyn Error>> {
    let measurement_set = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    if !measurement_set.is_dir() {
        return Err(format!(
            "VLASS MeasurementSet is missing at {}",
            measurement_set.display()
        )
        .into());
    }
    let staging = tempfile::tempdir()?;
    let staged_measurement_set = staging.path().join("ref_vlass_wtsp_creation.ms");
    copy_tree(&measurement_set, &staged_measurement_set)?;
    let output = tempfile::tempdir()?;
    let image_name = output.path().join("vlass-t61");

    let overrides = BTreeMap::from([
        (
            "vis".into(),
            ParameterValue::String(staged_measurement_set.display().to_string()),
        ),
        (
            "imagename".into(),
            ParameterValue::String(image_name.display().to_string()),
        ),
        (
            "imsize".into(),
            ParameterValue::Array(vec![ParameterValue::Integer(12_150); 2]),
        ),
        (
            "cell".into(),
            ParameterValue::Array(vec![ParameterValue::String("2.5arcsec".into()); 2]),
        ),
        ("field".into(), ParameterValue::String("0".into())),
        (
            "spw".into(),
            ParameterValue::String(FIXTURE_SPW_SELECTOR.into()),
        ),
        ("uvrange".into(), ParameterValue::String("<12km".into())),
        ("intent".into(), ParameterValue::String("*TARGET*".into())),
        ("stokes".into(), ParameterValue::String("I".into())),
        ("specmode".into(), ParameterValue::String("mfs".into())),
        ("deconvolver".into(), ParameterValue::String("mtmfs".into())),
        ("nterms".into(), ParameterValue::Integer(2)),
        ("gridder".into(), ParameterValue::String("awproject".into())),
        ("wprojplanes".into(), ParameterValue::Integer(32)),
        ("usepointing".into(), ParameterValue::Bool(true)),
        (
            "cfcache".into(),
            ParameterValue::String("cf-cache/vlass-spw2-17".into()),
        ),
        ("cf_resident_mb".into(), ParameterValue::Integer(384)),
        (
            "pointingoffsetsigdev".into(),
            ParameterValue::String("0.0".into()),
        ),
        (
            "normtype".into(),
            ParameterValue::String("flatnoise".into()),
        ),
        ("parallel".into(), ParameterValue::Bool(false)),
    ]);
    let bundle = builtin_surface_bundle("imager")?;
    let mut open = OpenSessionRequest::defaults(bundle.clone(), staging.path());
    open.override_patch = ResolutionPatch {
        values: overrides,
        unset: Default::default(),
    };
    let session = ParameterRuntime::default().open_session(open)?;
    let invocation = project_provider_invocation(&session, |_family, values, direct| {
        imager_provider_invocation(values, direct.args)
    })?;
    let ImagerTaskRequest::Run(parameters) = serde_json::from_str(
        invocation
            .stdin
            .as_deref()
            .ok_or("missing provider request")?,
    )?;
    let (_, request) = casars_imager::resolve_request(&parameters)?;
    request.validate()?;
    assert_eq!(request.imsize, 12_150);
    assert_eq!(request.spw.as_deref(), Some(FIXTURE_SPW_SELECTOR));
    assert!(!request.parallel);
    let Gridder::Awproject(AwProjection {
        wprojplanes,
        usepointing,
        normtype,
        cf_resident_mb,
        pointingoffsetsigdev,
        cf_source: AwCfSource::CasaImport { cfcache },
    }) = &request.gridder
    else {
        panic!("the VLASS controls make an imported-cache AW request: {request:?}");
    };
    assert_eq!(wprojplanes.map(usize::from), Some(32));
    assert!(*usepointing);
    assert_eq!(*normtype, ProductNormalization::FlatNoise);
    assert_eq!(*cf_resident_mb, 384);
    assert_eq!(pointingoffsetsigdev, &[0.0]);
    assert!(cfcache.ends_with("cf-cache/vlass-spw2-17"));

    let supported_image_name = output.path().join("vlass-t61-standard");
    let supported_overrides = BTreeMap::from([
        (
            "vis".into(),
            ParameterValue::String(staged_measurement_set.display().to_string()),
        ),
        (
            "imagename".into(),
            ParameterValue::String(supported_image_name.display().to_string()),
        ),
        (
            "imsize".into(),
            ParameterValue::Array(vec![ParameterValue::Integer(1024); 2]),
        ),
        (
            "cell".into(),
            ParameterValue::Array(vec![ParameterValue::String("2.5arcsec".into()); 2]),
        ),
        ("field".into(), ParameterValue::String("0".into())),
        (
            "spw".into(),
            ParameterValue::String(FIXTURE_SPW_SELECTOR.into()),
        ),
        ("uvrange".into(), ParameterValue::String("<12km".into())),
        ("intent".into(), ParameterValue::String("*TARGET*".into())),
        ("stokes".into(), ParameterValue::String("I".into())),
        ("specmode".into(), ParameterValue::String("mfs".into())),
        (
            "deconvolver".into(),
            ParameterValue::String("hogbom".into()),
        ),
        ("nterms".into(), ParameterValue::Integer(1)),
        ("gridder".into(), ParameterValue::String("standard".into())),
        ("niter".into(), ParameterValue::Integer(0)),
        ("parallel".into(), ParameterValue::Bool(false)),
    ]);
    let mut supported_open = OpenSessionRequest::defaults(bundle, staging.path());
    supported_open.override_patch = ResolutionPatch {
        values: supported_overrides,
        unset: Default::default(),
    };
    let supported_session = ParameterRuntime::default().open_session(supported_open)?;
    let supported_invocation =
        project_provider_invocation(&supported_session, |_family, values, direct| {
            imager_provider_invocation(values, direct.args)
        })?;
    let supported_request: ImagerTaskRequest = serde_json::from_str(
        supported_invocation
            .stdin
            .as_deref()
            .ok_or("missing supported provider request")?,
    )?;
    let ImagerTaskResult::Run(result) = supported_request.execute()?;
    assert_eq!(result.request.0["imsize"], serde_json::json!([1024, 1024]));
    assert_eq!(result.request.0["spw"], FIXTURE_SPW_SELECTOR);
    assert!(result.run.gridded_samples > 0);
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source = entry.path();
        let destination = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_tree(&source, &destination)?;
        } else if file_type.is_file() {
            fs::copy(source, destination)?;
        } else if file_type.is_symlink() {
            let target = fs::canonicalize(source)?;
            if target.is_dir() {
                copy_tree(&target, &destination)?;
            } else {
                fs::copy(target, destination)?;
            }
        }
    }
    Ok(())
}
