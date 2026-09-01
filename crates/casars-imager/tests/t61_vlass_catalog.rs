// SPDX-License-Identifier: LGPL-3.0-or-later

//! T61 real-data gate for the catalog-to-request availability seam.

use std::{collections::BTreeMap, error::Error, fs, path::Path};

use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};
use casa_task_runtime::{
    OpenSessionRequest, ParameterRuntime, ResolutionPatch, project_provider_invocation,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casars_imager::{ImagerTaskRequest, imager_provider_invocation};

const DATASET: &str = "measurementset/vla/ref_vlass_wtsp_creation.ms";
const FIXTURE_SPW_SELECTOR: &str = "0:0~15";

#[test]
#[ignore = "requires slow-parity casatestdata"]
fn t61_vlass_controls_reach_the_real_snapshot_and_exact_typed_unavailability()
-> Result<(), Box<dyn Error>> {
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
    casa_ms::initialize_measurement_set_owner_manifest(&staged_measurement_set)?;
    let output = tempfile::tempdir()?;
    let image_name = output.path().join("vlass-t61");
    unsafe {
        std::env::set_var("CASA_RS_IMAGING_SPILL_READ_BYTES_PER_SECOND", "1000000000");
        std::env::set_var("CASA_RS_IMAGING_SPILL_WRITE_BYTES_PER_SECOND", "1000000000");
    }

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
        ("aterm".into(), ParameterValue::Bool(true)),
        ("psterm".into(), ParameterValue::Bool(false)),
        ("wbawp".into(), ParameterValue::Bool(true)),
        ("conjbeams".into(), ParameterValue::Bool(true)),
        ("computepastep".into(), ParameterValue::Float(360.0)),
        ("rotatepastep".into(), ParameterValue::Float(360.0)),
        (
            "pointingoffsetsigdev".into(),
            ParameterValue::String("0.0".into()),
        ),
        ("mosweight".into(), ParameterValue::Bool(false)),
        (
            "normtype".into(),
            ParameterValue::String("flatnoise".into()),
        ),
        ("parallel".into(), ParameterValue::Bool(false)),
        ("write_preview_pngs".into(), ParameterValue::Bool(false)),
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
    let ImagerTaskRequest::Run(request) = serde_json::from_str(
        invocation
            .stdin
            .as_deref()
            .ok_or("missing typed provider request")?,
    )?;
    assert_eq!(request.image_size, 12_150);
    assert_eq!(request.spw_selector.as_deref(), Some(FIXTURE_SPW_SELECTOR));
    assert_eq!(request.parallel, Some(false));
    assert!(request.use_pointing);
    assert_eq!(request.w_project_planes, Some(32));
    let aw = request.aw_project.as_ref().expect("AWProject controls");
    assert_eq!(aw.cf_resident_mb, 384);
    assert!(!aw.ps_term);
    assert!(aw.wb_awp);
    assert!(aw.conjugate_beams);

    let error = request
        .execute()
        .expect_err("AWProject must fail at the typed installed-capability boundary");
    assert_eq!(
        error,
        "imaging request requires unsupported installed-implementation contract items: \
[Task(AwProjection), Task(WProjectionPlanes)]"
    );

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
        ("write_preview_pngs".into(), ParameterValue::Bool(false)),
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
    let ImagerTaskRequest::Run(supported_request) = serde_json::from_str(
        supported_invocation
            .stdin
            .as_deref()
            .ok_or("missing supported typed provider request")?,
    )?;
    let summary = supported_request.execute()?;
    assert_eq!(summary.request.image_size, 1024);
    assert_eq!(
        summary.request.spw_selector.as_deref(),
        Some(FIXTURE_SPW_SELECTOR)
    );
    assert!(summary.run.gridded_samples > 0);
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
