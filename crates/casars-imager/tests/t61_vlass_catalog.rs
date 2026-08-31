// SPDX-License-Identifier: LGPL-3.0-or-later

//! T61 real-data gate for the catalog-to-request availability seam.

use std::{error::Error, ffi::OsString, fs, path::Path};

use casa_imaging_application::{
    ApplicationDispatchError, TaskRequirement, UnsupportedRequirement, execute_continuum,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use casars_imager::{CliConfig, ImagerRunTaskRequest, project_application_request};

const DATASET: &str = "measurementset/vla/ref_vlass_wtsp_creation.ms";

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

    let config = CliConfig::parse(
        [
            "--ms".to_string(),
            staged_measurement_set.display().to_string(),
            "--imagename".to_string(),
            image_name.display().to_string(),
            "--imsize".to_string(),
            "128".to_string(),
            "--cell-arcsec".to_string(),
            "2.5".to_string(),
            "--field".to_string(),
            "0".to_string(),
            "--spw".to_string(),
            "0:0~15".to_string(),
            "--uvrange".to_string(),
            "<12km".to_string(),
            "--intent".to_string(),
            "*TARGET*".to_string(),
            "--stokes".to_string(),
            "I".to_string(),
            "--specmode".to_string(),
            "mfs".to_string(),
            "--deconvolver".to_string(),
            "mtmfs".to_string(),
            "--nterms".to_string(),
            "2".to_string(),
            "--gridder".to_string(),
            "awproject".to_string(),
            "--wprojplanes".to_string(),
            "32".to_string(),
            "--usepointing".to_string(),
            "--cfcache".to_string(),
            "cf-cache/vlass-spw2-17".to_string(),
            "--cf-resident-mb".to_string(),
            "384".to_string(),
            "--aterm".to_string(),
            "--no-psterm".to_string(),
            "--wbawp".to_string(),
            "--conjbeams".to_string(),
            "--computepastep".to_string(),
            "360".to_string(),
            "--rotatepastep".to_string(),
            "360".to_string(),
            "--pointingoffsetsigdev".to_string(),
            "0.0".to_string(),
            "--no-mosweight".to_string(),
            "--normtype".to_string(),
            "flatnoise".to_string(),
            "--no-parallel".to_string(),
        ]
        .into_iter()
        .map(OsString::from),
    )?;
    let request = ImagerRunTaskRequest::from_cli_config(&config);
    let encoded = serde_json::to_string(&request)?;
    let request: ImagerRunTaskRequest = serde_json::from_str(&encoded)?;
    assert_eq!(request.parallel, Some(false));
    assert!(request.use_pointing);
    assert_eq!(request.w_project_planes, Some(32));
    let aw = request.aw_project.as_ref().expect("AWProject controls");
    assert_eq!(aw.cf_resident_mb, 384);
    assert!(!aw.ps_term);
    assert!(aw.wb_awp);
    assert!(aw.conjugate_beams);

    let application = project_application_request(&request)?;
    assert!(
        application
            .task_requirements
            .contains(&TaskRequirement::AwProjection)
    );
    let error = match execute_continuum(application) {
        Ok(_) => {
            return Err(
                "AWProject unexpectedly executed despite installed-catalog unavailability".into(),
            );
        }
        Err(error) => error,
    };
    let ApplicationDispatchError::Unavailable(unavailable) = error else {
        return Err(format!(
            "expected typed unavailability after real snapshot resolution, got {error}"
        )
        .into());
    };
    assert!(
        unavailable
            .unsupported()
            .contains(&UnsupportedRequirement::Task(TaskRequirement::AwProjection,))
    );
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
