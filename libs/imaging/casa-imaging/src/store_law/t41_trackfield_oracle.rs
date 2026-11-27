// SPDX-License-Identifier: LGPL-3.0-or-later

//! Moving-field TrackField phase-centre check against frozen CASA 6.7.6-14 oracle.

use std::{error::Error, fs, path::Path};

use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use serde::Deserialize;
use serde_json::json;

use super::row_pointing;
use crate::counting_heap::{context, request as base_request};

const DATASET: &str = "measurementset/alma/alma_ephemobj_icrs.ms";
const TRACKFIELD_ORACLE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/t41_trackfield_casa_6_7_6_14.json"
));
const MAX_CASA_DIRECTION_SEPARATION_RAD: f64 = 2.0e-12;

#[derive(Deserialize)]
struct DirectionOracle {
    casa_version: String,
    samples: Vec<DirectionOracleSample>,
}

#[derive(Deserialize)]
struct DirectionOracleSample {
    label: String,
    physical_row: u64,
    time_mjd_days: f64,
    j2000_longitude_rad: f64,
    j2000_latitude_rad: f64,
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let target = destination.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

#[test]
#[ignore = "requires the slow-parity ALMA ephemeris MeasurementSet"]
fn t41_trackfield_phase_centre_matches_casa_at_three_row_times() -> Result<(), Box<dyn Error>> {
    let oracle: DirectionOracle = serde_json::from_str(TRACKFIELD_ORACLE)?;
    assert_eq!(oracle.casa_version, "6.7.6-14");
    assert_eq!(oracle.samples.len(), 3);
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("slow-parity casatestdata root is unavailable")?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    copy_tree(&source, &measurement_set)?;

    let request = base_request(json!({
        "vis": measurement_set,
        "imagename": staging.path().join("trackfield"),
        "imsize": 64,
        "cell": "0.1arcsec",
        "phasecenter": "TRACKFIELD",
        "field": "1",
        "spw": "0",
        "datacolumn": "DATA",
        "specmode": "mfs",
        "niter": 0,
    }));
    let rows = row_pointing(&request, &context(request.resource_policy()))
        .map_err(|error| error as Box<dyn Error>)?;
    assert_eq!(rows.len(), 1_620, "field 1's rows of spectral window 0");
    for expected in &oracle.samples {
        let (time_mjd_days, direction) = rows
            .get(&expected.physical_row)
            .ok_or("the batches omitted an oracle row")?;
        let longitude_delta = direction.longitude_rad() - expected.j2000_longitude_rad;
        let latitude_delta = direction.latitude_rad() - expected.j2000_latitude_rad;
        let separation =
            (longitude_delta * expected.j2000_latitude_rad.cos()).hypot(latitude_delta);
        assert!(
            (time_mjd_days - expected.time_mjd_days).abs() <= 1.0e-12,
            "{} row time differs: Rust {time_mjd_days:.17} CASA {:.17}",
            expected.label,
            expected.time_mjd_days,
        );
        assert!(
            separation <= MAX_CASA_DIRECTION_SEPARATION_RAD,
            "{} phase-centre separation {separation:.3e} rad exceeds the CASA oracle's \
             {MAX_CASA_DIRECTION_SEPARATION_RAD:.3e} rad",
            expected.label,
        );
    }
    Ok(())
}
