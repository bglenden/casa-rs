// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::{
    SelectedObservationEphemeris, SelectedObservationResolutionRequest,
    resolve_selected_observation,
};
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use serde::Deserialize;
use std::{collections::BTreeMap, error::Error, fs, path::Path};

const DATASET: &str = "measurementset/alma/alma_ephemobj_icrs.ms";
const MVC_MS_ENV: &str = "CASA_RS_T41_MVC_MS";
const ORACLE: &str = include_str!("../../../tests/fixtures/t41_trackfield_casa_6_7_6_14.json");
const FIELD_ID: u32 = 1;
const DATA_DESCRIPTION_ID: u32 = 0;
const SPECTRAL_WINDOW_ID: u32 = 0;
const POLARIZATION_ID: u32 = 0;
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

#[test]
#[ignore = "requires the slow-parity ALMA ephemeris MeasurementSet"]
fn t41_trackfield_phase_centre_matches_casa_at_three_row_times() -> Result<(), Box<dyn Error>> {
    let oracle: DirectionOracle = serde_json::from_str(ORACLE)?;
    assert_eq!(oracle.casa_version, "6.7.6-14");
    assert_eq!(oracle.samples.len(), 3);

    let source = std::env::var_os(MVC_MS_ENV)
        .map(std::path::PathBuf::from)
        .or_else(|| casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET))
        .ok_or("T41 MVC MeasurementSet is unavailable")?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    MeasurementSet::open(&source)?.save_as(&measurement_set)?;
    copy_attached_ephemerides(&source, &measurement_set)?;

    let ms = MeasurementSet::open(&measurement_set)?;
    let row_count = ms.row_count();
    let channel_frequency_hz = ms.spectral_window()?.chan_freq(0)?[0];
    let channel_width_hz = ms.spectral_window()?.chan_width(0)?[0];
    // The traversal replays every MAIN row the field and data-description
    // predicate admits, so the manifest lists exactly those rows.
    let mut selected_rows = Vec::new();
    for row in 0..row_count {
        if crate::columns::main_ids::field_id(ms.main_table()).get(row)? == i32::try_from(FIELD_ID)?
            && crate::columns::main_ids::data_desc_id(ms.main_table()).get(row)?
                == i32::try_from(DATA_DESCRIPTION_ID)?
        {
            selected_rows.push(SelectedMainRow::new(
                u64::try_from(row)?,
                DATA_DESCRIPTION_ID,
            ));
        }
    }
    let selected_row_count = selected_rows.len();
    let content_budget = SelectedObservationContentBudget::new(64 << 20, 1, 4);
    let ephemeris = SelectedObservationEphemeris::tracked_fields(
        &ms,
        [usize::try_from(FIELD_ID)?],
        content_budget.reference_data_budget(),
    )?;
    drop(ms);
    let measures = crate::test_helpers::production_measures_provider()?;

    let rows = SelectedRows::from_ordered_main_rows(u64::try_from(row_count)?, selected_rows)?;
    let selection = ObservationSelection::new(
        rows,
        RowSelection::new(
            IdSelection::Only(vec![FIELD_ID]),
            UvSelection::All,
            IntentSelection::All,
        ),
        vec![DataDescriptionSelection::new(
            DATA_DESCRIPTION_ID,
            SPECTRAL_WINDOW_ID,
            POLARIZATION_ID,
        )],
        vec![SpectralWindowSelection::new(SPECTRAL_WINDOW_ID, vec![0])],
        vec![CorrelationSelection::new(
            POLARIZATION_ID,
            vec![
                CorrelationProduct::new(0, CorrelationType::LinearXx),
                CorrelationProduct::new(1, CorrelationType::LinearYy),
            ],
        )],
    );
    let request = SelectedObservationResolutionRequest::new(
        measurement_set.display().to_string(),
        selection,
        VisibilityColumn::Data,
        WeightColumn::Weight,
        content_budget,
        measures,
    )
    .with_ephemeris(Some(ephemeris));
    let (snapshot_input, access) = resolve_selected_observation(request)?.into_parts();
    let snapshot = compile_observation(snapshot_input)?;
    let geometry = geometry_with_centres(CentreLaws::new(
        PhaseCentreLaw::Ephemeris("TRACKFIELD".to_string()),
        DelayCentreLaw::PhaseTrackingCentre,
        PointingCentreLaw::PhaseTrackingCentre,
    ))
    .with_spectral(SpectralCoordinateSpec::new(
        FrequencyFrame::Topocentric,
        FrequencyFrame::Topocentric,
        SpectralFrameAnchor::NotApplicable,
        SpectralWcs::Linear {
            channels: 1,
            reference_pixel: 0.0,
            reference_frequency_hz: channel_frequency_hz,
            increment_hz: channel_width_hz,
        },
        RestFrequency::NotApplicable,
        casa_imaging_model::DopplerConvention::NotApplicable,
    ));
    let problem = compile(ProblemInput::new(
        specification(),
        geometry,
        snapshot,
        model_lifecycle(),
    ))?;

    let (_, samples) = stream(&problem, access.open(&problem)?)?;
    let mut actual = BTreeMap::new();
    for sample in &samples {
        // The pointing law follows the phase-tracking centre, so each
        // antenna's pointing direction is the evaluated ephemeris direction.
        actual.entry(sample.row.physical_row).or_insert((
            sample.row.coordinates.time.mjd_days(),
            sample.row.coordinates.pointing_directions.antenna1,
        ));
    }
    assert_eq!(samples.len(), 2 * selected_row_count);
    assert_eq!(actual.len(), selected_row_count);

    for expected in &oracle.samples {
        let (actual_time, direction) = actual
            .get(&expected.physical_row)
            .ok_or("production traversal omitted an oracle row")?;
        let longitude_delta = direction.longitude_rad() - expected.j2000_longitude_rad;
        let latitude_delta = direction.latitude_rad() - expected.j2000_latitude_rad;
        let separation =
            (longitude_delta * expected.j2000_latitude_rad.cos()).hypot(latitude_delta);
        eprintln!(
            "t41_trackfield label={} row={} time_mjd_days={:.14} longitude_rad={:.17} latitude_rad={:.17} longitude_delta_rad={:.3e} latitude_delta_rad={:.3e} separation_rad={:.3e}",
            expected.label,
            expected.physical_row,
            actual_time,
            direction.longitude_rad(),
            direction.latitude_rad(),
            longitude_delta,
            latitude_delta,
            separation,
        );
        assert!(
            (actual_time - expected.time_mjd_days).abs() <= 1.0e-12,
            "{} row time differs: Rust {actual_time:.17} CASA {:.17}",
            expected.label,
            expected.time_mjd_days,
        );
        assert!(
            separation <= MAX_CASA_DIRECTION_SEPARATION_RAD,
            "{} phase-centre separation {separation:.3e} rad exceeds the CASA oracle tolerance {:.3e} rad",
            expected.label,
            MAX_CASA_DIRECTION_SEPARATION_RAD,
        );
    }
    Ok(())
}

fn copy_attached_ephemerides(source: &Path, destination: &Path) -> std::io::Result<()> {
    for entry in fs::read_dir(source.join("FIELD"))? {
        let entry = entry?;
        let name = entry.file_name();
        if entry.file_type()?.is_dir()
            && name
                .to_str()
                .is_some_and(|name| name.starts_with("EPHEM") && name.ends_with(".tab"))
        {
            copy_tree(&entry.path(), &destination.join("FIELD").join(name))?;
        }
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> std::io::Result<()> {
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
