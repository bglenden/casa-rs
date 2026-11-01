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

    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("the slow-parity ALMA ephemeris MeasurementSet is unavailable")?;
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
                CorrelationProduct::new(0, Correlation::XX),
                CorrelationProduct::new(1, Correlation::YY),
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

/// CASA 6.7.6.14 tclean, field="1", spw="0", specmode="mfs",
/// default phase centre and output frame: spectral coordinate increment.
/// MSUtil::getFreqRangeInSpw uses phaseDirMeas(field) at FIELD.TIME while
/// updating the conversion frame epoch for each selected integration.
#[test]
#[ignore = "requires the slow-parity ALMA ephemeris MeasurementSet"]
fn moving_field_mfs_envelope_matches_casa_field_origin_direction() -> Result<(), Box<dyn Error>> {
    let source = casatestdata_path_for_tier(CasaTestDataTier::SlowParity, DATASET)
        .ok_or("the slow-parity ALMA ephemeris MeasurementSet is unavailable")?;
    let ms = MeasurementSet::open(&source)?;
    let engine = MsCalEngine::new(&ms)?.with_field_ephemerides(&ms)?;
    assert!(engine.field_moves(1));
    let selection = ms.selected_observation_row_selection(&[0], Some(&[1]), None, None)?;
    let window = ms.spectral_window()?;
    let frequencies = window.chan_freq(0)?;
    let widths = window.chan_width(0)?;
    let channels = (0..frequencies.len()).collect::<Vec<_>>();
    let mut reducer = ms.selected_observation_spectral_envelope_reducer(
        &selection,
        [crate::SelectedObservationSpectralWindow::borrow_selected(
            0,
            FrequencyRef::TOPO,
            &frequencies,
            &widths,
            &channels,
        )],
        FrequencyRef::LSRK,
        &engine,
        1 << 20,
    )?;
    let mut rows = 0;
    let mut failure = None;
    ms.visit_selected_observation_rows(
        &selection,
        MsSelectionIoBudget {
            available_bytes: 64 * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            maximum_live_blocks: 2,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| {
            rows += 1;
            if failure.is_none() {
                failure = reducer.observe(row).err();
            }
        },
    )?;
    if let Some(error) = failure {
        return Err(error.into());
    }
    assert_eq!(rows, 1620);
    let [lower, upper] = reducer.finish()?.edges_hz();
    let increment = upper - lower;
    let casa_increment = 125_020_996.111_328_12;
    let relative_error = (increment - casa_increment).abs() / casa_increment;
    eprintln!(
        "moving MFS increment: native={increment:.8} CASA={casa_increment:.8} relative_error={relative_error:.3e}"
    );
    assert!(
        relative_error <= 2.0e-10,
        "MFS increment differs from CASA: {relative_error:.3e}"
    );
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
