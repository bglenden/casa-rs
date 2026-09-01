// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;
use crate::{
    SelectedObservationEphemeris, SelectedObservationResolutionRequest,
    initialize_measurement_set_owner_manifest, resolve_selected_observation,
};
use casa_imaging_model::AntennaBaseline;
use casa_test_support::{CasaTestDataTier, casatestdata_path_for_tier};
use serde::Deserialize;
use std::{collections::BTreeMap, error::Error, fs, path::Path, sync::Arc};

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
    time_mjd_seconds: f64,
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
        .ok_or("slow-parity casatestdata root is unavailable")?;
    let staging = tempfile::tempdir()?;
    let measurement_set = staging.path().join("alma_ephemobj_icrs.ms");
    MeasurementSet::open(&source)?.save_as(&measurement_set)?;
    copy_attached_ephemerides(&source, &measurement_set)?;
    initialize_measurement_set_owner_manifest(&measurement_set)?;

    let ms = MeasurementSet::open(&measurement_set)?;
    let row_count = u64::try_from(ms.row_count())?;
    let channel_frequency_hz = ms.spectral_window()?.chan_freq(0)?[0];
    let channel_width_hz = ms.spectral_window()?.chan_width(0)?[0];
    let representative_baselines = oracle
        .samples
        .iter()
        .map(|sample| {
            let row = usize::try_from(sample.physical_row)?;
            let antenna1 =
                u32::try_from(crate::columns::main_ids::antenna1(ms.main_table()).get(row)?)?;
            let antenna2 =
                u32::try_from(crate::columns::main_ids::antenna2(ms.main_table()).get(row)?)?;
            Ok::<_, Box<dyn Error>>(AntennaBaseline::new(antenna1, antenna2))
        })
        .collect::<Result<Vec<_>, _>>()?;
    assert!(
        representative_baselines
            .iter()
            .all(|baseline| *baseline == representative_baselines[0]),
        "the three CASA oracle rows must use one baseline for exact predicate replay",
    );
    let ephemeris =
        SelectedObservationEphemeris::tracked_fields(&ms, [usize::try_from(FIELD_ID)?])?;
    let ephemeris_identity = ephemeris.identity();
    drop(ms);
    let measures = casa_measures_data::MeasuresRuntime::open_discovered(Default::default())?;

    let rows = SelectedRows::from_ordered_main_rows(
        row_count,
        oracle
            .samples
            .iter()
            .map(|sample| SelectedMainRow::new(sample.physical_row, DATA_DESCRIPTION_ID)),
    )?;
    let selection = ObservationSelection::new(
        rows,
        RowSelection::new(
            IdSelection::Only(vec![FIELD_ID]),
            TimeSelection::Ranges(
                oracle
                    .samples
                    .iter()
                    .map(|sample| {
                        TimeRange::new(
                            Some(SelectionBound::inclusive(sample.time_mjd_seconds)),
                            Some(SelectionBound::inclusive(sample.time_mjd_seconds)),
                        )
                    })
                    .collect(),
            ),
            UvSelection::All,
            AntennaSelection::Only(vec![representative_baselines[0]]),
            IdSelection::All,
            IdSelection::All,
            IntentSelection::All,
            IdSelection::All,
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
        identity(0xd1),
        selection,
        VisibilityColumn::Data,
        WeightColumn::Weight,
        Vec::new(),
        ModelStateIdentity::Empty,
        SelectedObservationContentBudget::new(64 << 20, 1, 4),
        Arc::new(measures),
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
    let problem = compile(ImagingRequest::new(
        specification(),
        geometry,
        ProblemInputIdentities::new(snapshot.clone()),
        model_lifecycle(snapshot.model()),
    ))?;
    assert_eq!(
        problem.geometry().ephemeris_reference(),
        Some(ephemeris_identity),
        "compiled geometry must bind the immutable TRACKFIELD ephemeris snapshot",
    );
    assert!(
        problem
            .inputs()
            .observation_snapshot()
            .reference_data()
            .contains(&(ReferenceDataKind::Ephemeris, ephemeris_identity)),
        "the production observation snapshot must commit the same ephemeris identity",
    );

    let mut bound = access.open(&problem)?;
    let mut actual = BTreeMap::new();
    let completion = bound.traverse(&problem, |sample| {
        let selected = sample.selected();
        actual.entry(selected.address().physical_row).or_insert((
            selected.coordinates().time.mjd_days(),
            selected.coordinates().phase_direction,
        ));
        Ok::<_, std::convert::Infallible>(())
    })?;
    assert_eq!(completion.sample_count(), 6);
    assert_eq!(actual.len(), 3);

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
