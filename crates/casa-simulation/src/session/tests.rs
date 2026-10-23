// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session's bookkeeping: CASA's outcomes, the numbering of scans,
//! observations and states, what a scan captures when it is observed, and
//! the MeasurementSet `close` writes.

use casa_imaging_runtime::{HostResources, ResourcePolicy};
use casa_ms::{MeasurementSet, SubTable};
use casa_types::measures::EpochRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::{ScalarValue, Value};

use super::*;
use crate::{CasaOutcome, CoordinateSystem};

/// 2024-06-01 00:00 UTC.
const REFERENCE_MJD: f64 = 60_462.0;

fn options() -> SimulatorOptions {
    SimulatorOptions {
        measures: casa_test_support::deterministic_measures_provider(),
        resources: SessionResources::pinned(
            HostResources {
                threads: 4,
                performance_cores: 4,
                available_memory: 4 << 30,
                metal: false,
            },
            ResourcePolicy::Balanced,
            BackendRequest::Cpu,
        )
        .expect("resources"),
        now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
    }
}

/// `count` 25 m antennas at the VLA, 100 m apart.
fn vla_config(count: usize) -> ArrayConfig {
    let centre = [-1_601_185.0, -5_041_977.0, 3_554_875.0];
    ArrayConfig {
        telescope: "VLA".to_string(),
        x: (0..count)
            .map(|index| centre[0] + 100.0 * index as f64)
            .collect(),
        y: (0..count)
            .map(|index| centre[1] - 30.0 * index as f64)
            .collect(),
        z: (0..count)
            .map(|index| centre[2] + 60.0 * index as f64)
            .collect(),
        dish_diameter: vec![25.0],
        offset: vec![0.0],
        mount: vec!["alt-az".to_string()],
        antenna_names: vec!["ea".to_string()],
        pad_names: vec!["W".to_string()],
        coordinate_system: CoordinateSystem::Global,
    }
}

fn window(name: &str) -> SpectralWindowSpec {
    SpectralWindowSpec {
        name: name.to_string(),
        start_frequency_hz: 1.4e9,
        channel_width_hz: 1.0e6,
        resolution_hz: 1.0e6,
        frame: FrequencyRef::TOPO,
        channel_count: 4,
        stokes: "RR LL".to_string(),
    }
}

fn state(obs_mode: &str) -> StateSpec {
    StateSpec {
        sig: true,
        reference: false,
        cal: 0.0,
        load: 0.0,
        sub_scan: 0,
        obs_mode: obs_mode.to_string(),
    }
}

fn scan(source: &str, start_s: f64, stop_s: f64) -> ObserveSpec {
    ObserveSpec {
        source: source.to_string(),
        spectral_window: "LBand".to_string(),
        start_s,
        stop_s,
        add_observation: false,
        state: state("OBSERVE_TARGET.ON_SOURCE"),
        observer: "tester".to_string(),
        project: "tests".to_string(),
    }
}

fn direction(ra_deg: f64, dec_deg: f64) -> MDirection {
    MDirection::from_angles(
        ra_deg.to_radians(),
        dec_deg.to_radians(),
        DirectionRef::J2000,
    )
}

/// A session with four antennas, one window and two fields, observing in
/// seconds from the reference time.
fn ready(path: &Path) -> Simulator {
    let mut session = Simulator::open(path, options());
    session.set_config(&vla_config(4)).expect("setconfig");
    session
        .set_spectral_window(&window("LBand"))
        .expect("setspwindow");
    session
        .set_field("north", &direction(150.0, 34.0), "", 0.0)
        .expect("setfield");
    session
        .set_field("south", &direction(150.0, -10.0), "", 0.0)
        .expect("setfield");
    session
        .set_times(10.0, false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
        .expect("settimes");
    session
}

fn refusal(result: SimulatorResult<()>) -> (CasaOutcome, String) {
    let error = result.expect_err("refused");
    (error.outcome(), error.message().to_string())
}

#[test]
fn calls_before_setconfig_are_refused_where_casa_aborts() {
    let mut session = Simulator::open("unused.ms", options());
    assert_eq!(
        refusal(session.observe(&scan("north", 0.0, 10.0))),
        (
            CasaOutcome::ReturnsFalse,
            "Antenna information not yet defined".to_string()
        )
    );
    let (outcome, message) =
        refusal(session.set_feed("perfect R L", &[0.0], &[0.0], &["R".to_string()]));
    assert_eq!(outcome, CasaOutcome::Raises);
    assert!(message.contains("initAnt"), "{message}");
}

#[test]
fn observe_preconditions_return_false_with_casas_messages() {
    let mut session = Simulator::open("unused.ms", options());
    session.set_config(&vla_config(3)).expect("setconfig");
    let expect_false = |session: &mut Simulator, spec: &ObserveSpec, text: &str| {
        assert_eq!(
            refusal(session.observe(spec)),
            (CasaOutcome::ReturnsFalse, text.to_string())
        );
    };
    expect_false(
        &mut session,
        &scan("north", 0.0, 10.0),
        "Spectral window information not yet defined",
    );
    session
        .set_spectral_window(&window("LBand"))
        .expect("setspwindow");
    let mut other_window = scan("north", 0.0, 10.0);
    other_window.spectral_window = "XBand".to_string();
    expect_false(
        &mut session,
        &other_window,
        "Spectral window named XBand not yet defined",
    );
    expect_false(
        &mut session,
        &scan("north", 0.0, 10.0),
        "Field information not yet defined",
    );
    session
        .set_field("north", &direction(150.0, 34.0), "", 0.0)
        .expect("setfield");
    expect_false(
        &mut session,
        &scan("elsewhere", 0.0, 10.0),
        "Field named elsewhere not yet defined",
    );
    session
        .set_times(0.0, false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
        .expect("a zero integration time is accepted by settimes");
    expect_false(
        &mut session,
        &scan("north", 0.0, 10.0),
        "observe: the integration time must be positive",
    );
    assert!(session.scans.is_empty(), "a refused observe plans nothing");
}

#[test]
fn settimes_raises_for_a_negative_integration_time() {
    let mut session = Simulator::open("unused.ms", options());
    let (outcome, _) =
        refusal(session.set_times(-1.0, true, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC)));
    assert_eq!(outcome, CasaOutcome::Raises);
}

#[test]
fn observe_without_setfeed_uses_perfect_r_l_feeds() {
    let mut session = ready(Path::new("unused.ms"));
    session.observe(&scan("north", 0.0, 10.0)).expect("observe");
    assert_eq!(session.feeds.len(), 4, "one feed per antenna");
    assert!(!session.feeds_set);
}

#[test]
fn scans_count_up_and_observations_follow_add_observation() {
    let mut session = ready(Path::new("unused.ms"));
    session.observe(&scan("north", 0.0, 10.0)).expect("first");
    session.observe(&scan("south", 10.0, 20.0)).expect("second");
    let mut new_observation = scan("north", 20.0, 30.0);
    new_observation.add_observation = true;
    session.observe(&new_observation).expect("third");

    let numbers = session
        .scans
        .iter()
        .map(|scan| (scan.scan_number, scan.observation_id, scan.field_id))
        .collect::<Vec<_>>();
    assert_eq!(numbers, vec![(1, 0, 0), (2, 0, 1), (3, 1, 0)]);
    assert_eq!(session.observations.len(), 2);
    assert_eq!(session.scan_count(), 3);
}

/// `observemany`'s pointings as one call: a scan per run of one field.
fn many(pointings: &[(&str, f64, f64, Option<MDirection>)]) -> ObserveManySpec {
    ObserveManySpec {
        pointings: pointings
            .iter()
            .map(|(source, start_s, stop_s, direction)| observe::Pointing {
                source: (*source).to_string(),
                start_s: *start_s,
                stop_s: *stop_s,
                direction: direction.clone(),
            })
            .collect(),
        ..ObserveManySpec::from(&scan("north", 0.0, 0.0))
    }
}

/// `NewMSSimulator::observe` with several pointings: a new scan number at
/// the first and at each change of field; each pointing its own start and
/// integration count; a direction moves that pointing's phase centre, and
/// a pointing without one keeps the previous pointing's while the field
/// stays the same. OBSERVATION spans the first start to the last stop.
#[test]
fn observemany_numbers_a_scan_per_run_of_one_field() {
    let mut session = ready(Path::new("unused.ms"));
    session
        .observe(&scan("south", -20.0, -10.0))
        .expect("observe");
    let moved = direction(151.0, 35.0);
    session
        .observe_many(&many(&[
            ("north", 0.0, 10.0, None),
            ("north", 10.0, 30.0, Some(moved.clone())),
            ("north", 30.0, 40.0, None),
            ("south", 40.0, 45.0, None),
            ("north", 50.0, 60.0, None),
        ]))
        .expect("observemany");
    let north = session.fields[0].direction_rad;
    let south = session.fields[1].direction_rad;
    let moved_rad = [151.0_f64.to_radians(), 35.0_f64.to_radians()];
    let reference_s = REFERENCE_MJD * 86_400.0;
    let planned = session.scans[1..]
        .iter()
        .map(|scan| {
            (
                scan.scan_number,
                scan.field_id,
                scan.start_s - reference_s,
                scan.integration_count,
                scan.phase_center_rad,
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        planned,
        vec![
            (2, 0, 0.0, 1, north),
            (2, 0, 10.0, 2, moved_rad),
            (2, 0, 30.0, 1, moved_rad),
            (3, 1, 40.0, 1, south),
            (4, 0, 50.0, 1, north),
        ]
    );
    assert_eq!(session.observations.len(), 1, "OBSERVATION is the first's");
    let mut added = many(&[("north", 70.0, 80.0, None), ("south", 80.0, 95.0, None)]);
    added.add_observation = true;
    session.observe_many(&added).expect("observemany");
    assert_eq!(
        session.observations[1].time_range_s,
        [reference_s + 70.0, reference_s + 95.0]
    );
    assert!(
        session.scans[6..]
            .iter()
            .all(|scan| scan.observation_id == 1)
    );
}

/// CASA looks the later pointings' fields up after adding their rows;
/// casa-rs looks every one up first and plans nothing when one is missing.
#[test]
fn observemany_checks_every_field_before_planning_any() {
    let mut session = ready(Path::new("unused.ms"));
    let (outcome, message) = refusal(session.observe_many(&many(&[
        ("north", 0.0, 10.0, None),
        ("west", 10.0, 20.0, None),
    ])));
    assert_eq!(outcome, CasaOutcome::ReturnsFalse);
    assert_eq!(message, "   Field 1: west not yet defined");
    let (_, message) = refusal(session.observe_many(&many(&[("west", 0.0, 10.0, None)])));
    assert_eq!(message, "Field named west not yet defined");
    let (outcome, _) = refusal(session.observe_many(&many(&[])));
    assert_eq!(outcome, CasaOutcome::ReturnsFalse);
    assert!(session.scans.is_empty() && session.observations.is_empty());
}

/// `setmosaicfield` names its fields down each column and then across, and
/// centres the grid on the field centre: `(i − (n − 1)/2)` spacings, in
/// right ascension divided by the centre's cos δ. Over a pole the
/// declination folds back and the right ascension turns by π.
#[test]
fn setmosaicfield_centres_its_grid_on_the_field_centre() {
    let mut session = ready(Path::new("unused.ms"));
    let spacing = 0.5_f64.to_radians();
    let (ra0, dec0) = (150.0_f64.to_radians(), 34.0_f64.to_radians());
    session
        .set_mosaic_field("mos", "C", &direction(150.0, 34.0), (3, 2), spacing, 0.0)
        .expect("setmosaicfield");
    let added = &session.fields[2..];
    assert_eq!(
        added
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>(),
        ["mos_0", "mos_1", "mos_2", "mos_3", "mos_4", "mos_5"]
    );
    for (k, field) in added.iter().enumerate() {
        let (i, j) = (k / 2, k % 2);
        let ra = ra0 + (i as f64 - 1.0) * spacing / dec0.cos();
        let dec = dec0 + (j as f64 - 0.5) * spacing;
        assert!(
            (field.direction_rad[0] - ra).abs() < 1e-12
                && (field.direction_rad[1] - dec).abs() < 1e-12,
            "{}: {:?}",
            field.name,
            field.direction_rad
        );
        assert_eq!(field.code, "C");
    }
    // A column through the north pole.
    session
        .set_mosaic_field("pole", "", &direction(10.0, 89.75), (1, 3), spacing, 0.0)
        .expect("setmosaicfield");
    let over = session.fields.last().expect("field");
    assert!((over.direction_rad[1] - 89.75_f64.to_radians()).abs() < 1e-12);
    // 190°, kept in (−π, π] as casacore keeps a longitude.
    assert!((over.direction_rad[0] - (-170.0_f64).to_radians()).abs() < 1e-12);
}

#[test]
fn identical_states_share_a_row() {
    let mut session = ready(Path::new("unused.ms"));
    for mode in ["ON_SOURCE", "CALIBRATE", "ON_SOURCE"] {
        let mut spec = scan("north", 0.0, 10.0);
        spec.state = state(mode);
        session.observe(&spec).expect("observe");
    }
    let state_ids = session
        .scans
        .iter()
        .map(|scan| scan.state_id)
        .collect::<Vec<_>>();
    assert_eq!(state_ids, vec![0, 1, 0]);
    assert_eq!(session.states.len(), 2);
}

#[test]
fn a_scan_keeps_the_settings_in_force_when_it_was_observed() {
    let mut session = ready(Path::new("unused.ms"));
    session.observe(&scan("north", 0.0, 10.0)).expect("first");
    session.set_auto(0.0);
    session.set_limits(0.5, 0.25);
    session.set_config(&vla_config(2)).expect("more antennas");
    // The feeds auto-created for four antennas no longer fit six.
    assert_eq!(
        refusal(session.observe(&scan("north", 10.0, 20.0))),
        (
            CasaOutcome::ReturnsFalse,
            "Feed table format is incompatible with existing code of NewMSSimulator::observe"
                .to_string()
        )
    );
    session.feeds = feeds::perfect_feeds("perfect R L", 6);
    session.observe(&scan("north", 10.0, 20.0)).expect("second");

    let [first, second] = session.scans.as_slice() else {
        panic!("two scans");
    };
    assert_eq!(
        (first.antenna_count, first.autocorrelation_weight),
        (4, 1.0)
    );
    assert_eq!(
        first.baseline_count(),
        10,
        "4 antennas with autocorrelations"
    );
    assert_eq!(first.elevation_limit_rad, 8.0_f64.to_radians());
    assert_eq!(
        (second.antenna_count, second.autocorrelation_weight),
        (6, 0.0)
    );
    assert_eq!(
        second.baseline_count(),
        15,
        "6 antennas, no autocorrelations"
    );
    assert_eq!(
        (second.shadow_fraction_limit, second.elevation_limit_rad),
        (0.5, 0.25)
    );
}

#[test]
fn integrations_are_stored_at_their_centres() {
    let mut session = ready(Path::new("unused.ms"));
    session.observe(&scan("north", 0.0, 30.0)).expect("observe");
    let reference_s = REFERENCE_MJD * 86_400.0;
    let times = session.scans[0].integration_times_s().collect::<Vec<_>>();
    assert_eq!(
        times,
        vec![reference_s + 5.0, reference_s + 15.0, reference_s + 25.0]
    );
    assert_eq!(session.scans[0].row_count(), 3 * 10);
}

#[test]
fn the_hour_angle_offset_comes_from_the_first_source_after_settimes() {
    let mut session = ready(Path::new("unused.ms"));
    let reference = MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC);
    session
        .set_times(10.0, true, reference.clone())
        .expect("settimes");
    session.observe(&scan("north", 0.0, 10.0)).expect("north");
    let offset_north = session.times.hour_angle_offset_s.expect("computed");
    session.observe(&scan("south", 0.0, 10.0)).expect("south");
    assert_eq!(
        session.times.hour_angle_offset_s,
        Some(offset_north),
        "later sources keep the first source's offset"
    );
    assert_eq!(
        session.scans[1].start_s, session.scans[0].start_s,
        "both scans start at the first source's transit"
    );

    session.set_times(10.0, true, reference).expect("settimes");
    assert_eq!(session.times.hour_angle_offset_s, None);
    session.observe(&scan("south", 0.0, 10.0)).expect("south");
    // Both fields are at J2000 RA 150°. Precession since J2000 moves their
    // apparent RAs apart by seconds of time, so the offset is recomputed
    // from the new first source and differs from the old one by that much.
    let offset_south = session.times.hour_angle_offset_s.expect("recomputed");
    assert_ne!(offset_south, offset_north);
    assert!((offset_south - offset_north).abs() < 60.0);
    assert!(offset_north.abs() <= 12.0 * 3600.0);
}

fn cell(ms: &MeasurementSet, row: usize, column: &str) -> Value {
    ms.main_table()
        .row_accessor()
        .row(row)
        .expect("row")
        .get(column)
        .unwrap_or_else(|| panic!("{column} in row {row}"))
        .clone()
}

#[test]
fn close_writes_the_planned_rows() {
    let directory = tempfile::tempdir().expect("temporary directory");
    let path = directory.path().join("session.ms");
    let mut session = ready(&path);
    session.observe(&scan("north", 0.0, 20.0)).expect("north");
    session.set_auto(0.0);
    // Above the zenith: every antenna is flagged.
    session.set_limits(1.0e-6, 91.0_f64.to_radians());
    session.observe(&scan("south", 20.0, 30.0)).expect("south");
    session.close().expect("close");

    let ms = MeasurementSet::open(&path).expect("open");
    assert_eq!(ms.row_count(), 2 * 10 + 6);
    assert_eq!(ms.antenna().expect("ANTENNA").table().row_count(), 4);
    assert_eq!(
        cell(&ms, 0, "TIME"),
        Value::Scalar(ScalarValue::Float64(REFERENCE_MJD * 86_400.0 + 5.0))
    );
    assert_eq!(
        (cell(&ms, 0, "ANTENNA1"), cell(&ms, 0, "ANTENNA2")),
        (
            Value::Scalar(ScalarValue::Int32(0)),
            Value::Scalar(ScalarValue::Int32(0))
        ),
        "autocorrelations first in the first scan"
    );
    assert_eq!(
        cell(&ms, 20, "ANTENNA2"),
        Value::Scalar(ScalarValue::Int32(1)),
        "no autocorrelations in the second scan"
    );
    assert_eq!(
        cell(&ms, 20, "SCAN_NUMBER"),
        Value::Scalar(ScalarValue::Int32(2))
    );
    assert_eq!(
        cell(&ms, 25, "FLAG_ROW"),
        Value::Scalar(ScalarValue::Bool(true)),
        "below the elevation limit"
    );
    assert!(
        !ms.main_table()
            .is_cell_defined(0, "FLAG_CATEGORY")
            .expect("FLAG_CATEGORY"),
        "FLAG_CATEGORY is left undefined, as CASA leaves it"
    );
}
