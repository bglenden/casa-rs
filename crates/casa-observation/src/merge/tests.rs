// SPDX-License-Identifier: LGPL-3.0-or-later
//! The merge's laws, each entity's predicate against casacore's.

use std::collections::BTreeMap;

use casa_types::Correlation;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};
use casa_types::measures::position::MPosition;
use casa_types::measures::{EpochRef, MEpoch, MjdHighPrec};
use casa_types::quanta::Quantity;

use super::{MILLIARCSECOND, MergeTolerance, merge, separation};
use crate::{
    AntennaEntry, AntennaId, Catalogue, FieldEntry, FieldId, ObservationEntry, ObservationError,
    ObservationId, PartitionEntry, ProcessorEntry, ProcessorId, SourceEntry, SourceId,
    SourceRecord, SourceWindow, StateEntry, StateId, WindowEntry, WindowId,
};

fn antenna(name: &str, station: &str, x: f64) -> AntennaEntry {
    AntennaEntry {
        name: name.to_string(),
        station: station.to_string(),
        position: MPosition::new_itrf(x, 5.0e6, 3.0e6),
        mount: "ALT-AZ".to_string(),
        dish_diameter: Quantity::new(12.0, "m").expect("m"),
        flagged: Some(false),
    }
}

fn window(reference_hz: f64, if_chain: i32) -> WindowEntry {
    WindowEntry {
        name: "spw".to_string(),
        reference_frequency: Some(MFrequency::new(reference_hz, FrequencyRef::TOPO)),
        total_bandwidth_hz: Some(4.0e6),
        if_chain: Some(if_chain),
        net_sideband: Some(1),
        frequency_group: Some(0),
        frequency_group_name: Some(String::new()),
        effective_bandwidths_hz: None,
        resolutions_hz: None,
        flagged: Some(false),
    }
}

/// A partition of window `window`, its channels from `first_hz` by 1 MHz.
fn partition(window: u32, first_hz: f64, channels: usize) -> PartitionEntry {
    PartitionEntry {
        spectral_window: "spw".to_string(),
        window: WindowId(window),
        frequencies_hz: (0..channels)
            .map(|channel| first_hz + 1.0e6 * channel as f64)
            .collect(),
        frequency_frame: FrequencyRef::TOPO,
        channel_widths_hz: vec![1.0e6; channels],
        correlations: vec![Correlation::XX],
        intents: Vec::new(),
        observation: ObservationId(0),
        feeds: None,
        fields: vec![FieldId(0)],
        scans: Vec::new(),
        baselines: Vec::new(),
        time_scale: EpochRef::UTC,
        time_origin: MjdHighPrec::new(0.0, 0.0),
        data_groups: Vec::new(),
        receptors: BTreeMap::new(),
        states: Vec::new(),
        processor: None,
    }
}

/// A catalogue of one window at `reference_hz` (IF chain `if_chain`) whose
/// partition's channels start at `first_hz`.
fn with_window(reference_hz: f64, if_chain: i32, first_hz: f64) -> Catalogue {
    Catalogue {
        windows: vec![window(reference_hz, if_chain)],
        partitions: vec![partition(0, first_hz, 4)],
        ..Catalogue::default()
    }
}

fn field(name: &str, longitude: f64, latitude: f64) -> FieldEntry {
    let direction = MDirection::from_angles(longitude, latitude, DirectionRef::J2000);
    FieldEntry {
        name: name.to_string(),
        phase_centre: direction.clone(),
        reference_epoch: None,
        source_name: None,
        source: None,
        code: Some(String::new()),
        delay_centre: Some(direction.clone()),
        reference_centre: Some(direction),
        flagged: Some(false),
        ephemeris: None,
    }
}

fn state(cal_k: f64, sub_scan: i32) -> StateEntry {
    StateEntry {
        obs_mode: "OBSERVE_TARGET#ON_SOURCE".to_string(),
        cal_k: Some(cal_k),
        load_k: Some(0.0),
        signal: Some(true),
        reference: Some(false),
        sub_scan: Some(sub_scan),
        flagged: Some(false),
    }
}

fn session(project: &str, start_s: f64) -> ObservationEntry {
    ObservationEntry {
        telescope: "ALMA".to_string(),
        observers: vec!["astra".to_string()],
        project: project.to_string(),
        time_range: Some([
            MEpoch::from_mjd_seconds(start_s, EpochRef::UTC),
            MEpoch::from_mjd_seconds(start_s + 600.0, EpochRef::UTC),
        ]),
        release_date: Some(MEpoch::from_mjd_seconds(0.0, EpochRef::UTC)),
        flagged: Some(false),
        ..ObservationEntry::default()
    }
}

fn processor(mode_id: i32) -> ProcessorEntry {
    ProcessorEntry {
        kind: Some("CORRELATOR".to_string()),
        sub_type: Some("ALMA_CORRELATOR_MODE".to_string()),
        type_id: Some(-1),
        mode_id: Some(mode_id),
        pass_id: None,
        flagged: Some(false),
    }
}

fn record(window: SourceWindow, rest_hz: f64, longitude: f64) -> SourceRecord {
    SourceRecord {
        window,
        time: Some(MEpoch::from_mjd_seconds(5.0e9, EpochRef::UTC)),
        interval_s: Some(100.0),
        code: Some(String::new()),
        calibration_group: Some(0),
        direction: Some(MDirection::from_angles(longitude, 0.5, DirectionRef::J2000)),
        proper_motion_rad_s: Some([0.0, 0.0]),
        position: None,
        pulsar_id: None,
        num_lines: 1,
        transitions: Some(vec!["CO".to_string()]),
        rest_frequencies: Some(vec![MFrequency::new(rest_hz, FrequencyRef::LSRK)]),
        systemic_velocities: None,
    }
}

fn tolerance() -> MergeTolerance {
    MergeTolerance::default()
}

/// One catalogue merges into itself, every identity kept.
#[test]
fn one_input_is_taken_as_it_is() {
    let catalogue = Catalogue {
        antennas: vec![antenna("DA41", "A001", 0.0), antenna("DA41", "A001", 0.0)],
        states: vec![state(0.0, 1), state(0.0, 1)],
        ..with_window(1.0e11, 0, 1.0e11)
    };
    let merged = merge(&[&catalogue], &tolerance()).expect("merge");
    assert_eq!(merged.antennas.len(), 2, "the first input is not matched");
    assert_eq!(merged.states.len(), 2);
    assert_eq!(merged.inputs[0].antennas, [AntennaId(0), AntennaId(1)]);
    assert_eq!(merged.inputs[0].windows, [WindowId(0)]);
}

/// Antennas match from the last back by name, station (unless the incoming
/// one states none) and each position component within 1 m; a flagged one
/// is not a candidate, and positions in other frames are refused.
#[test]
fn antennas_match_by_name_station_and_position() {
    let base = Catalogue {
        antennas: vec![antenna("DA41", "A001", 0.0), antenna("DA41", "A001", 0.5)],
        ..Catalogue::default()
    };
    let incoming = Catalogue {
        antennas: vec![
            antenna("DA41", "A001", 0.9),
            antenna("DA41", "A001", 2.0),
            antenna("DA41", "", 0.0),
            antenna("DA41", "A002", 0.0),
        ],
        ..Catalogue::default()
    };
    let merged = merge(&[&base, &incoming], &tolerance()).expect("merge");
    assert_eq!(
        merged.inputs[1].antennas,
        [AntennaId(1), AntennaId(2), AntennaId(1), AntennaId(3)],
        "0.9 m matches the last candidate within 1 m; 2 m does not; an empty station matches; \
         another station does not"
    );
    let mut flagged = base.clone();
    flagged.antennas[1].flagged = Some(true);
    let merged = merge(&[&flagged, &incoming], &tolerance()).expect("merge");
    assert_eq!(
        merged.inputs[1].antennas[0],
        AntennaId(0),
        "the flagged antenna is passed"
    );
    let mut wgs = incoming.clone();
    wgs.antennas[0].position = MPosition::new_wgs84(0.0, 0.0, 0.0);
    assert!(matches!(
        merge(&[&base, &wgs], &tolerance()),
        Err(ObservationError::Unsupported { .. })
    ));
}

/// Windows match in row order, the first whose channels are all within
/// `freqtol`, among those with the same IF chain, a total bandwidth within a
/// quarter and a reference frequency within `freqtol` in the same frame. A
/// window whose channels match only reversed is refused, and one that
/// states no IF chain is appended with a notice.
#[test]
fn windows_match_within_freqtol() {
    let base = with_window(1.0e11, 0, 1.0e11);
    let within = with_window(1.0e11 + 0.5, 0, 1.0e11 + 0.5);
    let merged = merge(&[&base, &within], &tolerance()).expect("merge");
    assert_eq!(merged.inputs[1].windows, [WindowId(0)]);
    for outside in [
        with_window(1.0e11 + 1.5, 0, 1.0e11),
        with_window(1.0e11, 0, 1.0e11 + 1.5),
        with_window(1.0e11, 1, 1.0e11),
    ] {
        let merged = merge(&[&base, &outside], &tolerance()).expect("merge");
        assert_eq!(merged.inputs[1].windows, [WindowId(1)], "appended");
    }
    let mut narrow = with_window(1.0e11, 0, 1.0e11);
    narrow.windows[0].total_bandwidth_hz = Some(2.0e6);
    let merged = merge(&[&base, &narrow], &tolerance()).expect("merge");
    assert_eq!(
        merged.inputs[1].windows,
        [WindowId(1)],
        "half the bandwidth"
    );
    let mut reversed = with_window(1.0e11, 0, 1.0e11);
    reversed.partitions[0].frequencies_hz.reverse();
    assert!(matches!(
        merge(&[&base, &reversed], &tolerance()),
        Err(ObservationError::Unsupported { .. })
    ));
    let mut silent = with_window(1.0e11, 0, 1.0e11);
    silent.windows[0].if_chain = None;
    let merged = merge(&[&base, &silent], &tolerance()).expect("merge");
    assert_eq!(merged.inputs[1].windows, [WindowId(1)]);
    assert_eq!(merged.notices.len(), 1, "{:?}", merged.notices);
    // One window in two observations: the second input's matches the first's.
    let twice = merge(&[&base, &within, &within], &tolerance()).expect("merge");
    assert_eq!(twice.windows.len(), 1);
}

/// Incoming windows are visited in the order the partitions use them
/// (CASA's DATA_DESCRIPTION order), not in row order: Astra's CASA 6.7.6
/// case. The first input's window cannot match; the second's three are 0,
/// 0.75 and 1.5 Hz apart, used as windows 1, 0, 2. Visiting 0.75 Hz first,
/// both neighbours match it, giving two windows in all, as CASA gives; in
/// row order 1.5 Hz would not match 0 Hz, giving three.
#[test]
fn incoming_windows_are_visited_in_data_description_order() {
    let base = 1.0e11;
    let first = with_window(base + 10.0e6, 0, base + 10.0e6);
    let offsets = [0.0, 0.75, 1.5];
    let second = Catalogue {
        windows: offsets
            .iter()
            .map(|offset| window(base + offset, 0))
            .collect(),
        partitions: [1_u32, 0, 2]
            .iter()
            .map(|window| partition(*window, base + offsets[*window as usize], 4))
            .collect(),
        ..Catalogue::default()
    };
    let merged = merge(&[&first, &second], &tolerance()).expect("merge");
    assert_eq!(merged.windows.len(), 2, "{:?}", merged.inputs[1].windows);
    assert_eq!(
        merged.inputs[1].windows,
        [WindowId(1), WindowId(1), WindowId(1)]
    );
}

/// casacore's separation of two directions agrees with the haversine
/// formula, across the longitude wrap and near the pole.
#[test]
fn separations_are_great_circle_angles() {
    let at = |longitude: f64, latitude: f64| {
        MDirection::from_angles(longitude, latitude, DirectionRef::J2000)
    };
    let haversine = |a: (f64, f64), b: (f64, f64)| {
        let h = ((b.1 - a.1) / 2.0).sin().powi(2)
            + a.1.cos() * b.1.cos() * ((b.0 - a.0) / 2.0).sin().powi(2);
        2.0 * h.sqrt().asin()
    };
    for (a, b) in [
        ((0.0, 0.0), (0.8 * MILLIARCSECOND, 0.8 * MILLIARCSECOND)),
        ((std::f64::consts::TAU - 1e-9, 0.1), (1e-9, 0.1)),
        (
            (0.0, std::f64::consts::FRAC_PI_2 - 1e-7),
            (3.0, std::f64::consts::FRAC_PI_2 - 1e-7),
        ),
    ] {
        let expected = haversine(a, b);
        let found = separation(&at(a.0, a.1), &at(b.0, b.1));
        assert!(
            (found - expected).abs() < 1e-15,
            "{a:?} {b:?}: {found} {expected}"
        );
    }
}

/// Fields match when each direction is strictly within `dirtol`: 0.8 mas in
/// both axes at the equator is 1.13 mas, and does not match at 1 mas. The
/// last field of the incoming name is tried first, then every field from
/// the last back, so a field appended from this input represents the
/// fields near it that follow; with names respected, a match of another
/// name is not used.
#[test]
fn fields_match_strictly_within_dirtol() {
    let offset = 0.8 * MILLIARCSECOND;
    let base = Catalogue {
        fields: vec![field("A", 0.0, 0.0), field("B", 0.0, 0.0)],
        ..Catalogue::default()
    };
    let incoming = Catalogue {
        fields: vec![
            field("A", offset, offset),
            field("A", offset, 0.0),
            field("C", 0.0, 0.0),
            field("A", std::f64::consts::TAU - 0.1 * MILLIARCSECOND, 0.0),
        ],
        ..Catalogue::default()
    };
    let merged = merge(&[&base, &incoming], &tolerance()).expect("merge");
    assert_eq!(
        merged.inputs[1].fields,
        [FieldId(2), FieldId(2), FieldId(1), FieldId(1)],
        "1.13 mas is appended, and is then the last A, 0.8 mas from the next; C matches the \
         last field within 1 mas, B; so does the A across the wrap, 1.2 mas from the appended A"
    );
    let named = MergeTolerance {
        respect_names: true,
        ..tolerance()
    };
    let merged = merge(&[&base, &incoming], &named).expect("merge");
    assert_eq!(
        merged.inputs[1].fields,
        [FieldId(2), FieldId(2), FieldId(3), FieldId(4)],
        "with names respected, neither C nor the last A is B"
    );
    let mut silent = incoming.clone();
    silent.fields[2].delay_centre = None;
    let merged = merge(&[&base, &silent], &tolerance()).expect("merge");
    assert_eq!(
        merged.inputs[1].fields[2],
        FieldId(3),
        "appended with a notice"
    );
    assert_eq!(merged.notices.len(), 1);
}

/// States match from the last back with CAL and LOAD strictly within 1 K
/// and their mode, signal, reference and sub-scan equal.
#[test]
fn states_match_within_a_kelvin() {
    let base = Catalogue {
        states: vec![state(10.0, 1)],
        ..Catalogue::default()
    };
    let incoming = Catalogue {
        states: vec![state(10.5, 1), state(11.0, 1), state(10.0, 2)],
        ..Catalogue::default()
    };
    let merged = merge(&[&base, &incoming], &tolerance()).expect("merge");
    assert_eq!(
        merged.inputs[1].states,
        [StateId(0), StateId(1), StateId(2)]
    );
    let none = merge(&[&base, &Catalogue::default()], &tolerance()).expect("merge");
    assert!(none.inputs[1].states.is_empty());
    assert_eq!(none.states.len(), 1, "the states of the others are kept");
}

/// Observing sessions and processors are appended and made distinct over
/// the whole table, each mapped to its first equivalent, the earlier
/// inputs' maps renumbered without gaps; a session matching an earlier
/// input's is its counterpart, and one whose extent is unknown matches
/// nothing.
#[test]
fn sessions_and_processors_are_made_distinct_over_the_table() {
    let base = Catalogue {
        observations: vec![session("A", 0.0), session("B", 1000.0), session("A", 0.0)],
        processors: vec![processor(0), processor(0)],
        ..Catalogue::default()
    };
    let mut unknown = session("C", 2000.0);
    unknown.time_range = None;
    let incoming = Catalogue {
        observations: vec![session("B", 1000.0), unknown.clone(), unknown],
        processors: vec![processor(3), processor(0)],
        ..Catalogue::default()
    };
    let merged = merge(&[&base, &incoming], &tolerance()).expect("merge");
    assert_eq!(merged.observations.len(), 4);
    assert_eq!(
        merged.inputs[0].observations,
        [ObservationId(0), ObservationId(1), ObservationId(0)],
        "the first input's duplicate is made one"
    );
    assert_eq!(
        merged.inputs[1].observations,
        [ObservationId(1), ObservationId(2), ObservationId(3)],
        "an unknown extent matches nothing, not even itself"
    );
    assert_eq!(merged.inputs[1].counterparts, [true, false, false]);
    assert_eq!(merged.processors.len(), 2);
    assert_eq!(
        merged.inputs[0].processors,
        [ProcessorId(0), ProcessorId(0)]
    );
    assert_eq!(
        merged.inputs[1].processors,
        [ProcessorId(1), ProcessorId(0)]
    );
}

/// A source's record equal to a merged one in its window is one record,
/// spanning both validities and centred on them, and the fields of its
/// source go to the merged one; two sources of one name in different
/// directions stay two.
#[test]
fn equal_records_of_a_window_are_one() {
    let mut base = with_window(1.0e11, 0, 1.0e11);
    base.sources = vec![SourceEntry {
        name: "M51".to_string(),
        records: vec![record(SourceWindow::Window(WindowId(0)), 2.3e11, 1.0)],
    }];
    base.fields = vec![FieldEntry {
        source: Some(SourceId(0)),
        ..field("M51", 1.0, 0.5)
    }];
    let mut incoming = base.clone();
    let mut later = record(SourceWindow::Window(WindowId(0)), 2.3e11, 1.0);
    later.time = Some(MEpoch::from_mjd_seconds(5.0e9 + 1000.0, EpochRef::UTC));
    let mut elsewhere = record(SourceWindow::Window(WindowId(0)), 2.3e11, 1.2);
    elsewhere.code = Some(String::new());
    incoming.sources = vec![
        SourceEntry {
            name: "M51".to_string(),
            records: vec![later],
        },
        SourceEntry {
            name: "M51".to_string(),
            records: vec![elsewhere],
        },
    ];
    incoming.fields = vec![
        FieldEntry {
            source: Some(SourceId(0)),
            ..field("M51", 1.0, 0.6)
        },
        FieldEntry {
            source: Some(SourceId(1)),
            ..field("M51b", 1.2, 0.5)
        },
    ];
    let merged = merge(&[&base, &incoming], &tolerance()).expect("merge");
    assert_eq!(merged.sources.len(), 2, "{:?}", merged.sources);
    assert_eq!(
        merged.inputs[1].sources,
        [Some(SourceId(0)), Some(SourceId(1))]
    );
    let kept = &merged.sources[0].records[0];
    assert!((kept.interval_s.expect("interval") - 1100.0).abs() < 1e-6);
    let time = kept.time.as_ref().expect("time").value();
    let centre = time.day() * 86_400.0 + time.frac() * 86_400.0;
    assert!((centre - (5.0e9 + 500.0)).abs() < 1e-6, "{centre}");
    assert_eq!(merged.fields[1].source, Some(SourceId(0)));
    assert_eq!(merged.fields[2].source, Some(SourceId(1)));
}

/// A 7 m field matching a 12 m one reaches both inputs' lines: the 7 m
/// source's record in another window is equivalent to the 12 m source's on
/// direction, so once the 7 m field is the 12 m field, that record joins
/// the 12 m source (r3 §1).
#[test]
fn a_matched_field_reaches_both_inputs_lines() {
    let two_windows = |first: f64| Catalogue {
        windows: vec![window(1.0e11, 0), window(2.0e11, 1)],
        partitions: vec![partition(0, 1.0e11, 4), partition(1, 2.0e11, 4)],
        fields: vec![FieldEntry {
            source: Some(SourceId(0)),
            ..field("M51", 1.0, 0.5)
        }],
        sources: vec![SourceEntry {
            name: "M51".to_string(),
            records: vec![record(SourceWindow::Window(WindowId(0)), first, 1.0)],
        }],
        ..Catalogue::default()
    };
    let twelve = two_windows(2.30e11);
    let mut seven = two_windows(2.30e11);
    seven.sources[0].records = vec![record(SourceWindow::Window(WindowId(1)), 2.20e11, 1.0)];
    let merged = merge(&[&twelve, &seven], &tolerance()).expect("merge");
    assert_eq!(merged.inputs[1].fields, [FieldId(0)]);
    assert_eq!(merged.sources.len(), 1, "{:?}", merged.sources);
    let merged_field = FieldId(0);
    let merged_catalogue = Catalogue {
        fields: merged.fields.clone(),
        sources: merged.sources.clone(),
        ..Catalogue::default()
    };
    let rest = |window: u32| -> Vec<f64> {
        merged_catalogue
            .source_records(merged_field, WindowId(window))
            .iter()
            .flat_map(|record| record.rest_frequencies.clone().unwrap_or_default())
            .map(|frequency| frequency.hz())
            .collect()
    };
    assert_eq!(rest(0), [2.30e11]);
    assert_eq!(rest(1), [2.20e11]);
    // Without a matched field, the 7 m source is its own.
    let mut apart = seven.clone();
    apart.fields[0] = FieldEntry {
        source: Some(SourceId(0)),
        ..field("M51", 1.5, 0.5)
    };
    let merged = merge(&[&twelve, &apart], &tolerance()).expect("merge");
    assert_eq!(merged.sources.len(), 2);
}

/// A record for every window is compared with the merged records for every
/// window.
#[test]
fn records_for_every_window_merge_with_their_like() {
    let mut base = with_window(1.0e11, 0, 1.0e11);
    base.sources = vec![SourceEntry {
        name: "M51".to_string(),
        records: vec![record(SourceWindow::All, 2.3e11, 1.0)],
    }];
    let incoming = base.clone();
    let merged = merge(&[&base, &incoming], &tolerance()).expect("merge");
    assert_eq!(merged.sources.len(), 1);
    assert_eq!(merged.sources[0].records.len(), 1);
}
