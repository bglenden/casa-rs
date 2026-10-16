// SPDX-License-Identifier: LGPL-3.0-or-later
//! The MFS spectral envelope follows CASA's `MSUtil::getFreqRangeInSpw`:
//! one ordered converter per spectral window over the ascending
//! `TIME·(DATA_DESC_ID+1)` keys (#713).

use super::*;
use crate::SelectedObservationSpectralWindow;
use crate::derived::engine::raw_field_phase_direction;
use casa_types::measures::frequency::MFrequencyConverter;

/// A two-hour, three-antenna track at 230 GHz: 120 one-minute integrations,
/// long enough that the converter's 0.04-day aberration anchor refreshes.
fn two_hour_track(path: &std::path::Path) -> MeasurementSet {
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        "unused.fits",
        path,
        tutorial_vla_a_antennas().into_iter().take(3).collect(),
    );
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.duration_seconds = 7_200.0;
    request.integration_seconds = 60.0;
    request.spectral_windows = vec![SyntheticSpectralSetup {
        name: "envelope".to_string(),
        start_frequency_hz: 230.0e9,
        channel_width_hz: 2.0e6,
        channel_count: 16,
    }];
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    generate_synthetic_observation_ms(&request).expect("generate the two-hour track");
    MeasurementSet::open(path).expect("open the two-hour track")
}

#[test]
fn the_mfs_envelope_drives_one_ordered_converter_whatever_the_row_order() {
    let directory = tempfile::tempdir().expect("temporary envelope fixture");
    let measurement_set = two_hour_track(&directory.path().join("envelope.ms"));
    let engine = MsCalEngine::new(&measurement_set).expect("bind the frame engine");
    let selection = measurement_set
        .selected_observation_row_selection(&[0], None, None, None)
        .expect("select every row");
    let mut rows = Vec::new();
    measurement_set
        .visit_selected_observation_rows(
            &selection,
            MsSelectionIoBudget {
                available_bytes: 64 * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                maximum_live_blocks: 2,
                requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
                storage_alignment_rows: None,
            },
            |row| rows.push(row),
        )
        .expect("visit the selected rows");
    let window = measurement_set.spectral_window().expect("SPECTRAL_WINDOW");
    let frequencies_hz = window.chan_freq(0).expect("CHAN_FREQ");
    let widths_hz = window.chan_width(0).expect("CHAN_WIDTH");
    let channels = (0..frequencies_hz.len()).collect::<Vec<_>>();
    let edges = [
        frequencies_hz[0] - widths_hz[0].abs() / 2.0,
        frequencies_hz[15] + widths_hz[15].abs() / 2.0,
    ];
    let envelope = |rows: &mut dyn Iterator<Item = SelectedObservationRow>| {
        let mut reducer = measurement_set
            .selected_observation_spectral_envelope_reducer(
                &selection,
                [SelectedObservationSpectralWindow::borrow_selected(
                    0,
                    FrequencyRef::TOPO,
                    &frequencies_hz,
                    &widths_hz,
                    &channels,
                )],
                FrequencyRef::LSRK,
                &engine,
                1 << 30,
            )
            .expect("plan the envelope");
        for row in rows {
            reducer.observe(row).expect("observe a row");
        }
        reducer.finish().expect("finish the envelope").edges_hz()
    };

    // CASA's loop: ascending unique times, one converter, low edge then
    // high edge at each.
    let mut times = rows
        .iter()
        .map(|row| row.time_mjd_seconds())
        .collect::<Vec<_>>();
    times.sort_by(f64::total_cmp);
    times.dedup();
    assert_eq!(times.len(), 120);
    let direction = raw_field_phase_direction(&measurement_set, 0).expect("FIELD 0 direction");
    let frame = |time| {
        engine
            .spectral_frame_observatory_direction(time, direction.clone())
            .expect("spectral frame")
    };
    let mut ordered = MFrequencyConverter::new(FrequencyRef::TOPO, FrequencyRef::LSRK);
    let mut expected = [f64::INFINITY, f64::NEG_INFINITY];
    let mut largest_anchor_effect_hz = 0.0_f64;
    for &time in &times {
        for edge in edges {
            let hz = ordered
                .convert_hz(edge, &frame(time))
                .expect("ordered conversion")
                .hz();
            expected = [expected[0].min(hz), expected[1].max(hz)];
            let exact = MFrequencyConverter::new(FrequencyRef::TOPO, FrequencyRef::LSRK)
                .convert_hz(edge, &frame(time))
                .expect("fresh conversion")
                .hz();
            largest_anchor_effect_hz = largest_anchor_effect_hz.max((hz - exact).abs());
        }
    }

    assert_eq!(envelope(&mut rows.iter().copied()), expected);
    assert_eq!(
        envelope(&mut rows.iter().rev().copied()),
        expected,
        "the envelope depends on the keys, not the traversal order"
    );
    // The law sees the converter's state: a fresh exact conversion per time
    // differs by a measurable fraction of a hertz somewhere on the track.
    assert!(
        largest_anchor_effect_hz > 0.01,
        "anchored and exact conversions differ by only {largest_anchor_effect_hz} Hz"
    );
}

/// One window of CASA's loop: its spectral window (and data description),
/// source frame and selected edges.
struct LoopWindow {
    spw: u32,
    source: FrequencyRef,
    edges_hz: [f64; 2],
}

/// CASA's `MSUtil::getFreqRangeInSpw`, written plainly: each
/// `TIME·(DATA_DESC_ID+1)` key stands for its lowest physical row; per
/// window, REST edges and edges already in the (non-TOPO) output frame fold
/// as they are, others convert through one converter over the window's keys
/// in ascending order, at each key's row's FIELD direction.
fn casa_loop(
    ms: &MeasurementSet,
    engine: &MsCalEngine,
    windows: &[LoopWindow],
    rows: &[SelectedObservationRow],
) -> [f64; 2] {
    let key = |row: &SelectedObservationRow| {
        row.time_mjd_seconds() * (f64::from(row.data_description_id()) + 1.0)
    };
    let mut keyed = rows.to_vec();
    keyed.sort_by(|a, b| {
        key(a)
            .total_cmp(&key(b))
            .then(a.physical_row().cmp(&b.physical_row()))
    });
    keyed.dedup_by(|later, earlier| key(later) == key(earlier));
    let mut edges = [f64::INFINITY, f64::NEG_INFINITY];
    for window in windows {
        if window.source == FrequencyRef::REST || window.source == FrequencyRef::LSRK {
            edges = [
                edges[0].min(window.edges_hz[0]),
                edges[1].max(window.edges_hz[1]),
            ];
            continue;
        }
        let mut converter = MFrequencyConverter::new(window.source, FrequencyRef::LSRK);
        for row in keyed
            .iter()
            .filter(|row| row.data_description_id() == window.spw as i32)
        {
            let direction = raw_field_phase_direction(ms, row.field_id() as usize).expect("FIELD");
            let frame = engine
                .spectral_frame_observatory_direction(row.time_mjd_seconds(), direction)
                .expect("spectral frame");
            for edge in window.edges_hz {
                let hz = converter.convert_hz(edge, &frame).expect("conversion").hz();
                edges = [edges[0].min(hz), edges[1].max(hz)];
            }
        }
    }
    edges
}

/// Two spectral windows (two data descriptions) and two fields 30° apart.
/// Each key's lowest physical row decides the FIELD direction it converts
/// at, whatever order the rows arrive in: copies of every row with the
/// other field, at lower physical rows, arrive after the originals and
/// still stand for their keys. A REST window and a window already in the
/// output frame fold their edges unconverted.
#[test]
fn each_key_converts_at_its_lowest_rows_field_window_by_window() {
    let directory = tempfile::tempdir().expect("temporary envelope fixture");
    let path = directory.path().join("two-windows.ms");
    let mut request = SyntheticObservationRequest::vla_ppdisk(
        "unused.fits",
        &path,
        tutorial_vla_a_antennas().into_iter().take(3).collect(),
    );
    request.predict_model = false;
    request.allow_below_elevation_limit = true;
    request.duration_seconds = 7_200.0;
    request.integration_seconds = 300.0;
    request.fields = [0.4, -0.12]
        .into_iter()
        .enumerate()
        .map(|(index, declination)| crate::SyntheticField {
            name: format!("field{index}"),
            phase_center_rad: [4.0, declination],
        })
        .collect();
    request.spectral_windows = [230.0e9, 100.0e9]
        .map(|start_frequency_hz| SyntheticSpectralSetup {
            name: format!("{start_frequency_hz}"),
            start_frequency_hz,
            channel_width_hz: 2.0e6,
            channel_count: 4,
        })
        .to_vec();
    request.worker_policy = SyntheticWorkerPolicy::Fixed;
    request.row_workers = Some(1);
    request.channel_workers = Some(1);
    generate_synthetic_observation_ms(&request).expect("generate the two-window track");
    let ms = MeasurementSet::open(&path).expect("open the two-window track");
    let engine = MsCalEngine::new(&ms).expect("bind the frame engine");
    let selection = ms
        .selected_observation_row_selection(&[0, 1], None, None, None)
        .expect("select both data descriptions");
    let mut rows = Vec::new();
    ms.visit_selected_observation_rows(
        &selection,
        MsSelectionIoBudget {
            available_bytes: 64 * SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            maximum_live_blocks: 2,
            requested_bytes_per_row: SelectedObservationRow::STORAGE_BYTES_PER_ROW,
            storage_alignment_rows: None,
        },
        |row| rows.push(row),
    )
    .expect("visit the selected rows");
    let spectral = ms.spectral_window().expect("SPECTRAL_WINDOW");
    let lattices = (0..2)
        .map(|spw| {
            (
                spectral.chan_freq(spw).expect("CHAN_FREQ"),
                spectral.chan_width(spw).expect("CHAN_WIDTH"),
            )
        })
        .collect::<Vec<_>>();
    let channels = (0..4).collect::<Vec<_>>();
    let edges_of = |spw: usize| {
        let (frequencies, widths) = &lattices[spw];
        [
            frequencies[0] - widths[0].abs() / 2.0,
            frequencies[3] + widths[3].abs() / 2.0,
        ]
    };
    let reduce = |sources: [FrequencyRef; 2],
                  rows: &mut dyn Iterator<Item = SelectedObservationRow>| {
        let mut reducer = ms
            .selected_observation_spectral_envelope_reducer(
                &selection,
                [0_usize, 1].map(|spw| {
                    SelectedObservationSpectralWindow::borrow_selected(
                        spw as u32,
                        sources[spw],
                        &lattices[spw].0,
                        &lattices[spw].1,
                        &channels,
                    )
                }),
                FrequencyRef::LSRK,
                &engine,
                1 << 30,
            )
            .expect("plan the envelope");
        for row in rows {
            reducer.observe(row).expect("observe a row");
        }
        reducer.finish().expect("finish the envelope").edges_hz()
    };
    let loop_windows = |sources: [FrequencyRef; 2]| {
        [0_usize, 1].map(|spw| LoopWindow {
            spw: spw as u32,
            source: sources[spw],
            edges_hz: edges_of(spw),
        })
    };

    assert!(
        rows.iter().any(|row| row.field_id() == 0) && rows.iter().any(|row| row.field_id() == 1)
    );
    let count = rows.len();
    let originals = rows
        .iter()
        .map(|row| SelectedObservationRow {
            physical_row: row.physical_row() + count,
            ..*row
        })
        .collect::<Vec<_>>();
    let copies = rows
        .iter()
        .map(|row| SelectedObservationRow {
            field_id: 1 - row.field_id(),
            ..*row
        })
        .collect::<Vec<_>>();
    let topo = [FrequencyRef::TOPO; 2];
    let expected = casa_loop(&ms, &engine, &loop_windows(topo), &copies);
    assert_eq!(
        reduce(topo, &mut originals.iter().chain(&copies).copied()),
        expected,
        "the lower copies stand for their keys though they arrive last"
    );
    assert_ne!(
        expected,
        casa_loop(&ms, &engine, &loop_windows(topo), &originals),
        "the key's field changes the envelope"
    );
    for frame in [FrequencyRef::REST, FrequencyRef::LSRK] {
        let sources = [FrequencyRef::TOPO, frame];
        assert_eq!(
            reduce(sources, &mut rows.iter().copied()),
            casa_loop(&ms, &engine, &loop_windows(sources), &rows),
            "window 1 in {frame:?}"
        );
    }
}
