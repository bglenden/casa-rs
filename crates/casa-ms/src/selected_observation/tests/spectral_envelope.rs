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
