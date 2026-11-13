// SPDX-License-Identifier: LGPL-3.0-or-later
//! Explicit observed channel axes retain the reservation admitted before reading.
use super::{
    ALLOWANCE_BYTES, EXCESS, EpochRef, LIVE, MEpoch, Ordering, PEAK, REFERENCE_MJD,
    ReservationWindow, Simulator, SimulatorOptions, alone, charge_process_caches, configured,
    excess_now, reserved_memory, resources,
};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, Value};

#[test]
fn stored_channel_axes_are_admitted_before_read_and_while_retained() {
    if !alone("irregular_channels::stored_channel_axes_are_admitted_before_read_and_while_retained")
    {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for channels in [20_000, 200_000] {
        let path = directory.path().join(format!("channels-{channels}.ms"));
        // An empty MS isolates channel-axis ownership from MAIN's wide-row
        // writer buffers; the independent CASA cases exercise populated rows.
        let sm = configured(&path, &measures, 512 << 20, channels);
        sm.close().unwrap();
        let mut table = Table::open(TableOptions::new(path.join("SPECTRAL_WINDOW"))).unwrap();
        let axis: Vec<_> = (0..channels)
            .map(|channel| 230e9 + channel as f64 * 1e6 + (channel % 3) as f64 * 0.1e6)
            .collect();
        table
            .cell_accessor_mut(0, "CHAN_FREQ")
            .unwrap()
            .set(Value::Array(ArrayValue::Float64(
                ndarray::ArrayD::from_shape_vec(ndarray::IxDyn(&[channels]), axis).unwrap(),
            )))
            .unwrap();
        table.flush().unwrap();
        drop(table);
        let reserved = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let window = ReservationWindow::open();
        let opened = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: measures.clone(),
                resources: resources(128 << 20),
                now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .unwrap();
        let peak = PEAK.load(Ordering::Relaxed) - baseline;
        let admitted = window.close() - reserved;
        let beyond = EXCESS.load(Ordering::Relaxed) - before;
        let left = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let kept = reserved_memory() - reserved;
        println!(
            "{channels} stored channels: peak {peak}/{admitted}, beyond {beyond}, retained {left}/{kept}"
        );
        assert!(peak <= admitted + ALLOWANCE_BYTES);
        assert!(beyond <= ALLOWANCE_BYTES as i64);
        assert!(left <= kept + ALLOWANCE_BYTES);
        assert!(kept >= channels as u64 * 8);
        drop(opened);
        let error = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: measures.clone(),
                resources: resources(1024),
                now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .err()
        .expect("small budget refuses before channel payload read");
        assert!(error.admission_refused().is_some());
    }
}
