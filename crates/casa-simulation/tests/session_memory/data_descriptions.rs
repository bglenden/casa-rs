// SPDX-License-Identifier: LGPL-3.0-or-later
//! Data-description aliases retain their admitted mapping buffers.
use super::{
    ALLOWANCE_BYTES, EXCESS, EpochRef, LIVE, MEpoch, Ordering, PEAK, REFERENCE_MJD,
    ReservationWindow, Simulator, SimulatorOptions, alone, charge_process_caches, configured,
    excess_now, reserved_memory, resources,
};
use casa_tables::{Table, TableOptions};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};

#[test]
fn stored_description_mappings_are_admitted_while_retained() {
    if !alone("data_descriptions::stored_description_mappings_are_admitted_while_retained") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for descriptions in [20_000, 200_000] {
        let path = directory
            .path()
            .join(format!("descriptions-{descriptions}.ms"));
        // Empty MAIN isolates the mapping owner; CASA cases cover actual DATA.
        let sm = configured(&path, &measures, 512 << 20, 4);
        sm.close().unwrap();
        let mut table = Table::open(TableOptions::new(path.join("DATA_DESCRIPTION"))).unwrap();
        for _ in 1..descriptions {
            table
                .add_row(RecordValue::new(vec![
                    RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
                    RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
                    RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
                ]))
                .unwrap();
        }
        table
            .save(TableOptions::new(path.join("DATA_DESCRIPTION")))
            .unwrap();
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
            "{descriptions} stored descriptions: peak {peak}/{admitted}, beyond {beyond}, retained {left}/{kept}"
        );
        assert!(peak <= admitted + ALLOWANCE_BYTES);
        assert!(beyond <= ALLOWANCE_BYTES as i64);
        assert!(left <= kept + ALLOWANCE_BYTES);
        assert!(kept >= descriptions as u64 * 16);
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
        .expect("small budget refuses before mapping payload read");
        assert!(error.admission_refused().is_some());
    }
}
