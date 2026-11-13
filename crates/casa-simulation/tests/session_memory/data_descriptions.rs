// SPDX-License-Identifier: LGPL-3.0-or-later
//! Data-description aliases retain their admitted mapping buffers.
use super::{
    ALLOWANCE_BYTES, EXCESS, EpochRef, LIVE, MEpoch, Ordering, PEAK, REFERENCE_MJD,
    ReservationWindow, Simulator, SimulatorOptions, alone, charge_process_caches, configured,
    excess_now, measured_close, noise, reserved_memory, resources, scan, scan_from,
};
use casa_tables::{Table, TableOptions};
use casa_types::{RecordField, RecordValue, ScalarValue, Value};

#[test]
fn mapped_patch_is_admitted_before_expansion() {
    check(
        false,
        "data_descriptions::mapped_patch_is_admitted_before_expansion",
    );
}

#[test]
fn mapped_append_is_bounded_without_materializing_descriptions() {
    check(
        true,
        "data_descriptions::mapped_append_is_bounded_without_materializing_descriptions",
    );
}

fn check(append: bool, name: &str) {
    if !alone(name) {
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
        // A populated MS exercises read ownership and the patch/append consumers.
        let mut sm = configured(&path, &measures, 512 << 20, 4);
        sm.observe(&scan(1)).unwrap();
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
        let mut opened = Simulator::open_from_ms(
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
        if append {
            opened
                .set_times(10., false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
                .unwrap();
            opened.observe(&scan_from(1, 1)).unwrap();
        } else {
            opened.set_noise(&noise()).unwrap();
            opened.corrupt().unwrap();
        }
        let before_close = excess_now();
        EXCESS.store(before_close, Ordering::Relaxed);
        let (closed, _) = measured_close(opened, &path, false);
        let beyond_close = EXCESS.load(Ordering::Relaxed) - before_close;
        println!(
            "{descriptions} DD append={append}: close heap {}, admitted {}, beyond {beyond_close}",
            closed.heap, closed.admitted
        );
        assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
        assert!(beyond_close <= ALLOWANCE_BYTES as i64);
    }
}
