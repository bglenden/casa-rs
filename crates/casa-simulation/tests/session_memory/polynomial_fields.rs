// SPDX-License-Identifier: LGPL-3.0-or-later
//! Polynomial payload reads and selected chunk state retain their charges.
use super::{
    ALLOWANCE_BYTES, EXCESS, EpochRef, LIVE, MEpoch, Ordering, PEAK, PredictSpec, REFERENCE_MJD,
    ReservationWindow, Simulator, SimulatorOptions, alone, charge_process_caches, configured,
    excess_now, reserved_memory, resources, scan,
};
use casa_tables::{Table, TableOptions};
use casa_types::{ArrayValue, RecordField, RecordValue, ScalarValue, Value};
use std::path::Path;

fn polynomial(path: &Path, terms: usize) {
    let mut table = Table::open(TableOptions::new(path.join("FIELD"))).unwrap();
    let mut values = ndarray::ArrayD::zeros(ndarray::IxDyn(&[2, terms]));
    values[[0, 0]] = super::CENTRE_RAD[0];
    values[[1, 0]] = super::CENTRE_RAD[1];
    for column in ["PHASE_DIR", "DELAY_DIR", "REFERENCE_DIR"] {
        table
            .cell_accessor_mut(0, column)
            .unwrap()
            .set(Value::Array(ArrayValue::Float64(values.clone())))
            .unwrap();
    }
    table
        .cell_accessor_mut(0, "NUM_POLY")
        .unwrap()
        .set(Value::Scalar(ScalarValue::Int32(terms as i32 - 1)))
        .unwrap();
    table.save(TableOptions::new(path.join("FIELD"))).unwrap();
}

pub(super) fn measured(label: &str, operation: impl FnOnce() -> Simulator) -> Simulator {
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let started = std::time::Instant::now();
    let sm = operation();
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    let admitted = window.close().saturating_sub(reserved);
    let beyond = EXCESS.load(Ordering::Relaxed) - before;
    let left = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let kept = reserved_memory().saturating_sub(reserved);
    println!(
        "{label}: peak {peak}/{admitted}, beyond {beyond}, retained {left}/{kept}, wall {:.3}s",
        started.elapsed().as_secs_f64()
    );
    assert!(peak <= admitted + ALLOWANCE_BYTES);
    assert!(beyond <= ALLOWANCE_BYTES as i64);
    assert!(left <= kept + ALLOWANCE_BYTES);
    sm
}

#[test]
fn polynomial_payload_is_admitted_before_decoding() {
    if !alone("polynomial_fields::polynomial_payload_is_admitted_before_decoding") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    for terms in [20_000, 200_000] {
        let path = directory.path().join(format!("terms-{terms}.ms"));
        configured(&path, &measures, 512 << 20, 4).close().unwrap();
        polynomial(&path, terms);
        let options = |budget| SimulatorOptions {
            measures: measures.clone(),
            resources: resources(budget),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        };
        drop(measured(&format!("{terms} FIELD terms"), || {
            Simulator::open_from_ms(&path, options(128 << 20)).unwrap()
        }));
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let error = Simulator::open_from_ms(&path, options(1 << 20))
            .err()
            .expect("polynomial demand exceeds a megabyte free");
        let refused = error.admission_refused().expect("admission refusal");
        assert_eq!(refused.phase, "reading stored simulation FIELD polynomials");
        assert!(
            PEAK.load(Ordering::Relaxed).saturating_sub(baseline) <= ALLOWANCE_BYTES,
            "refuse before decoding a polynomial payload"
        );
    }
}

#[test]
fn selected_polynomial_chunks_keep_their_admitted_snapshot() {
    if !alone("polynomial_fields::selected_polynomial_chunks_keep_their_admitted_snapshot") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let model = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/data/sm_irregular_channels/spectrum.image");
    for count in [64usize, 20_000, 200_000] {
        let path = directory.path().join(format!("chunks-{count}.ms"));
        let rows = chunked_ms(&path, count, &measures);
        polynomial(&path, 2);
        let mut sm = Simulator::open_from_ms(
            &path,
            SimulatorOptions {
                measures: measures.clone(),
                resources: resources(512 << 20),
                now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
            },
        )
        .unwrap();
        sm.set_data(&[], &[], "TIME >= 0").unwrap();
        let predict = || {
            sm.predict(&PredictSpec {
                images: vec![model.clone()],
                component_lists: Vec::new(),
                incremental: false,
            })
            .unwrap();
            sm
        };
        if count == 64 {
            drop(predict());
        } else {
            drop(measured(
                &format!("{rows} selected polynomial chunks"),
                predict,
            ));
        }
    }
}

pub(super) fn chunked_ms(
    path: &Path,
    count: usize,
    measures: &std::sync::Arc<dyn casa_types::measures::MeasuresProvider>,
) -> usize {
    let mut initial = configured(path, measures, 512 << 20, 4);
    initial.observe(&scan(count.div_ceil(21))).unwrap();
    initial.close().unwrap();
    let rows = Table::open(TableOptions::new(path)).unwrap().row_count();
    let mut dd = Table::open(TableOptions::new(path.join("DATA_DESCRIPTION"))).unwrap();
    for _ in 1..rows {
        dd.add_row(RecordValue::new(vec![
            RecordField::new("SPECTRAL_WINDOW_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("POLARIZATION_ID", Value::Scalar(ScalarValue::Int32(0))),
            RecordField::new("FLAG_ROW", Value::Scalar(ScalarValue::Bool(false))),
        ]))
        .unwrap();
    }
    dd.save(TableOptions::new(path.join("DATA_DESCRIPTION")))
        .unwrap();
    drop(dd);
    super::patch::index_rows(path, "DATA_DESC_ID", 0);
    rows
}
