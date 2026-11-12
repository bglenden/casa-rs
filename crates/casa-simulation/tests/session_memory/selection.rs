// SPDX-License-Identifier: LGPL-3.0-or-later
//! Fragmented row selections keep bounded evaluation and batch scratch,
//! including existing scratch columns and increments after Jones (#1053).

#[path = "../selection/input.rs"]
pub(super) mod input;

use std::path::Path;
use std::sync::Arc;

use casa_simulation::{ApplySpec, NoiseSpec, PredictSpec, Simulator, SimulatorOptions};
use casa_types::measures::{EpochRef, MEpoch, MeasuresProvider};

use super::{
    ALLOWANCE_BYTES, BUDGET_32, Closed, LIVE, MEASURED_RUNS, Ordering, PEAK, REFERENCE_MJD,
    ROWS_ALLOWANCE_BYTES, ReservationWindow, alone, charge_process_caches, configured,
    measured_close, reserved_memory, resources, scan, write_model,
};

fn select(sm: &mut Simulator, query: &str) -> (u64, u64) {
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    sm.set_data(&[], &[], query).expect("row selection");
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
    let left = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let admitted = window.close().saturating_sub(reserved);
    assert!(
        peak <= admitted + 64 * 1024,
        "setdata peak {peak}, admitted {admitted}"
    );
    assert!(
        left <= 64 * 1024,
        "setdata retains bounded metadata, held {left}"
    );
    (peak, left)
}

fn run(
    root: &Path,
    model: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    integrations: usize,
    opened: bool,
) -> (Closed, (u64, u64)) {
    let input = root.join("input.ms");
    let calibration = root.join("gains.cal");
    let output = root.join("fresh.ms");
    let mut sm = configured(&input, measures, BUDGET_32, 32);
    sm.observe(&scan(integrations)).unwrap();
    sm.close().unwrap();
    input::prepare(&input);
    input::fixed_gains(&input, &calibration, 2.0);
    let (mut sm, path) = if opened {
        (
            Simulator::open_from_ms(
                &input,
                SimulatorOptions {
                    measures: Arc::clone(measures),
                    resources: resources(BUDGET_32),
                    now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
                },
            )
            .unwrap(),
            &input,
        )
    } else {
        let mut sm = configured(&output, measures, BUDGET_32, 32);
        sm.observe(&scan(integrations)).unwrap();
        (sm, &output)
    };
    let held = select(&mut sm, "abs(ANTENNA1-ANTENNA2)>0");
    let predict = |sm: &mut Simulator, incremental| {
        sm.predict(&PredictSpec {
            images: vec![model.to_path_buf()],
            component_lists: Vec::new(),
            incremental,
        })
        .unwrap();
    };
    predict(&mut sm, true);
    sm.set_noise(&NoiseSpec::simple(0.1)).unwrap();
    sm.corrupt().unwrap();
    select(&mut sm, "ANTENNA1==ANTENNA2");
    predict(&mut sm, false);
    select(&mut sm, "ANTENNA1%2==0");
    predict(&mut sm, true);
    sm.set_apply(&ApplySpec {
        table: calibration.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.0,
        interp: "nearest".to_string(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.0,
    })
    .unwrap();
    sm.corrupt().unwrap();
    predict(&mut sm, true);
    let (closed, summary) = measured_close(sm, path, true);
    let pass = summary.unwrap();
    if integrations == 600 {
        assert!(
            pass.batches > 1,
            "long streams reuse batch buffers: {pass:?}"
        );
    }
    assert!(
        closed.heap <= closed.admitted + ALLOWANCE_BYTES,
        "opened={opened}, {integrations}: heap {}, admitted {}",
        closed.heap,
        closed.admitted
    );
    if !opened {
        std::fs::remove_dir_all(input).unwrap();
    }
    std::fs::remove_dir_all(calibration).unwrap();
    (closed, held)
}

#[test]
fn fragmented_selections_hold_their_admission_without_retaining_rows() {
    if !alone("selection::fragmented_selections_hold_their_admission_without_retaining_rows") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().unwrap();
    let model = root.path().join("model.image");
    write_model(&model, 32);
    for opened in [false, true] {
        for integrations in [60, 600] {
            run(root.path(), &model, &measures, integrations, opened);
        }
        let [few, many] = [60, 600].map(|integrations| {
            let mut best = (
                Closed {
                    heap: u64::MAX,
                    admitted: 0,
                },
                (u64::MAX, u64::MAX),
            );
            for _ in 0..MEASURED_RUNS {
                let (close, selection) = run(root.path(), &model, &measures, integrations, opened);
                println!(
                    "opened={opened}, {integrations}: heap {} admitted {}, query peak {} left {}",
                    close.heap, close.admitted, selection.0, selection.1
                );
                best.0.heap = best.0.heap.min(close.heap);
                best.0.admitted = best.0.admitted.max(close.admitted);
                best.1.0 = best.1.0.min(selection.0);
                best.1.1 = best.1.1.min(selection.1);
            }
            best
        });
        // A longer stream may reserve less for its final batch geometry.
        // A reservation decrease is not heap growth: compare actual growth
        // with any increased admission (the patch's row-index plan).
        let admitted_growth = many.0.admitted.saturating_sub(few.0.admitted);
        assert!(
            many.0.heap <= few.0.heap + admitted_growth + ROWS_ALLOWANCE_BYTES,
            "ten times the rows must not grow unadmitted batch state"
        );
        assert!(many.1.0 <= few.1.0 + 4096, "query scratch is bounded");
        assert!(many.1.1 <= few.1.1 + 4096, "query retains no per-row state");
    }
}
