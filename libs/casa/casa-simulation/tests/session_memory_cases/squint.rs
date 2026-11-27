// SPDX-License-Identifier: LGPL-3.0-or-later
//! Full circular fluxes, a beam table and worker caches keep their charges.
use super::*;

fn measured(path: &Path, list: &Path, measures: &Arc<dyn MeasuresProvider>) -> (Predicted, Closed) {
    let mut sm = configured_with_beam(
        path,
        measures,
        BUDGET_320,
        16,
        &[25.],
        "RR RL LR LL",
        ("VLA", 1.4e9),
    );
    sm.observe(&scan(2)).unwrap();
    let mut later = scan(2);
    later.start_s += 3600.;
    later.stop_s += 3600.;
    sm.observe(&later).unwrap();
    sm.set_vp(&VoltagePatternSpec {
        parallactic_angle_increment_deg: 0.01,
        ..VoltagePatternSpec::default()
    })
    .unwrap();
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    sm.predict(&PredictSpec {
        component_lists: vec![list.to_path_buf()],
        ..PredictSpec::default()
    })
    .unwrap();
    let predicted = Predicted {
        during: Closed {
            heap: PEAK.load(Ordering::Relaxed) - baseline,
            admitted: window.close() - reserved,
        },
        beyond: EXCESS.load(Ordering::Relaxed) - before,
        kept: Closed {
            heap: LIVE.load(Ordering::Relaxed).saturating_sub(baseline),
            admitted: reserved_memory() - reserved,
        },
    };
    drop(window);
    let (closed, pass) = measured_close(sm, path, true);
    assert!(pass.unwrap().batches > 0);
    (predicted, closed)
}

#[test]
fn full_polarization_fluxes_and_squint_caches_stay_within_their_admission() {
    if !alone("squint::full_polarization_fluxes_and_squint_caches_stay_within_their_admission") {
        return;
    }
    let policy = resources(BUDGET_320);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("squint.ms");
    for count in [64, 2048] {
        let list = directory.path().join(format!("{count}.cl"));
        write_components(&list, count);
        measured(&path, &list, &measures);
        for _ in 0..MEASURED_RUNS {
            let (predicted, closed) = measured(&path, &list, &measures);
            println!("{count} squinted components: {predicted:?}; close {closed:?}");
            assert!(predicted.beyond <= ALLOWANCE_BYTES as i64);
            assert!(predicted.during.heap <= predicted.during.admitted + ALLOWANCE_BYTES);
            assert!(predicted.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
            assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
        }
    }
}
