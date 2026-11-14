// SPDX-License-Identifier: LGPL-3.0-or-later
//! Multiple retained beam curves keep their profile storage and charges.
use super::*;

#[test]
fn saved_curve_storage_stays_admitted_across_incremental_predictions() {
    if !alone("saved_curves::saved_curve_storage_stays_admitted_across_incremental_predictions") {
        return;
    }
    let policy = resources(BUDGET_320);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let model = directory.path().join("model.image");
    write_model_of(&model, 4, 64);
    let beam =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_saved_curves/numeric_voltage.vp");
    let mut few = None;
    for count in [16, 64] {
        let path = directory.path().join(format!("{count}.ms"));
        for _ in 0..MEASURED_RUNS {
            let mut sm = configured(&path, &measures, BUDGET_320, 4);
            sm.set_field(
                "offset",
                &MDirection::from_angles(
                    CENTRE_RAD[0] + 2e-5,
                    CENTRE_RAD[1] - 1e-5,
                    DirectionRef::J2000,
                ),
                "",
                0.,
            )
            .unwrap();
            sm.observe(&scan(2)).unwrap();
            let mut later = scan(2);
            later.source = "offset".into();
            later.start_s += 20.;
            later.stop_s += 20.;
            sm.observe(&later).unwrap();
            sm.set_vp(&VoltagePatternSpec {
                use_default_vp: false,
                vp_table: beam.to_string_lossy().into_owned(),
                do_squint: false,
                ..VoltagePatternSpec::default()
            })
            .unwrap();
            let reserved = reserved_memory();
            let baseline = LIVE.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            let window = ReservationWindow::open();
            let before = excess_now();
            EXCESS.store(before, Ordering::Relaxed);
            for _ in 0..count {
                sm.predict(&PredictSpec {
                    images: vec![model.clone()],
                    incremental: true,
                    ..PredictSpec::default()
                })
                .unwrap();
            }
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
            let (closed, _) = measured_close(sm, &path, false);
            println!("{count} saved curve predictions: {predicted:?}; close {closed:?}");
            assert!(predicted.beyond <= ALLOWANCE_BYTES as i64);
            assert!(predicted.during.heap <= predicted.during.admitted + ALLOWANCE_BYTES);
            assert!(predicted.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
            assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
            if count == 16 {
                few = Some(predicted.kept.admitted);
            } else {
                assert!(predicted.kept.admitted > 2 * few.unwrap());
            }
        }
    }
}
