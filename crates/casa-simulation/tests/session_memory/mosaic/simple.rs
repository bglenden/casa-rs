// SPDX-License-Identifier: LGPL-3.0-or-later
//! SimplePBConvFunc's fine screens and frequency cells retain their charge.
use super::super::*;

#[test]
fn common_beam_screens_and_cells_stay_within_their_admission() {
    if !alone("mosaic::simple::common_beam_screens_and_cells_stay_within_their_admission") {
        return;
    }
    let policy = resources(2 << 30);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("simple.ms");
    let mut first_kept = None;
    for channels in [4, 4096] {
        let model = directory.path().join(format!("{channels}.image"));
        write_model_of(&model, channels, 64);
        for _ in 0..MEASURED_RUNS {
            let mut sm = configured_with_beam(
                &path,
                &measures,
                2 << 30,
                channels,
                &[6.],
                "XX YY",
                ("SMA", FREQUENCY_HZ),
            );
            sm.observe(&scan(2)).unwrap();
            sm.set_options(&PredictionOptions {
                ftmachine: "mosaic".into(),
                ..PredictionOptions::default()
            })
            .unwrap();
            let reserved = reserved_memory();
            let baseline = LIVE.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            let window = ReservationWindow::open();
            let before = excess_now();
            EXCESS.store(before, Ordering::Relaxed);
            sm.predict(&PredictSpec {
                images: vec![model.clone()],
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
            let (closed, _) = measured_close(sm, &path, false);
            println!("{channels} common-beam channels: {predicted:?}; close {closed:?}");
            assert!(predicted.beyond <= ALLOWANCE_BYTES as i64);
            assert!(predicted.during.heap <= predicted.during.admitted + ALLOWANCE_BYTES);
            assert!(predicted.kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES);
            assert!(closed.heap <= closed.admitted + ALLOWANCE_BYTES);
            if channels == 4 {
                first_kept = Some(predicted.kept.admitted);
            } else {
                assert!(
                    predicted.kept.admitted > 2 * first_kept.unwrap(),
                    "the wide window must retain multiple admitted beam-frequency cells"
                );
            }
        }
    }
}
