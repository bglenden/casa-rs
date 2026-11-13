// SPDX-License-Identifier: LGPL-3.0-or-later
//! Correlation-specific grids and variable-cell patch ranges own their charges.
#[path = "../existing_ms_calculations/mod.rs"]
mod fixture;
use super::{
    EpochRef, MEpoch, PredictSpec, REFERENCE_MJD, Simulator, SimulatorOptions, alone,
    charge_process_caches, resources,
};
use std::path::{Path, PathBuf};

fn source() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_mixed_correlations/full_linear.ms")
}

fn opened(path: &Path) -> Simulator {
    Simulator::open_from_ms(
        path,
        SimulatorOptions {
            measures: casa_test_support::deterministic_measures_provider(),
            resources: resources(512 << 20),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        },
    )
    .unwrap()
}

#[test]
fn correlation_specific_grids_keep_their_charge() {
    if !alone("mixed_correlations::correlation_specific_grids_keep_their_charge") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("mixed.ms");
    fixture::copy(&source(), &path);
    for size in [64, 128, 512] {
        let model = directory.path().join(format!("model-{size}.image"));
        super::image_stokes::model(&model, size);
        let mut sm = opened(&path);
        let predict = || {
            sm.predict(&PredictSpec {
                images: vec![model],
                component_lists: Vec::new(),
                incremental: false,
            })
            .unwrap();
            sm
        };
        if size == 64 {
            drop(predict());
        } else {
            drop(super::polynomial_fields::measured(
                &format!("{size} squared, two correlation setups"),
                predict,
            ));
        }
    }
}

// Include the private range owner so its large allocations can be counted
// directly, without generating an unrelated large storage fixture.
#[path = "../../src/write/patch_ranges.rs"]
mod ranges;

#[test]
fn mixed_shape_patch_ranges_hold_admission_as_scans_grow() {
    if !alone("mixed_correlations::mixed_shape_patch_ranges_hold_admission_as_scans_grow") {
        return;
    }
    use super::{
        ALLOWANCE_BYTES, EXCESS, LIVE, Ordering, PEAK, ReservationWindow, excess_now,
        reserved_memory,
    };
    let policy = resources(512 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    for count in [20_000, 200_000] {
        for alternating in [false, true] {
            let reserved = reserved_memory();
            let baseline = LIVE.load(Ordering::Relaxed);
            PEAK.store(baseline, Ordering::Relaxed);
            let before = excess_now();
            EXCESS.store(before, Ordering::Relaxed);
            let window = ReservationWindow::open();
            let runs = ranges::plan(
                (0..count).map(|row| {
                    let parallel = if alternating {
                        row % 2 == 0
                    } else {
                        row < count / 2
                    };
                    (row..row + 1, (if parallel { 2 } else { 4 }, 4))
                }),
                policy.host(),
                policy.policy(),
            )
            .unwrap();
            let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
            let admitted = window.close().saturating_sub(reserved);
            let beyond = EXCESS.load(Ordering::Relaxed) - before;
            let retained = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
            let kept = reserved_memory().saturating_sub(reserved);
            println!(
                "{count} mixed-shape ranges, alternating={alternating}: peak {peak}/{admitted}, retained {retained}/{kept}, excess {beyond}"
            );
            assert!(peak <= admitted + ALLOWANCE_BYTES);
            assert!(beyond <= ALLOWANCE_BYTES as i64);
            assert!(retained <= kept + ALLOWANCE_BYTES);
            assert_eq!(
                runs.value.iter().map(|(rows, _)| rows.len()).sum::<usize>(),
                count
            );
        }
    }
}
