// SPDX-License-Identifier: LGPL-3.0-or-later
//! The actual mixed-shape writer stays within its admission as rows grow.
use super::{
    ALLOWANCE_BYTES, EXCESS, FrequencyRef, LIVE, Ordering, PEAK, PredictSpec, ReservationWindow,
    SpectralWindowSpec, alone, charge_process_caches, configured, excess_now, reserved_memory,
    resources, scan_from,
};
use std::path::Path;

#[test]
fn mixed_shape_creation_holds_admission_as_rows_grow() {
    if !alone("mixed_creation::mixed_shape_creation_holds_admission_as_rows_grow") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(320 << 20);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let measures = casa_test_support::deterministic_measures_provider();
    let directory = tempfile::tempdir().unwrap();
    let model =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/data/sm_image_layouts/iquv.image");
    for integrations in [1, 50, 500] {
        let path = directory.path().join(format!("mixed-{integrations}.ms"));
        let mut sm = configured(&path, &measures, 320 << 20, 4);
        sm.observe(&scan_from(0, integrations)).unwrap();
        sm.set_spectral_window(&SpectralWindowSpec {
            name: "mixed".into(),
            start_frequency_hz: 230e9,
            channel_width_hz: 15.625e6,
            resolution_hz: 15.625e6,
            frame: FrequencyRef::TOPO,
            channel_count: 2,
            stokes: "XX XY YX YY".into(),
        })
        .unwrap();
        let mut scan = scan_from(integrations, integrations);
        scan.spectral_window = "mixed".into();
        sm.observe(&scan).unwrap();
        sm.predict(&PredictSpec {
            images: vec![model.clone()],
            ..PredictSpec::default()
        })
        .unwrap();
        if integrations == 1 {
            sm.close().unwrap();
            continue;
        }
        let reserved = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let window = ReservationWindow::open();
        let started = std::time::Instant::now();
        sm.close().unwrap();
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
        let admitted = window.close().saturating_sub(reserved);
        let beyond = EXCESS.load(Ordering::Relaxed) - before;
        println!(
            "{integrations} integrations per shape: peak {peak}/{admitted}, excess {beyond}, {:.3}s",
            started.elapsed().as_secs_f64()
        );
        assert!(peak <= admitted + ALLOWANCE_BYTES);
        assert!(beyond <= ALLOWANCE_BYTES as i64);
    }
}
