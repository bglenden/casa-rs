// SPDX-License-Identifier: LGPL-3.0-or-later
//! Whole-task memory: observations, TP concatenation/gridding and combination.
use super::*;
use casa_simulation::simalma::simalma;
use serde_json::json;
#[path = "../common/simalma_fixture.rs"]
mod fixture;

#[test]
fn alma_orchestration_stays_within_admission_at_two_image_sizes() {
    if !alone("simalma::alma_orchestration_stays_within_admission_at_two_image_sizes") {
        return;
    }
    casa_tables::set_table_cache_budget_bytes(0);
    let policy = resources(1 << 30);
    charge_process_caches(policy.host(), policy.policy()).unwrap();
    let (_scratch, mut request, _oracle) = fixture::fixture(policy);
    for size in [128, 256] {
        request
            .parameters
            .insert("imsize".into(), json!([size, size]));
        drop(simalma(&request).unwrap());
    }
    for size in [128, 256] {
        request
            .parameters
            .insert("imsize".into(), json!([size, size]));
        let baseline = LIVE.load(Ordering::Relaxed);
        let reserved = reserved_memory();
        PEAK.store(baseline, Ordering::Relaxed);
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let window = ReservationWindow::open();
        drop(simalma(&request).unwrap());
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(baseline);
        let admitted = window.close().saturating_sub(reserved);
        let beyond = EXCESS.load(Ordering::Relaxed) - before;
        let retained = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let kept = reserved_memory().saturating_sub(reserved);
        println!(
            "simalma {size}: peak {peak}/{admitted}, unadmitted {beyond}, retained {retained}/{kept}"
        );
        assert!(peak <= admitted + ALLOWANCE_BYTES);
        assert!(beyond <= ALLOWANCE_BYTES as i64);
        assert!(retained <= kept + ALLOWANCE_BYTES);
    }
}
