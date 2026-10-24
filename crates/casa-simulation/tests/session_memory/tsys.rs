// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory law of `tsys-manual` noise (#841 Phase 3): the first
//! law's base and ten-times-the-rows cases with `tsys-manual` noise, whose
//! σ takes each row's airmasses and exposure. Those are in the admitted
//! geometry, and each integration's elevations and airmasses in the
//! workers' admitted scratch, so the session holds its admission, and ten
//! times the rows no more beyond it but a little. It shares the binary's
//! counting heap and runs alone as its other laws do ([`super::alone`]);
//! the patch's rows law with `tsys-manual` noise is in
//! `session_memory/patch.rs`.

use super::{
    ALLOWANCE_BYTES, BUDGET_32, CASES, Ordering, ROWS_ALLOWANCE_BYTES, TSYS_NOISE, alone,
    charge_process_caches, measure, resources, warm, write_batch_bytes,
};

#[test]
fn a_tsys_session_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("tsys::a_tsys_session_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    TSYS_NOISE.store(true, Ordering::Relaxed);
    holds_its_admission_and_no_more_for_more_rows("tsys-manual");
}

/// The law, the noise [`super::noise`] gives being `mode`'s.
pub(super) fn holds_its_admission_and_no_more_for_more_rows(mode: &str) {
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let cases = [CASES[0], CASES[1]];
    let passes = cases.map(|case| warm(case, root.path(), &measures));
    let held = cases.map(|case| measure(case, root.path(), &measures));
    for ((case, pass), session) in cases.iter().zip(&passes).zip(&held) {
        println!(
            "{} with {mode}: {} rows in {} batches of {}: heap {} to {}, admitted {}",
            case.name,
            pass.rows,
            pass.batches,
            pass.batch_rows,
            session.least,
            session.most,
            session.admitted
        );
        assert!(
            session.most <= session.admitted + ALLOWANCE_BYTES,
            "{}: the heap grew {} bytes, beyond the {} admitted and the allowance",
            case.name,
            session.most,
            session.admitted
        );
    }
    let ([few, many], [_, many_pass]) = (held, passes);
    let allowance = ROWS_ALLOWANCE_BYTES + write_batch_bytes(cases[1], &many_pass);
    println!(
        "rows x10 - base with {mode}: {} bytes (allowed {allowance})",
        many.least as i64 - few.least as i64
    );
    assert!(
        many.least <= few.least + allowance,
        "rows x10 with {mode}: the heap grew {} bytes, against {} for the base",
        many.least,
        few.least
    );
}
