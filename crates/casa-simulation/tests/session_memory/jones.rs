// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory laws of gains (#841 Phase 3): the first law's base
//! and ten-times-the-rows cases, and the patch's rows law, with an `fbm`
//! and a `random` gain term set before the `corrupt`. A row takes its
//! solution at its TIME, which the patch then reads and admits with its
//! batch; the terms themselves are the session's, admitted when they are
//! set (`session_memory/gain_setters.rs`) and held through `close`. Each
//! law shares the binary's counting heap and runs alone as its other laws
//! do ([`super::alone`]).

use std::sync::atomic::AtomicBool;

use casa_simulation::{GainSpec, Simulator};

use super::{
    ALLOWANCE_BYTES, BUDGET_32, CASES, Ordering, ROWS_ALLOWANCE_BYTES, alone,
    charge_process_caches, measure, resources, warm, write_batch_bytes,
};

/// Whether [`set_gains`] sets gain terms: the gain laws set it, each alone
/// in its process.
static GAINS: AtomicBool = AtomicBool::new(false);

/// An `fbm` and a `random` gain term on `sm`, once a gain law sets
/// [`GAINS`].
pub(super) fn set_gains(sm: &mut Simulator) {
    if !GAINS.load(Ordering::Relaxed) {
        return;
    }
    for mode in ["fbm", "random"] {
        sm.set_gain(&GainSpec {
            mode: mode.to_string(),
            table: String::new(),
            amplitude: vec![0.05],
        })
        .expect("setgain");
    }
}

#[test]
fn a_gain_session_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("jones::a_gain_session_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    GAINS.store(true, Ordering::Relaxed);
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let cases = [CASES[0], CASES[1]];
    let passes = cases.map(|case| warm(case, root.path(), &measures));
    let held = cases.map(|case| measure(case, root.path(), &measures));
    for ((case, pass), session) in cases.iter().zip(&passes).zip(&held) {
        println!(
            "{} with gains: {} rows in {} batches of {}: heap {} to {}, admitted {}",
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
    // The terms grow with the integrations and hold their admission from
    // `setgain` (#890 review), so `close`'s writer takes its share of what
    // is left and batches smaller: the session's admission need not grow.
    // Ten times the rows hold no more beyond whatever more was admitted
    // but a little.
    let ([few, many], [_, many_pass]) = (held, passes);
    let allowance = ROWS_ALLOWANCE_BYTES + write_batch_bytes(cases[1], &many_pass);
    let admitted_growth = many.admitted.saturating_sub(few.admitted);
    println!(
        "rows x10 - base with gains: {} bytes, {} admitted beside {} (allowed {allowance})",
        many.least as i64 - few.least as i64,
        many.admitted,
        few.admitted
    );
    assert!(
        many.least <= few.least + admitted_growth + allowance,
        "rows x10 with gains: the heap grew {} bytes, against {} for the base and {admitted_growth} \
         more admitted",
        many.least,
        few.least
    );
}

/// The patch's rows law with gains: the patch also reads each row's TIME.
#[test]
fn patching_with_gains_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("jones::patching_with_gains_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    GAINS.store(true, Ordering::Relaxed);
    super::patch::patches_hold_their_admission_for_more_rows();
}
