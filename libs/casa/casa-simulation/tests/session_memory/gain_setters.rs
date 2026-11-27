// SPDX-License-Identifier: LGPL-3.0-or-later
//! The gain and leakage setters' law (#890 review): `setgain` and
//! `setleakage` simulate their term over the rows observed so far, which
//! grows with the integrations, so each admits what the term keeps and what
//! forming it holds before it forms anything, and keeps the term's share
//! for as long as the term lives.
//!
//! Over 20,000 integrations of 6 antennas, each setter's heap stays within
//! what the reservations held while it ran, and what it leaves live stays
//! within what they keep after; `reset` drops the terms and their charge.
//! FFTW's plans are C heap, which this binary does not count; they are
//! admitted all the same. The law runs alone in its process, as the
//! binary's other laws do ([`super::alone`]).

use casa_simulation::{GainSpec, LeakageSpec, Simulator};

use super::{
    LIVE, Ordering, PEAK, ReservationWindow, charge_process_caches, configured, reserved_memory,
    resources, scan,
};

/// The session's budget beside the process caches: ample for the terms.
const BUDGET: u64 = 64 << 20;

/// Integrations observed before the setters run.
const INTEGRATIONS: usize = 20_000;

/// Heap a setter may hold beyond its reservation: the shared term's
/// header, the session's list of terms, and its log lines.
const SETTER_ALLOWANCE_BYTES: u64 = 64 * 1024;

/// A setter, named.
type Setter = (&'static str, fn(&mut Simulator));

/// One setter of each kind.
const SETTERS: [Setter; 3] = [
    ("setgain fbm", |sm| set_gain(sm, "fbm")),
    ("setgain random", |sm| set_gain(sm, "random")),
    ("setleakage", |sm| {
        sm.set_leakage(&LeakageSpec {
            table: String::new(),
            amplitude: vec![0.01],
            offset: vec![0.0],
        })
        .expect("setleakage");
    }),
];

fn set_gain(sm: &mut Simulator, mode: &str) {
    sm.set_gain(&GainSpec {
        mode: mode.to_string(),
        table: String::new(),
        amplitude: vec![0.05],
    })
    .expect("setgain");
}

#[test]
fn the_gain_setters_admit_what_they_form_and_keep_it_with_the_term() {
    if !super::alone(
        "gain_setters::the_gain_setters_admit_what_they_form_and_keep_it_with_the_term",
    ) {
        return;
    }
    let resources = resources(BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let mut sm = configured(&root.path().join("gains.ms"), &measures, BUDGET, 32);
    sm.observe(&scan(INTEGRATIONS)).expect("observe");
    // Once unmeasured, so the log's callsites and the planner's state are
    // the process's before any is measured.
    for (_, set) in SETTERS {
        set(&mut sm);
    }
    sm.reset();
    let unset = reserved_memory();
    for (name, set) in SETTERS {
        let reserved = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let window = ReservationWindow::open();
        set(&mut sm);
        let admitted = window.close() - reserved;
        let peak = PEAK.load(Ordering::Relaxed) - baseline;
        let left = LIVE.load(Ordering::Relaxed) - baseline;
        let kept = reserved_memory() - reserved;
        println!("{name}: heap peak {peak}, admitted {admitted}; left {left}, kept {kept}");
        assert!(
            peak <= admitted + SETTER_ALLOWANCE_BYTES,
            "{name}: the heap grew {peak} bytes, beyond the {admitted} admitted"
        );
        assert!(
            left <= kept + SETTER_ALLOWANCE_BYTES,
            "{name}: {left} bytes left live, beyond the {kept} the term keeps charged"
        );
        assert!(
            kept > 0 && kept <= left + SETTER_ALLOWANCE_BYTES,
            "{name}: the term keeps charged what it keeps ({kept} for {left} live)"
        );
    }
    sm.reset();
    assert_eq!(
        reserved_memory(),
        unset,
        "reset drops the terms and their charge"
    );
}
