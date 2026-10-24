// SPDX-License-Identifier: LGPL-3.0-or-later
//! The `setapply` law (#893 review): `setapply` reads its whole table and
//! keeps the lookup it loads, and each `corrupt` evaluates the table over
//! the integrations observed, so both grow with the table or the
//! integrations. `setapply` admits what reading the table holds and what
//! the lookup keeps before it reads, and keeps the lookup's share; `corrupt`
//! admits the term it evaluates as `setgain` admits one it simulates.
//!
//! With a table of 9,996 rows and 2,000 integrations of 6 antennas, the
//! heap stays within what the reservations hold at every allocation of
//! either call, and what each leaves live stays within what they keep
//! after. The law runs alone in its process, as the binary's other laws do
//! ([`super::alone`]).

use std::path::Path;

use casa_calibration::{SimulatedJones, SimulatedSolution, write_simulated_jones_table};
use casa_simulation::{ApplySpec, Simulator};
use num_complex::Complex32;

use super::{
    ANTENNAS, EXCESS, LIVE, Ordering, PEAK, ReservationWindow, charge_process_caches, configured,
    excess_now, reserved_memory, resources, scan,
};

/// The session's budget beside the process caches.
const BUDGET: u64 = 256 << 20;

/// Rows of the applied table: solutions at 1,666 times for each antenna.
const TABLE_ROWS: usize = 9_996;

/// Integrations the session observes before it applies the table.
const INTEGRATIONS: usize = 2_000;

/// Heap a call may hold beyond its reservations: the session's list of
/// terms, the recorded step, and its log lines.
const CALL_ALLOWANCE_BYTES: u64 = 64 * 1024;

/// What one measured call held.
struct Call {
    /// The heap's peak beyond what was live before.
    peak: u64,
    /// The most the reservations held at once.
    admitted: u64,
    /// The most the heap went beyond the reservations, at any allocation.
    beyond: i64,
    /// What the call left live.
    left: u64,
    /// What the reservations kept after.
    kept: u64,
}

fn measured(call: impl FnOnce()) -> Call {
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    call();
    let measured = Call {
        peak: PEAK.load(Ordering::Relaxed) - baseline,
        admitted: window.close() - reserved,
        beyond: EXCESS.load(Ordering::Relaxed) - before,
        left: LIVE.load(Ordering::Relaxed).saturating_sub(baseline),
        kept: reserved_memory().saturating_sub(reserved),
    };
    drop(window);
    measured
}

/// A gain table of [`TABLE_ROWS`] rows, written against the MeasurementSet
/// at `ms`.
fn write_table(ms: &Path, output: &Path) {
    let ms = casa_ms::MeasurementSet::open(ms).expect("open the MeasurementSet");
    let rows = (0..TABLE_ROWS / ANTENNAS).flat_map(|index| {
        (0..ANTENNAS as i32).map(move |antenna| SimulatedSolution {
            time_s: 5.0e9 + 10.0 * index as f64,
            field_id: 0,
            spectral_window_id: 0,
            antenna,
            receptors: [Complex32::new(1.0, 0.01 * index as f32); 2],
        })
    });
    write_simulated_jones_table(&ms, output, SimulatedJones::Gain, rows).expect("write the table");
}

fn apply(table: &Path) -> ApplySpec {
    ApplySpec {
        table: table.to_string_lossy().into_owned(),
        kind: String::new(),
        interval_s: 0.0,
        interp: "linear".to_string(),
        calwt: false,
        spwmap: vec![-1],
        opacity: 0.0,
    }
}

fn check(name: &str, call: &Call) {
    println!(
        "{name}: heap peak {}, admitted {}, at most {} beyond; left {}, kept {}",
        call.peak, call.admitted, call.beyond, call.left, call.kept
    );
    assert!(
        call.beyond <= CALL_ALLOWANCE_BYTES as i64,
        "{name}: the heap went {} bytes beyond what the reservations held",
        call.beyond
    );
    assert!(
        call.peak <= call.admitted + CALL_ALLOWANCE_BYTES,
        "{name}: the heap grew {} bytes, beyond the {} admitted",
        call.peak,
        call.admitted
    );
    assert!(
        call.left <= call.kept + CALL_ALLOWANCE_BYTES,
        "{name}: {} bytes left live, beyond the {} kept charged",
        call.left,
        call.kept
    );
}

#[test]
fn setapply_and_corrupt_admit_the_table_and_the_term() {
    if !super::alone("apply::setapply_and_corrupt_admit_the_table_and_the_term") {
        return;
    }
    let resources = resources(BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let path = |name: &str| root.path().join(name);
    let mut reference = configured(&path("reference.ms"), &measures, BUDGET, 32);
    reference.observe(&scan(10)).expect("observe");
    reference.close().expect("close");
    write_table(&path("reference.ms"), &path("gains.cal"));
    let session = |name: &str| -> Simulator {
        let mut sm = configured(&path(name), &measures, BUDGET, 32);
        sm.observe(&scan(INTEGRATIONS)).expect("observe");
        sm
    };
    // Once unmeasured, so what a process keeps once it has read a table and
    // corrupted is the process's before anything is measured.
    let mut warm = session("warm.ms");
    warm.set_apply(&apply(&path("gains.cal")))
        .expect("setapply");
    warm.corrupt().expect("corrupt");
    drop(warm);

    let mut sm = session("measured.ms");
    let set = measured(|| sm.set_apply(&apply(&path("gains.cal"))).expect("setapply"));
    println!(
        "setapply per table row: peak {:.0}, kept {:.0} bytes",
        set.peak as f64 / TABLE_ROWS as f64,
        set.left as f64 / TABLE_ROWS as f64
    );
    check("setapply", &set);
    let corrupted = measured(|| sm.corrupt().expect("corrupt"));
    check("corrupt", &corrupted);
}
