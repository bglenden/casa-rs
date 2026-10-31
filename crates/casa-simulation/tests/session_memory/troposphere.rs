// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory laws of `settrop` (#841 Phase 4d).
//!
//! `settrop` simulates its term over the integrations observed so far: it
//! keeps a solution per integration, an excess per slot and antenna and
//! each window's delays, and forming it holds the largest window's
//! atmosphere, then one series' transform (`individual`) or the whole
//! screen's (`screen`), which grows with the wind's run over the track. It
//! admits both before it forms anything and keeps the term's share while
//! the term lives: over 20,000 integrations (`individual`) and a screen of
//! 4,900 by 980 pixels (`screen`) the heap stays within what the
//! reservations held, what it leaves live is what they keep, and `reset`
//! drops both. FFTW's plans are C heap, which this binary does not count;
//! they are admitted all the same.
//!
//! `close` then multiplies each row channel by channel by its antennas'
//! phasors, which it works out once a batch for each antenna and solution
//! the batch reaches, and admits with the batch. At 4,096 channels a
//! batch's phasors hold megabytes, and what `close` holds apart from the
//! write batches more for a tropospheric term than for an `fbm` gain term
//! (which takes the same path through `corrupt`, without phasors) is within
//! what it is admitted more.
//!
//! The first law's base and ten-times-the-rows cases, with and without the
//! term's table, and the patch's rows law hold with an `individual` term
//! ([`super::jones`]'s laws).
//!
//! Each law runs alone in its process ([`super::alone`]).

use casa_simulation::{GainSpec, Simulator, TroposphereSpec};
use num_complex::Complex32;

use super::jones::{TROPOSPHERE, session_law, table_law};
use super::{
    BATCH_ALLOCATED, EXCESS, LIVE, OWN_LIVE, OWN_PEAK, Ordering, PEAK, ReservationWindow, alone,
    charge_process_caches, configured, excess_now, reserved_memory, resources, scan,
};

/// The setter law's budget beside the process caches: ample for the
/// screen.
const BUDGET: u64 = 256 << 20;

/// Heap `settrop` may hold beyond its reservation: the shared term's
/// header, the session's list of terms, the atmosphere's profile before it
/// is admitted, and the log lines.
const SETTER_ALLOWANCE_BYTES: u64 = 64 * 1024;

/// The setter law's sessions: (mode, integrations of 10 s). The screen
/// spans 4,000 s of a 7 m/s wind in 6 m pixels.
const SETTERS: [(&str, usize); 2] = [("individual", 20_000), ("screen", 400)];

fn trop(mode: &str) -> TroposphereSpec {
    TroposphereSpec {
        mode: mode.to_string(),
        ..TroposphereSpec::default()
    }
}

#[test]
fn settrop_admits_what_it_forms_and_keeps_it_with_the_term() {
    if !alone("troposphere::settrop_admits_what_it_forms_and_keeps_it_with_the_term") {
        return;
    }
    let resources = resources(BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    for (mode, integrations) in SETTERS {
        let mut sm = configured(
            &root.path().join(format!("{mode}.ms")),
            &measures,
            BUDGET,
            32,
        );
        sm.observe(&scan(integrations)).expect("observe");
        // Once unmeasured, so the log's callsites, the planner's and the
        // worker team's state are the process's before anything is
        // measured.
        sm.set_trop(&trop(mode)).expect("settrop");
        sm.reset();
        let unset = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let window = ReservationWindow::open();
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        sm.set_trop(&trop(mode)).expect("settrop");
        let admitted = window.close() - unset;
        let beyond = EXCESS.load(Ordering::Relaxed) - before;
        let peak = PEAK.load(Ordering::Relaxed) - baseline;
        let left = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let kept = reserved_memory() - unset;
        println!(
            "settrop {mode}: heap peak {peak}, admitted {admitted}, at most {beyond} beyond; \
             left {left}, kept {kept}"
        );
        assert!(
            beyond <= SETTER_ALLOWANCE_BYTES as i64,
            "{mode}: the heap went {beyond} bytes beyond what the reservations held"
        );
        assert!(
            peak <= admitted + SETTER_ALLOWANCE_BYTES,
            "{mode}: the heap grew {peak} bytes, beyond the {admitted} admitted"
        );
        assert!(
            left <= kept + SETTER_ALLOWANCE_BYTES && kept <= left + SETTER_ALLOWANCE_BYTES,
            "{mode}: the term keeps charged what it keeps ({kept} for {left} live)"
        );
        sm.reset();
        assert_eq!(
            reserved_memory(),
            unset,
            "{mode}: reset drops the term and its charge"
        );
    }
}

/// `close`'s budget beside the process caches at 4,096 channels: enough
/// that the writer's batches are its target's, about a thousand rows.
const WIDE_BUDGET: u64 = 1 << 30;

/// Channels of the phasor law's window.
const CHANNELS: usize = 4_096;

/// Heap `close` may hold beyond its reservations at any allocation.
const CLOSE_ALLOWANCE_BYTES: i64 = 512 * 1024;

/// Measured `close`s of each term: the troposphere's worst is compared with
/// the gain's least.
const CLOSE_RUNS: usize = 3;

/// A term on a session, named.
type Term = (&'static str, fn(&mut Simulator));

/// The phasor law's terms: an `fbm` gain term, whose rows take the same path
/// through `corrupt` without phasors, then a tropospheric one.
const TERMS: [Term; 2] = [
    ("fbm gain", |sm| {
        sm.set_gain(&GainSpec {
            mode: "fbm".to_string(),
            table: String::new(),
            amplitude: vec![0.05],
        })
        .expect("setgain");
    }),
    ("troposphere", |sm| {
        sm.set_trop(&trop("individual")).expect("settrop");
    }),
];

/// `close` admits each batch's phasors. At 4,096 channels a batch's
/// phasors hold megabytes, which the writer's reservation could hide (its
/// plan keeps room for batches it does not always hold), so the law
/// compares a tropospheric term with an `fbm` gain term, whose rows take
/// the same path through `corrupt` without phasors: what `close` holds
/// apart from the write batches more for the troposphere, on the worst of
/// [`CLOSE_RUNS`] runs against the gain's least, is within what it is
/// admitted more, and the heap with the batches is within the reservations
/// at every allocation of either. The batches are left out because how
/// many are live at the peak is the writer's pace ([`super::heap`]).
#[test]
fn close_admits_each_batchs_tropospheric_phasors() {
    if !alone("troposphere::close_admits_each_batchs_tropospheric_phasors") {
        return;
    }
    let resources = resources(WIDE_BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let close = |name: &str, set: &dyn Fn(&mut Simulator), integrations: usize| {
        let mut sm = configured(&root.path().join(name), &measures, WIDE_BUDGET, CHANNELS);
        sm.observe(&scan(integrations)).expect("observe");
        set(&mut sm);
        sm.corrupt().expect("corrupt");
        let unset = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let own = OWN_LIVE.load(Ordering::Relaxed);
        OWN_PEAK.store(own, Ordering::Relaxed);
        let batched = BATCH_ALLOCATED.load(Ordering::Relaxed);
        let window = ReservationWindow::open();
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let ((), pass) = super::capture(|| sm.close().expect("close"));
        let pass = pass.expect("the pass summary");
        let closed = (
            PEAK.load(Ordering::Relaxed) - baseline,
            OWN_PEAK.load(Ordering::Relaxed) - own,
            window.close() - unset,
            EXCESS.load(Ordering::Relaxed) - before,
        );
        drop(window);
        let batch_data = pass.batch_rows * (2 * CHANNELS * size_of::<Complex32>()) as u64;
        let batches = BATCH_ALLOCATED.load(Ordering::Relaxed) - batched;
        assert!(
            batches >= batch_data,
            "{name}: the write batches are told apart ({batches} bytes, a batch's DATA \
             {batch_data})"
        );
        (closed, pass.batches)
    };
    // Once unmeasured each, so what a process keeps once it has written is
    // the process's before anything is measured.
    for (name, set) in TERMS {
        close(&format!("warm-{name}.ms"), &set, 20);
    }
    let (mut least, mut most) = ([u64::MAX; 2], [0; 2]);
    let mut admitted = [0; 2];
    for run in 0..CLOSE_RUNS {
        for (index, (name, set)) in TERMS.into_iter().enumerate() {
            let ((peak, own, charged, beyond), batches) =
                close(&format!("{run}-{index}.ms"), &set, 200);
            println!(
                "{name}: {batches} batches at {CHANNELS} channels: heap peak {peak}, {own} apart \
                 from the batches, admitted {charged}, at most {beyond} beyond"
            );
            assert!(batches > 1, "the session is written in several batches");
            assert!(
                beyond <= CLOSE_ALLOWANCE_BYTES,
                "{name}: the heap went {beyond} bytes beyond what the reservations held"
            );
            least[index] = least[index].min(own);
            most[index] = most[index].max(own);
            admitted[index] = charged;
        }
    }
    let held = most[1] as i64 - least[0] as i64;
    let charged = admitted[1] as i64 - admitted[0] as i64;
    println!("troposphere - fbm gain: held {held}, admitted {charged}");
    assert!(
        held <= charged + CLOSE_ALLOWANCE_BYTES,
        "the troposphere's close held {held} bytes more than the gain's, beyond the {charged} it \
         was admitted more"
    );
}

#[test]
fn a_troposphere_session_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("troposphere::a_troposphere_session_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    TROPOSPHERE.store(true, Ordering::Relaxed);
    let root = tempfile::tempdir().expect("temporary directory");
    session_law(root.path());
}

/// The first law with the term writing its `Tf Jones` table at `close`: 32
/// values a row.
#[test]
fn a_troposphere_session_writing_its_table_holds_its_admission_and_no_more_for_more_rows() {
    if !alone(
        "troposphere::a_troposphere_session_writing_its_table_holds_its_admission_and_no_more_for_more_rows",
    ) {
        return;
    }
    TROPOSPHERE.store(true, Ordering::Relaxed);
    table_law();
}

/// Channels of the wide table law's window.
const TABLE_CHANNELS: usize = 1_024;

/// A `Tf Jones` table of [`TABLE_CHANNELS`] values a row, whose values
/// rather than its rows make its size (1,200 rows, 1.2 million values), is
/// built within what `close` admits for it: at every allocation of `close`
/// the heap stays within the reservations. The table is built after MAIN's
/// writer has released its admission, so this, not the peaks, holds it.
/// Admitting no bytes a value leaves the heap 20.4 MB beyond (16.6 bytes a
/// value); admitting 16 leaves it 0.78 MB beyond, and the law fails.
#[test]
fn a_wide_troposphere_table_is_built_within_its_admission() {
    if !alone("troposphere::a_wide_troposphere_table_is_built_within_its_admission") {
        return;
    }
    let resources = resources(WIDE_BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let close = |name: &str| {
        let ms = root.path().join(format!("{name}.ms"));
        let mut sm = configured(&ms, &measures, WIDE_BUDGET, TABLE_CHANNELS);
        sm.observe(&scan(200)).expect("observe");
        sm.set_trop(&TroposphereSpec {
            table: root
                .path()
                .join(format!("{name}.cal"))
                .to_string_lossy()
                .into_owned(),
            ..trop("individual")
        })
        .expect("settrop");
        sm.corrupt().expect("corrupt");
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        sm.close().expect("close");
        EXCESS.load(Ordering::Relaxed) - before
    };
    // Once unmeasured, as the other laws.
    close("warm");
    let beyond = close("measured");
    println!(
        "a table of {TABLE_CHANNELS} values a row: at most {beyond} bytes beyond the reservations"
    );
    assert!(
        beyond <= CLOSE_ALLOWANCE_BYTES,
        "the heap went {beyond} bytes beyond what the reservations held"
    );
}

/// The patch's rows law with a tropospheric term: the patch reads each
/// row's TIME, and admits each batch's phasors.
#[test]
fn patching_with_troposphere_holds_its_admission_and_no_more_for_more_rows() {
    if !alone(
        "troposphere::patching_with_troposphere_holds_its_admission_and_no_more_for_more_rows",
    ) {
        return;
    }
    TROPOSPHERE.store(true, Ordering::Relaxed);
    super::patch::patches_hold_their_admission_for_more_rows();
}
