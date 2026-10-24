// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory laws of `tsys-atm` noise (#841 Phase 4c).
//!
//! `setnoise` builds the term's atmosphere for every spectral window: each
//! channel's line of sight through ATM's layers, which grows with the
//! channels. It admits what the windows keep, and what building the largest
//! holds, before it builds anything, and keeps the windows' share for as
//! long as the term lives. Over a window of 4,096 channels the heap stays
//! within what the reservations hold at every allocation of `setnoise`,
//! what it leaves live is what they keep, and `reset` drops both.
//!
//! `close` then draws each row's noise channel by channel from each
//! antenna's scales, which it works out once a batch for each antenna and
//! integration the batch reaches, and admits with the batch. At 4,096
//! channels a batch's scales hold megabytes, and what `close` holds more for
//! `tsys-atm` than for `tsys-manual` is within what it is admitted more
//! (without that admission the law fails: 4.8 MB more held, none
//! admitted). The first law's base and ten-times-the-rows cases with
//! `tsys-atm` noise hold their admission, and ten times the rows no more
//! beyond it but a little ([`super::tsys`]'s law), as does the patch of an
//! opened MeasurementSet ([`super::patch`]'s rows law).
//!
//! Each law runs alone in its process, as the binary's other laws do
//! ([`super::alone`]).

use casa_simulation::{AtmosphereSpec, NoiseSpec, TsysParameters};

use super::{
    ATM_NOISE, EXCESS, LIVE, Ordering, PEAK, ReservationWindow, alone, charge_process_caches,
    configured, excess_now, reserved_memory, resources, scan,
};

/// The session's budget beside the process caches: ample for the
/// atmosphere.
const BUDGET: u64 = 128 << 20;

/// Channels of the session's window, 1 MHz apart from 230 GHz.
const CHANNELS: usize = 4_096;

/// Heap `setnoise` may hold beyond its reservation: the shared term's
/// header, the session's list of terms, the atmosphere's profile before it
/// is admitted (a few kilobytes of layers), and the log lines.
const SETNOISE_ALLOWANCE_BYTES: u64 = 64 * 1024;

fn tsys_atm() -> NoiseSpec {
    NoiseSpec::tsys_atm(TsysParameters::default(), 1.0, AtmosphereSpec::default())
}

#[test]
fn setnoise_admits_the_atmosphere_it_builds_and_keeps_it_with_the_term() {
    if !alone("tsys_atm::setnoise_admits_the_atmosphere_it_builds_and_keeps_it_with_the_term") {
        return;
    }
    let resources = resources(BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let mut sm = configured(&root.path().join("atm.ms"), &measures, BUDGET, CHANNELS);
    sm.observe(&scan(10)).expect("observe");
    // Once unmeasured, so the log's callsites and the worker team's state
    // are the process's before anything is measured.
    sm.set_noise(&tsys_atm()).expect("setnoise");
    sm.reset();
    let unset = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let before = excess_now();
    EXCESS.store(before, Ordering::Relaxed);
    sm.set_noise(&tsys_atm()).expect("setnoise");
    let admitted = window.close() - unset;
    let beyond = EXCESS.load(Ordering::Relaxed) - before;
    let peak = PEAK.load(Ordering::Relaxed) - baseline;
    let left = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let kept = reserved_memory() - unset;
    drop(window);
    println!(
        "setnoise of {CHANNELS} channels: heap peak {peak}, admitted {admitted}, at most \
         {beyond} beyond; left {left}, kept {kept}"
    );
    assert!(
        beyond <= SETNOISE_ALLOWANCE_BYTES as i64,
        "the heap went {beyond} bytes beyond what the reservations held"
    );
    assert!(
        peak <= admitted + SETNOISE_ALLOWANCE_BYTES,
        "the heap grew {peak} bytes, beyond the {admitted} admitted"
    );
    assert!(
        left <= kept + SETNOISE_ALLOWANCE_BYTES && kept <= left + SETNOISE_ALLOWANCE_BYTES,
        "the term keeps charged what it keeps ({kept} for {left} live)"
    );
    assert!(
        kept > 100 * CHANNELS as u64,
        "the atmosphere is charged ({kept} bytes)"
    );
    sm.reset();
    assert_eq!(
        reserved_memory(),
        unset,
        "reset drops the term and its charge"
    );
}

/// `close`'s budget beside the process caches for [`CHANNELS`] channels:
/// enough that the writer's batches are its target's, a few hundred rows,
/// whatever the atmosphere holds.
const WIDE_BUDGET: u64 = 1 << 30;

/// Heap `close` may hold beyond its reservations at any allocation (the
/// binary's first law's allowance), well under what a batch's antenna
/// scales take at [`CHANNELS`] channels.
const CLOSE_ALLOWANCE_BYTES: i64 = 512 * 1024;

/// Measured `close`s of each noise mode, of which the least peak is
/// compared, as one run's timing can hold a write batch more.
const CLOSE_RUNS: usize = 3;

/// What one measured `close` held.
struct Closed {
    /// The heap's peak beyond what was live before.
    peak: u64,
    /// The most the reservations held at once beyond what they held before.
    admitted: u64,
    /// The most the heap went beyond the reservations, at any allocation.
    beyond: i64,
    batches: u64,
    batch_rows: u64,
}

/// `close` admits each batch's antenna scales. At [`CHANNELS`] channels a
/// batch's table holds megabytes, which the writer's reservation could hide
/// (its plan keeps room for batches it does not always hold), so the law
/// compares `tsys-atm` with `tsys-manual`, whose rows need the same
/// geometry: what `close` holds more for `tsys-atm`, on the least of
/// [`CLOSE_RUNS`] runs, is within what it is admitted more, and the heap is
/// within the reservations at every allocation of either.
#[test]
fn close_admits_each_batchs_antenna_scales() {
    if !alone("tsys_atm::close_admits_each_batchs_antenna_scales") {
        return;
    }
    let resources = resources(WIDE_BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let close = |name: &str, noise: &NoiseSpec, integrations: usize| {
        let mut sm = configured(&root.path().join(name), &measures, WIDE_BUDGET, CHANNELS);
        sm.observe(&scan(integrations)).expect("observe");
        sm.set_noise(noise).expect("setnoise");
        sm.corrupt().expect("corrupt");
        let unset = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let window = ReservationWindow::open();
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let ((), pass) = super::capture(|| sm.close().expect("close"));
        let pass = pass.expect("the pass summary");
        let closed = Closed {
            peak: PEAK.load(Ordering::Relaxed) - baseline,
            admitted: window.close() - unset,
            beyond: EXCESS.load(Ordering::Relaxed) - before,
            batches: pass.batches,
            batch_rows: pass.batch_rows,
        };
        drop(window);
        closed
    };
    let manual = NoiseSpec::tsys_manual(TsysParameters::default());
    // Once unmeasured each, so what a process keeps once it has written is
    // the process's before anything is measured.
    close("warm-manual.ms", &manual, 20);
    close("warm-atm.ms", &tsys_atm(), 20);
    let mut least = [u64::MAX; 2];
    let mut admitted = [0; 2];
    for run in 0..CLOSE_RUNS {
        for (index, noise) in [&manual, &tsys_atm()].into_iter().enumerate() {
            let closed = close(&format!("{run}-{index}.ms"), noise, 200);
            println!(
                "{}: {} batches of {} at {CHANNELS} channels: heap peak {}, admitted {}, at \
                 most {} beyond",
                noise.mode,
                closed.batches,
                closed.batch_rows,
                closed.peak,
                closed.admitted,
                closed.beyond
            );
            assert!(
                closed.batches > 1,
                "the session is written in several batches"
            );
            assert!(
                closed.beyond <= CLOSE_ALLOWANCE_BYTES,
                "{}: the heap went {} bytes beyond what the reservations held",
                noise.mode,
                closed.beyond
            );
            least[index] = least[index].min(closed.peak);
            admitted[index] = closed.admitted;
        }
    }
    let held = least[1] as i64 - least[0] as i64;
    let charged = admitted[1] as i64 - admitted[0] as i64;
    println!("tsys-atm - tsys-manual: held {held}, admitted {charged}");
    assert!(
        held <= charged + CLOSE_ALLOWANCE_BYTES,
        "tsys-atm's close held {held} bytes more than tsys-manual's, beyond the {charged} it \
         was admitted more"
    );
}

#[test]
fn a_tsys_atm_session_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("tsys_atm::a_tsys_atm_session_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    ATM_NOISE.store(true, Ordering::Relaxed);
    super::tsys::holds_its_admission_and_no_more_for_more_rows("tsys-atm");
}

/// The patch's rows law ([`super::patch`]) with `tsys-atm` noise: the patch
/// admits each worker's scales with its batch.
#[test]
fn patching_with_tsys_atm_noise_holds_its_admission_and_no_more_for_more_rows() {
    if !alone(
        "tsys_atm::patching_with_tsys_atm_noise_holds_its_admission_and_no_more_for_more_rows",
    ) {
        return;
    }
    ATM_NOISE.store(true, Ordering::Relaxed);
    super::patch::patches_hold_their_admission_for_more_rows();
}
