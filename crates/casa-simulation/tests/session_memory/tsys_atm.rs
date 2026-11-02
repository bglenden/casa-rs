// SPDX-License-Identifier: LGPL-3.0-or-later
//! The session memory laws of `tsys-atm` noise (#841 Phase 4c).
//!
//! `setnoise` builds the term's atmosphere for every spectral window: each
//! channel's line of sight through ATM's layers, which grows with the
//! channels. It admits what the windows keep, and what building the largest
//! holds, before it builds anything, and keeps the windows' share for as
//! long as the term lives. Over a window of 4,096 channels, and over 4,096
//! windows of a channel each (whose vector the term keeps too, #897
//! review), the heap stays within what the reservations hold at every
//! allocation of `setnoise`, what it leaves live is what they keep, and
//! `reset` drops both.
//!
//! `close` then draws each row's noise channel by channel from each
//! antenna's scales, which it works out once a batch for each antenna and
//! integration the batch reaches, and admits with the batch. At 4,096
//! channels a batch's scales hold megabytes, and what `close` holds apart
//! from the write batches more for `tsys-atm` than for `tsys-manual` is
//! within what it is admitted more (without that admission the law fails:
//! 4.8 MB more held, none admitted); so it is at one channel with 65,544
//! entries in a batch. The
//! first law's base and ten-times-the-rows cases with
//! `tsys-atm` noise hold their admission, and ten times the rows no more
//! beyond it but a little ([`super::tsys`]'s law), as does the patch of an
//! opened MeasurementSet ([`super::patch`]'s rows law).
//!
//! Each law runs alone in its process, as the binary's other laws do
//! ([`super::alone`]).

use casa_simulation::{AtmosphereSpec, NoiseSpec, ObserveSpec, SpectralWindowSpec, TsysParameters};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch};
use num_complex::Complex32;

use super::{
    ATM_NOISE, BATCH_ALLOCATED, EXCESS, LIVE, OWN_LIVE, OWN_PEAK, Ordering, PEAK, REFERENCE_MJD,
    ReservationWindow, alone, charge_process_caches, configured, excess_now, reserved_memory,
    resources, scan,
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
    setnoise_holds_its_admission(1, CHANNELS);
}

/// Spectral windows of one channel each, whose atmospheres the term keeps
/// in a vector of its own.
const NARROW_WINDOWS: usize = 4_096;

/// The setnoise law over [`NARROW_WINDOWS`] windows of a channel each:
/// what grows with the windows (the term's vector of them, and each one's
/// atmosphere) is charged as it is kept (#897 review).
#[test]
fn setnoise_charges_every_window_it_keeps() {
    if !alone("tsys_atm::setnoise_charges_every_window_it_keeps") {
        return;
    }
    setnoise_holds_its_admission(NARROW_WINDOWS, 1);
}

/// `setnoise` over `windows` windows of `channels` channels holds within
/// its reservations at every allocation, leaves live what they keep, and
/// `reset` drops both.
fn setnoise_holds_its_admission(windows: usize, channels: usize) {
    let resources = resources(BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let mut sm = configured(&root.path().join("atm.ms"), &measures, BUDGET, channels);
    for window in 1..windows {
        sm.set_spectral_window(&SpectralWindowSpec {
            name: format!("narrow{window}"),
            start_frequency_hz: 230e9 + 1e6 * window as f64,
            channel_width_hz: 1e6,
            resolution_hz: 1e6,
            frame: FrequencyRef::TOPO,
            channel_count: channels,
            stokes: "XX YY".to_string(),
        })
        .expect("setspwindow");
    }
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
        "setnoise of {windows} windows of {channels} channels: heap peak {peak}, admitted \
         {admitted}, at most {beyond} beyond; left {left}, kept {kept}"
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
        kept > 100 * (windows * channels) as u64,
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

/// Measured `close`s of each noise mode: written in batches, `tsys-atm`'s
/// worst is compared with `tsys-manual`'s least; in one, the least of each.
const CLOSE_RUNS: usize = 3;

/// What one measured `close` held.
struct Closed {
    /// The heap's peak beyond what was live before.
    peak: u64,
    /// The peak, beyond what was live before, of the blocks that are not a
    /// write batch's ([`super::heap`]).
    own: u64,
    /// The most the reservations held at once beyond what they held before.
    admitted: u64,
    /// The most the heap went beyond the reservations, at any allocation.
    beyond: i64,
    batches: u64,
    batch_rows: u64,
    /// Bytes allocated as the write batches'.
    batch_bytes: u64,
}

/// `close` admits each batch's antenna scales. At [`CHANNELS`] channels a
/// batch's table holds megabytes, which the writer's reservation could hide
/// (its plan keeps room for batches it does not always hold), so the law
/// compares `tsys-atm` with `tsys-manual`, whose rows need the same
/// geometry: what `close` holds apart from the write batches more for
/// `tsys-atm`, on the worst of [`CLOSE_RUNS`] runs against `tsys-manual`'s
/// least, is within what it is admitted more, and the heap with the
/// batches is within the reservations at every allocation of either.
#[test]
fn close_admits_each_batchs_antenna_scales() {
    if !alone("tsys_atm::close_admits_each_batchs_antenna_scales") {
        return;
    }
    close_holds_what_it_is_admitted_more((CHANNELS, 10.0), [20, 200], true);
}

/// Integrations of the narrow law: 65,544 entries, just past a power of
/// two.
const NARROW_INTEGRATIONS: usize = 10_924;

/// The close law for many antenna scales of one channel (#897 review):
/// [`NARROW_INTEGRATIONS`] one-second integrations of six antennas, one
/// batch of 163,860 rows. The scales themselves are small, and what the
/// table holds beside them (its index and its pending computations) is
/// most of it; `close` holds within what it is admitted more. The writer's
/// room for batches it does not hold leaves megabytes to spare here, so
/// this law alone would pass a table that grew as its entries came;
/// `corrupt::batch_scales`' unit test holds the table to its reserved room.
#[test]
fn close_admits_a_narrow_batchs_many_antenna_scales() {
    if !alone("tsys_atm::close_admits_a_narrow_batchs_many_antenna_scales") {
        return;
    }
    close_holds_what_it_is_admitted_more((1, 1.0), [20, NARROW_INTEGRATIONS], false);
}

/// `close` of sessions of `channels` channels and `integrations` (warmed,
/// measured) integrations of `integration_s`, with `tsys-atm` noise and
/// then `tsys-manual`: what it holds more for `tsys-atm` is within what it
/// is admitted more, and the heap is within the reservations at every
/// allocation of either. `batched`: the measured sessions are written in
/// several batches, of which the writer's pace leaves one to three live at
/// the peak, so what `close` holds apart from them is compared, on
/// `tsys-atm`'s worst of [`CLOSE_RUNS`] runs against `tsys-manual`'s least.
/// Written in one batch, the heap's least peaks are compared.
fn close_holds_what_it_is_admitted_more(
    (channels, integration_s): (usize, f64),
    [warm, measured]: [usize; 2],
    batched: bool,
) {
    let resources = resources(WIDE_BUDGET);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let close = |name: &str, noise: &NoiseSpec, integrations: usize| {
        let mut sm = configured(&root.path().join(name), &measures, WIDE_BUDGET, channels);
        sm.set_times(
            integration_s,
            false,
            MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        )
        .expect("settimes");
        sm.observe(&ObserveSpec {
            stop_s: integration_s * integrations as f64,
            ..scan(0)
        })
        .expect("observe");
        sm.set_noise(noise).expect("setnoise");
        sm.corrupt().expect("corrupt");
        let unset = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let own = OWN_LIVE.load(Ordering::Relaxed);
        OWN_PEAK.store(own, Ordering::Relaxed);
        let batch_bytes = BATCH_ALLOCATED.load(Ordering::Relaxed);
        let window = ReservationWindow::open();
        let before = excess_now();
        EXCESS.store(before, Ordering::Relaxed);
        let ((), pass) = super::capture(|| sm.close().expect("close"));
        let pass = pass.expect("the pass summary");
        let closed = Closed {
            peak: PEAK.load(Ordering::Relaxed) - baseline,
            own: OWN_PEAK.load(Ordering::Relaxed) - own,
            admitted: window.close() - unset,
            beyond: EXCESS.load(Ordering::Relaxed) - before,
            batches: pass.batches,
            batch_rows: pass.batch_rows,
            batch_bytes: BATCH_ALLOCATED.load(Ordering::Relaxed) - batch_bytes,
        };
        drop(window);
        closed
    };
    let manual = NoiseSpec::tsys_manual(TsysParameters::default());
    // Once unmeasured each, so what a process keeps once it has written is
    // the process's before anything is measured.
    close("warm-manual.ms", &manual, warm);
    close("warm-atm.ms", &tsys_atm(), warm);
    let (mut least, mut most) = ([u64::MAX; 2], [0; 2]);
    let mut admitted = [0; 2];
    for run in 0..CLOSE_RUNS {
        for (index, noise) in [&manual, &tsys_atm()].into_iter().enumerate() {
            let closed = close(&format!("{run}-{index}.ms"), noise, measured);
            println!(
                "{}: {} batches of {} at {channels} channels: heap peak {}, {} apart from the \
                 batches, admitted {}, at most {} beyond",
                noise.mode,
                closed.batches,
                closed.batch_rows,
                closed.peak,
                closed.own,
                closed.admitted,
                closed.beyond
            );
            assert!(
                !batched || closed.batches > 1,
                "the session is written in several batches"
            );
            assert!(
                closed.beyond <= CLOSE_ALLOWANCE_BYTES,
                "{}: the heap went {} bytes beyond what the reservations held",
                noise.mode,
                closed.beyond
            );
            let batch_data = closed.batch_rows * (2 * channels * size_of::<Complex32>()) as u64;
            assert!(
                closed.batch_bytes >= batch_data,
                "{}: the write batches are told apart ({} bytes, a batch's DATA {batch_data})",
                noise.mode,
                closed.batch_bytes
            );
            let compared = if batched { closed.own } else { closed.peak };
            least[index] = least[index].min(compared);
            most[index] = most[index].max(compared);
            admitted[index] = closed.admitted;
        }
    }
    let atm = if batched { most[1] } else { least[1] };
    let held = atm as i64 - least[0] as i64;
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
