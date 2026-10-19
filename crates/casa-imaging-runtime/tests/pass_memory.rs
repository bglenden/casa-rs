// SPDX-License-Identifier: LGPL-3.0-or-later
//! A major-cycle pass holds no more than it is admitted: its wave's
//! [`WaveDemand::bytes`] and the source's blocks
//! ([`source_block_bytes`]), however wide the native spectrum is against the
//! output axis and however many chunks and owners place it.
//!
//! This binary's global allocator counts the live heap and keeps its peak
//! while a measured pass runs. Wide native spectra with few output planes
//! make the backends' per-block scratch large against the wave's grids:
//! Astra's probe on #712 held 40,136,661 bytes against 39,617,192 admitted
//! when the CPU backend's native prediction and residual batches went
//! uncharged. A global allocator applies to this test binary alone, and
//! each law runs alone in a process of its own ([`alone`]).

#[allow(
    dead_code,
    reason = "the pass laws share the fixture; this law uses part of it"
)]
#[path = "pass_laws/fixture.rs"]
mod fixture;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_operator::{
    Basis, GridPrecision, MeasurementOperator, ModeSet, PlaneRange, SpectralAxis, SpectralKernel,
    SpectralResampler, WeightingGeneration,
};
use casa_imaging_runtime::pass::{
    BackendChoice, BoundedSource, CHUNK_PLACEMENTS, Cancel, ModelPreparation, Partition,
    PassDomain, PassError, Residency, WaveDemand, WorkerTeam, row_work_pass_bytes,
    run_row_work_pass, source_block_bytes,
};

use fixture::{
    Rows, Run, TestKernels, operator_of, operator_with, planes, sparse_model, try_run, window_of,
};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their peak while measuring.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);

// SAFETY: every method forwards to `System` with the caller's arguments and
// only adds atomic bookkeeping, which allocates nothing.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (old, new) = (layout.size() as u64, new_size as u64);
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            if moved != pointer {
                // Old and new bytes were both held while the block moved.
                observe(LIVE.load(Ordering::Relaxed) + new);
            }
            if new >= old {
                grow(new - old);
            } else {
                LIVE.fetch_sub(old - new, Ordering::Relaxed);
            }
        }
        moved
    }
}

fn grow(bytes: u64) {
    observe(LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes);
}

fn observe(live: u64) {
    if MEASURING.load(Ordering::Relaxed) {
        PEAK.fetch_max(live, Ordering::Relaxed);
    }
}

/// The environment variable that names the law a child process of
/// [`alone`] runs.
const LAW_VAR: &str = "CASA_RS_COUNTING_LAW";

/// The test a law's process runs before the law ([`alone`]).
const SETTLE: &str = "a_counting_law_process_settles";

/// Whether [`SETTLE`] ran in this process.
static SETTLED: AtomicBool = AtomicBool::new(false);

/// Runs first in a law's process ([`alone`]): libtest's main thread sets up
/// its wait for results while the first test it starts runs, so the law,
/// which runs second, measures none of it. The pause makes sure the main
/// thread is waiting before this test reports.
#[test]
#[ignore = "alone runs it before a law, in the law's process"]
fn a_counting_law_process_settles() {
    std::thread::sleep(std::time::Duration::from_millis(10));
    SETTLED.store(true, Ordering::Relaxed);
}

/// Whether this process runs the law `name` alone; if it does not, run the
/// law alone and assert that it passed.
///
/// The counters see every allocation in the process, and libtest runs a
/// binary's tests in one process: under `cargo test` its main thread starts
/// tests and reports their results, and the binary's other tests run, while
/// a law measures. So each law begins `if !alone("law") { return; }`, with
/// its own name. In the harness's process it runs [`SETTLE`], then the law
/// (libtest runs tests in name order), in a child process of this binary
/// with one test thread, so that the harness only waits while the law runs;
/// in the child it returns true.
fn alone(name: &str) -> bool {
    // libtest names each test's thread after the test.
    assert_eq!(
        std::thread::current().name(),
        Some(name),
        "a law runs alone under its own name"
    );
    let ran = format!("{name} ran alone");
    if std::env::var_os(LAW_VAR).is_some_and(|law| law == name) {
        assert!(
            SETTLED.load(Ordering::Relaxed),
            "{SETTLE} runs before {name}"
        );
        println!("{ran}");
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            SETTLE,
            name,
            "--exact",
            "--include-ignored",
            "--test-threads=1",
            "--nocapture",
        ])
        .env(LAW_VAR, name)
        .output()
        .expect("the law's process runs");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{name} alone: {}\n{stdout}",
        output.status
    );
    assert!(stdout.contains(&ran), "{name} did not run alone:\n{stdout}");
    false
}

/// Native channels 10 kHz apart from 1 GHz.
fn natives(count: usize) -> Vec<f64> {
    (0..count)
        .map(|channel| 1.0e9 + channel as f64 * 1.0e4)
        .collect()
}

/// The row-work pass holds at most its charge and the source's blocks over
/// region domains of increasing, unequal heights (#742 review, Astra): a
/// 1024² main field then a 1536² one, 1,250 and 1,920 padded rows, on one,
/// two and four workers, each on a team just built. Chunk histograms that
/// grew per domain held 2,500 rows each, 75,443 bytes over the charge on
/// four workers; a team's first parallel call held 7 KB more until the
/// team made it while being built ([`WorkerTeam::new`]). Each case runs
/// without and with weight images, whose groups each chunk counts (#792).
#[test]
fn the_row_work_pass_holds_at_most_its_admission_over_unequal_domains() {
    if !alone("the_row_work_pass_holds_at_most_its_admission_over_unequal_domains") {
        return;
    }
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let main = operator_of(1024, GridPrecision::F64, Basis::Constant);
    let taller = operator_of(1536, GridPrecision::F64, Basis::Constant);
    // The same with weight images, whose groups each chunk counts (#792).
    let weighted = |image| {
        operator_with(
            image,
            GridPrecision::F64,
            Basis::Constant,
            |geometry, polarization| Box::new(TestKernels::new(geometry, polarization, true, None)),
        )
    };
    let weighted_main = weighted(1024);
    let weighted_taller = weighted(1536);
    assert!(taller.geometry().grid_shape()[1] > main.geometry().grid_shape()[1]);
    assert!(taller.geometry().grid_shape()[1] < 2 * main.geometry().grid_shape()[1]);
    let weighting = WeightingGeneration::Natural { taper: None };
    for (workers, weight) in [1, 2, 4].into_iter().flat_map(|w| [(w, false), (w, true)]) {
        let operators = if weight {
            [&weighted_main, &weighted_taller]
        } else {
            [&main, &taller]
        };
        let domains = operators.map(|operator| PassDomain {
            operator,
            resampler: &resampler,
            partition: Partition::regions(operator, workers.max(2)),
        });
        let mut rows = Rows::random(37, 83, main.geometry());
        rows.add_domain(0.2, 89);
        let team = WorkerTeam::new(workers).expect("team");
        let admitted = row_work_pass_bytes(&domains, workers, weight)
            + source_block_bytes(rows.maximum_block());
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let result = run_row_work_pass(&domains, &weighting, &mut rows, &team, &Cancel::new());
        MEASURING.store(false, Ordering::Relaxed);
        let work = result.expect("row-work pass").rows;
        assert!(work.iter().all(Option::is_some), "both domains are split");
        let held = PEAK.load(Ordering::Relaxed) - baseline;
        assert!(
            held <= admitted,
            "{workers} workers, weight image {weight}: the row-work pass held {held} bytes \
             against {admitted} admitted"
        );
    }
}

#[test]
fn a_pass_with_a_model_holds_at_most_its_admission_over_wide_native_spectra() {
    const IMAGE: usize = 16;
    const NATIVES: usize = 4096;
    if !alone("a_pass_with_a_model_holds_at_most_its_admission_over_wide_native_spectra") {
        return;
    }
    // Two linear output channels, each 2,048 natives wide.
    let resampler = SpectralResampler::channel_local(
        SpectralAxis::new(1.0e9 + 1023.5e4, 2048.0e4, 2).expect("axis"),
        SpectralKernel::Linear,
    );
    assert!(resampler.forms_native_residuals());
    let operator = operator_of(IMAGE, GridPrecision::F64, resampler.basis());
    let model = sparse_model(IMAGE, 2, 7);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    for workers in [1, 2, 3] {
        let mut rows = Rows::at(74, 5, operator.geometry(), natives(NATIVES));
        let domains = [planes(&operator, &resampler, workers)];
        let demand = WaveDemand {
            domains: &domains,
            modes: ModeSet::DATA,
            with_model: true,
            native_spacing_hz: 1.0e4,
            workers,
            backend: BackendChoice::Cpu,
            block: rows.maximum_block(),
            chunk_placements: CHUNK_PLACEMENTS,
            chunk_sets: 1,
        };
        let admitted = demand.bytes(demand.planes()) + source_block_bytes(rows.maximum_block());
        let run = Run {
            residency: Residency::All,
            workers,
            model: Some(&prepare),
            modes: ModeSet::DATA,
            native_spacing_hz: 1.0e4,
            ..Run::initial()
        };
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let result = try_run(&domains, &mut rows, &run);
        MEASURING.store(false, Ordering::Relaxed);
        let (images, summary) = result.expect("pass");
        assert!(summary.samples > 0 && !images.is_empty());
        let held = PEAK.load(Ordering::Relaxed) - baseline;
        assert!(
            held <= admitted,
            "{workers} workers: the pass held {held} bytes against {admitted} admitted"
        );
    }
}

/// #449: a region pass on the CPU whose owners pipeline over two sets of
/// chunks holds at most its admission at two sets
/// ([`WaveDemand::chunk_sets`]): the second set of chunks, the owners'
/// queues and the locks the placer and the owners share. With a model
/// subtracted, and with a weight image, whose owner's buffers every set
/// holds (#784 review, Astra 4).
#[test]
fn a_pipelined_pass_holds_at_most_its_admission_at_two_sets() {
    const IMAGE: usize = 32;
    if !alone("a_pipelined_pass_holds_at_most_its_admission_at_two_sets") {
        return;
    }
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let operator = operator_of(IMAGE, GridPrecision::F64, Basis::Constant);
    let weighted = operator_with(
        IMAGE,
        GridPrecision::F64,
        Basis::Constant,
        |geometry, polarization| Box::new(TestKernels::new(geometry, polarization, true, None)),
    );
    let model = sparse_model(IMAGE, 1, 11);
    let prepare = |_: usize, planes: PlaneRange| -> Result<_, PassError> {
        Ok(window_of(&operator, &model, planes))
    };
    let cases: [(
        &str,
        &MeasurementOperator,
        ModeSet,
        Option<&ModelPreparation<'_>>,
    ); 2] = [
        ("a model", &operator, ModeSet::DATA, Some(&prepare)),
        ("a weight image", &weighted, ModeSet::ALL, None),
    ];
    for ((name, operator, modes, model), workers) in cases
        .into_iter()
        .flat_map(|case| [2, 3].map(|workers| (case, workers)))
    {
        let mut rows = Rows::random(300, 17, operator.geometry());
        let domains = [PassDomain {
            operator,
            resampler: &resampler,
            partition: Partition::regions(operator, workers),
        }];
        let demand = WaveDemand {
            domains: &domains,
            modes,
            with_model: model.is_some(),
            native_spacing_hz: fixture::WIDTH_HZ,
            workers,
            backend: BackendChoice::Cpu,
            block: rows.maximum_block(),
            chunk_placements: 64,
            chunk_sets: 2,
        };
        let admitted = demand.bytes(demand.planes()) + source_block_bytes(rows.maximum_block());
        let run = Run {
            workers,
            model,
            modes,
            chunk_placements: 64,
            chunk_sets: 2,
            ..Run::initial()
        };
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let result = try_run(&domains, &mut rows, &run);
        MEASURING.store(false, Ordering::Relaxed);
        let (images, summary) = result.expect("pass");
        assert!(summary.blocks > 1 && !images.is_empty());
        let held = PEAK.load(Ordering::Relaxed) - baseline;
        assert!(
            held <= admitted,
            "{name}, {workers} workers: the pass held {held} bytes against {admitted} admitted"
        );
    }
}
