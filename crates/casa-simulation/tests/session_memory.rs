// SPDX-License-Identifier: LGPL-3.0-or-later
//! A simulator session holds no more than it is admitted, and no more for
//! more rows (simulation Phase 1d, #841; design §5).
//!
//! The session observes 6 unshadowed ALMA antennas in one XX YY window,
//! applies the default voltage pattern, predicts a one-plane model, and
//! corrupts with 0.1 Jy of noise, on two workers and the CPU. From
//! `sm.open` through `close`, the heap beyond what was live before stays
//! within the most the session's reservations held at once (its model
//! grids, their preparation, the writer and the pipeline) and a fixed
//! allowance, and within the budget. Ten times the integrations, at either
//! channel count, holds at most 256 KiB and one write batch more: nothing
//! the session keeps grows with the rows (MAIN's geometry is computed an
//! integration at a time into the batch; POINTING is generated as it is
//! saved).
//!
//! Each batch is predicted as one block (#855), and the law checks that it
//! is, so the largest block is held to the admission.
//!
//! The write batch is the writer's: it holds one batch or two, and keeps one
//! it has written for the producer, with how it keeps pace, all within its
//! plan, so a longer write may peak with one more. The least of three
//! measured runs is compared, as one run's timing can add a batch more.
//!
//! The process caches (table tiles, FFT plans) are charged once per process
//! and held for it, so the law charges them before measuring; the budget is
//! theirs plus the session's own ([`Case::budget`]), which is small enough
//! that the base cases are written in several batches, so that ten times
//! the rows batch alike. Every case runs once unmeasured before any is
//! measured, so state a process keeps once it has run a session (measures
//! and FFT plans, tracing's callsites) is out of every measurement, and to
//! read the pass's batches.
//!
//! A second law interleaves scans and corruptions (#853 review): the n-th
//! of N one-integration scans is corrupted by the n-th and every later
//! `corrupt`, so `close` adds N(N+1)/2 noise applications. What `close`
//! holds beyond the session it closes stays within its admission and the
//! allowance, and twice the scans hold no more than a little more: the
//! applications are read where the plan keeps them, not copied per scan.
//!
//! A third law corrupts one predicted scan 4,096 and then 65,536 times
//! (#856 review): sixteen times the applications hold no more beyond the
//! admission than a little, so what the prediction stage keeps per noise
//! term is charged where it is kept (on Metal only, for the device).
//!
//! A fourth law predicts through `WProjectFT` (#841 Phase 2): `predict`
//! holds no more than it is admitted while it builds the planes on their
//! screens, and what it keeps after (the kernels and the grids) is what its
//! reservations keep.
//!
//! A fifth law predicts total power (`setoptions(ftmachine='sd')`, #841
//! Phase 2): `predict` holds no more than it is admitted while it tabulates
//! the kernel and reads the model, and keeps what its reservation keeps, the
//! fields' samples.
//!
//! This binary's global allocator counts the live heap; the counters are
//! the process's, so each law runs alone in a process of its own
//! ([`alone`]).

mod pass_summary;

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_coordinates::{
    CoordinateSystem as ImageCoordinates, DirectionCoordinate, Projection, ProjectionType,
    SpectralCoordinate, StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_runtime::{
    HostResources, ReservationWindow, ResourcePolicy, charge_process_caches, process_cache_bytes,
    reserved_memory,
};
use casa_simulation::{
    ArrayConfig, BackendRequest, CoordinateSystem, NoiseSpec, ObserveSpec, PredictSpec,
    PredictionOptions, SessionResources, Simulator, SimulatorOptions, SpectralWindowSpec,
    StateSpec, VoltagePatternSpec,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MeasuresProvider};
use ndarray::{ArrayD, IxDyn};
use num_complex::Complex32;
use pass_summary::{PassSummary, capture};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes and their largest value.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: forwards to `System` with the caller's arguments and adds atomic
// bookkeeping only.
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
                // Old and new were held at once while the block moved.
                PEAK.fetch_max(LIVE.load(Ordering::Relaxed) + new, Ordering::Relaxed);
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
/// law alone and assert that it passed (casa-ms's
/// `creation_write_memory.rs`, whose helper this is).
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
    println!("{stdout}");
    assert!(
        output.status.success(),
        "{name} alone: {}\n{stdout}",
        output.status
    );
    assert!(stdout.contains(&ran), "{name} did not run alone:\n{stdout}");
    false
}

/// Heap a session holds beyond its reservations: its own setters' state,
/// the model image's metadata, the measures frames of an integration, the
/// writer's and the workers' threads, and the tables' metadata at the save.
const ALLOWANCE_BYTES: u64 = 512 * 1024;

/// What ten times the integrations may add beside one write batch.
const ROWS_ALLOWANCE_BYTES: u64 = 256 * 1024;

/// 2024-06-01 00:00 UTC.
const REFERENCE_MJD: f64 = 60_462.0;
const ANTENNAS: usize = 6;
const FREQUENCY_HZ: f64 = 230.0e9;
const CHANNEL_HZ: f64 = 1.0e6;
/// Model pixels a side, of 0.41″.
const PIXELS: usize = 64;
const CELL_RAD: f64 = 2.0e-6;
const CENTRE_RAD: [f64; 2] = [2.617_993_877_991_494_4, 0.593_411_945_678_072];

/// A pinned host of two threads and ample memory without a Metal device,
/// and an explicit policy of two workers and the process caches' charge plus
/// `budget`, on the CPU.
fn resources(budget: u64) -> SessionResources {
    SessionResources::pinned(
        HostResources {
            threads: 2,
            performance_cores: 2,
            available_memory: 8 << 30,
            metal: false,
        },
        ResourcePolicy::Explicit {
            workers: 2,
            memory: process_cache_bytes() + budget,
        },
        BackendRequest::Cpu,
    )
    .expect("resources")
}

/// A one-plane Stokes I model of a few points around [`CENTRE_RAD`], its
/// plane spanning `channels` channels from [`FREQUENCY_HZ`].
fn write_model(path: &Path, channels: usize) {
    let mut coordinates = ImageCoordinates::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        CENTRE_RAD,
        [-CELL_RAD, CELL_RAD],
        [(PIXELS / 2) as f64, (PIXELS / 2) as f64],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    let centre_hz = FREQUENCY_HZ + (channels as f64 - 1.0) / 2.0 * CHANNEL_HZ;
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        centre_hz,
        channels as f64 * CHANNEL_HZ,
        0.0,
        centre_hz,
    ));
    let shape = vec![PIXELS, PIXELS, 1, 1];
    let mut image =
        PagedImage::<f32>::create(shape.clone(), coordinates, path).expect("create model");
    let mut pixels = ArrayD::zeros(IxDyn(&shape));
    for (x, y, flux) in [(32, 32, 1.0_f32), (41, 28, 0.5), (12, 50, 0.25)] {
        pixels[IxDyn(&[x, y, 0, 0])] = flux;
    }
    image.put_slice(&pixels, &[0, 0, 0, 0]).expect("pixels");
    image.set_units("Jy/pixel").expect("units");
    image.save().expect("save model");
}

fn scan(integrations: usize) -> ObserveSpec {
    scan_from(0, integrations)
}

/// `integrations` integrations from integration `first`.
fn scan_from(first: usize, integrations: usize) -> ObserveSpec {
    ObserveSpec {
        source: "target".to_string(),
        spectral_window: "band6".to_string(),
        start_s: 10.0 * first as f64,
        stop_s: 10.0 * (first + integrations) as f64,
        add_observation: false,
        state: StateSpec {
            sig: true,
            reference: false,
            cal: 0.0,
            load: 0.0,
            sub_scan: 0,
            obs_mode: "OBSERVE_TARGET.ON_SOURCE".to_string(),
        },
        observer: "tester".to_string(),
        project: "memory".to_string(),
    }
}

/// One case: `integrations` integrations of `channels` channels.
#[derive(Debug, Clone, Copy)]
struct Case {
    name: &'static str,
    integrations: usize,
    channels: usize,
    /// The session's own budget, beside the process caches' charge. The
    /// writer gets half of what is free at `close`, and its tiled columns'
    /// streaming buffers alone take about 46 MB at 32 channels and 57 MB at
    /// 320; a little more makes batches of several hundred rows.
    budget: u64,
}

/// What a MAIN plan reserves for saving its 16 streamed scalar columns beyond
/// their append buffers (#844 review: 2,260,992 against 1,048,576 bytes);
/// the budgets carry it so the batches stay the sizes the law was set for.
const SCALAR_SAVE_RESERVATION: u64 = 2_260_992 - 1_048_576;

/// The session budget of the 32-channel cases: batches of about 730 rows,
/// small enough that one write batch (about 0.4 MB) leaves the comparison
/// of ten times the rows sensitive to some 20 bytes a row.
const BUDGET_32: u64 = (90 << 20) + (512 << 10) + SCALAR_SAVE_RESERVATION;

/// The session budget of the 320-channel cases: batches of about 460 rows.
const BUDGET_320: u64 = (120 << 20) + SCALAR_SAVE_RESERVATION;

/// Measured runs of each case, of which the least peak is compared.
const MEASURED_RUNS: usize = 3;

/// The cases, and after them `rows x2`: below 8,192 rows (the base cases'
/// 4,200) MAIN's streamed scalar columns stay within their 64 KiB buffers
/// and are saved from memory, within the writer's admitted scalar buffers;
/// with larger batches that put the save above the streaming peak, by about
/// 0.75 MB at 32 channels. Twice the base's rows are past it, so ten times
/// the rows are also held to that case.
const CASES: [Case; 5] = [
    Case {
        name: "base",
        integrations: 200,
        channels: 32,
        budget: BUDGET_32,
    },
    Case {
        name: "rows x10",
        integrations: 2_000,
        channels: 32,
        budget: BUDGET_32,
    },
    Case {
        name: "channels x10",
        integrations: 200,
        channels: 320,
        budget: BUDGET_320,
    },
    Case {
        name: "both x10",
        integrations: 2_000,
        channels: 320,
        budget: BUDGET_320,
    },
    Case {
        name: "rows x2",
        integrations: 400,
        channels: 32,
        budget: BUDGET_32,
    },
];

/// Run `case`'s session from `sm.open` through `close`, writing `ms` and
/// predicting `model`.
fn simulate(case: Case, ms: &Path, model: &Path, measures: &Arc<dyn MeasuresProvider>) {
    let mut sm = configured(ms, measures, case.budget, case.channels);
    sm.observe(&scan(case.integrations)).expect("observe");
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    sm.predict(&PredictSpec {
        images: vec![model.to_path_buf()],
        component_list: None,
        incremental: false,
    })
    .expect("predict");
    sm.set_noise(&noise()).expect("setnoise");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
}

/// 0.1 Jy of simple noise.
fn noise() -> NoiseSpec {
    NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: 0.1,
        on_the_fly: true,
    }
}

/// A session writing `ms` within `budget`, with its antennas, a window of
/// `channels` channels, the field and the times set.
fn configured(
    ms: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    budget: u64,
    channels: usize,
) -> Simulator {
    let mut sm = Simulator::open(
        ms,
        SimulatorOptions {
            measures: Arc::clone(measures),
            resources: resources(budget),
            now: MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC),
        },
    );
    // 60 m apart in a line: no 12 m dish shadows another.
    let centre = [-1_601_185.0, -5_041_977.0, 3_554_875.0];
    let along = |axis: usize, step: f64| {
        (0..ANTENNAS)
            .map(|index| centre[axis] + step * index as f64)
            .collect()
    };
    sm.set_config(&ArrayConfig {
        telescope: "ALMA".to_string(),
        x: along(0, 50.0),
        y: along(1, -15.0),
        z: along(2, 30.0),
        dish_diameter: vec![12.0],
        offset: vec![0.0],
        mount: vec!["alt-az".to_string()],
        antenna_names: vec!["DA".to_string()],
        pad_names: vec!["A".to_string()],
        coordinate_system: CoordinateSystem::Global,
    })
    .expect("setconfig");
    sm.set_spectral_window(&SpectralWindowSpec {
        name: "band6".to_string(),
        start_frequency_hz: FREQUENCY_HZ,
        channel_width_hz: CHANNEL_HZ,
        resolution_hz: CHANNEL_HZ,
        frame: FrequencyRef::TOPO,
        channel_count: channels,
        stokes: "XX YY".to_string(),
    })
    .expect("setspwindow");
    sm.set_field(
        "target",
        &MDirection::from_angles(CENTRE_RAD[0], CENTRE_RAD[1], DirectionRef::J2000),
        "",
        0.0,
    )
    .expect("setfield");
    sm.set_times(10.0, false, MEpoch::from_mjd(REFERENCE_MJD, EpochRef::UTC))
        .expect("settimes");
    sm
}

/// What a case's measured sessions held.
#[derive(Debug, Clone, Copy)]
struct Held {
    /// The least and the most of the runs' heap peaks beyond what was live
    /// before `sm.open`.
    least: u64,
    most: u64,
    /// The most the session's reservations held at once, in any run.
    admitted: u64,
    /// The most heap a run left live after `close` beyond what was live
    /// before `sm.open`: what a session leaves behind.
    left: i64,
}

/// Bytes of one of `case`'s write batches as `pass` batched it: DATA, and
/// FLAG_ROW, UVW, WEIGHT and SIGMA of each row.
fn write_batch_bytes(case: Case, pass: &PassSummary) -> u64 {
    let samples = 2 * case.channels as u64;
    pass.batch_rows * (samples * size_of::<Complex32>() as u64 + 1 + 24 + 4 + 4)
}

/// The directory of `case` under `root`, and its model.
fn case_paths(case: Case, root: &Path) -> (PathBuf, PathBuf) {
    let directory = root.join(case.name.replace(' ', "-"));
    let model = directory.join("model.image");
    (directory, model)
}

/// Write `case`'s model and run its session once, unmeasured: the process
/// then holds whatever a session's first run leaves, and the pass's
/// summary says how it batched.
fn warm(case: Case, root: &Path, measures: &Arc<dyn MeasuresProvider>) -> PassSummary {
    let (directory, model) = case_paths(case, root);
    std::fs::create_dir_all(&directory).expect("case directory");
    write_model(&model, case.channels);
    let warm = directory.join("warm.ms");
    let ((), pass) = capture(|| simulate(case, &warm, &model, measures));
    std::fs::remove_dir_all(&warm).expect("remove the warm-up");
    pass.expect("the pass's summary")
}

/// Run `case`'s session [`MEASURED_RUNS`] times more, measured, once every
/// case has warmed.
fn measure(case: Case, root: &Path, measures: &Arc<dyn MeasuresProvider>) -> Held {
    let (directory, model) = case_paths(case, root);
    let ms = directory.join("measured.ms");
    let mut held = Held {
        least: u64::MAX,
        most: 0,
        admitted: 0,
        left: i64::MIN,
    };
    for _ in 0..MEASURED_RUNS {
        let reserved = reserved_memory();
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let window = ReservationWindow::open();
        simulate(case, &ms, &model, measures);
        let admitted = window.close() - reserved;
        let heap = PEAK.load(Ordering::Relaxed) - baseline;
        let left = LIVE.load(Ordering::Relaxed) as i64 - baseline as i64;
        drop(window);
        std::fs::remove_dir_all(&ms).expect("remove the run");
        held.least = held.least.min(heap);
        held.most = held.most.max(heap);
        held.admitted = held.admitted.max(admitted);
        held.left = held.left.max(left);
    }
    std::fs::remove_dir_all(&directory).expect("remove the case");
    held
}

#[test]
fn a_session_holds_its_admission_and_no_more_for_more_rows() {
    if !alone("a_session_holds_its_admission_and_no_more_for_more_rows") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let passes = CASES.map(|case| warm(case, root.path(), &measures));
    let mut held = Vec::new();
    for (case, pass) in CASES.into_iter().zip(passes) {
        let session = measure(case, root.path(), &measures);
        println!(
            "{}: {} rows in {} batches of {}: heap {} to {}, admitted {} ({} reserved at \
             close), at most {} left after close",
            case.name,
            pass.rows,
            pass.batches,
            pass.batch_rows,
            session.least,
            session.most,
            session.admitted,
            pass.reserved_bytes,
            session.left
        );
        assert_eq!(pass.backend, "Cpu", "{}", case.name);
        assert!(
            pass.batches > 2,
            "{}: the rows are written in several batches: {pass:?}",
            case.name
        );
        assert_eq!(
            pass.block_rows, pass.batch_rows,
            "{}: a batch is predicted as one block, so the law holds the largest to the \
             admission: {pass:?}",
            case.name
        );
        assert!(
            session.most <= session.admitted + ALLOWANCE_BYTES,
            "{}: the heap grew {} bytes, beyond the {} admitted and the allowance",
            case.name,
            session.most,
            session.admitted
        );
        assert!(
            session.most <= case.budget,
            "{}: the heap grew {} bytes, beyond the budget",
            case.name,
            session.most
        );
        held.push((session, pass));
    }
    // Ten times the rows against the base at each channel count, and against
    // twice the base's rows at 32 channels.
    for (few, many) in [(0, 1), (2, 3), (4, 1)] {
        let (few_case, many_case) = (CASES[few], CASES[many]);
        let ((few, few_pass), (many, many_pass)) = (&held[few], &held[many]);
        assert_eq!(
            few_pass.batch_rows, many_pass.batch_rows,
            "{} and {} write batches alike",
            few_case.name, many_case.name
        );
        let allowance = ROWS_ALLOWANCE_BYTES + write_batch_bytes(many_case, many_pass);
        println!(
            "{} - {}: {} bytes (allowed {allowance})",
            many_case.name,
            few_case.name,
            many.least as i64 - few.least as i64
        );
        assert!(
            many.least <= few.least + allowance,
            "{}: the heap grew {} bytes, against {} for {}",
            many_case.name,
            many.least,
            few.least,
            few_case.name
        );
    }
}

/// Scans of the interleaved law's cases: twice the scans give `close` four
/// times the noise applications to add (the old copy of each scan's
/// applications held 12 bytes each: 1.5 MB, then 6 MB).
const INTERLEAVED_SCANS: [usize; 2] = [500, 1_000];

/// What `close` may hold beyond its admission for twice the interleaved
/// scans, beside one write batch. The session's own per-scan state is live
/// before `close`, and what `close` keeps per scan (POINTING's runs) is
/// admitted.
const SCANS_ALLOWANCE_BYTES: u64 = 64 * 1024;

/// What a measured `close` held: its heap peak beyond what the session held
/// before it, and the most its reservations held at once.
#[derive(Debug, Clone, Copy)]
struct Closed {
    heap: u64,
    admitted: u64,
}

impl Closed {
    /// The heap beyond the admission.
    fn unadmitted(self) -> i64 {
        self.heap as i64 - self.admitted as i64
    }
}

/// `scans` one-integration scans, each followed by a `corrupt`, written to
/// `ms` and closed; the pass's summary when `summary`.
fn close_interleaved(
    scans: usize,
    ms: &Path,
    measures: &Arc<dyn MeasuresProvider>,
    summary: bool,
) -> (Closed, Option<PassSummary>) {
    let mut sm = configured(ms, measures, BUDGET_32, 32);
    for scan in 0..scans {
        sm.observe(&scan_from(scan, 1)).expect("observe");
        if scan == 0 {
            // `setnoise` needs rows to corrupt.
            sm.set_noise(&noise()).expect("setnoise");
        }
        sm.corrupt().expect("corrupt");
    }
    measured_close(sm, ms, summary)
}

/// Close `sm`, which writes `ms`, measuring what `close` holds beyond the
/// session and what it was admitted, then delete `ms`; the pass's summary
/// when `summary`.
fn measured_close(sm: Simulator, ms: &Path, summary: bool) -> (Closed, Option<PassSummary>) {
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    let pass = if summary {
        capture(|| sm.close().expect("close")).1
    } else {
        sm.close().expect("close");
        None
    };
    let closed = Closed {
        heap: PEAK.load(Ordering::Relaxed) - baseline,
        admitted: window.close() - reserved,
    };
    drop(window);
    std::fs::remove_dir_all(ms).expect("remove the run");
    (closed, pass)
}

/// Run `scans` interleaved scans [`MEASURED_RUNS`] times, asserting that
/// each `close` held at most its admission and the allowance; the least
/// heap and the most admitted.
fn measure_interleaved(scans: usize, ms: &Path, measures: &Arc<dyn MeasuresProvider>) -> Closed {
    let mut held = Closed {
        heap: u64::MAX,
        admitted: 0,
    };
    for _ in 0..MEASURED_RUNS {
        let (run, _) = close_interleaved(scans, ms, measures, false);
        println!(
            "{scans} interleaved scans: close held {} bytes, admitted {}",
            run.heap, run.admitted
        );
        assert!(
            run.heap <= run.admitted + ALLOWANCE_BYTES,
            "{scans} interleaved scans: close held {} bytes, beyond the {} admitted and the \
             allowance",
            run.heap,
            run.admitted
        );
        held.heap = held.heap.min(run.heap);
        held.admitted = held.admitted.max(run.admitted);
    }
    held
}

#[test]
fn closing_interleaved_corruptions_holds_its_admission_and_no_more_for_more_scans() {
    if !alone("closing_interleaved_corruptions_holds_its_admission_and_no_more_for_more_scans") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let ms = root.path().join("interleaved.ms");
    let [few_pass, many_pass] = INTERLEAVED_SCANS.map(|scans| {
        close_interleaved(scans, &ms, &measures, true)
            .1
            .expect("the pass's summary")
    });
    let [few, many] = INTERLEAVED_SCANS.map(|scans| measure_interleaved(scans, &ms, &measures));
    assert_eq!(
        few_pass.batch_rows, many_pass.batch_rows,
        "the interleaved cases write batches alike"
    );
    let samples = 2 * 32_u64;
    let batch = many_pass.batch_rows * (samples * size_of::<Complex32>() as u64 + 1 + 24 + 4 + 4);
    let allowance = (SCANS_ALLOWANCE_BYTES + batch) as i64;
    println!(
        "{} - {} interleaved scans: {} bytes more heap, {} more admitted (allowed {allowance} \
         beyond it)",
        INTERLEAVED_SCANS[1],
        INTERLEAVED_SCANS[0],
        many.heap as i64 - few.heap as i64,
        many.admitted as i64 - few.admitted as i64
    );
    assert!(
        many.unadmitted() <= few.unadmitted() + allowance,
        "{} interleaved scans: close held {} bytes beyond its admission, against {} for {}",
        INTERLEAVED_SCANS[1],
        many.unadmitted(),
        few.unadmitted(),
        INTERLEAVED_SCANS[0]
    );
}

/// Noise applications of the repeated-corruption law's cases: one predicted
/// scan corrupted this many times. The prediction stage once allocated a
/// term for each, 12 bytes, on the CPU too, where it charged none (#856
/// review: 768 KiB at 65,536, beyond the allowance).
const REPEATED_APPLICATIONS: [usize; 2] = [4_096, 65_536];

/// A one-integration scan predicted from `model` and corrupted
/// `applications` times, written to `ms` and closed: what `close` held.
fn close_repeated(
    applications: usize,
    ms: &Path,
    model: &Path,
    measures: &Arc<dyn MeasuresProvider>,
) -> Closed {
    let mut sm = configured(ms, measures, BUDGET_32, 32);
    sm.observe(&scan(1)).expect("observe");
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    sm.predict(&PredictSpec {
        images: vec![model.to_path_buf()],
        component_list: None,
        incremental: false,
    })
    .expect("predict");
    sm.set_noise(&noise()).expect("setnoise");
    for _ in 0..applications {
        sm.corrupt().expect("corrupt");
    }
    measured_close(sm, ms, false).0
}

#[test]
fn a_predicting_close_holds_its_admission_however_often_its_scan_is_corrupted() {
    if !alone("a_predicting_close_holds_its_admission_however_often_its_scan_is_corrupted") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let model = root.path().join("model.image");
    write_model(&model, 32);
    let ms = root.path().join("repeated.ms");
    for applications in REPEATED_APPLICATIONS {
        close_repeated(applications, &ms, &model, &measures);
    }
    let [few, many] = REPEATED_APPLICATIONS.map(|applications| {
        let mut held = Closed {
            heap: u64::MAX,
            admitted: 0,
        };
        for _ in 0..MEASURED_RUNS {
            let run = close_repeated(applications, &ms, &model, &measures);
            println!(
                "{applications} applications: close held {} bytes, admitted {}",
                run.heap, run.admitted
            );
            assert!(
                run.heap <= run.admitted + ALLOWANCE_BYTES,
                "{applications} applications: close held {} bytes, beyond the {} admitted and \
                 the allowance",
                run.heap,
                run.admitted
            );
            held.heap = held.heap.min(run.heap);
            held.admitted = held.admitted.max(run.admitted);
        }
        held
    });
    println!(
        "{} - {} applications: {} bytes more beyond the admission",
        REPEATED_APPLICATIONS[1],
        REPEATED_APPLICATIONS[0],
        many.unadmitted() - few.unadmitted()
    );
    assert!(
        many.unadmitted() <= few.unadmitted() + SCANS_ALLOWANCE_BYTES as i64,
        "{} applications: close held {} bytes beyond its admission, against {} for {}",
        REPEATED_APPLICATIONS[1],
        many.unadmitted(),
        few.unadmitted(),
        REPEATED_APPLICATIONS[0]
    );
}

/// The W-projection law's planes.
const W_PLANES: i64 = 128;

/// The W-projection law's session budget: the 32-channel cases' and room
/// for the planes beside `close`'s writer.
const BUDGET_W: u64 = BUDGET_32 + (32 << 20);

/// What a `predict` held while it ran and kept after, beyond what was live
/// before it, and what was admitted.
#[derive(Debug, Clone, Copy)]
struct Predicted {
    /// The heap's peak during `predict`, and the most its reservations held.
    during: Closed,
    /// The heap left live after `predict`, and what its reservations keep.
    kept: Closed,
}

/// A one-integration scan predicted from `model` through `WProjectFT` with
/// [`W_PLANES`] planes: what `predict` held and kept. The session is closed
/// and `ms` deleted after.
fn predict_w(ms: &Path, model: &Path, measures: &Arc<dyn MeasuresProvider>) -> Predicted {
    let mut sm = configured(ms, measures, BUDGET_W, 32);
    sm.observe(&scan(1)).expect("observe");
    sm.set_options(&PredictionOptions {
        ftmachine: "wproject".to_string(),
        w_planes: W_PLANES,
        ..PredictionOptions::default()
    })
    .expect("setoptions");
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    sm.predict(&PredictSpec {
        images: vec![model.to_path_buf()],
        component_list: None,
        incremental: false,
    })
    .expect("predict");
    let predicted = Predicted {
        during: Closed {
            heap: PEAK.load(Ordering::Relaxed) - baseline,
            admitted: window.close() - reserved,
        },
        kept: Closed {
            heap: LIVE.load(Ordering::Relaxed).saturating_sub(baseline),
            admitted: reserved_memory() - reserved,
        },
    };
    drop(window);
    sm.close().expect("close");
    std::fs::remove_dir_all(ms).expect("remove the run");
    predicted
}

/// `predict` through `WProjectFT` builds its planes on screens and keeps the
/// kernels cropped from them (this PR, #841 Phase 2): while it runs, the
/// heap stays within the most its reservations held (the screens, then the
/// kernels, then the model's grids and their preparation) and the
/// allowance; after it, what the session keeps (the kernels and the grids)
/// is what its reservations keep, within the allowance either way. The
/// 128 planes keep about 2.1 MB of kernels, built on about 1.4 MB of
/// screens: a predict that kept the kernels uncharged held 2.18 MB against
/// 33 KB charged, and one that kept the screens' charge would be as far
/// apart the other way.
#[test]
fn a_w_projection_predict_holds_its_admission_and_keeps_what_it_charges() {
    if !alone("a_w_projection_predict_holds_its_admission_and_keeps_what_it_charges") {
        return;
    }
    let resources = resources(BUDGET_32);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let model = root.path().join("model.image");
    write_model(&model, 32);
    let ms = root.path().join("w.ms");
    // Once unmeasured: state a process keeps once it has predicted.
    predict_w(&ms, &model, &measures);
    for _ in 0..MEASURED_RUNS {
        let Predicted { during, kept } = predict_w(&ms, &model, &measures);
        println!(
            "W predict: held {} bytes against {} admitted; kept {} against {} charged",
            during.heap, during.admitted, kept.heap, kept.admitted
        );
        assert!(
            kept.admitted > 2 * ALLOWANCE_BYTES,
            "the kernels and grids are charged: {kept:?}"
        );
        assert!(
            during.heap <= during.admitted + ALLOWANCE_BYTES,
            "predict held {} bytes, beyond the {} admitted and the allowance",
            during.heap,
            during.admitted
        );
        assert!(
            kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES,
            "predict keeps {} bytes against the {} its reservations keep",
            kept.heap,
            kept.admitted
        );
    }
}

/// Fields of the total-power law, each observed for one integration.
const SINGLE_DISH_FIELDS: usize = 1_000;

/// Channels of the total-power law: each field keeps 5,120 bytes of
/// samples in XX and YY.
const SINGLE_DISH_CHANNELS: usize = 320;

/// [`SINGLE_DISH_FIELDS`] one-integration scans of their own fields,
/// predicted from `model` through `SDGrid` with the `PB` kernel: what
/// `predict` held and kept. The session is dropped unwritten.
fn predict_single_dish(model: &Path, measures: &Arc<dyn MeasuresProvider>) -> Predicted {
    let directory = tempfile::tempdir().expect("temporary directory");
    let mut sm = configured(
        &directory.path().join("sd.ms"),
        measures,
        BUDGET_320,
        SINGLE_DISH_CHANNELS,
    );
    for field in 0..SINGLE_DISH_FIELDS {
        let name = format!("f{field}");
        let direction = MDirection::from_angles(
            CENTRE_RAD[0] + 1.0e-7 * field as f64,
            CENTRE_RAD[1],
            DirectionRef::J2000,
        );
        sm.set_field(&name, &direction, "", 0.0).expect("setfield");
        sm.observe(&ObserveSpec {
            source: name,
            ..scan_from(field, 1)
        })
        .expect("observe");
    }
    sm.set_options(&PredictionOptions {
        ftmachine: "sd".to_string(),
        grid_function: "PB".to_string(),
        ..PredictionOptions::default()
    })
    .expect("setoptions");
    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    sm.predict(&PredictSpec {
        images: vec![model.to_path_buf()],
        component_list: None,
        incremental: false,
    })
    .expect("predict");
    let predicted = Predicted {
        during: Closed {
            heap: PEAK.load(Ordering::Relaxed) - baseline,
            admitted: window.close() - reserved,
        },
        kept: Closed {
            heap: LIVE.load(Ordering::Relaxed).saturating_sub(baseline),
            admitted: reserved_memory() - reserved,
        },
    };
    drop(window);
    predicted
}

/// `predict` through `SDGrid` admits its samples and its preparation (the
/// kernel, as tabulated and as kept, and the model plane read) before it
/// forms them (this PR, #841 Phase 2): while it runs, the heap stays within
/// that admission and the allowance; after it, what the session keeps (the
/// 1,000 fields' 5.1 MB of samples) is what its reservation keeps.
#[test]
fn a_total_power_predict_keeps_what_it_charges() {
    if !alone("a_total_power_predict_keeps_what_it_charges") {
        return;
    }
    let resources = resources(BUDGET_320);
    charge_process_caches(resources.host(), resources.policy()).expect("the process caches");
    let measures = casa_test_support::deterministic_measures_provider();
    let root = tempfile::tempdir().expect("temporary directory");
    let model = root.path().join("model.image");
    write_model(&model, SINGLE_DISH_CHANNELS);
    // Once unmeasured: state a process keeps once it has predicted.
    predict_single_dish(&model, &measures);
    for _ in 0..MEASURED_RUNS {
        let Predicted { during, kept } = predict_single_dish(&model, &measures);
        println!(
            "SD predict: held {} bytes against {} admitted; kept {} against {} charged",
            during.heap, during.admitted, kept.heap, kept.admitted
        );
        assert!(
            kept.admitted >= (SINGLE_DISH_FIELDS * SINGLE_DISH_CHANNELS * 2 * 8) as u64,
            "the samples are charged: {kept:?}"
        );
        assert!(
            during.heap <= during.admitted + ALLOWANCE_BYTES,
            "predict held {} bytes, beyond the {} admitted and the allowance",
            during.heap,
            during.admitted
        );
        assert!(
            kept.unadmitted().unsigned_abs() <= ALLOWANCE_BYTES,
            "predict keeps {} bytes against the {} its reservation keeps",
            kept.heap,
            kept.admitted
        );
    }
}
