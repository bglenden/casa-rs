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
//! channel count, holds at most 256 KiB more: nothing the session keeps
//! grows with the rows (MAIN's geometry is computed an integration at a time
//! into the batch; POINTING is generated as it is saved).
//!
//! The process caches (table tiles, FFT plans) are charged once per process
//! and held for it, so the law charges them before measuring; the budget is
//! theirs plus the session's own ([`Case::budget`]), which is small enough
//! that the base cases are written in several batches, so that ten times
//! the rows batch alike. Each case runs once unmeasured first, so one-time
//! state (measures and FFT plans, tracing's callsites) is out of the
//! measurement, and to read the pass's batches.
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
    SessionResources, Simulator, SimulatorOptions, SpectralWindowSpec, StateSpec,
    VoltagePatternSpec,
};
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, MEpoch, MeasuresProvider};
use ndarray::{ArrayD, IxDyn};
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

/// What ten times the integrations may add.
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
    ObserveSpec {
        source: "target".to_string(),
        spectral_window: "band6".to_string(),
        start_s: 0.0,
        stop_s: 10.0 * integrations as f64,
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
    /// 320; a few MB more make batches of a few thousand rows, and of a few
    /// hundred.
    budget: u64,
}

/// The session budget of the 32-channel cases: batches of 2,048 rows.
const BUDGET_32: u64 = 100 << 20;

/// The session budget of the 320-channel cases: batches of about 500 rows.
const BUDGET_320: u64 = 120 << 20;

const CASES: [Case; 4] = [
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
];

/// Run `case`'s session from `sm.open` through `close`, writing `ms` and
/// predicting `model`.
fn simulate(case: Case, ms: &Path, model: &Path, measures: &Arc<dyn MeasuresProvider>) {
    let mut sm = Simulator::open(
        ms,
        SimulatorOptions {
            measures: Arc::clone(measures),
            resources: resources(case.budget),
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
        channel_count: case.channels,
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
    sm.observe(&scan(case.integrations)).expect("observe");
    sm.set_vp(&VoltagePatternSpec::default()).expect("setvp");
    sm.predict(&PredictSpec {
        images: vec![model.to_path_buf()],
        component_list: None,
        incremental: false,
    })
    .expect("predict");
    sm.set_noise(&NoiseSpec {
        mode: "simplenoise".to_string(),
        simple_noise_jy: 0.1,
        on_the_fly: true,
    })
    .expect("setnoise");
    sm.corrupt().expect("corrupt");
    sm.close().expect("close");
}

/// What one measured session held.
#[derive(Debug, Clone, Copy)]
struct Held {
    /// The heap's peak beyond what was live before `sm.open`.
    heap: u64,
    /// The most the session's reservations held at once.
    admitted: u64,
}

/// Run `case` once to warm the process and read its pass, then once
/// measured.
fn measure(case: Case, root: &Path, measures: &Arc<dyn MeasuresProvider>) -> (Held, PassSummary) {
    let directory = root.join(case.name.replace(' ', "-"));
    std::fs::create_dir_all(&directory).expect("case directory");
    let model: PathBuf = directory.join("model.image");
    write_model(&model, case.channels);
    let warm = directory.join("warm.ms");
    let ((), pass) = capture(|| simulate(case, &warm, &model, measures));
    std::fs::remove_dir_all(&warm).expect("remove the warm-up");
    let ms = directory.join("measured.ms");

    let reserved = reserved_memory();
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let window = ReservationWindow::open();
    simulate(case, &ms, &model, measures);
    let admitted = window.close() - reserved;
    let heap = PEAK.load(Ordering::Relaxed) - baseline;
    drop(window);
    std::fs::remove_dir_all(&directory).expect("remove the case");
    (Held { heap, admitted }, pass.expect("the pass's summary"))
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
    let mut held = Vec::new();
    for case in CASES {
        let (session, pass) = measure(case, root.path(), &measures);
        println!(
            "{}: {} rows in {} batches of {}: heap {} admitted {} ({} reserved at close)",
            case.name,
            pass.rows,
            pass.batches,
            pass.batch_rows,
            session.heap,
            session.admitted,
            pass.reserved_bytes
        );
        assert_eq!(pass.backend, "Cpu", "{}", case.name);
        assert!(
            pass.batches > 2,
            "{}: the rows are written in several batches: {pass:?}",
            case.name
        );
        assert!(
            session.heap <= session.admitted + ALLOWANCE_BYTES,
            "{}: the heap grew {} bytes, beyond the {} admitted and the allowance",
            case.name,
            session.heap,
            session.admitted
        );
        assert!(
            session.heap <= case.budget,
            "{}: the heap grew {} bytes, beyond the budget",
            case.name,
            session.heap
        );
        held.push((session, pass));
    }
    for (few, many) in [(0, 1), (2, 3)] {
        let (few_case, many_case) = (CASES[few], CASES[many]);
        let ((few, few_pass), (many, many_pass)) = (&held[few], &held[many]);
        assert_eq!(
            few_pass.batch_rows, many_pass.batch_rows,
            "{} and {} write batches alike",
            few_case.name, many_case.name
        );
        assert!(
            many.heap <= few.heap + ROWS_ALLOWANCE_BYTES,
            "{}: the heap grew {} bytes, against {} for {}",
            many_case.name,
            many.heap,
            few.heap,
            few_case.name
        );
    }
}
