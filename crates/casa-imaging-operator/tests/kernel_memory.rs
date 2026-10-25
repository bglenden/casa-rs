// SPDX-License-Identifier: LGPL-3.0-or-later
//! Building a kernel set holds no more than the bytes it states before it
//! allocates them, and the set holds what it says it holds.
//!
//! A run admits a W-projection or mosaic set in two stages: the screens
//! (`screens_bytes`, before any is transformed), then the kernels cut from
//! them (`kernel_bytes`, once their supports are known), and keeps the
//! set's `resident_bytes` while the operator lives; a spheroidal set's
//! `bytes` before it is built; an AW catalog's `charge_bytes` once its
//! index is read, for the workers that fetch its cells. This binary's
//! global allocator counts the live heap and measures each stage against
//! its statement. A global allocator applies to this test binary alone,
//! and each law runs alone in a process of its own ([`alone`]).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_model::{
    EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput, NativeAwTerms,
    PolarizationCoordinate,
};
use casa_imaging_operator::{
    AiryDish, AwCatalog, AwIndexing, CellHold, CfKey, ConvolutionFunctionSet, GridGeometry,
    GridPadding, ImageExtent, MosaicPb, MosaicWindow, PaddingFactor, PolarizationRouting,
    Spheroidal, WPlaneCount, WPlanes,
};
use casa_types::Correlation;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);
static BASELINE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

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
        PEAK.fetch_max(
            live.saturating_sub(BASELINE.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
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

/// The most `stage` adds to the live heap while it runs, and what it adds
/// to or frees from it by its end, with its result.
fn measure<T>(stage: impl FnOnce() -> T) -> (u64, i64, T) {
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let result = stage();
    MEASURING.store(false, Ordering::Relaxed);
    let left = LIVE.load(Ordering::Relaxed) as i64 - baseline as i64;
    (PEAK.load(Ordering::Relaxed), left, result)
}

fn routing() -> PolarizationRouting {
    PolarizationRouting::compile(
        &[Correlation::XX, Correlation::YY],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing")
}

fn geometry(image: usize, increment_rad: f64, padding: GridPadding) -> GridGeometry {
    GridGeometry::new(
        ImageExtent {
            shape: [image, image],
            increment_rad: [-increment_rad, increment_rad],
            reference_pixel: [image / 2, image / 2],
        },
        padding,
    )
    .expect("geometry")
}

#[test]
fn w_projection_stages_hold_at_most_what_they_state() {
    if !alone("w_projection_stages_hold_at_most_what_they_state") {
        return;
    }
    let routing = routing();
    for (image, planes) in [(64, 1), (64, 8), (200, 16)] {
        let geometry = geometry(
            image,
            1.0e-3,
            GridPadding::CasaComposite(PaddingFactor::TCLEAN),
        );
        let count = WPlaneCount::Fixed(planes);
        let label = format!("{image} px, {planes} planes");
        let stated = WPlanes::screens_bytes(&geometry, count).expect("screens bytes");
        // FFTW's plan for the screen joins the process's plan cache, which
        // keeps it; plan it before measuring.
        drop(WPlanes::screens(&geometry, &routing, count).expect("screens"));
        let (peak, left, screens) =
            measure(|| WPlanes::screens(&geometry, &routing, count).expect("screens"));
        assert!(
            peak <= stated,
            "{label}: screens held {peak}, stated {stated}"
        );
        assert_eq!(
            left,
            screens.bytes() as i64,
            "{label}: the screens hold what they say"
        );
        let kernels = screens.kernel_bytes();
        let screens_bytes = screens.bytes() as i64;
        let (peak, left, set) = measure(|| screens.finish().expect("planes"));
        assert!(
            peak <= kernels,
            "{label}: the kernels held {peak} beside the screens, stated {kernels}"
        );
        assert_eq!(
            left + screens_bytes,
            set.resident_bytes() as i64,
            "{label}: the set holds what it says once its screens are freed"
        );
    }
}

#[test]
fn mosaic_stages_hold_at_most_what_they_state() {
    if !alone("mosaic_stages_hold_at_most_what_they_state") {
        return;
    }
    let routing = routing();
    let geometry = geometry(64, 2.0e-5, GridPadding::None);
    let dishes = [
        AiryDish::casa_alma(12.0, &geometry),
        AiryDish::casa_alma(7.0, &geometry),
    ];
    let window = |spectral_window, selected: Vec<f64>, top: f64| MosaicWindow {
        spectral_window,
        window_frequencies_hz: vec![top - 2.0e9, top],
        channel_width_hz: 1.0e6,
        selected_frequencies_hz: selected,
    };
    let eight = (0..8)
        .map(|channel| 98.0e9 + 0.25e9 * f64::from(channel))
        .collect::<Vec<_>>();
    let windows = [window(0, vec![1.0e11], 1.0e11), window(1, eight, 101.0e9)];
    for dishes in [&dishes[..1], &dishes[..]] {
        let label = format!("{} dish classes", dishes.len());
        let stated =
            MosaicPb::screens_bytes(&geometry, 1.0e11, dishes, &windows).expect("screens bytes");
        // FFTW's plan for the screen joins the process's plan cache, which
        // keeps it; plan it before measuring.
        drop(MosaicPb::screens(&geometry, &routing, 1.0e11, dishes, &windows).expect("screens"));
        let (peak, left, screens) = measure(|| {
            MosaicPb::screens(&geometry, &routing, 1.0e11, dishes, &windows).expect("screens")
        });
        assert!(
            peak <= stated,
            "{label}: screens held {peak}, stated {stated}"
        );
        assert_eq!(
            left,
            screens.bytes() as i64,
            "{label}: the screens hold what they say"
        );
        let kernels = screens.kernel_bytes();
        let screens_bytes = screens.bytes() as i64;
        let (peak, left, set) = measure(|| screens.finish().expect("cells"));
        assert!(
            peak <= kernels,
            "{label}: the cells held {peak} beside the screens, stated {kernels}"
        );
        assert_eq!(
            left + screens_bytes,
            set.resident_bytes() as i64,
            "{label}: the set holds what it says once its screens are freed"
        );
    }
}

#[test]
fn a_spheroidal_set_holds_what_it_states() {
    if !alone("a_spheroidal_set_holds_what_it_states") {
        return;
    }
    let routing = routing();
    for image in [64, 1000] {
        let geometry = geometry(
            image,
            1.0e-5,
            GridPadding::CasaComposite(PaddingFactor::TCLEAN),
        );
        let (peak, _, set) = measure(|| Spheroidal::new(&geometry, &routing));
        assert!(
            peak <= Spheroidal::bytes(&geometry),
            "{image} px: held {peak}, stated {}",
            Spheroidal::bytes(&geometry)
        );
        drop(set);
    }
}

/// A native AW request as the `aw_laws` make: one parallactic angle, two
/// frequencies, two w planes and two Mueller elements for an image of 20″
/// cells, oversampled four times, on a working grid of `size` cells.
fn aw_request(size: usize) -> NativeAwRequestInput {
    aw_request_with(size, 2, 2)
}

/// [`aw_request`] with `frequencies` SPWs 10 MHz apart from 1.4 GHz and
/// `w_planes` w planes: `frequencies × w_planes` groups of two cells.
fn aw_request_with(size: usize, frequencies: usize, w_planes: usize) -> NativeAwRequestInput {
    const CELL_RAD: f64 = 1.0e-4;
    const OVERSAMPLING: usize = 4;
    let surface = EvlaDishSurface::new(
        (0..=125)
            .map(|i| {
                let r = f64::from(i) / 10.0;
                [r, r * r / 36.0, r / 18.0]
            })
            .collect(),
    )
    .expect("surface");
    let max_w = 1.0 / (CELL_RAD * 4.0);
    let w_increment = f64::from(((w_planes - 1) * (w_planes - 1)) as f32) / max_w;
    let working = CELL_RAD * OVERSAMPLING as f64;
    let spacing_hz = if frequencies == 2 { 2.0e8 } else { 1.0e7 };
    NativeAwRequestInput {
        surface,
        antenna_diameter_m: 25.0,
        frequencies: (0..frequencies)
            .map(|window| {
                let frequency = 1.4e9 + window as f64 * spacing_hz;
                NativeAwFrequencyGroup {
                    spectral_window: window as u32,
                    channel_frequencies_hz: vec![frequency],
                    cf_frequency_hz: frequency,
                }
            })
            .collect(),
        w_values: (0..w_planes)
            .map(|index| (index * index) as f64 / w_increment)
            .collect(),
        w_increment,
        pa_values: vec![0.31],
        mueller_elements: vec![0, 15],
        reference_frequency_hz: 1.5e9,
        grid: NativeAwGrid {
            size,
            sky_increment_rad: [-working, working],
            oversampling: OVERSAMPLING,
        },
        terms: NativeAwTerms {
            aperture: true,
            w_term: true,
            prolate_spheroidal: false,
            wideband: true,
            conjugate_beams: false,
        },
        maximum_cells: 2 * frequencies * w_planes,
    }
}

/// #738, #708: native generation holds at most what the run admits for it
/// ([`AwCatalog::generation_bytes`]), on two working grids, on one worker
/// and on four, each of which generates one aperture's cells.
#[test]
fn native_generation_holds_at_most_its_charge() {
    if !alone("native_generation_holds_at_most_its_charge") {
        return;
    }
    for (size, workers) in [(64, 1), (64, 4), (256, 1), (256, 4)] {
        let request = aw_request(size);
        let root = tempfile::tempdir().expect("cache directory");
        let (peak, _, generated) =
            measure(|| AwCatalog::generate_native(root.path(), &request, false, workers));
        generated.expect("generate");
        let charge = AwCatalog::generation_bytes(&request, workers);
        eprintln!("working grid {size}, {workers} workers: held {peak}, charged {charge}");
        assert!(
            peak <= charge,
            "working grid {size}, {workers} workers: generation held {peak}, charged {charge}"
        );
    }
}

/// #747 review: generation's bookkeeping grows with the request, and is
/// charged. Requests of 16 frequencies and 512 or 2,048 w planes, 16,384
/// (the review's probe) and 65,536 cells, whose flat dish fails at its
/// first cell: on one worker and on four, from a cleared directory and as a
/// fill, which first checks the directory's state, generation holds at most
/// its charge.
#[test]
fn generation_of_a_large_request_holds_at_most_its_charge() {
    if !alone("generation_of_a_large_request_holds_at_most_its_charge") {
        return;
    }
    for w_planes in [512, 2048] {
        let mut request = aw_request_with(64, 16, w_planes);
        request.surface =
            EvlaDishSurface::new((0..=125).map(|i| [f64::from(i) / 10.0, 0.0, 0.0]).collect())
                .expect("a valid surface");
        for (only_missing, workers) in [(false, 1), (false, 4), (true, 1), (true, 4)] {
            let root = tempfile::tempdir().expect("cache directory");
            let (peak, _, generated) = measure(|| {
                AwCatalog::generate_native(root.path(), &request, only_missing, workers)
            });
            assert!(generated.is_err(), "a flat dish has no aperture");
            let charge = AwCatalog::generation_bytes(&request, workers);
            let case = format!("{w_planes} w planes, fill {only_missing}, {workers} workers");
            eprintln!("{case}: held {peak}, charged {charge}");
            assert!(
                peak <= charge,
                "{case}: generation held {peak}, charged {charge}"
            );
        }
    }
}

/// Catalogs of 4, 64 and 512 groups (the last two the #745 review's
/// probes), whose index and cache bookkeeping grow with the catalog.
#[test]
fn an_aw_catalog_holds_at_most_its_charge() {
    if !alone("an_aw_catalog_holds_at_most_its_charge") {
        return;
    }
    for (frequencies, w_planes) in [(2, 2), (2, 32), (16, 32)] {
        let root = tempfile::tempdir().expect("cache directory");
        AwCatalog::generate_native(
            root.path(),
            &aw_request_with(64, frequencies, w_planes),
            false,
            4,
        )
        .expect("generate");
        catalog_holds_at_most_its_charge(root.path());
    }
}

/// Open the catalog generated into `root`, at most `bound` bytes of cells
/// resident.
fn aw_open(root: &std::path::Path, bound: usize) -> AwCatalog {
    let routing = PolarizationRouting::compile(
        &[Correlation::RR, Correlation::LL],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing");
    let indexing = AwIndexing {
        conjugate_beams: false,
        image_reference_hz: 1.5e9,
    };
    let geometry = geometry(64, 1.0e-4, GridPadding::None);
    AwCatalog::open_casa(root, indexing, &geometry, &routing, bound).expect("open")
}

/// Fetch group `group`'s imaging and weight cells through the gridding
/// hold, as a pass's modes do, and the next group's prediction cell through
/// the prediction hold: a CPU backend's two holds (#449, #773 review).
fn fetch(catalog: &AwCatalog, group: usize, holds: &mut [CellHold; 2]) {
    let key = CfKey {
        group: group as u16,
        cube: ((group + 1) % catalog.groups()) as u16,
    };
    let [gridding, prediction] = holds;
    assert!(catalog.taps(key, gridding).half_support()[0] > 0);
    assert!(catalog.weight_taps(key, gridding).is_some());
    assert!(catalog.prediction_taps(key, prediction).half_support()[0] > 0);
}

/// Open the catalog in `root` and fetch every group through one worker's
/// holds, with a bound below one group and one above every group.
fn catalog_holds_at_most_its_charge(root: &std::path::Path) {
    let open_and_fetch = |bound: usize| {
        let catalog = aw_open(root, bound);
        let mut holds = [CellHold::new(), CellHold::new()];
        for group in 0..catalog.groups() {
            fetch(&catalog, group, &mut holds);
        }
        catalog
    };
    // The cells are read through casa-tables, whose process-wide tile cache
    // keeps what it reads under its own charge; read them once first.
    drop(open_and_fetch(usize::MAX));
    // A bound below one group evicts at every fetch; a large one keeps
    // every group.
    for bound in [1, 64 << 20] {
        let (peak, _, catalog) = measure(|| open_and_fetch(bound));
        let charge = catalog.charge_bytes(1);
        eprintln!("bound {bound}: held {peak}, charged {charge}");
        assert!(
            peak <= charge,
            "bound {bound}: opening and fetching held {peak}, charged {charge}"
        );
    }
}

/// #744: workers that load different groups at once, outside the cache
/// lock, each keeping the cells it fetched last after a cache below one
/// group has evicted them (a gridding group's and a prediction group's,
/// #449), hold at most the charge for that many holders.
#[test]
fn concurrent_holders_hold_at_most_their_charge() {
    const WORKERS: usize = 4;
    const ROUNDS: usize = 64;
    if !alone("concurrent_holders_hold_at_most_their_charge") {
        return;
    }
    let root = tempfile::tempdir().expect("cache directory");
    AwCatalog::generate_native(root.path(), &aw_request(64), false, 4).expect("generate");
    // Warm the process-wide tile cache, as above.
    let warm = aw_open(root.path(), usize::MAX);
    for group in 0..warm.groups() {
        fetch(&warm, group, &mut [CellHold::new(), CellHold::new()]);
    }
    drop(warm);
    let (peak, _, (loads, charge)) = measure(|| {
        let catalog = aw_open(root.path(), 1);
        let groups = catalog.groups();
        assert!(groups >= WORKERS, "every worker loads its own group");
        // Every round, each worker grids through a group no other worker
        // grids through, and predicts through the next, as a CPU residual
        // does.
        let round = std::sync::Barrier::new(WORKERS);
        std::thread::scope(|scope| {
            for worker in 0..WORKERS {
                let (catalog, round) = (&catalog, &round);
                scope.spawn(move || {
                    let mut holds = [CellHold::new(), CellHold::new()];
                    for step in 0..ROUNDS {
                        round.wait();
                        fetch(catalog, (worker + step) % groups, &mut holds);
                    }
                });
            }
        });
        (catalog.loads(), catalog.charge_bytes(WORKERS))
    });
    eprintln!("{WORKERS} workers, {loads} loads: held {peak}, charged {charge}");
    assert!(
        loads > ROUNDS as u64,
        "the workers loaded groups beside each other"
    );
    assert!(
        peak <= charge,
        "{WORKERS} workers held {peak}, charged {charge}"
    );
}
