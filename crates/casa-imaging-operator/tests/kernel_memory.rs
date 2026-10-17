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
//! its statement. A global allocator applies to this test binary alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_model::{
    CorrelationType, EvlaDishSurface, NativeAwFrequencyGroup, NativeAwGrid, NativeAwRequestInput,
    NativeAwTerms, PolarizationCoordinate,
};
use casa_imaging_operator::{
    AiryDish, AwCatalog, AwIndexing, CellHold, CfKey, ConvolutionFunctionSet, GridGeometry,
    GridPadding, ImageExtent, MosaicPb, MosaicWindow, PolarizationRouting, Spheroidal, WPlaneCount,
    WPlanes,
};

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

/// One measurement at a time: the allocator is the process's.
static MEASUREMENT: Mutex<()> = Mutex::new(());

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
        &[CorrelationType::LinearXx, CorrelationType::LinearYy],
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
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let routing = routing();
    for (image, planes) in [(64, 1), (64, 8), (200, 16)] {
        let geometry = geometry(image, 1.0e-3, GridPadding::CasaComposite);
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
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
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
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let routing = routing();
    for image in [64, 1000] {
        let geometry = geometry(image, 1.0e-5, GridPadding::CasaComposite);
        let (peak, _, set) = measure(|| Spheroidal::new(&geometry, &routing));
        assert!(
            peak <= Spheroidal::bytes(&geometry),
            "{image} px: held {peak}, stated {}",
            Spheroidal::bytes(&geometry)
        );
        drop(set);
    }
}

/// Generate an AW catalog into `root` natively, as the `aw_laws` do
/// ([`aw_request`] on a 64-cell working grid).
fn aw_cache(root: &std::path::Path) {
    AwCatalog::generate_native(root, &aw_request(64), false).expect("generate");
}

/// A native AW request as the `aw_laws` make: one parallactic angle, two
/// frequencies, two w planes and two Mueller elements for an image of 20″
/// cells, oversampled four times, on a working grid of `size` cells.
fn aw_request(size: usize) -> NativeAwRequestInput {
    const CELL_RAD: f64 = 1.0e-4;
    const OVERSAMPLING: usize = 4;
    const W_PLANES: usize = 2;
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
    let w_increment = f64::from(((W_PLANES - 1) * (W_PLANES - 1)) as f32) / max_w;
    let working = CELL_RAD * OVERSAMPLING as f64;
    NativeAwRequestInput {
        surface,
        antenna_diameter_m: 25.0,
        frequencies: [1.4e9, 1.6e9]
            .iter()
            .enumerate()
            .map(|(window, frequency)| NativeAwFrequencyGroup {
                spectral_window: window as u32,
                channel_frequencies_hz: vec![*frequency],
                cf_frequency_hz: *frequency,
            })
            .collect(),
        w_values: (0..W_PLANES)
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
        maximum_cells: 64,
    }
}

/// #738: native generation holds at most what the run admits for it
/// ([`AwCatalog::generation_bytes`]), on two working grids.
#[test]
fn native_generation_holds_at_most_its_charge() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    for size in [64, 256] {
        let request = aw_request(size);
        let root = tempfile::tempdir().expect("cache directory");
        let (peak, _, generated) =
            measure(|| AwCatalog::generate_native(root.path(), &request, false));
        generated.expect("generate");
        let charge = AwCatalog::generation_bytes(&request);
        eprintln!("working grid {size}: held {peak}, charged {charge}");
        assert!(
            peak <= charge,
            "working grid {size}: generation held {peak}, charged {charge}"
        );
    }
}

#[test]
fn an_aw_catalog_holds_at_most_its_charge() {
    let _measurement = MEASUREMENT.lock().expect("measurement lock");
    let root = tempfile::tempdir().expect("cache directory");
    aw_cache(root.path());
    let geometry = geometry(64, 1.0e-4, GridPadding::None);
    let routing = PolarizationRouting::compile(
        &[CorrelationType::CircularRr, CorrelationType::CircularLl],
        &[PolarizationCoordinate::StokesI],
    )
    .expect("routing");
    let indexing = AwIndexing {
        conjugate_beams: false,
        image_reference_hz: 1.5e9,
    };
    // Open the catalog and fetch every group's imaging and weight cells
    // through one worker's hold.
    let open_and_fetch = |bound: usize| {
        let catalog =
            AwCatalog::open_casa(root.path(), indexing, &geometry, &routing, bound).expect("open");
        let mut hold = CellHold::new();
        for group in 0..catalog.groups() {
            let key = CfKey {
                group: group as u16,
                cube: group as u16,
            };
            assert!(catalog.taps(key, &mut hold).half_support()[0] > 0);
            let mut weight_hold = CellHold::new();
            assert!(catalog.weight_taps(key, &mut weight_hold).is_some());
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
