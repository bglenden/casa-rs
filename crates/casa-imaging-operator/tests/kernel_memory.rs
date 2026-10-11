// SPDX-License-Identifier: LGPL-3.0-or-later
//! Building a kernel set holds no more than the bytes it states before it
//! allocates them, and the set holds what it says it holds.
//!
//! A run admits a W-projection or mosaic set in two stages: the screens
//! (`screens_bytes`, before any is transformed), then the kernels cut from
//! them (`kernel_bytes`, once their supports are known), and keeps the
//! set's `resident_bytes` while the operator lives; a spheroidal set's
//! `bytes` before it is built. This binary's global allocator counts the
//! live heap and measures each stage against its statement. A global
//! allocator applies to this test binary alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    AiryDish, GridGeometry, GridPadding, ImageExtent, MosaicPb, MosaicWindow, PolarizationRouting,
    Spheroidal, WPlaneCount, WPlanes,
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
