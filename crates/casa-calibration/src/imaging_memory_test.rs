// SPDX-License-Identifier: LGPL-3.0-or-later
//! Isolated live-allocation tests for calibration's imaging owners.

use std::alloc::{GlobalAlloc, Layout, System};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountingHeap;

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: forward the original layout and pointer, counting only allocations
// that successfully went through the system allocator.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            let after = LIVE.fetch_add(layout.size(), Ordering::SeqCst) + layout.size();
            PEAK.fetch_max(after, Ordering::SeqCst);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::SeqCst);
        unsafe { System.dealloc(pointer, layout) };
    }
}

pub(crate) fn isolated_test(module: &str, test: &str) -> bool {
    const CHILD: &str = "CASA_RS_IMAGING_MEMORY_CHILD";
    let module = module.split_once("::").expect("test module").1;
    let name = format!("{module}::{test}");
    if std::env::var(CHILD).is_ok_and(|value| value == name) {
        return true;
    }
    let output = Command::new(std::env::current_exe().expect("test binary"))
        .args(["--exact", &name])
        .env(CHILD, &name)
        .output()
        .expect("isolated counting test");
    let receipt = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success() && receipt.contains("1 passed"),
        "{receipt}"
    );
    false
}

pub(crate) fn reset_peak() -> usize {
    let baseline = LIVE.load(Ordering::SeqCst);
    PEAK.store(baseline, Ordering::SeqCst);
    baseline
}

pub(crate) fn peak_since(baseline: usize) -> u64 {
    PEAK.load(Ordering::SeqCst).saturating_sub(baseline) as u64
}

pub(crate) fn live_since(baseline: usize) -> u64 {
    LIVE.load(Ordering::SeqCst).saturating_sub(baseline) as u64
}

pub(crate) fn cube_coordinates(
    stokes: Vec<casa_coordinates::StokesType>,
) -> casa_coordinates::CoordinateSystem {
    use casa_coordinates::{
        CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
        StokesCoordinate,
    };
    use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};

    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1., 0.4],
        [-1e-5, 1e-5],
        [64., 48.],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(stokes));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::TOPO,
        1e9,
        1e6,
        0.,
        1e9,
    ));
    coordinates
}
