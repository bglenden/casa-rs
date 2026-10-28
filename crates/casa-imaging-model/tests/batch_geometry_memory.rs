// SPDX-License-Identifier: LGPL-3.0-or-later
//! The counting law of batch geometry's parallactic angle cache: what a
//! plan keeps across batches stays within the bytes it reports
//! ([`GeometryPlan::cached_bytes`]), which stay within the bound charged
//! before any batch is read ([`GeometryPlan::cache_bound`]), and another
//! partition over the same times adds nothing. The per-batch buffers stay
//! within [`GeometryPlan::geometry_bytes`]. This binary's global allocator counts the live
//! heap; it holds this one law, so the law runs in a process of its own.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

use casa_imaging_model::{
    BatchGeometry, FacetLayout, GeometryPlan, GeometryTraversal, PhaseCentreLaw,
    PolarizationCoordinate, PsfPhaseCentreLaw,
};
use casa_observation::BatchBuffers;

#[path = "fixtures/batch_observation.rs"]
mod batch_observation;
mod common;
#[path = "fixtures/model_lifecycle.rs"]
mod model_lifecycle_fixture;

use batch_observation::{catalogue, fill, j2000, measures, problem_in};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes.
struct CountingHeap;

static LIVE: AtomicI64 = AtomicI64::new(0);

// SAFETY: every method forwards to `System` with the caller's arguments and
// only adds atomic bookkeeping, which allocates nothing.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller's contract for `alloc_zeroed`.
        let pointer = unsafe { System.alloc_zeroed(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: the caller's contract for `dealloc`.
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            LIVE.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        moved
    }
}

fn live() -> i64 {
    LIVE.load(Ordering::Relaxed)
}

#[test]
fn the_parallactic_cache_holds_at_most_what_it_charges() {
    const TIMES: usize = 200;
    let problem = problem_in(
        PhaseCentreLaw::Observation,
        FacetLayout::Single,
        PsfPhaseCentreLaw::ImageDomainReference,
        vec![
            PolarizationCoordinate::StokesI,
            PolarizationCoordinate::StokesQ,
            PolarizationCoordinate::StokesU,
            PolarizationCoordinate::StokesV,
        ],
    );
    assert!(problem.requires_parallactic_angles());
    let catalogue = catalogue(j2000(0.9, -0.45), 2);
    let mut buffers = [BatchBuffers::default(), BatchBuffers::default()];
    fill(&mut buffers[0], &catalogue, 0, TIMES);
    fill(&mut buffers[1], &catalogue, 1, TIMES);
    let batches = [&buffers[0], &buffers[1]].map(|buffers| buffers.batch().expect("a batch"));
    let rows = vec![true; batches[0].samples()];
    let none = vec![false; batches[0].samples()];
    let mut traversal = GeometryTraversal::default();
    let mut geometry = BatchGeometry::default();
    // A plan of its own computes the same angles first, so that state
    // Measures keeps after its first conversions is not counted.
    GeometryPlan::compile(&problem, &catalogue, measures())
        .expect("a geometry plan")
        .apply(&batches[0], &rows, None, &mut traversal, &mut geometry)
        .expect("batch geometry");
    let plan = GeometryPlan::compile(&problem, &catalogue, measures()).expect("a geometry plan");
    plan.apply(&batches[0], &none, None, &mut traversal, &mut geometry)
        .expect("batch geometry");
    assert_eq!(
        plan.cached_bytes(),
        0,
        "a batch without rows computes no angle"
    );
    let before = live();
    plan.apply(&batches[0], &rows, None, &mut traversal, &mut geometry)
        .expect("batch geometry");
    let held = live() - before;
    let charged = plan.cached_bytes();
    assert!(
        held > 0 && held.unsigned_abs() as usize <= charged,
        "the cache holds {held} B against {charged} B charged"
    );
    plan.apply(&batches[1], &rows, None, &mut traversal, &mut geometry)
        .expect("batch geometry");
    assert_eq!(
        plan.cached_bytes(),
        charged,
        "the second partition reuses every angle"
    );
    assert!(
        (live() - before).unsigned_abs() as usize <= charged,
        "after the second partition the cache holds {} B against {charged} B charged",
        live() - before
    );
    let bound = plan.cache_bound(TIMES);
    assert!(
        charged <= bound,
        "the cache reports {charged} B over its a-priori bound {bound} B"
    );
    let batch = &batches[0];
    let buffers = plan.geometry_bytes(batch.samples(), batch.times(), batch.channel_count());
    assert!(
        geometry.capacity_bytes() <= buffers,
        "the geometry buffers hold {} B over the {buffers} B their batch charges",
        geometry.capacity_bytes()
    );
}
