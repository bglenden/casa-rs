// SPDX-License-Identifier: LGPL-3.0-or-later
//! What seeding a model from a start model holds (#841 Phase 6b) stays within
//! what the initial pass admits for it (`StartModel::seeding_bytes`), and the
//! seeded samples keep the start model's flux: a masked 256² Jy/pixel image
//! regridded onto a 1024² grid of a quarter its cell, and onto its own grid.
//!
//! This binary's global allocator counts the live heap; the law measures the
//! most seeding adds to it. A global allocator applies to this test binary
//! alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::{PagedImage, RegridTemplate};
use casa_imaging_application::start_model_law::{main_samples, seeding_bytes};
use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};

#[global_allocator]
static HEAP: CountingHeap = CountingHeap;

/// The system allocator, counting live bytes.
struct CountingHeap;

static LIVE: AtomicU64 = AtomicU64::new(0);
static BASELINE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);
static MEASURING: AtomicBool = AtomicBool::new(false);

// SAFETY: every call is forwarded to `System` unchanged; the counters only
// observe sizes.
unsafe impl GlobalAlloc for CountingHeap {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            grow(layout.size() as u64);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) };
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, size) };
        if !moved.is_null() {
            LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
            grow(size as u64);
        }
        moved
    }
}

fn grow(bytes: u64) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    if MEASURING.load(Ordering::Relaxed) {
        PEAK.fetch_max(
            live.saturating_sub(BASELINE.load(Ordering::Relaxed)),
            Ordering::Relaxed,
        );
    }
}

const ARCSEC: f64 = std::f64::consts::PI / (180.0 * 3600.0);

/// A SIN image of `size`² cells of `cell_arcsec`, one Stokes I plane at
/// 1 GHz.
fn coordinates(size: usize, cell_arcsec: f64) -> CoordinateSystem {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-cell_arcsec * ARCSEC, cell_arcsec * ARCSEC],
        [(size / 2) as f64; 2],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1.0e9,
        1.0e6,
        0.0,
        1.0e9,
    ));
    coordinates
}

#[test]
fn seeding_holds_at_most_what_it_is_admitted_and_keeps_the_flux() {
    let root = tempfile::tempdir().expect("test root");
    let path = root.path().join("start.model");
    let side = 256;
    let shape = [side, side, 1, 1];
    let mut image = PagedImage::<f32>::create(shape.to_vec(), coordinates(side, 4.0), &path)
        .expect("create the start model");
    image.set_units("Jy/pixel").expect("units");
    // A Gaussian of 20 cells' sigma at the centre, its left half masked.
    let pixels = ArrayD::from_shape_fn(IxDyn(&shape).f(), |index| {
        let [x, y] = [index[0], index[1]].map(|v| v as f64 - (side / 2) as f64);
        (-(x * x + y * y) / (2.0 * 20.0 * 20.0)).exp() as f32
    });
    image.put_slice(&pixels, &[0; 4]).expect("pixels");
    image.make_mask("mask0", true, true).expect("mask");
    let mask = ArrayD::from_shape_fn(IxDyn(&shape).f(), |index| index[0] >= side / 2);
    image.put_mask("mask0", &mask).expect("mask pixels");
    image.save().expect("save the start model");
    let unmasked: f64 = pixels
        .iter()
        .zip(&mask)
        .filter(|(_, valid)| **valid)
        .map(|(value, _)| f64::from(*value))
        .sum();
    drop((image, pixels, mask));

    for (label, size, cell) in [("regridded", 1024, 1.0), ("on its own grid", side, 4.0)] {
        let template = || RegridTemplate {
            coordinates: coordinates(size, cell),
            shape: vec![size, size, 1, 1],
        };
        let admitted = seeding_bytes(&path, template()).expect("a valid start model");
        PEAK.store(0, Ordering::Relaxed);
        BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let samples = main_samples(&path, template());
        MEASURING.store(false, Ordering::Relaxed);
        let samples = samples.expect("seeded samples");
        let peak = PEAK.load(Ordering::Relaxed);
        let flux: f64 = samples.iter().map(|value| f64::from(*value)).sum();
        eprintln!("{label}: held {peak} B, admitted {admitted} B; flux {flux} of {unmasked}");
        assert!(
            peak <= admitted,
            "{label}: held {peak} B, admitted {admitted} B"
        );
        assert!(
            (flux - unmasked).abs() < 1.0e-3 * unmasked,
            "{label}: flux {flux} against the unmasked {unmasked}"
        );
        assert_eq!(samples.len(), size * size);
    }
}
