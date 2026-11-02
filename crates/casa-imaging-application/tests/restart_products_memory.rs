// SPDX-License-Identifier: LGPL-3.0-or-later
//! The reader of an earlier run's PSF, sums of weights and residual
//! (`calcpsf=False`, `calcres=False`) holds at most what it is charged, and
//! returns the images a pass would form: normalized again, they are the
//! images it read.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_application::restart_products_law::Input;
use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};
use ndarray::{ArrayD, IxDyn};

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

const SIDE: usize = 256;
const CHANNELS: usize = 8;

fn coordinates() -> CoordinateSystem {
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-1e-5, 1e-5],
        [128.0, 128.0],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1e9,
        1e6,
        0.0,
        1e9,
    ));
    coordinates
}

fn write(path: &Path, shape: &[usize], value: impl Fn(&[usize]) -> f32) {
    let mut image = PagedImage::<f32>::create(shape.to_vec(), coordinates(), path).unwrap();
    let pixels = ArrayD::from_shape_fn(IxDyn(shape), |index| {
        value(&(0..shape.len()).map(|axis| index[axis]).collect::<Vec<_>>())
    });
    image.put_slice(&pixels, &vec![0; shape.len()]).unwrap();
    image.save().unwrap();
}

#[test]
fn reading_an_earlier_runs_normal_state_fits_its_charge_and_inverts_normalization() {
    let root = tempfile::tempdir().unwrap();
    let prefix = root.path().join("earlier");
    let cube = [SIDE, SIDE, 1, CHANNELS];
    let psf = |index: &[usize]| {
        let (x, y) = (index[0] as f32 - 128.0, index[1] as f32 - 128.0);
        (-(x * x + y * y) / 50.0).exp()
    };
    let residual = |index: &[usize]| (index[0] * 7 + index[1] * 3 + index[3]) as f32 / 1000.0;
    let sumwt = |index: &[usize]| 100.0 + index[3] as f32;
    write(&prefix.with_extension("psf"), &cube, psf);
    write(&prefix.with_extension("residual"), &cube, residual);
    write(&prefix.with_extension("sumwt"), &[1, 1, 1, CHANNELS], sumwt);

    let input = Input::open(&prefix, 1, ([SIDE, SIDE], 1, CHANNELS), true).unwrap();
    let window = 3;
    let (reading, holding) = (input.reading_bytes(window), input.images_bytes(window));
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let images = input.read(2..2 + window).unwrap();
    MEASURING.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    eprintln!(
        "existing products: read peak {peak}/{} B; held {held}/{holding} B",
        reading + holding
    );
    assert!(
        peak <= reading + holding,
        "read held {peak}, admitted {}",
        reading + holding
    );
    assert!(held <= holding, "images held {held}, charged {holding}");

    // A pass's images: the PSF and residual times each channel's sum of
    // weights, which normalization divides out again.
    assert_eq!(images.channels, 2..2 + window);
    let cells = SIDE * SIDE;
    for channel in 0..window {
        let absolute = 2 + channel;
        let weight = f64::from(sumwt(&[0, 0, 0, absolute]));
        assert_eq!(images.sum_weights[channel], weight);
        assert_eq!(images.published_sum_weights[channel], weight);
        let plane = channel * cells;
        for (x, y) in [(0, 0), (128, 128), (3, 250), (200, 17)] {
            let index = plane + x * SIDE + y;
            let at = [x, y, 0, absolute];
            let psf_value = images.psf.as_ref().unwrap()[index] / weight as f32;
            assert!((psf_value - psf(&at)).abs() <= 1e-6, "PSF at {at:?}");
            let residual_value = images.residual[index] / weight as f32;
            assert!(
                (residual_value - residual(&at)).abs() <= 1e-6 * residual(&at).abs().max(1.0),
                "residual at {at:?}"
            );
        }
    }
}
