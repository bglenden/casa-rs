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
fn reading_an_earlier_runs_normal_state_fits_its_charge_and_keeps_the_psf() {
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

    let input = Input::open(&prefix, None, ([SIDE, SIDE], 1, CHANNELS), true).unwrap();
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

    // The PSF stays in image units. The residual is scaled by sumwt to
    // recover the gridded units of the normal state.
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
            assert_eq!(
                images.psf.as_ref().unwrap()[index],
                psf(&at),
                "PSF at {at:?}"
            );
            let residual_value = images.residual[index] / weight as f32;
            assert!(
                (residual_value - residual(&at)).abs() <= 1e-6 * residual(&at).abs().max(1.0),
                "residual at {at:?}"
            );
        }
    }
}

/// A saved per-channel beam grid is loaded once and held through the minor
/// cycle and publication, inside the restart metadata admission.
#[test]
fn a_saved_psf_beam_grid_fits_its_reading_and_resident_charges() {
    use casa_images::{GaussianBeam, ImageBeamSet};
    const PLANES: usize = 2000;
    let root = tempfile::tempdir().unwrap();
    let prefix = root.path().join("beams");
    let shape = [2, 2, 1, PLANES];
    write(&prefix.with_extension("psf"), &shape, |_| 1.0);
    write(&prefix.with_extension("sumwt"), &[1, 1, 1, PLANES], |_| 1.0);
    let mut psf = PagedImage::<f32>::open(prefix.with_extension("psf")).unwrap();
    let mut info = psf.image_info().unwrap();
    info.beam_set = ImageBeamSet::from_grid(
        (0..PLANES)
            .map(|i| {
                vec![GaussianBeam {
                    major: 1e-5 + i as f64 * 1e-10,
                    minor: 5e-6,
                    position_angle: 0.1,
                }]
            })
            .collect(),
    );
    psf.set_image_info(&info).unwrap();
    psf.save().unwrap();
    drop((psf, info));
    let mut input = Input::open(&prefix, None, ([2, 2], 1, PLANES), false).unwrap();
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    input.read_psf_beams(PLANES).unwrap();
    MEASURING.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let admitted = Input::beam_reading_bytes(PLANES, 1);
    let retained = input.beam_resident_bytes();
    eprintln!("beam metadata: peak {peak}/{admitted}, retained {held}/{retained}");
    assert!(peak <= admitted, "peak {peak}, admission {admitted}");
    assert!(held <= retained, "held {held}, retained {retained}");
    assert!(retained > 0, "the saved beams were retained");
    let (world, pixels) = input
        .beam_pair(PLANES - 1)
        .expect("saved angular and pixel beams");
    assert_eq!(pixels.major_fwhm_rad(), world.major_fwhm_rad() / 1e-5);
    assert_eq!(pixels.minor_fwhm_rad(), world.minor_fwhm_rad() / 1e-5);
    assert_eq!(pixels.position_angle_rad(), world.position_angle_rad());
}

/// A residual beam grid has its own admitted read and retained allocation.
#[test]
fn a_saved_residual_beam_grid_fits_its_reading_and_resident_charges() {
    use casa_images::{GaussianBeam, ImageBeamSet};
    const PLANES: usize = 2000;
    let root = tempfile::tempdir().unwrap();
    let prefix = root.path().join("beams");
    let shape = [2, 2, 1, PLANES];
    write(&prefix.with_extension("psf"), &shape, |_| 1.0);
    write(&prefix.with_extension("residual"), &shape, |_| 1.0);
    write(&prefix.with_extension("sumwt"), &[1, 1, 1, PLANES], |_| 1.0);
    let mut image = PagedImage::<f32>::open(prefix.with_extension("residual")).unwrap();
    let mut info = image.image_info().unwrap();
    info.beam_set = ImageBeamSet::from_grid(
        (0..PLANES)
            .map(|i| {
                vec![GaussianBeam {
                    major: 1e-5 + i as f64 * 1e-10,
                    minor: 5e-6,
                    position_angle: 0.1,
                }]
            })
            .collect(),
    );
    image.set_image_info(&info).unwrap();
    image.save().unwrap();
    drop((image, info));
    let mut input = Input::open(&prefix, None, ([2, 2], 1, PLANES), true).unwrap();
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    input.read_mask_beams(PLANES).unwrap();
    MEASURING.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let admitted = Input::beam_reading_bytes(PLANES, 1);
    let retained = input.beam_resident_bytes();
    eprintln!("residual beam metadata: peak {peak}/{admitted}, retained {held}/{retained}");
    assert!(peak <= admitted, "peak {peak}, admission {admitted}");
    assert!(held <= retained, "held {held}, retained {retained}");
    let beam = input.mask_beam(PLANES - 1).expect("saved residual beam");
    assert!((beam.major_fwhm_rad() - (1e-5 + (PLANES - 1) as f64 * 1e-10) / 1e-5).abs() < 1e-12);
    assert_eq!(beam.minor_fwhm_rad(), 0.5);
    assert!((beam.position_angle_rad() - 0.1).abs() < 1e-12);
}
