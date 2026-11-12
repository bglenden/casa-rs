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
    let sensitivity = |index: &[usize]| 0.1 + (index[0] + 2 * index[1] + index[3]) as f32 / 1000.0;
    write(&prefix.with_extension("weight"), &cube, sensitivity);
    write(
        &prefix.with_extension("pb"),
        &[SIDE, SIDE, 1, 1],
        sensitivity,
    );

    for mosaic in [false, true] {
        let input =
            Input::open(&prefix, None, ([SIDE, SIDE], 1, CHANNELS), (true, mosaic)).unwrap();
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
            "existing products, mosaic {mosaic}: read peak {peak}/{} B; held {held}/{holding} B",
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
        assert_eq!(images.weight.is_some(), mosaic);
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
                if let Some(sensitivity_image) = &images.weight {
                    let actual = sensitivity_image[index] / weight as f32;
                    assert!(
                        (actual - sensitivity(&at)).abs() <= 1e-6,
                        "weight at {at:?}"
                    );
                }
            }
        }
    }
}

/// The saved mosaic PB and mask are read one channel and Stokes plane at
/// a time, with an x-major plane and a bound independent of cube depth.
#[test]
fn a_saved_mosaic_primary_beam_and_mask_fit_their_charge() {
    let root = tempfile::tempdir().unwrap();
    let prefix = root.path().join("earlier");
    let shape = [SIDE, SIDE, 2, 7];
    let pb_path = prefix.with_extension("pb");
    let mut image = PagedImage::<f32>::create_with_tile_shape(
        shape.to_vec(),
        vec![32, 32, 1, 1],
        coordinates(),
        &pb_path,
    )
    .unwrap();
    let values = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
        (index[0] + 2 * index[1] + 101 * index[2] + 251 * index[3]) as f32 / 1000.0
    });
    image.put_slice(&values, &[0; 4]).unwrap();
    let mask = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
        index[0] + 35 * index[2] < 2 * index[1] + 17 * index[3]
    });
    image.put_mask_slice("mask0", &mask, &[0; 4]).unwrap();
    image.set_default_mask("mask0").unwrap();
    image.save().unwrap();
    drop(image);
    write(&prefix.with_extension("psf"), &shape, |_| 1.0);
    write(&prefix.with_extension("weight"), &shape, |_| 1.0);
    write(&prefix.with_extension("sumwt"), &[1, 1, 2, 7], |_| 1.0);
    let input = Input::open(&prefix, None, ([SIDE, SIDE], 2, 7), (false, true)).unwrap();
    let reader = input.mosaic_beam().unwrap();
    let reading = reader.reading_bytes();
    assert!(reading < shape.iter().product::<usize>() as u64);
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let beam = reader.read_values(5, 1).unwrap();
    let values_peak = PEAK.load(Ordering::Relaxed);
    drop(beam);
    PEAK.store(0, Ordering::Relaxed);
    let valid = reader.read_mask(5, 1).unwrap();
    MEASURING.store(false, Ordering::Relaxed);
    let peak = values_peak.max(PEAK.load(Ordering::Relaxed));
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let retained = (SIDE * SIDE) as u64;
    eprintln!("saved mosaic plane and mask: read {peak}/{reading} B, held {held}/{retained} B");
    assert!(
        peak <= reading && held <= retained,
        "reading {peak}/{reading}, retained {held}/{retained}"
    );
    let beam = reader.read_values(5, 1).unwrap();
    assert_eq!(beam.len(), SIDE * SIDE);
    assert_eq!(beam[77 * SIDE + 21], 1.475);
    assert!(valid[77 * SIDE + 21] && !valid[90 * SIDE + 7]);
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
    let mut input = Input::open(&prefix, None, ([2, 2], 1, PLANES), (false, false)).unwrap();
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
    let mut input = Input::open(&prefix, None, ([2, 2], 1, PLANES), (true, false)).unwrap();
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
