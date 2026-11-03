// SPDX-License-Identifier: LGPL-3.0-or-later
//! A clean-mask image compile reads stays inside the charge it admits
//! before reading ([`Input::reading_bytes`]), and its plan inside what it
//! keeps admitted ([`Input::retained_bytes`]): the image is read one plane
//! at a time, not whole, so a cube mask holds one source plane while it is
//! read, beside the supports. This binary's global allocator counts the
//! live heap; a global allocator applies to this test binary alone.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_application::mask_image_law::Input;
use casa_imaging_deconvolution::{MaskPlanes, ReconstructionMaskPlan};
use casa_types::measures::{direction::DirectionRef, frequency::FrequencyRef};
use ndarray::{ArrayD, Dimension, IxDyn};

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
        // SAFETY: the caller's contract for `alloc`.
        let pointer = unsafe { System.alloc(layout) };
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

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        // SAFETY: the caller's contract for `realloc`.
        let moved = unsafe { System.realloc(pointer, layout, size) };
        if !moved.is_null() {
            // Old and new bytes were both held while the block moved.
            grow(size as u64);
            LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
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

/// Pixels per side of the masks.
const SIDE: usize = 256;

/// A mask image at `path` of `stokes` Stokes planes and `channels`
/// channels, set where `set([x, y, stokes, channel])`, with a default pixel
/// mask that leaves out its last row.
fn write_mask(path: &Path, stokes: usize, channels: usize, set: impl Fn(&[usize]) -> bool) {
    let shape = vec![SIDE, SIDE, stokes, channels];
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-1e-5, 1e-5],
        [SIDE as f64 / 2.0; 2],
    ));
    let types = [StokesType::I, StokesType::Q, StokesType::U, StokesType::V];
    coordinates.add_coordinate(StokesCoordinate::new(types[..stokes].to_vec()));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1e9,
        1e6,
        0.0,
        1e9,
    ));
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, path).unwrap();
    let pixels = ArrayD::from_shape_fn(
        IxDyn(&shape),
        |index| {
            if set(index.slice()) { 1.0 } else { 0.0 }
        },
    );
    let valid = ArrayD::from_shape_fn(IxDyn(&shape), |index| index[1] < SIDE - 1);
    image.put_slice(&pixels, &[0; 4]).unwrap();
    image.put_mask("mask0", &valid).unwrap();
    image.set_default_mask("mask0").unwrap();
    image.save().unwrap();
}

/// Read the mask at `path` onto an image of `channels` channels and
/// `polarizations` Stokes planes, measuring the read and what it keeps.
fn measure(path: &Path, channels: usize, polarizations: usize) -> (ReconstructionMaskPlan, String) {
    let input = Input::open(path, channels, polarizations).unwrap();
    let reading = input.reading_bytes();
    let retained = input.retained_bytes();
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let plan = input.read().unwrap();
    MEASURING.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    let mut breaches = String::new();
    if peak > reading {
        breaches += &format!("{}: read held {peak}, admitted {reading}; ", path.display());
    }
    if held > retained {
        breaches += &format!("{}: plan held {held}, kept {retained}", path.display());
    }
    (plan, breaches)
}

/// A cube Stokes I mask of 32 channels read onto a full-polarization cube
/// of its channels (`SDMaskHandler::expandMask`), and a continuum mask of
/// four Stokes planes onto a full-polarization cube, as tclean expands
/// them: each plane read alone, the plan's support its planes'.
#[test]
fn a_mask_image_is_read_plane_by_plane_within_its_charge() {
    let root = tempfile::tempdir().unwrap();
    let channels = 32;
    let cube = root.path().join("cube-stokes-i.mask");
    // A square of a side that grows with the channel.
    write_mask(&cube, 1, channels, |index| {
        index[0] < 16 + 4 * index[3] && index[1] < 64
    });
    let continuum = root.path().join("continuum-iquv.mask");
    write_mask(&continuum, 4, 1, |index| index[0] < 32 * (index[2] + 1));
    let mut breaches = String::new();
    let (plan, breach) = measure(&cube, channels, 4);
    breaches += &breach;
    let ReconstructionMaskPlan::Reprojected {
        planes, support, ..
    } = plan
    else {
        panic!("a mask image is reprojected");
    };
    assert_eq!(planes, MaskPlanes::new(channels, 1).unwrap());
    for channel in [0, 7, channels - 1] {
        let plane = &support[channel * SIDE * SIDE..(channel + 1) * SIDE * SIDE];
        let count = plane.iter().filter(|set| **set).count();
        assert_eq!(count, (16 + 4 * channel) * 64, "channel {channel}");
    }
    let (plan, breach) = measure(&continuum, 8, 4);
    breaches += &breach;
    let ReconstructionMaskPlan::Reprojected {
        planes, support, ..
    } = plan
    else {
        panic!("a mask image is reprojected");
    };
    assert_eq!(planes, MaskPlanes::new(1, 4).unwrap());
    for stokes in 0..4 {
        let plane = &support[stokes * SIDE * SIDE..(stokes + 1) * SIDE * SIDE];
        // The last row is invalid in the default pixel mask.
        let count = plane.iter().filter(|set| **set).count();
        assert_eq!(
            count,
            32 * (stokes + 1) * (SIDE - 1),
            "Stokes plane {stokes}"
        );
    }
    assert!(breaches.is_empty(), "{breaches}");
}
