// SPDX-License-Identifier: LGPL-3.0-or-later
//! Image-mask reads and their retained support fit the admission returned
//! by [`Input::bytes`]. The reader holds one source plane and the output
//! support. This test binary counts the live heap with its global allocator.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};
use casa_images::PagedImage;
use casa_imaging_application::{
    RunContext,
    mask_image_law::{ChannelMaskBox, Input, read_list},
};
use casa_imaging_deconvolution::{MaskPlanes, ReconstructionMaskPlan};
use casa_imaging_model::{
    DirectionCoordinateSpec, DirectionFrame, Projection as ImagingProjection, SkyDirection,
};
use casa_imaging_runtime::{HostResources, ReservationWindow, ResourcePolicy};
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
fn measure(
    path: &Path,
    channels: usize,
    polarizations: usize,
    target: Option<DirectionCoordinateSpec>,
    regions: [&[ChannelMaskBox]; 2],
) -> (ReconstructionMaskPlan, String, u64, u64) {
    let mut input = Input::open(path, channels, polarizations).unwrap();
    if let Some(target) = target {
        input = input.with_target_direction(target);
    }
    let (reading, retained) = input.bytes(regions);
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let plan = input.read(regions).unwrap();
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
    (plan, breaches, reading, peak)
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
    let (plan, breach, _, _) = measure(&cube, channels, 4, None, [&[], &[]]);
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
    let (plan, breach, aligned, _) = measure(&continuum, 8, 4, None, [&[], &[]]);
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
    // A different projection keeps one map with two coordinates per
    // pixel. All four planes share it, and the plan only retains support.
    let target = DirectionCoordinateSpec::new(
        ImagingProjection::Tan,
        SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
        [SIDE as f64 / 2.0; 2],
        [-1e-5, 1e-5],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 90.0],
    );
    let (plan, breach, mapped, mapped_peak) = measure(&continuum, 8, 4, Some(target), [&[], &[]]);
    breaches += &breach;
    assert_eq!(
        mapped - aligned,
        (SIDE * SIDE * size_of::<[f64; 2]>()) as u64
    );
    assert!(matches!(plan, ReconstructionMaskPlan::Reprojected { .. }));
    let left = SIDE as i64 - 2;
    let boxes = [
        &[([left, 2, left, 2], 0..=7)][..],
        &[([left, 4, left, 4], 0..=7)][..],
    ];
    let (plan, breach, mixed, mixed_peak) = measure(&continuum, 8, 4, Some(target), boxes);
    breaches += &breach;
    assert_eq!(mixed, mapped);
    assert!(
        mixed_peak <= mapped_peak,
        "mixed mask heap peaked at {mixed_peak} bytes, compared with {mapped_peak} for the image"
    );
    let ReconstructionMaskPlan::Reprojected {
        planes, support, ..
    } = plan
    else {
        panic!("a mask image is reprojected");
    };
    assert_eq!(planes, MaskPlanes::new(1, 4).unwrap());
    assert_eq!(support.len(), planes.count() * SIDE * SIDE);
    for plane in support.as_chunks::<{ SIDE * SIDE }>().0 {
        assert!(plane[(SIDE - 2) * SIDE + 2]);
        assert!(plane[(SIDE - 2) * SIDE + 4]);
    }
    assert!(breaches.is_empty(), "{breaches}");
    let coordinate = target.coordinate();
    // Many vertices on the same four edges make a deep clone visible
    // to the allocator without changing the expected support.
    let corners = [[8.25, 6.25], [94.25, 6.25], [94.25, 32.25], [8.25, 32.25]];
    let vertices = corners
        .iter()
        .zip(corners.iter().cycle().skip(1))
        .flat_map(|(&a, &b)| {
            (0..1024).map(move |index| {
                let fraction = f64::from(index) / 1024.0;
                [
                    a[0] + fraction * (b[0] - a[0]),
                    a[1] + fraction * (b[1] - a[1]),
                ]
            })
        })
        .map(|pixel| coordinate.pixel_to_world(pixel).unwrap())
        .collect();
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let (support, read, retained) =
        casa_imaging_application::mask_image_law::read_polygon(vertices, target, [SIDE; 2])
            .unwrap();
    MEASURING.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    assert!(
        peak <= read,
        "polygon read peaked at {peak} bytes, admitted {read}"
    );
    assert!(
        held <= retained,
        "polygon held {held} bytes, kept {retained}"
    );
    assert_eq!(support.iter().filter(|inside| **inside).count(), 86 * 26);
    check_mask_list(root.path());
}

fn check_mask_list(root: &Path) {
    let a = root.join("list-a.mask");
    let b = root.join("list-b.mask");
    write_mask(&a, 1, 1, |index| index[0] < 32);
    write_mask(&b, 1, 1, |index| (16..48).contains(&index[0]));
    let coordinate = DirectionCoordinateSpec::new(
        ImagingProjection::Sin,
        SkyDirection::new(DirectionFrame::J2000, 1.0, 0.5),
        [SIDE as f64 / 2.0; 2],
        [-1e-5, 1e-5],
        [[1.0, 0.0], [0.0, 1.0]],
        [180.0, 90.0],
    );
    let context = RunContext {
        host: HostResources::detect().unwrap(),
        policy: ResourcePolicy::Explicit {
            workers: 1,
            memory: 16 << 20,
        },
        cancel: Default::default(),
        summary: None,
    };
    let window = ReservationWindow::open();
    let measure = |paths: &[PathBuf], columns: usize| {
        window.close();
        PEAK.store(0, Ordering::Relaxed);
        let baseline = LIVE.load(Ordering::Relaxed);
        BASELINE.store(baseline, Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let (plan, charge) = read_list(paths, coordinate, [SIDE; 2], &context).unwrap();
        MEASURING.store(false, Ordering::Relaxed);
        let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let peak = PEAK.load(Ordering::Relaxed);
        let admitted = window.close();
        println!(
            "{} image masks: {peak} bytes peak / {admitted} admitted, {held} / {} kept",
            paths.len(),
            charge.memory()
        );
        assert!(peak <= admitted, "list peak {peak}, admitted {admitted}");
        assert!(
            held <= charge.memory(),
            "list held {held}, kept {}",
            charge.memory()
        );
        let ReconstructionMaskPlan::Reprojected {
            planes, support, ..
        } = plan
        else {
            panic!("expected image-mask support");
        };
        assert_eq!(planes, MaskPlanes::SHARED);
        assert_eq!(
            support.iter().filter(|inside| **inside).count(),
            columns * (SIDE - 1)
        );
        peak
    };
    check_ranged_mask(&a);
    let single = measure(std::slice::from_ref(&a), 32);
    let list = measure(&[a.clone(), b, a], 48);
    assert!(
        list <= single + 4096,
        "ordered mask list peaked at {list} bytes, single image {single}"
    );
}

fn check_ranged_mask(path: &Path) {
    let input = Input::open(path, 8, 1).unwrap();
    let shared = input.bytes([&[], &[]]);
    let all = [([32, 0, 47, 30], 0..=7)];
    assert_eq!(
        input.bytes([&[], &all]),
        shared,
        "a full range keeps shared support"
    );
    let range = [([32, 0, 47, 30], 2..=3)];
    let regions = [&[][..], &range[..]];
    let distinct = input.bytes(regions);
    let expanded_cells = (7 * SIDE * SIDE) as u64;
    assert_eq!(distinct.1 - shared.1, expanded_cells);
    assert_eq!(distinct.0 - shared.0, 2 * expanded_cells);
    drop(input);
    let (plan, breach, charged, peak) = measure(path, 8, 1, None, regions);
    assert!(breach.is_empty(), "ranged mask {breach}");
    assert_eq!(charged, distinct.0);
    println!(
        "ranged mask: {peak} bytes peak / {charged} admitted, {} kept",
        distinct.1
    );
    let ReconstructionMaskPlan::Reprojected {
        planes, support, ..
    } = plan
    else {
        panic!("expected ranged image-mask support");
    };
    assert_eq!(planes, MaskPlanes::new(8, 1).unwrap());
    for (channel, plane) in support.as_chunks::<{ SIDE * SIDE }>().0.iter().enumerate() {
        let extra = if (2..=3).contains(&channel) {
            16 * 31
        } else {
            0
        };
        assert_eq!(
            plane.iter().filter(|inside| **inside).count(),
            32 * (SIDE - 1) + extra
        );
    }
}
