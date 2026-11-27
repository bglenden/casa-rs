// SPDX-License-Identifier: LGPL-3.0-or-later

//! Counting-law test for the restart mask reader and retained plan clone.

use std::sync::atomic::Ordering;

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};
use casa_images::PagedImage;
use casa_imaging_deconvolution::MaskPlanes;
use ndarray::{ArrayD, IxDyn};

use super::Input;
use crate::counting_heap::{BASELINE, LIVE, MEASURING, PEAK, alone};

#[test]
fn restart_mask_read_and_retained_plans_fit_their_charges() {
    if !alone(
        "restart_mask_law::memory_tests::restart_mask_read_and_retained_plans_fit_their_charges",
    ) {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("existing.mask");
    let side = 512;
    let shape = vec![side, side, 1, 1];
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [1.0, 0.5],
        [-1e-5, 1e-5],
        [256.0, 256.0],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        1e9,
        1e6,
        0.0,
        1e9,
    ));
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, &path).unwrap();
    let pixels = ArrayD::from_shape_fn(
        IxDyn(&shape),
        |index| if index[0] < side / 2 { 1.0 } else { 0.0 },
    );
    image.put_slice(&pixels, &[0; 4]).unwrap();
    image.save().unwrap();
    drop((image, pixels));
    let input = Input::open(&path).unwrap();
    let reading = input.reading_bytes();
    let retained = Input::retained_bytes(MaskPlanes::SHARED, side * side);
    PEAK.store(0, Ordering::Relaxed);
    let baseline = LIVE.load(Ordering::Relaxed);
    BASELINE.store(baseline, Ordering::Relaxed);
    MEASURING.store(true, Ordering::Relaxed);
    let (planes, support) = input.read().unwrap();
    let clone = support.clone();
    MEASURING.store(false, Ordering::Relaxed);
    let peak = PEAK.load(Ordering::Relaxed);
    let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
    assert!(peak <= reading, "mask read held {peak}, admitted {reading}");
    assert!(
        held <= retained,
        "mask plans held {held}, charged {retained}"
    );
    assert_eq!(planes, MaskPlanes::SHARED);
    assert_eq!(support, clone);
    assert_eq!(support.len(), side * side);
    for x in 0..side {
        for y in 0..side {
            assert_eq!(support[x * side + y], x < side / 2);
        }
    }

    let channels = 4;
    for differs in [false, true] {
        let path = root.path().join(format!("cube-{differs}.mask"));
        let shape = vec![side, side, 1, channels];
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            DirectionRef::J2000,
            Projection::new(ProjectionType::SIN),
            [1.0, 0.5],
            [-1e-5, 1e-5],
            [256.0, 256.0],
        ));
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        coordinates.add_coordinate(SpectralCoordinate::new(
            FrequencyRef::LSRK,
            1e9,
            1e6,
            0.0,
            1e9,
        ));
        let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, &path).unwrap();
        let pixels = ArrayD::from_shape_fn(IxDyn(&shape), |index| {
            let edge = if differs && index[3] == channels - 1 {
                side / 4
            } else {
                side / 2
            };
            if index[1] < edge { 1.0 } else { 0.0 }
        });
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.save().unwrap();
        drop((image, pixels));
        let input = Input::open(&path).unwrap();
        let reading = input.reading_bytes();
        PEAK.store(0, Ordering::Relaxed);
        let baseline = LIVE.load(Ordering::Relaxed);
        BASELINE.store(baseline, Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let (planes, support) = input.read().unwrap();
        let clone = support.clone();
        MEASURING.store(false, Ordering::Relaxed);
        let peak = PEAK.load(Ordering::Relaxed);
        let held = LIVE.load(Ordering::Relaxed).saturating_sub(baseline);
        let retained = Input::retained_bytes(planes, side * side);
        assert!(
            peak <= reading,
            "cube mask read held {peak}, admitted {reading}"
        );
        assert!(
            held <= retained,
            "cube mask plans held {held}, charged {retained}"
        );
        assert_eq!(support, clone);
        let expected = if differs {
            MaskPlanes::new(channels, 1).unwrap()
        } else {
            MaskPlanes::SHARED
        };
        assert_eq!(planes, expected);
        assert_eq!(support.len(), planes.count() * side * side);
        for (plane, support) in support.chunks(side * side).enumerate() {
            let edge = if differs && plane == channels - 1 {
                side / 4
            } else {
                side / 2
            };
            for x in 0..side {
                for y in 0..side {
                    assert_eq!(support[x * side + y], y < edge);
                }
            }
        }
    }
}
