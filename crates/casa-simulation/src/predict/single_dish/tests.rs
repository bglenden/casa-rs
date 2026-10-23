// SPDX-License-Identifier: LGPL-3.0-or-later
//! The expected values are CASA 6.7.6's: `sm.setoptions(ftmachine='sd')`
//! then `predict` of the same 256-pixel, 1″ models, observed by one ALMA
//! 12 m antenna at 230 GHz (probed 2026-10-23, simulation Phase 2, #841).

use std::path::Path;

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{ArrayD, IxDyn};

use super::*;
use crate::predict::PrimaryBeam;

const PIXELS: usize = 256;
const CELL_RAD: f64 = std::f64::consts::PI / 180.0 / 3600.0;
const RA_DEG: f64 = 150.0;
const DEC_DEG: f64 = -30.0;

/// A model of `PIXELS` 1″ pixels at 230 GHz with `points` (pixel offsets
/// from the centre and flux) in planes of 7.8125 MHz.
fn model(dir: &Path, name: &str, points: &[(isize, isize, f32, usize)], planes: usize) -> SkyModel {
    model_of(dir, name, (points, planes), 1.0)
}

/// [`model`] with pixels of `cell_arcsec`.
fn model_of(
    dir: &Path,
    name: &str,
    (points, planes): (&[(isize, isize, f32, usize)], usize),
    cell_arcsec: f64,
) -> SkyModel {
    let cell_rad = CELL_RAD * cell_arcsec;
    let path = dir.join(name);
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        [RA_DEG.to_radians(), DEC_DEG.to_radians()],
        [-cell_rad, cell_rad],
        [(PIXELS / 2) as f64, (PIXELS / 2) as f64],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        230.0e9,
        7.8125e6,
        0.0,
        230.0e9,
    ));
    let shape = vec![PIXELS, PIXELS, 1, planes];
    let mut image = PagedImage::<f32>::create(shape.clone(), coordinates, &path).expect("create");
    let mut pixels = ArrayD::zeros(IxDyn(&shape));
    for &(dx, dy, flux, plane) in points {
        let (x, y) = (
            (PIXELS as isize / 2 + dx) as usize,
            (PIXELS as isize / 2 + dy) as usize,
        );
        pixels[IxDyn(&[x, y, 0, plane])] = flux;
    }
    image.put_slice(&pixels, &[0, 0, 0, 0]).expect("pixels");
    image.set_units("Jy/pixel").expect("units");
    image.save().expect("save");
    SkyModel::open(&path).expect("open")
}

/// The J2000 direction `(east, north)` arcsec from the model's centre, as
/// the probe set its fields.
fn offset(east_arcsec: f64, north_arcsec: f64) -> [f64; 2] {
    [
        (RA_DEG + east_arcsec / 3600.0 / DEC_DEG.to_radians().cos()).to_radians(),
        (DEC_DEG + north_arcsec / 3600.0).to_radians(),
    ]
}

fn window(channels: usize, correlations: Vec<i32>) -> SingleDishWindow {
    SingleDishWindow {
        frequencies_hz: (0..channels)
            .map(|c| 230.0e9 + c as f64 * 7.8125e6)
            .collect(),
        correlations,
    }
}

/// The XX sample of each pointing in `offsets`.
fn sampled(
    model: &SkyModel,
    kernel: SingleDishKernel,
    voltage_pattern: bool,
    offsets: &[(f64, f64)],
) -> Vec<f32> {
    let pattern = PrimaryBeam::ALMA.pattern();
    // One scan at each pointing.
    let pointings = SingleDishPointings::of_scans(
        offsets
            .iter()
            .map(|&(east, north)| offset(east, north))
            .collect::<Vec<_>>()
            .into_iter(),
    );
    let windows = [window(1, vec![9, 12])];
    let prediction = SingleDishPrediction::new(
        model,
        kernel,
        (Some(&pattern), voltage_pattern.then_some(&pattern)),
        (&windows, pointings),
        Reservation::none(),
    )
    .expect("prepared");
    (0..offsets.len())
        .map(|scan| {
            let samples = prediction.samples(scan, 0);
            assert_eq!(samples[0], samples[1], "XX and YY carry Stokes I");
            samples[0].re
        })
        .collect()
}

fn assert_close(actual: &[f32], casa: &[f32], tolerance: f32) {
    for (index, (actual, casa)) in actual.iter().zip(casa).enumerate() {
        assert!(
            (actual - casa).abs() <= tolerance * casa.abs().max(1.0),
            "pointing {index}: {actual} against CASA's {casa}"
        );
    }
}

#[test]
fn a_pb_kernel_weights_the_model_by_the_power_pattern_at_the_pointing() {
    let dir = tempfile::tempdir().expect("dir");
    let point = model(dir.path(), "point.image", &[(0, 0, 1.0, 0)], 1);
    let offsets = [
        (0.0, 0.0),
        (5.0, 0.0),
        (10.0, 0.0),
        (20.0, 0.0),
        (40.0, 0.0),
        (0.0, 13.5),
    ];
    let actual = sampled(&point, SingleDishKernel::Pb, false, &offsets);
    assert_close(
        &actual,
        &[
            1.0,
            0.914_690_43,
            0.674_355_15,
            0.159_741_49,
            0.0,
            0.472_352_56,
        ],
        1.0e-6,
    );
    // The beam applied first as well, as CASA's SkyEquation applies it.
    let twice = sampled(&point, SingleDishKernel::Pb, true, &offsets[..3]);
    assert_close(&twice, &[1.0, 0.836_658_6, 0.454_755], 1.0e-5);
}

/// The PB kernel's support comes from every sample counted before the cut,
/// though the last is dropped: with 28″ pixels the count is 100, a support
/// of one pixel, and a point one pixel from a pointing 0.4 pixel off centre
/// contributes. CASA 6.7.6 predicts 0.29592791 Jy (#874 review); a support
/// taken from the 99 samples kept predicted nothing.
#[test]
fn the_pb_support_counts_the_sample_casa_drops() {
    let dir = tempfile::tempdir().expect("dir");
    let point = model_of(dir.path(), "coarse.image", (&[(1, 0, 1.0, 0)], 1), 28.0);
    // +x is west: the pointing is 0.4 pixel towards the point.
    let actual = sampled(&point, SingleDishKernel::Pb, false, &[(-0.4 * 28.0, 0.0)]);
    assert_close(&actual, &[0.295_927_9], 1.0e-6);
}

#[test]
fn a_flat_model_samples_the_kernels_sum() {
    let dir = tempfile::tempdir().expect("dir");
    let points: Vec<_> = (-100..100)
        .flat_map(|dx| (-100..100).map(move |dy| (dx, dy, 1.0, 0)))
        .collect();
    let flat = model(dir.path(), "flat.image", &points, 1);
    let actual = sampled(
        &flat,
        SingleDishKernel::Pb,
        false,
        &[(0.0, 0.0), (20.0, 0.0)],
    );
    // CASA's 679.77099609, in single precision.
    assert_close(&actual, &[679.771, 679.771], 1.0e-6);
}

#[test]
fn box_and_spheroidal_kernels_sample_the_nearest_pixels() {
    let dir = tempfile::tempdir().expect("dir");
    let point = model(dir.path(), "point.image", &[(0, 0, 1.0, 0)], 1);
    let boxed = sampled(
        &point,
        SingleDishKernel::Box,
        false,
        &[(0.4, 0.0), (0.6, 0.0), (0.5, 0.5)],
    );
    assert_close(&boxed, &[1.0, 0.0, 0.0], 0.0);
    let spheroidal = sampled(
        &point,
        SingleDishKernel::Spheroidal,
        false,
        &[(0.4, 0.0), (1.5, 0.0), (2.5, 0.0)],
    );
    assert_close(&spheroidal, &[0.916_892, 0.270_799, 0.012_011], 2.0e-6);
}

#[test]
fn a_pointing_off_the_image_samples_nothing() {
    let dir = tempfile::tempdir().expect("dir");
    let point = model(dir.path(), "point.image", &[(0, 0, 1.0, 0)], 1);
    let actual = sampled(
        &point,
        SingleDishKernel::Pb,
        false,
        &[(200.0, 0.0), (0.0, 127.6), (0.0, 128.6)],
    );
    assert_close(&actual, &[0.0, 0.0, 0.0], 0.0);
}

#[test]
fn channels_take_their_planes_and_cross_hands_take_none() {
    let dir = tempfile::tempdir().expect("dir");
    let cube = model(
        dir.path(),
        "cube.image",
        &[(0, 0, 1.0, 0), (0, 0, 2.0, 1), (0, 0, 3.0, 2)],
        3,
    );
    let pattern = PrimaryBeam::ALMA.pattern();
    let windows = [window(4, vec![9, 10, 11, 12])];
    // Two scans at one pointing share its samples.
    let pointings = SingleDishPointings::of_scans([offset(0.0, 0.0), offset(0.0, 0.0)].into_iter());
    assert_eq!(pointings.directions_rad.len(), 1);
    let prediction = SingleDishPrediction::new(
        &cube,
        SingleDishKernel::Pb,
        (Some(&pattern), None),
        (&windows, pointings),
        Reservation::none(),
    )
    .expect("prepared");
    let reals: Vec<f32> = prediction
        .samples(0, 0)
        .iter()
        .map(|value| value.re)
        .collect();
    // The fourth channel is off the model's axis (`matchChannel`).
    assert_eq!(
        reals,
        [
            1.0, 0.0, 0.0, 1.0, 2.0, 0.0, 0.0, 2.0, 3.0, 0.0, 0.0, 3.0, 0.0, 0.0, 0.0, 0.0
        ]
    );
    assert_eq!(prediction.samples(1, 0), prediction.samples(0, 0));
    assert!(
        prediction.samples(2, 0).is_empty(),
        "an unprepared scan has none"
    );
}

#[test]
fn grid_functions_name_casas_kernels_in_any_case() {
    assert_eq!(
        SingleDishKernel::from_grid_function("PB"),
        Some(SingleDishKernel::Pb)
    );
    assert_eq!(
        SingleDishKernel::from_grid_function("box"),
        Some(SingleDishKernel::Box)
    );
    assert_eq!(
        SingleDishKernel::from_grid_function("Sf"),
        Some(SingleDishKernel::Spheroidal)
    );
    for other in ["gauss", "GJINC", "", "kb"] {
        assert_eq!(SingleDishKernel::from_grid_function(other), None, "{other}");
    }
}
