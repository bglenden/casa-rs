// SPDX-License-Identifier: LGPL-3.0-or-later
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequency};

use super::*;
use crate::{Ellipse, Flux, SpectralIndex, SpectralKind, SpectralModel};

const ARCSEC: f64 = PI / 180.0 / 3600.0;
const CENTRE: (f64, f64) = (4.97, -0.41);

/// A square grid of `n` × `n` pixels of `cell` rad about `CENTRE`, as
/// direction cosines, with RA increasing to the left.
fn grid(n: usize, cell: f64) -> (Vec<[f64; 3]>, PixelGrid) {
    let half = (n as f64 - 1.0) / 2.0;
    let mut directions = Vec::with_capacity(n * n);
    for y in 0..n {
        for x in 0..n {
            let lat = CENTRE.1 + (y as f64 - half) * cell;
            let lon = CENTRE.0 - (x as f64 - half) * cell / CENTRE.1.cos();
            directions.push(MDirection::from_angles(lon, lat, DirectionRef::J2000).cosines());
        }
    }
    let grid = PixelGrid {
        frame: DirectionRef::J2000,
        longitude_size_rad: cell,
        latitude_size_rad: cell,
    };
    (directions, grid)
}

fn shape(kind: ShapeKind) -> ComponentShape {
    ComponentShape::new(
        MDirection::from_angles(CENTRE.0, CENTRE.1, DirectionRef::J2000),
        kind,
    )
    .expect("shape")
}

/// The direction `east` and `north` rad from `CENTRE`, small offsets.
fn offset(east: f64, north: f64) -> [f64; 3] {
    MDirection::from_angles(
        CENTRE.0 + east / CENTRE.1.cos(),
        CENTRE.1 + north,
        DirectionRef::J2000,
    )
    .cosines()
}

#[test]
fn a_point_fills_the_pixel_it_lies_within() {
    let point = shape(ShapeKind::Point);
    let (_, grid) = grid(1, ARCSEC);
    let directions = [
        offset(0.0, 0.0),
        offset(0.4 * ARCSEC, -0.4 * ARCSEC),
        offset(0.6 * ARCSEC, 0.0),
        offset(0.0, -0.6 * ARCSEC),
    ];
    let scales = point.pixel_scales(&directions, &grid).expect("scales");
    assert_eq!(scales, [1.0, 1.0, 0.0, 0.0]);
}

#[test]
fn a_gaussian_holds_its_flux_and_lies_along_its_position_angle() {
    let axes = Ellipse::new(1.2 * ARCSEC, 0.5 * ARCSEC, 90.0_f64.to_radians()).expect("axes");
    let gaussian = shape(ShapeKind::Gaussian {
        axes,
        errors: Default::default(),
    });
    let (directions, grid) = grid(121, 0.05 * ARCSEC);
    let total: f64 = gaussian
        .pixel_scales(&directions, &grid)
        .expect("scales")
        .iter()
        .sum();
    assert!((total - 1.0).abs() < 1e-3, "flux {total}");
    // At position angle 90° the major axis runs east-west.
    let probes = [offset(0.6 * ARCSEC, 0.0), offset(0.0, 0.6 * ARCSEC)];
    let scales = gaussian.pixel_scales(&probes, &grid).expect("scales");
    let pixel = grid.longitude_size_rad * grid.latitude_size_rad;
    let height = 4.0 * 2.0_f64.ln() / (PI * 1.2 * 0.5 * ARCSEC * ARCSEC);
    assert!(
        (scales[0] / pixel / height - 0.5).abs() < 1e-6,
        "half power at half the major axis"
    );
    assert!((scales[1] / pixel / height - 2.0_f64.powf(-(1.2 / 0.5_f64).powi(2))).abs() < 1e-6);
    // Nothing beyond four major axes.
    let far = gaussian
        .pixel_scales(&[offset(4.9 * ARCSEC, 0.0)], &grid)
        .expect("scales");
    assert_eq!(far, [0.0]);
}

#[test]
fn a_disk_spreads_its_flux_evenly_inside_its_ellipse() {
    let axes = Ellipse::new(1.4 * ARCSEC, 0.7 * ARCSEC, 30.0_f64.to_radians()).expect("axes");
    let disk = shape(ShapeKind::Disk {
        axes,
        errors: Default::default(),
    });
    let (directions, grid) = grid(201, 0.01 * ARCSEC);
    let total: f64 = disk
        .pixel_scales(&directions, &grid)
        .expect("scales")
        .iter()
        .sum();
    assert!((total - 1.0).abs() < 5e-3, "flux {total}");
    // Along the position angle the edge is at 0.7"; across it at 0.35".
    let (sin, cos) = 30.0_f64.to_radians().sin_cos();
    let probes = [
        offset(0.68 * ARCSEC * sin, 0.68 * ARCSEC * cos),
        offset(0.72 * ARCSEC * sin, 0.72 * ARCSEC * cos),
        offset(0.33 * ARCSEC * cos, -0.33 * ARCSEC * sin),
        offset(0.37 * ARCSEC * cos, -0.37 * ARCSEC * sin),
    ];
    let scales = disk.pixel_scales(&probes, &grid).expect("scales");
    assert!(scales[0] > 0.0 && scales[2] > 0.0, "{scales:?}");
    assert_eq!([scales[1], scales[3]], [0.0, 0.0]);
}

#[test]
fn a_galactic_component_is_sampled_at_j2000_pixels() {
    let galactic = MDirection::from_angles(CENTRE.0, CENTRE.1, DirectionRef::J2000)
        .convert_to(DirectionRef::GALACTIC, &MeasFrame::default())
        .expect("galactic");
    let point = ComponentShape::new(galactic, ShapeKind::Point).expect("shape");
    let (_, grid) = grid(1, 0.1 * ARCSEC);
    let scales = point
        .pixel_scales(&[offset(0.0, 0.0)], &grid)
        .expect("scales");
    assert_eq!(scales, [1.0]);
}

#[test]
fn what_casa_rs_does_not_sample_is_refused() {
    let axes = Ellipse::new(1.0 * ARCSEC, 1.0 * ARCSEC, 0.0).expect("axes");
    let limb = shape(ShapeKind::LimbDarkenedDisk {
        axes,
        errors: Default::default(),
    });
    let (directions, grid) = grid(3, ARCSEC);
    assert!(matches!(
        limb.pixel_scales(&directions, &grid),
        Err(ComponentError::ImageSample { .. })
    ));
    let horizon = ComponentShape::new(
        MDirection::from_angles(1.0, 0.5, DirectionRef::AZEL),
        ShapeKind::Point,
    )
    .expect("shape");
    assert!(matches!(
        horizon.pixel_scales(&directions, &grid),
        Err(ComponentError::Direction { .. })
    ));
}

#[test]
fn a_component_adds_its_stokes_flux_scaled_by_its_spectrum() {
    let component = SkyComponent::new(
        Flux::stokes_jy(2.0, 0.2, 0.1, 0.0),
        shape(ShapeKind::Point),
        SpectralModel::new(
            MFrequency::new(100.0e9, FrequencyRef::LSRK),
            SpectralKind::SpectralIndex(SpectralIndex::new(-1.0)),
        ),
    );
    let (_, grid) = grid(1, ARCSEC);
    let directions = [offset(0.0, 0.0), offset(2.0 * ARCSEC, 0.0)];
    let mut samples = Array3::zeros((4, 2, 2));
    component
        .sample_pixels(&mut samples, &directions, &grid, &[100.0e9, 200.0e9])
        .expect("sample");
    assert_eq!(samples[[0, 0, 0]], 2.0);
    assert_eq!(samples[[0, 0, 1]], 1.0);
    assert_eq!(samples[[1, 0, 0]], 0.2);
    assert_eq!(samples[[3, 0, 1]], 0.0);
    assert!(
        samples
            .slice(ndarray::s![.., 1, ..])
            .iter()
            .all(|value| *value == 0.0)
    );
}
