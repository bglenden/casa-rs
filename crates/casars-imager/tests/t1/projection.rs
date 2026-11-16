// SPDX-License-Identifier: LGPL-3.0-or-later
//! Projections other than SIN (`projection`; CASA 6.7.6.14's
//! `test_onefield_projections`). CASA's gridders sample a regular grid of
//! direction cosines whatever the projection, so a projection leaves a
//! single field's pixels alone: it labels them, in the image's direction
//! coordinate, and places directions on them.

use casa_coordinates::{CoordinateModel, CoordinateType, ProjectionType};
use serde_json::json;

use super::fixture::{Component, Observation, PHASE_CENTER_RAD};

const POINT: Component = Component {
    offset_px: [6, -4],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};

/// Each projection casa-rs images in cleans and restores the pixels the
/// default SIN does, and records itself in every product's direction
/// coordinate as CASA writes it: its name with the parameters `[0, 0]`,
/// and `NCP` as `SIN` slanted by `[0, cot δ₀]`.
#[test]
fn a_projection_labels_the_pixels_sin_images() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let controls = |projection: &str| json!({ "niter": 10, "projection": projection });
    let (sin_run, sin) = observation.image("sin", controls("SIN"));
    let cot = 1.0 / PHASE_CENTER_RAD[1].tan();
    for (projection, written, parameters) in [
        ("SIN", ProjectionType::SIN, [0.0, 0.0]),
        ("NCP", ProjectionType::SIN, [0.0, cot]),
        ("TAN", ProjectionType::TAN, [0.0, 0.0]),
        ("ARC", ProjectionType::ARC, [0.0, 0.0]),
        ("STG", ProjectionType::STG, [0.0, 0.0]),
        ("ZEA", ProjectionType::ZEA, [0.0, 0.0]),
        ("CAR", ProjectionType::CAR, [0.0, 0.0]),
        ("SFL", ProjectionType::SFL, [0.0, 0.0]),
        ("MER", ProjectionType::MER, [0.0, 0.0]),
        ("AIT", ProjectionType::AIT, [0.0, 0.0]),
    ] {
        let (run, products) = observation.image(projection, controls(projection));
        assert_eq!(
            (run.actual_minor_iterations, run.major_cycles),
            (sin_run.actual_minor_iterations, sin_run.major_cycles),
            "{projection}"
        );
        for suffix in [".image", ".psf", ".residual", ".model", ".sumwt"] {
            assert_eq!(
                products.get(suffix).values(),
                sin.get(suffix).values(),
                "{projection} {suffix}"
            );
            let coordinates = products.get(suffix).image.coordinates();
            let CoordinateModel::Direction(direction) = coordinates.coordinate(
                coordinates
                    .find_coordinate(CoordinateType::Direction)
                    .expect("a direction coordinate"),
            ) else {
                panic!("{projection} {suffix}: not a direction coordinate");
            };
            assert_eq!(
                direction.projection().projection_type(),
                written,
                "{projection} {suffix}"
            );
            let stated = direction.projection().parameters();
            assert!(
                stated.len() == 2
                    && stated
                        .iter()
                        .zip(parameters)
                        .all(|(stated, expected)| (stated - expected).abs() <= 1e-12),
                "{projection} {suffix}: {stated:?}"
            );
        }
    }
}
