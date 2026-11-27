// SPDX-License-Identifier: LGPL-3.0-or-later

//! Counting-law tests for start-model seeding and per-field outlier admission.

use std::path::Path;
use std::sync::atomic::Ordering;

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_core::measures::{direction::DirectionRef, frequency::FrequencyRef};
use casa_imageanalysis::RegridTemplate;
use casa_images::PagedImage;
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use serde_json::json;

use super::{main_samples, seeding_bytes};
use crate::counting_heap::{
    AMPLE, BASELINE, LIVE, MEASURING, PEAK, SEED_BYTES, admission_request, alone, check,
    synthetic_observation, warm,
};

const ARCSEC: f64 = std::f64::consts::PI / (180.0 * 3600.0);

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

fn spectral(mut coordinates: CoordinateSystem, first_hz: f64, step_hz: f64) -> CoordinateSystem {
    let index = coordinates
        .find_coordinate(casa_coordinates::CoordinateType::Spectral)
        .expect("a spectral coordinate");
    coordinates.replace_coordinate(
        index,
        SpectralCoordinate::new(FrequencyRef::LSRK, first_hz, step_hz, 0.0, 1.0e9),
    );
    coordinates
}

#[test]
fn seeding_holds_at_most_what_it_is_admitted_and_keeps_the_flux() {
    if !alone(
        "start_model_law::memory_tests::seeding_holds_at_most_what_it_is_admitted_and_keeps_the_flux",
    ) {
        return;
    }
    let root = tempfile::tempdir().expect("test root");
    let path = root.path().join("start.model");
    let side = 256;
    let shape = [side, side, 1, 1];
    let mut image = PagedImage::<f32>::create(shape.to_vec(), coordinates(side, 4.0), &path)
        .expect("create the start model");
    image.set_units("Jy/pixel").expect("units");
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

    let (side, channels) = (128, 4);
    for (label, input_channels, output_channels, start, width) in [
        ("more out", channels, 8, 1.0e9, 0.5e6),
        ("more in", 16, channels, 1.0e9, 4.0e6),
        ("shifted", channels, channels, 1.0005e9, 1.0e6),
    ] {
        let cube_path = root.path().join(format!("cube {label}.model"));
        let shape = [side, side, 1, input_channels];
        let mut cube = PagedImage::<f32>::create(
            shape.to_vec(),
            spectral(coordinates(side, 4.0), 1.0e9, 1.0e6),
            &cube_path,
        )
        .expect("create the cube start model");
        cube.set_units("Jy/pixel").expect("units");
        let pixels = ArrayD::from_shape_fn(IxDyn(&shape).f(), |index| {
            let [x, y] = [index[0], index[1]].map(|v| v as f64 - (side / 2) as f64);
            ((-(x * x + y * y) / (2.0 * 10.0 * 10.0)).exp() * (1.0 + index[3] as f64)) as f32
        });
        cube.put_slice(&pixels, &[0; 4]).expect("pixels");
        cube.save().expect("save the cube start model");
        drop((cube, pixels));
        let template = || RegridTemplate {
            coordinates: spectral(coordinates(512, 1.0), start, width),
            shape: vec![512, 512, 1, output_channels],
        };
        let admitted = seeding_bytes(&cube_path, template()).expect("a valid cube start model");
        PEAK.store(0, Ordering::Relaxed);
        BASELINE.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
        MEASURING.store(true, Ordering::Relaxed);
        let samples = main_samples(&cube_path, template());
        MEASURING.store(false, Ordering::Relaxed);
        let samples = samples.expect("seeded cube samples");
        let peak = PEAK.load(Ordering::Relaxed);
        assert!(
            peak <= admitted,
            "spectral regrid, {label}: held {peak} B, admitted {admitted} B"
        );
        assert_eq!(samples.len(), 512 * 512 * output_channels);
    }
}

#[test]
fn explicit_outlier_seeds_are_admitted_one_field_at_a_time() {
    if !alone(
        "start_model_law::memory_tests::explicit_outlier_seeds_are_admitted_one_field_at_a_time",
    ) {
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let ms = synthetic_observation::observation(root.path(), 6);
    warm(&ms, root.path());
    let define = |name: &str, outlier: &str, model: Option<&Path>| {
        let path = root.path().join(name);
        let seed = model.map_or_else(String::new, |model| {
            format!("startmodel={}\n", model.display())
        });
        std::fs::write(
            &path,
            format!(
                "imagename={}\nimsize=384\nphasecenter=J2000 18h00m00s -23d00m10s\n{seed}",
                root.path().join(outlier).display()
            ),
        )
        .unwrap();
        path
    };
    let initial = admission_request(
        &ms,
        &root.path().join("initial-main"),
        512,
        json!({"niter":0, "outlierfile":define("initial.txt", "initial-outlier", None)}),
    );
    let mut breaches = Vec::new();
    assert_eq!(
        check(&mut breaches, "unseeded control", &initial, 1, AMPLE)
            .outcome
            .unwrap()
            .major_cycle_count,
        1
    );

    let main = root.path().join("initial-main.model");
    let outlier = root.path().join("initial-outlier.model");
    let expected = |path: &Path| {
        let image = casa_images::PagedImage::<f32>::open(path).unwrap();
        let template = RegridTemplate {
            coordinates: image.coordinates().clone(),
            shape: image.shape().to_vec(),
        };
        seeding_bytes(path, template).unwrap()
    };
    let largest = expected(&main).max(expected(&outlier));
    let fresh = admission_request(
        &ms,
        &root.path().join("fresh-main"),
        512,
        json!({"niter":0, "startmodel":main, "outlierfile":define("fresh.txt","fresh-outlier",Some(&outlier))}),
    );
    SEED_BYTES.store(0, Ordering::Relaxed);
    let run = check(&mut breaches, "fresh outlier models", &fresh, 1, AMPLE);
    assert!(breaches.is_empty(), "{}", breaches.join("\n"));
    assert_eq!(run.outcome.unwrap().major_cycle_count, 1);
    let admitted = SEED_BYTES.load(Ordering::Relaxed);
    assert_eq!(admitted, largest, "admit just one field's source scratch");
}
