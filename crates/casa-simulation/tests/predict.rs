// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of sky-model prediction.
//!
//! - A point at the tangent point predicts its flux on every baseline.
//! - An offset point predicts the analytic Fourier phase.
//! - The primary beam scales a point by the power pattern at its offset.
//! - Channels beyond a one-plane model's width predict zero.
//! - A Stokes I model leaves the cross-hands at zero.
//!
//! Agreement with CASA's own `sm.predict`, sample by sample, is measured by
//! `tools/simulation/predict_spike.py` against an installed CASA (#841).

use std::f64::consts::TAU;
use std::path::{Path, PathBuf};

use casa_coordinates::{
    CoordinateSystem, DirectionCoordinate, Projection, ProjectionType, SpectralCoordinate,
    StokesCoordinate, StokesType,
};
use casa_images::PagedImage;
use casa_imaging_model::CorrelationType;
use casa_simulation::predict::{
    ModelPredictor, PredictError, PredictOptions, PredictRow, PredictionWorkspace, PrimaryBeam,
    SkyModel,
};
use casa_simulation_synthesis::{AiryPrimaryBeam, AiryVoltagePattern};
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{ArrayD, IxDyn};
use num_complex::Complex32;

const N: usize = 32;
const CELL_RAD: f64 = 1.0e-5;
const CENTRE_RAD: [f64; 2] = [1.2, -0.4];
const FREQUENCY_HZ: f64 = 100.0e9;
const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];

/// A one-Stokes CASA image of `planes` planes from `first_hz` in steps of
/// `width_hz`, with `points` of `(x, y, flux)` on every plane.
fn write_model(
    dir: &Path,
    planes: usize,
    first_hz: f64,
    width_hz: f64,
    points: &[(usize, usize, f32)],
) -> PathBuf {
    let path = dir.join("model.image");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        CENTRE_RAD,
        [-CELL_RAD, CELL_RAD],
        [(N / 2) as f64, (N / 2) as f64],
    ));
    coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
    coordinates.add_coordinate(SpectralCoordinate::new(
        FrequencyRef::LSRK,
        first_hz,
        width_hz,
        0.0,
        first_hz,
    ));
    let mut image = PagedImage::<f32>::create(vec![N, N, 1, planes], coordinates, &path)
        .expect("create model image");
    let mut pixels = ArrayD::zeros(IxDyn(&[N, N, 1, planes]));
    for plane in 0..planes {
        for &(x, y, flux) in points {
            pixels[IxDyn(&[x, y, 0, plane])] = flux;
        }
    }
    image
        .put_slice(&pixels, &[0, 0, 0, 0])
        .expect("write pixels");
    image.save().expect("save model image");
    path
}

/// Predict one row at `uvw_m` for `frequencies_hz`, `[channel][correlation]`.
fn predict(predictor: &ModelPredictor, uvw_m: [f64; 3], frequencies_hz: &[f64]) -> Vec<Complex32> {
    let cells = frequencies_hz.len() * predictor.correlations();
    let mut out = vec![Complex32::new(9.0, 9.0); cells];
    predictor
        .predict_row(
            &mut PredictionWorkspace::default(),
            &PredictRow {
                uvw_m,
                phase_shift_m: 0.0,
                frequencies_hz,
                flags: &vec![false; cells],
                row_flag: false,
                antennas: [0, 1],
            },
            &mut out,
        )
        .expect("prediction");
    out
}

/// Baselines inside the padded grid, in metres.
fn baselines() -> Vec<[f64; 3]> {
    vec![
        [12.0, -7.0, 0.0],
        [-31.0, 18.0, 0.0],
        [44.0, 29.0, 0.0],
        [-6.0, -52.0, 0.0],
    ]
}

#[test]
fn a_point_at_the_tangent_point_predicts_its_flux_on_every_baseline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let model = SkyModel::open(write_model(
        dir.path(),
        1,
        FREQUENCY_HZ,
        1.0e6,
        &[(N / 2, N / 2, 2.5)],
    ))
    .expect("model");
    assert_eq!(model.centre_rad(), CENTRE_RAD);
    let predictor = ModelPredictor::new(&model, &XX_YY, CENTRE_RAD, &PredictOptions::default())
        .expect("predictor");
    for uvw in baselines() {
        for value in predict(&predictor, uvw, &[FREQUENCY_HZ]) {
            assert!(
                (value - Complex32::new(2.5, 0.0)).norm() < 2.5e-3,
                "{uvw:?}: {value}"
            );
        }
    }
}

#[test]
fn an_offset_point_predicts_the_fourier_phase() {
    let dir = tempfile::tempdir().expect("tempdir");
    let [px, py] = [3_i32, -2_i32];
    let at = [(N as i32 / 2 + px) as usize, (N as i32 / 2 + py) as usize];
    let model = SkyModel::open(write_model(
        dir.path(),
        1,
        FREQUENCY_HZ,
        1.0e6,
        &[(at[0], at[1], 1.0)],
    ))
    .expect("model");
    let predictor = ModelPredictor::new(&model, &XX_YY, CENTRE_RAD, &PredictOptions::default())
        .expect("predictor");
    // The MeasurementSet convention V = ∫ I e^{+2πi(ul + vm)}, with l and m
    // the pixel offsets times the signed increments. Gridding a 32-pixel
    // image approximates the transform to a few parts in 1e3 at the longest
    // baselines; CASA's GridFT makes the same approximation.
    let [l, m] = [f64::from(px) * -CELL_RAD, f64::from(py) * CELL_RAD];
    let wavelength_m = 299_792_458.0 / FREQUENCY_HZ;
    for uvw in baselines() {
        let phase = TAU * (uvw[0] * l + uvw[1] * m) / wavelength_m;
        let expected = Complex32::new(phase.cos() as f32, phase.sin() as f32);
        for value in predict(&predictor, uvw, &[FREQUENCY_HZ]) {
            assert!(
                (value - expected).norm() < 5.0e-3,
                "{uvw:?}: {value} vs {expected}"
            );
        }
    }
}

#[test]
fn the_primary_beam_scales_a_point_by_the_power_pattern() {
    let dir = tempfile::tempdir().expect("tempdir");
    let offset = 6;
    let model = SkyModel::open(write_model(
        dir.path(),
        1,
        FREQUENCY_HZ,
        1.0e6,
        &[(N / 2 + offset, N / 2, 1.0)],
    ))
    .expect("model");
    let options = PredictOptions {
        primary_beam: Some(PrimaryBeam::ALMA),
        ..PredictOptions::default()
    };
    let predictor = ModelPredictor::new(&model, &XX_YY, CENTRE_RAD, &options).expect("predictor");
    let voltage = AiryVoltagePattern::new(AiryPrimaryBeam {
        dish_diameter_m: 10.7,
        blockage_diameter_m: 0.75,
    })
    .evaluate_offsets(offset as f64 * -CELL_RAD, 0.0, FREQUENCY_HZ);
    let power = voltage * voltage;
    assert!(
        power > 0.2 && power < 0.99,
        "the offset should be inside the main lobe: {power}"
    );
    // At u = v = 0 the prediction is the beam-weighted flux.
    for value in predict(&predictor, [0.0, 0.0, 0.0], &[FREQUENCY_HZ]) {
        assert!((value.norm() - power).abs() < 2.0e-3, "{value} vs {power}");
    }
}

#[test]
fn channels_beyond_a_one_plane_models_width_predict_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let width_hz = 2.0e6;
    let model = SkyModel::open(write_model(
        dir.path(),
        1,
        FREQUENCY_HZ,
        width_hz,
        &[(N / 2, N / 2, 1.0)],
    ))
    .expect("model");
    let predictor = ModelPredictor::new(&model, &XX_YY, CENTRE_RAD, &PredictOptions::default())
        .expect("predictor");
    let frequencies = [
        FREQUENCY_HZ - 3.0e6,
        FREQUENCY_HZ,
        FREQUENCY_HZ + 0.5e6,
        FREQUENCY_HZ + 3.0e6,
    ];
    let values = predict(&predictor, [10.0, 5.0, 0.0], &frequencies);
    for (channel, inside) in [false, true, true, false].into_iter().enumerate() {
        for value in &values[channel * 2..channel * 2 + 2] {
            assert_eq!(value.norm() > 0.5, inside, "channel {channel}: {value}");
        }
    }
}

#[test]
fn a_stokes_i_model_leaves_the_cross_hands_at_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let model = SkyModel::open(write_model(
        dir.path(),
        1,
        FREQUENCY_HZ,
        1.0e6,
        &[(N / 2 + 2, N / 2, 1.0)],
    ))
    .expect("model");
    let circular = [
        CorrelationType::CircularRr,
        CorrelationType::CircularRl,
        CorrelationType::CircularLr,
        CorrelationType::CircularLl,
    ];
    let predictor = ModelPredictor::new(&model, &circular, CENTRE_RAD, &PredictOptions::default())
        .expect("predictor");
    let values = predict(&predictor, [20.0, -11.0, 0.0], &[FREQUENCY_HZ]);
    assert!(
        (values[0] - values[3]).norm() < 1.0e-6,
        "RR {} LL {}",
        values[0],
        values[3]
    );
    assert!(values[0].norm() > 0.5);
    assert_eq!(values[1], Complex32::new(0.0, 0.0));
    assert_eq!(values[2], Complex32::new(0.0, 0.0));
}

#[test]
fn a_model_without_direction_stokes_and_spectral_axes_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("plane.image");
    let mut coordinates = CoordinateSystem::new();
    coordinates.add_coordinate(DirectionCoordinate::new(
        DirectionRef::J2000,
        Projection::new(ProjectionType::SIN),
        CENTRE_RAD,
        [-CELL_RAD, CELL_RAD],
        [(N / 2) as f64, (N / 2) as f64],
    ));
    let mut image = PagedImage::<f32>::create(vec![N, N], coordinates, &path).expect("create");
    image.save().expect("save");
    assert!(matches!(
        SkyModel::open(&path),
        Err(PredictError::UnsupportedModel(_))
    ));
}
