// SPDX-License-Identifier: LGPL-3.0-or-later
//! Laws of sky-model prediction.
//!
//! - A point at the tangent point predicts its flux on every baseline, and an
//!   offset point the analytic Fourier phase.
//! - The tangent point is pixel `(nx/2, ny/2)` whatever the reference pixel.
//! - Each plane of a cube predicts the channel at its own frequency, and a
//!   channel beyond a one-plane model's width predicts zero.
//! - The spectral axis is normalized to Hz from any frequency unit.
//! - The primary beam scales a point by the analytic Airy power pattern.
//! - A Stokes I model leaves the cross-hands at zero, and flagged samples
//!   and rows predict zero.
//! - Models in other frames or layouts are refused rather than misread.
//!
//! Agreement with CASA's own `sm.predict`, sample by sample, is measured by
//! `tools/simulation/predict_spike.py` against an installed CASA (#841).

use std::f64::consts::{PI, TAU};
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
use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use ndarray::{ArrayD, IxDyn};
use num_complex::Complex32;

const N: usize = 32;
const CELL_RAD: f64 = 1.0e-5;
const CENTRE_RAD: [f64; 2] = [1.2, -0.4];
const FREQUENCY_HZ: f64 = 100.0e9;
const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];

/// A one-Stokes CASA model image.
struct Model {
    /// `(x, y, flux)` points of each plane.
    planes: Vec<Vec<(usize, usize, f32)>>,
    first_hz: f64,
    width_hz: f64,
    reference_pixel: [f64; 2],
    frame: DirectionRef,
    /// The spectral unit and how many Hz it holds.
    spectral_unit: (&'static str, f64),
}

impl Model {
    fn points(points: &[(usize, usize, f32)]) -> Self {
        Self {
            planes: vec![points.to_vec()],
            first_hz: FREQUENCY_HZ,
            width_hz: 1.0e6,
            reference_pixel: [(N / 2) as f64, (N / 2) as f64],
            frame: DirectionRef::J2000,
            spectral_unit: ("Hz", 1.0),
        }
    }

    fn write(&self, dir: &Path) -> PathBuf {
        let path = dir.join("model.image");
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(DirectionCoordinate::new(
            self.frame,
            Projection::new(ProjectionType::SIN),
            CENTRE_RAD,
            [-CELL_RAD, CELL_RAD],
            self.reference_pixel,
        ));
        coordinates.add_coordinate(StokesCoordinate::new(vec![StokesType::I]));
        let (unit, hz) = self.spectral_unit;
        coordinates.add_coordinate(
            SpectralCoordinate::new(
                FrequencyRef::LSRK,
                self.first_hz / hz,
                self.width_hz / hz,
                0.0,
                self.first_hz / hz,
            )
            .with_unit(unit),
        );
        let planes = self.planes.len();
        let mut image = PagedImage::<f32>::create(vec![N, N, 1, planes], coordinates, &path)
            .expect("create model image");
        let mut pixels = ArrayD::zeros(IxDyn(&[N, N, 1, planes]));
        for (plane, points) in self.planes.iter().enumerate() {
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

    fn open(&self, dir: &Path) -> Result<SkyModel, PredictError> {
        SkyModel::open(self.write(dir))
    }
}

fn predictor(model: &SkyModel, correlations: &[CorrelationType]) -> ModelPredictor {
    ModelPredictor::new(model, correlations, CENTRE_RAD, &PredictOptions::default())
        .expect("predictor")
}

/// Predict one unflagged row at `uvw_m`, `[channel][correlation]`.
fn predict(predictor: &ModelPredictor, uvw_m: [f64; 3], frequencies_hz: &[f64]) -> Vec<Complex32> {
    let cells = frequencies_hz.len() * predictor.correlations();
    let mut out = vec![Complex32::new(9.0, 9.0); cells];
    predictor
        .predict_row(
            &mut PredictionWorkspace::default(),
            &row(uvw_m, frequencies_hz, &vec![false; cells], false),
            &mut out,
        )
        .expect("prediction");
    out
}

fn row<'a>(
    uvw_m: [f64; 3],
    frequencies_hz: &'a [f64],
    flags: &'a [bool],
    row_flag: bool,
) -> PredictRow<'a> {
    PredictRow {
        uvw_m,
        phase_shift_m: 0.0,
        frequencies_hz,
        flags,
        row_flag,
        antennas: [0, 1],
    }
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

/// The visibility of a point at pixel offset `[px, py]` from the tangent
/// point, in the MeasurementSet convention `V = ∫ I e^{+2πi(ul + vm)}`.
fn fourier(flux: f64, [px, py]: [f64; 2], uvw_m: [f64; 3], frequency_hz: f64) -> Complex32 {
    let [l, m] = [px * -CELL_RAD, py * CELL_RAD];
    let phase = TAU * (uvw_m[0] * l + uvw_m[1] * m) * frequency_hz / 299_792_458.0;
    Complex32::new((flux * phase.cos()) as f32, (flux * phase.sin()) as f32)
}

#[test]
fn a_point_at_the_tangent_point_predicts_its_flux_on_every_baseline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let model = Model::points(&[(N / 2, N / 2, 2.5)])
        .open(dir.path())
        .expect("model");
    assert_eq!(model.centre_rad(), CENTRE_RAD);
    let predictor = predictor(&model, &XX_YY);
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
    let model = Model::points(&[(N / 2 + 3, N / 2 - 2, 1.0)])
        .open(dir.path())
        .expect("model");
    let predictor = predictor(&model, &XX_YY);
    // Gridding a 32-pixel image approximates the transform to a few parts in
    // 1e3 at the longest baselines; CASA's GridFT makes the same
    // approximation.
    for uvw in baselines() {
        let expected = fourier(1.0, [3.0, -2.0], uvw, FREQUENCY_HZ);
        for value in predict(&predictor, uvw, &[FREQUENCY_HZ]) {
            assert!(
                (value - expected).norm() < 5.0e-3,
                "{uvw:?}: {value} vs {expected}"
            );
        }
    }
}

#[test]
fn the_tangent_point_is_the_centre_pixel_whatever_the_reference_pixel() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut spec = Model::points(&[(N / 2, N / 2, 1.0)]);
    spec.reference_pixel = [(N / 2 - 5) as f64, (N / 2 + 3) as f64];
    let model = spec.open(dir.path()).expect("model");
    assert_ne!(model.centre_rad(), CENTRE_RAD);
    // The point on pixel (n/2, n/2) sits on the tangent point: flux, no phase.
    let predictor = predictor(&model, &XX_YY);
    for uvw in baselines() {
        for value in predict(&predictor, uvw, &[FREQUENCY_HZ]) {
            assert!(
                (value - Complex32::new(1.0, 0.0)).norm() < 1.0e-3,
                "{uvw:?}: {value}"
            );
        }
    }
}

#[test]
fn each_plane_of_a_cube_predicts_the_channel_at_its_frequency() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut spec = Model::points(&[]);
    spec.planes = (1..=3)
        .map(|flux| vec![(N / 2, N / 2, flux as f32)])
        .collect();
    let model = spec.open(dir.path()).expect("model");
    let predictor = predictor(&model, &XX_YY);
    let frequencies: Vec<f64> = (0..3)
        .map(|plane| FREQUENCY_HZ + plane as f64 * 1.0e6)
        .collect();
    let values = predict(&predictor, [10.0, 4.0, 0.0], &frequencies);
    for (channel, flux) in [1.0_f32, 2.0, 3.0].into_iter().enumerate() {
        for value in &values[channel * 2..channel * 2 + 2] {
            assert!(
                (value.re - flux).abs() < 3.0e-3,
                "channel {channel}: {value}"
            );
        }
    }
}

#[test]
fn channels_beyond_a_one_plane_models_width_predict_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut spec = Model::points(&[(N / 2, N / 2, 1.0)]);
    spec.width_hz = 2.0e6;
    let model = spec.open(dir.path()).expect("model");
    let predictor = predictor(&model, &XX_YY);
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
fn a_ghz_spectral_axis_predicts_as_its_hz_equivalent() {
    let points = [(N / 2 + 2, N / 2 + 1, 1.5)];
    let frequencies = [FREQUENCY_HZ - 0.3e6, FREQUENCY_HZ, FREQUENCY_HZ + 0.4e6];
    let mut predictions = Vec::new();
    for unit in [("Hz", 1.0), ("GHz", 1.0e9)] {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut spec = Model::points(&points);
        spec.spectral_unit = unit;
        let model = spec.open(dir.path()).expect("model");
        predictions.push(predict(
            &predictor(&model, &XX_YY),
            [25.0, -9.0, 0.0],
            &frequencies,
        ));
    }
    assert!(
        predictions[1].iter().all(|value| value.norm() > 1.0),
        "{:?}",
        predictions[1]
    );
    for (hz, ghz) in predictions[0].iter().zip(&predictions[1]) {
        assert!((hz - ghz).norm() < 1.0e-5, "{hz} vs {ghz}");
    }
}

/// `J₁(x)` from its integral `(1/π)∫₀^π cos(τ − x sin τ) dτ` (Simpson's rule).
fn bessel_j1(x: f64) -> f64 {
    const STEPS: usize = 2000;
    let h = PI / STEPS as f64;
    let f = |tau: f64| (tau - x * tau.sin()).cos();
    let interior: f64 = (1..STEPS)
        .map(|k| f(k as f64 * h) * if k % 2 == 1 { 4.0 } else { 2.0 })
        .sum();
    (f(0.0) + interior + f(PI)) * h / (3.0 * PI)
}

/// CASA's Airy voltage pattern (`PBMath1DAiry::fillPBArray`) for a dish of
/// diameter `d` blocked to `b`, at angle `theta` and `frequency_hz`.
///
/// CASA evaluates the blockage term at `x · d/b` where the blocked disk's
/// pattern is at `x · b/d`, which raises an ALMA beam by about 1% in its main
/// lobe; casa-rs follows CASA (`docs/CASA (C++) bugs.md`).
fn casa_airy_voltage(d: f64, b: f64, theta: f64, frequency_hz: f64) -> f64 {
    // CASA's scale: 7.016 (J₁'s second zero) at 1.566° · 60 for 24.5 m at 1 GHz.
    let radius_arcmin_ghz = theta.to_degrees() * 60.0 * frequency_hz / 1.0e9;
    let x = radius_arcmin_ghz * 7.016 / (1.566 * 60.0) * d / 24.5;
    let jinc = |x: f64| 2.0 * bessel_j1(x) / x;
    let area_ratio = (d / b).powi(2);
    (area_ratio * jinc(x) - jinc(x * d / b)) / (area_ratio - 1.0)
}

#[test]
fn the_primary_beam_scales_a_point_by_casas_airy_power_pattern() {
    let dir = tempfile::tempdir().expect("tempdir");
    let offset = 6;
    let model = Model::points(&[(N / 2 + offset, N / 2, 1.0)])
        .open(dir.path())
        .expect("model");
    let options = PredictOptions {
        primary_beam: Some(PrimaryBeam::ALMA),
        ..PredictOptions::default()
    };
    let beamed = ModelPredictor::new(&model, &XX_YY, CENTRE_RAD, &options).expect("predictor");
    let voltage = casa_airy_voltage(10.7, 0.75, offset as f64 * CELL_RAD, FREQUENCY_HZ);
    let power = (voltage * voltage) as f32;
    assert!(
        power > 0.2 && power < 0.99,
        "the offset should be inside the main lobe: {power}"
    );
    // The beam multiplies the model, so on every baseline the beamed
    // prediction is the unbeamed one times the power at the point. The ratio
    // cancels the gridding approximation, which for a point this far from
    // the centre of a 32-pixel image is about 1%.
    let plain = predictor(&model, &XX_YY);
    for uvw in [[0.0, 0.0, 0.0], [17.0, -9.0, 0.0]] {
        let with_beam = predict(&beamed, uvw, &[FREQUENCY_HZ]);
        let without = predict(&plain, uvw, &[FREQUENCY_HZ]);
        for (with_beam, without) in with_beam.iter().zip(&without) {
            let ratio = with_beam / without;
            assert!(
                (ratio - power).norm() < 2.0e-3,
                "{uvw:?}: {ratio} vs {power}"
            );
        }
    }
}

#[test]
fn a_stokes_i_model_leaves_the_cross_hands_at_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let model = Model::points(&[(N / 2 + 2, N / 2, 1.0)])
        .open(dir.path())
        .expect("model");
    let circular = [
        CorrelationType::CircularRr,
        CorrelationType::CircularRl,
        CorrelationType::CircularLr,
        CorrelationType::CircularLl,
    ];
    let values = predict(
        &predictor(&model, &circular),
        [20.0, -11.0, 0.0],
        &[FREQUENCY_HZ],
    );
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
fn flagged_samples_and_rows_predict_zero_and_lengths_are_checked() {
    let dir = tempfile::tempdir().expect("tempdir");
    let model = Model::points(&[(N / 2 + 1, N / 2, 1.0)])
        .open(dir.path())
        .expect("model");
    let predictor = predictor(&model, &XX_YY);
    let frequencies = [FREQUENCY_HZ, FREQUENCY_HZ + 0.2e6];
    let mut workspace = PredictionWorkspace::default();
    let mut out = vec![Complex32::new(9.0, 9.0); 4];
    let flags = [false, true, false, false];
    predictor
        .predict_row(
            &mut workspace,
            &row([15.0, 3.0, 0.0], &frequencies, &flags, false),
            &mut out,
        )
        .expect("prediction");
    assert_eq!(out[1], Complex32::new(0.0, 0.0));
    assert!(
        out.iter()
            .enumerate()
            .all(|(cell, value)| cell == 1 || value.norm() > 0.5)
    );
    predictor
        .predict_row(
            &mut workspace,
            &row([15.0, 3.0, 0.0], &frequencies, &[false; 4], true),
            &mut out,
        )
        .expect("prediction");
    assert!(out.iter().all(|value| *value == Complex32::new(0.0, 0.0)));
    let mut short = vec![Complex32::new(0.0, 0.0); 3];
    assert!(
        predictor
            .predict_row(
                &mut workspace,
                &row([15.0, 3.0, 0.0], &frequencies, &[false; 4], false),
                &mut short
            )
            .is_err()
    );
}

#[test]
fn a_model_in_another_frame_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut spec = Model::points(&[(N / 2, N / 2, 1.0)]);
    spec.frame = DirectionRef::B1950;
    assert!(matches!(
        spec.open(dir.path()),
        Err(PredictError::UnsupportedModel(reason)) if reason.contains("J2000")
    ));
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
