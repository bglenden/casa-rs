// SPDX-License-Identifier: LGPL-3.0-or-later

//! CASA's dirty MT-MFS-via-cube products: cube-major images and the weighted
//! Taylor family both belong to the same run (#1069).

use casa_ms::{MeasurementSet, OptionalMainColumn};
use casa_types::{ArrayValue, Value};
use serde_json::json;

use super::fixture::{Component, Geometry, Observation, Setup};

const REFERENCE_HZ: f64 = 45.0e9;
const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 5.0,
    fwhm_px: None,
    spectral_index: -1.0,
};

fn weighted_observation() -> Observation {
    let mut setup = Setup::vla_q_band(3);
    setup.geometry = Geometry::square(256, 0.3);
    let observation = Observation::synthesise_setup(setup, &[("point", POINT)]);
    set_unequal_channel_weights(&observation);
    observation
}

fn set_unequal_channel_weights(observation: &Observation) {
    // Give each channel a different positive weight so a plain channel
    // average cannot satisfy CASA's weighted Taylor law.
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    if !ms
        .main_table()
        .schema()
        .unwrap()
        .contains_column("WEIGHT_SPECTRUM")
    {
        let schema = casa_ms::column_def::build_table_schema(&[
            *OptionalMainColumn::WeightSpectrum.column_def(),
        ])
        .unwrap();
        ms.main_table_mut()
            .add_column(schema.column("WEIGHT_SPECTRUM").unwrap().clone(), None)
            .unwrap();
    }
    for row in 0..ms.row_count() {
        let ArrayValue::Float32(weight) = ms.weight_column().get(row).unwrap().clone() else {
            panic!("Float WEIGHT")
        };
        let ArrayValue::Bool(flags) = ms.flag_column().get(row).unwrap().clone() else {
            panic!("boolean FLAG")
        };
        let spectrum = ndarray::ArrayD::from_shape_fn(flags.raw_dim(), |index| {
            weight[[index[0]]] * [1.0_f32, 0.75, 0.4][index[1]]
        });
        ms.main_table_mut()
            .cell_accessor_mut(row, "WEIGHT_SPECTRUM")
            .unwrap()
            .set(Value::Array(ArrayValue::Float32(spectrum)))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
}

/// CASA's `cubeToTaylorSum` weights each cube plane by its sumwt and its
/// frequency offset. PSF moments also use the per-channel primary beam,
/// divided by the weighted mean PB where it exceeds the limit. `.sumwt.tt*`
/// is itself weighted, so it contains the square of each channel's sumwt.
#[test]
fn mvc_dirty_publishes_the_cube_and_weighted_taylor_products() {
    let observation = weighted_observation();
    let (_, products) = observation.image(
        "mvc-dirty",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 0, "write_pb": true, "pblimit": -0.1,
        }),
    );
    let expected = [
        ".alpha",
        ".alpha.error",
        ".image.tt0",
        ".image.tt1",
        ".model.tt0",
        ".model.tt1",
        ".pb",
        ".pb.tt0",
        ".psf",
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".residual",
        ".residual.tt0",
        ".residual.tt1",
        ".sumwt",
        ".sumwt.tt0",
        ".sumwt.tt1",
        ".sumwt.tt2",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    assert_eq!(products.suffixes(), expected);
    let psf = products.get(".psf");
    let pb = products.get(".pb");
    let weights = products.get(".sumwt").channel_values();
    assert_eq!(psf.channels(), 3);
    assert_eq!(pb.channels(), 3);
    let taylor_psf = products.get(".psf.tt0");
    assert_eq!(taylor_psf.channels(), 1);
    let cube_midpoint = (psf.channel_frequency_hz(0) + psf.channel_frequency_hz(2)) / 2.0;
    assert!((taylor_psf.channel_frequency_hz(0) - cube_midpoint).abs() < 1e-3);
    assert!((taylor_psf.channel_width_hz() - 3.0 * psf.channel_width_hz()).abs() < 1e-3);
    assert!(
        (taylor_psf.channel_frequency_hz(0) - REFERENCE_HZ).abs() > 1e6,
        "the Taylor image WCS is the band midpoint, not the expansion reference"
    );
    assert_eq!(weights.len(), 3);
    assert!(
        weights[0] > weights[1] && weights[1] > weights[2],
        "channel weights must differ: {weights:?}"
    );
    let weights: Vec<_> = weights.into_iter().map(f64::from).collect();
    let total: f64 = weights.iter().sum();
    assert!(total > 0.0);
    let x: Vec<_> = (0..3)
        .map(|channel| (psf.channel_frequency_hz(channel) - REFERENCE_HZ) / REFERENCE_HZ)
        .collect();
    let beams: Vec<Vec<f32>> = (0..3)
        .map(|c| pb.plane(c).iter().copied().collect())
        .collect();
    let psfs: Vec<Vec<f32>> = (0..3)
        .map(|c| psf.plane(c).iter().copied().collect())
        .collect();
    // CASA removes the frequency-dependent beam from each cube residual,
    // then restores the common Taylor PB before making Taylor terms.
    let (_, control) = observation.image(
        "ordinary-cube",
        json!({
            "specmode": "cube", "deconvolver": "hogbom", "nchan": 3,
            "start": "44GHz", "width": "128MHz", "weighting": "natural",
            "niter": 0, "write_pb": true, "pblimit": -0.1,
        }),
    );
    let cube_residual = products.get(".residual");
    let ordinary_residual = control.get(".residual");
    assert_eq!(cube_residual.channels(), 3);
    assert_eq!(ordinary_residual.channels(), 3);
    for channel in 0..3 {
        assert!(
            (psf.channel_frequency_hz(channel) - control.get(".psf").channel_frequency_hz(channel))
                .abs()
                < 1e-3
        );
    }
    let residuals: Vec<Vec<f32>> = (0..3)
        .map(|c| cube_residual.plane(c).iter().copied().collect())
        .collect();
    let ordinary_residuals: Vec<Vec<f32>> = (0..3)
        .map(|c| ordinary_residual.plane(c).iter().copied().collect())
        .collect();
    let residual_moments: Vec<Vec<f32>> = (0..2)
        .map(|t| {
            products
                .get(&format!(".residual.tt{t}"))
                .values()
                .iter()
                .copied()
                .collect()
        })
        .collect();
    let pb0 = products.get(".pb.tt0").values();
    let actual_pb: Vec<_> = pb0.iter().copied().collect();
    let moments: Vec<Vec<f32>> = (0..3)
        .map(|t| {
            products
                .get(&format!(".psf.tt{t}"))
                .values()
                .iter()
                .copied()
                .collect()
        })
        .collect();
    let mut below_limit = 0;
    for (pixel, &actual) in actual_pb.iter().enumerate() {
        let mean_pb: f64 = (0..3)
            .map(|c| f64::from(beams[c][pixel]) * weights[c])
            .sum::<f64>()
            / total;
        assert!(
            (f64::from(actual) - mean_pb).abs() < 1e-5,
            "PB pixel {pixel}"
        );
        for channel in 0..3 {
            let pb = f64::from(beams[channel][pixel]);
            let expected = if pb > 0.1 {
                f64::from(ordinary_residuals[channel][pixel]) * mean_pb / pb
            } else {
                0.0
            };
            assert!(
                (f64::from(residuals[channel][pixel]) - expected).abs() < 1e-5,
                "cube residual channel {channel} pixel {pixel}"
            );
        }
        if mean_pb > 0.1 {
            for (t, moment) in moments.iter().enumerate() {
                let expected: f64 = (0..3)
                    .map(|c| {
                        x[c].powi(t as i32)
                            * weights[c]
                            * f64::from(psfs[c][pixel])
                            * f64::from(beams[c][pixel])
                    })
                    .sum::<f64>()
                    / total
                    / mean_pb;
                assert!(
                    (f64::from(moment[pixel]) - expected).abs() < 1e-5,
                    "PSF term {t} pixel {pixel}"
                );
            }
            for (t, moment) in residual_moments.iter().enumerate() {
                let expected: f64 = (0..3)
                    .map(|c| {
                        x[c].powi(t as i32)
                            * weights[c]
                            * f64::from(residuals[c][pixel])
                            * f64::from(beams[c][pixel])
                    })
                    .sum::<f64>()
                    / total
                    / mean_pb;
                assert!(
                    (f64::from(moment[pixel]) - expected).abs() < 1e-5,
                    "residual term {t} pixel {pixel}"
                );
            }
        } else {
            below_limit += 1;
            for (term, moment) in residual_moments.iter().enumerate() {
                assert!(
                    moment[pixel].abs() < 1e-5,
                    "residual term {term} below PB limit at pixel {pixel}"
                );
            }
            for (term, moment) in moments.iter().enumerate() {
                assert!(
                    moment[pixel].abs() < 1e-5,
                    "PSF term {term} below PB limit at pixel {pixel}"
                );
            }
        }
    }
    assert!(
        below_limit > 0,
        "the field must exercise the primary-beam limit"
    );
    for t in 0..3 {
        let expected: f64 = (0..3)
            .map(|c| x[c].powi(t) * weights[c] * weights[c])
            .sum::<f64>()
            / total;
        let actual = f64::from(products.get(&format!(".sumwt.tt{t}")).pixels[[0, 0]]);
        assert!(
            (actual - expected).abs() < 1e-4 * expected.abs().max(1.0),
            "sumwt term {t}: {actual} != {expected}"
        );
    }
}

/// MVC's output cube channel count does not select that many input channels:
/// the cube spans the whole selected spectral window, as CASA's
/// `determineFreqRange` does before it creates the cube.
#[test]
fn mvc_output_channel_count_does_not_narrow_the_input_band() {
    let mut setup = Setup::vla_q_band(6);
    setup.geometry = Geometry::square(256, 0.3);
    let observation = Observation::synthesise_setup(setup, &[("point", POINT)]);
    let (_, products) = observation.image(
        "mvc-six-to-three",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 0, "write_pb": true, "pblimit": -0.1,
        }),
    );
    let psf = products.get(".psf");
    assert_eq!(psf.channels(), 3);
    assert!((psf.channel_width_hz() - 256e6).abs() < 1.0);
    assert!((psf.channel_frequency_hz(0) - 44.064e9).abs() < 1.0);
    assert!((psf.channel_frequency_hz(2) - 44.576e9).abs() < 1.0);
    let taylor = products.get(".psf.tt0");
    assert!((taylor.channel_frequency_hz(0) - 44.320e9).abs() < 1.0);
    assert!((taylor.channel_width_hz() - 768e6).abs() < 1.0);
    assert!(
        products
            .get(".sumwt")
            .channel_values()
            .iter()
            .all(|weight| *weight > 20_000.0)
    );
    // CASA's bandwidth rule chooses these same three cube channels for
    // nchan < 1 on this six-input-channel band.
    let (_, automatic) = observation.image(
        "mvc-six-auto",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": -1, "reffreq": "45GHz", "weighting": "natural",
            "niter": 0, "write_pb": true, "pblimit": -0.1,
        }),
    );
    assert_eq!(automatic.suffixes(), products.suffixes());
    for suffix in [".psf", ".psf.tt0", ".sumwt", ".residual", ".pb"] {
        let left = automatic.get(suffix);
        let right = products.get(suffix);
        assert_eq!(left.image.shape(), right.image.shape());
        assert_eq!(left.mask(), right.mask());
        assert_eq!(left.channel_width_hz(), right.channel_width_hz());
        for channel in 0..left.channels() {
            assert_eq!(
                left.channel_frequency_hz(channel),
                right.channel_frequency_hz(channel)
            );
        }
        let a = left.values();
        let b = right.values();
        let (difference, power) = a
            .iter()
            .zip(b.iter())
            .fold((0.0_f64, 0.0_f64), |(d, p), (&x, &y)| {
                (d + f64::from(x - y).powi(2), p + f64::from(y).powi(2))
            });
        assert!(
            difference.sqrt() <= 1e-6 * power.sqrt().max(1.0),
            "{suffix}: unequal automatic depth"
        );
    }
}

/// CASA's cube sum weights can be positive while every Float Taylor sum
/// underflows to zero. A dirty, unrestored MVC run still publishes its zero
/// Taylor moments and the nonzero common primary beam.
#[test]
fn mvc_unrestored_tiny_weights_publish_blank_taylor_moments() {
    let mut setup = Setup::vla_q_band(6);
    setup.geometry = Geometry::square(256, 0.3);
    let observation = Observation::synthesise_setup(setup, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    assert!(
        !ms.main_table()
            .schema()
            .unwrap()
            .contains_column("WEIGHT_SPECTRUM"),
        "this fixture scales its WEIGHT column"
    );
    for row in 0..ms.row_count() {
        let ArrayValue::Float32(mut weight) = ms.weight_column().get(row).unwrap().clone() else {
            panic!("Float WEIGHT")
        };
        weight.mapv_inplace(|value| value * 1e-34);
        ms.main_table_mut()
            .cell_accessor_mut(row, "WEIGHT")
            .unwrap()
            .set(Value::Array(ArrayValue::Float32(weight)))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
    let (_, products) = observation.image(
        "mvc-tiny-weight",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 0, "write_pb": true, "pblimit": -0.1,
            "restoration": false,
        }),
    );
    assert!(
        products
            .get(".sumwt")
            .channel_values()
            .iter()
            .all(|w| *w > 0.0)
    );
    for suffix in [
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".residual.tt0",
        ".residual.tt1",
        ".sumwt.tt0",
        ".sumwt.tt1",
        ".sumwt.tt2",
    ] {
        assert!(
            products
                .get(suffix)
                .values()
                .iter()
                .all(|value| *value == 0.0),
            "CASA writes blank Taylor moment {suffix}"
        );
    }
    assert!(
        products
            .get(".pb.tt0")
            .values()
            .iter()
            .any(|value| *value > 0.0)
    );
    assert!(!products.suffixes().contains(".image.tt0"));
    let restored = observation.try_image(
        "mvc-tiny-restored",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 0, "write_pb": true, "pblimit": -0.1,
            "restoration": true,
        }),
    );
    assert!(
        restored.is_err(),
        "a blank restored Taylor PSF cannot be fitted"
    );
    let directory = observation.measurement_set().parent().unwrap();
    assert!(
        !directory.join("mvc-tiny-restored.psf").exists(),
        "a refused family must not publish its staged cube"
    );
}
