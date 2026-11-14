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

/// CASA reuses the cube image's rest-frequency label in its Taylor image
/// family, even though the Taylor WCS reference is the LSRK band midpoint.
/// These differ for topocentric source data without a SOURCE rest frequency.
#[test]
fn mvc_taylor_rest_frequency_follows_the_cube_label() {
    use casa_coordinates::CoordinateModel;
    use casa_types::measures::frequency::FrequencyRef;

    let observation = Observation::synthesise_band(6, &[("point", POINT)]);
    let mut ms = MeasurementSet::open(observation.measurement_set()).unwrap();
    ms.spectral_window_mut()
        .unwrap()
        .set_i32(0, "MEAS_FREQ_REF", FrequencyRef::TOPO.casacore_code())
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    let controls = json!({
        "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
        "nchan": 3, "niter": 0, "weighting": "natural", "write_pb": true,
        "restoration": false,
    });
    let (_, implicit) = observation.image("mvc-topo-rest-implicit", controls.clone());
    let rest = |products: &super::fixture::Products, suffix: &str| {
        let CoordinateModel::Spectral(axis) =
            products.get(suffix).image.coordinates().coordinate(2)
        else {
            panic!("spectral coordinate")
        };
        axis.rest_frequency()
    };
    let cube_rest = rest(&implicit, ".psf");
    let taylor_rest = rest(&implicit, ".psf.tt0");
    let taylor_reference = implicit.get(".psf.tt0").channel_frequency_hz(0);
    assert!(
        (cube_rest - taylor_reference).abs() > 1_000.0,
        "the test must distinguish the source-frame rest label and LSRK reference"
    );
    assert!(
        (cube_rest - taylor_rest).abs() < 1e-3,
        "cube rest {cube_rest}, Taylor rest {taylor_rest}"
    );
    let mut explicit = controls;
    explicit["restfreq"] = json!(["45GHz", "46GHz"]);
    let (_, explicit) = observation.image("mvc-topo-rest-explicit", explicit);
    assert_eq!(rest(&explicit, ".psf"), 45e9);
    assert_eq!(rest(&explicit, ".psf.tt0"), 45e9);
    for suffix in [".psf", ".psf.tt0"] {
        let CoordinateModel::Spectral(axis) =
            explicit.get(suffix).image.coordinates().coordinate(2)
        else {
            panic!("spectral coordinate")
        };
        assert_eq!(
            axis.rest_frequencies(),
            [45e9, 46e9],
            "{suffix} rest-frequency list"
        );
    }
}

/// CASA 6.7.6 on this weighted, wide-field fixture makes a cube model from
/// the Taylor model after each minor cycle, then grids that cube model to
/// form the next Taylor residual (#1069). Three one-iteration minor cycles
/// exercise more than one model handoff and an expansion reference that is
/// different from the Taylor image's WCS center.
#[test]
fn mvc_clean_refreshes_the_cube_from_the_taylor_model() {
    let observation = weighted_observation();
    for (iterations, majors, model, residual, cube) in [
        (
            1,
            2,
            [0.481_774_9_f32, -0.441_601_57],
            [4.416_162_f32, -0.089_114_144],
            [0.491_723_6_f32, 0.490_279_85, 0.488_836_23],
        ),
        (
            3,
            4,
            [1.305_975_3_f32, -1.178_515_7],
            [3.577_092_2_f32, -0.072_181_99],
            [1.332_531_3_f32, 1.328_670_6, 1.324_810_1],
        ),
    ] {
        let (summary, products) = observation.image(
            &format!("mvc-clean-{iterations}"),
            json!({
                "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
                "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
                "niter": iterations, "cycleniter": 1, "write_pb": true, "pblimit": -0.1,
            }),
        );
        assert_eq!(summary.actual_minor_iterations, iterations);
        assert_eq!(summary.major_cycles, majors);
        let close = |label: &str, actual: f32, expected: f32| {
            assert!(
                (actual - expected).abs() <= 0.001 * expected.abs(),
                "{label}: {actual} vs CASA {expected}"
            );
        };
        for term in 0..2 {
            close(
                &format!("model.tt{term}"),
                products.get(&format!(".model.tt{term}")).pixels[[152, 144]],
                model[term],
            );
            close(
                &format!("residual.tt{term}"),
                products.get(&format!(".residual.tt{term}")).pixels[[152, 144]],
                residual[term],
            );
        }
        for (channel, expected) in cube.into_iter().enumerate() {
            close(
                &format!("cube model channel {channel}"),
                products.get(".model").plane(channel)[[152, 144]],
                expected,
            );
        }
    }
}

/// The first resumed major cycle evaluates both supplied terms and uses
/// the saved channel and Taylor beams before it makes any minor update.
#[test]
fn mvc_start_model_uses_the_saved_cube_and_taylor_beams() {
    use super::fixture::Product;
    use casa_coordinates::CoordinateType;

    let observation = weighted_observation();
    let name = "mvc-startmodel";
    let controls = json!({
        "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
        "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
        "niter": 1, "cycleniter": 1, "write_pb": true, "pblimit": -0.1,
    });
    let mut initial_controls = controls.clone();
    initial_controls["mask"] = json!("box[[148pix,140pix],[156pix,148pix]]");
    let (_, initial) = observation.image(name, initial_controls);
    let root = observation.measurement_set().parent().unwrap();
    let path = |suffix: &str| root.join(format!("{name}.{suffix}"));
    let start = [root.join("seed.tt0"), root.join("seed.tt1")];
    for (term, target) in start.iter().enumerate() {
        std::fs::rename(path(&format!("model.tt{term}")), target).unwrap();
    }
    let point = [152, 144];
    let coefficients = start
        .each_ref()
        .map(|model| Product::open(model).pixels[point]);
    assert!(coefficients.iter().all(|value| value.abs() > 0.01));
    let mut cube_beam = casa_images::PagedImage::<f32>::open(path("pb")).unwrap();
    let spectral = cube_beam.coordinates().coordinate(
        cube_beam
            .coordinates()
            .find_coordinate(CoordinateType::Spectral)
            .unwrap(),
    );
    let reference = spectral.reference_value()[0];
    let increment = spectral.increment()[0];
    let pixel = spectral.reference_pixel()[0];
    let beam = [0.8_f32, 0.6, 0.4];
    for (channel, value) in beam.into_iter().enumerate() {
        cube_beam
            .put_slice(
                &ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1, 1, 1, 1]), value),
                &[point[0], point[1], 0, channel],
            )
            .unwrap();
    }
    cube_beam.save().unwrap();
    drop(cube_beam);
    let mut taylor_beam = casa_images::PagedImage::<f32>::open(path("pb.tt0")).unwrap();
    taylor_beam
        .put_slice(
            &ndarray::ArrayD::from_elem(ndarray::IxDyn(&[1, 1, 1, 1]), 0.9),
            &[point[0], point[1], 0, 0],
        )
        .unwrap();
    taylor_beam.save().unwrap();
    drop(taylor_beam);

    let mut resumed = controls;
    resumed["startmodel"] = json!(start);
    resumed["threshold"] = json!("100Jy");
    let (summary, products) = observation.image(name, resumed.clone());
    assert_eq!(summary.actual_minor_iterations, 0);
    assert_eq!(summary.major_cycles, 1);
    let old_mask = &initial.get(".mask").pixels;
    assert!(old_mask.iter().any(|&value| value == 0.0));
    assert!(old_mask.iter().any(|&value| value == 1.0));
    assert_eq!(products.get(".mask").pixels, *old_mask);
    for (channel, response) in beam.into_iter().enumerate() {
        let hz = reference + (channel as f64 - pixel) * increment;
        let model = (f64::from(coefficients[0])
            + ((hz - REFERENCE_HZ) / REFERENCE_HZ) * f64::from(coefficients[1]))
            as f32;
        let expected = model * response / 0.9;
        let actual = products.get(".model").plane(channel)[point];
        assert!(
            (actual - expected).abs() < 1e-6,
            "channel {channel}: {actual} vs {expected}"
        );
    }
    for (term, expected) in coefficients.into_iter().enumerate() {
        assert_eq!(
            products.get(&format!(".model.tt{term}")).pixels[point],
            expected
        );
    }
    assert_ne!(
        products.get(".residual").pixels[point],
        initial.get(".residual").pixels[point]
    );
    assert_eq!(
        products.get(".pb").pixels[point],
        initial.get(".pb").pixels[point],
        "new normal products use the freshly formed beam",
    );
    let refused = observation
        .try_image(name, resumed)
        .err()
        .expect("existing Taylor terms conflict");
    assert!(
        refused.contains("startmodel cannot be supplied"),
        "{refused}"
    );
    for term in 0..2 {
        std::fs::remove_dir_all(path(&format!("model.tt{term}"))).unwrap();
    }
    let reply = super::casa_return::json_run(
        root,
        json!({
            "vis": observation.measurement_set(), "imagename": name,
            "imsize": 256, "cell": "0.3arcsec", "backend": "cpu",
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 1, "cycleniter": 1, "write_pb": true, "pblimit": -0.1,
            "startmodel": start, "fullsummary": true,
        }),
    );
    let casa_provider_contracts::TaskReply::Succeeded { value, .. } = reply else {
        panic!("seeded tclean return: {reply:?}");
    };
    let row = |field| super::casa_return::plane_row(&value, [0, 0, 0], field)[0];
    let tt0_flux: f64 = Product::open(&path("model.tt0"))
        .pixels
        .iter()
        .map(|&v| f64::from(v))
        .sum();
    let start_flux: f64 = Product::open(&start[0])
        .pixels
        .iter()
        .map(|&v| f64::from(v))
        .sum();
    assert!((row("startModelFlux") - start_flux).abs() < 1e-6);
    assert!((row("modelFlux") - tt0_flux).abs() < 1e-6);
}

/// Without explicit startmodel, CASA predicts the first resumed residual
/// from the saved cube model and initializes CLEAN with the saved Taylor
/// coefficients. Editing them independently makes that distinction visible.
#[test]
fn mvc_restart_reads_each_saved_model_on_its_own_grid() {
    let observation = weighted_observation();
    let name = "mvc-restart";
    let controls = json!({
        "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
        "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
        "niter": 1, "cycleniter": 1, "write_pb": true, "pblimit": -0.1,
    });
    let (_, dirty) = observation.image("mvc-before-clean", {
        let mut values = controls.clone();
        values["niter"] = json!(0);
        values
    });
    let mut boxed = controls.clone();
    boxed["mask"] = json!("box[[148pix,140pix],[156pix,148pix]]");
    let (_, first) = observation.image(name, boxed);
    let root = observation.measurement_set().parent().unwrap();
    let point = [152, 144];
    let (scale, taylor_scales) = (0.6, [0.8, 0.5]);
    for (suffix, scale) in [
        (".model", scale),
        (".model.tt0", taylor_scales[0]),
        (".model.tt1", taylor_scales[1]),
    ] {
        let mut image =
            casa_images::PagedImage::<f32>::open(root.join(format!("{name}{suffix}"))).unwrap();
        let pixels = image.get().unwrap().mapv(|value| value * scale);
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.save().unwrap();
    }
    let mut stopped = controls;
    stopped["threshold"] = json!("100Jy");
    let (summary, resumed) = observation.image(name, stopped);
    assert_eq!(summary.actual_minor_iterations, 0);
    assert_eq!(summary.major_cycles, 1);
    assert_eq!(resumed.get(".mask").pixels, first.get(".mask").pixels);
    for channel in 0..3 {
        let model = first.get(".model").plane(channel)[point];
        assert!(model > 0.01);
        assert_eq!(resumed.get(".model").plane(channel)[point], scale * model);
        let uncleaned = dirty.get(".residual").plane(channel)[point];
        let cleaned = first.get(".residual").plane(channel)[point];
        let actual = resumed.get(".residual").plane(channel)[point];
        let expected = uncleaned + scale * (cleaned - uncleaned);
        assert!(
            (actual - expected).abs() < 1e-5,
            "{channel}: {actual} vs {expected}"
        );
    }
    for (term, scale) in taylor_scales.into_iter().enumerate() {
        let suffix = format!(".model.tt{term}");
        let original = first.get(&suffix).pixels[point];
        assert!(original.abs() > 0.01);
        assert_eq!(resumed.get(&suffix).pixels[point], scale * original);
    }
}

/// Skipping makePSF preserves the cube and Taylor PSFs, beams and weights.
#[test]
fn mvc_restart_keeps_the_saved_psf_and_primary_beam_families() {
    use super::restart::{assert_unchanged, snapshot};
    use casa_images::PagedImage;

    let observation = weighted_observation();
    let name = "mvc-saved-psf";
    let mut controls = json!({
        "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
        "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
        "niter": 1, "cycleniter": 1, "write_pb": true, "pblimit": -0.1,
    });
    let (_, seed) = observation.image(name, controls.clone());
    let path = |suffix: &str| observation.scratch(&format!("{name}{suffix}"));
    for (suffix, scale) in [(".pb", 0.8), (".pb.tt0", 0.7), (".psf.tt1", 0.5)] {
        let mut image = PagedImage::<f32>::open(path(suffix)).unwrap();
        let pixels = image.get().unwrap().mapv(|value| value * scale);
        image.put_slice(&pixels, &[0; 4]).unwrap();
        image.save().unwrap();
    }
    let saved = [
        ".pb",
        ".pb.tt0",
        ".psf",
        ".psf.tt0",
        ".psf.tt1",
        ".psf.tt2",
        ".sumwt",
        ".sumwt.tt0",
        ".sumwt.tt1",
        ".sumwt.tt2",
    ]
    .map(path);
    let before = saved.each_ref().map(|path| snapshot(path));
    controls["calcpsf"] = json!(false);
    controls["threshold"] = json!("100Jy");
    let (summary, resumed) = observation.image(name, controls);
    assert_eq!(summary.actual_minor_iterations, 0);
    assert_eq!(summary.major_cycles, 1);
    assert_eq!(resumed.get(".mask").pixels, seed.get(".mask").pixels);
    for (path, before) in saved.iter().zip(before) {
        assert_unchanged(&before, &snapshot(path));
    }
    let point = [152, 144];
    for channel in 0..3 {
        let expected = seed.get(".residual").plane(channel)[point] * (0.7 / 0.8);
        let actual = resumed.get(".residual").plane(channel)[point];
        assert!(
            (actual - expected).abs() < 1e-5,
            "{channel}: {actual} vs {expected}"
        );
    }
    // The restored image uses the inverse of the saved Taylor Hessian.
    // The same model and restoring beam cancel between the two runs.
    let principal_tt1 = |products: &super::fixture::Products, off_diagonal: f64| {
        let centre = [128, 128];
        let a = f64::from(seed.get(".psf.tt0").pixels[centre]);
        let b = f64::from(seed.get(".psf.tt1").pixels[centre]) * off_diagonal;
        let c = f64::from(seed.get(".psf.tt2").pixels[centre]);
        let r0 = f64::from(products.get(".residual.tt0").pixels[point]);
        let r1 = f64::from(products.get(".residual.tt1").pixels[point]);
        a * (a * r1 - b * r0) / (a * c - b * b)
    };
    let expected = principal_tt1(&resumed, 0.5) - principal_tt1(&seed, 1.0);
    let recomputed = principal_tt1(&resumed, 1.0) - principal_tt1(&seed, 1.0);
    assert!((recomputed - expected).abs() > 0.1);
    let actual = f64::from(resumed.get(".image.tt1").pixels[point])
        - f64::from(seed.get(".image.tt1").pixels[point]);
    assert!((actual - expected).abs() < 1e-5, "{actual} vs {expected}");
}

/// CASA copies the common Taylor PB support onto MVC's restored and
/// residual Taylor images when pblimit is positive, while cleaning the
/// source through repeated cube-major cycles (#1069).
#[test]
fn mvc_clean_with_positive_pblimit_uses_casa_taylor_support() {
    let observation = weighted_observation();
    for (iterations, model) in [
        (1, [0.481_774_9_f32, -0.441_406_25]),
        (3, [1.305_981_4_f32, -1.177_929_6]),
    ] {
        let (summary, products) = observation.image(
            &format!("mvc-clean-positive-pb-{iterations}"),
            json!({
                "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
                "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
                "niter": iterations, "cycleniter": 1, "write_pb": true, "pblimit": 0.8,
            }),
        );
        assert_eq!(summary.actual_minor_iterations, iterations);
        assert_eq!(summary.major_cycles, iterations + 1);
        for (term, expected) in model.into_iter().enumerate() {
            let actual = products.get(&format!(".model.tt{term}")).pixels[[152, 144]];
            assert!(
                (actual - expected).abs() <= 0.001 * expected.abs(),
                "iterations {iterations}, term {term}: native {actual}, CASA {expected}"
            );
        }
        let pb_mask = products.get(".pb.tt0").mask().expect("Taylor PB mask");
        assert_eq!(pb_mask.iter().filter(|value| **value).count(), 10_437);
        for suffix in [".residual.tt0", ".residual.tt1", ".image.tt0", ".image.tt1"] {
            assert_eq!(
                products.get(suffix).mask().as_ref(),
                Some(&pb_mask),
                "{suffix}"
            );
        }
    }
}

/// MVC corrects both restored Taylor terms with the shared weighted PB.
/// CASA copies its mask even when a negative pblimit leaves the restored
/// Taylor images unmasked. `pbcor` also touches a zero, unmasked `.pb.tt1`.
#[test]
fn mvc_corrects_both_taylor_images_with_the_shared_primary_beam() {
    let observation = weighted_observation();
    for (name, pblimit) in [("negative", -0.8), ("positive", 0.8)] {
        let (summary, products) = observation.image(
            &format!("mvc-pbcor-{name}"),
            json!({
                "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
                "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
                "niter": 1, "cycleniter": 1, "pbcor": true, "pblimit": pblimit,
            }),
        );
        assert_eq!(summary.actual_minor_iterations, 1);
        assert_eq!(summary.major_cycles, 2);
        let beam = products.get(".pb.tt0");
        let mask = beam.mask().expect("Taylor PB mask");
        assert_eq!(mask.iter().filter(|value| **value).count(), 10_437);
        for term in 0..2 {
            let restored = products.get(&format!(".image.tt{term}"));
            let corrected = products.get(&format!(".image.tt{term}.pbcor"));
            assert_eq!(corrected.mask().as_ref(), Some(&mask));
            assert_eq!(restored.mask().as_ref(), (pblimit > 0.0).then_some(&mask));
            for (index, &pb) in beam.pixels.indexed_iter() {
                if mask[[index.0, index.1, 0, 0]] {
                    let expected = restored.pixels[index] / pb;
                    let actual = corrected.pixels[index];
                    assert!((actual - expected).abs() <= 1e-6 * expected.abs());
                }
            }
        }
        let pb1 = products.get(".pb.tt1");
        assert!(pb1.pixels.iter().all(|value| *value == 0.0));
        assert!(pb1.mask().is_none());
        assert!(!products.suffixes().contains(".image.pbcor"));
        assert!(!products.suffixes().contains(".alpha.pbcor"));
    }
}

/// W-projected cube passes supply the two-term Taylor solver with PSFs and
/// refresh the cube from its updated model on each major cycle.
#[test]
fn mvc_w_projection_refreshes_the_cube_and_taylor_residual() {
    let observation = weighted_observation();
    let controls = json!({
        "specmode": "mvc", "gridder": "wproject", "wprojplanes": 4,
        "deconvolver": "mtmfs", "nterms": 2, "nchan": 3, "reffreq": "45GHz",
        "weighting": "natural", "niter": 0, "cycleniter": 1, "pblimit": -0.1,
    });
    let (_, dirty) = observation.image("mvc-w-dirty", controls.clone());
    let mut clean_controls = controls;
    clean_controls["niter"] = json!(3);
    let (summary, clean) = observation.image("mvc-w-clean", clean_controls);
    assert_eq!(summary.actual_minor_iterations, 3);
    assert_eq!(summary.major_cycles, 4);
    assert!(!clean.suffixes().contains(".weight"));
    let point = [152, 144];
    assert!(clean.get(".model.tt0").pixels[point] > 0.0);
    for channel in 0..3 {
        let dirty_peak = dirty.get(".residual").plane(channel)[point];
        let residual = clean.get(".residual").plane(channel)[point];
        assert!((0.0..0.85 * dirty_peak).contains(&residual));
        let model = clean.get(".model").plane(channel)[point];
        assert!(model > 0.0);
    }
    let dirty_peak = dirty.get(".residual.tt0").pixels[point];
    assert!((0.0..0.85 * dirty_peak).contains(&clean.get(".residual.tt0").pixels[point]));
}

/// The mosaic cube uses channel-dependent primary beams, then the Taylor
/// images divide by one common beam without a separate corrected alpha.
#[test]
fn mvc_mosaic_cleans_and_corrects_both_terms_with_the_common_beam() {
    let mut setup = super::mosaic::setup();
    setup.fields = vec![super::fixture::PHASE_CENTER_RAD];
    setup.spectral_windows[0].channel_width_hz = 4.0e9;
    setup.spectral_windows[0].channel_count = 3;
    setup.noise_jy = 0.0;
    let observation = Observation::synthesise_setup(setup, &[("point", POINT)]);
    let (summary, products) = observation.image(
        "mvc-mosaic",
        json!({
            "specmode": "mvc", "gridder": "mosaic", "normtype": "flatnoise",
            "deconvolver": "mtmfs", "nterms": 2, "nchan": 3, "reffreq": "54GHz",
            "weighting": "natural", "niter": 3, "cycleniter": 1,
            "pblimit": 0.1, "pbcor": true,
        }),
    );
    assert_eq!(summary.actual_minor_iterations, 3);
    assert_eq!(summary.major_cycles, 4);
    assert!(products.suffixes().contains(".weight"));
    assert!(!products.suffixes().contains(".weight.tt0"));
    assert!(!products.suffixes().contains(".alpha.pbcor"));
    let source = [280, 272];
    let beam = products.get(".pb.tt0");
    let mean = beam.pixels[source];
    assert!((0.1..0.99).contains(&mean), "{mean}");
    let mask = beam.mask().expect("Taylor PB mask");
    let mut restored = [0.0; 2];
    for (term, value) in restored.iter_mut().enumerate() {
        let raw = products.get(&format!(".image.tt{term}"));
        let corrected = products.get(&format!(".image.tt{term}.pbcor"));
        assert_eq!(corrected.mask().as_ref(), Some(&mask));
        *value = raw.pixels[source];
        assert!((corrected.pixels[source] - *value / mean).abs() < 1e-6);
    }
    assert!((products.get(".alpha").pixels[source] - restored[1] / restored[0]).abs() < 1e-6);
}

/// CASA does not create a cube model when the initial threshold check
/// stops before any minor cycle, but does publish the Taylor models (#1069).
#[test]
fn mvc_clean_stopping_before_minor_does_not_publish_cube_model() {
    let observation = weighted_observation();
    let (summary, products) = observation.image(
        "mvc-clean-stopped",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 3, "cycleniter": 1, "threshold": "100Jy",
            "write_pb": true, "pblimit": -0.1,
        }),
    );
    assert_eq!(summary.actual_minor_iterations, 0);
    assert_eq!(summary.major_cycles, 1);
    assert!(!products.suffixes().contains(".model"));
    for suffix in [".model.tt0", ".model.tt1"] {
        assert!(
            products
                .get(suffix)
                .pixels
                .iter()
                .all(|value| *value == 0.0)
        );
    }
    let _ = products.get(".mask");
}

/// CASA publishes the Taylor deconvolver's user mask for cleaned MVC,
/// including the box edges, rather than a full-plane mask (#1069).
#[test]
fn mvc_clean_publishes_the_taylor_user_box_mask() {
    let observation = weighted_observation();
    let (_, products) = observation.image(
        "mvc-clean-box",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 1, "cycleniter": 1, "write_pb": true, "pblimit": -0.1,
            "mask": "140,130,160,150",
        }),
    );
    let mask = &products.get(".mask").pixels;
    for ((x, y), value) in mask.indexed_iter() {
        let inside = (140..=160).contains(&x) && (130..=150).contains(&y);
        assert_eq!(*value, f32::from(inside), "mask at [{x}, {y}]");
    }
}

/// A spectral range is prepared on the cube, while MVC cleans one Taylor
/// plane. Refuse it before imaging until that mask can be prepared for the
/// Taylor view (#1069).
#[test]
fn mvc_clean_refuses_spectrally_ranged_user_masks() {
    let observation = weighted_observation();
    let result = observation.try_image(
        "mvc-clean-ranged-mask",
        json!({
            "specmode": "mvc", "deconvolver": "mtmfs", "nterms": 2,
            "nchan": 3, "reffreq": "45GHz", "weighting": "natural",
            "niter": 1, "write_pb": true, "pblimit": -0.1,
            "mask": "box[[140pix,130pix],[160pix,150pix]],range=[1pix,1pix]",
        }),
    );
    assert!(
        matches!(result, Err(error) if error.contains("spatial-only") && error.contains("#1069"))
    );
}
