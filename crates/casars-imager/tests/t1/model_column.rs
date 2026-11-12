// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard gridder, natural weighting, MFS Hogbom with the final model
//! predicted into `MODEL_DATA`.

use casa_ms::{MeasurementSet, VisibilityDataColumn};
use casa_types::{ArrayValue, Complex32, Value};
use serde_json::json;

use super::fixture::{Component, Observation, box_sum, component_pixel};

const POINT: Component = Component {
    offset_px: [24, 16],
    flux_jy: 1.0,
    fwhm_px: None,
    spectral_index: 0.0,
};
/// Five times the 1.0 mJy thermal image noise.
const THRESHOLD: &str = "0.005Jy";

#[test]
fn model_column_predicts_the_deconvolved_sky_at_every_sample() {
    let observation = Observation::synthesise(&[("point", POINT)]);
    let (_, products) = observation.image(
        "model-column",
        json!({
            "weighting": "natural",
            "deconvolver": "hogbom",
            "niter": 500,
            "threshold": THRESHOLD,
            "savemodel": "modelcolumn",
        }),
    );
    let model_jy = box_sum(&products.get(".model").pixels, component_pixel(POINT), 2);
    let samples = observation.data_and_model();
    assert_eq!(samples.len(), observation.stokes_i_samples());

    // DATA is the true sky plus noise and MODEL_DATA the deconvolved sky,
    // so the least-squares gain g = Σ D·M*/Σ|M|² is the ratio of injected
    // to model flux, with no phase: any error of scale, phase convention or
    // conjugation in the prediction moves it. The noise term is
    // σ/(S·√N) ≈ 1e-3.
    let (cross_re, cross_im, power) =
        samples
            .iter()
            .fold((0.0, 0.0, 0.0), |(re, im, power), (data, model)| {
                let (d_re, d_im) = (f64::from(data.re), f64::from(data.im));
                let (m_re, m_im) = (f64::from(model.re), f64::from(model.im));
                (
                    re + d_re * m_re + d_im * m_im,
                    im + d_im * m_re - d_re * m_im,
                    power + m_re * m_re + m_im * m_im,
                )
            });
    let gain = [cross_re / power, cross_im / power];
    let expected = POINT.flux_jy / model_jy;
    eprintln!(
        "T1 model-column: {} samples, model flux {model_jy:.5} Jy, gain {:.5} + {:.5}i",
        samples.len(),
        gain[0],
        gain[1]
    );
    assert!(
        (gain[0] - expected).abs() < 5.0e-3,
        "gain {gain:?}, injected/model flux {expected}"
    );
    assert!(gain[1].abs() < 5.0e-3, "gain {gain:?} has a phase");
}

/// CASA's standard direct GridFT applies the OR of correlation flags to
/// the MODEL_DATA prediction for a channel (CTB80, #1276). Both saving
/// after CLEAN and saving a persisted model follow that rule.
#[test]
fn a_standard_continuum_model_zeroes_both_hands_of_a_partly_flagged_channel() {
    let observation = Observation::synthesise_band(2, &[("point", POINT)]);
    let path = observation.measurement_set();
    let mut ms = MeasurementSet::open(path).unwrap();
    let row = 0;
    let ArrayValue::Bool(mut flags) = ms.flag_column().get(row).unwrap().clone() else {
        panic!("a boolean FLAG cell");
    };
    assert!(flags.iter().all(|flag| !flag));
    flags[[0, 0]] = true;
    ms.main_table_mut()
        .cell_accessor_mut(row, "FLAG")
        .unwrap()
        .set(Value::Array(ArrayValue::Bool(flags.clone())))
        .unwrap();
    ms.save().unwrap();
    drop(ms);

    let prefix = "partly-flagged-model";
    observation.image(prefix, json!({"niter": 10, "savemodel": "modelcolumn"}));
    let read_model = |ms: &MeasurementSet| {
        let ArrayValue::Complex32(model) = ms
            .data_column(VisibilityDataColumn::ModelData)
            .unwrap()
            .get(row)
            .unwrap()
            .clone()
        else {
            panic!("a complex MODEL_DATA cell");
        };
        assert_eq!(
            ms.flag_column().get(row).unwrap(),
            &ArrayValue::Bool(flags.clone())
        );
        model
    };
    let assert_model = |model: &ndarray::ArrayD<Complex32>| {
        for hand in 0..2 {
            assert_eq!(model[[hand, 0]], Complex32::default(), "hand {hand}");
            assert!(
                model[[hand, 1]].norm() > 0.1,
                "the unflagged channel predicts"
            );
        }
    };

    let mut ms = MeasurementSet::open(path).unwrap();
    let mut model = read_model(&ms);
    assert_model(&model);
    model[[0, 0]] = Complex32::new(17.0, -3.0);
    model[[1, 0]] = Complex32::new(17.0, -3.0);
    ms.main_table_mut()
        .cell_accessor_mut(row, "MODEL_DATA")
        .unwrap()
        .set(Value::Array(ArrayValue::Complex32(model)))
        .unwrap();
    ms.save().unwrap();
    drop(ms);

    observation.image(
        prefix,
        json!({
            "niter": 0, "calcpsf": false, "calcres": false, "restoration": false,
            "restart": true, "savemodel": "modelcolumn",
        }),
    );
    assert_model(&read_model(&MeasurementSet::open(path).unwrap()));
}
