// SPDX-License-Identifier: LGPL-3.0-or-later

//! Standard-gridder MFS with the final model
//! predicted into `MODEL_DATA`.

use casa_ms::{MeasurementSet, VisibilityDataColumn};
use casa_types::{ArrayValue, Complex32, Value};
use serde_json::json;

use super::fixture::{Component, Observation, Product, box_sum, component_pixel};

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

#[test]
fn fresh_prediction_publishes_the_start_model_without_a_residual() {
    let seed = Observation::synthesise_band(2, &[("point", POINT)]);
    let (_, products) = seed.image(
        "seed",
        json!({ "deconvolver": "hogbom", "niter": 2, "savemodel": "modelcolumn" }),
    );
    let model_path = seed.scratch("seed.model");
    let expected_model = &products.get(".model").pixels;
    let expected_visibilities = seed.data_and_model();
    assert!(expected_model.iter().any(|value| *value > 0.0));
    let psf_only = Observation::synthesise_band(2, &[("point", POINT)]);
    psf_only.image(
        "unseeded-psf",
        json!({ "niter": 0, "calcpsf": true, "calcres": false, "restoration": false }),
    );
    assert!(!psf_only.scratch("unseeded-psf.model").exists());

    for (solver, saver) in [
        ("hogbom", "modelcolumn"),
        ("mem", "modelcolumn"),
        ("hogbom", "none"),
    ] {
        let replay = Observation::synthesise_band(2, &[("point", POINT)]);
        let (_, products) = replay.image(
            "prediction",
            json!({
                "deconvolver": solver,
                "startmodel": model_path,
                "niter": 0,
                "calcpsf": true,
                "calcres": false,
                "restoration": false,
                "savemodel": saver,
            }),
        );
        assert_eq!(
            &products.get(".model").pixels,
            expected_model,
            "{solver}, {saver}"
        );
        assert!(!replay.scratch("prediction.residual").exists());
        assert!(!replay.scratch("prediction.image").exists());
        assert_eq!(&Product::open(&model_path).pixels, expected_model);
        if saver == "modelcolumn" {
            let visibilities = replay.data_and_model();
            assert_eq!(visibilities.len(), expected_visibilities.len());
            for ((data, model), (expected_data, expected_model)) in
                visibilities.iter().zip(&expected_visibilities)
            {
                assert_eq!(data, expected_data);
                assert!((model - expected_model).norm() < 1e-5, "{solver}");
            }
        }
    }
}

#[test]
fn fresh_empty_prediction_zeroes_only_selected_model_cells() {
    let observation = Observation::synthesise_band(2, &[("point", POINT)]);
    observation.image(
        "seed-column",
        json!({ "niter": 2, "savemodel": "modelcolumn" }),
    );

    let path = observation.measurement_set();
    let mut ms = MeasurementSet::open(path).unwrap();
    let rows = ms.main_table().row_count();
    assert!(rows > 0);
    let ArrayValue::Complex32(mut models) = ms
        .data_column(VisibilityDataColumn::ModelData)
        .unwrap()
        .get(0)
        .unwrap()
        .clone()
    else {
        panic!("a complex MODEL_DATA cell");
    };
    let sentinel = Complex32::new(17.0, -3.0);
    models.fill(sentinel);
    for row in 0..rows {
        ms.main_table_mut()
            .cell_accessor_mut(row, "MODEL_DATA")
            .unwrap()
            .set(Value::Array(ArrayValue::Complex32(models.clone())))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);

    let (_, products) = observation.image(
        "empty-prediction",
        json!({
            "deconvolver": "hogbom",
            "spw": "0:0",
            "niter": 0,
            "calcpsf": true,
            "calcres": false,
            "restoration": false,
            "write_pb": true,
            "savemodel": "modelcolumn",
        }),
    );
    for suffix in [".psf", ".pb", ".sumwt"] {
        products.get(suffix);
    }
    for suffix in [".model", ".residual", ".image"] {
        assert!(
            !observation
                .scratch(&format!("empty-prediction{suffix}"))
                .exists()
        );
    }
    let ms = MeasurementSet::open(path).unwrap();
    let column = ms.data_column(VisibilityDataColumn::ModelData).unwrap();
    for row in 0..rows {
        let ArrayValue::Complex32(models) = column.get(row).unwrap() else {
            panic!("a complex MODEL_DATA cell");
        };
        for hand in 0..models.shape()[0] {
            assert_eq!(
                models[[hand, 0]],
                Complex32::default(),
                "row {row}, hand {hand}"
            );
            assert_eq!(models[[hand, 1]], sentinel, "row {row}, hand {hand}");
        }
    }
}

#[test]
fn mem_writes_the_solved_and_replayed_model_prediction() {
    let observation = Observation::synthesise_band(2, &[("point", POINT)]);
    let controls = json!({
        "deconvolver":"mem", "niter":2, "savemodel":"modelcolumn",
        "cyclefactor":0.0001, "minpsffraction":0.0
    });
    let (run, _) = observation.image("mem-column", controls.clone());
    assert!(
        run.visibility_products.is_some(),
        "MEM must write its prediction"
    );
    let solved = observation.data_and_model();
    assert!(solved.iter().any(|(_, model)| model.im.abs() > 0.01));

    let replay = Observation::synthesise_band(2, &[("point", POINT)]);
    let mut controls = controls;
    controls["niter"] = json!(0);
    controls["startmodel"] = json!(observation.scratch("mem-column.model"));
    let (run, _) = replay.image("mem-column-replay", controls.clone());
    assert!(
        run.visibility_products.is_some(),
        "MEM must write its prediction"
    );
    let predicted = replay.data_and_model();
    assert_eq!(predicted.len(), solved.len());
    for ((data, model), (replay_data, replay_model)) in solved.iter().zip(&predicted) {
        assert_eq!(data, replay_data);
        assert!((model - replay_model).norm() < 1e-5);
    }
    let marker = Complex32::new(17.0, -3.0);
    let mut ms = MeasurementSet::open(replay.measurement_set()).unwrap();
    let column = VisibilityDataColumn::ModelData;
    let ArrayValue::Complex32(mut model) = ms.data_column(column).unwrap().get(0).unwrap().clone()
    else {
        panic!("a complex MODEL_DATA cell");
    };
    model.fill(marker);
    ms.main_table_mut()
        .cell_accessor_mut(0, "MODEL_DATA")
        .unwrap()
        .set(Value::Array(ArrayValue::Complex32(model)))
        .unwrap();
    ms.save().unwrap();
    drop(ms);
    controls["spw"] = json!("0:0");
    replay.image("mem-column-selected", controls);
    let ms = MeasurementSet::open(replay.measurement_set()).unwrap();
    let ArrayValue::Complex32(model) = ms.data_column(column).unwrap().get(0).unwrap().clone()
    else {
        panic!("a complex MODEL_DATA cell");
    };
    for hand in 0..2 {
        assert_ne!(model[[hand, 0]], marker, "write the selected channel");
        assert_eq!(model[[hand, 1]], marker, "keep the unselected channel");
    }
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
