// SPDX-License-Identifier: LGPL-3.0-or-later
//! Independent constant fits over three equal-weight channels (2, 2, 8).
//! One pass gives 4. Reweighting rejects 8 and leaves 2. A fourth
//! channel contains a line and must stay outside the fitting window.

use casa_core::{ArrayValue, Complex32, Value};
use casa_ms::{MeasurementSet, MsSelection};
use casa_mstransform::{
    ContinuumFitOptions, MsTransformRequest, MsTransformTaskOptions, TransformDataColumn,
    mstransform_task,
};
use ndarray::{ArrayD, IxDyn};

use super::common::{self, NUM_CHAN, NUM_CORR};

#[test]
fn continuum_modes_select_fit_channels_and_publish_models_or_residuals() {
    let temp = tempfile::tempdir().unwrap();
    let input = common::create_msexplore_spectrum_fixture_ms(temp.path(), false, &[]);
    let mut ms = MeasurementSet::open(&input).unwrap();
    let signal = |row: usize, corr: usize, channel: usize| {
        let base = (10 * row + corr) as f32;
        let signal = base
            + match channel {
                0 | 1 => 2.,
                2 => 8.,
                _ => 14.,
            };
        Complex32::new(signal, -0.5 * signal)
    };
    for row in 0..ms.main_table().row_count() {
        let mut cells = ms.main_table_mut().row_accessor_mut();
        let data =
            ArrayD::from_shape_fn(IxDyn(&[NUM_CORR, NUM_CHAN]), |at| signal(row, at[0], at[1]));
        cells
            .set_cell(row, "DATA", Value::Array(ArrayValue::Complex32(data)))
            .unwrap();
        // The tiny positive weights still define an ordinary equal-weight fit.
        // The last correlation has no admitted channels.
        let sigma = ArrayD::from_shape_fn(IxDyn(&[NUM_CORR]), |at| {
            if at[0] + 1 == NUM_CORR { 0. } else { 1e14 }
        });
        cells
            .set_cell(row, "SIGMA", Value::Array(ArrayValue::Float32(sigma)))
            .unwrap();
    }
    ms.save().unwrap();
    drop(ms);
    for (name, reweight, iterations, want_continuum, order, fit_spw, fitted) in [
        ("legacy", false, 5, true, 0, "0:0~2", Some(4.)),
        ("reweighted", true, 3, true, 0, "0:0~2", Some(2.)),
        ("residual", true, 3, false, 0, "0:0~2", Some(2.)),
        ("zero-passes", true, 0, false, 0, "0:0~2", None),
        ("reduce-order", true, 1, true, 8, "0:2", Some(8.)),
    ] {
        let output = temp.path().join(format!("{name}.ms"));
        let request = MsTransformRequest {
            input_ms: input.clone(),
            output_ms: output.clone(),
            spw: "0".into(),
            width: 1,
            data_column: TransformDataColumn::Data,
            selection: MsSelection::new(),
            keep_flags: true,
        };
        let options = MsTransformTaskOptions {
            continuum: Some(ContinuumFitOptions {
                fit_spw: fit_spw.into(),
                order,
                want_continuum,
                reweight,
                iterations,
                threads: 2,
            }),
            ..Default::default()
        };
        mstransform_task(&request, &options).unwrap();
        let product = MeasurementSet::open(&output).unwrap();
        assert_eq!(product.main_table().row_count(), 4, "{name}");
        for row in 0..product.main_table().row_count() {
            let column = product.main_table().column_accessor("DATA").unwrap();
            let values = column.array_cells_owned_uncached(&[row]).unwrap();
            let value = values[0].as_ref().unwrap();
            let ArrayValue::Complex32(values) = value else {
                panic!("complex DATA")
            };
            assert_eq!(values.shape(), &[NUM_CORR, NUM_CHAN]);
            for corr in 0..NUM_CORR {
                let fit = fitted
                    .filter(|_| corr + 1 != NUM_CORR)
                    .map_or(0., |v| (10 * row + corr) as f32 + v);
                let model = Complex32::new(fit, -0.5 * fit);
                for channel in 0..NUM_CHAN {
                    let expected = if want_continuum {
                        model
                    } else {
                        signal(row, corr, channel) - model
                    };
                    assert!(
                        (values[[corr, channel]] - expected).norm() < 1e-4,
                        "{name}: row {row}, correlation {corr}, channel {channel}"
                    );
                }
            }
        }
    }
}
