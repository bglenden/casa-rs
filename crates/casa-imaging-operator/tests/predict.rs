// SPDX-License-Identifier: LGPL-3.0-or-later
//! Model-column prediction (CASA `GridFT::get` then
//! `FTMachine::interpolateFrequencyFromgrid`): direct mapping equals the
//! degridded value at each native channel with CASA's flag rules; linear
//! mapping reproduces a spectrum linear in frequency exactly, a flat
//! spectrum across wide output channels, extrapolates native channels in
//! CASA's `matchChannel` halo and predicts zero beyond it.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, CpuBackend, GridBackend, GridPrecision, MeasurementOperator, ModelImages, ModelPlane,
    ModelPrescale, NativeRow, PredictionScratch, PreparedModelGrids, RowContext, SampleBuffer,
    SpectralAxis, SpectralKernel, SpectralResampler, WeightingGeneration, Work,
};
use common::{IMAGE, operator};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];
const UVW_M: [f64; 3] = [37.0, -21.0, 4.0];

fn context() -> RowContext {
    RowContext {
        time_s: 0.0,
        antennas: [0, 1],
        antenna_types: [0, 0],
        parallactic_angle_rad: [0.0, 0.0],
        field: 0,
        spectral_window: 0,
    }
}

/// A point source of `flux[plane]` at the reference pixel on every plane.
fn point_model(operator: &MeasurementOperator, flux: &[f32]) -> PreparedModelGrids {
    let model = ModelImages {
        first_plane: 0,
        planes: flux
            .iter()
            .map(|flux| {
                let mut image = Array2::zeros((IMAGE, IMAGE));
                image[(IMAGE / 2, IMAGE / 2)] = *flux;
                ModelPlane {
                    images: vec![image],
                }
            })
            .collect(),
    };
    operator
        .prepare_model(&model, ModelPrescale::Unit)
        .expect("model")
}

fn predict(
    resampler: &SpectralResampler,
    operator: &MeasurementOperator,
    model: &PreparedModelGrids,
    frequencies_hz: &[f64],
    flags: &[bool],
    row_flag: bool,
    phase_shift_m: f64,
) -> Vec<Complex32> {
    let values = vec![Complex32::new(1.0, 0.0); frequencies_hz.len() * 2];
    let weights = vec![1.0; frequencies_hz.len() * 2];
    let row = NativeRow {
        uvw_m: UVW_M,
        phase_shift_m,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz,
        values: &values,
        weights: &weights,
        flags,
        row_flag,
        context: context(),
    };
    let mut out = vec![Complex32::new(9.0, 9.0); values.len()];
    resampler
        .predict_row(
            operator,
            &mut CpuBackend::new(),
            model,
            &row,
            &mut PredictionScratch::default(),
            &mut out,
        )
        .expect("prediction");
    out
}

#[test]
fn direct_prediction_is_the_degridded_value_at_each_native_channel() {
    let basis = Basis::Constant;
    let operator = operator(GridPrecision::F64, basis, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::direct(basis).expect("direct");
    // An off-centre point gives every channel a different visibility.
    let model = {
        let mut image = Array2::zeros((IMAGE, IMAGE));
        image[(IMAGE / 2 + 3, IMAGE / 2 - 5)] = 1.3;
        operator
            .prepare_model(
                &ModelImages {
                    first_plane: 0,
                    planes: vec![ModelPlane {
                        images: vec![image],
                    }],
                },
                ModelPrescale::Unit,
            )
            .expect("model")
    };
    let frequencies = [1.00e9, 1.05e9, 1.10e9];
    let flags = [false, false, false, true, false, false];
    let predicted = predict(
        &resampler,
        &operator,
        &model,
        &frequencies,
        &flags,
        false,
        0.013,
    );
    for (channel, frequency_hz) in frequencies.iter().enumerate() {
        let values = vec![Complex32::default(); 2];
        let mut buffer = SampleBuffer::new(2);
        let row = NativeRow {
            uvw_m: UVW_M,
            phase_shift_m: 0.013,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz: std::slice::from_ref(frequency_hz),
            values: &values,
            weights: &[1.0, 1.0],
            flags: &[false, false],
            row_flag: false,
            context: context(),
        };
        resampler
            .place(
                &operator,
                &WeightingGeneration::Natural { taper: None },
                &row,
                &mut buffer,
            )
            .expect("place");
        let mut expected = vec![Complex32::default(); 2];
        CpuBackend::new()
            .apply(
                &buffer.block(),
                operator.cf(),
                Work::Predict {
                    model: &model,
                    out: &mut expected,
                },
            )
            .expect("predict");
        for pol in 0..2 {
            let actual = predicted[channel * 2 + pol];
            if flags[channel * 2 + pol] {
                assert_eq!(actual, Complex32::default(), "flagged correlation");
            } else {
                assert_eq!(actual, expected[pol], "channel {channel} pol {pol}");
            }
        }
    }
    assert!(
        predict(
            &resampler,
            &operator,
            &model,
            &frequencies,
            &[false; 6],
            true,
            0.013
        )
        .iter()
        .all(|value| *value == Complex32::default()),
        "a flagged row predicts zero"
    );
}

/// The visibility of a unit point at the phase centre without a phase
/// shift: constant over uv and frequency, it is the operator's correction
/// at the centre.
fn unit_point(operator: &MeasurementOperator, resampler: &SpectralResampler) -> Complex64 {
    let planes = resampler.basis().planes() as usize;
    let model = point_model(operator, &vec![1.0; planes]);
    let value = predict(
        resampler,
        operator,
        &model,
        &[1.0003e9, 1.0013e9],
        &[false; 4],
        false,
        0.0,
    )[0];
    Complex64::new(f64::from(value.re), f64::from(value.im))
}

#[test]
fn linear_prediction_reproduces_a_spectrum_linear_in_frequency() {
    // Output and native channels both 1 MHz wide, offset by a quarter
    // channel; CASA interpolates between output-channel centres.
    let axis = SpectralAxis::new(1.0e9, 1.0e6, 16).expect("axis");
    let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let fluxes = (0..16).map(|k| 1.0 + 0.25 * k as f32).collect::<Vec<_>>();
    let model = point_model(&operator, &fluxes);
    let native = (0..14)
        .map(|channel| 1.0e9 + 0.25e6 + channel as f64 * 1.0e6)
        .collect::<Vec<_>>();
    let predicted = predict(
        &resampler,
        &operator,
        &model,
        &native,
        &vec![false; native.len() * 2],
        false,
        0.0,
    );
    let unit = unit_point(&operator, &resampler);
    for (channel, frequency_hz) in native.iter().enumerate() {
        // flux(ν) = 1 + 0.25·(ν − 1 GHz)/1 MHz, the line through the centres,
        // up to the highest output channel the row maps to (13). Beyond its
        // centre CASA interpolates toward output channel 14, which this row
        // did not degrid and so holds zero.
        let pixel = (frequency_hz - 1.0e9) / 1.0e6;
        let flux = if pixel <= 13.0 {
            1.0 + 0.25 * pixel
        } else {
            (1.0 + 0.25 * 13.0) * (14.0 - pixel)
        };
        let expected = unit * flux;
        let actual = predicted[channel * 2];
        let error = (Complex64::new(f64::from(actual.re), f64::from(actual.im)) - expected).norm();
        assert!(
            error < 1.0e-5 * expected.norm(),
            "channel {channel}: {actual} vs {expected}"
        );
    }
}

#[test]
fn linear_prediction_extrapolates_natives_in_casa_halo_and_zeroes_the_rest() {
    // Two 0.1 GHz output channels at 1.05 and 1.15 GHz on a native spacing
    // of 0.05 GHz (the row's first two channels): CASA's fine grid holds
    // each channel's value twice, at 1.025/1.075 and 1.125/1.175 GHz.
    // `matchChannel` keeps an off-axis native (chanMap −2) within
    // (ν(0) − 2Δ, ν(0) + Δ/2) = (0.95, 1.075) or (ν(2) − Δ/2, ν(2) + 2Δ) =
    // (1.225, 1.35) GHz, ν(2) being the world frequency of pixel nchan.
    let axis = SpectralAxis::new(1.05e9, 0.1e9, 2).expect("axis");
    let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let model = point_model(&operator, &[2.0, 5.0]);
    let native = [0.94e9, 0.99e9, 1.06e9, 1.11e9, 1.21e9, 1.23e9, 1.36e9];
    // Off axis below the halo; low halo (end-pair extrapolation of channel 0);
    // channel 0; between the channels (0.7 of the way from 1.075 to 1.125);
    // past the last channel but short of the halo; high halo; past it.
    let expected = [0.0, 2.0, 2.0, 2.0 + 0.7 * 3.0, 0.0, 5.0, 0.0];
    let predicted = predict(
        &resampler,
        &operator,
        &model,
        &native,
        &[false; 14],
        false,
        0.0,
    );
    let unit = unit_point(&operator, &resampler);
    for (channel, flux) in expected.iter().enumerate() {
        let actual = predicted[channel * 2];
        let actual = Complex64::new(f64::from(actual.re), f64::from(actual.im));
        let expected = unit * *flux;
        assert!(
            (actual - expected).norm() <= 1.0e-5 * unit.norm() * 5.0,
            "native {channel} at {} Hz: {actual} vs {expected}",
            native[channel]
        );
    }
}

/// CASA degrids the output channels `FTMachine::getInterpolateArrays` leaves
/// unflagged: with `chanMap` −1 replaced by its maximum, from its minimum to
/// its maximum, from channel 0 when a native lies in the halo (−2) or the
/// row maps to one output channel, every channel when every native lies in
/// the halo, and none when nothing maps (`GridFT::get`). An output channel
/// outside that range predicts zero.
#[test]
fn linear_prediction_degrids_the_casa_channel_range() {
    // Ten 1 MHz output channels centred on 1003 … 1012 MHz.
    let axis = SpectralAxis::new(1.003e9, 1.0e6, 10).expect("axis");
    let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let model = point_model(&operator, &[1.0; 10]);
    let on_axis = predict(
        &resampler,
        &operator,
        &model,
        &[1.0043e9, 1.0053e9],
        &[false; 4],
        false,
        0.0,
    )[0];
    let unit = Complex64::new(f64::from(on_axis.re), f64::from(on_axis.im));
    assert!(unit.norm() > 0.5, "a unit point predicts about one: {unit}");
    for (natives, expected) in [
        // chanMap [−2, 7, −2]: channels 0 … 7; 1000 MHz extrapolates from
        // channels 0 and 1, 1020 MHz from 8 and 9, which are not degridded.
        (&[1.000e9, 1.010e9, 1.020e9][..], &[1.0, 1.0, 0.0][..]),
        // Both natives map to channel 4: channels 0 … 4, so 1006.55 MHz
        // interpolates channel 3 (CASA's fine grid is five points wide).
        (&[1.00655e9, 1.00675e9], &[1.0, 1.0]),
        // Every native in the low halo: every channel.
        (&[0.9995e9, 1.0015e9], &[1.0, 1.0]),
        // chanMap [−2, −1]: nothing maps, nothing is degridded.
        (&[1.030e9, 1.040e9], &[0.0, 0.0]),
    ] {
        let predicted = predict(
            &resampler,
            &operator,
            &model,
            natives,
            &vec![false; natives.len() * 2],
            false,
            0.0,
        );
        for (channel, flux) in expected.iter().enumerate() {
            let actual = predicted[channel * 2];
            let actual = Complex64::new(f64::from(actual.re), f64::from(actual.im));
            assert!(
                (actual - unit * *flux).norm() <= 1.0e-5 * unit.norm(),
                "natives {natives:?}, native {channel}: {actual} vs {}",
                unit * *flux
            );
        }
    }
}

#[test]
fn wide_output_channels_reproduce_a_flat_spectrum_and_unmapped_channels_predict_zero() {
    // Output channels three native channels wide: CASA's fine grid repeats
    // each output value three times.
    let axis = SpectralAxis::new(1.0e9, 3.0e6, 4).expect("axis");
    let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let model = point_model(&operator, &[2.0; 4]);
    // Native channels from below the first output channel to above the
    // last, off CASA's halo boundaries (see the halo law above): only
    // 0.9982 GHz of those off the axis lies in the halo, (0.998, 1.0005).
    let native = (0..16)
        .map(|channel| 0.9942e9 + channel as f64 * 1.0e6)
        .collect::<Vec<_>>();
    let predicted = predict(
        &resampler,
        &operator,
        &model,
        &native,
        &vec![false; native.len() * 2],
        false,
        0.0,
    );
    let unit = unit_point(&operator, &resampler);
    for (channel, frequency_hz) in native.iter().enumerate() {
        let actual = predicted[channel * 2];
        let in_halo = (0.998e9..1.0005e9).contains(frequency_hz);
        if axis.nearest_channel(*frequency_hz).is_none() && !in_halo {
            assert_eq!(actual, Complex32::default(), "unmapped channel {channel}");
            continue;
        }
        let expected = unit * 2.0;
        let error = (Complex64::new(f64::from(actual.re), f64::from(actual.im)) - expected).norm();
        assert!(
            error < 1.0e-5 * expected.norm(),
            "channel {channel}: {actual} vs {expected}"
        );
    }
}
