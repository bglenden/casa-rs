// SPDX-License-Identifier: LGPL-3.0-or-later
//! Cubic spectral interpolation (tclean's `interpolation='cubic'`, #1066):
//! CASA's `FTMachine::interpolateFrequencyTogrid` and
//! `interpolateFrequencyFromgrid` fit casacore `InterpolateArray1D`'s cubic
//! polynomial through four points. A cubic spectrum is reproduced exactly,
//! where linear interpolation is not; flags follow the two channels a
//! sample lies between; weights are the cubic of the channels' imaging
//! weights, a flagged channel weighing nothing; a native channel is
//! predicted within four native widths of the axis (`matchChannel`).

mod common;

use casa_imaging_model::PolarizationCoordinate;
use casa_imaging_operator::{
    CpuBackend, GridPrecision, MeasurementOperator, ModelImages, ModelPlane, ModelPrescale,
    NativeRow, PredictionScratch, PreparedModelGrids, RowContext, SampleBuffer, SpectralAxis,
    SpectralKernel, SpectralResampler, WeightingGeneration,
};
use casa_types::Correlation;
use common::{IMAGE, operator};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [Correlation; 2] = [Correlation::XX, Correlation::YY];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];
const UVW_M: [f64; 3] = [37.0, -21.0, 4.0];

fn context() -> RowContext {
    RowContext {
        original_uvw_m: None,
        time_s: 0.0,
        antennas: [0, 1],
        antenna_types: [0, 0],
        parallactic_angle_rad: [0.0, 0.0],
        field: 0,
        spectral_window: 0,
    }
}

/// A cubic in the channel coordinate `x = (ν − 1 GHz)/1 MHz`.
fn spectrum(x: f64) -> f64 {
    1.0 + 0.3 * x - 0.05 * x * x + 0.004 * x * x * x
}

fn pixel(frequency_hz: f64) -> f64 {
    (frequency_hz - 1.0e9) / 1.0e6
}

/// Twelve 1 MHz native channels from 1 GHz.
fn native() -> Vec<f64> {
    (0..12)
        .map(|channel| 1.0e9 + f64::from(channel) * 1.0e6)
        .collect()
}

/// The samples `kernel` places from a row whose visibilities and weights
/// follow [`spectrum`] at the native channels, on an axis of ten 1 MHz
/// channels a quarter channel above them; `flags` per channel.
fn placed(kernel: SpectralKernel, flags: &[bool]) -> Vec<(f64, Complex32, f32)> {
    let axis = SpectralAxis::new(1.00025e9, 1.0e6, 10).expect("axis");
    let resampler = SpectralResampler::channel_local(axis, kernel);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let native = native();
    let mut values = native
        .iter()
        .flat_map(|frequency_hz| [Complex32::new(spectrum(pixel(*frequency_hz)) as f32, 0.0); 2])
        .collect::<Vec<_>>();
    let weights = native
        .iter()
        .flat_map(|frequency_hz| [spectrum(pixel(*frequency_hz)) as f32; 2])
        .collect::<Vec<_>>();
    let flags = flags.iter().flat_map(|flag| [*flag; 2]).collect::<Vec<_>>();
    // A flagged channel's stored value may be anything, NaN included.
    for (channel, flag) in flags.chunks(2).enumerate() {
        if flag[0] {
            values[channel * 2] = Complex32::new(f32::NAN, 0.0);
            values[channel * 2 + 1] = Complex32::new(f32::NAN, 0.0);
        }
    }
    let row = NativeRow {
        uvw_m: [10.0, 10.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &native,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = SampleBuffer::new(2);
    resampler
        .place(
            &operator,
            &WeightingGeneration::Natural { taper: None },
            &row,
            &mut out,
        )
        .expect("place");
    let block = out.block();
    (0..out.len())
        .map(|sample| {
            let placement = out.placements()[sample];
            let frequency_hz = placement.u * 299_792_458.0 / 10.0;
            (
                frequency_hz,
                block.values_of(sample)[0] / block.weights_of(sample)[0],
                block.weights_of(sample)[0],
            )
        })
        .collect()
}

#[test]
fn cubic_gridding_reproduces_a_cubic_spectrum_and_linear_does_not() {
    let cubic = placed(SpectralKernel::Cubic, &[false; 12]);
    assert_eq!(cubic.len(), 10, "one sample per output channel");
    for (frequency_hz, value, weight) in &cubic {
        let expected = spectrum(pixel(*frequency_hz));
        assert!(
            (f64::from(value.re) - expected).abs() < 1e-5,
            "{frequency_hz}: {value} against {expected}"
        );
        // Natural weights are interpolated as the values are.
        assert!(
            (f64::from(*weight) - expected).abs() < 1e-5,
            "{frequency_hz}: weight {weight} against {expected}"
        );
    }
    let linear = placed(SpectralKernel::Linear, &[false; 12]);
    let worst = linear
        .iter()
        .map(|(frequency_hz, value, _)| {
            (f64::from(value.re) - spectrum(pixel(*frequency_hz))).abs()
        })
        .fold(0.0, f64::max);
    assert!(worst > 1e-3, "linear interpolation misses a cubic: {worst}");
}

#[test]
fn a_cubic_sample_takes_the_flags_of_its_two_channels_and_weighs_a_flagged_neighbour_nothing() {
    // Channel 5 flagged: the samples between channels 4–5 and 5–6 are
    // flagged; those whose stencil only reaches channel 5 stay, finite.
    let mut flags = [false; 12];
    flags[5] = true;
    let cubic = placed(SpectralKernel::Cubic, &flags);
    let pixels = cubic
        .iter()
        .map(|(frequency_hz, _, _)| pixel(*frequency_hz))
        .collect::<Vec<_>>();
    assert!(
        pixels.iter().all(|pixel| !(4.0..6.0).contains(pixel)),
        "{pixels:?}"
    );
    assert_eq!(cubic.len(), 8, "{pixels:?}");
    for (frequency_hz, value, weight) in &cubic {
        assert!(value.re.is_finite() && weight.is_finite(), "{frequency_hz}");
    }
    // At 3.25 the stencil is channels 2–5: channel 5 weighs nothing.
    let (_, _, weight) = cubic
        .iter()
        .find(|(frequency_hz, _, _)| (pixel(*frequency_hz) - 3.25).abs() < 1e-9)
        .expect("the sample at 3.25");
    let lagrange = |x: f64, points: [f64; 4], at: usize| {
        (0..4)
            .filter(|j| *j != at)
            .map(|j| (x - points[j]) / (points[at] - points[j]))
            .product::<f64>()
    };
    let points = [2.0, 3.0, 4.0, 5.0];
    let expected = (0..3)
        .map(|at| lagrange(3.25, points, at) * spectrum(points[at]))
        .sum::<f64>();
    assert!(
        (f64::from(*weight) - expected).abs() < 1e-5,
        "{weight} against {expected}"
    );
}

/// A point source whose flux on output plane `k` is `flux[k]`.
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

/// The prediction of `model` at `native`, per channel, first correlation.
fn predict(
    resampler: &SpectralResampler,
    operator: &MeasurementOperator,
    model: &PreparedModelGrids,
    native: &[f64],
) -> Vec<Complex64> {
    let values = vec![Complex32::new(1.0, 0.0); native.len() * 2];
    let weights = vec![1.0; native.len() * 2];
    let flags = vec![false; native.len() * 2];
    let row = NativeRow {
        uvw_m: UVW_M,
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: native,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = vec![Complex32::default(); values.len()];
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
    out.chunks(2)
        .map(|pair| Complex64::new(f64::from(pair[0].re), f64::from(pair[0].im)))
        .collect()
}

#[test]
fn cubic_prediction_reproduces_a_cubic_spectrum_and_extrapolates_in_its_halo() {
    let axis = SpectralAxis::new(1.0e9, 1.0e6, 12).expect("axis");
    let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Cubic);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let fluxes = (0..12)
        .map(|plane| spectrum(f64::from(plane)) as f32)
        .collect::<Vec<_>>();
    let model = point_model(&operator, &fluxes);
    // Natives a third of a channel above the planes, through the axis's
    // end and on into the halo: CASA's cubic halo reaches four native
    // widths past pixel `nchan` (12.0 here, the 13th native at 12.33), so
    // the polynomial through the last four planes extrapolates there. The
    // row maps to every plane, so every plane is degridded (a row mapping
    // to fewer leaves the rest zero, which a stencil at its ends reads, as
    // CASA's `getInterpolateArrays` does).
    let native = (0..14)
        .map(|channel| 1.0e9 + (f64::from(channel) + 1.0 / 3.0) * 1.0e6)
        .collect::<Vec<_>>();
    let unit = {
        let flat = point_model(&operator, &[1.0; 12]);
        predict(&resampler, &operator, &flat, &native)[5]
    };
    let predicted = predict(&resampler, &operator, &model, &native);
    for (channel, frequency_hz) in native.iter().enumerate() {
        let expected = unit * spectrum(pixel(*frequency_hz));
        assert!(
            (predicted[channel] - expected).norm() < 1e-5 * expected.norm(),
            "native {channel}: {} against {expected}",
            predicted[channel]
        );
    }
    // Two natives three and four widths past pixel `nchan`: within the
    // cubic halo, every plane is degridded (CASA's `chanMap` of only −2)
    // and they extrapolate; beyond linear's halo of two widths, the row
    // maps nothing and predicts zero.
    let linear = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
    let far = [1.0e9 + 14.5e6, 1.0e9 + 15.5e6];
    assert_eq!(
        predict(&linear, &operator, &model, &far)[0],
        Complex64::default()
    );
    assert_ne!(
        predict(&resampler, &operator, &model, &far)[0],
        Complex64::default()
    );
}
