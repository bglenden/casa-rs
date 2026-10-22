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
    ModelPrescale, NativeRow, PlaneRange, PredictionScratch, PreparedModelGrids, RowContext,
    SampleBuffer, SpectralAxis, SpectralKernel, SpectralResampler, WeightingGeneration, Work,
};
use common::{IMAGE, operator};
use ndarray::Array2;
use num_complex::{Complex32, Complex64};

const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
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

/// Neighbouring channels that read the same taps of the model share one
/// degridded sample (#855), and every channel still predicts exactly what
/// it predicts alone. A row of 256 channels 2 MHz apart near 1 GHz crosses
/// a fine offset every 16 channels or so:
/// - without a phase shift it places a sample per run, whether its
///   channels ascend or descend in frequency (the run ends found by
///   probing) or are shuffled (placed channel by channel);
/// - with the phase centre shifted 0.013 m, each channel's phase moves
///   about 5e-4 rad, far past the shared tolerance, and nothing is shared;
/// - under a Taylor basis the Taylor variable tells channels apart, and
///   nothing is shared.
#[test]
fn neighbouring_channels_that_read_the_same_taps_share_one_sample() {
    let ascending: Vec<f64> = (0..256)
        .map(|channel| 1.0e9 + 2.0e6 * f64::from(channel))
        .collect();
    let descending: Vec<f64> = ascending.iter().rev().copied().collect();
    // A stride coprime to 256 visits every channel out of order.
    let shuffled: Vec<f64> = (0..256).map(|index| ascending[index * 97 % 256]).collect();
    let constant = SpectralResampler::direct(Basis::Constant).expect("direct");
    let taylor = SpectralResampler::direct(Basis::Taylor {
        terms: 2,
        reference_hz: 1.0e9,
    })
    .expect("direct");
    // Four output channels across the band: runs also end where a channel's
    // nearest plane changes.
    let planes = SpectralAxis::new(1.064e9, 1.28e8, 4).expect("axis");
    let nearest = SpectralResampler::channel_local(planes, SpectralKernel::Nearest);
    // `Some(true)`: few samples; `Some(false)`: one per channel.
    for (resampler, phase_shift_m, frequencies, shares) in [
        (constant, 0.0, &ascending, Some(true)),
        (constant, 0.0, &descending, Some(true)),
        (constant, 0.0, &shuffled, None),
        (nearest, 0.0, &ascending, Some(true)),
        (nearest, 0.0, &descending, Some(true)),
        (constant, 0.013, &ascending, Some(false)),
        (taylor, 0.0, &ascending, Some(false)),
    ] {
        let basis = resampler.basis();
        let operator = operator(GridPrecision::F64, basis, &XX_YY, &STOKES_I);
        let mut image = Array2::zeros((IMAGE, IMAGE));
        image[(IMAGE / 2 + 3, IMAGE / 2 - 5)] = 1.3;
        let model = operator
            .prepare_model(
                &ModelImages {
                    first_plane: 0,
                    planes: (0..basis.planes())
                        .map(|plane| ModelPlane {
                            // A flux per plane, so a run that crossed planes
                            // would predict the wrong one.
                            images: vec![&image * (1.0 + plane as f32); basis.data_terms()],
                        })
                        .collect(),
                },
                ModelPrescale::Unit,
            )
            .expect("model");
        let values = vec![Complex32::new(1.0, 0.0); frequencies.len() * 2];
        let weights = vec![1.0; values.len()];
        let flags = vec![false; values.len()];
        let row = |frequencies_hz| NativeRow {
            phase_shift_m,
            ..row_on(
                UVW_M,
                [0, 1],
                frequencies_hz,
                (
                    &values[..frequencies_hz.len() * 2],
                    &weights[..frequencies_hz.len() * 2],
                    &flags[..frequencies_hz.len() * 2],
                ),
            )
        };
        let whole = row(frequencies);
        let mut scratch = PredictionScratch::default();
        scratch.begin(2);
        resampler
            .place_prediction(&operator, &model, &whole, &mut scratch)
            .expect("placed");
        let label = format!(
            "{basis:?}, phase shift {phase_shift_m} m, first {} Hz, second {} Hz",
            frequencies[0], frequencies[1]
        );
        match shares {
            Some(true) => assert!(
                scratch.samples() < frequencies.len() / 4,
                "{label}: {} samples for {} channels",
                scratch.samples(),
                frequencies.len()
            ),
            Some(false) => assert_eq!(scratch.samples(), frequencies.len(), "{label}"),
            None => {}
        }
        let predicted = predict_one(&resampler, &operator, &model, &whole);
        for (channel, frequency_hz) in frequencies.iter().enumerate() {
            let alone = predict_one(
                &resampler,
                &operator,
                &model,
                &row(std::slice::from_ref(frequency_hz)),
            );
            assert_eq!(
                predicted[channel * 2..channel * 2 + 2],
                alone[..],
                "{label}: channel {channel}"
            );
        }
    }
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

/// One row of `frequencies_hz` at `uvw_m` on `antennas`, every sample
/// unflagged.
fn row_on<'a>(
    uvw_m: [f64; 3],
    antennas: [u32; 2],
    frequencies_hz: &'a [f64],
    cells: (&'a [Complex32], &'a [f32], &'a [bool]),
) -> NativeRow<'a> {
    NativeRow {
        uvw_m,
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz,
        values: cells.0,
        weights: cells.1,
        flags: cells.2,
        row_flag: false,
        context: RowContext {
            antennas,
            ..context()
        },
    }
}

/// `row`'s prediction by `resampler` alone.
fn predict_one(
    resampler: &SpectralResampler,
    operator: &MeasurementOperator,
    model: &PreparedModelGrids,
    row: &NativeRow<'_>,
) -> Vec<Complex32> {
    let mut out = vec![Complex32::new(9.0, 9.0); row.values.len()];
    resampler
        .predict_row(
            operator,
            &mut CpuBackend::new(),
            model,
            row,
            &mut PredictionScratch::default(),
            &mut out,
        )
        .expect("prediction");
    out
}

/// CASA's simulator predicts autocorrelations (its `GridFT` has `usezero`
/// true): an autocorrelation row predicts what a cross-correlation of zero
/// length predicts, bit for bit, under direct, nearest and linear mapping;
/// a point model's zero spacing is its flux, and an extended model's the
/// sum of its pixels; and a resampler not asked to predicts zero, as
/// tclean's `GridFT` does.
#[test]
fn an_autocorrelation_predicts_the_zero_spacing_only_when_asked() {
    let axis = SpectralAxis::new(1.0e9, 1.0e6, 4).expect("axis");
    let resamplers = [
        SpectralResampler::direct(Basis::Constant).expect("direct"),
        SpectralResampler::channel_local(axis, SpectralKernel::Nearest),
        SpectralResampler::channel_local(axis, SpectralKernel::Linear),
    ];
    // Natives at output channel centres, which a linear mapping predicts
    // as those channels' own values.
    let frequencies = [1.0e9, 1.001e9, 1.002e9];
    let values = vec![Complex32::default(); frequencies.len() * 2];
    let weights = vec![1.0; frequencies.len() * 2];
    let flags = vec![false; frequencies.len() * 2];
    let cells = (&values[..], &weights[..], &flags[..]);
    for resampler in resamplers {
        for precision in [GridPrecision::F32, GridPrecision::F64] {
            let operator = operator(precision, resampler.basis(), &XX_YY, &STOKES_I);
            let planes = resampler.basis().planes() as usize;
            let point = point_model(&operator, &vec![1.7; planes]);
            // An extended model: a 5 × 3 patch of unequal pixels about the
            // reference pixel.
            let mut extended = ModelImages {
                first_plane: 0,
                planes: Vec::new(),
            };
            let mut sum = 0.0_f64;
            let mut image = Array2::zeros((IMAGE, IMAGE));
            for (index, (y, x)) in (IMAGE / 2 - 1..=IMAGE / 2 + 1)
                .flat_map(|y| (IMAGE / 2 - 2..=IMAGE / 2 + 2).map(move |x| (y, x)))
                .enumerate()
            {
                let value = 0.1 + 0.05 * index as f32;
                image[(y, x)] = value;
                sum += f64::from(value);
            }
            extended.planes = vec![
                ModelPlane {
                    images: vec![image],
                };
                planes
            ];
            let extended = operator
                .prepare_model(&extended, ModelPrescale::Unit)
                .expect("extended model");
            let auto = row_on([0.0; 3], [3, 3], &frequencies, cells);
            let zero_length = row_on([0.0; 3], [2, 3], &frequencies, cells);
            let predicting = resampler.predicting_autocorrelations();
            assert!(predicting.predicts_autocorrelations());
            assert!(!resampler.predicts_autocorrelations(), "off by default");
            for (label, model, flux) in [("point", &point, 1.7), ("extended", &extended, sum)] {
                let label = format!("{resampler:?} {precision:?} {label}");
                let predicted = predict_one(&predicting, &operator, model, &auto);
                assert_eq!(
                    predicted,
                    predict_one(&predicting, &operator, model, &zero_length),
                    "{label}: an autocorrelation predicts a zero-length baseline"
                );
                assert_eq!(
                    predicted,
                    predict_one(&resampler, &operator, model, &zero_length),
                    "{label}: the option leaves cross-correlations alone"
                );
                for value in &predicted {
                    assert!(
                        (f64::from(value.re) - flux).abs() <= 1.0e-4 * flux
                            && f64::from(value.im).abs() <= 1.0e-6 * flux,
                        "{label}: the zero spacing {value} is the model's flux {flux}"
                    );
                }
                assert!(
                    predict_one(&resampler, &operator, model, &auto)
                        .iter()
                        .all(|value| *value == Complex32::default()),
                    "{label}: an autocorrelation predicts zero unless asked"
                );
            }
        }
    }
}

/// Model grids prepared plane by plane, the planes filled out of order on
/// threads of their own, equal [`MeasurementOperator::prepare_model`]'s bit
/// for bit: in both precisions, with a prescale, over several grid
/// polarizations, and over several planes or several Taylor terms. Grids
/// filled twice hold only the second fill, and grids of another operator
/// are refused.
#[test]
fn model_grids_filled_plane_by_plane_equal_the_prepared_model() {
    let rr_ll = [CorrelationType::CircularRr, CorrelationType::CircularLl];
    let i_v = [
        PolarizationCoordinate::StokesI,
        PolarizationCoordinate::StokesV,
    ];
    let cube = SpectralAxis::new(1.0e9, 1.0e6, 5).expect("axis");
    let bases = [
        SpectralResampler::channel_local(cube, SpectralKernel::Nearest).basis(),
        Basis::Taylor {
            terms: 2,
            reference_hz: 1.0e9,
        },
    ];
    let mut seed = 7_u32;
    let mut pixel = move || {
        seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        f32::from(u16::try_from(seed >> 16).expect("16 bits")) / 65_536.0 - 0.5
    };
    for basis in bases {
        for precision in [GridPrecision::F32, GridPrecision::F64] {
            let operator = operator(precision, basis, &rr_ll, &i_v);
            let planes = basis.planes() as usize;
            let images = basis.data_terms() * i_v.len();
            let model = ModelImages {
                first_plane: 0,
                planes: (0..planes)
                    .map(|_| ModelPlane {
                        images: (0..images)
                            .map(|_| Array2::from_shape_fn((IMAGE, IMAGE), |_| pixel()))
                            .collect(),
                    })
                    .collect(),
            };
            let prescale = (0..planes)
                .map(|_| Array2::from_shape_fn((IMAGE, IMAGE), |_| 0.5 + pixel()))
                .collect::<Vec<_>>();
            let prepared = operator
                .prepare_model(&model, ModelPrescale::Planes(&prescale))
                .expect("prepared");
            let mut grids = operator
                .empty_model_grids(prepared.layout().planes())
                .expect("empty grids");
            assert_eq!(grids.bytes(), prepared.bytes());
            assert_eq!(
                grids.bytes(),
                prepared.layout().bytes(precision) as u64,
                "the grids' bytes are their cells'"
            );
            // A first fill from other images, replaced by the second.
            for target in grids.planes_mut() {
                let other = &model.planes[(target.plane() as usize + 1) % planes];
                operator
                    .fill_model_plane(other, None, target)
                    .expect("first fill");
            }
            std::thread::scope(|scope| {
                for target in grids.planes_mut().into_iter().rev() {
                    let operator = &operator;
                    let plane = target.plane() as usize;
                    let (images, factor) = (&model.planes[plane], &prescale[plane]);
                    scope.spawn(move || {
                        operator
                            .fill_model_plane(images, Some(factor), target)
                            .expect("filled");
                    });
                }
            });
            assert!(grids == prepared, "{basis:?} {precision:?}");
        }
    }
    let operator64 = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let operator32 = operator(GridPrecision::F32, Basis::Constant, &XX_YY, &STOKES_I);
    let plane = ModelPlane {
        images: vec![Array2::zeros((IMAGE, IMAGE))],
    };
    let mut grids = operator64
        .empty_model_grids(PlaneRange::single(0))
        .expect("empty grids");
    let target = grids.planes_mut().pop().expect("one plane");
    assert!(
        operator32.fill_model_plane(&plane, None, target).is_err(),
        "grids of another precision are refused"
    );
    assert!(
        operator64.empty_model_grids(PlaneRange::single(1)).is_err(),
        "planes past the basis are refused"
    );
}

/// A batch predicts each row as that row alone does, and a scratch made for
/// a batch of the widest rows never grows. Batches of three rows of every
/// width up to the widest, narrow after wide, with a flagged row and an
/// autocorrelation among them, give bit-identical visibilities to each
/// row's own [`SpectralResampler::predict_row`] and keep every buffer at
/// the capacity [`PredictionScratch::bytes`] charges, for direct, nearest
/// and linear resamplers, widths that are not powers of two, and
/// autocorrelations predicted or not.
#[test]
fn a_batch_predicts_each_row_as_alone_within_what_it_charges() {
    let axis =
        |width_hz: f64, channels: u32| SpectralAxis::new(1.0e9, width_hz, channels).expect("axis");
    let resamplers = [
        SpectralResampler::direct(Basis::Constant).expect("direct"),
        SpectralResampler::channel_local(axis(1.0e6, 37), SpectralKernel::Nearest),
        SpectralResampler::channel_local(axis(3.0e6, 37), SpectralKernel::Linear),
        SpectralResampler::channel_local(axis(0.5e6, 301), SpectralKernel::Linear),
    ];
    let resamplers = resamplers
        .into_iter()
        .flat_map(|resampler| [resampler, resampler.predicting_autocorrelations()]);
    let widest = 257;
    let rows_per_batch = 3;
    for resampler in &resamplers.collect::<Vec<_>>() {
        let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
        let model = point_model(&operator, &vec![1.0; resampler.basis().planes() as usize]);
        let mut scratch =
            SpectralResampler::prediction_scratch([resampler], 2, widest, rows_per_batch);
        let bytes =
            SpectralResampler::prediction_scratch_bytes([resampler], 2, widest, rows_per_batch);
        assert_eq!(scratch.capacity_bytes(), bytes);
        let mut backend = CpuBackend::new();
        for widths in [[3, widest, 1], [129, widest, 7], [widest, widest, widest]] {
            let frequencies = widths.map(|channels| {
                (0..channels)
                    .map(|channel| 1.0e9 + 1.0e6 * channel as f64)
                    .collect::<Vec<_>>()
            });
            let values = widths.map(|channels| {
                (0..channels * 2)
                    .map(|cell| Complex32::new(1.0, cell as f32))
                    .collect::<Vec<_>>()
            });
            let weights = widths.map(|channels| vec![1.0; channels * 2]);
            let flags = widths.map(|channels| vec![false; channels * 2]);
            let rows = std::array::from_fn::<_, 3, _>(|index| NativeRow {
                uvw_m: UVW_M,
                phase_shift_m: 0.0,
                pointing_offset_rad: [0.0; 2],
                frequencies_hz: &frequencies[index],
                values: &values[index],
                weights: &weights[index],
                flags: &flags[index],
                // The middle row predicts nothing: flagged in the first
                // batch, an autocorrelation in the others.
                row_flag: index == 1 && widths[0] == 3,
                context: if index == 1 && widths[0] != 3 {
                    RowContext {
                        antennas: [1, 1],
                        ..context()
                    }
                } else {
                    context()
                },
            });
            scratch.begin(2);
            let placed = rows.each_ref().map(|row| {
                resampler
                    .place_prediction(&operator, &model, row, &mut scratch)
                    .expect("placed")
            });
            scratch
                .apply(&mut backend, operator.cf(), &model)
                .expect("applied");
            for (row, placed) in rows.iter().zip(placed) {
                let mut batched = vec![Complex32::default(); row.values.len()];
                resampler
                    .finish_prediction(&operator, row, placed, &mut scratch, &mut batched)
                    .expect("finished");
                let mut alone = vec![Complex32::default(); row.values.len()];
                resampler
                    .predict_row(
                        &operator,
                        &mut CpuBackend::new(),
                        &model,
                        row,
                        &mut PredictionScratch::default(),
                        &mut alone,
                    )
                    .expect("prediction");
                assert_eq!(batched, alone, "a batched row predicts as alone");
            }
            assert_eq!(
                scratch.capacity_bytes(),
                bytes,
                "rows of {widths:?} channels grew the scratch"
            );
        }
    }
}
