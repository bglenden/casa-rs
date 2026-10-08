// SPDX-License-Identifier: LGPL-3.0-or-later
//! The spectral resampler: direct, nearest and CASA linear channel mapping
//! with flags, weights, the phase-centre phasor and CASA's one-channel
//! bypasses.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, DensityCellRule, DensityGridShape, DensityUv, GridGeometry, GridPadding, GridPrecision,
    ImageExtent, MeasurementOperator, NativeRow, OperatorError, PolarizationRouting, RowContext,
    SampleBuffer, SpectralAxis, SpectralKernel, SpectralResampler, Spheroidal, WPlaneCount,
    WPlanes, WeightingGeneration, build_density_grid,
};
use common::{geometry, operator};
use num_complex::Complex32;

const C: f64 = 299_792_458.0;
const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];

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

fn natural() -> WeightingGeneration {
    WeightingGeneration::Natural { taper: None }
}

fn axis(first_ghz: f64, width_ghz: f64, channels: u32) -> SpectralAxis {
    SpectralAxis::new(first_ghz * 1.0e9, width_ghz * 1.0e9, channels).expect("axis")
}

/// A row of unit visibilities and weights at `frequencies_ghz` on a short
/// baseline, placed by `resampler`; returns the planes and frequencies.
fn planes_of(
    resampler: &SpectralResampler,
    frequencies_ghz: &[f64],
    density: bool,
) -> Vec<(u32, f64)> {
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let frequencies = frequencies_ghz
        .iter()
        .map(|f| f * 1.0e9)
        .collect::<Vec<_>>();
    let values = vec![Complex32::new(1.0, 0.0); frequencies.len() * 2];
    let weights = vec![1.0; frequencies.len() * 2];
    let flags = vec![false; frequencies.len() * 2];
    let row = NativeRow {
        uvw_m: [10.0, 10.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = SampleBuffer::new(if density { 1 } else { 2 });
    if density {
        let shape = DensityGridShape {
            width: 64,
            height: 64,
            planes: resampler.basis().planes() as usize,
            padding: 0,
            increment_rad: [-2.0e-5, 2.0e-5],
            rule: DensityCellRule::Cube,
        };
        resampler
            .place_density(&operator, &row, &shape, &mut out)
            .expect("density");
    } else {
        resampler
            .place(&operator, &natural(), &row, &mut out)
            .expect("place");
    }
    out.placements()
        .iter()
        .map(|p| (p.plane, (p.u * C / 10.0 / 1.0e6).round() / 1.0e3))
        .collect()
}

#[test]
fn direct_sampling_places_every_unflagged_channel_on_plane_zero() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("resampler");
    let frequencies = [1.0e9, 1.1e9, 1.2e9];
    let values = [
        Complex32::new(1.0, 0.0),
        Complex32::new(2.0, 0.0),
        Complex32::new(3.0, 1.0),
        Complex32::new(4.0, 1.0),
        Complex32::new(5.0, 2.0),
        Complex32::new(6.0, 2.0),
    ];
    let weights = [2.0, 4.0, 1.0, 1.0, 8.0, 8.0];
    let flags = [false, false, true, false, false, false];
    let row = NativeRow {
        uvw_m: [300.0, -150.0, 20.0],
        phase_shift_m: 0.25,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = SampleBuffer::new(2);
    resampler
        .place(&operator, &natural(), &row, &mut out)
        .expect("place");
    // CASA `FTMachine::setSpectralFlag` flags every correlation of a channel
    // when any one is flagged (pseudo-I aside), before gridding.
    assert_eq!(
        out.len(),
        2,
        "the channel with a flagged correlation is dropped"
    );
    let placed = out.placements();
    let scale = 1.0e9 / C;
    assert!((placed[0].u - 300.0 * scale).abs() < 1e-9);
    assert!((placed[0].v + 150.0 * scale).abs() < 1e-9);
    assert!((placed[0].w - 20.0 * scale).abs() < 1e-9);
    assert!((placed[0].phase - std::f64::consts::TAU * 0.25 * scale).abs() < 1e-12);
    assert_eq!(placed[0].plane, 0);
    assert_eq!(placed[0].spectral, 0.0);
    let block = out.block();
    // Unpolarized weight (2 + 4)/2 = 3 on both polarizations.
    assert_eq!(block.weights_of(0), &[3.0, 3.0]);
    let phasor = num_complex::Complex64::from_polar(3.0, placed[0].phase);
    let expected = phasor * 2.0;
    assert!((f64::from(block.values_of(0)[1].re) - expected.re).abs() < 1e-5);
    assert!((f64::from(block.values_of(0)[1].im) - expected.im).abs() < 1e-5);
    assert_eq!(block.weights_of(1), &[8.0, 8.0]);

    let flagged_row = NativeRow {
        row_flag: true,
        ..row
    };
    out.clear();
    resampler
        .place(&operator, &natural(), &flagged_row, &mut out)
        .expect("place");
    assert!(out.is_empty());
    assert!(matches!(
        SpectralResampler::direct(Basis::ChannelLocal { planes: 2 }),
        Err(OperatorError::SpectralAxis { .. })
    ));
}

#[test]
fn samples_whose_support_leaves_the_grid_are_dropped() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("resampler");
    let frequencies = [1.0e9];
    let values = [Complex32::new(1.0, 0.0); 2];
    let weights = [1.0; 2];
    let flags = [false; 2];
    // 1 cell = 1/(80·2e-5) = 625 wavelengths; 40 cells reach the grid edge.
    let far = 39.0 * 625.0 * C / 1.0e9;
    for (u, expected) in [(0.0, 1), (far, 0), (-far, 0), (35.0 * 625.0 * C / 1.0e9, 1)] {
        let row = NativeRow {
            uvw_m: [u, 0.0, 0.0],
            phase_shift_m: 0.0,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz: &frequencies,
            values: &values,
            weights: &weights,
            flags: &flags,
            row_flag: false,
            context: context(),
        };
        let mut out = SampleBuffer::new(2);
        resampler
            .place(&operator, &natural(), &row, &mut out)
            .expect("place");
        assert_eq!(out.len(), expected, "u = {u}");
    }
}

#[test]
fn taylor_basis_sets_the_spectral_variable() {
    let basis = Basis::Taylor {
        terms: 2,
        reference_hz: 1.0e9,
    };
    let operator = operator(GridPrecision::F64, basis, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::direct(basis).expect("resampler");
    let frequencies = [0.9e9, 1.1e9];
    let values = [Complex32::new(1.0, 0.0); 4];
    let weights = [1.0; 4];
    let flags = [false; 4];
    let row = NativeRow {
        uvw_m: [10.0, 10.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = SampleBuffer::new(2);
    resampler
        .place(&operator, &natural(), &row, &mut out)
        .expect("place");
    let spectral = out
        .placements()
        .iter()
        .map(|p| p.spectral)
        .collect::<Vec<_>>();
    assert_eq!(spectral, [-0.1, 0.1]);
    assert!(out.placements().iter().all(|p| p.plane == 0));
}

#[test]
fn nearest_mapping_rounds_the_spectral_pixel() {
    let resampler = SpectralResampler::channel_local(axis(1.0, 0.1, 3), SpectralKernel::Nearest);
    let placed = planes_of(&resampler, &[0.94, 0.96, 1.04, 1.149, 1.151, 1.26], false);
    assert_eq!(
        placed,
        [(0, 0.96), (0, 1.04), (1, 1.149), (2, 1.151)],
        "0.94 and 1.26 GHz lie outside the axis; samples keep their native frequency"
    );
}

#[test]
fn linear_mapping_interpolates_values_and_channel_weights_and_ors_flags() {
    let resampler = SpectralResampler::channel_local(axis(1.05, 0.1, 2), SpectralKernel::Linear);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let frequencies = [1.0e9, 1.1e9, 1.2e9];
    let values = [
        Complex32::new(1.0, 0.0),
        Complex32::new(10.0, 0.0),
        Complex32::new(3.0, 0.0),
        Complex32::new(30.0, 0.0),
        Complex32::new(5.0, 0.0),
        Complex32::new(50.0, 0.0),
    ];
    let weights = [2.0, 2.0, 4.0, 4.0, 8.0, 8.0];
    let flags = [false; 6];
    let row = NativeRow {
        uvw_m: [10.0, 0.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = SampleBuffer::new(2);
    resampler
        .place(&operator, &natural(), &row, &mut out)
        .expect("place");
    assert_eq!(out.len(), 2);
    let placed = out.placements();
    assert_eq!(placed[0].plane, 0);
    assert_eq!(placed[1].plane, 1);
    assert!(
        (placed[0].u - 10.0 * 1.05e9 / C).abs() < 1e-12,
        "fine frequency"
    );
    let block = out.block();
    // CASA weights native channels (`VisImagingWeight`) and interpolates the
    // weights linearly: 3 then 6 at the midpoints; values (1+3)/2·3 and
    // (3+5)/2·6.
    assert_eq!(block.weights_of(0), &[3.0, 3.0]);
    assert_eq!(block.weights_of(1), &[6.0, 6.0]);
    assert!((block.values_of(0)[0].re - 6.0).abs() < 1e-5);
    assert!((block.values_of(0)[1].re - 60.0).abs() < 1e-5);
    assert!((block.values_of(1)[0].re - 24.0).abs() < 1e-5);

    // A global density accumulates the native channels, which share one
    // cell here (2 + 4 + 8); the channels' uniform weights then
    // interpolate: (2 + 4)/2/14 and (4 + 8)/2/14.
    let global = DensityGridShape {
        width: 64,
        height: 64,
        planes: 1,
        padding: 0,
        increment_rad: [-2.0e-5, 2.0e-5],
        rule: DensityCellRule::Standard,
    };
    let mut density = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &global, &mut density)
        .expect("density");
    assert_eq!(density.len(), 3, "one density sample per native channel");
    let uniform = WeightingGeneration::density(
        build_density_grid(std::iter::once(density.block()), global),
        None,
        None,
        None,
    )
    .expect("uniform");
    out.clear();
    resampler
        .place(&operator, &uniform, &row, &mut out)
        .expect("place");
    let block = out.block();
    assert!((block.weights_of(0)[0] - 3.0 / 14.0).abs() < 1e-6);
    assert!((block.weights_of(1)[0] - 6.0 / 14.0).abs() < 1e-6);

    // CASA's cube Briggs weightor weights each output sample from the
    // nearest native weight instead, a tie keeping the left one; this
    // robustness makes the density term vanish.
    let per_channel = DensityGridShape {
        planes: 2,
        rule: DensityCellRule::Cube,
        ..global
    };
    let mut density = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &per_channel, &mut density)
        .expect("density");
    let briggs = WeightingGeneration::density(
        build_density_grid(std::iter::once(density.block()), per_channel),
        Some(10.0),
        None,
        None,
    )
    .expect("briggs");
    out.clear();
    resampler
        .place(&operator, &briggs, &row, &mut out)
        .expect("place");
    let block = out.block();
    assert!((block.weights_of(0)[0] - 2.0).abs() < 1e-5);
    assert!((block.weights_of(1)[0] - 4.0).abs() < 1e-5);

    let mut flagged = [false; 6];
    flagged[2] = true;
    let row = NativeRow {
        flags: &flagged,
        ..row
    };
    out.clear();
    resampler
        .place(&operator, &natural(), &row, &mut out)
        .expect("place");
    assert!(
        out.is_empty(),
        "a flag on either neighbour flags both interpolated samples"
    );
}

#[test]
fn cube_briggs_weights_come_from_the_nearest_native_channel_on_the_padded_axis() {
    // CASA `BriggsCubeWeightor::getWeightUniform` weights native channels on
    // the density plane their rounded spectral pixel names on the padded
    // axis (`FTMachine::matchChannel`); `interpolateFrequencyTogrid` hands
    // each output sample its nearest channel's weight. Image channels here
    // are half the native width, so the nearest channel's plane differs from
    // the sample's, and the first sample's lies on a padding plane.
    let output = axis(1.0, 0.05, 4);
    let resampler = SpectralResampler::channel_local(output, SpectralKernel::Linear);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let shape = DensityGridShape {
        width: 64,
        height: 64,
        planes: 6,
        padding: 1,
        increment_rad: [-2.0e-5, 2.0e-5],
        rule: DensityCellRule::Cube,
    };
    let row = |frequencies: &[f64], weights: &[f32]| -> (Vec<f64>, Vec<Complex32>, Vec<f32>) {
        (
            frequencies.iter().map(|f| f * 1.0e9).collect(),
            vec![Complex32::new(1.0, 0.0); frequencies.len() * 2],
            weights.iter().flat_map(|w| [*w, *w]).collect(),
        )
    };
    // A density row with one native channel on every padded plane centre,
    // weight `plane + 1`; on this short baseline every sample and its
    // conjugate share one cell, so plane `p` holds density `2(p + 1)`.
    let (frequencies, values, weights) = row(
        &[0.95, 1.0, 1.05, 1.1, 1.15, 1.2],
        &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
    );
    let flags = vec![false; values.len()];
    fn native<'a>(
        frequencies_hz: &'a [f64],
        values: &'a [Complex32],
        weights: &'a [f32],
        flags: &'a [bool],
    ) -> NativeRow<'a> {
        NativeRow {
            uvw_m: [10.0, 0.0, 0.0],
            phase_shift_m: 0.0,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz,
            values,
            weights,
            flags,
            row_flag: false,
            context: context(),
        }
    }
    let mut density = SampleBuffer::new(1);
    resampler
        .place_density(
            &operator,
            &native(&frequencies, &values, &weights, &flags),
            &shape,
            &mut density,
        )
        .expect("density");
    let grid = build_density_grid(std::iter::once(density.block()), shape);
    let uniform = WeightingGeneration::density(grid, None, None, None).expect("uniform");

    // Natives 0.97, 1.07, 1.17 GHz: the samples at 1.00, 1.05, 1.10 and
    // 1.15 GHz take the weights of 0.97 (padding plane 0), 1.07 (plane 2),
    // 1.07 and 1.17 (plane 4).
    let (frequencies, values, weights) = row(&[0.97, 1.07, 1.17], &[1.0, 1.0, 1.0]);
    let flags = vec![false; values.len()];
    let mut out = SampleBuffer::new(2);
    resampler
        .place(
            &operator,
            &uniform,
            &native(&frequencies, &values, &weights, &flags),
            &mut out,
        )
        .expect("place");
    let block = out.block();
    let placed = (0..out.len())
        .map(|sample| (out.placements()[sample].plane, block.weights_of(sample)[0]))
        .collect::<Vec<_>>();
    let expected = [(0, 0), (1, 2), (2, 2), (3, 4)]
        .map(|(plane, density_plane)| (plane, 1.0 / (2.0 * (density_plane as f32 + 1.0))));
    assert_eq!(placed.len(), expected.len());
    for ((plane, weight), (expected_plane, expected_weight)) in placed.iter().zip(expected) {
        assert_eq!(*plane, expected_plane);
        assert!(
            (weight - expected_weight).abs() < 1e-6,
            "plane {plane}: weight {weight}, expected {expected_weight}"
        );
    }

    // Without padding the first sample's nearest channel is off the axis
    // and weighs nothing.
    let unpadded = DensityGridShape {
        planes: 4,
        padding: 0,
        ..shape
    };
    let (frequencies, values, weights) = row(&[1.0, 1.05, 1.1, 1.15], &[2.0, 3.0, 4.0, 5.0]);
    let flags = vec![false; values.len()];
    let mut density = SampleBuffer::new(1);
    resampler
        .place_density(
            &operator,
            &native(&frequencies, &values, &weights, &flags),
            &unpadded,
            &mut density,
        )
        .expect("density");
    let grid = build_density_grid(std::iter::once(density.block()), unpadded);
    let uniform = WeightingGeneration::density(grid, None, None, None).expect("uniform");
    let (frequencies, values, weights) = row(&[0.97, 1.07, 1.17], &[1.0, 1.0, 1.0]);
    let flags = vec![false; values.len()];
    out.clear();
    resampler
        .place(
            &operator,
            &uniform,
            &native(&frequencies, &values, &weights, &flags),
            &mut out,
        )
        .expect("place");
    assert_eq!(
        out.placements().iter().map(|p| p.plane).collect::<Vec<_>>(),
        [1, 2, 3]
    );
}

/// On identical output and native grids every output sample lies on a
/// native channel, where casacore `InterpolateArray1D` takes that channel
/// alone: a flagged non-finite neighbour must not reach it.
#[test]
fn an_end_point_sample_takes_its_own_channel_value() {
    let operator = operator(
        GridPrecision::F64,
        Basis::ChannelLocal { planes: 3 },
        &XX_YY,
        &STOKES_I,
    );
    let resampler = SpectralResampler::channel_local(axis(1.0, 0.1, 3), SpectralKernel::Linear);
    let frequencies = [1.0e9, 1.1e9, 1.2e9];
    let mut values = vec![Complex32::new(2.0, -1.0); 6];
    values[2] = Complex32::new(f32::NAN, 0.0);
    values[3] = Complex32::new(f32::NAN, 0.0);
    let flags = [false, false, true, true, false, false];
    let row = NativeRow {
        uvw_m: [10.0, 10.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &values,
        weights: &[1.0; 6],
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = SampleBuffer::new(2);
    resampler
        .place(&operator, &natural(), &row, &mut out)
        .expect("place");
    let block = out.block();
    let planes = block
        .placements
        .iter()
        .map(|placement| placement.plane)
        .collect::<Vec<_>>();
    assert_eq!(planes, [0, 2], "the flagged channel's plane is empty");
    for index in 0..block.placements.len() {
        assert_eq!(
            block.values_of(index),
            [Complex32::new(2.0, -1.0); 2],
            "sample {index}"
        );
    }
}

#[test]
fn a_one_channel_row_bypasses_linear_interpolation() {
    // CASA `interpolateFrequencyTogrid`: with one native channel the row
    // maps like `nearest`, keeping its own frequency and weight.
    let resampler = SpectralResampler::channel_local(axis(1.0, 0.1, 3), SpectralKernel::Linear);
    assert_eq!(planes_of(&resampler, &[1.1], false), [(1, 1.1)]);
    assert_eq!(planes_of(&resampler, &[1.1], true), [(1, 1.1)]);
    assert_eq!(planes_of(&resampler, &[1.26], false), []);
}

#[test]
fn a_one_channel_image_accepts_native_channels_within_its_width() {
    // A 1 GHz channel 100 MHz wide accepts 0.99, 1.00 and 1.01 GHz and
    // rejects 1.06 GHz, under either kernel and for the density pass.
    for kernel in [SpectralKernel::Nearest, SpectralKernel::Linear] {
        let resampler = SpectralResampler::channel_local(axis(1.0, 0.1, 1), kernel);
        for density in [false, true] {
            let placed = planes_of(&resampler, &[0.94, 0.99, 1.0, 1.01, 1.06], density);
            assert_eq!(
                placed,
                [(0, 0.99), (0, 1.0), (0, 1.01)],
                "kernel {kernel:?} density {density}"
            );
        }
    }
    assert!(matches!(
        SpectralAxis::new(1.0e9, 0.0, 1),
        Err(OperatorError::SpectralAxis { .. })
    ));
}

#[test]
fn wide_output_channels_use_casa_fine_grid_points() {
    // Output channels twice as wide as native ones: two fine points per
    // output channel at the quarter positions.
    let resampler = SpectralResampler::channel_local(axis(1.1, 0.2, 2), SpectralKernel::Linear);
    let placed = planes_of(&resampler, &[1.0, 1.1, 1.2, 1.3, 1.4], false);
    assert_eq!(placed, [(0, 1.05), (0, 1.15), (1, 1.25), (1, 1.35)]);
}

#[test]
fn density_pass_carries_the_unpolarized_weight_without_a_support_test() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("resampler");
    let frequencies = [1.0e9];
    let values = [Complex32::new(1.0, 0.0); 2];
    let weights = [2.0, 6.0];
    let flags = [false; 2];
    let far = 39.0 * 625.0 * C / 1.0e9;
    let row = NativeRow {
        uvw_m: [far, 0.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let shape = DensityGridShape {
        width: 64,
        height: 64,
        planes: 1,
        padding: 0,
        increment_rad: [-2.0e-5, 2.0e-5],
        rule: DensityCellRule::Standard,
    };
    let mut out = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &shape, &mut out)
        .expect("density");
    assert_eq!(out.len(), 1);
    assert_eq!(out.block().weights_of(0), &[4.0]);
    let mut wrong = SampleBuffer::new(2);
    assert!(matches!(
        resampler.place_density(&operator, &row, &shape, &mut wrong),
        Err(OperatorError::NativeRow { .. })
    ));
}

/// CASA grids the cube density with natural weights linearly interpolated
/// onto its fine grid (`BriggsCubeWeightor` uses a `GridFT` without a
/// Briggs weightor), not the nearest native weight: a sample a quarter of
/// the way from a weight-2 to a weight-6 channel carries 3.
#[test]
fn cube_density_samples_interpolate_native_weights_linearly() {
    // Output channels at 1.025 and 1.075 GHz, 50 MHz wide, over natives at
    // 1.0 and 1.1 GHz: the output centres are a quarter and three quarters
    // of the way between them.
    let resampler = SpectralResampler::channel_local(axis(1.025, 0.05, 2), SpectralKernel::Linear);
    let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
    let frequencies = [1.0e9, 1.1e9];
    let row = NativeRow {
        uvw_m: [10.0, 10.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &[Complex32::new(1.0, 0.0); 4],
        weights: &[2.0, 2.0, 6.0, 6.0],
        flags: &[false; 4],
        row_flag: false,
        context: context(),
    };
    let shape = DensityGridShape {
        width: 64,
        height: 64,
        planes: 2,
        padding: 0,
        increment_rad: [-2.0e-5, 2.0e-5],
        rule: DensityCellRule::Cube,
    };
    let mut out = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &shape, &mut out)
        .expect("density");
    let block = out.block();
    let weights = (0..block.placements.len())
        .map(|index| (block.placements[index].plane, block.weights_of(index)[0]))
        .collect::<Vec<_>>();
    assert_eq!(weights, [(0, 3.0), (1, 5.0)]);
}

/// An autocorrelation weighs in the density grid, as CASA's
/// `VisImagingWeight` and its cube weightor's `GridFT` (`usezero = true`)
/// count it, but is neither gridded nor predicted (`GridFT::put`/`get` with
/// `usezero = false`).
#[test]
fn autocorrelations_reach_the_density_but_not_the_grid() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::direct(Basis::Constant).expect("resampler");
    let frequencies = [1.0e9];
    let row = NativeRow {
        uvw_m: [0.0; 3],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &[Complex32::new(1.0, 0.0); 2],
        weights: &[1.0, 1.0],
        flags: &[false; 2],
        row_flag: false,
        context: RowContext {
            antennas: [3, 3],
            ..context()
        },
    };
    let shape = DensityGridShape {
        width: 64,
        height: 64,
        planes: 1,
        padding: 0,
        increment_rad: [-2.0e-5, 2.0e-5],
        rule: DensityCellRule::Standard,
    };
    let mut density = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &shape, &mut density)
        .expect("density");
    assert_eq!(density.len(), 1);
    let mut placed = SampleBuffer::new(2);
    resampler
        .place(&operator, &natural(), &row, &mut placed)
        .expect("place");
    assert!(placed.is_empty());
}

#[test]
fn standard_density_cells_use_casa_single_precision_coordinates() {
    // CASA forms `Float f = ν/c`, then `Float u = uvw·f`, before it picks a
    // density cell (`VisImagingWeight`). On 4096 cells of 0.05″ this
    // baseline then lands in cell 1760; single-precision rounding of the
    // double u would put it in cell 1761.
    let uvw_m = [-13_719.554, 0.0, 0.0];
    let frequency_hz = 6_316_229_891.0;
    let casa = DensityUv::casa(uvw_m, frequency_hz);
    // The f32 values -289052.84375 and -289052.8125, one ulp apart.
    assert_eq!(casa.u, -289_052.84);
    assert_eq!((uvw_m[0] * frequency_hz / C) as f32, -289_052.8);

    let increment = 0.05_f64.to_radians() / 3600.0;
    let geometry = GridGeometry::new(
        ImageExtent {
            shape: [64, 64],
            increment_rad: [increment, increment],
            reference_pixel: [32, 32],
        },
        GridPadding::CasaComposite,
    )
    .expect("geometry");
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let cf = Spheroidal::new(&geometry, &polarization);
    let operator = MeasurementOperator::new(
        geometry,
        Basis::Constant,
        polarization,
        Box::new(cf),
        GridPrecision::F64,
    );
    let resampler = SpectralResampler::direct(Basis::Constant).expect("resampler");
    let frequencies = [frequency_hz];
    let values = [Complex32::new(1.0, 0.0); 2];
    let weights = [1.0; 2];
    let flags = [false; 2];
    let row = NativeRow {
        uvw_m,
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &frequencies,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let shape = DensityGridShape {
        width: 4096,
        height: 4096,
        planes: 1,
        padding: 0,
        increment_rad: [increment, increment],
        rule: DensityCellRule::Standard,
    };
    let mut density = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &shape, &mut density)
        .expect("density");
    let grid = build_density_grid(std::iter::once(density.block()), shape);
    assert_eq!(grid.plane(0)[2048 * 4096 + 1760], 1.0);
    assert_eq!(grid.plane(0)[2048 * 4096 + 1761], 0.0);

    // The imaging weight looks the sample up in the cell it was added to.
    let uniform = WeightingGeneration::density(grid, None, None, None).expect("uniform");
    let mut placed = SampleBuffer::new(2);
    resampler
        .place(&operator, &uniform, &row, &mut placed)
        .expect("place");
    assert_eq!(placed.len(), 1);
    assert_eq!(placed.block().weights_of(0), &[1.0, 1.0]);
}

#[test]
fn a_row_past_the_last_w_plane_is_not_placed() {
    // `wprojgrid.f`: `swp` rounds the plane of the unclamped w and `owp`
    // drops the row, so it reaches no grid, PSF or sumwt; the prediction
    // shares the placement. Plane 2 of four is kept, plane 4 is not.
    let polarization = PolarizationRouting::compile(&XX_YY, &STOKES_I).expect("routing");
    let planes = WPlanes::new(&geometry(), &polarization, WPlaneCount::Fixed(4)).expect("planes");
    let w_scale = planes.w_scale();
    let operator = MeasurementOperator::new(
        geometry(),
        Basis::Constant,
        polarization,
        Box::new(planes),
        GridPrecision::F64,
    );
    let resampler = SpectralResampler::direct(Basis::Constant).expect("direct");
    let frequencies = [1.0e9];
    let wavelength_m = C / frequencies[0];
    let values = [Complex32::new(1.0, 0.0); 2];
    let weights = [1.0_f32; 2];
    let flags = [false; 2];
    for (root, placed) in [(2.0_f64, 1), (4.0, 0)] {
        let row = NativeRow {
            uvw_m: [10.0, 10.0, root * root / w_scale * wavelength_m],
            phase_shift_m: 0.0,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz: &frequencies,
            values: &values,
            weights: &weights,
            flags: &flags,
            row_flag: false,
            context: context(),
        };
        let mut out = SampleBuffer::new(2);
        resampler
            .place(&operator, &natural(), &row, &mut out)
            .expect("place");
        assert_eq!(out.len(), placed, "root {root}");
    }
}
