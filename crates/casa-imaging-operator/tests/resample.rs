// SPDX-License-Identifier: LGPL-3.0-or-later
//! The spectral resampler: direct, nearest and CASA linear channel mapping
//! with flags, weights and the phase-centre phasor.

mod common;

use casa_imaging_model::{CorrelationType, PolarizationCoordinate};
use casa_imaging_operator::{
    Basis, GridPrecision, NativeRow, OperatorError, RowContext, SampleBuffer, SpectralKernel,
    SpectralResampler, WeightingGeneration,
};
use common::operator;
use num_complex::Complex32;

const C: f64 = 299_792_458.0;
const XX_YY: [CorrelationType; 2] = [CorrelationType::LinearXx, CorrelationType::LinearYy];
const STOKES_I: [PolarizationCoordinate; 1] = [PolarizationCoordinate::StokesI];

fn context() -> RowContext {
    RowContext {
        time_s: 0.0,
        antennas: [0, 1],
        parallactic_angle_rad: [0.0, 0.0],
        field: 0,
        pointing_offset_rad: [0.0, 0.0],
    }
}

fn natural() -> WeightingGeneration {
    WeightingGeneration::Natural { taper: None }
}

#[test]
fn direct_sampling_places_every_unflagged_channel_on_plane_zero() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let resampler =
        SpectralResampler::new(Basis::Constant, &[], SpectralKernel::Nearest).expect("resampler");
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
}

#[test]
fn samples_whose_support_leaves_the_grid_are_dropped() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let resampler =
        SpectralResampler::new(Basis::Constant, &[], SpectralKernel::Nearest).expect("resampler");
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
    let resampler = SpectralResampler::new(basis, &[], SpectralKernel::Nearest).expect("resampler");
    let frequencies = [0.9e9, 1.1e9];
    let values = [Complex32::new(1.0, 0.0); 4];
    let weights = [1.0; 4];
    let flags = [false; 4];
    let row = NativeRow {
        uvw_m: [10.0, 10.0, 0.0],
        phase_shift_m: 0.0,
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
    let basis = Basis::ChannelLocal { planes: 3 };
    let operator = operator(GridPrecision::F64, basis, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::new(basis, &[1.0e9, 1.1e9, 1.2e9], SpectralKernel::Nearest)
        .expect("resampler");
    let frequencies = [0.94e9, 0.96e9, 1.04e9, 1.149e9, 1.151e9, 1.26e9];
    let values = [Complex32::new(1.0, 0.0); 12];
    let weights = [1.0; 12];
    let flags = [false; 12];
    let row = NativeRow {
        uvw_m: [10.0, 10.0, 0.0],
        phase_shift_m: 0.0,
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
    let planes = out.placements().iter().map(|p| p.plane).collect::<Vec<_>>();
    assert_eq!(
        planes,
        [0, 0, 1, 2],
        "0.94 and 1.26 GHz lie outside the axis"
    );
    let placed = out.placements();
    assert!(
        (placed[0].u - 10.0 * 0.96e9 / C).abs() < 1e-12,
        "samples keep their native frequency"
    );
}

#[test]
fn linear_mapping_interpolates_values_keeps_the_nearer_weight_and_ors_flags() {
    let basis = Basis::ChannelLocal { planes: 2 };
    let operator = operator(GridPrecision::F64, basis, &XX_YY, &STOKES_I);
    let resampler = SpectralResampler::new(basis, &[1.05e9, 1.15e9], SpectralKernel::Linear)
        .expect("resampler");
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
    // Midpoint ties keep the left weight: 2 then 4; values (1+3)/2·2 and (3+5)/2·4.
    assert_eq!(block.weights_of(0), &[2.0, 2.0]);
    assert_eq!(block.weights_of(1), &[4.0, 4.0]);
    assert!((block.values_of(0)[0].re - 4.0).abs() < 1e-5);
    assert!((block.values_of(0)[1].re - 40.0).abs() < 1e-5);
    assert!((block.values_of(1)[0].re - 16.0).abs() < 1e-5);

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

    let single = [1.0e9];
    let row = NativeRow {
        frequencies_hz: &single,
        values: &values[..2],
        weights: &weights[..2],
        flags: &flags[..2],
        ..row
    };
    out.clear();
    assert!(matches!(
        resampler.place(&operator, &natural(), &row, &mut out),
        Err(OperatorError::SpectralAxis { .. })
    ));
}

#[test]
fn wide_output_channels_use_casa_fine_grid_points() {
    // Output channels twice as wide as native ones: two fine points per
    // output channel at the quarter positions.
    let basis = Basis::ChannelLocal { planes: 2 };
    let operator = operator(GridPrecision::F64, basis, &XX_YY, &STOKES_I);
    let resampler =
        SpectralResampler::new(basis, &[1.1e9, 1.3e9], SpectralKernel::Linear).expect("resampler");
    let frequencies = [1.0e9, 1.1e9, 1.2e9, 1.3e9, 1.4e9];
    let values = [Complex32::new(1.0, 0.0); 10];
    let weights = [1.0; 10];
    let flags = [false; 10];
    let row = NativeRow {
        uvw_m: [10.0, 0.0, 0.0],
        phase_shift_m: 0.0,
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
    let fine = out
        .placements()
        .iter()
        .map(|p| ((p.u * C / 10.0 / 1.0e6).round() as i64, p.plane))
        .collect::<Vec<_>>();
    assert_eq!(fine, [(1050, 0), (1150, 0), (1250, 1), (1350, 1)]);
}

#[test]
fn density_pass_carries_the_unpolarized_weight_without_a_support_test() {
    let operator = operator(GridPrecision::F64, Basis::Constant, &XX_YY, &STOKES_I);
    let resampler =
        SpectralResampler::new(Basis::Constant, &[], SpectralKernel::Nearest).expect("resampler");
    let frequencies = [1.0e9];
    let values = [Complex32::new(1.0, 0.0); 2];
    let weights = [2.0, 6.0];
    let flags = [false; 2];
    let far = 39.0 * 625.0 * C / 1.0e9;
    let row = NativeRow {
        uvw_m: [far, 0.0, 0.0],
        phase_shift_m: 0.0,
        frequencies_hz: &frequencies,
        values: &values,
        weights: &weights,
        flags: &flags,
        row_flag: false,
        context: context(),
    };
    let mut out = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &mut out)
        .expect("density");
    assert_eq!(out.len(), 1);
    assert_eq!(out.block().weights_of(0), &[4.0]);
    let mut wrong = SampleBuffer::new(2);
    assert!(matches!(
        resampler.place_density(&operator, &row, &mut wrong),
        Err(OperatorError::NativeRow { .. })
    ));
}
