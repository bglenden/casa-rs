// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// On a moving-source AW cube, the imaging baseline is already scaled to
/// stored wavelengths. Density accumulation undoes that scale and the
/// cube weight lookup uses the stored frequency and original baseline.
#[test]
fn rest_aw_density_accumulates_rest_and_looks_up_stored_wavelengths() {
    let frequency = 1.0e9;
    let ratio = 1.01;
    let axis = SpectralAxis::new(frequency, 5.0e7, 1).expect("axis");
    let resampler =
        SpectralResampler::channel_local(axis, SpectralKernel::Cubic).with_rest_aw_density();
    let cell = 5.0e-5;
    let increments = [-cell, cell];
    let operator = operator_on(
        geometry_of([1024, 1024], increments),
        GridPrecision::F64,
        resampler.basis(),
        &XX_YY,
        &STOKES_I,
    );
    let cell_metres = C / (1024.0 * cell * frequency);
    let row = NativeRow {
        uvw_m: [49.8 * cell_metres * ratio, 0.0, 0.0],
        phase_shift_m: 0.0,
        pointing_offset_rad: [0.0; 2],
        frequencies_hz: &[frequency],
        values: &[Complex32::new(1.0, 0.0); 2],
        weights: &[1.0; 2],
        flags: &[false; 2],
        row_flag: false,
        context: RowContext {
            native_frequency_ratio: ratio,
            original_uvw_m: Some([50.2 * cell_metres / ratio, 0.0, 0.0]),
            ..context()
        },
    };
    let shape = DensityGridShape {
        width: 1024,
        height: 1024,
        increment_rad: increments,
        ..density_shape(&resampler, 0)
    };
    let mut density = SampleBuffer::new(1);
    resampler
        .place_density(&operator, &row, &shape, &mut density)
        .expect("REST density");
    assert_eq!(density.len(), 1);
    assert!((density.placements()[0].u - 49.8 / (1024.0 * cell)).abs() < 1e-12);
    let grid = build_density_grid(std::iter::once(density.block()), shape);
    let uniform = WeightingGeneration::density(grid, None, None, None).expect("uniform");
    let mut out = SampleBuffer::new(2);
    // The image-wavelength baseline rounds into the neighboring cell.
    let query = NativeRow {
        uvw_m: [48.9 * cell_metres * ratio, 0.0, 0.0],
        ..row
    };
    resampler
        .place(&operator, &uniform, &query, &mut out)
        .expect("stored wavelength lookup");
    assert_eq!(out.len(), 1);
    assert_eq!(out.block().weights_of(0), &[0.5; 2]);
}
