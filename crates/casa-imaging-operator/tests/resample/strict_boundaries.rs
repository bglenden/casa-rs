// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// CASA's interpolation flags a channel genuinely outside the row's
/// native frequency range, even by one representable step (#1198).
#[test]
fn a_one_step_out_of_band_channel_is_not_extrapolated() {
    for ascending in [true, false] {
        let first = if ascending { 1.0e9_f64 } else { 1.2e9_f64 };
        let increment = if ascending { 50.0e6 } else { -50.0e6 };
        let axis = SpectralAxis::new(first, increment, 5).expect("axis");
        for kernel in [SpectralKernel::Linear, SpectralKernel::Cubic] {
            let resampler = SpectralResampler::channel_local(axis, kernel);
            let operator = operator(GridPrecision::F64, resampler.basis(), &XX_YY, &STOKES_I);
            let mut frequencies = (0..5)
                .map(|channel| first + f64::from(channel) * increment)
                .collect::<Vec<_>>();
            if ascending {
                frequencies[0] = frequencies[0].next_up();
                frequencies[4] = frequencies[4].next_down();
            } else {
                frequencies[0] = frequencies[0].next_down();
                frequencies[4] = frequencies[4].next_up();
            }
            let row = NativeRow {
                uvw_m: [10.0, 10.0, 0.0],
                phase_shift_m: 0.0,
                pointing_offset_rad: [0.0; 2],
                frequencies_hz: &frequencies,
                values: &[Complex32::new(1.0, 0.0); 10],
                weights: &[1.0; 10],
                flags: &[false; 10],
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
                [1, 2, 3],
                "ascending {ascending}, kernel {kernel:?}"
            );
        }
    }
}
