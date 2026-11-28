// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_core::measures::MeasFrame;
use casa_core::measures::direction::{DirectionRef, MDirection};
use casa_core::measures::frequency::FrequencyRef;

use super::{
    FineGrid, NativeRow, Order, PreparedFrequencyConversion, RowContext, SpectralAxis,
    SpectralKernel, SpectralResampler,
};

fn reporting_axis() -> (SpectralAxis, PreparedFrequencyConversion) {
    let frame = MeasFrame::new().with_direction(MDirection::from_angles(
        5.233_697_01,
        0.710_938_054,
        DirectionRef::J2000,
    ));
    let convert = |source, target| {
        PreparedFrequencyConversion::new(source, target, Some(&frame), Some(&frame))
            .expect("Doppler conversion")
    };
    let to_lsrk = convert(FrequencyRef::BARY, FrequencyRef::LSRK);
    let to_reporting = convert(FrequencyRef::LSRK, FrequencyRef::BARY);
    let first = to_lsrk.convert_hz(1_375_000_000.0);
    let second = to_lsrk.convert_hz(1_425_000_000.0);
    let axis = SpectralAxis::new(first, second - first, 6)
        .expect("working axis")
        .with_reporting_conversion([0.0, 1_375_000_000.0, 50_000_000.0], to_lsrk, to_reporting);
    (axis, to_lsrk)
}

#[test]
fn direct_sampling_converts_each_reported_centre() {
    let (axis, to_lsrk) = reporting_axis();
    let native = [0.0, axis.increment_hz()];
    let mut grid = FineGrid::compile(&axis, &native, Order::Linear, None).expect("equal width");
    let expected = to_lsrk.convert_hz(1_525_000_000.0);
    assert_eq!(grid.frequency_hz(3), expected);
}

#[test]
fn padded_density_measures_its_own_first_spacing() {
    let (axis, to_lsrk) = reporting_axis();
    let padded = axis.padded(1).expect("padded density axis");
    let first = to_lsrk.convert_hz(1_325_000_000.0);
    let second = to_lsrk.convert_hz(1_375_000_000.0);
    assert_eq!(padded.first_hz(), first);
    assert_eq!(padded.increment_hz(), second - first);
}

#[test]
fn padded_density_rounds_after_conversion_to_reported_pixel() {
    let (axis, to_lsrk) = reporting_axis();
    let padded = axis.padded(1).expect("padded density axis");
    let midpoint = to_lsrk.convert_hz(1_450_000_000.0);
    assert_eq!(padded.nearest_channel(midpoint), Some(3));
}

#[test]
fn width_tie_uses_fewer_samples_without_rounding_a_nonintegral_ratio() {
    let axis = SpectralAxis::new(1.025e9, 100.0e6, 2)
        .expect("wide axis")
        .with_exact_width_from_below();
    let native_step = 50.0e6_f64;
    for (spacing, expected) in [
        (native_step.next_up(), 2),
        (native_step, 2),
        (native_step.next_down(), 4),
    ] {
        for sign in [1.0, -1.0] {
            let grid = FineGrid::compile(&axis, &[0.0, spacing * sign], Order::Linear, None)
                .expect("fine grid");
            assert_eq!(grid.count(), expected, "spacing {spacing}, sign {sign}");
        }
    }
}

#[test]
fn the_first_lsrk_fine_point_must_be_in_the_native_span() {
    // refim_eptwochan: the first TOPO integration converted to LSRK.
    // The first point of the fine grid reaches the first visibility
    // frequency, as in the CASA run that retains the first plane (#823).
    let axis = SpectralAxis::new(999_988_750.387_257_2, 499_994_375.193_629_15, 2).unwrap();
    let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
    let first = 999_988_750.387_257_3_f64;
    for (edge, expected) in [(first, &[0, 1][..]), (first.next_up(), &[1][..])] {
        let frequencies = [edge, 1_499_983_125.580_886_1, 1_999_977_500.774_515];
        let row = NativeRow {
            selection_centres_hz: None,
            uvw_m: [10.0, 10.0, 0.0],
            phase_shift_m: 0.0,
            pointing_offset_rad: [0.0; 2],
            frequencies_hz: &frequencies,
            values: &[num_complex::Complex32::new(1.0, 0.0); 3],
            weights: &[1.0; 3],
            flags: &[false; 3],
            row_flag: false,
            context: RowContext {
                antennas: [0, 1],
                antenna_types: [0, 0],
                time_s: 0.0,
                field: 0,
                spectral_window: 0,
                parallactic_angle_rad: [0.0, 0.0],
                native_frequency_ratio: 1.0,
                original_uvw_m: None,
            },
        };
        let mut planes = Vec::new();
        resampler.for_each_sample(&row, |plane, _, _| planes.push(plane));
        assert_eq!(planes, expected, "first visibility frequency {edge}");
    }
}
