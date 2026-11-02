// SPDX-License-Identifier: LGPL-3.0-or-later
//! The CASA rule `regrid` ports, pinned with the spectral axes CASA
//! 6.7.6.14's tclean wrote (`niter=0`, `outframe='TOPO'`, so that no frame
//! conversion enters) for `refim_point.ms` and
//! `refim_point_descendingfreqs.ms` from casatestdata: spectral window 0,
//! 20 TOPO channels of 50 MHz from 1.0 to 1.95 GHz, `restfreq='1.25GHz'`.

use super::super::casa_regridded_image_axis_hz;
use super::*;
use casa_types::measures::doppler::DopplerRef;

/// `refim_point.ms` spectral window 0 as its `CHAN_FREQ` stores it.
fn ascending_hz() -> Vec<f64> {
    let mut frequencies_hz = (0..20)
        .map(|channel| 1.0e9 + f64::from(channel) * 5.0e7)
        .collect::<Vec<_>>();
    frequencies_hz[0] = 999_999_999.999_999_9;
    frequencies_hz[1] = 1_049_999_999.999_999_9;
    frequencies_hz
}

/// `refim_point_descendingfreqs.ms` spectral window 0.
fn descending_hz() -> Vec<f64> {
    let mut frequencies_hz = ascending_hz();
    frequencies_hz.reverse();
    frequencies_hz
}

const WIDTHS_HZ: [f64; 20] = [5.0e7; 20];
const REST_HZ: f64 = 1.25e9;

fn request(
    quantity: RegridQuantity,
    start: Option<f64>,
    width: Option<f64>,
    nchan: usize,
) -> RegridRequest {
    RegridRequest {
        quantity,
        start,
        width,
        nchan,
        rest_frequency_hz: REST_HZ,
    }
}

fn channels(request: &RegridRequest, frequencies_hz: &[f64]) -> CasaChannelFrequencies {
    casa_channel_frequencies(request, frequencies_hz, &WIDTHS_HZ).unwrap()
}

/// The image axis tclean writes for `request` with `veltype`.
fn image_axis(request: &RegridRequest, frequencies_hz: &[f64], veltype: DopplerRef) -> Vec<f64> {
    casa_regridded_image_axis_hz(&channels(request, frequencies_hz), request.nchan, veltype)
}

/// CASA's linear axis: `crval + k·cdelt`.
fn linear(first_hz: f64, increment_hz: f64, nchan: usize) -> Vec<f64> {
    (0..nchan)
        .map(|plane| first_hz + plane as f64 * increment_hz)
        .collect()
}

#[test]
fn radio_velocity_start_below_the_data_clamps_the_band_centre_to_the_lower_edge() {
    // test_cube_11: start='11991.7km/s' is 1199999992.995 Hz, and the band
    // of ten 50 MHz channels below it would be centred at 974999993 Hz,
    // under the data's lower edge at 975 MHz. `regridChanBounds` resets
    // the centre to that edge, which puts the first channel at 1.2 GHz
    // exactly; it is not a snap to the nearest data channel.
    let grid = request(RegridQuantity::RadioVelocity, Some(11_991_700.0), None, 10);
    let exact_hz = REST_HZ * (1.0 - 11_991_700.0 / C);
    assert!((exact_hz - 1_199_999_992.995_154).abs() < 1e-3);
    assert_eq!(
        image_axis(&grid, &ascending_hz(), DopplerRef::RADIO),
        linear(1.2e9, -5.0e7, 10)
    );
}

#[test]
fn radio_velocity_start_inside_the_data_is_kept_exactly() {
    // test_cube_11 at start='11977.6km/s' (test_cube_12's start, in TOPO):
    // the band centre lies inside the data, so the first channel is the
    // start's radio-velocity frequency, not the data channel at 1.2 GHz.
    let grid = request(RegridQuantity::RadioVelocity, Some(11_977_600.0), None, 10);
    assert_eq!(
        image_axis(&grid, &ascending_hz(), DopplerRef::RADIO),
        linear(1_200_058_783.666_932_8, -5.0e7, 10)
    );
}

#[test]
fn radio_velocity_width_sets_the_order_and_the_channel_width() {
    // test_cube_9 and test_cube_10: no start, so the band starts at the
    // data's lower edge; a positive velocity width runs down in frequency.
    let down = request(RegridQuantity::RadioVelocity, None, Some(23_983_400.0), 10);
    assert_eq!(
        image_axis(&down, &ascending_hz(), DopplerRef::RADIO),
        linear(1_925_000_133.092_070_6, -100_000_014.009_691_72, 10)
    );
    let up = request(RegridQuantity::RadioVelocity, None, Some(-23_983_400.0), 10);
    assert_eq!(
        image_axis(&up, &ascending_hz(), DopplerRef::RADIO),
        linear(1_025_000_007.004_846_6, 100_000_014.009_691_72, 10)
    );
    let both = request(
        RegridQuantity::RadioVelocity,
        Some(11_991_700.0),
        Some(11_991_700.0),
        6,
    );
    assert_eq!(
        image_axis(&both, &ascending_hz(), DopplerRef::RADIO),
        linear(1_199_999_992.995_154_4, -50_000_007.004_846_57, 6)
    );
}

#[test]
fn frequency_start_is_kept_exactly_and_its_edge_clamped_into_the_data() {
    // test_cube_6 and test_cube_8.
    let start = request(RegridQuantity::Frequency, Some(1.1e9), None, 10);
    assert_eq!(
        image_axis(&start, &ascending_hz(), DopplerRef::RADIO),
        linear(1.1e9, 5.0e7, 10)
    );
    let downward = request(RegridQuantity::Frequency, Some(1.5e9), Some(-5.0e7), 10);
    assert_eq!(
        image_axis(&downward, &ascending_hz(), DopplerRef::RADIO),
        linear(1.5e9, -5.0e7, 10)
    );
    // test_cube_5: no start, so the band starts at the data's lower edge.
    let wide = request(RegridQuantity::Frequency, None, Some(1.0e8), 10);
    assert_eq!(
        image_axis(&wide, &ascending_hz(), DopplerRef::RADIO),
        linear(1.025e9, 1.0e8, 10)
    );
    // A frequency grid's starting edge (not its centre) is kept within the
    // data: below the data it moves up to the lower edge, above it down to
    // the upper edge.
    let low = request(RegridQuantity::Frequency, Some(0.9e9), None, 5);
    assert_eq!(
        image_axis(&low, &ascending_hz(), DopplerRef::RADIO),
        linear(1.0e9, 5.0e7, 5)
    );
    let high = request(RegridQuantity::Frequency, Some(2.5e9), None, 5);
    assert_eq!(
        image_axis(&high, &ascending_hz(), DopplerRef::RADIO),
        linear(2.0e9, 5.0e7, 5)
    );
}

#[test]
fn descending_data_keep_the_requested_order() {
    // test_cube_descendF5 and test_cube_descendF10, in TOPO.
    let frequency = request(RegridQuantity::Frequency, Some(1.5e9), None, 10);
    assert_eq!(
        image_axis(&frequency, &descending_hz(), DopplerRef::RADIO),
        linear(1.5e9, 5.0e7, 10)
    );
    let velocity = request(
        RegridQuantity::RadioVelocity,
        Some(-107_920_698.0),
        None,
        10,
    );
    assert_eq!(
        image_axis(&velocity, &descending_hz(), DopplerRef::RADIO),
        linear(1_699_980_874.769_037_7, -5.0e7, 10)
    );
}

#[test]
fn a_single_channel_takes_its_width_as_the_increment() {
    let velocity = request(RegridQuantity::RadioVelocity, Some(11_991_700.0), None, 1);
    let result = channels(&velocity, &ascending_hz());
    assert_eq!(result.centres_hz, vec![1_199_999_992.995_154]);
    assert_eq!(result.steps_hz, vec![-5.0e7]);
    let frequency = request(RegridQuantity::Frequency, Some(1.1e9), None, 1);
    let result = channels(&frequency, &ascending_hz());
    assert_eq!(result.centres_hz, vec![1.1e9]);
    assert_eq!(result.steps_hz, vec![5.0e7]);
}

#[test]
fn optical_velocity_grid_is_truncated_to_the_data() {
    // test_cube_13 in TOPO: CASA's table holds four of the eight channels.
    let grid = request(
        RegridQuantity::OpticalVelocity,
        Some(-41_347_800.0),
        Some(20_000_000.0),
        8,
    );
    let table = [
        1_253_244_052.817_556_9,
        1_176_783_981.410_044_4,
        1_109_124_277.594_536_8,
        1_048_827_218.620_406_3,
    ];
    assert_eq!(channels(&grid, &ascending_hz()).centres_hz, table);
    // The tabular coordinate's header: the first entry and the mean step,
    // CASA's reference value and increment.
    let axis = image_axis(&grid, &ascending_hz(), DopplerRef::Z);
    assert_eq!(axis, linear(table[0], -68_138_944.732_383_53, 8));
}

#[test]
fn optical_velocity_grids_without_start_or_with_a_negative_width() {
    let no_start = request(RegridQuantity::OpticalVelocity, None, Some(20_000_000.0), 8);
    assert_eq!(
        channels(&no_start, &ascending_hz()).centres_hz,
        [
            1_607_164_356.204_674_7,
            1_475_000_000.0,
            1_362_949_519.199_134_3,
            1_266_740_743.405_724,
            1_183_232_574.136_740_2,
        ]
    );
    let upward = request(
        RegridQuantity::OpticalVelocity,
        Some(40_000_000.0),
        Some(-15_000_000.0),
        6,
    );
    assert_eq!(
        channels(&upward, &ascending_hz()).centres_hz,
        [
            1_075_316_900.294_067_9,
            1_125_030_445.004_171_4,
            1_179_566_288.607_190_4,
            1_239_662_328.922_543,
            1_306_215_217.140_808_6,
            1_380_325_513.633_546_4,
        ]
    );
}

#[test]
fn a_width_narrower_than_every_data_channel_is_refused() {
    let grid = request(RegridQuantity::Frequency, Some(1.1e9), Some(1.0e7), 5);
    let error = casa_channel_frequencies(&grid, &ascending_hz(), &WIDTHS_HZ).unwrap_err();
    assert!(error.to_string().contains("smaller than smallest original"));
}

#[test]
fn unordered_data_and_missing_rest_frequencies_are_refused() {
    let grid = request(RegridQuantity::Frequency, Some(1.1e9), None, 2);
    let error = casa_channel_frequencies(&grid, &[1.1e9, 1.0e9, 1.05e9], &[5.0e7; 3]).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("neither in ascending nor in descending")
    );
    let velocity = RegridRequest {
        rest_frequency_hz: 0.0,
        ..request(RegridQuantity::RadioVelocity, Some(0.0), None, 2)
    };
    let error = casa_channel_frequencies(&velocity, &ascending_hz(), &WIDTHS_HZ).unwrap_err();
    assert!(error.to_string().contains("rest frequency"));
}
