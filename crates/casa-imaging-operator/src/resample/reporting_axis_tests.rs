// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_types::measures::MeasFrame;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::FrequencyRef;

use super::{FineGrid, PreparedFrequencyConversion, SpectralAxis};

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
    let grid = FineGrid::compile(axis, axis.increment_hz()).expect("equal width");
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
