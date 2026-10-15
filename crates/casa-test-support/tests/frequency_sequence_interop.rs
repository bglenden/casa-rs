// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA forms an MFS image's spectral reference with ONE frequency converter
//! driven over the selected rows' sorted times (`MSUtil::getFreqRangeInSpw`).
//! Its aberration is anchored at the first time and extrapolated within 0.04
//! days, so the far end of a long track moves by about 1e-12. casa-rs's
//! `MFrequencyConverter` must reproduce that order-dependent result (#713).
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use std::sync::{Arc, OnceLock};

use casa_measures_data::MeasuresRuntime;
use casa_test_support::FrequencySequence;
use casa_test_support::measures_interop::MeasuresOracle;
use casa_types::measures::direction::{DirectionRef, MDirection};
use casa_types::measures::frequency::{FrequencyRef, MFrequencyConverter};
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame};

// A southern target from the VLA, at ALMA band 6.
const DIRECTION_RAD: [f64; 2] = [1.459_7, -0.087_3];
const VLA: [f64; 3] = [-1.878_283_2, 0.595_370_3, 2124.0];
const EDGES_HZ: [f64; 2] = [229.987_864_765e9, 230.019_903_713e9];

fn measures_runtime() -> Arc<MeasuresRuntime> {
    static RUNTIME: OnceLock<Arc<MeasuresRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(|| {
        Arc::new(
            MeasuresRuntime::open_discovered(Default::default()).expect("test measures runtime"),
        )
    }))
}

#[test]
fn an_ordered_converter_reproduces_casacores_over_a_two_hour_track() {
    // 120 one-minute epochs: the 0.04-day anchor refreshes once.
    let start_mjd = 60_234.25;
    let epochs_mjd = (0..120)
        .map(|minute| start_mjd + f64::from(minute) * 60.0 / 86_400.0)
        .collect::<Vec<_>>();
    let casacore = MeasuresOracle::frequency_convert_sequence(FrequencySequence {
        frequencies_hz: &EDGES_HZ,
        ref_in: "TOPO",
        ref_out: "LSRK",
        epochs_mjd: &epochs_mjd,
        direction_rad: DIRECTION_RAD,
        direction_ref: "J2000",
        observatory: VLA,
    })
    .expect("C++ ordered conversion");

    let direction =
        MDirection::from_angles(DIRECTION_RAD[0], DIRECTION_RAD[1], DirectionRef::J2000);
    let observatory = MPosition::new_wgs84(VLA[0], VLA[1], VLA[2]);
    let mut ordered = MFrequencyConverter::new(FrequencyRef::TOPO, FrequencyRef::LSRK);
    let (mut ordered_error_hz, mut fresh_error_hz) = (0.0_f64, 0.0_f64);
    for (index, &epoch_mjd) in epochs_mjd.iter().enumerate() {
        let frame = MeasFrame::new()
            .with_measures(measures_runtime())
            .with_direction(direction.clone())
            .with_epoch(MEpoch::from_mjd(epoch_mjd, EpochRef::UTC))
            .with_position(observatory.clone());
        for (edge, &hz) in EDGES_HZ.iter().enumerate() {
            let expected = casacore[index * EDGES_HZ.len() + edge];
            let rust = ordered
                .convert_hz(hz, &frame)
                .expect("ordered conversion")
                .hz();
            let fresh = MFrequencyConverter::new(FrequencyRef::TOPO, FrequencyRef::LSRK)
                .convert_hz(hz, &frame)
                .expect("fresh conversion")
                .hz();
            ordered_error_hz = ordered_error_hz.max((rust - expected).abs());
            fresh_error_hz = fresh_error_hz.max((fresh - expected).abs());
        }
    }

    eprintln!(
        "ordered: largest miss {ordered_error_hz:.3e} Hz; fresh per epoch: {fresh_error_hz:.3e} Hz"
    );
    assert!(
        ordered_error_hz < 1.0e-3,
        "ordered conversion is {ordered_error_hz} Hz from casacore's"
    );
    assert!(
        fresh_error_hz > 1.0e-2,
        "a fresh exact conversion per epoch should miss casacore's ordered result \
         by more than 10 mHz somewhere on the track, but misses by {fresh_error_hz} Hz"
    );
}
