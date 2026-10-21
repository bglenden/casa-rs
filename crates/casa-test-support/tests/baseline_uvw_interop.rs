// SPDX-License-Identifier: LGPL-3.0-or-later
//! `UvwFrame` against casacore C++: the ITRF-to-J2000 `MBaseline`
//! conversion and the `MVuvw` projection `NewMSSimulator::calcAntUVW` uses
//! for each antenna's uvw.
#![cfg(all(feature = "cpp-interop-tests", has_casacore_cpp))]

use std::sync::{Arc, OnceLock};

use casa_measures_data::MeasuresRuntime;
use casa_test_support::measures_interop::MeasuresOracle;
use casa_types::measures::{EpochRef, MEpoch, MPosition, MeasFrame, UvwFrame};

fn measures_runtime() -> Arc<MeasuresRuntime> {
    static RUNTIME: OnceLock<Arc<MeasuresRuntime>> = OnceLock::new();
    Arc::clone(RUNTIME.get_or_init(|| {
        Arc::new(
            MeasuresRuntime::open_discovered(Default::default()).expect("test measures runtime"),
        )
    }))
}

/// VLA and ALMA array centres (ITRF metres).
const OBSERVATORIES: [[f64; 3]; 2] = [
    [-1_601_185.4, -5_041_977.5, 3_554_875.9],
    [2_225_142.18, -5_440_307.37, -2_481_029.85],
];

/// Antenna offsets from the observatory, from tens of metres to tens of km.
const OFFSETS: [[f64; 3]; 4] = [
    [35.2, -12.4, 8.1],
    [-410.0, 1_250.0, -600.0],
    [7_300.0, -2_100.0, 4_400.0],
    [-21_000.0, -15_000.0, 26_000.0],
];

/// J2000 directions: equatorial, southern, northern and near the pole.
const DIRECTIONS: [[f64; 2]; 4] = [
    [0.0, 0.0],
    [2.617_993_877_991_494, -0.523_598_775_598_298_9],
    [4.712_388_980_384_69, 0.593_411_945_678_072],
    [1.0, 1.55],
];

/// UT1 epochs from 2020 to 2026, at different times of day.
const EPOCHS_MJD: [f64; 4] = [58_849.0, 59_580.375, 60_390.812_5, 61_120.25];

/// The largest uvw difference allowed, as a fraction of the baseline length:
/// 6 mas, well under a millimetre on a 30 km baseline. First measured
/// (2026-10-21) at 1.7e-8 with both sides' Earth-orientation data covering
/// the epoch.
const RELATIVE_TOLERANCE: f64 = 3e-8;

/// Whether casa-rs's and casacore's polar motion at `epoch_mjd` agree to
/// 1 mas. Where one side's IERS data end before the epoch, it uses no polar
/// motion and the two differ by up to about 0.5 arcsec, a data difference
/// rather than a conversion one.
fn polar_motion_agrees(measures: &Arc<MeasuresRuntime>, epoch_mjd: f64) -> bool {
    let frame = MeasFrame::new().with_measures(measures.clone());
    let rust = frame
        .polar_motion_for_mjd(epoch_mjd)
        .expect("casa-rs polar motion")
        .unwrap_or((0.0, 0.0));
    let (_, xp, yp) = MeasuresOracle::eop_query(epoch_mjd).expect("casacore polar motion");
    (rust.0 - xp).abs() < 1e-3 && (rust.1 - yp).abs() < 1e-3
}

#[test]
fn uvw_frame_matches_casacore_mbaseline_and_mvuvw() {
    let measures = measures_runtime();
    let mut largest = 0.0f64;
    let mut compared = 0;
    for observatory in OBSERVATORIES {
        let position = MPosition::new_itrf(observatory[0], observatory[1], observatory[2]);
        for epoch in EPOCHS_MJD {
            if !polar_motion_agrees(&measures, epoch) {
                println!("epoch {epoch}: the two sides' polar-motion data differ; skipped");
                continue;
            }
            compared += 1;
            for direction in DIRECTIONS {
                let frame = UvwFrame::new(
                    direction,
                    MEpoch::from_mjd(epoch, EpochRef::UT1),
                    &position,
                    measures.clone(),
                )
                .expect("uvw frame");
                for offset in OFFSETS {
                    let antenna = [
                        observatory[0] + offset[0],
                        observatory[1] + offset[1],
                        observatory[2] + offset[2],
                    ];
                    let (_, expected) = MeasuresOracle::simulator_baseline_uvw(
                        observatory,
                        antenna,
                        direction,
                        epoch,
                    )
                    .expect("casacore baseline uvw");
                    let actual = frame.baseline_uvw([
                        observatory[0] - antenna[0],
                        observatory[1] - antenna[1],
                        observatory[2] - antenna[2],
                    ]);
                    let difference = (0..3)
                        .map(|axis| (actual[axis] - expected[axis]).abs())
                        .fold(0.0, f64::max);
                    let length = offset.iter().map(|value| value * value).sum::<f64>().sqrt();
                    largest = largest.max(difference / length);
                    assert!(
                        difference < RELATIVE_TOLERANCE * length,
                        "observatory {observatory:?} epoch {epoch} direction {direction:?} \
                         offset {offset:?}: casa-rs {actual:?}, casacore {expected:?}"
                    );
                }
            }
        }
    }
    assert!(
        compared >= 4,
        "too few epochs with common polar-motion data"
    );
    println!("largest relative uvw difference {largest:.3e} over {compared} observatory epochs");
}
