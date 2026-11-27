// SPDX-License-Identifier: LGPL-3.0-or-later
//! A flagged antenna is identity; the valid partner still corrects the data.

use crate::common;

use casa_calibration::apply::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, execute_apply_from_path,
};
use casa_core::{ArrayValue, Complex32};
use casa_ms::{MeasurementSet, MsSelection};

#[test]
fn flagged_antenna_keeps_valid_partner_correction_and_weight_scale() {
    // CASA 6.7.6.14 same-gain probe: correcting a baseline with one flagged
    // gain preserves the valid antenna's division and squared-amplitude weight
    // factor. JonesDiag::invert/setMatByOk makes only the invalid gain unity.
    for calwt in [false, true] {
        for mode in [ApplyMode::CalOnly, ApplyMode::CalFlag] {
            let dir = tempfile::tempdir().unwrap();
            let ms_path = common::create_apply_fixture_ms(dir.path(), true);
            let ms = MeasurementSet::open(&ms_path).unwrap();
            let ArrayValue::Complex32(input) = ms
                .main_table()
                .cell_accessor(0, "DATA")
                .unwrap()
                .array()
                .unwrap()
            else {
                panic!("DATA type")
            };
            let input = input.clone();
            drop(ms);
            let rows =
                [(0, [2., 4.], false), (1, [99., 88.], true)].map(|(antenna, gains, flag)| {
                    common::SyntheticGainSolutionRow {
                        time_seconds: 100.,
                        field_id: 0,
                        spectral_window_id: 0,
                        antenna_id: antenna,
                        gains: gains
                            .into_iter()
                            .map(|value| Complex32::new(value, 0.))
                            .collect(),
                        flags: vec![flag; 2],
                    }
                });
            let gain = common::create_apply_gain_caltable(
                &dir.path().join("flagged.gcal"),
                &["TARGET0"],
                &rows,
            );
            let mut spec = ApplyCalibrationTableSpec::new(gain);
            spec.calwt = calwt;
            execute_apply_from_path(
                &ms_path,
                &ApplyPlanRequest {
                    selection: MsSelection::new().spw(&[0]),
                    apply_mode: mode,
                    parang: false,
                    calibration_tables: vec![spec],
                },
            )
            .unwrap();
            let ms = MeasurementSet::open(&ms_path).unwrap();
            let column = |name| {
                ms.main_table()
                    .cell_accessor(0, name)
                    .and_then(|cell| cell.array())
                    .unwrap()
            };
            let ArrayValue::Complex32(output) = column("CORRECTED_DATA") else {
                panic!("CORRECTED_DATA type")
            };
            let ArrayValue::Float32(weights) = column("WEIGHT") else {
                panic!("WEIGHT type")
            };
            let ArrayValue::Bool(flags) = column("FLAG") else {
                panic!("FLAG type")
            };
            for receptor in 0..2 {
                let gain = [2., 4.][receptor];
                assert_eq!(weights[[receptor]], if calwt { gain * gain } else { 1. });
                for channel in 0..input.shape()[1] {
                    assert_eq!(
                        output[[receptor, channel]],
                        input[[receptor, channel]] / gain
                    );
                    assert_eq!(flags[[receptor, channel]], mode == ApplyMode::CalFlag);
                }
            }
        }
    }
}
