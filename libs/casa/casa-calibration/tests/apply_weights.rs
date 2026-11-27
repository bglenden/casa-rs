// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calwt resets from SIGMA, so reapplying the same gains is idempotent.

use crate::common;

use casa_calibration::apply::{
    ApplyCalibrationTableSpec, ApplyMode, ApplyPlanRequest, execute_apply_from_path,
};
use casa_core::{ArrayValue, Complex32, Value};
use casa_ms::{MeasurementSet, MsSelection};

#[test]
fn reapplying_calwt_uses_sigma_and_preserves_sigma_and_disabled_weights() {
    // CASA's VisBufferImpl2::resetWeightsUsingSigma: sigma=2 gives 1/4;
    // two antenna amplitudes of 2 then give (1/4)*2²*2² = 4, every call.
    for spectral in [false, true] {
        for calwt in [false, true] {
            for mode in [ApplyMode::CalOnly, ApplyMode::CalFlag] {
                let dir = tempfile::tempdir().unwrap();
                let path = common::create_apply_fixture_ms_with_options(dir.path(), true, spectral);
                let mut ms = MeasurementSet::open(&path).unwrap();
                for (column, value) in [("WEIGHT", 3.), ("SIGMA", 2.)] {
                    ms.main_table_mut()
                        .cell_accessor_mut(0, column)
                        .unwrap()
                        .set(Value::Array(ArrayValue::from_f32_vec(vec![value; 2])))
                        .unwrap();
                }
                let original_spectrum = spectral.then(|| {
                    ms.main_table()
                        .cell_accessor(0, "WEIGHT_SPECTRUM")
                        .and_then(|cell| cell.array())
                        .unwrap()
                        .clone()
                });
                ms.save().unwrap();
                drop(ms);
                let rows = [0, 1].map(|antenna| common::SyntheticGainSolutionRow {
                    time_seconds: 100.,
                    field_id: 0,
                    spectral_window_id: 0,
                    antenna_id: antenna,
                    gains: vec![Complex32::new(2., 0.); 2],
                    flags: vec![false; 2],
                });
                let gain = common::create_apply_gain_caltable(
                    &dir.path().join("gain.gcal"),
                    &["TARGET0"],
                    &rows,
                );
                let mut spec = ApplyCalibrationTableSpec::new(gain);
                spec.calwt = calwt;
                let request = ApplyPlanRequest {
                    selection: MsSelection::new().spw(&[0]),
                    apply_mode: mode,
                    parang: false,
                    calibration_tables: vec![spec],
                };
                for _ in 0..2 {
                    execute_apply_from_path(&path, &request).unwrap();
                    let ms = MeasurementSet::open(&path).unwrap();
                    let column = |name| {
                        ms.main_table()
                            .cell_accessor(0, name)
                            .and_then(|cell| cell.array())
                            .unwrap()
                    };
                    assert_eq!(
                        column("WEIGHT"),
                        &ArrayValue::from_f32_vec(vec![if calwt { 4. } else { 3. }; 2])
                    );
                    assert_eq!(column("SIGMA"), &ArrayValue::from_f32_vec(vec![2.; 2]));
                    if spectral {
                        let spectrum = column("WEIGHT_SPECTRUM");
                        if calwt {
                            let ArrayValue::Float32(spectrum) = spectrum else {
                                panic!("WEIGHT_SPECTRUM type")
                            };
                            assert!(spectrum.iter().all(|value| *value == 4.));
                        } else {
                            assert_eq!(spectrum, original_spectrum.as_ref().unwrap());
                        }
                    }
                }
            }
        }
    }
}
