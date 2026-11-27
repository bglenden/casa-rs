// SPDX-License-Identifier: LGPL-3.0-or-later
//! Evaluate legacy GSPLINE calibration at each visibility's time and frequency.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use super::{ApplyExecutionError, CalibrationGrid, LoadedCalibrationTable};
use crate::plan::ApplyCalibrationTablePlan;
pub(super) use crate::table::LegacySplineTable as SplineTable;

pub(super) fn load(
    plan: &ApplyCalibrationTablePlan,
) -> Result<LoadedCalibrationTable, ApplyExecutionError> {
    Ok(LoadedCalibrationTable {
        baseline: false,
        path: plan.spec.path.clone(),
        interp: plan.interp,
        supports_calwt: false,
        engine: None,
        antpos_tropo: None,
        ionosphere: None,
        sky_exposures: None,
        spline: Some(
            SplineTable::open(&plan.spec.path, plan.calibration_spectral_windows.len())
                .map_err(|reason| invalid(&plan.spec.path, reason))?,
        ),
        solutions: HashMap::new(),
        callibrary: None,
        time_plans: HashMap::new(),
        missing_grids: HashMap::new(),
        time_parameter_family: None,
        phase_delay: false,
    })
}

pub(super) fn grid(
    spline: &SplineTable,
    field: i32,
    antenna: i32,
    time: f64,
    frequency_hz: Option<f64>,
) -> Option<Arc<CalibrationGrid>> {
    let mut solution =
        spline.solution(antenna, usize::try_from(field).ok()?, time, frequency_hz)?;
    for (value, flag) in solution.values.iter_mut().zip(&mut solution.flags) {
        if !value.is_finite() {
            *flag = true;
            *value = Default::default();
        }
    }
    Some(Arc::new(CalibrationGrid::Complex(
        solution.into_gain_grid(),
    )))
}

fn invalid(path: &Path, reason: String) -> ApplyExecutionError {
    ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason,
    }
}
