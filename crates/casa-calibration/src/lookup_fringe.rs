// SPDX-License-Identifier: LGPL-3.0-or-later
//! Rate-aware fringe phases, following CTRateAwareTimeInterp1. Other
//! parameters and flags retain the ordinary signed-parameter interpolator.
use super::*;
use crate::execute::CalibrationGrid;

impl CalibrationTableLookup {
    /// Interpolate a supplied fringe table, advancing its phases to the
    /// requested epoch before combining adjacent unit phasors. `frequency_hz`
    /// is the calibration window's first channel, as in CASA's rate wrapper.
    ///
    /// # Errors
    /// The table is not an eight-parameter real fringe table, or its curves
    /// or reference frequency are invalid.
    pub fn simulation_fringe_solution(
        &self,
        observation: i32,
        scan: i32,
        window: i32,
        antenna: i32,
        time: f64,
        frequency_hz: f64,
    ) -> Result<Option<ComplexSolution>, ApplyExecutionError> {
        let Some(mut solution) =
            self.simulation_solution(observation, scan, window, antenna, time, None)?
        else {
            return Ok(None);
        };
        if solution.receptors != 8
            || solution.channels != 1
            || !frequency_hz.is_finite()
            || frequency_hz <= 0.
        {
            return Err(Self::table_error(
                &self.path,
                "invalid fringe parameters or reference frequency",
            ));
        }
        let phases = self.fringe_phases(observation, scan, window, antenna, time, frequency_hz)?;
        for (receptor, phase) in phases.into_iter().enumerate() {
            solution.values[4 * receptor] = Complex32::new(phase, 0.);
        }
        Ok(Some(solution))
    }
}

pub(super) fn advanced_phase(
    row: &CalibrationSolution,
    receptor: usize,
    time: f64,
    frequency: f64,
    path: &Path,
) -> Result<f64, ApplyExecutionError> {
    let CalibrationGrid::Complex(grid) = row.grid.as_ref() else {
        return Err(CalibrationTableLookup::table_error(
            path,
            "fringe parameters are not real curves",
        ));
    };
    if grid.receptor_count != 8 || grid.channel_count != 1 {
        return Err(CalibrationTableLookup::table_error(
            path,
            "fringe parameters have an invalid shape",
        ));
    }
    let phase = f64::from(grid.values[[4 * receptor, 0]].re);
    let rate = f64::from(grid.values[[4 * receptor + 2, 0]].re);
    Ok(phase + std::f64::consts::TAU * rate * frequency * (time - row.time_seconds))
}
