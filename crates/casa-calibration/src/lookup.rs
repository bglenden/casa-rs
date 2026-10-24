// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-table lookup: a table's solution for an antenna, spectral
//! window, field and time, found and interpolated in time as the apply
//! executor finds it, for callers that apply a table to visibilities of
//! their own (`casa-simulation`'s `sm.setapply`).
//!
//! [`CalibrationTableLookup`] loads a complex (`CPARAM`) table as the
//! executor does and answers [`CalibrationTableLookup::solution`]: the
//! solutions of the row's field, or of the table's sole field, nearest in
//! time or interpolated linearly in amplitude and phase. The time
//! interpolation the executor shares lives here.

use std::path::Path;
use std::sync::Arc;

use casa_types::Complex32;

use crate::execute::{
    ApplyExecutionError, CalibrationGrid, CalibrationSolution, DelayGrid, GainGrid,
    LoadedCalibrationTable, load_calibration_table,
};
use crate::model::CalibrationParameterFamily;
use crate::plan::{ApplyCalibrationTablePlan, ApplyCalibrationTableSpec, ApplyInterpolationMode};
use crate::summary::summarize_table;

/// A complex calibration table, loaded to look its solutions up.
pub struct CalibrationTableLookup {
    table: LoadedCalibrationTable,
    subtype: String,
    interp: ApplyInterpolationMode,
}

/// One antenna's solution: CPARAM's values and FLAG, per receptor and
/// channel.
#[derive(Debug, Clone, PartialEq)]
pub struct ComplexSolution {
    /// Receptors (CPARAM's first axis).
    pub receptors: usize,
    /// Channels (its second axis): one for a term that does not depend on
    /// frequency.
    pub channels: usize,
    /// `[receptor · channels + channel]`.
    pub values: Vec<Complex32>,
    /// `[receptor · channels + channel]`.
    pub flags: Vec<bool>,
}

impl ComplexSolution {
    fn of(grid: &GainGrid) -> Self {
        let (receptors, channels) = (grid.receptor_count, grid.channel_count);
        let mut values = Vec::with_capacity(receptors * channels);
        let mut flags = Vec::with_capacity(receptors * channels);
        for receptor in 0..receptors {
            for channel in 0..channels {
                let (value, flag) = if grid.values.ndim() == 1 {
                    (grid.values[[receptor]], grid.flags[[receptor]])
                } else {
                    (
                        grid.values[[receptor, channel]],
                        grid.flags[[receptor, channel]],
                    )
                };
                values.push(value);
                flags.push(flag);
            }
        }
        Self {
            receptors,
            channels,
            values,
            flags,
        }
    }

    /// The value of `receptor` in `channel`.
    #[must_use]
    pub fn value(&self, receptor: usize, channel: usize) -> Complex32 {
        self.values[receptor * self.channels + channel]
    }
}

impl CalibrationTableLookup {
    /// Load the complex calibration table at `path` to look its solutions
    /// up with `interp`.
    ///
    /// # Errors
    ///
    /// The table cannot be read, or holds other than complex (`CPARAM`)
    /// solutions.
    pub fn open(
        path: impl AsRef<Path>,
        interp: ApplyInterpolationMode,
    ) -> Result<Self, ApplyExecutionError> {
        let path = path.as_ref();
        let unsupported = |reason: String| ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason,
        };
        let summary = summarize_table(path).map_err(|error| unsupported(error.to_string()))?;
        if summary.parameter_family != CalibrationParameterFamily::Complex
            || summary.table_subtype == "BPOLY"
        {
            return Err(unsupported(format!(
                "a {} table is not complex (CPARAM)",
                summary.table_subtype
            )));
        }
        let subtype = summary.table_subtype.clone();
        let plan = ApplyCalibrationTablePlan {
            spec: ApplyCalibrationTableSpec {
                interp,
                ..ApplyCalibrationTableSpec::new(path)
            },
            applicable_selected_row_count: 0,
            summary,
            resolved_gainfield: None,
            resolved_nearest_gainfields: Vec::new(),
            spw_mapping: Vec::new(),
            calibration_spectral_windows: Vec::new(),
            interp,
            calwt: false,
        };
        Ok(Self {
            table: load_calibration_table(&plan, None)?,
            subtype,
            interp,
        })
    }

    /// The table's subtype (`G Jones`, `D Jones`, `B Jones`, …).
    #[must_use]
    pub fn subtype(&self) -> &str {
        &self.subtype
    }

    /// The solution of `antenna` in spectral window `spw` for a row of
    /// field `field` at `time_s`: from the field's solutions, or the
    /// table's sole field's, nearest in time or interpolated linearly in
    /// amplitude and phase. `None` where the table has none.
    ///
    /// # Errors
    ///
    /// The solutions cannot be interpolated (shapes differ in time).
    pub fn solution(
        &self,
        field: i32,
        spw: i32,
        antenna: i32,
        time_s: f64,
    ) -> Result<Option<ComplexSolution>, ApplyExecutionError> {
        let Some(grid) = self
            .table
            .lookup(field, spw, antenna, time_s, self.interp)?
        else {
            return Ok(None);
        };
        match grid.as_ref() {
            CalibrationGrid::Complex(grid) => Ok(Some(ComplexSolution::of(grid))),
            _ => Err(ApplyExecutionError::UnsupportedCalibrationTable {
                path: String::new(),
                reason: "a complex table gave other than complex solutions".to_string(),
            }),
        }
    }
}

pub(crate) fn interpolate_time_linear(
    path: &Path,
    candidates: &[CalibrationSolution],
    time_seconds: f64,
) -> Result<Arc<CalibrationGrid>, ApplyExecutionError> {
    let mut sorted = candidates.iter().collect::<Vec<_>>();
    sorted.sort_by(|a, b| a.time_seconds.total_cmp(&b.time_seconds));

    let lower = sorted
        .iter()
        .rev()
        .find(|solution| solution.time_seconds <= time_seconds)
        .copied();
    let upper = sorted
        .iter()
        .find(|solution| solution.time_seconds >= time_seconds)
        .copied();

    match (lower, upper) {
        (Some(lower), Some(upper))
            if (upper.time_seconds - lower.time_seconds).abs() > f64::EPSILON =>
        {
            let fraction = ((time_seconds - lower.time_seconds)
                / (upper.time_seconds - lower.time_seconds)) as f32;
            match (lower.grid.as_ref(), upper.grid.as_ref()) {
                (CalibrationGrid::Complex(lower), CalibrationGrid::Complex(upper)) => {
                    if lower.values.shape() != upper.values.shape() {
                        return Err(ApplyExecutionError::UnsupportedInterpolation {
                            path: path.display().to_string(),
                            interp: ApplyInterpolationMode::Linear,
                            reason: "time interpolation requires matching parameter shapes"
                                .to_string(),
                        });
                    }
                    let mut values = lower.values.clone();
                    for (value, upper_value) in values.iter_mut().zip(upper.values.iter()) {
                        *value = interpolate_gain_amplitude_phase(*value, *upper_value, fraction);
                    }
                    let mut flags = lower.flags.clone();
                    for (flag, upper_flag) in flags.iter_mut().zip(upper.flags.iter()) {
                        *flag = *flag || *upper_flag;
                    }
                    Ok(Arc::new(CalibrationGrid::Complex(GainGrid {
                        receptor_count: lower.receptor_count,
                        channel_count: lower.channel_count,
                        values,
                        flags,
                    })))
                }
                (CalibrationGrid::Delay(lower), CalibrationGrid::Delay(upper)) => {
                    if lower.values_ns.shape() != upper.values_ns.shape() {
                        return Err(ApplyExecutionError::UnsupportedInterpolation {
                            path: path.display().to_string(),
                            interp: ApplyInterpolationMode::Linear,
                            reason: "time interpolation requires matching parameter shapes"
                                .to_string(),
                        });
                    }
                    let mut values_ns = lower.values_ns.clone();
                    for (value, upper_value) in values_ns.iter_mut().zip(upper.values_ns.iter()) {
                        *value = *value + (*upper_value - *value) * fraction;
                    }
                    let mut flags = lower.flags.clone();
                    for (flag, upper_flag) in flags.iter_mut().zip(upper.flags.iter()) {
                        *flag = *flag || *upper_flag;
                    }
                    Ok(Arc::new(CalibrationGrid::Delay(DelayGrid {
                        receptor_count: lower.receptor_count,
                        channel_count: lower.channel_count,
                        values_ns,
                        flags,
                    })))
                }
                (
                    CalibrationGrid::Antpos(_)
                    | CalibrationGrid::GainCurve(_)
                    | CalibrationGrid::Opacity(_),
                    _,
                )
                | (
                    _,
                    CalibrationGrid::Antpos(_)
                    | CalibrationGrid::GainCurve(_)
                    | CalibrationGrid::Opacity(_),
                ) => Err(ApplyExecutionError::UnsupportedInterpolation {
                    path: path.display().to_string(),
                    interp: ApplyInterpolationMode::Linear,
                    reason:
                        "VLA prior tables are row-geometry dependent and use nearest table rows"
                            .to_string(),
                }),
                _ => Err(ApplyExecutionError::UnsupportedInterpolation {
                    path: path.display().to_string(),
                    interp: ApplyInterpolationMode::Linear,
                    reason: "time interpolation requires matching calibration parameter families"
                        .to_string(),
                }),
            }
        }
        (Some(lower), _) => Ok(lower.grid.clone()),
        (_, Some(upper)) => Ok(upper.grid.clone()),
        _ => Err(ApplyExecutionError::UnsupportedInterpolation {
            path: path.display().to_string(),
            interp: ApplyInterpolationMode::Linear,
            reason: "no calibration rows available for linear interpolation".to_string(),
        }),
    }
}

pub(crate) fn interpolate_gain_amplitude_phase(
    lower: Complex32,
    upper: Complex32,
    fraction: f32,
) -> Complex32 {
    let lower_amp = lower.norm();
    let upper_amp = upper.norm();
    let amp = lower_amp + (upper_amp - lower_amp) * fraction;
    let lower_phase = lower.arg();
    let mut upper_phase = upper.arg();
    while upper_phase > lower_phase + std::f32::consts::PI {
        upper_phase -= 2.0 * std::f32::consts::PI;
    }
    while upper_phase < lower_phase - std::f32::consts::PI {
        upper_phase += 2.0 * std::f32::consts::PI;
    }
    let phase = lower_phase + (upper_phase - lower_phase) * fraction;
    Complex32::new(amp * phase.cos(), amp * phase.sin())
}
