// SPDX-License-Identifier: LGPL-3.0-or-later
//! Calibration-table lookup: a table's solution for an antenna, spectral
//! window, field and time, found and interpolated in time as the apply
//! executor finds it, for callers that apply a table to visibilities of
//! their own (`casa-simulation`'s `sm.setapply`).
//!
//! [`CalibrationTableLookup`] loads a complex (`CPARAM`) table as the
//! executor does and answers [`CalibrationTableLookup::solution`]: the
//! solutions of the row's field, or of the table's sole field, nearest in
//! time or interpolated linearly in amplitude and phase. With
//! [`FieldSelection::AnyField`] it takes every field's solutions as one
//! series in time instead, as CASA's simulator applies a table. The time
//! interpolation the executor shares lives here.
//!
//! Loading reads the whole table; [`CalibrationTableLookup::rows_on_disk`],
//! [`CalibrationTableLookup::loading_bytes`] and
//! [`CalibrationTableLookup::kept_bytes`] size it beforehand, for a caller
//! that admits the memory first.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_tables::Table;
use casa_types::Complex32;

use crate::execute::{
    ApplyExecutionError, CalibrationGrid, CalibrationSolution, DelayGrid, GainGrid,
    LoadedCalibrationTable, load_calibration_table,
};
use crate::model::CalibrationParameterFamily;
use crate::plan::{ApplyCalibrationTablePlan, ApplyCalibrationTableSpec, ApplyInterpolationMode};
use crate::summary::summarize_table;

/// Which fields' solutions a row takes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSelection {
    /// The row's field's, or the table's sole field's: the apply
    /// executor's default.
    RowField,
    /// Every field's, as one series in time: CASA's `Simulator::setapply`
    /// ignores fields and does not partition the table by them (#893
    /// review).
    AnyField,
}

/// Heap loading a complex table holds per row at most: the row as
/// casa-tables reads the table, and the solution it becomes. Measured at
/// about 3,300 bytes (the #893 review's 9,996-row probe, and the setapply
/// law in `casa-simulation`'s `session_memory/apply.rs`), with a quarter
/// more for the allocator.
pub const LOOKUP_LOADING_ROW_BYTES: u64 = 4_096;

/// Heap a loaded lookup keeps per row: the solution's grid, its place in
/// the table's series, and in the series across fields. Measured at about
/// 320 bytes with the loading figure, with a quarter more.
pub const LOOKUP_KEPT_ROW_BYTES: u64 = 400;

/// A complex calibration table, loaded to look its solutions up.
pub struct CalibrationTableLookup {
    path: PathBuf,
    table: LoadedCalibrationTable,
    subtype: String,
    interp: ApplyInterpolationMode,
    /// For [`FieldSelection::AnyField`]: each (spectral window, antenna)'s
    /// solutions across the fields, in time order.
    across_fields: Option<HashMap<(i32, i32), Vec<CalibrationSolution>>>,
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
    /// up with `interp`, each row taking its own field's
    /// ([`FieldSelection::RowField`]).
    ///
    /// # Errors
    ///
    /// The table cannot be read, or holds other than complex (`CPARAM`)
    /// solutions.
    pub fn open(
        path: impl AsRef<Path>,
        interp: ApplyInterpolationMode,
    ) -> Result<Self, ApplyExecutionError> {
        Self::open_with(path, interp, FieldSelection::RowField)
    }

    /// The rows of the table at `path`, read without loading them.
    ///
    /// # Errors
    ///
    /// The table's control file cannot be read.
    pub fn rows_on_disk(path: impl AsRef<Path>) -> Result<usize, ApplyExecutionError> {
        let path = path.as_ref();
        Table::rows_on_disk(path).map_err(|error| {
            ApplyExecutionError::UnsupportedCalibrationTable {
                path: path.display().to_string(),
                reason: error.to_string(),
            }
        })
    }

    /// Heap loading a table of `rows` rows holds at most, beside what the
    /// lookup then keeps.
    #[must_use]
    pub const fn loading_bytes(rows: usize) -> u64 {
        rows as u64 * LOOKUP_LOADING_ROW_BYTES
    }

    /// Heap a lookup of a table of `rows` rows keeps.
    #[must_use]
    pub const fn kept_bytes(rows: usize) -> u64 {
        rows as u64 * LOOKUP_KEPT_ROW_BYTES
    }

    /// [`Self::open`], each row taking the solutions `fields` selects.
    ///
    /// # Errors
    ///
    /// As [`Self::open`].
    pub fn open_with(
        path: impl AsRef<Path>,
        interp: ApplyInterpolationMode,
        fields: FieldSelection,
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
        let table = load_calibration_table(&plan, None)?;
        let across_fields = (fields == FieldSelection::AnyField).then(|| across_fields(&table));
        Ok(Self {
            path: path.to_path_buf(),
            table,
            subtype,
            interp,
            across_fields,
        })
    }

    /// The table's subtype (`G Jones`, `D Jones`, `B Jones`, …).
    #[must_use]
    pub fn subtype(&self) -> &str {
        &self.subtype
    }

    /// The solution of `antenna` in spectral window `spw` for a row of
    /// field `field` at `time_s`: from the field's solutions, or the
    /// table's sole field's (or, with [`FieldSelection::AnyField`], every
    /// field's), nearest in time or interpolated linearly in amplitude and
    /// phase. `None` where the table has none.
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
        let grid = match &self.across_fields {
            Some(series) => self.across_fields_grid(series, spw, antenna, time_s)?,
            None => self
                .table
                .lookup(field, spw, antenna, time_s, self.interp)?,
        };
        let Some(grid) = grid else {
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

impl CalibrationTableLookup {
    /// The grid of `antenna` in window `spw` at `time_s` from the series
    /// across fields: the nearest solution (the earlier of two as near), or
    /// the bracketing pair interpolated as the executor interpolates.
    fn across_fields_grid(
        &self,
        series: &HashMap<(i32, i32), Vec<CalibrationSolution>>,
        spw: i32,
        antenna: i32,
        time_s: f64,
    ) -> Result<Option<Arc<CalibrationGrid>>, ApplyExecutionError> {
        let Some(candidates) = series
            .get(&(spw, antenna))
            .or_else(|| series.get(&(spw, -1)))
            .filter(|candidates| !candidates.is_empty())
        else {
            return Ok(None);
        };
        // The first solution at or after the time, and the last at or
        // before it.
        let after = candidates.partition_point(|solution| solution.time_seconds < time_s);
        let through = candidates.partition_point(|solution| solution.time_seconds <= time_s);
        match self.interp {
            ApplyInterpolationMode::Nearest | ApplyInterpolationMode::NearestLinear => {
                let nearest = match (after.checked_sub(1), candidates.get(after)) {
                    (Some(before), Some(later))
                        if (later.time_seconds - time_s).abs()
                            < (time_s - candidates[before].time_seconds).abs() =>
                    {
                        after
                    }
                    (Some(before), _) => before,
                    (None, _) => 0,
                };
                Ok(Some(Arc::clone(&candidates[nearest].grid)))
            }
            ApplyInterpolationMode::Linear => {
                let last = candidates.len() - 1;
                let lower = through.checked_sub(1).unwrap_or(after.min(last));
                let upper = after.min(last).max(lower);
                interpolate_time_linear(&self.path, &candidates[lower..=upper], time_s).map(Some)
            }
        }
    }
}

/// Each (spectral window, antenna)'s solutions across the fields of
/// `table`, in time order, fields in order at equal times.
fn across_fields(table: &LoadedCalibrationTable) -> HashMap<(i32, i32), Vec<CalibrationSolution>> {
    let mut by_window: HashMap<(i32, i32), Vec<(i32, &CalibrationSolution)>> = HashMap::new();
    for (&(field, spw, antenna), solutions) in &table.solutions {
        by_window
            .entry((spw, antenna))
            .or_default()
            .extend(solutions.iter().map(|solution| (field, solution)));
    }
    by_window
        .into_iter()
        .map(|(key, mut solutions)| {
            solutions.sort_by(|a, b| {
                a.1.time_seconds
                    .total_cmp(&b.1.time_seconds)
                    .then(a.0.cmp(&b.0))
            });
            let series = solutions
                .into_iter()
                .map(|(_, solution)| CalibrationSolution {
                    time_seconds: solution.time_seconds,
                    grid: Arc::clone(&solution.grid),
                })
                .collect();
            (key, series)
        })
        .collect()
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
