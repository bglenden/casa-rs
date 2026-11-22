// SPDX-License-Identifier: LGPL-3.0-or-later
//! Shared calibration time curves: local cubic stencils and retained natural splines.
//! Complex parameters use amplitude/phase; real parameters preserve their sign.
use super::{
    ApplyExecutionError, ApplyInterpolationMode, CalibrationGrid, CalibrationSolution,
    CalibrationTableLayout, CalibrationTableLookup, ComplexSolution, FieldSelection, GainGrid,
    LoadedCalibrationTable, across_fields,
};
use crate::interpolation::{Curve, sample_polar, spline_derivatives, unwrap_phase};
use casa_types::Complex32;
use std::collections::HashMap;
use std::path::Path;

#[path = "lookup_context.rs"]
mod context;
pub use context::TimeInterpolationOptions;
pub(crate) use context::TimeQuery;
use context::TimeScope;

/// Amplitude/phase interpolation in time for calibration tables.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
pub enum TimeInterpolation {
    /// Nearest time, clamped to the endpoint outside the table's time range.
    Nearest,
    /// Linear amplitude and shortest phase between adjacent solutions.
    Linear,
    /// Cubic polynomial through four neighboring time samples.
    Cubic,
    /// Natural cubic spline through all samples, with continuous phase.
    Spline,
}

impl CalibrationTableLayout {
    /// Additional retained heap for the selected time interpolator.
    /// Spline coefficients are built once, then shared by all lookups.
    #[must_use]
    pub fn time_kept_bytes(self, mode: TimeInterpolation) -> u64 {
        if mode == TimeInterpolation::Spline {
            (self.rows as u64)
                .saturating_mul(128)
                .saturating_add((self.values as u64).saturating_mul(24))
        } else {
            0
        }
    }

    /// Temporary heap while forming time coefficients, beside retained heap.
    /// One parameter's curve is solved at a time; its length cannot exceed
    /// the physical row count. Cubic queries use a four-element stack stencil.
    #[must_use]
    pub fn time_loading_bytes(self, mode: TimeInterpolation) -> u64 {
        if mode == TimeInterpolation::Spline {
            (self.rows as u64).saturating_mul(48)
        } else {
            0
        }
    }
}

impl CalibrationTableLookup {
    /// Load a simulator table, treating all fields as one time series per
    /// spectral window and antenna. Cubic follows CASA's four-time local
    /// stencils, including flagged samples; only the adjacent time flags are
    /// combined. Spline corrects CASA's uninitialized-interpolator crash and
    /// incomplete natural-spline back-substitution (#1053).
    ///
    /// Callers admit the table layout's loading/kept bytes and its additional
    /// time loading/kept bytes before calling, retaining the kept charges.
    ///
    /// # Errors
    /// The parameter table cannot be read. Spline time axes must be
    /// strictly increasing after CASA's relative Float time conversion, and
    /// their parameter shapes must match. Interior cubic queries require at
    /// least four times; exact and endpoint queries do not.
    pub fn open_across_fields(
        path: impl AsRef<Path>,
        mode: TimeInterpolation,
    ) -> Result<Self, ApplyExecutionError> {
        Self::open_for_simulation(path, TimeInterpolationOptions::new(mode))
    }
}

type TimeKey = (i32, i32, i32, i32);
type TimeSeries = HashMap<TimeKey, Vec<CalibrationSolution>>;
type SelectedSeries<'a> = (&'a TimeKey, &'a [CalibrationSolution]);

pub(crate) struct TimePlan {
    mode: TimeInterpolation,
    real: bool,
    scope: TimeScope,
    series: TimeSeries,
    splines: HashMap<TimeKey, Spline>,
    cycles: HashMap<TimeKey, Vec<f32>>,
    reference_hz: Vec<f64>,
}

fn invalid(path: &Path, reason: &str) -> ApplyExecutionError {
    ApplyExecutionError::UnsupportedCalibrationTable {
        path: path.display().to_string(),
        reason: reason.to_string(),
    }
}

fn grid<'a>(
    path: &Path,
    solution: &'a CalibrationSolution,
) -> Result<&'a GainGrid, ApplyExecutionError> {
    match solution.grid.as_ref() {
        CalibrationGrid::Complex(grid) => Ok(grid),
        _ => Err(invalid(
            path,
            "time interpolation requires complex parameters",
        )),
    }
}

fn value(grid: &GainGrid, index: usize) -> (Complex32, bool) {
    let receptor = index / grid.channel_count;
    let channel = index % grid.channel_count;
    if grid.values.ndim() == 1 {
        (grid.values[[receptor]], grid.flags[[receptor]])
    } else {
        (
            grid.values[[receptor, channel]],
            grid.flags[[receptor, channel]],
        )
    }
}

fn relative(time: f64, reference: f64) -> f64 {
    f64::from((time - reference) as f32)
}

struct Registration {
    reference: f64,
    time: f64,
    upper: usize,
    exact: bool,
}

impl Registration {
    fn new(rows: &[CalibrationSolution], time: f64) -> Self {
        let reference = (rows[0].time_seconds - 1.).floor();
        let time = relative(time, reference);
        let upper = rows.partition_point(|row| relative(row.time_seconds, reference) < time);
        let exact = upper == 0
            || upper == rows.len()
            || relative(rows[upper].time_seconds, reference) == time;
        Self {
            reference,
            time,
            upper,
            exact,
        }
    }
    fn nearest(&self, rows: &[CalibrationSolution]) -> usize {
        if self.exact {
            self.upper.min(rows.len() - 1)
        } else {
            // CTTimeInterp1 subtracts the registered Float epochs before
            // comparing the distances. A rounded tie selects the earlier row.
            let later =
                (relative(rows[self.upper].time_seconds, self.reference) - self.time) as f32;
            let earlier =
                (self.time - relative(rows[self.upper - 1].time_seconds, self.reference)) as f32;
            if later < earlier {
                self.upper
            } else {
                self.upper - 1
            }
        }
    }
}

impl TimePlan {
    fn candidates(&self, query: &TimeQuery) -> Option<SelectedSeries<'_>> {
        let (observation, scan) = self.scope.coordinates(query.observation, query.scan);
        self.series
            .get_key_value(&(observation, scan, query.window, query.antenna))
            .or_else(|| {
                self.series
                    .get_key_value(&(observation, scan, query.window, -1))
            })
            .filter(|(_, rows)| !rows.is_empty())
            .map(|(key, rows)| (key, rows.as_slice()))
    }

    /// Exact, nearest and endpoint queries can share an immutable payload.
    /// Real families can share it when the caller can use the raw parameters.
    pub(crate) fn registered_grid(
        &self,
        query: &TimeQuery,
        allow_raw_real: bool,
    ) -> Option<std::sync::Arc<CalibrationGrid>> {
        if (self.real && !allow_raw_real) || query.frequency_hz.is_some() {
            return None;
        }
        let (_, rows) = self.candidates(query)?;
        let registration = Registration::new(rows, query.time);
        (registration.exact || self.mode == TimeInterpolation::Nearest)
            .then(|| std::sync::Arc::clone(&rows[registration.nearest(rows)].grid))
    }

    pub(crate) fn solution(
        &self,
        path: &Path,
        query: TimeQuery,
    ) -> Result<Option<ComplexSolution>, ApplyExecutionError> {
        let Some((key, candidates)) = self.candidates(&query) else {
            return Ok(None);
        };
        let spw = query.window;
        let frequency_hz = query.frequency_hz;
        let registration = Registration::new(candidates, query.time);
        let Registration {
            reference,
            time,
            upper,
            exact,
        } = registration;
        if exact || self.mode == TimeInterpolation::Nearest {
            let index = registration.nearest(candidates);
            let mut output = ComplexSolution::of(grid(path, &candidates[index])?);
            if let Some(frequency) = frequency_hz.filter(|_| self.cycles.contains_key(key)) {
                let scale = self.phase_scale(path, spw, frequency)?;
                for (parameter, value) in output.values.iter_mut().enumerate() {
                    let phase = value.arg()
                        + self.cycles[key][index * output.receptors * output.channels + parameter];
                    *value = Complex32::from_polar(value.norm(), phase * scale);
                }
            }
            return Ok(Some(output));
        }
        let lower = upper - 1;
        let first = if self.mode == TimeInterpolation::Cubic {
            if candidates.len() < 4 {
                return Err(invalid(
                    path,
                    "cubic time interpolation requires four samples for an interior output",
                ));
            }
            lower.saturating_sub(1).min(candidates.len() - 4)
        } else {
            lower
        };
        let stencil = &candidates[first
            ..first
                + if self.mode == TimeInterpolation::Cubic {
                    4
                } else {
                    2
                }];
        let base = grid(path, &candidates[lower])?;
        for row in stencil {
            if grid(path, row)?.values.shape() != base.values.shape() {
                return Err(invalid(
                    path,
                    "time interpolation requires matching parameter shapes",
                ));
            }
        }
        if !stencil.windows(2).all(|pair| {
            relative(pair[0].time_seconds, reference) < relative(pair[1].time_seconds, reference)
        }) {
            return Err(invalid(path, "time samples must be strictly increasing"));
        }
        let high = grid(path, &candidates[upper])?;
        let (receptors, channels) = (base.receptor_count, base.channel_count);
        let spline = self.splines.get(key);
        let phase_scale = frequency_hz
            .filter(|_| self.cycles.contains_key(key))
            .map(|frequency| self.phase_scale(path, spw, frequency))
            .transpose()?;
        let mut values = Vec::with_capacity(receptors * channels);
        let mut flags = Vec::with_capacity(receptors * channels);
        for index in 0..receptors * channels {
            let mut points = [(0., 0., 0.); 4];
            for (i, row) in stencil.iter().enumerate() {
                let z = value(grid(path, row)?, index).0;
                let phase = spline.map_or_else(
                    || if self.real { 0. } else { z.arg() },
                    |s| s.phases[(first + i) * receptors * channels + index],
                );
                points[i] = (
                    relative(row.time_seconds, reference),
                    if self.real { z.re } else { z.norm() },
                    phase,
                );
                if i > 0 && spline.is_none() {
                    points[i].2 = unwrap_phase(points[i - 1].2, points[i].2);
                }
            }
            let result = if self.mode == TimeInterpolation::Cubic {
                sample_polar(&points, time, Curve::Cubic, false, &[])
            } else if self.mode == TimeInterpolation::Linear {
                sample_polar(&points[..2], time, Curve::Linear, false, &[])
            } else {
                let spline = spline.expect("spline coefficients");
                let left = lower * receptors * channels + index;
                let right = left + receptors * channels;
                sample_polar(
                    &points[..2],
                    time,
                    Curve::Spline,
                    false,
                    &[spline.second[left], spline.second[right]],
                )
            };
            let (amplitude, mut phase) = result.expect("nonempty time stencil");
            if let Some(scale) = phase_scale {
                // A spline already retains the globally unwrapped phase. Local
                // linear/cubic stencils need CASA's lower-bracket cycle offset.
                if spline.is_none() {
                    phase += self.cycles[key][lower * receptors * channels + index];
                }
                phase *= scale;
            }
            values.push(Complex32::from_polar(amplitude, phase));
            flags.push(value(base, index).1 || value(high, index).1);
        }
        Ok(Some(ComplexSolution {
            receptors,
            channels,
            values,
            flags,
        }))
    }
}

struct Spline {
    phases: Vec<f32>,
    second: Vec<[f64; 2]>,
}

impl Spline {
    fn new(
        path: &Path,
        series: &[CalibrationSolution],
        real: bool,
    ) -> Result<Self, ApplyExecutionError> {
        let base = grid(path, &series[0])?;
        let count = base.receptor_count * base.channel_count;
        let size = series
            .len()
            .checked_mul(count)
            .ok_or_else(|| invalid(path, "time coefficient count overflows usize"))?;
        let reference = (series[0].time_seconds - 1.).floor();
        for pair in series.windows(2) {
            if relative(pair[0].time_seconds, reference)
                >= relative(pair[1].time_seconds, reference)
            {
                return Err(invalid(
                    path,
                    "spline time samples must be strictly increasing",
                ));
            }
        }
        for row in series {
            if grid(path, row)?.values.shape() != base.values.shape() {
                return Err(invalid(
                    path,
                    "time interpolation requires matching parameter shapes",
                ));
            }
        }
        let mut result = Self {
            phases: vec![0.; size],
            second: vec![[0.; 2]; size],
        };
        let mut points = Vec::with_capacity(series.len());
        for index in 0..count {
            points.clear();
            let mut phase = 0.;
            for (i, row) in series.iter().enumerate() {
                let z = value(grid(path, row)?, index).0;
                phase = if real {
                    0.
                } else if i == 0 {
                    z.arg()
                } else {
                    unwrap_phase(phase, z.arg())
                };
                points.push((
                    relative(row.time_seconds, reference),
                    if real { z.re } else { z.norm() },
                    phase,
                ));
                result.phases[i * count + index] = phase;
            }
            for (i, second) in spline_derivatives(&points).into_iter().enumerate() {
                result.second[i * count + index] = second;
            }
        }
        Ok(result)
    }
}
