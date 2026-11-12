// SPDX-License-Identifier: LGPL-3.0-or-later
//! Simulator time curves: local cubic stencils and retained natural splines.
use super::{
    ApplyExecutionError, ApplyInterpolationMode, CalibrationGrid, CalibrationSolution,
    CalibrationTableLayout, CalibrationTableLookup, ComplexSolution, FieldSelection, GainGrid,
};
use crate::interpolation::{Curve, sample_polar, spline_derivatives, unwrap_phase};
use casa_types::Complex32;
use std::collections::HashMap;
use std::path::Path;

#[path = "lookup_context.rs"]
mod context;
pub use context::TimeInterpolationOptions;
use context::{TimeQuery, TimeScope};

/// Amplitude/phase interpolation in time for simulator calibration tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
    /// The table cannot be read or is not complex. Spline time axes must be
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

pub(super) struct TimePlan {
    mode: TimeInterpolation,
    scope: TimeScope,
    series: HashMap<(i32, i32, i32), Vec<CalibrationSolution>>,
    splines: HashMap<(i32, i32, i32), Spline>,
    cycles: HashMap<(i32, i32, i32), Vec<f32>>,
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

impl TimePlan {
    fn solution(
        &self,
        path: &Path,
        query: TimeQuery,
    ) -> Result<Option<ComplexSolution>, ApplyExecutionError> {
        let TimeQuery {
            observation,
            scan,
            window: spw,
            antenna,
            time,
            frequency_hz,
        } = query;
        let segment = match self.scope {
            TimeScope::All => -1,
            TimeScope::Observation => observation,
            TimeScope::Scan => scan,
        };
        let Some((key, candidates)) = self
            .series
            .get_key_value(&(segment, spw, antenna))
            .or_else(|| self.series.get_key_value(&(segment, spw, -1)))
            .filter(|(_, s)| !s.is_empty())
        else {
            return Ok(None);
        };
        let reference = (candidates[0].time_seconds - 1.).floor();
        let time = relative(time, reference);
        let at = |index: usize| relative(candidates[index].time_seconds, reference);
        let upper = candidates.partition_point(|s| relative(s.time_seconds, reference) < time);
        let exact = upper == 0 || upper == candidates.len() || at(upper) == time;
        if exact || self.mode == TimeInterpolation::Nearest {
            let index = if exact {
                upper.min(candidates.len() - 1)
            } else if at(upper) - time < time - at(upper - 1) {
                upper
            } else {
                upper - 1
            };
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
                    || z.arg(),
                    |s| s.phases[(first + i) * receptors * channels + index],
                );
                points[i] = (relative(row.time_seconds, reference), z.norm(), phase);
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
                phase = (phase + self.cycles[key][lower * receptors * channels + index]) * scale;
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
    fn new(path: &Path, series: &[CalibrationSolution]) -> Result<Self, ApplyExecutionError> {
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
                phase = if i == 0 {
                    z.arg()
                } else {
                    unwrap_phase(phase, z.arg())
                };
                points.push((relative(row.time_seconds, reference), z.norm(), phase));
                result.phases[i * count + index] = phase;
            }
            for (i, second) in spline_derivatives(&points).into_iter().enumerate() {
                result.second[i * count + index] = second;
            }
        }
        Ok(result)
    }
}
