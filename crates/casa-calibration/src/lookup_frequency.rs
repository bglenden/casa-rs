// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's frequency resampling interpolates amplitude and unwrapped phase.
use crate::{ApplyExecutionError, CalibrationTableLookup, ComplexSolution};
use num_complex::Complex32;

/// Frequency interpolation for a complex calibration grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrequencyInterpolation {
    /// Nearest valid frequency sample, with nearest endpoint extrapolation.
    Nearest,
    /// Linear amplitude and unwrapped phase, skipping flagged inputs.
    Linear,
    /// A local polynomial through four valid samples.
    Cubic,
    /// A natural cubic spline through valid samples.
    Spline,
    /// Nearest interpolation retaining registered input flags.
    NearestFlag,
    /// Linear interpolation retaining registered input flags.
    LinearFlag,
    /// Cubic interpolation retaining registered input flags.
    CubicFlag,
    /// Spline interpolation retaining registered input flags.
    SplineFlag,
}

impl FrequencyInterpolation {
    fn base(self) -> Self {
        match self {
            Self::NearestFlag => Self::Nearest,
            Self::LinearFlag => Self::Linear,
            Self::CubicFlag => Self::Cubic,
            Self::SplineFlag => Self::Spline,
            mode => mode,
        }
    }
    fn retains_flags(self) -> bool {
        self != self.base()
    }
}

impl ComplexSolution {
    /// Resample each receptor to the requested data frequencies.
    ///
    /// CASA's `CTPatchedInterp::resampleInFreq` first removes flagged
    /// input channels, unwraps phase and interpolates amplitude/phase
    /// separately. Without a valid input the output remains flagged.
    /// One valid input and frequencies beyond either edge use its nearest
    /// endpoint. A single-channel solution is frequency independent.
    ///
    /// # Errors
    /// A nonconstant grid disagrees with its frequency axis, or its valid
    /// frequencies are not strictly monotonic.
    pub fn resample_frequency(
        &self,
        input_hz: &[f64],
        output_hz: &[f64],
        mode: FrequencyInterpolation,
    ) -> Result<Self, ApplyExecutionError> {
        let error = |reason: &str| ApplyExecutionError::UnsupportedCalibrationTable {
            path: String::new(),
            reason: reason.to_string(),
        };
        if self.channels != 1 && input_hz.len() != self.channels {
            return Err(error(
                "calibration frequency grid does not match CPARAM channel axis",
            ));
        }
        let size = self
            .receptors
            .checked_mul(output_hz.len())
            .ok_or_else(|| error("resampled parameter count overflows usize"))?;
        let mut values = Vec::with_capacity(size);
        let mut flags = Vec::with_capacity(size);
        for receptor in 0..self.receptors {
            if self.channels == 1 {
                values.extend(std::iter::repeat_n(
                    self.value(receptor, 0),
                    output_hz.len(),
                ));
                flags.extend(std::iter::repeat_n(self.flags[receptor], output_hz.len()));
                continue;
            }
            let mut points = (0..self.channels)
                .filter(|&channel| !self.flags[receptor * self.channels + channel])
                .map(|channel| {
                    let value = self.value(receptor, channel);
                    (input_hz[channel] / 1.0e9, value.norm(), value.arg())
                })
                .collect::<Vec<_>>();
            let descending = input_hz
                .first()
                .zip(input_hz.last())
                .is_some_and(|(first, last)| first > last);
            if !points.windows(2).all(|pair| {
                if descending {
                    pair[0].0 > pair[1].0
                } else {
                    pair[0].0 < pair[1].0
                }
            }) {
                return Err(error(
                    "valid calibration frequencies must be strictly monotonic",
                ));
            }
            for index in 1..points.len() {
                let previous = points[index - 1].2;
                while f64::from(points[index].2 - previous) > std::f64::consts::PI {
                    points[index].2 = (f64::from(points[index].2) - std::f64::consts::TAU) as f32;
                }
                while f64::from(points[index].2 - previous) < -std::f64::consts::PI {
                    points[index].2 = (f64::from(points[index].2) + std::f64::consts::TAU) as f32;
                }
            }
            if descending {
                points.reverse();
            }
            let derivatives = if mode.base() == FrequencyInterpolation::Spline {
                spline_derivatives(&points)
            } else {
                Vec::new()
            };
            for &frequency in output_hz {
                let value = sample(
                    &points,
                    frequency / 1.0e9,
                    mode.base(),
                    descending,
                    &derivatives,
                );
                flags.push(
                    value.is_none()
                        || (mode.retains_flags()
                            && registered_flag(self, receptor, input_hz, frequency, mode.base())),
                );
                values.push(value.unwrap_or_default());
            }
        }
        Ok(Self {
            receptors: self.receptors,
            channels: output_hz.len(),
            values,
            flags,
        })
    }
}

fn sample(
    points: &[(f64, f32, f32)],
    frequency: f64,
    mode: FrequencyInterpolation,
    descending: bool,
    derivatives: &[[f64; 2]],
) -> Option<Complex32> {
    let first = points.first()?;
    if points.len() == 1 || frequency <= first.0 {
        return Some(Complex32::from_polar(first.1, first.2));
    }
    let last = points.last().expect("nonempty");
    if frequency >= last.0 {
        return Some(Complex32::from_polar(last.1, last.2));
    }
    let upper = points.partition_point(|point| point.0 < frequency);
    let (low, high) = (points[upper - 1], points[upper]);
    let (amplitude, phase) = if mode == FrequencyInterpolation::Nearest {
        let nearest = if high.0 - frequency < frequency - low.0
            || (descending && high.0 - frequency == frequency - low.0)
        {
            high
        } else {
            low
        };
        (nearest.1, nearest.2)
    } else if mode == FrequencyInterpolation::Cubic {
        let first = upper.saturating_sub(2).min(points.len().saturating_sub(4));
        let stencil = &points[first..(first + 4).min(points.len())];
        let polynomial = |axis: usize| {
            let mut c = [0.0f32; 4];
            let mut d = [0.0f32; 4];
            for (index, point) in stencil.iter().enumerate() {
                c[index] = if axis == 0 { point.1 } else { point.2 };
                d[index] = c[index];
            }
            let mut value = c[0];
            for order in 1..stencil.len() {
                for index in 0..stencil.len() - order {
                    let w = f64::from(c[index + 1] - d[index])
                        / (stencil[index].0 - stencil[index + order].0);
                    c[index] = ((stencil[index].0 - frequency) * w) as f32;
                    d[index] = ((stencil[index + order].0 - frequency) * w) as f32;
                }
                value += c[0];
            }
            value
        };
        (polynomial(0), polynomial(1))
    } else if mode == FrequencyInterpolation::Spline {
        let width = high.0 - low.0;
        let a = (high.0 - frequency) / width;
        let b = 1.0 - a;
        let evaluate = |left: f32, right: f32, axis: usize| {
            (a * f64::from(left)
                + b * f64::from(right)
                + width * width / 6.0
                    * ((a * a * a - a) * derivatives[upper - 1][axis]
                        + (b * b * b - b) * derivatives[upper][axis])) as f32
        };
        (evaluate(low.1, high.1, 0), evaluate(low.2, high.2, 1))
    } else {
        let fraction = (frequency - low.0) / (high.0 - low.0);
        let interpolate = |a: f32, b: f32| (f64::from(a) + f64::from(b - a) * fraction) as f32;
        (interpolate(low.1, high.1), interpolate(low.2, high.2))
    };
    Some(Complex32::from_polar(amplitude, phase))
}

impl CalibrationTableLookup {
    /// Read the calibration table's spectral-window frequency axes.
    ///
    /// Callers admit the frequency values counted by `layout_on_disk`
    /// before calling this method, and retain that charge with the result.
    ///
    /// # Errors
    /// The SPECTRAL_WINDOW table or a CHAN_FREQ cell cannot be read.
    pub fn frequency_grids_on_disk(
        path: impl AsRef<std::path::Path>,
    ) -> Result<Vec<Vec<f64>>, ApplyExecutionError> {
        let path = path.as_ref();
        let error = |reason: String| ApplyExecutionError::UnsupportedCalibrationTable {
            path: path.display().to_string(),
            reason,
        };
        let table =
            casa_tables::Table::open(casa_tables::TableOptions::new(path.join("SPECTRAL_WINDOW")))
                .map_err(|e| error(e.to_string()))?;
        let spw = casa_ms::MsSpectralWindow::new(&table);
        (0..table.row_count())
            .map(|row| spw.chan_freq(row).map_err(|e| error(e.to_string())))
            .collect()
    }
}

/// Tridiagonal solution for natural (zero endpoint curvature) cubic splines.
fn spline_derivatives(points: &[(f64, f32, f32)]) -> Vec<[f64; 2]> {
    let mut second = vec![[0.0; 2]; points.len()];
    if points.len() < 3 {
        return second;
    }
    let mut upper = vec![0.0; points.len()];
    for index in 1..points.len() - 1 {
        let left = points[index].0 - points[index - 1].0;
        let right = points[index + 1].0 - points[index].0;
        let diagonal = 2.0 * (left + right) - left * upper[index - 1];
        upper[index] = right / diagonal;
        for axis in 0..2 {
            let y = |index: usize| {
                f64::from(if axis == 0 {
                    points[index].1
                } else {
                    points[index].2
                })
            };
            let slope = (y(index + 1) - y(index)) / right - (y(index) - y(index - 1)) / left;
            second[index][axis] = (6.0 * slope - left * second[index - 1][axis]) / diagonal;
        }
    }
    for index in (1..points.len() - 1).rev() {
        for axis in 0..2 {
            second[index][axis] -= upper[index] * second[index + 1][axis];
        }
    }
    second
}

/// Registration follows CTPatchedInterp::resampleFlagsInFreq on the full
/// input axis, independently of the compressed valid interpolation samples.
fn registered_flag(
    solution: &ComplexSolution,
    receptor: usize,
    input: &[f64],
    frequency: f64,
    mode: FrequencyInterpolation,
) -> bool {
    let n = input.len();
    if n == 0 {
        return true;
    }
    let descending = input[0] > input[n - 1];
    let upper = input.partition_point(|&value| {
        if descending {
            value > frequency
        } else {
            value < frequency
        }
    });
    let mut left = upper.saturating_sub(1).min(n - 1);
    let flag = |index: usize| solution.flags[receptor * solution.channels + index];
    match mode {
        FrequencyInterpolation::Nearest => {
            if left + 1 < n && (frequency - input[left]).abs() > (input[left + 1] - frequency).abs()
            {
                left += 1;
            }
            flag(left)
        }
        FrequencyInterpolation::Linear => {
            left = left.min(n.saturating_sub(2));
            (left..(left + 2).min(n)).any(flag)
        }
        _ => {
            let center = left.max(1).min(n.saturating_sub(3).max(1));
            (center - 1..(center + 3).min(n)).any(flag)
        }
    }
}
