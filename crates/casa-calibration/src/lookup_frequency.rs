// SPDX-License-Identifier: LGPL-3.0-or-later
//! CASA's frequency resampling interpolates amplitude and unwrapped phase.
use crate::interpolation::{Curve, sample, spline_derivatives, unwrap_phase};
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
    fn curve(self) -> Curve {
        match self.base() {
            Self::Nearest => Curve::Nearest,
            Self::Linear => Curve::Linear,
            Self::Cubic => Curve::Cubic,
            Self::Spline => Curve::Spline,
            _ => unreachable!("flag modifiers removed"),
        }
    }
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
    /// frequencies are not strictly monotonic. Cubic interpolation of an
    /// interior frequency requires at least four valid samples on a physical
    /// axis of four or more channels. CASA downgrades shorter physical
    /// cubic/spline axes to linear before compressing flagged samples.
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
        // CTPatchedInterp::interpolate selects its effective method from
        // the physical axis, before removing flagged input channels.
        let interpolation = match mode.base() {
            FrequencyInterpolation::Cubic | FrequencyInterpolation::Spline if self.channels < 4 => {
                FrequencyInterpolation::Linear
            }
            interpolation => interpolation,
        };
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
            // Reserve the known upper bound: filtered collect otherwise
            // doubles near growth boundaries, exceeding admitted scratch.
            let mut points = Vec::with_capacity(self.channels);
            for (channel, &frequency) in input_hz.iter().enumerate().take(self.channels) {
                if !self.flags[receptor * self.channels + channel] {
                    let value = self.value(receptor, channel);
                    points.push((frequency / 1.0e9, value.norm(), value.arg()));
                }
            }
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
                points[index].2 = unwrap_phase(points[index - 1].2, points[index].2);
            }
            if descending {
                points.reverse();
            }
            if interpolation == FrequencyInterpolation::Cubic
                && (2..4).contains(&points.len())
                && output_hz.iter().any(|frequency| {
                    frequency / 1e9 > points[0].0 && frequency / 1e9 < points[points.len() - 1].0
                })
            {
                return Err(error(
                    "cubic frequency interpolation requires four valid samples for an interior output",
                ));
            }
            let derivatives = if interpolation == FrequencyInterpolation::Spline {
                spline_derivatives(&points)
            } else {
                Vec::new()
            };
            for &frequency in output_hz {
                let value = sample(
                    &points,
                    frequency / 1.0e9,
                    interpolation.curve(),
                    descending,
                    &derivatives,
                );
                flags.push(
                    value.is_none()
                        || (mode.retains_flags()
                            && registered_flag(self, receptor, input_hz, frequency, interpolation)),
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
        let spectral = crate::lookup::spectral_window_path(path)?
            .ok_or_else(|| error("missing SPECTRAL_WINDOW table reference".to_string()))?;
        let table = casa_tables::Table::open(casa_tables::TableOptions::new(spectral))
            .map_err(|e| error(e.to_string()))?;
        let spw = casa_ms::MsSpectralWindow::new(&table);
        (0..table.row_count())
            .map(|row| spw.chan_freq(row).map_err(|e| error(e.to_string())))
            .collect()
    }
}

/// Tridiagonal solution for natural (zero endpoint curvature) cubic splines.

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
    let frequency = frequency / 1e9;
    let descending = input[0] > input[n - 1];
    let upper = input.partition_point(|&value| {
        if descending {
            value / 1e9 > frequency
        } else {
            value / 1e9 < frequency
        }
    });
    let flag = |index: usize| solution.flags[receptor * solution.channels + index];
    // CASA assigns exact input-channel flags verbatim, regardless of the
    // interpolation neighborhood used between channels.
    if upper < n && input[upper] / 1e9 == frequency {
        return flag(upper);
    }
    let mut left = upper.saturating_sub(1).min(n - 1);
    match mode {
        FrequencyInterpolation::Nearest => {
            if left + 1 < n
                && (frequency - input[left] / 1e9).abs() > (input[left + 1] / 1e9 - frequency).abs()
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn natural_spline_matches_the_closed_form_at_every_half_knot() {
        // For unit-spaced amplitudes [1,2,1/2,5/4], the interior
        // curvature equations are 4*m1+m2=-15, m1+4*m2=27/2.
        // m1=-49/10 and m2=23/5 give these exact half-knot values.
        let solution = ComplexSolution {
            receptors: 1,
            channels: 4,
            values: vec![1.0, 2.0, 0.5, 1.25]
                .into_iter()
                .map(|a| Complex32::new(a, 0.0))
                .collect(),
            flags: vec![false; 4],
        };
        let output = solution
            .resample_frequency(
                &[1e9, 1.001e9, 1.002e9, 1.003e9],
                &[1.0005e9, 1.0015e9, 1.0025e9],
                FrequencyInterpolation::Spline,
            )
            .unwrap();
        for (actual, expected) in
            output
                .values
                .iter()
                .zip([289.0 / 160.0, 203.0 / 160.0, 47.0 / 80.0])
        {
            assert!(
                (actual.re - expected).abs() < 1e-6,
                "{actual} versus {expected}"
            );
            assert_eq!(actual.im, 0.0);
        }
    }
}
