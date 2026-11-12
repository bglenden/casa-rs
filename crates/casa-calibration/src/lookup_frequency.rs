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
            if points
                .first()
                .zip(points.last())
                .is_some_and(|(first, last)| first.0 > last.0)
            {
                points.reverse();
            }
            if !points.windows(2).all(|pair| pair[0].0 < pair[1].0) {
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
            for &frequency in output_hz {
                let value = sample(&points, frequency / 1.0e9, mode);
                flags.push(value.is_none());
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
        let nearest = if high.0 - frequency < frequency - low.0 {
            high
        } else {
            low
        };
        (nearest.1, nearest.2)
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
