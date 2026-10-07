// SPDX-License-Identifier: LGPL-3.0-or-later
//! The single spectral resampler: native rows to placements.
//!
//! A constant or Taylor basis grids every native channel on plane 0. A
//! channel-local basis follows CASA: `nearest` maps a native channel to the
//! output channel its rounded spectral pixel names (`FTMachine::matchChannel`);
//! `linear` interpolates adjacent native visibilities onto CASA's fine
//! frequency grid, keeps the nearer channel's weight and ORs the flags
//! (`FTMachine::interpolateFrequencyTogrid`). A one-channel image bypasses
//! interpolation and accepts native channels inside the channel.

use num_complex::{Complex32, Complex64};

use crate::convolution::RowContext;
use crate::error::OperatorError;
use crate::operator::{Basis, MeasurementOperator};
use crate::sample::{Placement, SampleBuffer};
use crate::weighting::WeightingGeneration;

const SPEED_OF_LIGHT_M_PER_S: f64 = 299_792_458.0;

/// One native MeasurementSet row: every selected channel and correlation.
///
/// Arrays are `[channel][correlation]`, correlation fastest, over the
/// selected correlations in the operator's routing order. Frequencies are
/// finite and strictly monotonic; `uvw_m` is finite.
#[derive(Clone, Copy, Debug)]
pub struct NativeRow<'a> {
    /// Baseline `[u, v, w]` in metres, in the image phase-centre frame.
    pub uvw_m: [f64; 3],
    /// Path-length shift to the image phase centre in metres; the phasor
    /// `e^{i·2π·shift·ν/c}` is applied to the data.
    pub phase_shift_m: f64,
    /// Native channel centres in Hz.
    pub frequencies_hz: &'a [f64],
    /// Visibilities.
    pub values: &'a [Complex32],
    /// Input weights (`WEIGHT_SPECTRUM` or `WEIGHT` repeated per channel).
    pub weights: &'a [f32],
    /// Flags.
    pub flags: &'a [bool],
    /// Row flag.
    pub row_flag: bool,
    /// Per-row context for kernel-set cell selection.
    pub context: RowContext,
}

/// CASA cube channel mapping rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SpectralKernel {
    /// Nearest output channel by rounded spectral pixel.
    Nearest,
    /// Linear interpolation onto CASA's fine frequency grid.
    Linear,
}

/// Uniform output spectral axis.
#[derive(Clone, Copy, Debug, PartialEq)]
struct OutputAxis {
    first_hz: f64,
    increment_hz: f64,
    channels: usize,
}

impl OutputAxis {
    fn compile(centres_hz: &[f64]) -> Result<Self, OperatorError> {
        let channels = centres_hz.len();
        let Some(first_hz) = centres_hz.first().copied() else {
            return Err(OperatorError::SpectralAxis {
                reason: "output axis has no channels",
            });
        };
        if centres_hz
            .iter()
            .any(|centre| !centre.is_finite() || *centre <= 0.0)
        {
            return Err(OperatorError::SpectralAxis {
                reason: "output channel centres must be finite and positive",
            });
        }
        let increment_hz = if channels > 1 {
            let increment = centres_hz[1] - first_hz;
            let uniform = centres_hz
                .windows(2)
                .all(|pair| ((pair[1] - pair[0]) - increment).abs() <= 1.0e-6 * increment.abs());
            if increment == 0.0 || !uniform {
                return Err(OperatorError::SpectralAxis {
                    reason: "output channel centres must be uniformly spaced",
                });
            }
            increment
        } else {
            0.0
        };
        Ok(Self {
            first_hz,
            increment_hz,
            channels,
        })
    }

    fn last_hz(self) -> f64 {
        self.first_hz + (self.channels - 1) as f64 * self.increment_hz
    }

    /// `FTMachine::matchChannel`: the output channel of a rounded spectral pixel.
    fn nearest_channel(self, frequency_hz: f64) -> Option<usize> {
        let pixel = ((frequency_hz - self.first_hz) / self.increment_hz + 0.5).floor();
        (pixel >= 0.0 && pixel < self.channels as f64).then_some(pixel as usize)
    }
}

/// CASA's fine frequency grid for linear interpolation: the output centres
/// when output channels are no wider than native ones, otherwise
/// `floor(width ratio)` points per output channel.
#[derive(Clone, Copy, Debug, PartialEq)]
struct FineGrid {
    start_hz: f64,
    increment_hz: f64,
    per_output: usize,
    output: OutputAxis,
}

impl FineGrid {
    fn compile(output: OutputAxis, native_increment_hz: f64) -> Result<Self, OperatorError> {
        let output_increment_hz = output.increment_hz;
        if output_increment_hz == 0.0 || native_increment_hz == 0.0 {
            return Err(OperatorError::SpectralAxis {
                reason: "linear interpolation needs non-zero increments",
            });
        }
        let width = output_increment_hz.abs() / native_increment_hz.abs();
        if width <= 1.0 {
            let increment_hz = output_increment_hz.abs().copysign(native_increment_hz);
            let start_hz = if increment_hz.signum() == output_increment_hz.signum() {
                output.first_hz
            } else {
                output.last_hz()
            };
            return Ok(Self {
                start_hz,
                increment_hz,
                per_output: 1,
                output,
            });
        }
        let per_output = width.floor() as usize;
        let fine_abs = output_increment_hz.abs() / per_output as f64;
        let first_edge = output.first_hz - output_increment_hz / 2.0;
        let last_edge = output.last_hz() + output_increment_hz / 2.0;
        let low_edge = first_edge.min(last_edge);
        let high_edge = first_edge.max(last_edge);
        let increment_hz = fine_abs.copysign(native_increment_hz);
        let start_hz = if increment_hz > 0.0 {
            low_edge + fine_abs / 2.0
        } else {
            high_edge - fine_abs / 2.0
        };
        Ok(Self {
            start_hz,
            increment_hz,
            per_output,
            output,
        })
    }

    fn count(self) -> usize {
        self.per_output * self.output.channels
    }

    fn frequency_hz(self, ordinal: usize) -> f64 {
        self.start_hz + ordinal as f64 * self.increment_hz
    }

    fn output_channel(self, ordinal: usize) -> usize {
        let output_ordinal = ordinal / self.per_output;
        if self.increment_hz.signum() == self.output.increment_hz.signum() {
            output_ordinal
        } else {
            self.output.channels - 1 - output_ordinal
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Sampling {
    Direct,
    Nearest(OutputAxis),
    Linear(OutputAxis),
    SingleChannel { centre_hz: f64, increment_hz: f64 },
}

/// One spectral sample of a row: its plane, frequency and native source.
#[derive(Clone, Copy, Debug)]
enum Source {
    Channel(usize),
    Pair { left: usize, right_factor: f64 },
}

/// Turns native rows into placed, weighted samples for one operator.
#[derive(Clone, Debug, PartialEq)]
pub struct SpectralResampler {
    sampling: Sampling,
    basis: Basis,
}

impl SpectralResampler {
    /// Resampler for `basis`. A channel-local basis needs the uniform output
    /// channel centres (one per plane) and the mapping `kernel`; the
    /// centres are ignored for the other bases.
    pub fn new(
        basis: Basis,
        output_centres_hz: &[f64],
        kernel: SpectralKernel,
    ) -> Result<Self, OperatorError> {
        let sampling = match basis {
            Basis::Constant | Basis::Taylor { .. } => Sampling::Direct,
            Basis::ChannelLocal { planes } => {
                let output = OutputAxis::compile(output_centres_hz)?;
                if output.channels != planes as usize {
                    return Err(OperatorError::SpectralAxis {
                        reason: "output channel count must equal the basis planes",
                    });
                }
                match kernel {
                    SpectralKernel::Nearest => Sampling::Nearest(output),
                    SpectralKernel::Linear if output.channels == 1 => Sampling::SingleChannel {
                        centre_hz: output.first_hz,
                        increment_hz: output.increment_hz,
                    },
                    SpectralKernel::Linear => Sampling::Linear(output),
                }
            }
        };
        Ok(Self { sampling, basis })
    }

    /// Place one row's unflagged samples with imaging weights into `out`.
    ///
    /// Each sample's value is `W · V · e^{iφ}` and its weight `W` for every
    /// polarization; samples whose kernel support leaves the padded grid or
    /// whose imaging weight is zero are dropped.
    pub fn place(
        &self,
        operator: &MeasurementOperator,
        weighting: &WeightingGeneration,
        row: &NativeRow<'_>,
        out: &mut SampleBuffer,
    ) -> Result<(), OperatorError> {
        let npol = self.validate(operator, row, out)?;
        let cf = operator.cf();
        let geometry = operator.geometry();
        let mut values = vec![Complex32::default(); npol];
        let mut weights = vec![0.0_f32; npol];
        self.for_each_sample(row, |plane, frequency_hz, source| {
            let Some(input_weight) = self.input_weight(row, npol, source) else {
                return;
            };
            let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
            let w = row.uvw_m[2] * scale;
            let key = cf.key(&row.context, frequency_hz, w);
            let taps = cf.taps(key);
            let u = row.uvw_m[0] * scale;
            let v = row.uvw_m[1] * scale;
            let location = geometry.locate(u, v, taps.oversampling());
            if !geometry.fits(location, taps.half_support()) {
                return;
            }
            let placement = Placement {
                u,
                v,
                w,
                phase: std::f64::consts::TAU * row.phase_shift_m * scale,
                plane,
                spectral: self.basis.spectral(frequency_hz),
                cf: key,
                gradient: [0.0, 0.0],
            };
            let weight = weighting.imaging_weight(&placement, input_weight);
            if weight <= 0.0 {
                return;
            }
            let phasor = Complex64::from_polar(f64::from(weight), placement.phase);
            for pol in 0..npol {
                let value = self.value(row, npol, source, pol);
                let scaled = Complex64::new(f64::from(value.re), f64::from(value.im)) * phasor;
                values[pol] = Complex32::new(scaled.re as f32, scaled.im as f32);
                weights[pol] = weight;
            }
            out.push(placement, &values, &weights);
        });
        Ok(())
    }

    /// Place one row's density-pass samples: one polarization carrying
    /// CASA's unpolarized input weight, zero values, no support test.
    pub fn place_density(
        &self,
        operator: &MeasurementOperator,
        row: &NativeRow<'_>,
        out: &mut SampleBuffer,
    ) -> Result<(), OperatorError> {
        let npol = operator.polarization().correlations().len();
        self.validate_row(row, npol)?;
        if out.npol() != 1 {
            return Err(OperatorError::NativeRow {
                reason: "density buffers carry one polarization",
            });
        }
        let cf = operator.cf();
        self.for_each_sample(row, |plane, frequency_hz, source| {
            let Some(input_weight) = self.input_weight(row, npol, source) else {
                return;
            };
            let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
            let w = row.uvw_m[2] * scale;
            let placement = Placement {
                u: row.uvw_m[0] * scale,
                v: row.uvw_m[1] * scale,
                w,
                phase: 0.0,
                plane,
                spectral: self.basis.spectral(frequency_hz),
                cf: cf.key(&row.context, frequency_hz, w),
                gradient: [0.0, 0.0],
            };
            out.push(placement, &[Complex32::default()], &[input_weight]);
        });
        Ok(())
    }

    fn validate(
        &self,
        operator: &MeasurementOperator,
        row: &NativeRow<'_>,
        out: &SampleBuffer,
    ) -> Result<usize, OperatorError> {
        let npol = operator.polarization().correlations().len();
        if out.npol() != npol {
            return Err(OperatorError::NativeRow {
                reason: "buffer polarizations must match the operator",
            });
        }
        self.validate_row(row, npol)?;
        Ok(npol)
    }

    fn validate_row(&self, row: &NativeRow<'_>, npol: usize) -> Result<(), OperatorError> {
        let channels = row.frequencies_hz.len();
        if channels == 0 {
            return Err(OperatorError::NativeRow {
                reason: "row has no channels",
            });
        }
        if row.values.len() != channels * npol
            || row.weights.len() != channels * npol
            || row.flags.len() != channels * npol
        {
            return Err(OperatorError::NativeRow {
                reason: "values, weights and flags must be channels × correlations",
            });
        }
        if matches!(self.sampling, Sampling::Linear(_)) && channels < 2 {
            return Err(OperatorError::SpectralAxis {
                reason: "linear interpolation needs at least two native channels",
            });
        }
        Ok(())
    }

    /// Visit every spectral sample of the row as `(plane, frequency, source)`.
    fn for_each_sample(&self, row: &NativeRow<'_>, mut visit: impl FnMut(u32, f64, Source)) {
        match self.sampling {
            Sampling::Direct => {
                for (channel, frequency_hz) in row.frequencies_hz.iter().enumerate() {
                    visit(0, *frequency_hz, Source::Channel(channel));
                }
            }
            Sampling::Nearest(output) => {
                for (channel, frequency_hz) in row.frequencies_hz.iter().enumerate() {
                    if let Some(plane) = output.nearest_channel(*frequency_hz) {
                        visit(plane as u32, *frequency_hz, Source::Channel(channel));
                    }
                }
            }
            Sampling::SingleChannel {
                centre_hz,
                increment_hz,
            } => {
                for (channel, frequency_hz) in row.frequencies_hz.iter().enumerate() {
                    let inside = if increment_hz == 0.0 {
                        *frequency_hz == centre_hz
                    } else {
                        ((frequency_hz - centre_hz) / increment_hz + 0.5).floor() == 0.0
                    };
                    if inside {
                        visit(0, *frequency_hz, Source::Channel(channel));
                    }
                }
            }
            Sampling::Linear(output) => {
                let native = row.frequencies_hz;
                let Ok(grid) = FineGrid::compile(output, native[1] - native[0]) else {
                    return;
                };
                let count = grid.count();
                let ascending = grid.increment_hz > 0.0;
                let mut next = 0;
                for left in 0..native.len() - 1 {
                    let left_hz = native[left];
                    let right_hz = native[left + 1];
                    let span = right_hz - left_hz;
                    while next < count {
                        let frequency_hz = grid.frequency_hz(next);
                        let before_left = if ascending {
                            frequency_hz < left_hz
                        } else {
                            frequency_hz > left_hz
                        };
                        if before_left {
                            next += 1;
                            continue;
                        }
                        let after_right = if ascending {
                            frequency_hz > right_hz
                        } else {
                            frequency_hz < right_hz
                        };
                        if after_right {
                            break;
                        }
                        let right_factor = ((frequency_hz - left_hz) / span).clamp(0.0, 1.0);
                        visit(
                            grid.output_channel(next) as u32,
                            frequency_hz,
                            Source::Pair { left, right_factor },
                        );
                        next += 1;
                    }
                }
            }
        }
    }

    /// CASA's unpolarized input weight `(w_first + w_last)/2` of a sample,
    /// or `None` when the row or any selected correlation is flagged.
    fn input_weight(&self, row: &NativeRow<'_>, npol: usize, source: Source) -> Option<f32> {
        if row.row_flag {
            return None;
        }
        let (channel, right_factor) = match source {
            Source::Channel(channel) => (channel, None),
            Source::Pair { left, right_factor } => (left, Some(right_factor)),
        };
        let flagged = |channel: usize| {
            row.flags[channel * npol..(channel + 1) * npol]
                .iter()
                .any(|flag| *flag)
        };
        let unpolarized = |channel: usize| {
            let weights = &row.weights[channel * npol..(channel + 1) * npol];
            (weights[0] + weights[npol - 1]) / 2.0
        };
        let (flag, weight) = match right_factor {
            None => (flagged(channel), unpolarized(channel)),
            Some(right_factor) => {
                let left_flag = flagged(channel);
                let right_flag = flagged(channel + 1);
                let flag = if right_factor <= f64::EPSILON {
                    left_flag
                } else if right_factor >= 1.0 - f64::EPSILON {
                    right_flag
                } else {
                    left_flag || right_flag
                };
                // CASA nearest-weight interpolation keeps the left element at a tie.
                let nearest = if right_factor > 0.5 {
                    channel + 1
                } else {
                    channel
                };
                (flag, unpolarized(nearest))
            }
        };
        if flag || !weight.is_finite() || weight <= 0.0 {
            return None;
        }
        Some(weight)
    }

    fn value(&self, row: &NativeRow<'_>, npol: usize, source: Source, pol: usize) -> Complex32 {
        match source {
            Source::Channel(channel) => row.values[channel * npol + pol],
            Source::Pair { left, right_factor } => {
                let left_value = row.values[left * npol + pol];
                let right_value = row.values[(left + 1) * npol + pol];
                let left_factor = (1.0 - right_factor) as f32;
                left_value * left_factor + right_value * right_factor as f32
            }
        }
    }
}
