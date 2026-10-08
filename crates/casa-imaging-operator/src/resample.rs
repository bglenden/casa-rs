// SPDX-License-Identifier: LGPL-3.0-or-later
//! The single spectral resampler: native rows to placements.
//!
//! A constant or Taylor basis grids every native channel on plane 0. A
//! channel-local basis follows CASA `FTMachine`: `nearest` maps a native
//! channel to the output channel its rounded spectral pixel names
//! (`matchChannel`); `linear` interpolates adjacent native visibilities onto
//! CASA's fine frequency grid, keeps the nearer channel's weight and ORs the
//! flags (`interpolateFrequencyTogrid`), except that a one-channel image or
//! a one-channel row bypasses interpolation and maps like `nearest`.

use num_complex::{Complex32, Complex64};

use crate::accumulator::PlaneRange;
use crate::backend::{GridBackend, PreparedModelGrids, Work};
use crate::convolution::RowContext;
use crate::error::OperatorError;
use crate::operator::{Basis, MeasurementOperator};
use crate::sample::{Placement, SampleBuffer};
use crate::weighting::{
    DensityCellRule, DensityGridShape, DensityUv, SPEED_OF_LIGHT_M_PER_S, WeightingGeneration,
};

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

/// The uniform output spectral axis of a channel-local basis: first channel
/// centre, signed channel width and channel count. The width matters even
/// for one channel, because it decides which native channels the channel
/// accepts.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpectralAxis {
    first_hz: f64,
    increment_hz: f64,
    channels: u32,
}

impl SpectralAxis {
    /// An axis of `channels` channels of width `increment_hz` starting at
    /// `first_hz`; the frequency must be finite and positive, the increment
    /// finite and non-zero, and there must be at least one channel.
    pub fn new(first_hz: f64, increment_hz: f64, channels: u32) -> Result<Self, OperatorError> {
        if !first_hz.is_finite() || first_hz <= 0.0 {
            return Err(OperatorError::SpectralAxis {
                reason: "the first channel centre must be finite and positive",
            });
        }
        if !increment_hz.is_finite() || increment_hz == 0.0 {
            return Err(OperatorError::SpectralAxis {
                reason: "the channel width must be finite and non-zero",
            });
        }
        if channels == 0 {
            return Err(OperatorError::SpectralAxis {
                reason: "the axis has no channels",
            });
        }
        Ok(Self {
            first_hz,
            increment_hz,
            channels,
        })
    }

    /// Centre of the first channel in Hz.
    #[must_use]
    pub const fn first_hz(self) -> f64 {
        self.first_hz
    }

    /// Signed channel width in Hz.
    #[must_use]
    pub const fn increment_hz(self) -> f64 {
        self.increment_hz
    }

    /// Number of channels.
    #[must_use]
    pub const fn channels(self) -> u32 {
        self.channels
    }

    /// Centre of `channel` in Hz.
    #[must_use]
    pub fn centre_hz(self, channel: u32) -> f64 {
        self.first_hz + f64::from(channel) * self.increment_hz
    }

    fn last_hz(self) -> f64 {
        self.centre_hz(self.channels - 1)
    }

    /// The axis extended by `planes_per_side` channels of the same width at
    /// each end; the new first centre must stay positive.
    pub fn padded(self, planes_per_side: u32) -> Result<Self, OperatorError> {
        let channels = planes_per_side
            .checked_mul(2)
            .and_then(|padding| padding.checked_add(self.channels))
            .ok_or(OperatorError::SpectralAxis {
                reason: "the padded axis has too many channels",
            })?;
        Self::new(
            self.first_hz - f64::from(planes_per_side) * self.increment_hz,
            self.increment_hz,
            channels,
        )
    }

    /// `FTMachine::matchChannel`: the channel whose rounded spectral pixel
    /// `floor((ν − ν₀)/Δν + 0.5)` lies on the axis.
    #[must_use]
    pub fn nearest_channel(self, frequency_hz: f64) -> Option<u32> {
        let pixel = ((frequency_hz - self.first_hz) / self.increment_hz + 0.5).floor();
        (pixel >= 0.0 && pixel < f64::from(self.channels)).then_some(pixel as u32)
    }

    /// `FTMachine::matchChannel` under linear interpolation: a native
    /// channel off the axis is still predicted (`chanMap = −2`) within half a
    /// native width inside, or two widths outside, the world frequency of
    /// pixel 0 or of pixel `channels` (one past the last centre, as CASA
    /// takes it); `native_width_hz` is the row's first native spacing.
    fn in_linear_halo(self, frequency_hz: f64, native_width_hz: f64) -> bool {
        let first = self.first_hz;
        let beyond = self.first_hz + f64::from(self.channels) * self.increment_hz;
        let (low, high) = (first.min(beyond), first.max(beyond));
        let width = native_width_hz.abs();
        (frequency_hz < high + 2.0 * width && frequency_hz > high - 0.5 * width)
            || (frequency_hz < low + 0.5 * width && frequency_hz > low - 2.0 * width)
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
    output: SpectralAxis,
}

impl FineGrid {
    fn compile(output: SpectralAxis, native_increment_hz: f64) -> Option<Self> {
        let output_increment_hz = output.increment_hz;
        if native_increment_hz == 0.0 || !native_increment_hz.is_finite() {
            return None;
        }
        let width = output_increment_hz.abs() / native_increment_hz.abs();
        if width <= 1.0 {
            let increment_hz = output_increment_hz.abs().copysign(native_increment_hz);
            let start_hz = if increment_hz.signum() == output_increment_hz.signum() {
                output.first_hz
            } else {
                output.last_hz()
            };
            return Some(Self {
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
        Some(Self {
            start_hz,
            increment_hz,
            per_output,
            output,
        })
    }

    fn count(self) -> usize {
        self.per_output * self.output.channels as usize
    }

    fn frequency_hz(self, ordinal: usize) -> f64 {
        self.start_hz + ordinal as f64 * self.increment_hz
    }

    fn output_channel(self, ordinal: usize) -> u32 {
        let output_ordinal = (ordinal / self.per_output) as u32;
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
    Nearest(SpectralAxis),
    Linear(SpectralAxis),
}

/// One spectral sample of a row: its native source.
#[derive(Clone, Copy, Debug)]
enum Source {
    Channel(usize),
    Pair { left: usize, right_factor: f64 },
}

/// Reusable buffers for [`SpectralResampler::predict_row`]; one per worker.
#[derive(Debug, Default)]
pub struct PredictionScratch {
    buffer: Option<SampleBuffer>,
    zeros: Vec<Complex32>,
    ones: Vec<f32>,
    predicted: Vec<Complex32>,
    sources: Vec<usize>,
    values: Vec<Complex32>,
}

impl PredictionScratch {
    /// Clear every buffer for a row of `npol` polarizations.
    fn reset(&mut self, npol: usize) {
        if self.zeros.len() != npol {
            self.zeros = vec![Complex32::default(); npol];
            self.ones = vec![1.0; npol];
        }
        let buffer = self.buffer.get_or_insert_with(|| SampleBuffer::new(npol));
        if buffer.npol() != npol {
            *buffer = SampleBuffer::new(npol);
        }
        buffer.clear();
        self.sources.clear();
    }
}

/// CASA `FTMachine::interpolateFrequencyFromgrid`: output-channel
/// predictions `values` (`[channel][pol]`, zero where the row was not
/// degridded) on CASA's image-frequency grid, refined to `floor(width
/// ratio)` repeated points per channel when output channels are wider than
/// native ones, interpolated linearly to every native channel that maps to
/// the axis or lies in its linear halo (`chanMap` 0… or −2; −1 stays
/// zero); the end pair extrapolates (casacore `InterpolateArray1D` linear).
fn interpolate_from_grid(
    axis: SpectralAxis,
    row: &NativeRow<'_>,
    values: &[Complex32],
    npol: usize,
    out: &mut [Complex32],
) {
    let native = row.frequencies_hz;
    let width = (axis.increment_hz / (native[1] - native[0])).abs();
    let per_channel = if width > 1.0 {
        width.floor() as usize
    } else {
        1
    };
    let fine_increment = axis.increment_hz / per_channel as f64;
    let fine_start = if per_channel > 1 {
        axis.first_hz - axis.increment_hz / 2.0 + fine_increment / 2.0
    } else {
        axis.first_hz
    };
    let points = axis.channels as usize * per_channel;
    let point = |index: usize| fine_start + index as f64 * fine_increment;
    let native_width_hz = native[1] - native[0];
    for (channel, frequency_hz) in native.iter().enumerate() {
        if axis.nearest_channel(*frequency_hz).is_none()
            && !axis.in_linear_halo(*frequency_hz, native_width_hz)
        {
            continue;
        }
        let position = (frequency_hz - fine_start) / fine_increment;
        let left = (position.floor().max(0.0) as usize).min(points - 2);
        let fraction = ((frequency_hz - point(left)) / (point(left + 1) - point(left))) as f32;
        let (low, high) = (left / per_channel, (left + 1) / per_channel);
        for pol in 0..npol {
            if !row.flags[channel * npol + pol] {
                let a = values[low * npol + pol];
                let b = values[high * npol + pol];
                out[channel * npol + pol] = a + (b - a) * fraction;
            }
        }
    }
}

/// The output channels CASA degrids for one row under linear mapping
/// (`FTMachine::getInterpolateArrays`, after `GridFT::get` returns early
/// when no native maps): each native's `chanMap` is its channel, −2 in the
/// linear halo or −1; −1 is replaced by the maximum, and the channels from
/// the minimum to the maximum are degridded, from channel 0 when the
/// minimum is the halo's −2 or equals the maximum. When every native lies
/// in the halo the maximum is −2 and CASA's unsigned loop flags nothing, so
/// every channel is degridded. `None` when nothing maps.
fn degridded_channels(axis: SpectralAxis, native: &[f64]) -> Option<std::ops::RangeInclusive<u32>> {
    let native_width_hz = native[1] - native[0];
    let map = |frequency_hz: f64| match axis.nearest_channel(frequency_hz) {
        Some(channel) => i64::from(channel),
        None if axis.in_linear_halo(frequency_hz, native_width_hz) => -2,
        None => -1,
    };
    let maximum = native.iter().map(|frequency_hz| map(*frequency_hz)).max()?;
    if maximum == -1 {
        return None;
    }
    if maximum == -2 {
        return Some(0..=axis.channels - 1);
    }
    let minimum = native
        .iter()
        .map(|frequency_hz| match map(*frequency_hz) {
            -1 => maximum,
            channel => channel,
        })
        .min()?;
    let first = if minimum == maximum {
        0
    } else {
        minimum.max(0)
    };
    Some(first as u32..=maximum as u32)
}

/// CASA's flag of a sample: the row flag, then any selected correlation of
/// its channel; for an interpolated pair, of the left channel at its end, of
/// the right channel at its end and of either between (casacore
/// `InterpolateArray1D` linear with flags).
fn sample_flagged(row: &NativeRow<'_>, npol: usize, source: Source) -> bool {
    let flagged = |channel: usize| {
        row.flags[channel * npol..(channel + 1) * npol]
            .iter()
            .any(|flag| *flag)
    };
    row.row_flag
        || match source {
            Source::Channel(channel) => flagged(channel),
            Source::Pair { left, right_factor } if right_factor <= f64::EPSILON => flagged(left),
            Source::Pair { left, right_factor } if right_factor >= 1.0 - f64::EPSILON => {
                flagged(left + 1)
            }
            Source::Pair { left, .. } => flagged(left) || flagged(left + 1),
        }
}

/// Whether `row` correlates an antenna with itself, which CASA's imaging
/// `GridFT` neither grids nor degrids (`GridFT::put`/`get` flag the row
/// when `usezero` is false, as tclean builds it).
fn is_autocorrelation(row: &NativeRow<'_>) -> bool {
    row.context.antennas[0] == row.context.antennas[1]
}

/// CASA's unpolarized input weight `(w_first + w_last)/2` of a native channel.
fn unpolarized_weight(row: &NativeRow<'_>, npol: usize, channel: usize) -> f32 {
    let weights = &row.weights[channel * npol..(channel + 1) * npol];
    (weights[0] + weights[npol - 1]) / 2.0
}

/// The imaging weight of an unflagged sample at `placement`.
///
/// CASA weights native channels at their own frequencies: `u`, `v`, the
/// density cell, the taper and the bandwidth-taper distance all use the
/// channel's frequency. `FTMachine::interpolateFrequencyTogrid` then carries
/// those weights to an output sample between two native channels: linearly
/// (taking one end's weight at that end) for `VisImagingWeight`, and from
/// the nearer channel for the cube Briggs weightor
/// (`BriggsCubeWeightor::getWeightUniform`), whose density plane for a
/// native channel is its rounded spectral pixel on the padded density axis
/// `density_axis` (`FTMachine::matchChannel`); a channel off that axis
/// weighs nothing.
fn sample_weight(
    weighting: &WeightingGeneration,
    row: &NativeRow<'_>,
    npol: usize,
    source: Source,
    placement: &Placement,
    density_axis: Option<SpectralAxis>,
) -> f32 {
    let channel_weight = |channel: usize| {
        let input = unpolarized_weight(row, npol, channel);
        if !input.is_finite() {
            return 0.0;
        }
        let frequency_hz = row.frequencies_hz[channel];
        let plane = match density_axis {
            Some(axis) => match axis.nearest_channel(frequency_hz) {
                Some(plane) => plane,
                None => return 0.0,
            },
            None => placement.plane,
        };
        let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
        let native = Placement {
            u: row.uvw_m[0] * scale,
            v: row.uvw_m[1] * scale,
            plane,
            ..*placement
        };
        weighting.imaging_weight(&native, DensityUv::casa(row.uvw_m, frequency_hz), input)
    };
    match source {
        Source::Pair { left, right_factor } if density_axis.is_some() => {
            channel_weight(if right_factor > 0.5 { left + 1 } else { left })
        }
        Source::Pair { left, right_factor } => {
            if right_factor <= f64::EPSILON {
                channel_weight(left)
            } else if right_factor >= 1.0 - f64::EPSILON {
                channel_weight(left + 1)
            } else {
                let (low, high) = (channel_weight(left), channel_weight(left + 1));
                low + (high - low) * right_factor as f32
            }
        }
        Source::Channel(channel) => channel_weight(channel),
    }
}

/// A placement predicting `row` on `plane` at `frequency_hz`, when its kernel
/// support fits the padded grid.
fn prediction_placement(
    operator: &MeasurementOperator,
    basis: Basis,
    row: &NativeRow<'_>,
    plane: u32,
    frequency_hz: f64,
) -> Option<Placement> {
    let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
    let w = row.uvw_m[2] * scale;
    let cf = operator.cf();
    let key = cf.key(&row.context, frequency_hz, w);
    let (u, v) = (row.uvw_m[0] * scale, row.uvw_m[1] * scale);
    let geometry = operator.geometry();
    let taps = cf.taps(key);
    geometry
        .fits(
            geometry.locate(u, v, taps.oversampling()),
            taps.half_support(),
        )
        .then_some(Placement {
            u,
            v,
            w,
            phase: std::f64::consts::TAU * row.phase_shift_m * scale,
            plane,
            spectral: basis.spectral(frequency_hz),
            cf: key,
            gradient: [0.0, 0.0],
        })
}

/// Turns native rows into placed, weighted samples for one operator.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SpectralResampler {
    sampling: Sampling,
    basis: Basis,
}

impl SpectralResampler {
    /// Resampler for a constant or Taylor basis: every native channel is a
    /// sample on plane 0.
    pub fn direct(basis: Basis) -> Result<Self, OperatorError> {
        if matches!(basis, Basis::ChannelLocal { .. }) {
            return Err(OperatorError::SpectralAxis {
                reason: "a channel-local basis needs its output axis and kernel",
            });
        }
        Ok(Self {
            sampling: Sampling::Direct,
            basis,
        })
    }

    /// Resampler for the channel-local basis whose planes are the channels
    /// of `output`, mapped by `kernel`.
    #[must_use]
    pub fn channel_local(output: SpectralAxis, kernel: SpectralKernel) -> Self {
        Self {
            sampling: match kernel {
                SpectralKernel::Nearest => Sampling::Nearest(output),
                SpectralKernel::Linear => Sampling::Linear(output),
            },
            basis: Basis::ChannelLocal {
                planes: output.channels,
            },
        }
    }

    /// The basis the resampler produces placements for.
    #[must_use]
    pub const fn basis(&self) -> Basis {
        self.basis
    }

    /// Whether a residual pass forms its residual at native channels:
    /// linear interpolation onto more than one output channel. CASA
    /// subtracts the model's native-channel predictions from the data
    /// (`SIMapperCollection::grid`, after `interpolateFrequencyFromgrid`) and
    /// interpolates the difference onto the image grid, which is not the
    /// model subtracted at each output sample. Direct and nearest sampling
    /// predict each native channel at its own frequency, where the two agree,
    /// so their residual is gridded in one sweep (`Work::ResidualGrid`).
    #[must_use]
    pub const fn forms_native_residuals(&self) -> bool {
        matches!(self.sampling, Sampling::Linear(axis) if axis.channels > 1)
    }

    /// Output planes on each side of a wave whose model a residual pass over
    /// the wave needs, for native channels at most `native_spacing_hz` apart.
    ///
    /// When residuals are formed at native channels, a sample on plane `p`
    /// lies within half an output width `Δ` of `p`'s centre; the natives it
    /// interpolates lie within the native spacing `s` of it; each of their
    /// predictions interpolates output-channel values on CASA's fine grid
    /// within one output width of the native
    /// (`interpolateFrequencyFromgrid`). So every model plane a wave's
    /// samples reach lies within `⌈1.5 + s/|Δ|⌉` planes of the wave. Direct
    /// and nearest sampling predict a native on its own plane: no halo.
    #[must_use]
    pub fn model_halo(&self, native_spacing_hz: f64) -> u32 {
        match self.sampling {
            Sampling::Linear(axis) if axis.channels > 1 => {
                let planes = (1.5 + native_spacing_hz.abs() / axis.increment_hz.abs()).ceil();
                if planes.is_finite() && planes < f64::from(axis.channels) {
                    planes as u32
                } else {
                    axis.channels
                }
            }
            Sampling::Direct | Sampling::Nearest(_) | Sampling::Linear(_) => 0,
        }
    }

    /// The model planes a residual pass over `planes` needs: `planes`
    /// widened by [`Self::model_halo`] on each side, clipped to the axis.
    #[must_use]
    pub fn model_planes(&self, planes: PlaneRange, native_spacing_hz: f64) -> PlaneRange {
        let halo = self.model_halo(native_spacing_hz);
        PlaneRange::new(
            planes.start.saturating_sub(halo),
            planes
                .end
                .saturating_add(halo)
                .min(self.basis.planes().max(planes.end)),
        )
    }

    /// The output axis padded by `padding` density planes on each side.
    fn density_axis(&self, padding: u32) -> Result<SpectralAxis, OperatorError> {
        match self.sampling {
            Sampling::Nearest(axis) | Sampling::Linear(axis) => axis.padded(padding),
            Sampling::Direct => Err(OperatorError::SpectralAxis {
                reason: "a per-channel density generation needs a channel-local resampler",
            }),
        }
    }

    /// This resampler over the padded density axis.
    fn over_density_axis(&self, padding: u32) -> Result<Self, OperatorError> {
        let axis = self.density_axis(padding)?;
        Ok(Self {
            sampling: match self.sampling {
                Sampling::Linear(_) => Sampling::Linear(axis),
                Sampling::Direct | Sampling::Nearest(_) => Sampling::Nearest(axis),
            },
            basis: Basis::ChannelLocal {
                planes: axis.channels,
            },
        })
    }

    /// Place one row's unflagged samples with imaging weights into `out`.
    ///
    /// Each sample's value is `W · V · e^{iφ}` and its weight `W` for every
    /// polarization; samples whose kernel support leaves the padded grid or
    /// whose imaging weight is zero are dropped. An autocorrelation places
    /// nothing: CASA's imaging `GridFT` flags rows whose antennas agree
    /// (`GridFT::put`, `usezero = false`), while the weight densities
    /// ([`Self::place_density`]) still count them.
    pub fn place(
        &self,
        operator: &MeasurementOperator,
        weighting: &WeightingGeneration,
        row: &NativeRow<'_>,
        out: &mut SampleBuffer,
    ) -> Result<(), OperatorError> {
        let npol = self.validate(operator, row, out)?;
        if is_autocorrelation(row) {
            return Ok(());
        }
        let cf = operator.cf();
        let geometry = operator.geometry();
        let density_axis = weighting
            .cube_padding()
            .map(|padding| self.density_axis(padding))
            .transpose()?;
        let mut values = vec![Complex32::default(); npol];
        let mut weights = vec![0.0_f32; npol];
        self.for_each_sample(row, |plane, frequency_hz, source| {
            if sample_flagged(row, npol, source) {
                return;
            }
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
            let weight = sample_weight(weighting, row, npol, source, &placement, density_axis);
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

    /// Place one row's density-pass samples for a grid of `shape`: one
    /// polarization carrying CASA's unpolarized input weight, zero values,
    /// no support test. Under the standard cell rule every unflagged native
    /// channel is a sample at its own frequency whose `u` and `v` are the
    /// [`DensityUv`] coordinates (`VisImagingWeight` accumulates native
    /// channels); under the cube rule the samples are the resampled samples
    /// of the output axis padded by `shape.padding` planes on each side, at
    /// the double coordinates CASA grids its weight density with
    /// (`BriggsCubeWeightor::init` grids the PSF onto its padded template).
    pub fn place_density(
        &self,
        operator: &MeasurementOperator,
        row: &NativeRow<'_>,
        shape: &DensityGridShape,
        out: &mut SampleBuffer,
    ) -> Result<(), OperatorError> {
        let rule = shape.rule;
        let npol = operator.polarization().correlations().len();
        self.validate_row(operator, row, npol)?;
        if out.npol() != 1 {
            return Err(OperatorError::NativeRow {
                reason: "density buffers carry one polarization",
            });
        }
        let cf = operator.cf();
        let mut visit = |plane: u32, frequency_hz: f64, source: Source| {
            let Some(input_weight) = self.input_weight(row, npol, source) else {
                return;
            };
            let scale = frequency_hz / SPEED_OF_LIGHT_M_PER_S;
            let w = row.uvw_m[2] * scale;
            let (u, v) = match rule {
                DensityCellRule::Standard => {
                    let uv = DensityUv::casa(row.uvw_m, frequency_hz);
                    (f64::from(uv.u), f64::from(uv.v))
                }
                DensityCellRule::Cube => (row.uvw_m[0] * scale, row.uvw_m[1] * scale),
            };
            let placement = Placement {
                u,
                v,
                w,
                phase: 0.0,
                plane,
                spectral: self.basis.spectral(frequency_hz),
                cf: cf.key(&row.context, frequency_hz, w),
                gradient: [0.0, 0.0],
            };
            out.push(placement, &[Complex32::default()], &[input_weight]);
        };
        match rule {
            DensityCellRule::Standard => {
                for (channel, frequency_hz) in row.frequencies_hz.iter().enumerate() {
                    visit(0, *frequency_hz, Source::Channel(channel));
                }
            }
            DensityCellRule::Cube => self
                .over_density_axis(shape.padding)?
                .for_each_sample(row, visit),
        }
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
        self.validate_row(operator, row, npol)?;
        Ok(npol)
    }

    fn validate_row(
        &self,
        operator: &MeasurementOperator,
        row: &NativeRow<'_>,
        npol: usize,
    ) -> Result<(), OperatorError> {
        assert_eq!(
            self.basis,
            operator.basis(),
            "the resampler and the operator must share a basis"
        );
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
        Ok(())
    }

    /// Visit every spectral sample of the row as `(plane, frequency, source)`.
    fn for_each_sample(&self, row: &NativeRow<'_>, mut visit: impl FnMut(u32, f64, Source)) {
        let native = row.frequencies_hz;
        let nearest = |axis: SpectralAxis, visit: &mut dyn FnMut(u32, f64, Source)| {
            for (channel, frequency_hz) in native.iter().enumerate() {
                if let Some(plane) = axis.nearest_channel(*frequency_hz) {
                    visit(plane, *frequency_hz, Source::Channel(channel));
                }
            }
        };
        match self.sampling {
            Sampling::Direct => {
                for (channel, frequency_hz) in native.iter().enumerate() {
                    visit(0, *frequency_hz, Source::Channel(channel));
                }
            }
            Sampling::Nearest(axis) => nearest(axis, &mut visit),
            // CASA bypasses interpolation for a one-channel image or row.
            Sampling::Linear(axis) if axis.channels == 1 || native.len() == 1 => {
                nearest(axis, &mut visit);
            }
            Sampling::Linear(axis) => {
                let Some(grid) = FineGrid::compile(axis, native[1] - native[0]) else {
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
                            grid.output_channel(next),
                            frequency_hz,
                            Source::Pair { left, right_factor },
                        );
                        next += 1;
                    }
                }
            }
        }
    }

    /// CASA's unpolarized input weight of a sample, from the nearest native
    /// channel of a pair, or `None` when the sample is flagged.
    fn input_weight(&self, row: &NativeRow<'_>, npol: usize, source: Source) -> Option<f32> {
        if sample_flagged(row, npol, source) {
            return None;
        }
        let channel = match source {
            Source::Channel(channel) => channel,
            // CASA nearest-weight interpolation keeps the left element at a tie.
            Source::Pair { left, right_factor } if right_factor > 0.5 => left + 1,
            Source::Pair { left, .. } => left,
        };
        let weight = unpolarized_weight(row, npol, channel);
        (weight.is_finite() && weight > 0.0).then_some(weight)
    }

    /// The model visibility of every selected sample of `row`, written to
    /// `out` as `[channel][correlation]`, the way CASA predicts `MODEL_DATA`
    /// (`GridFT::get`, then `FTMachine::interpolateFrequencyFromgrid`).
    ///
    /// A flagged row, a flagged correlation, a native channel off the output
    /// axis and a sample whose kernel leaves the grid predict zero.
    /// A constant or Taylor basis, `nearest` mapping and the one-channel
    /// bypass degrid each native channel at its own frequency on its mapped
    /// plane. `linear` mapping degrids the output channels CASA degrids for
    /// the row (see `degridded_channels`) at the channel centre, repeats
    /// each value `floor(width ratio)` times on CASA's fine grid when output
    /// channels are wider than native ones, and interpolates linearly to the
    /// native frequencies on the axis and in its halo (extrapolating from the
    /// end pair); unmapped output channels contribute zeros to that grid.
    /// Only the planes `model` holds are degridded; a caller predicting a
    /// window of planes prepares the window's halo (see
    /// [`Self::model_planes`]).
    #[allow(clippy::too_many_arguments)]
    pub fn predict_row(
        &self,
        operator: &MeasurementOperator,
        backend: &mut dyn GridBackend,
        model: &PreparedModelGrids,
        row: &NativeRow<'_>,
        scratch: &mut PredictionScratch,
        out: &mut [Complex32],
    ) -> Result<(), OperatorError> {
        let npol = operator.polarization().correlations().len();
        self.validate_row(operator, row, npol)?;
        if out.len() != row.values.len() {
            return Err(OperatorError::NativeRow {
                reason: "the prediction holds one value per selected sample",
            });
        }
        out.fill(Complex32::default());
        if row.row_flag || is_autocorrelation(row) {
            return Ok(());
        }
        let native = row.frequencies_hz;
        let linear = match self.sampling {
            Sampling::Linear(axis) if axis.channels > 1 && native.len() > 1 => Some(axis),
            Sampling::Direct | Sampling::Nearest(_) | Sampling::Linear(_) => None,
        };
        scratch.reset(npol);
        let PredictionScratch {
            buffer,
            zeros,
            ones,
            predicted,
            sources,
            values,
        } = scratch;
        let buffer = buffer.as_mut().expect("reset installs the buffer");
        let prepared = model.layout().planes();
        let mut place = |plane: u32, frequency_hz: f64, source: usize| {
            if !prepared.contains(plane) {
                return;
            }
            if let Some(placement) =
                prediction_placement(operator, self.basis, row, plane, frequency_hz)
            {
                buffer.push(placement, zeros, ones);
                sources.push(source);
            }
        };
        match linear {
            None => {
                for (channel, frequency_hz) in native.iter().enumerate() {
                    let plane = match self.sampling {
                        Sampling::Direct => Some(0),
                        Sampling::Nearest(axis) | Sampling::Linear(axis) => {
                            axis.nearest_channel(*frequency_hz)
                        }
                    };
                    if let Some(plane) = plane {
                        place(plane, *frequency_hz, channel);
                    }
                }
            }
            Some(axis) => {
                let Some(channels) = degridded_channels(axis, native) else {
                    return Ok(());
                };
                for channel in channels {
                    place(channel, axis.centre_hz(channel), channel as usize);
                }
            }
        }
        predicted.resize(buffer.len() * npol, Complex32::default());
        if !buffer.is_empty() {
            backend.apply(
                &buffer.block(),
                operator.cf(),
                Work::Predict {
                    model,
                    out: predicted,
                },
            )?;
        }
        match linear {
            None => {
                for (index, channel) in sources.iter().enumerate() {
                    for pol in 0..npol {
                        if !row.flags[channel * npol + pol] {
                            out[channel * npol + pol] = predicted[index * npol + pol];
                        }
                    }
                }
            }
            Some(axis) => {
                values.clear();
                values.resize(axis.channels as usize * npol, Complex32::default());
                for (index, channel) in sources.iter().enumerate() {
                    values[channel * npol..(channel + 1) * npol]
                        .copy_from_slice(&predicted[index * npol..(index + 1) * npol]);
                }
                interpolate_from_grid(axis, row, values, npol, out);
            }
        }
        Ok(())
    }

    /// The value of a sample: its channel's, or the linear interpolation of
    /// a pair, which takes the single end at an end point as casacore
    /// `InterpolateArray1D` does, so the other channel's value (unchecked
    /// there by [`sample_flagged`]) cannot reach the sample.
    fn value(&self, row: &NativeRow<'_>, npol: usize, source: Source, pol: usize) -> Complex32 {
        match source {
            Source::Channel(channel) => row.values[channel * npol + pol],
            Source::Pair { left, right_factor } if right_factor <= f64::EPSILON => {
                row.values[left * npol + pol]
            }
            Source::Pair { left, right_factor } if right_factor >= 1.0 - f64::EPSILON => {
                row.values[(left + 1) * npol + pol]
            }
            Source::Pair { left, right_factor } => {
                let left_value = row.values[left * npol + pol];
                let right_value = row.values[(left + 1) * npol + pol];
                let left_factor = (1.0 - right_factor) as f32;
                left_value * left_factor + right_value * right_factor as f32
            }
        }
    }
}
