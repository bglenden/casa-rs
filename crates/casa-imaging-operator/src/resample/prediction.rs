// SPDX-License-Identifier: LGPL-3.0-or-later
//! Predicting a batch of rows: the samples a row's prediction degrids, and
//! how the predictions reach the row's native channels
//! (`FTMachine::interpolateFrequencyFromgrid`), as [`NativeTerm`]s a backend
//! applies ([`Work::PredictNative`]) or the host applies for one row
//! ([`SpectralResampler::finish_prediction`]).

use num_complex::Complex32;

use super::{NativeRow, Sampling, SpectralAxis, SpectralResampler, is_autocorrelation};
use super::{RowPlacing, locate_sample};
use crate::backend::{GridBackend, PreparedModelGrids, Work};
use crate::convolution::ConvolutionFunctionSet;
use crate::error::OperatorError;
use crate::geometry::CellLocation;
use crate::native::{NO_CELL, NativeTarget, NativeTerm, interpolate_terms};
use crate::operator::MeasurementOperator;
use crate::sample::{CfKey, Placement, SampleBlock};

/// Reusable buffers for predicting a batch of rows; one per row chunk.
///
/// A batch is [`Self::begin`], then [`SpectralResampler::place_prediction`]
/// for each row, and either one [`Self::apply_native`] that degrids every
/// placed sample and interpolates the predictions to the rows' native cells
/// ([`SpectralResampler::native_terms`]) in a single backend call, or one
/// [`Self::apply`] and [`SpectralResampler::finish_prediction`] for each
/// row. [`SpectralResampler::predict_row`] is the batch of one row.
/// [`SpectralResampler::prediction_scratch`] allocates the buffers once for
/// a batch of its widest rows; a default scratch grows as batches need.
#[derive(Debug, Default)]
pub struct PredictionScratch {
    /// The batch's placements: a prediction reads no values or weights.
    placements: Vec<Placement>,
    npol: usize,
    predicted: Vec<Complex32>,
    /// The output or native channels the placements predict, row by row:
    /// several share a placement that reads the same taps.
    sources: Vec<Source>,
    /// While one row's terms are formed, the batch index of the placement
    /// at each of its output or native channels.
    slots: Vec<u32>,
    /// The native terms of the batch's rows.
    terms: Vec<NativeTerm>,
}

impl PredictionScratch {
    /// Scratch for `npol` correlations with room for `samples` degridded
    /// samples, `slots` output or native channels of one row and `terms`
    /// native cells, so that predicting rows within them never grows it
    /// beyond [`Self::bytes`].
    #[must_use]
    pub fn with_capacity(npol: usize, samples: usize, slots: usize, terms: usize) -> Self {
        Self {
            placements: Vec::with_capacity(samples),
            npol,
            predicted: Vec::with_capacity(samples * npol),
            sources: Vec::with_capacity(samples),
            slots: Vec::with_capacity(slots),
            terms: Vec::with_capacity(terms),
        }
    }

    /// Bytes [`Self::with_capacity`] allocates.
    #[must_use]
    pub const fn bytes(npol: usize, samples: usize, slots: usize, terms: usize) -> u64 {
        (samples * (size_of::<Placement>() + npol * size_of::<Complex32>() + size_of::<Source>())
            + slots * size_of::<u32>()
            + terms * size_of::<NativeTerm>()) as u64
    }

    /// Samples placed since [`Self::begin`]: one per run of a row's
    /// channels that read the same taps ([`SpectralResampler::place_prediction`]).
    #[must_use]
    pub fn samples(&self) -> usize {
        self.placements.len()
    }

    /// Bytes the scratch's buffers hold now, from their capacities.
    #[must_use]
    pub fn capacity_bytes(&self) -> u64 {
        (self.placements.capacity() * size_of::<Placement>()
            + self.predicted.capacity() * size_of::<Complex32>()
            + self.sources.capacity() * size_of::<Source>()
            + self.slots.capacity() * size_of::<u32>()
            + self.terms.capacity() * size_of::<NativeTerm>()) as u64
    }

    /// Start a batch of rows of `npol` correlations, clearing every buffer.
    pub fn begin(&mut self, npol: usize) {
        self.npol = npol;
        self.placements.clear();
        self.sources.clear();
        self.terms.clear();
    }

    /// The native terms [`SpectralResampler::native_terms`] added since
    /// [`Self::begin`].
    #[must_use]
    pub fn terms(&self) -> &[NativeTerm] {
        &self.terms
    }

    /// The batch's placements, as the block a backend predicts.
    fn placed(&self) -> SampleBlock<'_> {
        SampleBlock {
            placements: &self.placements,
            values: &[],
            weights: &[],
            sources: &[],
            npol: self.npol,
        }
    }

    /// Degrid every sample placed since [`Self::begin`] from `model` in one
    /// [`Work::Predict`] on `backend`, for
    /// [`SpectralResampler::finish_prediction`].
    ///
    /// # Errors
    ///
    /// Whatever `backend` returns.
    pub fn apply(
        &mut self,
        backend: &mut dyn GridBackend,
        cf: &dyn ConvolutionFunctionSet,
        model: &PreparedModelGrids,
    ) -> Result<(), OperatorError> {
        self.predicted.clear();
        self.predicted
            .resize(self.placements.len() * self.npol, Complex32::default());
        if self.placements.is_empty() {
            return Ok(());
        }
        let block = SampleBlock {
            placements: &self.placements,
            values: &[],
            weights: &[],
            sources: &[],
            npol: self.npol,
        };
        backend.apply(
            &block,
            cf,
            Work::Predict {
                model,
                out: &mut self.predicted,
            },
        )
    }

    /// Degrid every sample placed since [`Self::begin`] from `grids` and
    /// interpolate the predictions to the batch's native cells
    /// ([`Self::terms`]) in one [`Work::PredictNative`] on `backend`,
    /// writing `model`, or with `add` adding to it.
    ///
    /// # Errors
    ///
    /// Whatever `backend` returns.
    pub fn apply_native(
        &self,
        backend: &mut dyn GridBackend,
        cf: &dyn ConvolutionFunctionSet,
        grids: &PreparedModelGrids,
        model: NativeTarget<'_>,
        add: bool,
    ) -> Result<(), OperatorError> {
        if self.terms.is_empty() {
            return Ok(());
        }
        backend.apply(
            &self.placed(),
            cf,
            Work::PredictNative {
                grids,
                terms: &self.terms,
                model,
                add,
            },
        )
    }
}

/// One row's degridded samples within a [`PredictionScratch`] batch, from
/// [`SpectralResampler::place_prediction`]: the range of its channels'
/// sources.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PlacedRow {
    start: usize,
    end: usize,
}

/// One output or native channel a row's placement predicts: the channel's
/// slot in the row and the placement's index in the batch.
#[derive(Clone, Copy, Debug)]
struct Source {
    slot: u32,
    placement: u32,
}

/// The largest difference of phase-centre phase, in radians, at which two
/// samples that read the same taps of the same model share one placement:
/// 2⁻³⁰, 64 times below the resolution of an `f32` unit phasor (2⁻²⁴), so a
/// shared prediction differs from the sample's own by less than its
/// rounding. A row whose phase centre is shifted has phases that change
/// with every channel by far more, and shares nothing.
const SHARED_PHASE_RAD: f64 = 1.0 / (1u64 << 30) as f64;

/// What a backend's degrid of a placement reads beside its phasor: its
/// kernel cell and location on the grid, its plane, its signs of w, its
/// pointing gradient and, for a model of several terms, its Taylor
/// variable.
#[derive(Clone, Copy, PartialEq)]
struct Gathered {
    cf: CfKey,
    location: CellLocation,
    plane: u32,
    w_positive: bool,
    prediction_w_positive: bool,
    gradient: [u32; 2],
    spectral: u32,
}

impl Gathered {
    /// What `placement`, at `location`, reads for a model of `terms` terms.
    fn of(placement: &Placement, location: CellLocation, terms: usize) -> Self {
        Self {
            cf: placement.cf,
            location,
            plane: placement.plane,
            w_positive: placement.w > 0.0,
            prediction_w_positive: placement.prediction_w_positive,
            gradient: placement.gradient.map(f32::to_bits),
            spectral: if terms > 1 {
                placement.spectral.to_bits()
            } else {
                0
            },
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

/// Whether `frequencies_hz` strictly increase or strictly decrease.
fn monotonic(frequencies_hz: &[f64]) -> bool {
    frequencies_hz.windows(2).all(|pair| pair[0] < pair[1])
        || frequencies_hz.windows(2).all(|pair| pair[0] > pair[1])
}

/// Bit `p` set for each flagged correlation `p` of `channel`.
fn flag_bits(row: &NativeRow<'_>, npol: usize, channel: usize) -> u32 {
    row.flags[channel * npol..(channel + 1) * npol]
        .iter()
        .enumerate()
        .fold(0, |bits, (pol, flag)| bits | (u32::from(*flag) << pol))
}

impl SpectralResampler {
    /// The most samples [`Self::predict_row`] degrids for one native row of
    /// `channels` channels: one per channel, or under linear interpolation
    /// one per output channel.
    #[must_use]
    pub fn prediction_samples_per_row(&self, channels: usize) -> usize {
        match self.sampling {
            Sampling::Direct | Sampling::Nearest(_) => channels,
            Sampling::Linear(axis) => channels.max(axis.channels as usize),
        }
    }

    /// Reusable buffers for predicting batches of `rows` rows of at most
    /// `channels` channels of `npol` correlations, allocated once at
    /// [`PredictionScratch::bytes`] for every resampler in `resamplers`.
    #[must_use]
    pub fn prediction_scratch<'r>(
        resamplers: impl IntoIterator<Item = &'r Self>,
        npol: usize,
        channels: usize,
        rows: usize,
    ) -> PredictionScratch {
        let (samples, slots) = Self::prediction_extent(resamplers, channels);
        PredictionScratch::with_capacity(npol, rows * samples, slots, rows * channels)
    }

    /// Bytes [`Self::prediction_scratch`] holds for the same arguments.
    #[must_use]
    pub fn prediction_scratch_bytes<'r>(
        resamplers: impl IntoIterator<Item = &'r Self>,
        npol: usize,
        channels: usize,
        rows: usize,
    ) -> u64 {
        let (samples, slots) = Self::prediction_extent(resamplers, channels);
        PredictionScratch::bytes(npol, rows * samples, slots, rows * channels)
    }

    /// The most samples any of `resamplers` predicts with for rows of at
    /// most `channels` channels, and the most output or native channels of
    /// one row.
    fn prediction_extent<'r>(
        resamplers: impl IntoIterator<Item = &'r Self>,
        channels: usize,
    ) -> (usize, usize) {
        resamplers
            .into_iter()
            .fold((0, channels), |(samples, slots), resampler| {
                let linear = match resampler.sampling {
                    Sampling::Linear(axis) => axis.channels as usize,
                    Sampling::Direct | Sampling::Nearest(_) => 0,
                };
                (
                    samples.max(resampler.prediction_samples_per_row(channels)),
                    slots.max(linear),
                )
            })
    }

    /// The model visibility of every selected sample of `row`, written to
    /// `out` as `[channel][correlation]`, the way CASA predicts `MODEL_DATA`
    /// (`GridFT::get`, then `FTMachine::interpolateFrequencyFromgrid`).
    ///
    /// A flagged row, a flagged correlation, a native channel off the output
    /// axis and a sample whose kernel leaves the grid predict zero, and so
    /// does an autocorrelation unless the resampler is
    /// [`Self::predicting_autocorrelations`].
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
    ///
    /// This is the batch of one row; a caller predicting many rows places
    /// them all into one [`PredictionScratch`] batch, so that one backend
    /// call degrids them.
    ///
    /// # Errors
    ///
    /// A row inconsistent with `operator`, an `out` of another length than
    /// the row's values, or whatever `backend` returns.
    pub fn predict_row(
        &self,
        operator: &MeasurementOperator,
        backend: &mut dyn GridBackend,
        model: &PreparedModelGrids,
        row: &NativeRow<'_>,
        scratch: &mut PredictionScratch,
        out: &mut [Complex32],
    ) -> Result<(), OperatorError> {
        scratch.begin(operator.polarization().correlations().len());
        let placed = self.place_prediction(operator, model, row, scratch)?;
        scratch.apply(backend, operator.cf(), model)?;
        self.finish_prediction(operator, row, placed, scratch, out)
    }

    /// Whether `row`'s prediction interpolates degridded output channels to
    /// its native channels (see [`Self::predict_row`]).
    fn linear_prediction(&self, row: &NativeRow<'_>) -> Option<SpectralAxis> {
        match self.sampling {
            Sampling::Linear(axis) if axis.channels > 1 && row.frequencies_hz.len() > 1 => {
                Some(axis)
            }
            Sampling::Direct | Sampling::Nearest(_) | Sampling::Linear(_) => None,
        }
    }

    /// Add `row`'s degridded samples to the batch `scratch` began, as
    /// [`Self::predict_row`] degrids them; a flagged row adds none, nor an
    /// autocorrelation unless the resampler is
    /// [`Self::predicting_autocorrelations`].
    ///
    /// A sample that reads the same taps of the same model as the row's
    /// sample before it ([`Gathered`]), at a phase-centre phase within
    /// [`SHARED_PHASE_RAD`] of the first of their run, is not placed again:
    /// its channel reads that sample's prediction. A row's neighbouring
    /// channels sit a small fraction of a cell apart in uv, so they mostly
    /// share their kernel's fine offset, and a backend's degrid of a fine
    /// offset is the same for every sample at it, as CASA's gather reads
    /// the nearest one: an ALMA row of 4096 channels across 2 GHz placed
    /// one sample in 45 (#855).
    ///
    /// # Errors
    ///
    /// A row inconsistent with `operator`.
    pub fn place_prediction(
        &self,
        operator: &MeasurementOperator,
        model: &PreparedModelGrids,
        row: &NativeRow<'_>,
        scratch: &mut PredictionScratch,
    ) -> Result<PlacedRow, OperatorError> {
        let npol = operator.polarization().correlations().len();
        self.validate_row(operator, row, npol)?;
        let PredictionScratch {
            placements,
            sources,
            ..
        } = scratch;
        let start = sources.len();
        if row.row_flag || (!self.autocorrelations && is_autocorrelation(row)) {
            return Ok(PlacedRow { start, end: start });
        }
        let native = row.frequencies_hz;
        let prepared = model.layout().planes();
        let terms = self.basis.data_terms();
        let mut placing = RowPlacing::new(operator.cf(), operator.geometry(), row);
        if matches!(self.sampling, Sampling::Direct | Sampling::Nearest(_))
            && terms == 1
            && placing.uniform.is_some()
            && monotonic(native)
        {
            self.place_runs(operator, model, row, &mut placing, placements, sources);
            return Ok(PlacedRow {
                start,
                end: sources.len(),
            });
        }
        // The run of samples the last placement serves: what they read, the
        // first one's phase, and its batch index.
        let mut run: Option<(Gathered, f64, u32)> = None;
        let mut place = |plane: u32, frequency_hz: f64, slot: usize| {
            if !prepared.contains(plane) {
                return;
            }
            let Some((placement, location)) = locate_sample(
                operator.cf(),
                operator.geometry(),
                self.basis,
                row,
                plane,
                frequency_hz,
                &mut placing,
            ) else {
                return;
            };
            let gathered = Gathered::of(&placement, location, terms);
            let placement_index = match run {
                Some((first, phase, index))
                    if first == gathered && (placement.phase - phase).abs() <= SHARED_PHASE_RAD =>
                {
                    index
                }
                _ => {
                    let index = u32::try_from(placements.len()).expect("a batch fits u32 indices");
                    placements.push(placement);
                    run = Some((gathered, placement.phase, index));
                    index
                }
            };
            sources.push(Source {
                slot: u32::try_from(slot).expect("a row's channels fit u32"),
                placement: placement_index,
            });
        };
        match self.linear_prediction(row) {
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
                if let Some(channels) = degridded_channels(axis, native) {
                    for channel in channels {
                        place(channel, axis.centre_hz(channel), channel as usize);
                    }
                }
            }
        }
        Ok(PlacedRow {
            start,
            end: sources.len(),
        })
    }

    /// [`Self::place_prediction`] of `row`'s channels under direct or
    /// nearest sampling with one kernel cell and one term, in the runs the
    /// channel-by-channel placement finds, placing each run's first
    /// channel. A channel's plane, its location on the grid and its
    /// phase-centre phase move monotonically with its frequency, which
    /// `row`'s channels order, so once a channel reads other taps than its
    /// run's first, or its phase has moved past [`SHARED_PHASE_RAD`], every
    /// later one does too. A run's end is then found by locating a few
    /// channels rather than each: from the last run's length, then doubling
    /// and halving.
    fn place_runs(
        &self,
        operator: &MeasurementOperator,
        model: &PreparedModelGrids,
        row: &NativeRow<'_>,
        placing: &mut RowPlacing,
        placements: &mut Vec<Placement>,
        sources: &mut Vec<Source>,
    ) {
        let native = row.frequencies_hz;
        let channels = native.len();
        let prepared = model.layout().planes();
        let mut locate = |channel: usize| {
            let plane = match self.sampling {
                Sampling::Direct => Some(0),
                Sampling::Nearest(axis) | Sampling::Linear(axis) => {
                    axis.nearest_channel(native[channel])
                }
            }
            .filter(|plane| prepared.contains(*plane))?;
            locate_sample(
                operator.cf(),
                operator.geometry(),
                self.basis,
                row,
                plane,
                native[channel],
                placing,
            )
        };
        let (mut channel, mut length) = (0, 1);
        while channel < channels {
            let Some((placement, location)) = locate(channel) else {
                channel += 1;
                continue;
            };
            let (gathered, phase) = (Gathered::of(&placement, location, 1), placement.phase);
            let index = u32::try_from(placements.len()).expect("a batch fits u32 indices");
            placements.push(placement);
            let mut shares = |probe: usize| {
                locate(probe).is_some_and(|(placement, location)| {
                    Gathered::of(&placement, location, 1) == gathered
                        && (placement.phase - phase).abs() <= SHARED_PHASE_RAD
                })
            };
            // The last channel known to share, and the first known not to.
            let (mut last, mut past) = (channel, channels);
            let expected = channel + length - 1;
            if expected > channel && expected < channels {
                if shares(expected) {
                    last = expected;
                } else {
                    past = expected;
                }
            }
            let mut step = 1;
            while past == channels && last + step < channels {
                if shares(last + step) {
                    last += step;
                    step *= 2;
                } else {
                    past = last + step;
                }
            }
            while past - last > 1 {
                let middle = last + (past - last) / 2;
                if shares(middle) {
                    last = middle;
                } else {
                    past = middle;
                }
            }
            sources.extend((channel..=last).map(|slot| Source {
                slot: u32::try_from(slot).expect("a row's channels fit u32"),
                placement: index,
            }));
            length = last + 1 - channel;
            channel = last + 1;
        }
    }

    /// Add `row`'s native terms to the batch `scratch` began: one per native
    /// channel, the row's cells numbered from `first_cell`, interpolating
    /// its samples `placed` in the batch as [`Self::predict_row`] does. A
    /// cell whose channel is off the output axis and its halo, or whose row
    /// placed nothing, gets the zero model; a flagged correlation keeps it
    /// zero.
    pub fn native_terms(
        &self,
        row: &NativeRow<'_>,
        npol: usize,
        placed: PlacedRow,
        scratch: &mut PredictionScratch,
        first_cell: u32,
    ) {
        let PredictionScratch {
            sources,
            slots,
            terms,
            ..
        } = scratch;
        let native = row.frequencies_hz;
        let mut term = |channel: usize, low: u32, high: u32, fraction: f32| {
            terms.push(NativeTerm {
                cell: first_cell + channel as u32,
                low,
                high,
                fraction,
                flags: flag_bits(row, npol, channel),
            });
        };
        let mut index_slots = |count: usize| {
            slots.clear();
            slots.resize(count, NO_CELL);
            for source in &sources[placed.start..placed.end] {
                slots[source.slot as usize] = source.placement;
            }
        };
        let Some(axis) = self.linear_prediction(row) else {
            index_slots(native.len());
            for (channel, &index) in slots.iter().enumerate() {
                term(channel, index, index, 0.0);
            }
            return;
        };
        // `FTMachine::interpolateFrequencyFromgrid`: the output-channel
        // predictions on CASA's image-frequency grid, refined to
        // `floor(width ratio)` repeated points per channel when output
        // channels are wider than native ones, interpolated linearly to every
        // native channel that maps to the axis or lies in its linear halo
        // (`chanMap` 0… or −2; −1 stays zero); the end pair extrapolates
        // (casacore `InterpolateArray1D` linear).
        index_slots(axis.channels as usize);
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
                term(channel, NO_CELL, NO_CELL, 0.0);
                continue;
            }
            let position = (frequency_hz - fine_start) / fine_increment;
            let left = (position.floor().max(0.0) as usize).min(points - 2);
            let fraction = ((frequency_hz - point(left)) / (point(left + 1) - point(left))) as f32;
            let (low, high) = (left / per_channel, (left + 1) / per_channel);
            term(channel, slots[low], slots[high], fraction);
        }
    }

    /// Write `row`'s model visibilities to `out` as `[channel][correlation]`
    /// from its samples `placed` in the batch `scratch` applied, as
    /// [`Self::predict_row`] does.
    ///
    /// # Errors
    ///
    /// An `out` of another length than the row's values.
    pub fn finish_prediction(
        &self,
        operator: &MeasurementOperator,
        row: &NativeRow<'_>,
        placed: PlacedRow,
        scratch: &mut PredictionScratch,
        out: &mut [Complex32],
    ) -> Result<(), OperatorError> {
        if out.len() != row.values.len() {
            return Err(OperatorError::NativeRow {
                reason: "the prediction holds one value per selected sample",
            });
        }
        let npol = operator.polarization().correlations().len();
        let first = scratch.terms.len();
        self.native_terms(row, npol, placed, scratch, 0);
        interpolate_terms(
            &scratch.predicted,
            npol,
            &scratch.terms[first..],
            out,
            0,
            false,
        );
        scratch.terms.truncate(first);
        Ok(())
    }
}
