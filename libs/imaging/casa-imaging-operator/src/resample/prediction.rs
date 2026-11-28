// SPDX-License-Identifier: LGPL-3.0-or-later
//! Predicting a batch of rows: the samples a row's prediction degrids, and
//! how the predictions reach the row's native channels
//! (`FTMachine::interpolateFrequencyFromgrid`), as [`NativeTerm`]s a backend
//! applies ([`Work::PredictNative`]) or the host applies for one row
//! ([`SpectralResampler::finish_prediction`]).

use num_complex::Complex32;

use super::spline::SplineScratch;
use super::stencil::Stencil;
use super::{NativeRow, Order, Sampling, SpectralAxis, SpectralResampler, is_autocorrelation};
use super::{RowPlacing, locate_sample};
use crate::backend::{GridBackend, PreparedModelGrids, Work};
use crate::convolution::ConvolutionFunctionSet;
use crate::error::OperatorError;
use crate::geometry::CellLocation;
use crate::native::{NO_CELL, NativeTarget, NativeTerm, STENCIL, interpolate_terms};
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
    /// The tridiagonal system for the legacy spline's image planes.
    spline: SplineScratch,
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
            spline: SplineScratch::default(),
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
            + self.spline.capacity_bytes()
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

/// Consecutive output or native channels of a row that one placement
/// predicts: the first channel's slot in the row, how many, and the
/// placement's index in the batch. A row's sources run in slot order.
#[derive(Clone, Copy, Debug)]
struct Source {
    slot: u32,
    count: u32,
    placement: u32,
}

impl Source {
    /// The row's slots this source covers.
    fn slots(self) -> std::ops::Range<usize> {
        self.slot as usize..(self.slot + self.count) as usize
    }
}

fn index_sources(slots: &mut Vec<u32>, sources: &[Source], count: usize) {
    slots.clear();
    slots.resize(count, NO_CELL);
    for source in sources {
        slots[source.slots()].fill(source.placement);
    }
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

/// The output channels CASA degrids for one row under interpolation
/// (`FTMachine::getInterpolateArrays`, after `GridFT::get` returns early
/// when no native maps): each native's `chanMap` is its channel, −2 in the
/// interpolation's halo or −1; −1 is replaced by the maximum, and the channels from
/// the minimum to the maximum are degridded, from channel 0 when the
/// minimum is the halo's −2 or equals the maximum. When every native lies
/// in the halo the maximum is −2 and CASA's unsigned loop flags nothing, so
/// every channel is degridded. `None` when nothing maps.
fn degridded_channels(
    planes: u32,
    native: impl Iterator<Item = f64> + Clone,
    map: impl Fn(f64) -> i64,
) -> Option<std::ops::RangeInclusive<u32>> {
    let maximum = native.clone().map(&map).max()?;
    if maximum == -1 {
        return None;
    }
    if maximum == -2 {
        return Some(0..=planes - 1);
    }
    let minimum = native
        .map(|frequency_hz| match map(frequency_hz) {
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
        match &self.sampling {
            Sampling::Direct | Sampling::Nearest(_) => channels,
            Sampling::Interpolated(axis, _) => channels.max(axis.channels as usize),
        }
    }

    /// Reusable buffers for predicting batches of `rows` rows of at most
    /// `channels` channels of `npol` correlations, allocated once at
    /// [`Self::prediction_scratch_bytes`] for every resampler in `resamplers`.
    #[must_use]
    pub fn prediction_scratch<'r>(
        resamplers: impl IntoIterator<Item = &'r Self>,
        npol: usize,
        channels: usize,
        rows: usize,
    ) -> PredictionScratch {
        let (samples, slots, spline) = Self::prediction_extent(resamplers, channels);
        let mut scratch =
            PredictionScratch::with_capacity(npol, rows * samples, slots, rows * channels);
        scratch.spline = SplineScratch::with_capacity(spline, npol);
        scratch
    }

    /// Bytes [`Self::prediction_scratch`] holds for the same arguments.
    #[must_use]
    pub fn prediction_scratch_bytes<'r>(
        resamplers: impl IntoIterator<Item = &'r Self>,
        npol: usize,
        channels: usize,
        rows: usize,
    ) -> u64 {
        let (samples, slots, spline) = Self::prediction_extent(resamplers, channels);
        PredictionScratch::bytes(npol, rows * samples, slots, rows * channels)
            + SplineScratch::bytes(spline, npol)
    }

    /// The most samples any of `resamplers` predicts with for rows of at
    /// most `channels` channels, and the most output or native channels of
    /// one row and the largest legacy spline.
    fn prediction_extent<'r>(
        resamplers: impl IntoIterator<Item = &'r Self>,
        channels: usize,
    ) -> (usize, usize, usize) {
        resamplers
            .into_iter()
            .fold((0, channels, 0), |(samples, slots, spline), resampler| {
                let planes = match &resampler.sampling {
                    Sampling::Interpolated(axis, _) => axis.channels as usize,
                    Sampling::Direct | Sampling::Nearest(_) => 0,
                };
                (
                    samples.max(resampler.prediction_samples_per_row(channels)),
                    slots.max(planes),
                    if resampler.uses_legacy_spline() {
                        spline.max(planes)
                    } else {
                        spline
                    },
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
        self.predict_row_with_coverage(operator, backend, model, row, scratch, (out, None))
    }

    /// Predict a row and optionally mark the samples the gridder assigned.
    ///
    /// This distinguishes a sampled zero from an untouched cell in CASA's
    /// legacy imager, which degrids into the existing model visibility.
    /// # Errors
    /// The same errors as [`Self::predict_row`] and a mismatched coverage slice.
    pub fn predict_row_with_coverage(
        &self,
        operator: &MeasurementOperator,
        backend: &mut dyn GridBackend,
        model: &PreparedModelGrids,
        row: &NativeRow<'_>,
        scratch: &mut PredictionScratch,
        output: (&mut [Complex32], Option<&mut [bool]>),
    ) -> Result<(), OperatorError> {
        scratch.begin(operator.polarization().correlations().len());
        let placed = self.place_prediction(operator, model, row, scratch)?;
        scratch.apply(backend, operator.cf(), model)?;
        self.finish_prediction_with_coverage(operator, row, placed, scratch, output)
    }

    /// Interpolate already sampled image channels for a legacy imager that
    /// samples its sky in the image plane, such as SDGrid. `sample` fills a
    /// plane's correlations in row order. Returns false for a one-channel
    /// or nearest-neighbour prediction, which the caller samples directly.
    ///
    /// # Errors
    /// The native flags or output do not have one cell per channel and
    /// correlation, or the live cubic grid is too short for interpolation.
    pub fn predict_sampled_spectrum(
        &self,
        row: &NativeRow<'_>,
        correlations: usize,
        scratch: &mut PredictionScratch,
        output: &mut [Complex32],
        mut sample: impl FnMut(u32, &mut [Complex32]),
    ) -> Result<bool, OperatorError> {
        let Some((axis, order)) = self.interpolated_prediction(row) else {
            return Ok(false);
        };
        if correlations == 0
            || output.len() != row.frequencies_hz.len() * correlations
            || row.flags.len() != output.len()
        {
            return Err(OperatorError::NativeRow {
                reason: "sampled spectrum does not match native channel and correlation dimensions",
            });
        }
        if row.row_flag {
            output.fill(Complex32::default());
            return Ok(true);
        }
        self.validate_legacy_prediction(row.frequencies_hz)?;
        scratch.begin(correlations);
        scratch.predicted.clear();
        if let Some(channels) =
            self.interpolated_planes(axis, order, row.frequencies_hz, row.selection_centres_hz)
        {
            for channel in channels {
                let start = scratch.predicted.len();
                scratch
                    .predicted
                    .resize(start + correlations, Complex32::default());
                sample(channel, &mut scratch.predicted[start..]);
                scratch.sources.push(Source {
                    slot: channel,
                    count: 1,
                    placement: (start / correlations) as u32,
                });
            }
        }
        let placed = PlacedRow {
            start: 0,
            end: scratch.sources.len(),
        };
        if self.uses_legacy_spline() {
            self.finish_spline(axis, row, placed, scratch, (output, None));
        } else {
            self.native_terms(row, correlations, placed, scratch, 0);
            interpolate_terms(
                &scratch.predicted,
                correlations,
                &scratch.terms,
                output,
                0,
                false,
            );
        }
        Ok(true)
    }

    fn prediction_channels<'a>(&self, native: &'a [f64]) -> impl Iterator<Item = f64> + Clone + 'a {
        let [start, count, step] = self
            .legacy_prediction_map
            .as_ref()
            .and_then(|mapping| mapping.selection)
            .unwrap_or([0, native.len(), 1]);
        native
            .iter()
            .copied()
            .skip(start)
            .step_by(step.max(1))
            .take(count)
    }

    fn prediction_pair(&self, native: &[f64], selection_centres_hz: Option<[f64; 2]>) -> [f64; 2] {
        if let Some(pair) = selection_centres_hz
            && self
                .legacy_prediction_map
                .as_ref()
                .is_none_or(|mapping| mapping.selection.is_none())
        {
            return pair;
        }
        let mut selected = self.prediction_channels(native);
        selected
            .next()
            .zip(selected.next())
            .map_or([0.; 2], |(first, second)| [first, second])
    }

    fn prediction_width(&self, native: &[f64], selection_centres_hz: Option<[f64; 2]>) -> f64 {
        let [first, second] = self.prediction_pair(native, selection_centres_hz);
        second - first
    }

    fn prediction_channel(
        &self,
        axis: &SpectralAxis,
        order: Order,
        frequency: f64,
        width: f64,
    ) -> i64 {
        if let Some(mapping) = self.legacy_prediction_map.as_ref() {
            mapping
                .axis
                .legacy_prediction_channel(frequency, width, order)
        } else if let Some(channel) = axis.nearest_channel(frequency) {
            i64::from(channel)
        } else if axis.in_halo(frequency, width, order) {
            -2
        } else {
            -1
        }
    }

    fn interpolated_planes(
        &self,
        axis: &SpectralAxis,
        order: Order,
        native: &[f64],
        selection_centres_hz: Option<[f64; 2]>,
    ) -> Option<std::ops::RangeInclusive<u32>> {
        let width = self.prediction_width(native, selection_centres_hz);
        let map = |frequency| self.prediction_channel(axis, order, frequency, width);
        degridded_channels(axis.channels, self.prediction_channels(native), map)
    }

    /// Validate CASA's four-image-plane minimum for live cubic prediction
    /// before a caller opens the scratch writer. Single-channel and entirely
    /// unmapped buffers bypass interpolation.
    /// # Errors
    /// This is an active legacy cubic buffer with fewer than four image planes.
    pub fn validate_legacy_prediction(&self, native: &[f64]) -> Result<(), OperatorError> {
        if let Sampling::Interpolated(axis, Order::Cubic) = &self.sampling
            && self.legacy_prediction_map.is_some()
            && !self.uses_legacy_spline()
            && (2..4).contains(&axis.channels)
            && self.prediction_channels(native).nth(1).is_some()
            && self
                .interpolated_planes(axis, Order::Cubic, native, None)
                .is_some()
        {
            return Err(OperatorError::SpectralAxis {
                reason: "legacy cubic scratch prediction requires at least four image channels",
            });
        }
        Ok(())
    }

    fn matching_axis<'a>(&'a self, axis: &'a SpectralAxis) -> &'a SpectralAxis {
        self.legacy_prediction_map
            .as_ref()
            .map_or(axis, |mapping| &mapping.axis)
    }

    /// The axis and order by which `row`'s prediction interpolates
    /// degridded output channels to its native channels, when it does (see
    /// [`Self::predict_row`]).
    fn interpolated_prediction(&self, row: &NativeRow<'_>) -> Option<(&SpectralAxis, Order)> {
        match &self.sampling {
            Sampling::Interpolated(axis, order)
                if axis.channels > 1
                    && self
                        .prediction_channels(row.frequencies_hz)
                        .nth(1)
                        .is_some() =>
            {
                Some((axis, *order))
            }
            Sampling::Direct | Sampling::Nearest(_) | Sampling::Interpolated(..) => None,
        }
    }

    /// Add `row`'s degridded samples to the batch `scratch` began, as
    /// [`Self::predict_row`] degrids them; a flagged row adds none, nor an
    /// autocorrelation unless the resampler is
    /// [`Self::predicting_autocorrelations`].
    ///
    /// A sample that reads the same taps of the same model as the row's
    /// sample before it (the same kernel cell, location, plane, signs of w
    /// and pointing gradient, and Taylor variable for a model of several
    /// terms), at a phase-centre phase within 2⁻³⁰ rad of the first of
    /// their run, is not placed again:
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
        let Some(mut placing) = RowPlacing::new(operator.cf(), operator.geometry(), row)? else {
            return Ok(PlacedRow { start, end: start });
        };
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
        let mut place = |plane: u32, frequency: (f64, Option<usize>), slot: usize| {
            if !prepared.contains(plane) {
                return;
            }
            let Some((placement, location)) = locate_sample(
                operator.cf(),
                operator.geometry(),
                self.basis,
                row,
                plane,
                frequency,
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
            let slot = u32::try_from(slot).expect("a row's channels fit u32");
            match sources[start..].last_mut() {
                Some(last)
                    if last.placement == placement_index && last.slot + last.count == slot =>
                {
                    last.count += 1;
                }
                _ => sources.push(Source {
                    slot,
                    count: 1,
                    placement: placement_index,
                }),
            }
        };
        match self.interpolated_prediction(row) {
            None => {
                for (channel, frequency_hz) in native.iter().enumerate() {
                    let plane = match &self.sampling {
                        Sampling::Direct => Some(0),
                        Sampling::Nearest(axis) | Sampling::Interpolated(axis, _) => {
                            self.matching_axis(axis).nearest_channel(*frequency_hz)
                        }
                    };
                    if let Some(plane) = plane {
                        place(plane, (*frequency_hz, Some(channel)), channel);
                    }
                }
            }
            Some((axis, order)) => {
                if let Some(channels) =
                    self.interpolated_planes(axis, order, native, row.selection_centres_hz)
                {
                    for channel in channels {
                        place(channel, (axis.centre_hz(channel), None), channel as usize);
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
            let plane = match &self.sampling {
                Sampling::Direct => Some(0),
                Sampling::Nearest(axis) | Sampling::Interpolated(axis, _) => {
                    self.matching_axis(axis).nearest_channel(native[channel])
                }
            }
            .filter(|plane| prepared.contains(*plane))?;
            locate_sample(
                operator.cf(),
                operator.geometry(),
                self.basis,
                row,
                plane,
                (native[channel], Some(channel)),
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
            sources.push(Source {
                slot: u32::try_from(channel).expect("a row's channels fit u32"),
                count: u32::try_from(last + 1 - channel).expect("a row's channels fit u32"),
                placement: index,
            });
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
    ///
    /// # Panics
    /// Active legacy spline interpolation needs [`Self::finish_prediction`].
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
        let sources = &sources[placed.start..placed.end];
        // A row without a flag packs no flag bits.
        let flagged = row.flags.contains(&true);
        let mut term = |channel: usize, points: &[(u32, f64)]| {
            let mut term = NativeTerm {
                cell: first_cell + channel as u32,
                points: [NO_CELL; STENCIL],
                factors: [0.0; STENCIL],
                flags: if flagged {
                    flag_bits(row, npol, channel)
                } else {
                    0
                },
            };
            for (slot, (point, factor)) in points.iter().enumerate() {
                term.points[slot] = *point;
                term.factors[slot] = *factor as f32;
            }
            terms.push(term);
        };
        let Some((axis, order)) = self.interpolated_prediction(row) else {
            // Each native channel reads its source's placement, in slot
            // order; a channel no source covers reads nothing.
            let mut channel = 0;
            for source in sources {
                debug_assert!(source.slot as usize >= channel, "sources run in slot order");
                for unplaced in channel..source.slot as usize {
                    term(unplaced, &[]);
                }
                for placed in source.slots() {
                    term(placed, &[(source.placement, 1.0)]);
                }
                channel = source.slots().end;
            }
            for unplaced in channel..native.len() {
                term(unplaced, &[]);
            }
            return;
        };
        assert!(!self.uses_legacy_spline(), "a spline needs host prediction");
        // `FTMachine::interpolateFrequencyFromgrid`: the output-channel
        // predictions on CASA's image-frequency grid, refined to
        // `floor(width ratio)` repeated points per channel when output
        // channels are wider than native ones, interpolated to every native
        // channel that maps to the axis or lies in its halo (`chanMap` 0… or
        // −2; −1 stays zero); the end points extrapolate (casacore
        // `InterpolateArray1D`, linear or cubic).
        index_sources(slots, sources, axis.channels as usize);
        let pair = self.prediction_pair(native, row.selection_centres_hz);
        let native_width_hz = pair[1] - pair[0];
        let width = (axis.increment_hz / native_width_hz).abs();
        let legacy = self.legacy_prediction_map.is_some();
        let per_channel = if legacy {
            1
        } else {
            axis.fine_samples_per_plane(width, native_width_hz, pair)
        };
        let fine_increment = axis.increment_hz / per_channel as f64;
        let fine_start = if per_channel > 1 {
            axis.first_hz - axis.increment_hz / 2.0 + fine_increment / 2.0
        } else {
            axis.first_hz
        };
        let points = axis.channels as usize * per_channel;
        let point = |index: usize| {
            if legacy || (width <= 1.0 && (axis.reporting_axis.is_some() || axis.tabular.is_some()))
            {
                axis.centre_hz(index as u32)
            } else {
                fine_start + index as f64 * fine_increment
            }
        };
        for (channel, frequency_hz) in native.iter().enumerate() {
            if self.prediction_channel(axis, order, *frequency_hz, native_width_hz) == -1 {
                term(channel, &[]);
                continue;
            }
            let position = if legacy || width <= 1.0 {
                axis.tabular_pixel(*frequency_hz)
            } else {
                None
            }
            .unwrap_or_else(|| (frequency_hz - fine_start) / fine_increment);
            let slot = |point: usize| slots[point / per_channel];
            match order {
                Order::Linear => {
                    let left = (position.floor().max(0.0) as usize).min(points - 2);
                    let fraction = (frequency_hz - point(left)) / (point(left + 1) - point(left));
                    // casacore's `y1 + frac·(y2 − y1)`, as two factors.
                    term(
                        channel,
                        &[(slot(left), 1.0 - fraction), (slot(left + 1), fraction)],
                    );
                }
                Order::Cubic => {
                    // `binarySearchBrackets`: the point at the frequency,
                    // else the first beyond it.
                    let bracket = if position <= 0.0 {
                        0
                    } else {
                        (position.ceil() as usize).min(points)
                    };
                    let stencil = Stencil::cubic(points, bracket, *frequency_hz, point);
                    let mut factors = [(NO_CELL, 0.0); STENCIL];
                    for (entry, (index, factor)) in factors.iter_mut().zip(stencil.terms()) {
                        *entry = (slot(index), factor);
                    }
                    term(channel, &factors[..stencil.len]);
                }
            }
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
        self.finish_prediction_with_coverage(operator, row, placed, scratch, (out, None))
    }

    fn finish_prediction_with_coverage(
        &self,
        operator: &MeasurementOperator,
        row: &NativeRow<'_>,
        placed: PlacedRow,
        scratch: &mut PredictionScratch,
        (out, coverage): (&mut [Complex32], Option<&mut [bool]>),
    ) -> Result<(), OperatorError> {
        if out.len() != row.values.len() {
            return Err(OperatorError::NativeRow {
                reason: "the prediction holds one value per selected sample",
            });
        }
        if coverage
            .as_ref()
            .is_some_and(|bits| bits.len() != out.len())
        {
            return Err(OperatorError::NativeRow {
                reason: "coverage holds one bit per selected sample",
            });
        }
        if self.uses_legacy_spline()
            && let Some((axis, _)) = self.interpolated_prediction(row)
        {
            self.finish_spline(axis, row, placed, scratch, (out, coverage));
            return Ok(());
        }
        let npol = operator.polarization().correlations().len();
        let first = scratch.terms.len();
        self.native_terms(row, npol, placed, scratch, 0);
        if let Some(coverage) = coverage {
            let polarization = operator.polarization().pol_map();
            let legacy = self
                .legacy_prediction_map
                .as_ref()
                .and(self.interpolated_prediction(row))
                .map(|(axis, order)| {
                    (
                        axis,
                        order,
                        self.prediction_width(row.frequencies_hz, row.selection_centres_hz),
                        self.interpolated_planes(
                            axis,
                            order,
                            row.frequencies_hz,
                            row.selection_centres_hz,
                        )
                        .is_some(),
                    )
                });
            for (channel, (term, covered)) in scratch.terms[first..]
                .iter()
                .zip(coverage.chunks_exact_mut(npol))
                .enumerate()
            {
                let has_sample = term.points.iter().any(|&point| point != NO_CELL);
                // Legacy swapyz copies its values even if the degrid skipped
                // a row or correlation. In that case the updated model is zero.
                let legacy_assignment = legacy.map(|(axis, order, width, reaches_grid)| {
                    reaches_grid
                        && self.prediction_channel(axis, order, row.frequencies_hz[channel], width)
                            != -1
                });
                for (index, value) in covered.iter_mut().enumerate() {
                    let assigned =
                        legacy_assignment.unwrap_or(has_sample && polarization[index].is_some());
                    *value = assigned && term.flags & (1 << index) == 0;
                }
            }
        }
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

    fn finish_spline(
        &self,
        axis: &SpectralAxis,
        row: &NativeRow<'_>,
        placed: PlacedRow,
        scratch: &mut PredictionScratch,
        (out, mut coverage): (&mut [Complex32], Option<&mut [bool]>),
    ) {
        out.fill(Complex32::default());
        if let Some(bits) = coverage.as_deref_mut() {
            bits.fill(false);
        }
        let native = row.frequencies_hz;
        if self
            .interpolated_planes(axis, Order::Cubic, native, row.selection_centres_hz)
            .is_none()
        {
            return;
        }
        let npol = scratch.npol;
        let sources = &scratch.sources[placed.start..placed.end];
        let PredictionScratch {
            slots,
            spline,
            predicted,
            ..
        } = scratch;
        index_sources(slots, sources, axis.channels as usize);
        let value = |plane: usize, pol: usize| match slots[plane] {
            NO_CELL => Complex32::default(),
            sample => predicted[sample as usize * npol + pol],
        };
        if !sources.is_empty() {
            spline.prepare(axis, npol, value);
        }
        let width = self.prediction_width(native, row.selection_centres_hz);
        for (channel, &frequency) in native.iter().enumerate() {
            if self.prediction_channel(axis, Order::Cubic, frequency, width) == -1 {
                continue;
            }
            let start = channel * npol;
            let flags = &row.flags[start..start + npol];
            if let Some(bits) = coverage.as_deref_mut() {
                for (covered, &flag) in bits[start..start + npol].iter_mut().zip(flags) {
                    *covered = !flag;
                }
            }
            if !sources.is_empty() {
                spline.evaluate(axis, frequency, value, flags, &mut out[start..start + npol]);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RowContext, SpectralKernel};

    #[test]
    fn tabular_model_prediction_refines_a_uniform_inverse_only_for_wider_image_planes() {
        let axis = SpectralAxis::tabular(&[140.0, 120.0, 107.0, 100.0], -20.0).unwrap();
        let resampler = SpectralResampler::channel_local(axis, SpectralKernel::Linear);
        for (frequencies, factor) in [
            ([100.0, 110.0, 120.0], 0.5),
            ([92.0, 119.0, 146.0], 1.0 / 13.0),
        ] {
            let mut scratch = PredictionScratch::with_capacity(1, 4, 4, 3);
            scratch.sources.extend((0..4).map(|slot| Source {
                slot,
                count: 1,
                placement: slot,
            }));
            let row = NativeRow {
                selection_centres_hz: None,
                uvw_m: [1.0, 0.0, 0.0],
                phase_shift_m: 0.0,
                pointing_offset_rad: [0.0; 2],
                frequencies_hz: &frequencies,
                values: &[Complex32::default(); 3],
                weights: &[1.0; 3],
                flags: &[false; 3],
                row_flag: false,
                context: RowContext {
                    antennas: [0, 1],
                    antenna_types: [0, 0],
                    time_s: 0.0,
                    field: 0,
                    spectral_window: 0,
                    parallactic_angle_rad: [0.0; 2],
                    native_frequency_ratio: 1.0,
                    original_uvw_m: None,
                },
            };
            resampler.native_terms(&row, 1, PlacedRow { start: 0, end: 4 }, &mut scratch, 0);
            assert_eq!(&scratch.terms()[1].points[..2], [1, 2]);
            assert!((scratch.terms()[1].factors[1] - factor).abs() < 2e-7);
        }
    }
}
