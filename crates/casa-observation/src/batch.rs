// SPDX-License-Identifier: LGPL-3.0-or-later
//! The visibility batch: whole times of one partition's time × baseline
//! grid, with their coordinates and the borrowed payload of one data group.

use std::ops::Range;
use std::sync::Arc;

use casa_types::Correlation;
use casa_types::measures::{EpochView, FrequencyView, UvwView};
use ndarray::{ArrayView1, ArrayView3, s};
use num_complex::Complex32;

use crate::ObservationError;
use crate::catalogue::{Catalogue, DataGroupEntry, FieldId, OrderedBaseline, PartitionId, ScanId};
use crate::grouping::GroupFragment;
use crate::request::CorrelationAxis;

/// One sample's coordinates, assembled on the stack.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SampleCoordinate {
    /// Time offset in seconds from the batch's [`EpochView::origin`].
    pub time_offset_s: f64,
    /// The baseline.
    pub baseline: OrderedBaseline,
    /// The field.
    pub field: FieldId,
}

/// A per-sample quantity in the form the store shares it: one value per
/// sample, one per time shared by its baselines, or one for the whole batch.
/// [`Self::get`] resolves any of them for a sample, so a consumer needs no
/// rule of either format.
#[derive(Clone, Copy, Debug)]
pub enum Shared<'a, T> {
    /// One value per sample (MSv2 rows; MSv4 `[time, baseline]` variables).
    PerSample(ArrayView1<'a, T>),
    /// One value per time, shared by its baselines.
    PerTime(ArrayView1<'a, T>),
    /// One value for every sample (MSv4 `time.integration_time`).
    Constant(T),
}

impl<'a, T: Copy> Shared<'a, T> {
    /// The value of `sample`, in a batch with `samples_per_time` samples
    /// per time ([`VisibilityBatch::samples_per_time`]).
    ///
    /// # Panics
    ///
    /// If `sample` is out of range.
    #[must_use]
    pub fn get(&self, sample: usize, samples_per_time: usize) -> T {
        match self {
            Self::PerSample(values) => values[sample],
            Self::PerTime(values) => values[sample / samples_per_time],
            Self::Constant(value) => *value,
        }
    }

    fn window(&self, samples: Range<usize>, times: Range<usize>) -> Self {
        match self {
            Self::PerSample(values) => Self::PerSample(values.slice_move(s![samples])),
            Self::PerTime(values) => Self::PerTime(values.slice_move(s![times])),
            Self::Constant(value) => Self::Constant(*value),
        }
    }
}

/// The samples' time auxiliaries, each present when the request asked for
/// it. Times are in the batch's time scale and origin.
#[derive(Clone, Copy, Debug, Default)]
pub struct SampleTiming<'a> {
    /// Time centroid of each sample: one entry per sample, same scale and
    /// origin as [`VisibilityBatch::time`].
    pub centroid: Option<EpochView<'a>>,
    /// Integration interval in seconds.
    pub interval_s: Option<Shared<'a, f64>>,
    /// Effective integration time in seconds.
    pub exposure_s: Option<Shared<'a, f64>>,
}

impl SampleTiming<'_> {
    fn window(&self, samples: Range<usize>, times: Range<usize>) -> Self {
        Self {
            centroid: self
                .centroid
                .map(|centroid| centroid.slice(samples.clone())),
            interval_s: self
                .interval_s
                .map(|interval| interval.window(samples.clone(), times.clone())),
            exposure_s: self
                .exposure_s
                .map(|exposure| exposure.window(samples, times)),
        }
    }
}

/// The borrowed payload of one data group.
///
/// A role is `None` only when the request did not ask for it; a role the
/// request needs but the store lacks fails binding. Every array is
/// `[sample, channel, correlation]` over the delivered correlations. The
/// weights of a store that holds one weight per correlation (MSv2 `WEIGHT`)
/// are the same view with a zero stride along the channel axis: no copy,
/// and `as_slice()` is `None` on it. `flags` is the stored flag state with
/// absent acquisitions folded in (MSv2 `FLAG_ROW` and padding; in MSv4 a
/// non-finite stored weight); [`VisibilityBatch::validity`] is the one rule
/// that decides whether a value is used.
#[derive(Clone, Copy, Debug)]
pub struct DataGroupView<'a> {
    /// The group's catalogue entry: name, units, UVW frame.
    pub entry: &'a DataGroupEntry,
    /// Visibilities.
    pub values: Option<ArrayView3<'a, Complex32>>,
    /// Flags, absent acquisitions folded in; true excludes the value.
    pub flags: Option<ArrayView3<'a, bool>>,
    /// Weights.
    pub weights: Option<ArrayView3<'a, f32>>,
    /// UVW per sample, in the data group's effective frame
    /// ([`UvwFrame`](crate::UvwFrame)).
    pub uvw: Option<UvwView<'a>>,
}

impl DataGroupView<'_> {
    fn window(&self, samples: Range<usize>, channels: Range<usize>) -> Self {
        Self {
            entry: self.entry,
            values: self
                .values
                .map(|values| values.slice_move(s![samples.clone(), channels.clone(), ..])),
            flags: self
                .flags
                .map(|flags| flags.slice_move(s![samples.clone(), channels.clone(), ..])),
            weights: self
                .weights
                .map(|weights| weights.slice_move(s![samples.clone(), channels, ..])),
            uvw: self.uvw.map(|uvw| uvw.slice(samples)),
        }
    }
}

/// Whether each value of a batch is used: the contract's single validity
/// rule, the same for every store.
///
/// A value `(sample, channel, correlation)` is excluded when its delivered
/// flag is set or, among the roles read, its visibility or weight is not
/// finite. A delivered flag already marks every absent acquisition, in
/// every store: MSv2 `FLAG_ROW` and the cells the MSv2 adapter pads (no row
/// filled them: flagged, values and weights NaN) are flagged, and the MSv4 adapter
/// folds a non-finite stored weight into the flag, because XRADIO pads a
/// cell no row filled with NaN and leaves its `FLAG` false. So the rule
/// gives one answer for one observation whichever store holds it and
/// whichever roles are read, a flags-only read included, and a consumer
/// applies no format-specific rule; the values and weights catch corrupt
/// values besides.
#[derive(Clone, Copy, Debug)]
pub struct Validity<'a> {
    flags: Option<ArrayView3<'a, bool>>,
    values: Option<ArrayView3<'a, Complex32>>,
    weights: Option<ArrayView3<'a, f32>>,
}

impl Validity<'_> {
    /// Whether `(sample, channel, correlation)` is excluded.
    ///
    /// # Panics
    ///
    /// If an index is out of range.
    #[must_use]
    pub fn excluded(&self, sample: usize, channel: usize, correlation: usize) -> bool {
        let at = [sample, channel, correlation];
        self.flags.is_some_and(|flags| flags[at])
            || self.values.is_some_and(|values| !values[at].is_finite())
            || self.weights.is_some_and(|weights| !weights[at].is_finite())
    }

    /// The rule evaluated for every element into `mask`, replacing its
    /// contents: `[sample, channel, correlation]` in standard order, true
    /// where excluded. For a consumer that reads the batch more than once
    /// and would otherwise evaluate the rule each time.
    pub fn mask_into(&self, mask: &mut Vec<bool>, shape: [usize; 3]) {
        mask.clear();
        mask.resize(shape.iter().product(), false);
        if let Some(flags) = self.flags {
            for (slot, flag) in mask.iter_mut().zip(flags.iter()) {
                *slot = *flag;
            }
        }
        if let Some(values) = self.values {
            for (slot, value) in mask.iter_mut().zip(values.iter()) {
                *slot |= !value.is_finite();
            }
        }
        if let Some(weights) = self.weights {
            for (slot, weight) in mask.iter_mut().zip(weights.iter()) {
                *slot |= !weight.is_finite();
            }
        }
    }
}

/// Whole times of one partition's time × baseline grid, their coordinates
/// and one data group's payload, borrowed from the caller's
/// [`BatchBuffers`] and the observation's [`Catalogue`].
///
/// Sample `t × B + b` is time `t` of the batch and baseline `b` of the
/// partition's table ([`Self::baselines`]); the coordinates cost `T + B`
/// entries, not `T × B`. Both stores deliver this shape: MSv4 holds it, the
/// MSv2 adapter places rows on it at opening and pads the cells no row
/// filled (flag true, values and weights NaN). Field and scan are per time.
///
/// Every view is filled and checked before the batch is delivered: reading
/// an element performs no I/O or decoding. The borrow keeps the buffers
/// from being refilled while any view of them is alive.
///
/// [`BatchBuffers`]: crate::BatchBuffers
/// [`Catalogue`]: crate::Catalogue
#[derive(Clone, Copy, Debug)]
pub struct VisibilityBatch<'a> {
    /// The observation's catalogue, which the batch was checked against:
    /// every identity in the batch (partition, field, scan, antenna) names
    /// an entry of it.
    pub catalogue: &'a Arc<Catalogue>,
    /// The partition the samples belong to.
    pub partition: PartitionId,
    /// The batch's times, in the partition's scale and origin.
    pub time: EpochView<'a>,
    /// The partition's baseline table, from the catalogue.
    pub baselines: &'a [OrderedBaseline],
    /// The field at each time.
    pub field: ArrayView1<'a, FieldId>,
    /// The scan at each time, when requested.
    pub scan: Option<ArrayView1<'a, ScanId>>,
    /// The samples' time auxiliaries.
    pub timing: SampleTiming<'a>,
    /// The partition's native channel of this batch's first channel.
    pub first_channel: usize,
    /// The channels' frequencies: a window of the catalogue's.
    pub frequency: FrequencyView<'a>,
    /// The correlation products along the last axis: the delivered ones, in
    /// the partition's order.
    pub correlations: &'a [Correlation],
    /// The traversal's correlation axis these are: what a packed piece
    /// shares rather than copies.
    pub(crate) axis: &'a CorrelationAxis,
    /// The requested data group.
    pub group: DataGroupView<'a>,
    /// The scientific-group fragments in this batch, in sample order. Only
    /// a delivered batch has them; its compute windows have none.
    pub fragments: &'a [GroupFragment],
}

impl<'a> VisibilityBatch<'a> {
    /// Number of times.
    #[must_use]
    pub fn times(&self) -> usize {
        self.time.len()
    }

    /// Samples per time: the partition's baselines.
    #[must_use]
    pub fn samples_per_time(&self) -> usize {
        self.baselines.len()
    }

    /// Number of samples.
    #[must_use]
    pub fn samples(&self) -> usize {
        self.times() * self.samples_per_time()
    }

    /// Number of channels.
    #[must_use]
    pub fn channel_count(&self) -> usize {
        self.frequency.len()
    }

    /// The partition's native channels this batch holds.
    #[must_use]
    pub fn channels(&self) -> Range<usize> {
        self.first_channel..self.first_channel + self.channel_count()
    }

    /// The shape of every payload array: `[samples, channels, correlations]`.
    #[must_use]
    pub fn shape(&self) -> [usize; 3] {
        [
            self.samples(),
            self.channel_count(),
            self.correlations.len(),
        ]
    }

    /// Sample `sample`'s coordinates.
    ///
    /// # Panics
    ///
    /// If `sample` is out of range.
    #[must_use]
    pub fn sample(&self, sample: usize) -> SampleCoordinate {
        let per_time = self.samples_per_time();
        let (time, baseline) = (sample / per_time, sample % per_time);
        SampleCoordinate {
            time_offset_s: self.time.offsets_s()[time],
            baseline: self.baselines[baseline],
            field: self.field[time],
        }
    }

    /// Sample `sample`'s integration interval in seconds, if requested.
    #[must_use]
    pub fn interval_s(&self, sample: usize) -> Option<f64> {
        self.timing
            .interval_s
            .map(|interval| interval.get(sample, self.samples_per_time()))
    }

    /// Sample `sample`'s effective integration time in seconds, if
    /// requested.
    #[must_use]
    pub fn exposure_s(&self, sample: usize) -> Option<f64> {
        self.timing
            .exposure_s
            .map(|exposure| exposure.get(sample, self.samples_per_time()))
    }

    /// Sample `sample`'s scan, if requested.
    #[must_use]
    pub fn scan(&self, sample: usize) -> Option<ScanId> {
        self.scan.map(|scan| scan[sample / self.samples_per_time()])
    }

    /// The batch's validity rule ([`Validity`]).
    #[must_use]
    pub fn validity(&self) -> Validity<'a> {
        Validity {
            flags: self.group.flags,
            values: self.group.values,
            weights: self.group.weights,
        }
    }

    /// A compute window of channels `range` (relative to this batch's
    /// channels) of every sample: views of the same buffers, nothing copied.
    /// The window carries no fragments: group completion stays with the
    /// delivered batch, so splitting a batch among work units cannot
    /// announce a group's end more than once.
    ///
    /// # Errors
    ///
    /// If `range` is outside the batch's channels.
    pub fn channel_window(&self, range: Range<usize>) -> Result<Self, ObservationError> {
        if range.start > range.end || range.end > self.channel_count() {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "channel window {range:?} outside the batch's {} channels",
                    self.channel_count()
                ),
            });
        }
        Ok(Self {
            first_channel: self.first_channel + range.start,
            frequency: self.frequency.slice(range.clone()),
            group: self.group.window(0..self.samples(), range),
            fragments: &[],
            ..*self
        })
    }

    /// A compute window of times `range` (relative to this batch's times),
    /// every channel. Like a channel window it carries no fragments.
    ///
    /// # Errors
    ///
    /// If `range` is outside the batch's times.
    pub fn time_window(&self, range: Range<usize>) -> Result<Self, ObservationError> {
        if range.start > range.end || range.end > self.times() {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "time window {range:?} outside the batch's {} times",
                    self.times()
                ),
            });
        }
        let per_time = self.samples_per_time();
        let samples = range.start * per_time..range.end * per_time;
        Ok(Self {
            time: self.time.slice(range.clone()),
            field: self.field.slice_move(s![range.clone()]),
            scan: self.scan.map(|scan| scan.slice_move(s![range.clone()])),
            timing: self.timing.window(samples.clone(), range),
            group: self.group.window(samples, 0..self.channel_count()),
            fragments: &[],
            ..*self
        })
    }

    /// A compute window of samples `range`, which must be whole times (as a
    /// [`GroupFragment`]'s samples are): [`Self::time_window`] in sample
    /// units.
    ///
    /// # Errors
    ///
    /// If `range` is outside the batch or splits a time.
    pub fn sample_window(&self, range: Range<usize>) -> Result<Self, ObservationError> {
        let per_time = self.samples_per_time().max(1);
        if !range.start.is_multiple_of(per_time) || !range.end.is_multiple_of(per_time) {
            return Err(ObservationError::InvalidRequest {
                reason: format!("sample window {range:?} splits a time of {per_time} samples"),
            });
        }
        self.time_window(range.start / per_time..range.end / per_time)
    }
}
