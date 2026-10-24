// SPDX-License-Identifier: LGPL-3.0-or-later
//! The visibility batch: a bounded set of samples with their coordinates and
//! the borrowed payload of one data group.

use std::ops::Range;

use casa_types::measures::{EpochView, FrequencyView, UvwView};
use ndarray::{ArrayView1, ArrayView2, ArrayView3, Axis, s};
use num_complex::{Complex32, Complex64};

use crate::catalogue::{AntennaId, Correlation, FieldId, OrderedBaseline, PartitionId, ScanId};
use crate::grouping::GroupFragment;
use crate::{ObservationError, VisibilityUnits};

/// Whether the store records which time/baseline slots were measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Occupancy {
    /// Every slot is a measured acquisition (each MSv2 row is one).
    KnownPresent,
    /// The store does not say: an MSv4 slot may be padding. The batch's
    /// [`Validity`] excludes what is flagged or not finite; it does not say
    /// whether a slot was measured.
    Unknown,
}

/// Samples listed one by one, as MSv2 rows are: each has its own time,
/// baseline and field. Repeated coordinates are distinct acquisitions.
#[derive(Clone, Copy, Debug)]
pub struct IrregularSamples<'a> {
    /// Time of each sample (MSv2 `TIME`, the integration midpoint).
    pub time: EpochView<'a>,
    /// First antenna of each sample.
    pub antenna1: ArrayView1<'a, u32>,
    /// Second antenna of each sample.
    pub antenna2: ArrayView1<'a, u32>,
    /// Field of each sample.
    pub field: ArrayView1<'a, u32>,
}

/// Samples on a time × baseline rectangle, as MSv4 holds them: sample
/// `t * baselines.len() + b` is time `t`, baseline `b`. The coordinates cost
/// `T + B` entries, not `T × B`.
#[derive(Clone, Copy, Debug)]
pub struct TimeBaselineSamples<'a> {
    /// Times (MSv4 `time`).
    pub time: EpochView<'a>,
    /// Baselines (MSv4 `baseline_id`, resolved to antennas).
    pub baselines: &'a [OrderedBaseline],
    /// Field at each time (MSv4 `field_name`, a coordinate along `time`).
    pub field: ArrayView1<'a, u32>,
    /// Whether every slot is a measured acquisition.
    pub occupancy: Occupancy,
}

/// How a batch's samples are laid out in time and baseline.
#[derive(Clone, Copy, Debug)]
pub enum SampleTopology<'a> {
    /// One coordinate entry per sample.
    Irregular(IrregularSamples<'a>),
    /// Factored time and baseline axes.
    TimeBaseline(TimeBaselineSamples<'a>),
}

/// One sample's coordinates, assembled on the stack.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SampleCoordinate {
    /// Time offset in seconds from the topology's [`EpochView::origin`].
    pub time_offset_s: f64,
    /// The baseline.
    pub baseline: OrderedBaseline,
    /// The field.
    pub field: FieldId,
}

impl<'a> SampleTopology<'a> {
    /// Number of samples.
    #[must_use]
    pub fn samples(&self) -> usize {
        match self {
            Self::Irregular(samples) => samples.time.len(),
            Self::TimeBaseline(samples) => samples.time.len() * samples.baselines.len(),
        }
    }

    /// Samples that share one time entry: one for listed samples, the
    /// baselines of a rectangle.
    #[must_use]
    pub fn samples_per_time(&self) -> usize {
        match self {
            Self::Irregular(_) => 1,
            Self::TimeBaseline(samples) => samples.baselines.len(),
        }
    }

    /// The time coordinate: one entry per sample, or per time of the
    /// rectangle.
    #[must_use]
    pub fn time(&self) -> EpochView<'a> {
        match self {
            Self::Irregular(samples) => samples.time,
            Self::TimeBaseline(samples) => samples.time,
        }
    }

    /// Sample `index`'s coordinates.
    ///
    /// # Panics
    ///
    /// If `index` is out of range.
    #[must_use]
    pub fn sample(&self, index: usize) -> SampleCoordinate {
        match self {
            Self::Irregular(samples) => SampleCoordinate {
                time_offset_s: samples.time.offsets_s()[index],
                baseline: OrderedBaseline {
                    antenna1: AntennaId(samples.antenna1[index]),
                    antenna2: AntennaId(samples.antenna2[index]),
                },
                field: FieldId(samples.field[index]),
            },
            Self::TimeBaseline(samples) => {
                let baselines = samples.baselines.len();
                let (time, baseline) = (index / baselines, index % baselines);
                SampleCoordinate {
                    time_offset_s: samples.time.offsets_s()[time],
                    baseline: samples.baselines[baseline],
                    field: FieldId(samples.field[time]),
                }
            }
        }
    }

    /// Whether the store records every slot as measured.
    #[must_use]
    pub fn occupancy(&self) -> Occupancy {
        match self {
            Self::Irregular(_) => Occupancy::KnownPresent,
            Self::TimeBaseline(samples) => samples.occupancy,
        }
    }

    /// The samples `range`. A rectangle keeps its factored form only for a
    /// range of whole times; any other range is an error rather than a
    /// silent expansion into per-sample coordinates.
    ///
    /// # Errors
    ///
    /// For a range of a rectangle that is not whole times.
    pub fn window(&self, range: Range<usize>) -> Result<Self, ObservationError> {
        match self {
            Self::Irregular(samples) => Ok(Self::Irregular(IrregularSamples {
                time: samples.time.slice(range.clone()),
                antenna1: samples.antenna1.slice_move(s![range.clone()]),
                antenna2: samples.antenna2.slice_move(s![range.clone()]),
                field: samples.field.slice_move(s![range]),
            })),
            Self::TimeBaseline(samples) => {
                let times = self.time_range(&range)?;
                Ok(Self::TimeBaseline(TimeBaselineSamples {
                    time: samples.time.slice(times.clone()),
                    field: samples.field.slice_move(s![times]),
                    ..*samples
                }))
            }
        }
    }

    /// The time entries of samples `range`, which must be whole times.
    fn time_range(&self, range: &Range<usize>) -> Result<Range<usize>, ObservationError> {
        let per_time = self.samples_per_time();
        if per_time == 0
            || !range.start.is_multiple_of(per_time)
            || !range.end.is_multiple_of(per_time)
        {
            return Err(ObservationError::Unsupported {
                what: "a sample window of a time × baseline batch that is not whole times",
            });
        }
        Ok(range.start / per_time..range.end / per_time)
    }
}

/// A per-sample quantity in the form the store shares it: one value per
/// sample, one per time of a factored batch, or one for the whole batch.
/// [`Self::get`] resolves any of them for a sample, so a consumer needs no
/// rule of either format.
#[derive(Clone, Copy, Debug)]
pub enum Shared<'a, T> {
    /// One value per sample (MSv2 rows; MSv4 `[time, baseline]` variables).
    PerSample(ArrayView1<'a, T>),
    /// One value per time, shared by its baselines (MSv4 `scan_name`).
    PerTime(ArrayView1<'a, T>),
    /// One value for every sample (MSv4 `time.integration_time`).
    Constant(T),
}

impl<'a, T: Copy> Shared<'a, T> {
    /// The value of `sample`, in a batch with `samples_per_time` samples
    /// per time entry ([`SampleTopology::samples_per_time`]).
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
/// it. Times are in the topology's time scale and origin.
#[derive(Clone, Copy, Debug, Default)]
pub struct SampleTiming<'a> {
    /// Time centroid of each sample: one entry per sample, same scale and
    /// origin as [`SampleTopology::time`].
    pub centroid: Option<EpochView<'a>>,
    /// Integration interval in seconds.
    pub interval_s: Option<Shared<'a, f64>>,
    /// Effective integration time in seconds.
    pub exposure_s: Option<Shared<'a, f64>>,
}

/// Visibilities `[sample, channel, correlation]` in their delivered
/// precision. Match once per batch, then run a typed loop.
#[derive(Clone, Copy, Debug)]
pub enum VisibilityValues<'a> {
    /// `Complex<f32>`, MSv2 `DATA` and MSv4 `complex64`.
    Complex32(ArrayView3<'a, Complex32>),
    /// `Complex<f64>`.
    Complex64(ArrayView3<'a, Complex64>),
}

impl<'a> VisibilityValues<'a> {
    /// The single-precision values, if that is what the batch holds.
    #[must_use]
    pub fn as_complex32(&self) -> Option<ArrayView3<'a, Complex32>> {
        match self {
            Self::Complex32(values) => Some(*values),
            Self::Complex64(_) => None,
        }
    }

    fn slice(&self, samples: Range<usize>, channels: Range<usize>) -> Self {
        match self {
            Self::Complex32(values) => {
                Self::Complex32(values.slice_move(s![samples, channels, ..]))
            }
            Self::Complex64(values) => {
                Self::Complex64(values.slice_move(s![samples, channels, ..]))
            }
        }
    }

    fn is_finite(&self, sample: usize, channel: usize, correlation: usize) -> bool {
        match self {
            Self::Complex32(values) => values[[sample, channel, correlation]].is_finite(),
            Self::Complex64(values) => values[[sample, channel, correlation]].is_finite(),
        }
    }
}

/// Weights in their stored shape: one per sample and correlation, shared
/// by every channel, or one per sample, channel and correlation. A
/// per-correlation weight is never expanded across channels to make the
/// view.
#[derive(Clone, Copy, Debug)]
pub enum WeightView<'a> {
    /// `[sample, correlation]`, every channel's weight (MSv2 `WEIGHT`).
    PerCorrelation(ArrayView2<'a, f32>),
    /// `[sample, channel, correlation]` (MSv2 `WEIGHT_SPECTRUM`, MSv4
    /// `WEIGHT`).
    Spectral(ArrayView3<'a, f32>),
}

impl WeightView<'_> {
    /// The weight of `(sample, channel, correlation)`.
    ///
    /// # Panics
    ///
    /// If an index is out of range.
    #[must_use]
    pub fn get(&self, sample: usize, channel: usize, correlation: usize) -> f32 {
        match self {
            Self::PerCorrelation(weights) => weights[[sample, correlation]],
            Self::Spectral(weights) => weights[[sample, channel, correlation]],
        }
    }

    fn slice(&self, samples: Range<usize>, channels: Range<usize>) -> Self {
        match self {
            Self::PerCorrelation(weights) => {
                Self::PerCorrelation(weights.slice_move(s![samples, ..]))
            }
            Self::Spectral(weights) => {
                Self::Spectral(weights.slice_move(s![samples, channels, ..]))
            }
        }
    }
}

/// The borrowed payload of one data group.
///
/// A role is `None` only when the request did not ask for it; a role the
/// request needs but the store lacks fails binding. `flags` and
/// `row_flags` are the stored flag state; [`Self::validity`] is the one
/// rule that decides whether a value is used.
#[derive(Clone, Copy, Debug)]
pub struct DataGroupView<'a> {
    /// The group's name.
    pub name: &'a str,
    /// The visibilities' units.
    pub units: &'a VisibilityUnits,
    /// Visibilities `[sample, channel, correlation]`.
    pub values: Option<VisibilityValues<'a>>,
    /// Stored flags `[sample, channel, correlation]`; true excludes the
    /// value.
    pub flags: Option<ArrayView3<'a, bool>>,
    /// Stored MSv2 `FLAG_ROW` per sample; MSv4 has no row flag.
    pub row_flags: Option<ArrayView1<'a, bool>>,
    /// Weights.
    pub weights: Option<WeightView<'a>>,
    /// UVW per sample, in the data group's effective frame
    /// ([`UvwFrame`](crate::UvwFrame)).
    pub uvw: Option<UvwView<'a>>,
}

/// Whether each value of a batch is used: the contract's single validity
/// rule, the same for every store.
///
/// A value `(sample, channel, correlation)` is excluded when its stored
/// flag is set, its sample's row flag is set, or, among the roles read, its
/// visibility or weight is not finite. That covers MSv2 `FLAG_ROW`, MSv4
/// padding (XRADIO flags absent slots and fills them with NaN) and corrupt
/// values alike, so a consumer applies no format-specific rule. The stored
/// flags stay available unchanged in [`DataGroupView`].
#[derive(Clone, Copy, Debug)]
pub struct Validity<'a> {
    group: DataGroupView<'a>,
}

impl Validity<'_> {
    /// Whether `(sample, channel, correlation)` is excluded.
    ///
    /// # Panics
    ///
    /// If an index is out of range.
    #[must_use]
    pub fn excluded(&self, sample: usize, channel: usize, correlation: usize) -> bool {
        let group = &self.group;
        group
            .flags
            .is_some_and(|flags| flags[[sample, channel, correlation]])
            || group.row_flags.is_some_and(|flags| flags[sample])
            || group
                .values
                .is_some_and(|values| !values.is_finite(sample, channel, correlation))
            || group
                .weights
                .is_some_and(|weights| !weights.get(sample, channel, correlation).is_finite())
    }
}

/// A bounded batch of samples of one partition, its coordinates and one
/// data group's payload, borrowed from the caller's [`BatchBuffers`].
///
/// Every view is filled and checked before the batch is delivered: reading
/// an element performs no I/O or decoding. The borrow keeps the buffers
/// from being refilled while any view of them is alive.
///
/// [`BatchBuffers`]: crate::BatchBuffers
#[derive(Clone, Copy, Debug)]
pub struct VisibilityBatch<'a> {
    /// The partition the samples belong to.
    pub partition: PartitionId,
    /// The samples' times, baselines and fields.
    pub topology: SampleTopology<'a>,
    /// The samples' time auxiliaries.
    pub timing: SampleTiming<'a>,
    /// Each sample's scan, when requested.
    pub scan: Option<Shared<'a, u32>>,
    /// The partition's native channel of this batch's first channel.
    pub first_channel: usize,
    /// The channels' frequencies.
    pub frequency: FrequencyView<'a>,
    /// The correlation products along the last axis.
    pub correlations: &'a [Correlation],
    /// The requested data group.
    pub group: DataGroupView<'a>,
    /// The scientific-group fragments in this batch, in sample order. Only
    /// a delivered batch has them; its compute windows have none.
    pub fragments: &'a [GroupFragment],
}

impl<'a> VisibilityBatch<'a> {
    /// Number of samples.
    #[must_use]
    pub fn samples(&self) -> usize {
        self.topology.samples()
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

    /// Sample `sample`'s integration interval in seconds, if requested.
    #[must_use]
    pub fn interval_s(&self, sample: usize) -> Option<f64> {
        let per_time = self.topology.samples_per_time();
        self.timing
            .interval_s
            .map(|interval| interval.get(sample, per_time))
    }

    /// Sample `sample`'s effective integration time in seconds, if
    /// requested.
    #[must_use]
    pub fn exposure_s(&self, sample: usize) -> Option<f64> {
        let per_time = self.topology.samples_per_time();
        self.timing
            .exposure_s
            .map(|exposure| exposure.get(sample, per_time))
    }

    /// Sample `sample`'s scan, if requested.
    #[must_use]
    pub fn scan(&self, sample: usize) -> Option<ScanId> {
        let per_time = self.topology.samples_per_time();
        self.scan.map(|scan| ScanId(scan.get(sample, per_time)))
    }

    /// The batch's validity rule ([`Validity`]).
    #[must_use]
    pub fn validity(&self) -> Validity<'a> {
        Validity { group: self.group }
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
        let samples = 0..self.samples();
        Ok(Self {
            first_channel: self.first_channel + range.start,
            frequency: self.frequency.slice(range.clone()),
            group: self.group.window(samples, range),
            fragments: &[],
            ..*self
        })
    }

    /// A compute window of samples `range`, every channel. Like a channel
    /// window it carries no fragments: the caller took it from a fragment
    /// and keeps that fragment's key.
    ///
    /// # Errors
    ///
    /// If `range` is outside the batch, or splits the times of a factored
    /// batch ([`SampleTopology::window`]).
    pub fn sample_window(&self, range: Range<usize>) -> Result<Self, ObservationError> {
        if range.start > range.end || range.end > self.samples() {
            return Err(ObservationError::InvalidRequest {
                reason: format!(
                    "sample window {range:?} outside the batch's {} samples",
                    self.samples()
                ),
            });
        }
        let times = match self.topology {
            SampleTopology::Irregular(_) => range.clone(),
            SampleTopology::TimeBaseline(_) => self.topology.time_range(&range)?,
        };
        Ok(Self {
            topology: self.topology.window(range.clone())?,
            timing: SampleTiming {
                centroid: self
                    .timing
                    .centroid
                    .map(|centroid| centroid.slice(range.clone())),
                interval_s: self
                    .timing
                    .interval_s
                    .map(|interval| interval.window(range.clone(), times.clone())),
                exposure_s: self
                    .timing
                    .exposure_s
                    .map(|exposure| exposure.window(range.clone(), times.clone())),
            },
            scan: self
                .scan
                .map(|scan| scan.window(range.clone(), times.clone())),
            group: self.group.window(range, 0..self.channel_count()),
            fragments: &[],
            ..*self
        })
    }
}

impl DataGroupView<'_> {
    fn window(&self, samples: Range<usize>, channels: Range<usize>) -> Self {
        Self {
            values: self
                .values
                .map(|values| values.slice(samples.clone(), channels.clone())),
            flags: self
                .flags
                .map(|flags| flags.slice_move(s![samples.clone(), channels.clone(), ..])),
            row_flags: self
                .row_flags
                .map(|flags| flags.slice_move(s![samples.clone()])),
            weights: self
                .weights
                .map(|weights| weights.slice(samples.clone(), channels)),
            uvw: self.uvw.map(|uvw| uvw.slice(samples)),
            ..*self
        }
    }

    /// Samples in the group's arrays, from whichever role was delivered.
    #[must_use]
    pub fn samples(&self) -> Option<usize> {
        self.flags
            .map(|flags| flags.len_of(Axis(0)))
            .or_else(|| self.uvw.map(|uvw| uvw.len()))
    }
}
