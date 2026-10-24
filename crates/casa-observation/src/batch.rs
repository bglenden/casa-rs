// SPDX-License-Identifier: LGPL-3.0-or-later
//! The visibility batch: a bounded set of samples with their coordinates and
//! the borrowed payload of one data group.

use std::ops::Range;

use casa_types::measures::{EpochView, FrequencyView, UvwView};
use ndarray::{ArrayView1, ArrayView2, ArrayView3, Axis, s};
use num_complex::{Complex32, Complex64};

use crate::catalogue::{AntennaId, Correlation, FieldId, OrderedBaseline, PartitionId};
use crate::grouping::GroupFragment;
use crate::{ObservationError, VisibilityUnits};

/// Whether the store records which time/baseline slots were measured.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Occupancy {
    /// Every slot is a measured acquisition (each MSv2 row is one).
    KnownPresent,
    /// The store does not say: an MSv4 slot may be padding. Flags and
    /// finite values say whether a value is usable, not whether it was
    /// measured.
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
    pub fn window(&self, range: Range<usize>) -> Result<Self, ObservationError> {
        match self {
            Self::Irregular(samples) => Ok(Self::Irregular(IrregularSamples {
                time: samples.time.slice(range.clone()),
                antenna1: samples.antenna1.slice_move(s![range.clone()]),
                antenna2: samples.antenna2.slice_move(s![range.clone()]),
                field: samples.field.slice_move(s![range]),
            })),
            Self::TimeBaseline(samples) => {
                let baselines = samples.baselines.len();
                if baselines == 0
                    || !range.start.is_multiple_of(baselines)
                    || !range.end.is_multiple_of(baselines)
                {
                    return Err(ObservationError::Unsupported {
                        what: "a sample window of a time × baseline batch that is not whole times",
                    });
                }
                let times = range.start / baselines..range.end / baselines;
                Ok(Self::TimeBaseline(TimeBaselineSamples {
                    time: samples.time.slice(times.clone()),
                    field: samples.field.slice_move(s![times]),
                    ..*samples
                }))
            }
        }
    }
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
/// request needs but the store lacks fails binding.
#[derive(Clone, Copy, Debug)]
pub struct DataGroupView<'a> {
    /// The group's name.
    pub name: &'a str,
    /// The visibilities' units.
    pub units: &'a VisibilityUnits,
    /// Visibilities `[sample, channel, correlation]`.
    pub values: Option<VisibilityValues<'a>>,
    /// Flags `[sample, channel, correlation]`; true excludes the value.
    pub flags: Option<ArrayView3<'a, bool>>,
    /// MSv2 `FLAG_ROW` per sample; MSv4 has no row flag.
    pub row_flags: Option<ArrayView1<'a, bool>>,
    /// Weights.
    pub weights: Option<WeightView<'a>>,
    /// UVW per sample.
    pub uvw: Option<UvwView<'a>>,
}

/// A bounded batch of samples of one partition, its coordinates and one
/// data group's payload, borrowed from the caller's [`BatchBuffers`].
///
/// Every view is filled before the batch is delivered: reading an element
/// performs no I/O or decoding. The borrow keeps the buffers from being
/// refilled while any view of them is alive.
///
/// [`BatchBuffers`]: crate::BatchBuffers
#[derive(Clone, Copy, Debug)]
pub struct VisibilityBatch<'a> {
    /// The partition the samples belong to.
    pub partition: PartitionId,
    /// The samples' times, baselines and fields.
    pub topology: SampleTopology<'a>,
    /// The partition's native channel of this batch's first channel.
    pub first_channel: usize,
    /// The channels' frequencies.
    pub frequency: FrequencyView<'a>,
    /// The correlation products along the last axis.
    pub correlations: &'a [Correlation],
    /// The requested data group.
    pub group: DataGroupView<'a>,
    /// The scientific-group fragments in this batch, in sample order.
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

    /// A compute window of channels `range` (relative to this batch's
    /// channels) of every sample: views of the same buffers, nothing copied.
    /// Fragments are kept: a window does not end a group.
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
            ..*self
        })
    }

    /// A compute window of samples `range`, every channel. The fragments
    /// are not carried: the caller took the window from a fragment and
    /// keeps its key.
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
        Ok(Self {
            topology: self.topology.window(range.clone())?,
            group: self.group.window(range, 0..self.channel_count()),
            fragments: &[],
            ..*self
        })
    }
}

impl<'a> DataGroupView<'a> {
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
