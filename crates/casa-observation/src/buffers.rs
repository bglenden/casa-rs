// SPDX-License-Identifier: LGPL-3.0-or-later
//! Caller-owned reusable storage that a cursor fills and a batch borrows.

use std::ops::Range;

use casa_types::measures::direction::DirectionRef;
use casa_types::measures::frequency::FrequencyRef;
use casa_types::measures::{EpochRef, EpochView, FrequencyView, MjdHighPrec, UvwView};
use ndarray::{ArrayView1, ArrayView2, ArrayView3};
use num_complex::Complex32;

use crate::batch::{
    DataGroupView, IrregularSamples, Occupancy, SampleTopology, TimeBaselineSamples,
    VisibilityBatch, VisibilityValues, WeightView,
};
use crate::catalogue::{Correlation, OrderedBaseline, PartitionId};
use crate::grouping::GroupFragment;
use crate::request::Roles;
use crate::{ObservationError, VisibilityUnits};

/// The storage behind one [`VisibilityBatch`].
///
/// Owned by the caller (or a work unit), filled in place by a cursor and
/// lent to the batch. Clearing keeps capacity, so after the first fill of
/// the largest batch a traversal allocates nothing. The buffers are `Send`:
/// a pool of them can be handed between work units whole, which is how a
/// batch's lifetime is separated from a traversal's.
#[derive(Debug, Default)]
pub struct BatchBuffers {
    /// Visibilities, `[sample][channel][correlation]`.
    pub values: Vec<Complex32>,
    /// Flags, the same shape.
    pub flags: Vec<bool>,
    /// `FLAG_ROW` per sample, for stores that have one.
    pub row_flags: Vec<bool>,
    /// Weights, `[sample][correlation]` or `[sample][channel][correlation]`.
    pub weights: Vec<f32>,
    /// UVW, `[sample][3]`.
    pub uvw: Vec<f64>,
    /// Time offsets in seconds: per sample, or per time of a rectangle.
    pub time_offsets_s: Vec<f64>,
    /// First antenna per sample (irregular topology).
    pub antenna1: Vec<u32>,
    /// Second antenna per sample (irregular topology).
    pub antenna2: Vec<u32>,
    /// Field per sample, or per time of a rectangle.
    pub field: Vec<u32>,
    /// Baselines of a rectangle.
    pub baselines: Vec<OrderedBaseline>,
    /// Channel frequencies in hertz.
    pub frequency_hz: Vec<f64>,
    /// Correlation products.
    pub correlations: Vec<Correlation>,
    /// Scientific-group fragments.
    pub fragments: Vec<GroupFragment>,
    /// The data group's name.
    pub group_name: String,
    /// The data group's units.
    pub units: Option<VisibilityUnits>,
}

/// Shape and references of a filled [`BatchBuffers`], from which
/// [`BatchBuffers::view`] builds the batch after checking every extent.
#[derive(Clone, Debug, PartialEq)]
pub struct BatchShape {
    /// The partition.
    pub partition: PartitionId,
    /// Listed or factored samples.
    pub topology: TopologyShape,
    /// The partition's channels delivered.
    pub channels: Range<usize>,
    /// The roles filled.
    pub roles: Roles,
    /// Whether weights are per channel.
    pub spectral_weights: bool,
    /// Time scale of the time offsets.
    pub time_scale: EpochRef,
    /// Origin of the time offsets.
    pub time_origin: MjdHighPrec,
    /// Spectral frame of the frequencies.
    pub frequency_frame: FrequencyRef,
    /// Frame of UVW.
    pub uvw_frame: DirectionRef,
    /// Whether `row_flags` was filled.
    pub row_flags: bool,
}

/// The sample layout of a filled batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TopologyShape {
    /// `samples` listed samples.
    Irregular {
        /// Samples.
        samples: usize,
    },
    /// `times × baselines` samples.
    TimeBaseline {
        /// Times.
        times: usize,
        /// Whether every slot is a measured acquisition.
        occupancy: Occupancy,
    },
}

impl BatchBuffers {
    /// Empty every array, keeping its capacity.
    pub fn clear(&mut self) {
        self.values.clear();
        self.flags.clear();
        self.row_flags.clear();
        self.weights.clear();
        self.uvw.clear();
        self.time_offsets_s.clear();
        self.antenna1.clear();
        self.antenna2.clear();
        self.field.clear();
        self.baselines.clear();
        self.frequency_hz.clear();
        self.correlations.clear();
        self.fragments.clear();
    }

    /// Bytes the arrays hold now, from their capacities.
    #[must_use]
    pub fn capacity_bytes(&self) -> usize {
        self.values.capacity() * size_of::<Complex32>()
            + (self.flags.capacity() + self.row_flags.capacity()) * size_of::<bool>()
            + self.weights.capacity() * size_of::<f32>()
            + (self.uvw.capacity() + self.time_offsets_s.capacity() + self.frequency_hz.capacity())
                * size_of::<f64>()
            + (self.antenna1.capacity() + self.antenna2.capacity() + self.field.capacity())
                * size_of::<u32>()
            + self.baselines.capacity() * size_of::<OrderedBaseline>()
            + self.correlations.capacity() * size_of::<Correlation>()
            + self.fragments.capacity() * size_of::<GroupFragment>()
    }

    /// Lend the filled arrays as a batch of `shape`, checking that every
    /// filled array has the length the shape implies.
    ///
    /// # Errors
    ///
    /// [`ObservationError::Shape`] when an array's length disagrees with
    /// `shape`: a cursor bug, never a property of the data.
    pub fn view(&self, shape: &BatchShape) -> Result<VisibilityBatch<'_>, ObservationError> {
        let channels = shape.channels.len();
        let correlations = self.correlations.len();
        let samples = match shape.topology {
            TopologyShape::Irregular { samples } => samples,
            TopologyShape::TimeBaseline { times, .. } => times * self.baselines.len(),
        };
        let check = |name: &'static str, actual: usize, expected: usize| {
            if actual == expected {
                Ok(())
            } else {
                Err(ObservationError::Shape {
                    array: name,
                    actual,
                    expected,
                })
            }
        };
        check("frequencies", self.frequency_hz.len(), channels)?;
        let topology = match shape.topology {
            TopologyShape::Irregular { samples } => {
                check("time", self.time_offsets_s.len(), samples)?;
                check("antenna1", self.antenna1.len(), samples)?;
                check("antenna2", self.antenna2.len(), samples)?;
                check("field", self.field.len(), samples)?;
                SampleTopology::Irregular(IrregularSamples {
                    time: self.epochs(shape),
                    antenna1: ArrayView1::from(&self.antenna1[..]),
                    antenna2: ArrayView1::from(&self.antenna2[..]),
                    field: ArrayView1::from(&self.field[..]),
                })
            }
            TopologyShape::TimeBaseline { times, occupancy } => {
                check("time", self.time_offsets_s.len(), times)?;
                check("field", self.field.len(), times)?;
                SampleTopology::TimeBaseline(TimeBaselineSamples {
                    time: self.epochs(shape),
                    baselines: &self.baselines,
                    field: ArrayView1::from(&self.field[..]),
                    occupancy,
                })
            }
        };
        let cube = [samples, channels, correlations];
        let roles = shape.roles;
        let values = if roles.values {
            check(
                "values",
                self.values.len(),
                samples * channels * correlations,
            )?;
            Some(VisibilityValues::Complex32(
                ArrayView3::from_shape(cube, &self.values[..]).expect("checked length"),
            ))
        } else {
            None
        };
        let flags = if roles.flags {
            check("flags", self.flags.len(), samples * channels * correlations)?;
            Some(ArrayView3::from_shape(cube, &self.flags[..]).expect("checked length"))
        } else {
            None
        };
        let row_flags = if shape.row_flags {
            check("row flags", self.row_flags.len(), samples)?;
            Some(ArrayView1::from(&self.row_flags[..]))
        } else {
            None
        };
        let weights = if !roles.weights {
            None
        } else if shape.spectral_weights {
            check(
                "weights",
                self.weights.len(),
                samples * channels * correlations,
            )?;
            Some(WeightView::Spectral(
                ArrayView3::from_shape(cube, &self.weights[..]).expect("checked length"),
            ))
        } else {
            check("weights", self.weights.len(), samples * correlations)?;
            Some(WeightView::PerCorrelation(
                ArrayView2::from_shape([samples, correlations], &self.weights[..])
                    .expect("checked length"),
            ))
        };
        let uvw = if roles.uvw {
            check("uvw", self.uvw.len(), samples * 3)?;
            Some(UvwView::new(
                shape.uvw_frame,
                ArrayView2::from_shape([samples, 3], &self.uvw[..]).expect("checked length"),
            ))
        } else {
            None
        };
        Ok(VisibilityBatch {
            partition: shape.partition,
            topology,
            first_channel: shape.channels.start,
            frequency: FrequencyView::new(
                shape.frequency_frame,
                ArrayView1::from(&self.frequency_hz[..]),
            ),
            correlations: &self.correlations,
            group: DataGroupView {
                name: &self.group_name,
                units: self.units.as_ref().unwrap_or(&VisibilityUnits::Undeclared),
                values,
                flags,
                row_flags,
                weights,
                uvw,
            },
            fragments: &self.fragments,
        })
    }

    fn epochs(&self, shape: &BatchShape) -> EpochView<'_> {
        EpochView::new(
            shape.time_scale,
            shape.time_origin,
            ArrayView1::from(&self.time_offsets_s[..]),
        )
    }
}
