// SPDX-License-Identifier: LGPL-3.0-or-later
//! The trial's imaging source: the production major-cycle pass reads
//! [`VisibilityBatch`]es from either adapter through the runtime's
//! [`BoundedSource`] seam.

use std::ops::Range;
use std::time::{Duration, Instant};

use casa_imaging_operator::{PlaneRange, RowContext, RowSpectrum, SpectralAxis};
use casa_imaging_runtime::pass::{
    BlockShape, BoundedSource, DomainProjection, NativeBlock, NativeRowHeader, RowAddress,
    SampleContent, SourceError,
};
use casa_ms::observation_io::{Msv2Observation, Msv4Observation};
use casa_observation::{
    AccessRequest, BatchBuffers, Closure, Correlation, Fill, Grouping, IoCounters, Layout,
    ObservationError, PartitionId, Precision, ReadEvent, Realization, Roles, VisibilityBatch,
    VisibilityCursor,
};

/// One of the two opened fixtures.
pub enum Observation {
    V2(Box<Msv2Observation>),
    V4(Msv4Observation),
}

/// A bound traversal of either: static dispatch per batch.
pub enum Cursor<'o> {
    V2(casa_ms::observation_io::Msv2Cursor<'o>),
    V4(casa_ms::observation_io::Msv4Cursor<'o>),
}

impl VisibilityCursor for Cursor<'_> {
    fn realization(&self) -> &Realization {
        match self {
            Self::V2(cursor) => cursor.realization(),
            Self::V4(cursor) => cursor.realization(),
        }
    }

    fn fill(&mut self, buffers: &mut BatchBuffers) -> Result<Option<Fill>, ObservationError> {
        match self {
            Self::V2(cursor) => cursor.fill(buffers),
            Self::V4(cursor) => cursor.fill(buffers),
        }
    }

    fn counters(&self) -> IoCounters {
        match self {
            Self::V2(cursor) => cursor.counters(),
            Self::V4(cursor) => cursor.counters(),
        }
    }
}

impl Observation {
    pub fn catalogue(&self) -> &casa_observation::Catalogue {
        match self {
            Self::V2(observation) => observation.catalogue(),
            Self::V4(observation) => observation.catalogue(),
        }
    }

    pub fn bind(&self, request: &AccessRequest) -> Result<Cursor<'_>, ObservationError> {
        Ok(match self {
            Self::V2(observation) => Cursor::V2(observation.bind_read(request)?),
            Self::V4(observation) => Cursor::V4(observation.bind_read(request)?),
        })
    }
}

/// The request one wave of the pass binds: the native channels `channels`,
/// with or without the visibilities.
pub fn request(channels: Range<usize>, values: bool, max_samples: usize) -> AccessRequest {
    AccessRequest {
        partition: PartitionId(0),
        data_group: "base".to_string(),
        roles: Roles {
            values,
            ..Roles::ALL
        },
        channels,
        layout: Layout::SampleMajor,
        precision: Precision::Complex32 {
            allow_narrowing: false,
        },
        grouping: Grouping::None,
        closure: Closure::AtDelivery,
        max_samples,
    }
}

/// What the source did over a pass, for the evidence.
#[derive(Clone, Debug, Default)]
pub struct SourceTrace {
    /// Each traversal's native channel window and visibility read.
    pub traversals: Vec<(Range<usize>, bool)>,
    /// Adapter counters, summed over traversals.
    pub io: IoCounters,
    /// Time inside the adapter's `next`.
    pub read: Duration,
    /// Time copying batches into the pass's native blocks.
    pub pack: Duration,
    /// Elements copied into native blocks: values, flags and weights
    /// (each per-correlation weight once per channel).
    pub packed_elements: usize,
    /// Largest buffers capacity seen.
    pub buffer_bytes: usize,
}

/// The production pass's view of a trial observation.
///
/// Each wave binds a new [`AccessRequest`] for the native channels that
/// reach its planes (the pass's `restrict`), so the adapter decodes only
/// those channels' chunks. The pass's [`NativeBlock`] owns its arrays, so
/// every batch is copied into it once: the correlations the operator routes
/// (here XX and YY of four) are gathered and a per-correlation weight is
/// written for each channel. That copy is the seam's cost today.
pub struct BatchSource<'o> {
    observation: &'o Observation,
    /// Native channels the image selects, and their frequencies.
    selected: Range<usize>,
    frequencies: Vec<f64>,
    /// Output plane axis.
    axis: SpectralAxis,
    /// Indices of the routed correlations within the batch's.
    routed: Vec<usize>,
    max_samples: usize,
    cursor: Option<Cursor<'o>>,
    buffers: BatchBuffers,
    pub trace: SourceTrace,
}

impl<'o> BatchSource<'o> {
    /// A source of `observation`'s channels `selected` for an image on
    /// `axis`, routing correlations `routed` (in operator order), at most
    /// `max_samples` samples per batch.
    pub fn new(
        observation: &'o Observation,
        selected: Range<usize>,
        axis: SpectralAxis,
        routed: &[Correlation],
        max_samples: usize,
    ) -> Self {
        let partition = &observation.catalogue().partitions[0];
        let routed = routed
            .iter()
            .map(|wanted| {
                partition
                    .correlations
                    .iter()
                    .position(|have| have == wanted)
                    .expect("routed correlation present")
            })
            .collect();
        Self {
            observation,
            frequencies: partition.frequencies_hz[selected.clone()].to_vec(),
            selected,
            axis,
            routed,
            max_samples,
            cursor: None,
            buffers: BatchBuffers::default(),
            trace: SourceTrace::default(),
        }
    }

    /// The selected native channels whose samples can reach `planes`: those
    /// within one native channel of the planes' edges, which keeps both
    /// linear-interpolation partners.
    fn window(&self, planes: PlaneRange, restrict: bool) -> Range<usize> {
        if !restrict {
            return self.selected.clone();
        }
        let half = self.axis.increment_hz().abs() / 2.0;
        let a = self.axis.centre_hz(planes.start);
        let b = self.axis.centre_hz(planes.end - 1);
        let (low, high) = (a.min(b) - half, a.max(b) + half);
        let spacing = (self.frequencies[1] - self.frequencies[0]).abs();
        let reach: Vec<usize> = self
            .frequencies
            .iter()
            .enumerate()
            .filter(|(_, f)| **f >= low - spacing && **f <= high + spacing)
            .map(|(index, _)| self.selected.start + index)
            .collect();
        reach[0]..reach[reach.len() - 1] + 1
    }

    fn pack(
        &mut self,
        block: &mut NativeBlock,
        batch: &VisibilityBatch<'_>,
        values: bool,
    ) -> usize {
        let channels = batch.channel_count();
        let first = batch.first_channel - self.selected.start;
        let channel_indices: Vec<u32> = (first..first + channels).map(|c| c as u32).collect();
        let correlation_indices: Vec<u32> = self.routed.iter().map(|&c| c as u32).collect();
        block.reset(1, &channel_indices, &correlation_indices);
        let flags = batch.group.flags.expect("flags");
        let weights = batch.group.weights.expect("weights");
        let uvw = batch.group.uvw.expect("uvw");
        let row_flags = batch.group.row_flags;
        let data = values.then(|| {
            batch
                .group
                .values
                .expect("values")
                .as_complex32()
                .expect("c64")
        });
        let time = batch.topology.time();
        let routed = &self.routed;
        let cells = channels * routed.len();
        for sample in 0..batch.samples() {
            let coordinate = batch.topology.sample(sample);
            let header = NativeRowHeader {
                row_flag: row_flags.is_some_and(|flags| flags[sample]),
                context: RowContext {
                    original_uvw_m: None,
                    time_s: time.origin().day() * 86_400.0 + coordinate.time_offset_s,
                    antennas: [
                        coordinate.baseline.antenna1.0,
                        coordinate.baseline.antenna2.0,
                    ],
                    antenna_types: [0, 0],
                    parallactic_angle_rad: [0.0; 2],
                    field: coordinate.field.0,
                    spectral_window: 0,
                },
                address: RowAddress {
                    physical_row: sample as u64,
                    data_description: 0,
                },
            };
            // The image is centred on the field's phase centre, so the
            // projection is the stored UVW with no shift.
            let projection = [DomainProjection {
                uvw_m: uvw.get(sample),
                phase_shift_m: 0.0,
                pointing_offset_rad: [0.0; 2],
            }];
            block
                .push_row_with(
                    header,
                    &projection,
                    batch
                        .frequency
                        .hz()
                        .as_slice()
                        .expect("contiguous frequencies"),
                    |row_values, row_weights, row_flags_out| {
                        for channel in 0..channels {
                            for (k, &correlation) in routed.iter().enumerate() {
                                let cell = channel * routed.len() + k;
                                if let Some(data) = data {
                                    row_values[cell] = data[[sample, channel, correlation]];
                                }
                                row_weights[cell] = weights.get(sample, channel, correlation);
                                row_flags_out[cell] = flags[[sample, channel, correlation]];
                            }
                        }
                        Ok::<_, std::convert::Infallible>(())
                    },
                )
                .unwrap_or_else(|never| match never {});
        }
        batch.samples() * cells * (usize::from(values) + 2)
    }
}

impl BoundedSource for BatchSource<'_> {
    fn begin(
        &mut self,
        planes: PlaneRange,
        restrict: bool,
        content: SampleContent,
    ) -> Result<(), SourceError> {
        if let Some(cursor) = self.cursor.take() {
            add(&mut self.trace.io, cursor.counters());
        }
        let window = self.window(planes, restrict);
        let values = content == SampleContent::Visibilities;
        self.trace.traversals.push((window.clone(), values));
        self.cursor = Some(
            self.observation
                .bind(&request(window, values, self.max_samples))?,
        );
        Ok(())
    }

    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError> {
        let mut cursor = self.cursor.take().expect("a traversal has begun");
        let values = self.trace.traversals.last().expect("traversal").1;
        let mut buffers = std::mem::take(&mut self.buffers);
        let result = loop {
            let started = Instant::now();
            let event = cursor.next(&mut buffers)?;
            self.trace.read += started.elapsed();
            match event {
                None => break false,
                Some(ReadEvent::GroupEnd(_)) => continue,
                Some(ReadEvent::Batch(batch)) => {
                    let started = Instant::now();
                    self.trace.packed_elements += self.pack(block, &batch, values);
                    self.trace.pack += started.elapsed();
                    break true;
                }
            }
        };
        self.trace.buffer_bytes = self.trace.buffer_bytes.max(buffers.capacity_bytes());
        self.buffers = buffers;
        if result {
            self.cursor = Some(cursor);
        } else {
            add(&mut self.trace.io, cursor.counters());
        }
        Ok(result)
    }

    fn maximum_block(&self) -> BlockShape {
        let channels = self.frequencies.len();
        BlockShape {
            rows: self.max_samples,
            spectrum: RowSpectrum {
                channels,
                first_spacing_hz: (self.frequencies[1] - self.frequencies[0]).abs(),
                span_hz: (self.frequencies[channels - 1] - self.frequencies[0]).abs(),
            },
            correlations: self.routed.len(),
            domains: 1,
        }
    }
}

fn add(total: &mut IoCounters, more: IoCounters) {
    total.batches += more.batches;
    total.read_calls += more.read_calls;
    total.decode_units += more.decode_units;
    total.delivered_bytes += more.delivered_bytes;
    total.copied_elements += more.copied_elements;
    total.buffer_growths += more.buffer_growths;
}
