// SPDX-License-Identifier: LGPL-3.0-or-later
//! The trial's imaging source: the production major-cycle pass reads
//! [`VisibilityBatch`]es through the runtime's [`BoundedSource`] seam.
//!
//! This module is the reusable consumer: it sees only the shared
//! [`Observation`] interface, binds one [`AccessRequest`] per wave and
//! reads batches. It imports no MSv2 or MSv4 type; the test setup opens the
//! store (`ms_trial_imaging.rs`, `open`).

use std::ops::Range;
use std::time::{Duration, Instant};

use casa_imaging_operator::{PlaneRange, RowContext, RowSpectrum, SpectralAxis};
use casa_imaging_runtime::pass::{
    BlockShape, BoundedSource, DomainProjection, NativeBlock, NativeRowHeader, RowAddress,
    SampleContent, SourceError,
};
use casa_observation::{
    AccessRequest, BatchBuffers, Correlation, CorrelationRequest, Grouping, IoCounters,
    Observation, PartitionId, ReadEvent, Roles, TimeSelection, Traversal, VisibilityBatch,
    WeightRequest,
};
use casa_types::measures::MjdHighPrec;

/// The request one wave of the pass binds: the native channels `channels`
/// of partition `partition`, with or without the visibilities, under
/// `batch_bytes`.
pub fn request(
    partition: PartitionId,
    channels: Range<usize>,
    values: bool,
    batch_bytes: usize,
) -> AccessRequest {
    AccessRequest {
        partitions: vec![partition],
        data_group: "base".to_string(),
        roles: Roles {
            values,
            ..Roles::PAYLOAD
        },
        channels,
        times: TimeSelection::All,
        weights: WeightRequest::Stored,
        correlations: CorrelationRequest::ALL,
        grouping: Grouping::None,
        batch_bytes,
    }
}

/// `request` budgeted for `times` whole times per batch on `observation`,
/// from the estimate alone.
pub fn per_times(
    observation: &dyn Observation,
    request: AccessRequest,
    times: usize,
) -> AccessRequest {
    let probe = AccessRequest {
        batch_bytes: usize::MAX,
        ..request.clone()
    };
    let estimate = observation.estimate(&probe).expect("estimate");
    let partition = &estimate.partitions[0];
    AccessRequest {
        batch_bytes: partition.held_bytes_per_time() * times,
        ..request
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

/// The production pass's view of an observation.
///
/// Each wave binds a new [`AccessRequest`] for the native channels that
/// reach its planes (the pass's `restrict`), so the adapter decodes only
/// those channels' chunks. The pass's [`NativeBlock`] owns its arrays, so
/// every batch is copied into it once: the correlations the operator routes
/// are gathered, and each cell's flag is the batch's
/// [`Validity`](casa_observation::Validity) rule. That copy is the seam's
/// cost today.
pub struct BatchSource<'o> {
    observation: &'o dyn Observation,
    partition: PartitionId,
    /// Native channels the image selects, and their frequencies.
    selected: Range<usize>,
    frequencies: Vec<f64>,
    /// Output plane axis.
    axis: SpectralAxis,
    /// Indices of the routed correlations within the batch's.
    routed: Vec<usize>,
    batch_bytes: usize,
    /// The most samples one batch of any wave holds under the budget, from
    /// the estimate of the narrowest window.
    max_samples: usize,
    traversal: Option<Traversal<'o>>,
    buffers: BatchBuffers,
    /// The validity rule over a batch, evaluated once per batch.
    mask: Vec<bool>,
    pub trace: SourceTrace,
}

impl<'o> BatchSource<'o> {
    /// A source of `observation`'s channels `selected` of its first
    /// partition for an image on `axis`, routing correlations `routed` (in
    /// operator order), at most `batch_bytes` of payload per batch.
    pub fn new(
        observation: &'o dyn Observation,
        selected: Range<usize>,
        axis: SpectralAxis,
        routed: &[Correlation],
        batch_bytes: usize,
    ) -> Self {
        let partition = PartitionId(0);
        let entry = observation.catalogue().partition(partition);
        let routed = routed
            .iter()
            .map(|wanted| {
                entry
                    .correlation_index(*wanted)
                    .expect("routed correlation present")
            })
            .collect();
        // The most samples any wave's batch can hold: under a byte budget
        // the narrowest window (one channel) fits the most times.
        let narrowest = selected.start..selected.start + 1;
        let estimate = observation
            .estimate(&request(partition, narrowest, true, batch_bytes))
            .expect("estimate");
        Self {
            observation,
            partition,
            frequencies: entry.frequencies_hz[selected.clone()].to_vec(),
            selected,
            axis,
            routed,
            batch_bytes,
            max_samples: estimate.partitions[0].samples_per_batch(),
            traversal: None,
            buffers: BatchBuffers::default(),
            mask: Vec::new(),
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
        let mut mask = std::mem::take(&mut self.mask);
        batch.validity().mask_into(&mut mask, batch.shape());
        let delivered = batch.correlations.len();
        let per_time = batch.samples_per_time();
        let mjd_zero = MjdHighPrec::new(0.0, 0.0);
        let weights = batch.group.weights.expect("weights");
        let uvw = batch.group.uvw.expect("uvw");
        let data = values.then(|| batch.group.values.expect("values"));
        let routed = &self.routed;
        let cells = channels * routed.len();
        for sample in 0..batch.samples() {
            let coordinate = batch.sample(sample);
            let header = NativeRowHeader {
                // Row flags do not exist at this boundary: MSv2 FLAG_ROW is
                // folded into the flags, which the validity rule reads.
                row_flag: false,
                context: RowContext {
                    original_uvw_m: None,
                    time_s: batch.time.seconds_since(sample / per_time, mjd_zero),
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
                uvw_m: uvw.metres_at(sample),
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
                                row_weights[cell] = weights[[sample, channel, correlation]];
                                row_flags_out[cell] =
                                    mask[(sample * channels + channel) * delivered + correlation];
                            }
                        }
                        Ok::<_, std::convert::Infallible>(())
                    },
                )
                .unwrap_or_else(|never| match never {});
        }
        self.mask = mask;
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
        if let Some(traversal) = self.traversal.take() {
            self.trace.io.add(traversal.counters());
        }
        let window = self.window(planes, restrict);
        let values = content == SampleContent::Visibilities;
        self.trace.traversals.push((window.clone(), values));
        self.traversal = Some(self.observation.bind_read(&request(
            self.partition,
            window,
            values,
            self.batch_bytes,
        ))?);
        Ok(())
    }

    fn fill(&mut self, block: &mut NativeBlock) -> Result<bool, SourceError> {
        let mut traversal = self.traversal.take().expect("a traversal has begun");
        let values = self.trace.traversals.last().expect("traversal").1;
        let mut buffers = std::mem::take(&mut self.buffers);
        let result = loop {
            let started = Instant::now();
            let event = traversal.next(&mut buffers)?;
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
            self.traversal = Some(traversal);
        } else {
            self.trace.io.add(traversal.counters());
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
