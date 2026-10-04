// SPDX-License-Identifier: LGPL-3.0-or-later

//! Resident compiled scalar A*WA on the shared admitted Metal runtime.

use super::*;
use crate::metal_runtime::{
    MetalBatchAccess, MetalBufferRegion, MetalReplayBuffer, NORMAL_BATCHES_PER_COMMAND,
    NormalDispatch,
};
use casa_imaging_reconstruction::runtime_adapter::{
    DeviceNormalApply, DeviceNormalGroup, DeviceNormalPosition, DeviceNormalPreparedBatch,
    DeviceNormalRecord, GRIDDED_NORMAL_OPERATOR_RECORD_BYTES,
};

#[derive(Clone, Debug)]
pub(crate) struct MetalNormalPlan {
    pub allocation: AllocationId,
    pub bytes: usize,
    shape: [usize; 2],
    capacity: usize,
    weights: usize,
    model: usize,
    normal: usize,
    staging: usize,
    stride: usize,
}

fn aligned(bytes: usize) -> io::Result<usize> {
    bytes
        .checked_add(63)
        .map(|n| n & !63)
        .ok_or_else(|| io::Error::other("device normal arena overflow"))
}

impl MetalNormalPlan {
    fn replay_layout(
        &self,
        frame_records: impl Iterator<Item = usize>,
    ) -> io::Result<ReplayLayout> {
        let mut batches = Vec::new();
        let mut count = 0;
        let mut frames = 0;
        let mut offset = aligned(
            casa_imaging_reconstruction::runtime_adapter::BandPlan::spatial_weight_bytes(),
        )?;
        for records in frame_records {
            if records == 0 || records > self.capacity {
                return Err(io::Error::other("invalid Metal replay frame capacity"));
            }
            if count + records > self.capacity {
                batches.push(ReplayBatchLayout {
                    offset,
                    records: count,
                    frames,
                });
                offset = offset
                    .checked_add(aligned(count * 48)?)
                    .ok_or_else(|| io::Error::other("Metal replay size overflow"))?;
                count = 0;
                frames = 0;
            }
            count += records;
            frames += 1;
        }
        if count > 0 {
            batches.push(ReplayBatchLayout {
                offset,
                records: count,
                frames,
            });
            offset = offset
                .checked_add(aligned(count * 48)?)
                .ok_or_else(|| io::Error::other("Metal replay size overflow"))?;
        }
        let charged_bytes = offset
            .checked_add(
                batches.len()
                    * (size_of::<ReplayBatchLayout>() + size_of::<DeviceNormalPreparedBatch>()),
            )
            .and_then(|bytes| bytes.checked_add(size_of::<PreparedMetalNormalReplay>() + 64))
            .ok_or_else(|| io::Error::other("Metal replay metadata overflow"))?;
        Ok(ReplayLayout {
            batches: batches.into_boxed_slice(),
            buffer_bytes: offset,
            charged_bytes,
        })
    }

    pub(super) fn replay_capacity_bytes(
        &self,
        program: &GriddedNormalOperatorProgram,
    ) -> io::Result<u64> {
        u64::try_from(
            self.replay_layout(program.device_normal_frame_records())?
                .charged_bytes,
        )
        .map_err(io::Error::other)
    }
    pub(crate) fn new(
        program: &GriddedNormalOperatorProgram,
        window: &GriddedNormalReplayWindowPlan,
        node: &WorkNodeId,
    ) -> io::Result<Self> {
        let shape = program.device_normal_shape().map_err(io::Error::other)?;
        let grid_bytes = shape[0]
            .checked_mul(shape[1])
            .and_then(|n| n.checked_mul(8))
            .ok_or_else(|| io::Error::other("device normal grid overflow"))?;
        // Reuse the admitted route envelope to select substantial device batches,
        // with whole source frames as the minimum, rather than synchronizing at
        // every small artifact frame. The added arena is priced independently.
        let per_record = size_of::<DeviceNormalRecord>() + size_of::<DeviceNormalGroup>() + 8;
        let capacity = (window.route_capacity_bytes() as usize / (2 * per_record))
            .max(window.maximum_records())
            .min(program.record_count() as usize)
            .max(1);
        if capacity > u32::MAX as usize
            || shape[0]
                .checked_mul(shape[1])
                .is_none_or(|n| n > u32::MAX as usize)
        {
            return Err(io::Error::other("device normal indexing overflow"));
        }
        let weights = 0;
        let model = aligned(
            casa_imaging_reconstruction::runtime_adapter::BandPlan::spatial_weight_bytes(),
        )?;
        let normal = model
            .checked_add(aligned(grid_bytes)?)
            .ok_or_else(|| io::Error::other("device normal arena overflow"))?;
        let staging = normal
            .checked_add(aligned(grid_bytes)?)
            .ok_or_else(|| io::Error::other("device normal arena overflow"))?;
        let stride = aligned(
            capacity
                .checked_mul(per_record)
                .and_then(|n| n.checked_add(64))
                .ok_or_else(|| io::Error::other("device normal staging overflow"))?,
        )?;
        let bytes = stride
            .checked_mul(2)
            .and_then(|n| n.checked_add(staging))
            .ok_or_else(|| io::Error::other("device normal arena overflow"))?;
        Ok(Self {
            allocation: AllocationId::new(format!("{}-metal-normal", node.as_str())),
            bytes,
            shape,
            capacity,
            weights,
            model,
            normal,
            staging,
            stride,
        })
    }

    fn region(&self, offset: usize, bytes: usize) -> MetalBufferRegion<'_> {
        MetalBufferRegion {
            allocation: &self.allocation,
            offset,
            bytes,
        }
    }

    fn slot(&self, slot: usize) -> [MetalBufferRegion<'_>; 4] {
        let records = self.staging + slot * self.stride;
        let groups = records + self.capacity * size_of::<DeviceNormalRecord>();
        let predictions = groups + self.capacity * size_of::<DeviceNormalGroup>();
        let status = predictions + self.capacity * 8;
        [
            self.region(records, self.capacity * size_of::<DeviceNormalRecord>()),
            self.region(groups, self.capacity * size_of::<DeviceNormalGroup>()),
            self.region(predictions, self.capacity * 8),
            self.region(status, 4),
        ]
    }
}

struct ReplayBatchLayout {
    offset: usize,
    records: usize,
    frames: usize,
}

impl ReplayBatchLayout {
    fn groups_offset(&self) -> usize {
        self.offset + self.records * size_of::<DeviceNormalRecord>()
    }
}

struct ReplayLayout {
    batches: Box<[ReplayBatchLayout]>,
    buffer_bytes: usize,
    charged_bytes: usize,
}

pub(super) struct PreparedMetalNormalReplay {
    buffer: Arc<MetalReplayBuffer>,
    layout: ReplayLayout,
    coverage: Box<[DeviceNormalPreparedBatch]>,
    shape: [usize; 2],
    capacity: usize,
    pub(super) read: ManagedSpillMeasurements,
}

impl PreparedMetalNormalReplay {
    pub(super) fn matches(&self, plan: &MetalNormalPlan) -> bool {
        self.shape == plan.shape && self.capacity == plan.capacity
    }

    pub(super) fn resident_bytes(&self) -> u64 {
        self.layout.charged_bytes as u64
    }
}

struct ReplayBuilder {
    buffer: MetalReplayBuffer,
    layout: ReplayLayout,
    coverage: Vec<DeviceNormalPreparedBatch>,
    start: DeviceNormalPosition,
}

pub(crate) struct DeviceNormalOperatorState {
    pub(super) state: DeviceNormalApply,
    pub(super) backing: Arc<RetainedGriddedBacking>,
    pub(super) binding: CompleteDataExecutionBinding,
}

pub(super) struct MetalNormalReplayKernel<'a> {
    state: DeviceNormalOperatorState,
    access: MetalBatchAccess<'a>,
    plan: MetalNormalPlan,
    tickets: [Option<u64>; 2],
    slot: usize,
    records: usize,
    groups: usize,
    packing_seconds: f64,
    wait_seconds: f64,
    upload_bytes: u64,
    building: Option<ReplayBuilder>,
    prepared: Option<Arc<PreparedMetalNormalReplay>>,
}

fn device_error(error: crate::MetalRuntimeError) -> CompleteDataOperatorError {
    CompleteDataOperatorError::Owner(SpectralOperatorError::SpatialExecution(error.to_string()))
}

impl<'a> MetalNormalReplayKernel<'a> {
    pub(super) fn new(
        state: DeviceNormalOperatorState,
        access: MetalBatchAccess<'a>,
        plan: &MetalNormalPlan,
        reservation: Option<crate::ResourceLease>,
        prepared: Option<Arc<PreparedMetalNormalReplay>>,
    ) -> Result<Self, CompleteDataOperatorError> {
        let grid_bytes = plan.shape[0] * plan.shape[1] * 8;
        let weights = access
            .with_bytes(plan.region(plan.model, grid_bytes), |bytes| {
                state
                    .state
                    .pack_model(bytemuck::cast_slice_mut::<u8, [f32; 2]>(bytes))
            })
            .map_err(device_error)??;
        access
            .with_bytes(plan.region(plan.normal, grid_bytes), |bytes| bytes.fill(0))
            .map_err(device_error)?;
        let building = reservation
            .map(|reservation| {
                let layout = plan
                    .replay_layout(state.backing.program.device_normal_frame_records())
                    .map_err(|_| CompleteDataOperatorError::ExecutionBinding)?;
                let Some(mut buffer) = access
                    .allocate_replay(reservation, layout.buffer_bytes)
                    .map_err(device_error)?
                else {
                    return Ok(None);
                };
                buffer
                    .with_bytes_mut(0, size_of_val(weights.as_slice()), |bytes| {
                        bytes.copy_from_slice(bytemuck::cast_slice(&weights));
                    })
                    .map_err(device_error)?;
                let count = layout.batches.len();
                Ok::<_, CompleteDataOperatorError>(Some(ReplayBuilder {
                    buffer,
                    layout,
                    coverage: Vec::with_capacity(count),
                    start: state.state.position(),
                }))
            })
            .transpose()?
            .flatten();
        if building.is_none() && prepared.is_none() {
            access
                .with_bytes(
                    plan.region(plan.weights, size_of_val(weights.as_slice())),
                    |bytes| {
                        bytes.copy_from_slice(bytemuck::cast_slice(&weights));
                    },
                )
                .map_err(device_error)?;
        }
        let upload_bytes = grid_bytes as u64
            + if prepared.is_none() {
                size_of_val(weights.as_slice()) as u64
            } else {
                0
            };
        Ok(Self {
            state,
            access,
            plan: plan.clone(),
            tickets: [None; 2],
            slot: 0,
            records: 0,
            groups: 0,
            packing_seconds: 0.0,
            wait_seconds: 0.0,
            upload_bytes,
            building,
            prepared,
        })
    }

    fn settle(&mut self, slot: usize) -> Result<(), CompleteDataOperatorError> {
        if let Some(ticket) = self.tickets[slot].take() {
            let started = Instant::now();
            self.access.wait(ticket).map_err(device_error)?;
            self.wait_seconds += started.elapsed().as_secs_f64();
        }
        Ok(())
    }

    fn submit(&mut self) -> Result<(), CompleteDataOperatorError> {
        if self.records == 0 {
            return Ok(());
        }
        if let Some(builder) = &mut self.building {
            let batch = &builder.layout.batches[builder.coverage.len()];
            let coverage = self.state.state.prepared_since(builder.start.clone())?;
            if coverage.frames() as usize != batch.frames || self.records != batch.records {
                return Err(CompleteDataOperatorError::ExecutionBinding);
            }
            builder.coverage.push(coverage);
            builder.start = self.state.state.position();
            self.upload_bytes += (self.records * size_of::<DeviceNormalRecord>()
                + self.groups * size_of::<DeviceNormalGroup>())
                as u64;
            self.records = 0;
            self.groups = 0;
            return Ok(());
        }
        self.dispatch(None)
    }

    fn dispatch(
        &mut self,
        replay: Option<(&Arc<MetalReplayBuffer>, [usize; 3])>,
    ) -> Result<(), CompleteDataOperatorError> {
        let [records, groups, predictions, status] = self.plan.slot(self.slot);
        self.access
            .with_bytes(status, |bytes| bytes.fill(0))
            .map_err(device_error)?;
        let grid_bytes = self.plan.shape[0] * self.plan.shape[1] * 8;
        let ticket = self.access.submit_normal(&[NormalDispatch {
            regions: [records, groups,
                self.plan.region(self.plan.weights, casa_imaging_reconstruction::runtime_adapter::BandPlan::spatial_weight_bytes()),
                self.plan.region(self.plan.model, grid_bytes), predictions,
                self.plan.region(self.plan.normal, grid_bytes), status],
            shape: [self.records as u32, self.groups as u32, self.plan.shape[0] as u32, self.plan.shape[1] as u32],
            replay,
        }]).map_err(device_error)?;
        if replay.is_none() {
            self.upload_bytes += (self.records * size_of::<DeviceNormalRecord>()
                + self.groups * size_of::<DeviceNormalGroup>())
                as u64;
        }
        self.tickets[self.slot] = Some(ticket);
        self.slot ^= 1;
        self.records = 0;
        self.groups = 0;
        Ok(())
    }

    fn dispatch_prepared(
        &mut self,
        prepared: &Arc<PreparedMetalNormalReplay>,
        range: std::ops::Range<usize>,
    ) -> Result<(), CompleteDataOperatorError> {
        self.settle(self.slot)?;
        let [records, groups, predictions, status] = self.plan.slot(self.slot);
        self.access
            .with_bytes(status, |bytes| bytes.fill(0))
            .map_err(device_error)?;
        let grid_bytes = self.plan.shape[0] * self.plan.shape[1] * 8;
        let mut dispatches = Vec::with_capacity(range.len());
        for ordinal in range {
            let batch = &prepared.layout.batches[ordinal];
            let (count, group_count) = prepared.coverage[ordinal].counts();
            dispatches.push(NormalDispatch {
                regions: [records, groups,
                    self.plan.region(self.plan.weights, casa_imaging_reconstruction::runtime_adapter::BandPlan::spatial_weight_bytes()),
                    self.plan.region(self.plan.model, grid_bytes), predictions,
                    self.plan.region(self.plan.normal, grid_bytes), status],
                shape: [count as u32, group_count as u32, self.plan.shape[0] as u32, self.plan.shape[1] as u32],
                replay: Some((&prepared.buffer, [batch.offset, batch.groups_offset(), 0])),
            });
        }
        self.tickets[self.slot] = Some(
            self.access
                .submit_normal(&dispatches)
                .map_err(device_error)?,
        );
        self.slot ^= 1;
        Ok(())
    }

    pub(super) fn run_prepared(
        mut self,
        prepared: Arc<PreparedMetalNormalReplay>,
    ) -> Result<MetalNormalCompletion, CompleteDataOperatorError> {
        // These inputs are already resident: no producer thread, cursor slots,
        // replay copies or CPU partition jobs are needed. Two GPU tickets bound
        // the mutable prediction/status slots exactly as in the streaming case.
        for start in (0..prepared.coverage.len()).step_by(NORMAL_BATCHES_PER_COMMAND) {
            let end = (start + NORMAL_BATCHES_PER_COMMAND).min(prepared.coverage.len());
            for ordinal in start..end {
                self.state
                    .state
                    .accept_prepared(&prepared.coverage[ordinal])?;
            }
            self.dispatch_prepared(&prepared, start..end)?;
        }
        self.prepared = Some(prepared);
        self.finish()
    }

    fn finish(mut self) -> Result<MetalNormalCompletion, CompleteDataOperatorError> {
        self.submit()?;
        if let Some(builder) = self.building.take() {
            if builder.coverage.len() != builder.layout.batches.len() {
                return Err(CompleteDataOperatorError::ExecutionBinding);
            }
            let prepared = Arc::new(PreparedMetalNormalReplay {
                buffer: Arc::new(builder.buffer),
                layout: builder.layout,
                coverage: builder.coverage.into_boxed_slice(),
                shape: self.plan.shape,
                capacity: self.plan.capacity,
                read: ManagedSpillMeasurements::retained_device(self.state.backing.spill.seal()),
            });
            for start in (0..prepared.coverage.len()).step_by(NORMAL_BATCHES_PER_COMMAND) {
                let end = (start + NORMAL_BATCHES_PER_COMMAND).min(prepared.coverage.len());
                self.dispatch_prepared(&prepared, start..end)?;
            }
            self.prepared = Some(prepared);
        }
        self.settle(0)?;
        self.settle(1)?;
        let grid_bytes = self.plan.shape[0] * self.plan.shape[1] * 8;
        let started = Instant::now();
        let state = self.state;
        let (evidence, routing, recycle) = self
            .access
            .with_bytes(self.plan.region(self.plan.normal, grid_bytes), |bytes| {
                state.state.finish(bytemuck::cast_slice(bytes))
            })
            .map_err(device_error)??;
        if evidence.completion().problem_id() != state.binding.problem {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        eprintln!(
            "imaging_metal_normal_summary capacity_records={} arena_bytes={} packing_seconds={:.6} wait_seconds={:.6} finish_seconds={:.6} upload_bytes={} readback_bytes={} frames={} records={} groups={} prepared_replay_bytes={}",
            self.plan.capacity,
            self.plan.bytes,
            self.packing_seconds,
            self.wait_seconds,
            started.elapsed().as_secs_f64(),
            self.upload_bytes,
            grid_bytes,
            routing.frames_routed,
            routing.encoded_records,
            routing.prediction_groups,
            self.prepared
                .as_ref()
                .map_or(0, |cache| cache.resident_bytes()),
        );
        Ok((
            CompleteDataSlabResult {
                evidence,
                binding: state.binding,
            },
            routing,
            recycle,
            self.prepared,
        ))
    }
}

type MetalNormalCompletion = (
    CompleteDataSlabResult,
    GriddedNormalRoutingMeasurements,
    PreparedSpectralOperatorRecycle,
    Option<Arc<PreparedMetalNormalReplay>>,
);

impl PartitionedKernel<ManagedSpillWindowStorage> for MetalNormalReplayKernel<'_> {
    type Partition = ();
    type Partial = ();
    type Completion = MetalNormalCompletion;
    type Error = CompleteDataOperatorError;

    fn partition_count(
        &self,
        _: BlockIdentity,
        storage: &ManagedSpillWindowStorage,
    ) -> Result<usize, Self::Error> {
        if storage.frame_count() == 0 {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(1)
    }
    fn partition(
        &self,
        _: BlockIdentity,
        _: &ManagedSpillWindowStorage,
        ordinal: usize,
    ) -> Result<KernelPartition<()>, Self::Error> {
        if ordinal != 0 {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(KernelPartition::exclusive(0, 0, ()))
    }
    fn execute(
        &self,
        _: WorkIdentity,
        _: &ManagedSpillWindowStorage,
        _: &(),
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn commit(
        &mut self,
        _: WorkIdentity,
        storage: &ManagedSpillWindowStorage,
        _: (),
        _: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<(), Self::Error> {
        let mut total = 0;
        for frame in storage.frames() {
            let count = frame.record_count() as usize;
            if frame.payload().len() != count * GRIDDED_NORMAL_OPERATOR_RECORD_BYTES
                || count > self.plan.capacity
            {
                return Err(CompleteDataOperatorError::ExecutionBinding);
            }
            if self.records + count > self.plan.capacity {
                self.submit()?;
            }
            let started = Instant::now();
            let state = &mut self.state.state;
            let counts = if let Some(builder) = &mut self.building {
                let batch = &builder.layout.batches[builder.coverage.len()];
                let records_bytes = batch.records * size_of::<DeviceNormalRecord>();
                builder
                    .buffer
                    .with_bytes_mut(batch.offset, batch.records * 48, |bytes| {
                        let (records, groups) = bytes.split_at_mut(records_bytes);
                        state.pack_frame(
                            frame.sequence(),
                            frame.payload(),
                            bytemuck::cast_slice_mut(records),
                            bytemuck::cast_slice_mut(groups),
                            self.records,
                            self.groups,
                        )
                    })
                    .map_err(device_error)??
            } else {
                // CPU writes cannot overlap the preceding use of this slot.
                if let Some(ticket) = self.tickets[self.slot].take() {
                    let started = Instant::now();
                    self.access.wait(ticket).map_err(device_error)?;
                    self.wait_seconds += started.elapsed().as_secs_f64();
                }
                let [records, groups, _, _] = self.plan.slot(self.slot);
                self.access
                    .with_bytes(
                        self.plan
                            .region(records.offset, records.bytes + groups.bytes),
                        |bytes| {
                            let (records_bytes, groups_bytes) = bytes.split_at_mut(records.bytes);
                            state.pack_frame(
                                frame.sequence(),
                                frame.payload(),
                                bytemuck::cast_slice_mut(records_bytes),
                                bytemuck::cast_slice_mut(groups_bytes),
                                self.records,
                                self.groups,
                            )
                        },
                    )
                    .map_err(device_error)??
            };
            self.packing_seconds += started.elapsed().as_secs_f64();
            self.records += counts.0;
            self.groups += counts.1;
            total += count as u64;
        }
        if total != storage.record_count() {
            return Err(CompleteDataOperatorError::ExecutionBinding);
        }
        Ok(())
    }

    fn complete(
        self,
        _: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<Self::Completion, Self::Error> {
        self.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_layout_preserves_whole_frame_batches_and_prices_both_tables() {
        let plan = MetalNormalPlan {
            allocation: AllocationId::new("test"),
            bytes: 0,
            shape: [16, 16],
            capacity: 6,
            weights: 0,
            model: 0,
            normal: 0,
            staging: 0,
            stride: 0,
        };
        let layout = plan.replay_layout([4, 3, 2, 6, 1].into_iter()).unwrap();
        assert_eq!(
            layout
                .batches
                .iter()
                .map(|batch| (batch.records, batch.frames))
                .collect::<Vec<_>>(),
            [(4, 1), (5, 2), (6, 1), (1, 1)]
        );
        for batch in &layout.batches {
            assert_eq!(batch.offset % 64, 0);
            assert_eq!(batch.groups_offset(), batch.offset + batch.records * 40);
            assert!(batch.groups_offset() + batch.records * 8 <= layout.buffer_bytes);
        }
        assert!(
            layout.charged_bytes
                >= layout.buffer_bytes
                    + layout.batches.len()
                        * (size_of::<ReplayBatchLayout>() + size_of::<DeviceNormalPreparedBatch>())
        );
        assert!(plan.replay_layout([7].into_iter()).is_err());
        assert!(plan.replay_layout([0].into_iter()).is_err());
        assert!(
            plan.replay_layout([].into_iter())
                .unwrap()
                .batches
                .is_empty()
        );
    }
}
