// SPDX-License-Identifier: LGPL-3.0-or-later

//! Resident compiled scalar A*WA on the shared admitted Metal runtime.

use super::*;
use crate::metal_runtime::{MetalBatchAccess, MetalBufferRegion, NormalDispatch};
use casa_imaging_reconstruction::runtime_adapter::{
    DeviceNormalApply, DeviceNormalGroup, DeviceNormalRecord, GRIDDED_NORMAL_OPERATOR_RECORD_BYTES,
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
}

fn device_error(error: crate::MetalRuntimeError) -> CompleteDataOperatorError {
    CompleteDataOperatorError::Owner(SpectralOperatorError::SpatialExecution(error.to_string()))
}

impl<'a> MetalNormalReplayKernel<'a> {
    pub(super) fn new(
        state: DeviceNormalOperatorState,
        access: MetalBatchAccess<'a>,
        plan: &MetalNormalPlan,
    ) -> Result<Self, CompleteDataOperatorError> {
        let (model, weights) = state.state.model()?;
        let grid_bytes = model.len() * 8;
        access
            .with_bytes(
                plan.region(plan.weights, size_of_val(weights.as_slice())),
                |bytes| bytes.copy_from_slice(bytemuck::cast_slice(&weights)),
            )
            .map_err(device_error)?;
        access
            .with_bytes(plan.region(plan.model, grid_bytes), |bytes| {
                for (source, target) in model
                    .iter()
                    .zip(bytemuck::cast_slice_mut::<u8, [f32; 2]>(bytes))
                {
                    *target = [source.re as f32, source.im as f32];
                    if !target.iter().all(|n| n.is_finite()) {
                        return Err(SpectralOperatorError::GeneratedNonfinite);
                    }
                }
                Ok(())
            })
            .map_err(device_error)??;
        access
            .with_bytes(plan.region(plan.normal, grid_bytes), |bytes| bytes.fill(0))
            .map_err(device_error)?;
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
            upload_bytes: grid_bytes as u64 + size_of_val(weights.as_slice()) as u64,
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
        let [records, groups, predictions, status] = self.plan.slot(self.slot);
        self.access
            .with_bytes(status, |bytes| bytes.fill(0))
            .map_err(device_error)?;
        let grid_bytes = self.plan.shape[0] * self.plan.shape[1] * 8;
        let ticket = self.access.submit_normal(NormalDispatch {
            regions: [records, groups,
                self.plan.region(self.plan.weights, casa_imaging_reconstruction::runtime_adapter::BandPlan::spatial_weight_bytes()),
                self.plan.region(self.plan.model, grid_bytes), predictions,
                self.plan.region(self.plan.normal, grid_bytes), status],
            shape: [self.records as u32, self.groups as u32, self.plan.shape[0] as u32, self.plan.shape[1] as u32],
        }).map_err(device_error)?;
        self.upload_bytes += (self.records * size_of::<DeviceNormalRecord>()
            + self.groups * size_of::<DeviceNormalGroup>()) as u64;
        self.tickets[self.slot] = Some(ticket);
        self.slot ^= 1;
        self.records = 0;
        self.groups = 0;
        Ok(())
    }
}

impl PartitionedKernel<ManagedSpillWindowStorage> for MetalNormalReplayKernel<'_> {
    type Partition = ();
    type Partial = ();
    type Completion = (
        CompleteDataSlabResult,
        GriddedNormalRoutingMeasurements,
        PreparedSpectralOperatorRecycle,
    );
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
            self.settle(self.slot)?;
            let [records, groups, _, _] = self.plan.slot(self.slot);
            let started = Instant::now();
            let state = &mut self.state.state;
            let counts = self
                .access
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
                .map_err(device_error)??;
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
        mut self,
        _: crate::bounded_stream::BoundedExecution<'_>,
    ) -> Result<Self::Completion, Self::Error> {
        self.submit()?;
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
            "imaging_metal_normal_summary capacity_records={} arena_bytes={} packing_seconds={:.6} wait_seconds={:.6} finish_seconds={:.6} upload_bytes={} readback_bytes={} frames={} records={} groups={}",
            self.plan.capacity,
            self.plan.bytes,
            self.packing_seconds,
            self.wait_seconds,
            started.elapsed().as_secs_f64(),
            self.upload_bytes,
            grid_bytes,
            routing.frames_routed,
            routing.encoded_records,
            routing.prediction_groups
        );
        drop(state.backing);
        Ok((
            CompleteDataSlabResult {
                evidence,
                binding: state.binding,
            },
            routing,
            recycle,
        ))
    }
}
