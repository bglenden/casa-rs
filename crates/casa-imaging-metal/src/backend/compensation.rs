// SPDX-License-Identifier: LGPL-3.0-or-later
//! #991 experiment: fold GPU rounding losses once per apply, without host I/O.

use objc2_metal::{
    MTLCommandBuffer, MTLCommandEncoder, MTLCommandQueue, MTLComputeCommandEncoder, MTLSize,
};

use super::{Command, DeviceFailure, MetalBackend, OperatorError, RING};
use crate::device::Buffer;

impl MetalBackend<'_> {
    pub(super) fn finish_grid(&mut self, grid: &Buffer, floats: u64) -> Result<(), OperatorError> {
        let failed = OperatorError::Device(DeviceFailure::CommandFailed { code: -1 });
        let device = self.device;
        let pipeline = &device.fold;
        objc2::rc::autoreleasepool(|_| {
            let command = device.queue.commandBuffer().ok_or(failed.clone())?;
            let encoder = command.computeCommandEncoder().ok_or(failed)?;
            encoder.setComputePipelineState(&pipeline.state);
            // SAFETY: the command retains the allocation; its high/low grids
            // are three disjoint spans. Metal executes after all earlier spread
            // commands on this queue. The last ring slot retains both commands,
            // so completion/readback inspect both statuses; deferred reads wait
            // for this final fold, and complete() reports failures as usual.
            unsafe {
                encoder.setBuffer_offset_atIndex(Some(&grid.raw), 0, 0);
                encoder.setBytes_length_atIndex(
                    std::ptr::NonNull::from(&floats).cast(),
                    size_of::<u64>(),
                    1,
                );
            }
            encoder.dispatchThreadgroups_threadsPerThreadgroup(
                MTLSize {
                    width: (floats as usize).div_ceil(pipeline.threads),
                    height: 1,
                    depth: 1,
                },
                MTLSize {
                    width: pipeline.threads,
                    height: 1,
                    depth: 1,
                },
            );
            encoder.endEncoding();
            command.commit();
            let slot = &mut self.ring[(self.next + RING - 1) % RING];
            assert!(
                slot.fold_command.is_none(),
                "the reused slot retired its fold"
            );
            slot.fold_command = Some(Command(command));
            Ok(())
        })
    }
}
