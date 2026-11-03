// SPDX-License-Identifier: LGPL-3.0-or-later
//! #991: renormalize GPU rounding losses once per apply, without
//! host I/O. Unrepresented remainders survive until the accumulator is reset.

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

#[cfg(test)]
mod tests {
    use super::*;
    use casa_imaging_model::PolarizationCoordinate;
    use casa_imaging_operator::{
        Basis, CfKey, GridBackend, GridGeometry, GridPadding, GridPrecision, ImageExtent,
        MeasurementOperator, Mode, ModeSet, Placement, PlaneRange, PolarizationRouting,
        SampleBuffer, Spheroidal, Work,
    };
    use casa_types::Correlation;
    use num_complex::Complex32;
    use objc2_metal::MTLCommandBufferStatus;

    #[test]
    fn dropping_a_deferred_backend_completes_spread_and_fold_before_returning() {
        if !crate::available() {
            return;
        }
        let geometry = GridGeometry::new(
            ImageExtent {
                shape: [32, 32],
                increment_rad: [-1e-5, 1e-5],
                reference_pixel: [16, 16],
            },
            GridPadding::None,
        )
        .unwrap();
        let routing = PolarizationRouting::compile(
            &[Correlation::XX, Correlation::YY],
            &[PolarizationCoordinate::StokesI],
        )
        .unwrap();
        let cf = Spheroidal::new(&geometry, &routing);
        let operator = MeasurementOperator::new(
            geometry,
            Basis::Constant,
            routing,
            Box::new(cf),
            GridPrecision::F32,
        );
        let mut backend = MetalBackend::new(operator.cf(), false)
            .unwrap()
            .with_deferred_grid_completion();
        let mut acc = MetalBackend::accumulator(operator.accumulator_layout(
            PlaneRange::single(0),
            None,
            ModeSet::DATA_PSF,
        ))
        .unwrap();
        let mut samples = SampleBuffer::new(2);
        for _ in 0..100_000 {
            samples.push(
                Placement {
                    u: 0.0,
                    v: 0.0,
                    w: 0.0,
                    phase: 0.0,
                    plane: 0,
                    spectral: 0.0,
                    cf: CfKey { group: 0, cube: 0 },
                    gradient: [0.0; 2],
                    prediction_w_positive: false,
                },
                &[Complex32::new(1.1, -0.7); 2],
                &[0.7; 2],
            );
        }
        backend
            .apply(
                &samples.block(),
                operator.cf(),
                Work::Grid {
                    mode: Mode::Psf,
                    acc: &mut acc,
                },
            )
            .unwrap();
        let slot = &backend.ring[(backend.next + RING - 1) % RING];
        let spread = slot.command.clone().unwrap();
        let fold = slot.fold_command.clone().unwrap();
        // Inspect command states directly, before any DeviceCells accessor can
        // wait for deferred work and conceal an incomplete destructor drain.
        drop(backend);
        assert_eq!(spread.0.status(), MTLCommandBufferStatus::Completed);
        assert_eq!(fold.0.status(), MTLCommandBufferStatus::Completed);
    }
}
