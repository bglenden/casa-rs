// SPDX-License-Identifier: LGPL-3.0-or-later

//! Crate-private Metal operators for contiguous cube grids.

#![allow(dead_code, reason = "T57 application dispatch is not connected yet")]

use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::NSString;
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandEncoder, MTLComputeCommandEncoder,
    MTLComputePipelineState, MTLDevice, MTLLibrary, MTLSize,
};
use std::ffi::c_void;
use std::ptr::NonNull;

const SOURCE: &str = r#"
#include <metal_stdlib>
using namespace metal;

struct CubeTap {
    uint x;
    uint y;
    uint x_weights;
    uint y_weights;
    float2 value;
};

kernel void cube_grid_taps(
    device const CubeTap *samples [[buffer(0)]],
    device const float *weights [[buffer(1)]],
    device atomic_float *grid [[buffer(2)]],
    constant uint4 &shape [[buffer(3)]],
    uint sample_index [[thread_position_in_grid]]) {
    if (sample_index >= shape.x) return;
    CubeTap sample = samples[sample_index];
    for (uint x = 0; x < 7; ++x) {
        float x_weight = weights[sample.x_weights * 7 + x];
        for (uint y = 0; y < 7; ++y) {
            float y_weight = weights[sample.y_weights * 7 + y];
            uint cell = (sample.x + x) * shape.z + sample.y + y;
            atomic_fetch_add_explicit(&grid[2 * cell], sample.value.x * x_weight * y_weight, memory_order_relaxed);
            atomic_fetch_add_explicit(&grid[2 * cell + 1], sample.value.y * x_weight * y_weight, memory_order_relaxed);
        }
    }
}

kernel void cube_degrid_taps(
    device const CubeTap *samples [[buffer(0)]],
    device const float *weights [[buffer(1)]],
    device const float2 *grid [[buffer(2)]],
    device float2 *predicted [[buffer(3)]],
    constant uint4 &shape [[buffer(4)]],
    uint sample_index [[thread_position_in_grid]]) {
    if (sample_index >= shape.x) return;
    CubeTap sample = samples[sample_index];
    float2 result = float2(0.0);
    for (uint x = 0; x < 7; ++x) {
        float2 row_even = float2(0.0);
        float2 row_odd = float2(0.0);
        for (uint y = 0; y < 7; ++y) {
            uint cell = (sample.x + x) * shape.z + sample.y + y;
            float2 contribution = grid[cell] * weights[sample.y_weights * 7 + y];
            if ((y & 1) == 0) row_even += contribution;
            else row_odd += contribution;
        }
        result += (row_even + row_odd) * weights[sample.x_weights * 7 + x];
    }
    predicted[sample_index] = result;
}
"#;

/// The CPU supplies CASA's selected sample geometry and seven-tap tables. The
/// same compiled pipelines are reused across blocks and major phases.
pub(super) struct MetalCubeKernels {
    grid: Retained<ProtocolObject<dyn MTLComputePipelineState>>,
    degrid: Retained<ProtocolObject<dyn MTLComputePipelineState>>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct CubeTap {
    pub x: u32,
    pub y: u32,
    pub x_weights: u32,
    pub y_weights: u32,
    pub value: [f32; 2],
}

impl MetalCubeKernels {
    pub(super) fn compile(device: &ProtocolObject<dyn MTLDevice>) -> Result<Self, String> {
        let source = NSString::from_str(SOURCE);
        let library = device
            .newLibraryWithSource_options_error(&source, None)
            .map_err(|error| error.localizedDescription().to_string())?;
        let pipeline = |name| {
            let function = library
                .newFunctionWithName(&NSString::from_str(name))
                .ok_or_else(|| format!("Metal function {name} is missing"))?;
            device
                .newComputePipelineStateWithFunction_error(&function)
                .map_err(|error| error.localizedDescription().to_string())
        };
        Ok(Self {
            grid: pipeline("cube_grid_taps")?,
            degrid: pipeline("cube_degrid_taps")?,
        })
    }

    pub(super) fn encode_grid(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        samples: &ProtocolObject<dyn MTLBuffer>,
        weights: &ProtocolObject<dyn MTLBuffer>,
        grid: &ProtocolObject<dyn MTLBuffer>,
        count: u32,
        width: u32,
        height: u32,
    ) -> Result<(), String> {
        self.encode(
            command,
            &self.grid,
            &[samples, weights, grid],
            count,
            width,
            height,
        )
    }

    pub(super) fn encode_degrid(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        samples: &ProtocolObject<dyn MTLBuffer>,
        weights: &ProtocolObject<dyn MTLBuffer>,
        grid: &ProtocolObject<dyn MTLBuffer>,
        predicted: &ProtocolObject<dyn MTLBuffer>,
        count: u32,
        width: u32,
        height: u32,
    ) -> Result<(), String> {
        self.encode(
            command,
            &self.degrid,
            &[samples, weights, grid, predicted],
            count,
            width,
            height,
        )
    }

    fn encode(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
        buffers: &[&ProtocolObject<dyn MTLBuffer>],
        count: u32,
        width: u32,
        height: u32,
    ) -> Result<(), String> {
        if count == 0 || width < 7 || height < 7 {
            return Err("invalid Metal cube grid shape".to_string());
        }
        let encoder = command
            .computeCommandEncoder()
            .ok_or_else(|| "Metal compute encoder unavailable".to_string())?;
        encoder.setComputePipelineState(pipeline);
        for (index, buffer) in buffers.iter().enumerate() {
            // The caller owns every buffer through the terminal command fence.
            unsafe { encoder.setBuffer_offset_atIndex(Some(buffer), 0, index) };
        }
        let shape = [count, width, height, 0_u32];
        let shape_ptr = NonNull::new((&shape as *const [u32; 4]).cast_mut().cast::<c_void>())
            .expect("stack value has a non-null address");
        // Metal copies these 16 bytes into the encoder before this stack frame ends.
        unsafe { encoder.setBytes_length_atIndex(shape_ptr, size_of_val(&shape), buffers.len()) };
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: count as usize,
                height: 1,
                depth: 1,
            },
            MTLSize {
                width: 64,
                height: 1,
                depth: 1,
            },
        );
        encoder.endEncoding();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use objc2_metal::{
        MTLCommandBufferStatus, MTLCommandQueue, MTLCreateSystemDefaultDevice, MTLResourceOptions,
    };

    fn shared_buffer(
        device: &ProtocolObject<dyn MTLDevice>,
        bytes: usize,
    ) -> Retained<ProtocolObject<dyn MTLBuffer>> {
        device
            .newBufferWithLength_options(bytes, MTLResourceOptions::StorageModeShared)
            .expect("shared Metal allocation")
    }

    fn upload<T: Copy>(buffer: &ProtocolObject<dyn MTLBuffer>, values: &[T]) {
        assert!(buffer.length() >= size_of_val(values));
        // The test waits for the command fence before touching these bytes again.
        unsafe {
            std::ptr::copy_nonoverlapping(
                values.as_ptr().cast::<u8>(),
                buffer.contents().as_ptr().cast::<u8>(),
                size_of_val(values),
            );
        }
    }

    #[test]
    #[ignore = "requires a process-accessible Apple Metal device"]
    fn overlapping_grid_and_degrid_match_float_cpu_operator() {
        let device = MTLCreateSystemDefaultDevice().expect("actual Metal device");
        let queue = device.newCommandQueue().expect("Metal queue");
        let kernels = MetalCubeKernels::compile(&device).expect("science pipelines");
        let samples = [
            CubeTap {
                x: 3,
                y: 4,
                x_weights: 0,
                y_weights: 1,
                value: [0.75, -0.25],
            },
            CubeTap {
                x: 4,
                y: 5,
                x_weights: 1,
                y_weights: 0,
                value: [-0.125, 0.5],
            },
        ];
        let weights = [
            0.01_f32, 0.04, 0.12, 0.26, 0.12, 0.04, 0.01, 0.02, 0.05, 0.16, 0.31, 0.16, 0.05, 0.02,
        ];
        let mut reference = vec![[0.0_f32; 2]; 16 * 16];
        for sample in samples {
            for x in 0..7 {
                for y in 0..7 {
                    let cell = (sample.x as usize + x) * 16 + sample.y as usize + y;
                    let weight = weights[sample.x_weights as usize * 7 + x]
                        * weights[sample.y_weights as usize * 7 + y];
                    reference[cell][0] += sample.value[0] * weight;
                    reference[cell][1] += sample.value[1] * weight;
                }
            }
        }
        let sample_buffer = shared_buffer(&device, size_of_val(&samples));
        let weight_buffer = shared_buffer(&device, size_of_val(&weights));
        let grid_buffer = shared_buffer(&device, size_of_val(reference.as_slice()));
        let predicted_buffer = shared_buffer(&device, samples.len() * size_of::<[f32; 2]>());
        upload(&sample_buffer, &samples);
        upload(&weight_buffer, &weights);
        upload(&grid_buffer, &vec![[0.0_f32; 2]; reference.len()]);
        let command = queue.commandBuffer().expect("Metal command");
        kernels
            .encode_grid(
                &command,
                &sample_buffer,
                &weight_buffer,
                &grid_buffer,
                2,
                16,
                16,
            )
            .expect("grid encoding");
        kernels
            .encode_degrid(
                &command,
                &sample_buffer,
                &weight_buffer,
                &grid_buffer,
                &predicted_buffer,
                2,
                16,
                16,
            )
            .expect("degrid encoding");
        command.commit();
        command.waitUntilCompleted();
        assert_eq!(command.status(), MTLCommandBufferStatus::Completed);
        let actual = unsafe {
            std::slice::from_raw_parts(grid_buffer.contents().as_ptr().cast::<[f32; 2]>(), 256)
        };
        for (left, right) in actual.iter().zip(&reference) {
            for lane in 0..2 {
                assert!((left[lane] - right[lane]).abs() < 2.0e-7);
            }
        }
        let predicted = unsafe {
            std::slice::from_raw_parts(
                predicted_buffer.contents().as_ptr().cast::<[f32; 2]>(),
                samples.len(),
            )
        };
        for (index, sample) in samples.iter().enumerate() {
            let mut expected = [0.0_f32; 2];
            for x in 0..7 {
                let mut row = [[0.0_f32; 2]; 2];
                for y in 0..7 {
                    let cell = (sample.x as usize + x) * 16 + sample.y as usize + y;
                    let weight = weights[sample.y_weights as usize * 7 + y];
                    row[y % 2][0] += reference[cell][0] * weight;
                    row[y % 2][1] += reference[cell][1] * weight;
                }
                let weight = weights[sample.x_weights as usize * 7 + x];
                expected[0] += (row[0][0] + row[1][0]) * weight;
                expected[1] += (row[0][1] + row[1][1]) * weight;
            }
            for lane in 0..2 {
                assert!((predicted[index][lane] - expected[lane]).abs() < 2.0e-6);
            }
        }
    }
}
