// SPDX-License-Identifier: LGPL-3.0-or-later

//! Crate-private Metal operators for contiguous cube grids.

pub(super) use casa_imaging_reconstruction::runtime_adapter::SpatialTap as CubeTap;
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::NSString;
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandEncoder, MTLCompileOptions, MTLComputeCommandEncoder,
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

struct Prediction { CubeTap tap; uint plane; uint padding; };
struct NativePrediction { uint indices[2]; float factors[2]; };
struct ResidualSample { CubeTap tap; uint left; uint right; uint nearest_flags; uint plane; float factors[2]; };
struct ResidualParameters { uint shape[8]; float2 coefficients[4]; uint correlations; int direct; uint padding[2]; };

float2 multiply_complex(float2 a, float2 b) {
    return float2(a.x * b.x - a.y * b.y, a.x * b.y + a.y * b.x);
}

kernel void cube_predict_unique(
    device const Prediction *requests [[buffer(0)]],
    device const NativePrediction *native [[buffer(1)]],
    device const ResidualSample *samples [[buffer(2)]],
    device const float2 *values [[buffer(3)]],
    device const float *input_weights [[buffer(4)]],
    device const uchar *flags [[buffer(5)]],
    device const float *weights [[buffer(6)]],
    device const float2 *models [[buffer(7)]],
    device float2 *predicted [[buffer(8)]],
    device atomic_float *residual [[buffer(9)]],
    device atomic_uint *status [[buffer(10)]],
    constant ResidualParameters &p [[buffer(11)]],
    uint index [[thread_position_in_grid]]) {
    if (index >= p.shape[0]) return;
    Prediction request = requests[index];
    uint width = p.shape[4], height = p.shape[5];
    if (request.plane >= p.shape[3] || request.tap.x > width - 7 || request.tap.y > height - 7
        || request.tap.x_weights >= p.shape[7] || request.tap.y_weights >= p.shape[7]) {
        atomic_fetch_or_explicit(status, 1u, memory_order_relaxed); return;
    }
    uint base = request.plane * width * height;
    float2 result = float2(0.0);
    for (uint x = 0; x < 7; ++x) {
        float2 even = float2(0.0), odd = float2(0.0);
        for (uint y = 0; y < 7; ++y) {
            uint cell = base + (request.tap.x + x) * height + request.tap.y + y;
            float2 value = models[cell] * weights[request.tap.y_weights * 7 + y];
            if ((y & 1) == 0) even += value; else odd += value;
        }
        result += (even + odd) * weights[request.tap.x_weights * 7 + x];
    }
    result = multiply_complex(result, request.tap.value);
    if (!all(isfinite(result))) atomic_fetch_or_explicit(status, 2u, memory_order_relaxed);
    predicted[index] = result;
}

float2 native_prediction(device const NativePrediction *native, device const float2 *predicted,
                         uint index, constant ResidualParameters &p, device atomic_uint *status) {
    NativePrediction terms = native[index];
    float2 value = float2(0.0);
    for (uint i = 0; i < 2; ++i) {
        if (terms.indices[i] == 0xffffffffu) continue;
        if (terms.indices[i] >= p.shape[0]) {
            atomic_fetch_or_explicit(status, 1u, memory_order_relaxed); continue;
        }
        value += predicted[terms.indices[i]] * terms.factors[i];
    }
    return value;
}

kernel void cube_residual_connected(
    device const Prediction *requests [[buffer(0)]],
    device const NativePrediction *native [[buffer(1)]],
    device const ResidualSample *samples [[buffer(2)]],
    device const float2 *values [[buffer(3)]],
    device const float *input_weights [[buffer(4)]],
    device const uchar *flags [[buffer(5)]],
    device const float *weights [[buffer(6)]],
    device const float2 *models [[buffer(7)]],
    device const float2 *predicted [[buffer(8)]],
    device atomic_float *residual [[buffer(9)]],
    device atomic_uint *status [[buffer(10)]],
    constant ResidualParameters &p [[buffer(11)]],
    uint index [[thread_position_in_grid]]) {
    if (index >= p.shape[1]) return;
    ResidualSample sample = samples[index];
    uint nearest = sample.nearest_flags & 0x3fffffffu;
    uint mask = sample.nearest_flags >> 30;
    if (sample.left >= p.shape[2] || sample.right >= p.shape[2] || nearest >= p.shape[2]
        || sample.plane >= p.shape[6]) {
        atomic_fetch_or_explicit(status, 1u, memory_order_relaxed); return;
    }
    float2 lp = native_prediction(native, predicted, sample.left, p, status);
    float2 rp = native_prediction(native, predicted, sample.right, p, status);
    float2 observed = float2(0.0), prediction = float2(0.0);
    float2 direct_observed = float2(0.0), direct_prediction = float2(0.0);
    float diagonal = 0.0;
    for (uint correlation = 0; correlation < p.correlations; ++correlation) {
        uint left = sample.left * p.correlations + correlation;
        uint right = sample.right * p.correlations + correlation;
        uint weight_index = nearest * p.correlations + correlation;
        bool flagged = ((mask & 1) && (flags[left] & 1)) || ((mask & 2) && (flags[right] & 1))
            || (flags[weight_index] & 2);
        if (flagged) continue;
        float weight = input_weights[weight_index];
        if (!isfinite(weight) || weight < 0.0) {
            atomic_fetch_or_explicit(status, 2u, memory_order_relaxed); continue;
        }
        if (weight == 0.0) continue;
        float2 coefficient = p.coefficients[correlation];
        float2 observation = values[left] * sample.factors[0] + values[right] * sample.factors[1];
        float2 estimate = multiply_complex(coefficient, lp) * sample.factors[0]
            + multiply_complex(coefficient, rp) * sample.factors[1];
        if (!all(isfinite(observation)) || !all(isfinite(estimate))) {
            atomic_fetch_or_explicit(status, 2u, memory_order_relaxed); continue;
        }
        float2 conjugate = float2(coefficient.x, -coefficient.y);
        observed += multiply_complex(conjugate, observation * weight);
        prediction += multiply_complex(conjugate, estimate * weight);
        diagonal += weight * dot(coefficient, coefficient);
        if (p.direct == int(correlation)) {
            direct_observed = observation; direct_prediction = estimate;
        }
    }
    if (diagonal == 0.0 || sample.tap.x == 0xffffffffu) return;
    if (p.direct >= 0) { observed = direct_observed; prediction = direct_prediction; }
    else { observed /= diagonal; prediction /= diagonal; }
    float2 value = multiply_complex(observed - prediction, sample.tap.value) * diagonal;
    if (!all(isfinite(value))) {
        atomic_fetch_or_explicit(status, 2u, memory_order_relaxed); return;
    }
    uint width = p.shape[4], height = p.shape[5];
    if (sample.tap.x > width - 7 || sample.tap.y > height - 7
        || sample.tap.x_weights >= p.shape[7] || sample.tap.y_weights >= p.shape[7]) {
        atomic_fetch_or_explicit(status, 1u, memory_order_relaxed); return;
    }
    uint base = sample.plane * width * height;
    for (uint x = 0; x < 7; ++x) {
        float x_weight = weights[sample.tap.x_weights * 7 + x];
        for (uint y = 0; y < 7; ++y) {
            float y_weight = weights[sample.tap.y_weights * 7 + y];
            uint cell = base + (sample.tap.x + x) * height + sample.tap.y + y;
            atomic_fetch_add_explicit(&residual[2 * cell], value.x * x_weight * y_weight, memory_order_relaxed);
            atomic_fetch_add_explicit(&residual[2 * cell + 1], value.y * x_weight * y_weight, memory_order_relaxed);
        }
    }
}
"#;

/// The CPU supplies CASA's selected sample geometry and seven-tap tables. The
/// same compiled pipelines are reused across blocks within the admitted execution.
pub(super) struct MetalCubeKernels {
    grid: Retained<ProtocolObject<dyn MTLComputePipelineState>>,
    degrid: Retained<ProtocolObject<dyn MTLComputePipelineState>>,
    unique_prediction: Retained<ProtocolObject<dyn MTLComputePipelineState>>,
    residual: Retained<ProtocolObject<dyn MTLComputePipelineState>>,
}

impl MetalCubeKernels {
    pub(super) fn compile(device: &ProtocolObject<dyn MTLDevice>) -> Result<Self, String> {
        let source = NSString::from_str(SOURCE);
        let options = MTLCompileOptions::new();
        #[allow(deprecated)]
        options.setFastMathEnabled(false);
        let library = device
            .newLibraryWithSource_options_error(&source, Some(&options))
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
            unique_prediction: pipeline("cube_predict_unique")?,
            residual: pipeline("cube_residual_connected")?,
        })
    }

    pub(super) fn encode_grid(
        &self,
        encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
        samples: (&ProtocolObject<dyn MTLBuffer>, usize),
        weights: (&ProtocolObject<dyn MTLBuffer>, usize),
        grid: (&ProtocolObject<dyn MTLBuffer>, usize),
        count: u32,
        width: u32,
        height: u32,
    ) -> Result<(), String> {
        self.encode(
            encoder,
            &self.grid,
            &[samples, weights, grid],
            count,
            width,
            height,
        )
    }

    pub(super) fn encode_degrid(
        &self,
        encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
        samples: (&ProtocolObject<dyn MTLBuffer>, usize),
        weights: (&ProtocolObject<dyn MTLBuffer>, usize),
        grid: (&ProtocolObject<dyn MTLBuffer>, usize),
        predicted: (&ProtocolObject<dyn MTLBuffer>, usize),
        count: u32,
        width: u32,
        height: u32,
    ) -> Result<(), String> {
        self.encode(
            encoder,
            &self.degrid,
            &[samples, weights, grid, predicted],
            count,
            width,
            height,
        )
    }

    pub(super) fn encode_residual(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        buffers: &[(&ProtocolObject<dyn MTLBuffer>, usize)],
        shape: [u32; 8],
        correlations: casa_imaging_reconstruction::runtime_adapter::DeviceCorrelations,
    ) -> Result<(), String> {
        #[repr(C)]
        struct Parameters {
            shape: [u32; 8],
            correlations: casa_imaging_reconstruction::runtime_adapter::DeviceCorrelations,
        }
        let parameters = Parameters {
            shape,
            correlations,
        };
        for (pipeline, count) in [
            (&self.unique_prediction, shape[0]),
            (&self.residual, shape[1]),
        ] {
            if count == 0 {
                continue;
            }
            let encoder = command
                .computeCommandEncoder()
                .ok_or("Metal compute encoder unavailable")?;
            encoder.setComputePipelineState(pipeline);
            for (index, (buffer, offset)) in buffers.iter().enumerate() {
                unsafe { encoder.setBuffer_offset_atIndex(Some(buffer), *offset, index) };
            }
            let pointer = NonNull::from(&parameters).cast::<c_void>();
            unsafe { encoder.setBytes_length_atIndex(pointer, size_of::<Parameters>(), 11) };
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
        }
        Ok(())
    }

    fn encode(
        &self,
        encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
        pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
        buffers: &[(&ProtocolObject<dyn MTLBuffer>, usize)],
        count: u32,
        width: u32,
        height: u32,
    ) -> Result<(), String> {
        if count == 0 || width < 7 || height < 7 {
            return Err("invalid Metal cube grid shape".to_string());
        }
        encoder.setComputePipelineState(pipeline);
        for (index, (buffer, offset)) in buffers.iter().enumerate() {
            // The caller owns every buffer through the terminal command fence.
            unsafe { encoder.setBuffer_offset_atIndex(Some(buffer), *offset, index) };
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use objc2_metal::{
        MTLCommandBufferStatus, MTLCommandQueue, MTLCreateSystemDefaultDevice, MTLDispatchType,
        MTLResourceOptions,
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
        let encoder = command
            .computeCommandEncoder()
            .expect("serial compute pass");
        assert_eq!(encoder.dispatchType(), MTLDispatchType::Serial);
        kernels
            .encode_grid(
                &encoder,
                (&sample_buffer, 0),
                (&weight_buffer, 0),
                (&grid_buffer, 0),
                2,
                16,
                16,
            )
            .expect("grid encoding");
        kernels
            .encode_degrid(
                &encoder,
                (&sample_buffer, 0),
                (&weight_buffer, 0),
                (&grid_buffer, 0),
                (&predicted_buffer, 0),
                2,
                16,
                16,
            )
            .expect("degrid encoding");
        encoder.endEncoding();
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
