// SPDX-License-Identifier: LGPL-3.0-or-later

//! Crate-private Metal operators for contiguous cube grids.

pub(super) use casa_imaging_reconstruction::runtime_adapter::SpatialTap as CubeTap;
use objc2::Message;
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::{NSRange, NSString};
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandEncoder, MTLCommonCounterSetTimestamp,
    MTLCompileOptions, MTLComputeCommandEncoder, MTLComputePassDescriptor, MTLComputePipelineState,
    MTLCounterErrorValue, MTLCounterSampleBuffer, MTLCounterSampleBufferDescriptor,
    MTLCounterSamplingPoint, MTLCounterSet, MTLDevice, MTLLibrary, MTLSize, MTLStorageMode,
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
    stage_profiler: Option<MetalStageProfiler>,
}

struct MetalStageProfiler {
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    timestamps: Retained<ProtocolObject<dyn MTLCounterSet>>,
}

pub(super) struct MetalStageProfile {
    samples: Retained<ProtocolObject<dyn MTLCounterSampleBuffer>>,
    clock_start: [u64; 2],
    active: [bool; 2],
}

fn sample_clocks(device: &ProtocolObject<dyn MTLDevice>) -> [u64; 2] {
    let mut clocks = [0_u64; 2];
    unsafe {
        device.sampleTimestamps_gpuTimestamp(
            NonNull::from(&mut clocks[0]),
            NonNull::from(&mut clocks[1]),
        );
    }
    clocks
}

impl MetalStageProfiler {
    fn new(device: &ProtocolObject<dyn MTLDevice>) -> Result<Self, String> {
        if !device.supportsCounterSampling(MTLCounterSamplingPoint::AtStageBoundary) {
            return Err("Metal stage-boundary timestamp profiling is unsupported".into());
        }
        let sets = device
            .counterSets()
            .ok_or("Metal counter sets are unavailable")?;
        let timestamps = sets
            .iter()
            .find(|set| {
                set.name()
                    .isEqualToString(unsafe { MTLCommonCounterSetTimestamp })
            })
            .ok_or_else(|| {
                format!(
                    "Metal timestamp counter set is unavailable; advertised sets: {:?}",
                    sets.iter()
                        .map(|set| set.name().to_string())
                        .collect::<Vec<_>>(),
                )
            })?;
        Ok(Self {
            device: device.retain(),
            timestamps,
        })
    }

    fn begin(&self, shape: [u32; 8]) -> Result<MetalStageProfile, String> {
        let descriptor = MTLCounterSampleBufferDescriptor::new();
        descriptor.setCounterSet(Some(&self.timestamps));
        descriptor.setStorageMode(MTLStorageMode::Shared);
        unsafe { descriptor.setSampleCount(4) };
        let samples = self
            .device
            .newCounterSampleBufferWithDescriptor_error(&descriptor)
            .map_err(|error| error.localizedDescription().to_string())?;
        Ok(MetalStageProfile {
            samples,
            clock_start: sample_clocks(&self.device),
            active: [shape[0] != 0, shape[1] != 0],
        })
    }
}

impl MetalStageProfile {
    /// Called only after the existing command fence. CPU timestamps from Metal's
    /// paired clock API are nanoseconds; GPU counter ticks require calibration.
    pub(super) fn seconds(&self) -> Result<[f64; 2], String> {
        let clock_end = sample_clocks(&self.samples.device());
        let data = unsafe { self.samples.resolveCounterRange(NSRange::new(0, 4)) }
            .ok_or("Metal timestamp resolve failed")?;
        let bytes = unsafe { data.as_bytes_unchecked() };
        if bytes.len() != 4 * size_of::<u64>() {
            return Err("invalid Metal timestamp result length".into());
        }
        let ticks = std::array::from_fn(|i| {
            u64::from_ne_bytes(
                bytes[i * 8..(i + 1) * 8]
                    .try_into()
                    .expect("timestamp size"),
            )
        });
        calibrated_stage_seconds(ticks, self.active, self.clock_start, clock_end)
    }
}

fn calibrated_stage_seconds(
    ticks: [u64; 4],
    active: [bool; 2],
    clock_start: [u64; 2],
    clock_end: [u64; 2],
) -> Result<[f64; 2], String> {
    let cpu_span = clock_end[0]
        .checked_sub(clock_start[0])
        .filter(|&span| span != 0)
        .ok_or("invalid Metal CPU timestamp calibration")?;
    let gpu_span = clock_end[1]
        .checked_sub(clock_start[1])
        .filter(|&span| span != 0)
        .ok_or("invalid Metal GPU timestamp calibration")?;
    let mut seconds = [0.0; 2];
    for (stage, active) in active.into_iter().enumerate() {
        if !active {
            continue;
        }
        let [start, end] = [ticks[2 * stage], ticks[2 * stage + 1]];
        if start == 0 || start == MTLCounterErrorValue || end == MTLCounterErrorValue {
            return Err("unavailable Metal stage timestamps".into());
        }
        let span = end
            .checked_sub(start)
            .ok_or("non-monotone Metal timestamps")?;
        seconds[stage] = span as f64 * (cpu_span as f64 / gpu_span as f64) * 1e-9;
    }
    Ok(seconds)
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
            stage_profiler: std::env::var_os("CASA_RS_PROFILE_METAL_STAGES")
                .map(|_| MetalStageProfiler::new(device))
                .transpose()?,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn encode_grid(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        samples: (&ProtocolObject<dyn MTLBuffer>, usize),
        weights: (&ProtocolObject<dyn MTLBuffer>, usize),
        grid: (&ProtocolObject<dyn MTLBuffer>, usize),
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

    #[allow(clippy::too_many_arguments)]
    pub(super) fn encode_degrid(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        samples: (&ProtocolObject<dyn MTLBuffer>, usize),
        weights: (&ProtocolObject<dyn MTLBuffer>, usize),
        grid: (&ProtocolObject<dyn MTLBuffer>, usize),
        predicted: (&ProtocolObject<dyn MTLBuffer>, usize),
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

    pub(super) fn encode_residual(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        buffers: &[(&ProtocolObject<dyn MTLBuffer>, usize)],
        shape: [u32; 8],
        correlations: casa_imaging_reconstruction::runtime_adapter::DeviceCorrelations,
    ) -> Result<Option<MetalStageProfile>, String> {
        #[repr(C)]
        struct Parameters {
            shape: [u32; 8],
            correlations: casa_imaging_reconstruction::runtime_adapter::DeviceCorrelations,
        }
        let parameters = Parameters {
            shape,
            correlations,
        };
        let profile = self
            .stage_profiler
            .as_ref()
            .map(|profiler| profiler.begin(shape))
            .transpose()?;
        for (stage, (pipeline, count)) in [
            (&self.unique_prediction, shape[0]),
            (&self.residual, shape[1]),
        ]
        .into_iter()
        .enumerate()
        {
            if count == 0 {
                continue;
            }
            let encoder = if let Some(profile) = &profile {
                let pass = MTLComputePassDescriptor::computePassDescriptor();
                let attachment =
                    unsafe { pass.sampleBufferAttachments().objectAtIndexedSubscript(0) };
                attachment.setSampleBuffer(Some(&profile.samples));
                unsafe {
                    attachment.setStartOfEncoderSampleIndex(stage * 2);
                    attachment.setEndOfEncoderSampleIndex(stage * 2 + 1);
                }
                command.computeCommandEncoderWithDescriptor(&pass)
            } else {
                command.computeCommandEncoder()
            }
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
        Ok(profile)
    }

    fn encode(
        &self,
        command: &ProtocolObject<dyn MTLCommandBuffer>,
        pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
        buffers: &[(&ProtocolObject<dyn MTLBuffer>, usize)],
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

    #[test]
    fn stage_timestamps_use_calibrated_clock_and_reject_invalid_samples() {
        let clocks = ([1_000, 100], [3_000, 200]);
        let actual =
            calibrated_stage_seconds([110, 120, 130, 150], [true; 2], clocks.0, clocks.1).unwrap();
        assert!((actual[0] - 200e-9).abs() < 1e-15);
        assert!((actual[1] - 400e-9).abs() < 1e-15);
        for ticks in [
            [0, 120, 130, 150],
            [120, 110, 130, 150],
            [110, MTLCounterErrorValue, 130, 150],
        ] {
            assert!(calibrated_stage_seconds(ticks, [true; 2], clocks.0, clocks.1).is_err());
        }
        assert!(
            calibrated_stage_seconds([110, 120, 130, 150], [true; 2], clocks.0, clocks.0).is_err()
        );
        assert_eq!(
            calibrated_stage_seconds(
                [110, 120, MTLCounterErrorValue, MTLCounterErrorValue],
                [true, false],
                clocks.0,
                clocks.1,
            )
            .unwrap()[1],
            0.0,
        );
    }

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
                &command,
                (&sample_buffer, 0),
                (&weight_buffer, 0),
                (&grid_buffer, 0),
                (&predicted_buffer, 0),
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

    #[test]
    #[ignore = "requires a process-accessible Apple Metal device"]
    fn unique_predictions_preserve_tails_phases_and_failure_status() {
        use casa_imaging_reconstruction::runtime_adapter::{
            DeviceCorrelations, ResidualPrediction,
        };
        let device = MTLCreateSystemDefaultDevice().expect("actual Metal device");
        let queue = device.newCommandQueue().unwrap();
        let kernels = MetalCubeKernels::compile(&device).unwrap();
        let weights = [
            0.01_f32, 0.04, 0.12, 0.26, 0.12, 0.04, 0.01, 0.02, 0.05, 0.16, 0.31, 0.16, 0.05, 0.02,
        ];
        let models = (0..3 * 32 * 32)
            .map(|cell| {
                [
                    (cell % 41) as f32 * 0.03125 - 0.5,
                    (cell % 37) as f32 * -0.015625 + 0.25,
                ]
            })
            .collect::<Vec<_>>();
        let dummy = shared_buffer(&device, 16);
        let weight_buffer = shared_buffer(&device, size_of_val(&weights));
        let model_buffer = shared_buffer(&device, size_of_val(models.as_slice()));
        let status_buffer = shared_buffer(&device, size_of::<u32>());
        upload(&weight_buffer, &weights);
        upload(&model_buffer, &models);
        let correlations = DeviceCorrelations {
            coefficients: [[1.0, 0.0], [0.0; 2], [0.0; 2], [0.0; 2]],
            correlations: 1,
            direct: 0,
            padding: [0; 2],
        };
        for count in [1, 3, 4, 7, 8, 9, 31, 33, 137] {
            let requests = (0..count)
                .map(|index| ResidualPrediction {
                    tap: CubeTap {
                        x: index % 26,
                        y: (index * 11) % 26,
                        x_weights: index % 2,
                        y_weights: (index + 1) % 2,
                        value: [0.75, -0.25],
                    },
                    plane: index % 3,
                    padding: 0,
                })
                .collect::<Vec<_>>();
            let request_buffer = shared_buffer(&device, size_of_val(requests.as_slice()));
            let predicted_buffer =
                shared_buffer(&device, (count as usize + 2) * size_of::<[f32; 2]>());
            let sentinel = [-321.0, 123.0];
            let encode = |values: &[ResidualPrediction]| {
                upload(&request_buffer, values);
                upload(&predicted_buffer, &vec![sentinel; count as usize + 2]);
                upload(&status_buffer, &[0_u32]);
                let command = queue.commandBuffer().unwrap();
                let profile = kernels
                    .encode_residual(
                        &command,
                        &[
                            (&request_buffer, 0),
                            (&dummy, 0),
                            (&dummy, 0),
                            (&dummy, 0),
                            (&dummy, 0),
                            (&dummy, 0),
                            (&weight_buffer, 0),
                            (&model_buffer, 0),
                            (&predicted_buffer, 0),
                            (&dummy, 0),
                            (&status_buffer, 0),
                        ],
                        [count, 0, 0, 3, 32, 32, 1, 2],
                        correlations,
                    )
                    .unwrap();
                command.commit();
                command.waitUntilCompleted();
                assert_eq!(command.status(), MTLCommandBufferStatus::Completed);
                if let Some(profile) = profile {
                    let seconds = profile
                        .seconds()
                        .expect("calibrated fenced stage timestamps");
                    assert!(seconds[0] > 0.0);
                    assert_eq!(seconds[1], 0.0);
                }
                // Every host access follows the terminal device fence.
                unsafe { *status_buffer.contents().as_ptr().cast::<u32>() }
            };
            assert_eq!(encode(&requests), 0);
            let predicted = unsafe {
                std::slice::from_raw_parts(
                    predicted_buffer.contents().as_ptr().cast::<[f32; 2]>(),
                    count as usize + 2,
                )
            };
            assert_eq!(&predicted[count as usize..], &[sentinel; 2]);
            for (request, actual) in requests.iter().zip(predicted) {
                let mut expected = [0.0_f32; 2];
                for x in 0..7 {
                    let mut row = [[0.0_f32; 2]; 2];
                    for y in 0..7 {
                        let cell = request.plane as usize * 32 * 32
                            + (request.tap.x as usize + x) * 32
                            + request.tap.y as usize
                            + y;
                        for part in 0..2 {
                            row[y % 2][part] += models[cell][part]
                                * weights[request.tap.y_weights as usize * 7 + y];
                        }
                    }
                    for part in 0..2 {
                        expected[part] += (row[0][part] + row[1][part])
                            * weights[request.tap.x_weights as usize * 7 + x];
                    }
                }
                let phase = request.tap.value;
                let expected = [
                    expected[0] * phase[0] - expected[1] * phase[1],
                    expected[0] * phase[1] + expected[1] * phase[0],
                ];
                for part in 0..2 {
                    assert!((actual[part] - expected[part]).abs() < 2e-6);
                }
            }
            let mut invalid = requests.clone();
            invalid.last_mut().unwrap().tap.x = 26;
            assert_eq!(encode(&invalid), 1);
            let mut nonfinite = models.clone();
            nonfinite[0][0] = f32::NAN;
            upload(&model_buffer, &nonfinite);
            assert_eq!(encode(&requests), 2);
            upload(&model_buffer, &models);
        }
    }
}
