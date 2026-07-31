// SPDX-License-Identifier: LGPL-3.0-or-later
//! Production-inert complete-boundary probe for the VLASS ordered response.

use std::{
    env,
    ffi::c_void,
    fs, mem,
    path::{Path, PathBuf},
    ptr::NonNull,
    slice,
    time::Duration,
};

use objc2::rc::Retained;
use objc2_foundation::{NSArray, NSString};
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder,
    MTLComputeCommandEncoder, MTLComputePipelineState, MTLCreateSystemDefaultDevice, MTLDevice,
    MTLLibrary, MTLResourceOptions, MTLSize,
};
use objc2_metal_performance_shaders::MPSCommandBuffer;
use objc2_metal_performance_shaders_graph::{
    MPSGraphExecutableExecutionDescriptor, MPSGraphTensorData,
};
use serial_test::serial;

use super::*;

const SIDE: usize = 192;
const PIXELS: usize = SIDE * SIDE;
const IMAGING_STATES: usize = 28;
const PREDICTION_STATES: usize = 32;
const ETA_POWERS: usize = 3;
const MODEL_TERMS: usize = 2;
const OUTPUT_TERMS: usize = 2;
const ORDERED_PAIRS: usize = 54;
const W_ORDERS: usize = 3;
const RESPONSE_MOMENTS: usize = 3;
const RESPONSE_COEFFICIENTS: usize = W_ORDERS * RESPONSE_MOMENTS;
const RHS_COEFFICIENTS: usize = W_ORDERS * OUTPUT_TERMS;
const FORWARD_PLANES: usize = PREDICTION_STATES * ETA_POWERS * MODEL_TERMS;
const INVERSE_PLANES: usize = IMAGING_STATES * ETA_POWERS * OUTPUT_TERMS;
const RESPONSE_PLANES: usize = ORDERED_PAIRS * W_ORDERS * RESPONSE_MOMENTS;
const ACTIVE_PIXELS: usize = 7_304;
const MEASURED_APPLICATIONS: usize = 11;
const COMPLEX_BYTES: usize = mem::size_of::<Complex32>();
const FFT_BUFFER_BYTES: usize = FORWARD_PLANES * PIXELS * COMPLEX_BYTES;
const RESPONSE_BYTES: usize = RESPONSE_PLANES * PIXELS * COMPLEX_BYTES;
const LEFT_FACTOR_BYTES: usize = INVERSE_PLANES * PIXELS * COMPLEX_BYTES;
const FEEDBACK_BYTES: usize = ACTIVE_PIXELS * OUTPUT_TERMS * COMPLEX_BYTES;

const ORDERED_RESPONSE_SHADER: &str = r#"
#include <metal_stdlib>
using namespace metal;

struct LinearParams {
    uint elements;
    uint plane_elements;
    uint _pad0;
    uint _pad1;
};

struct MixerAction {
    uint input_plane;
    uint kernel_plane;
    float coefficient;
    uint _pad0;
};

struct MixerParams {
    uint pixels;
    uint output_planes;
    uint _pad0;
    uint _pad1;
};

struct ReductionParams {
    uint pixels;
    uint active_pixels;
    uint imaging_states;
    uint eta_powers;
};

struct ConstructionParams {
    uint side;
    uint states;
    uint coefficients;
    uint support_width;
    uint oversampling;
    uint offset_bias;
    uint group_count;
    uint _pad0;
};

struct SamplePoint {
    uint state;
    uint x;
    uint y;
    uint _pad0;
};

struct GatherParams {
    uint side;
    uint coefficients;
    uint samples;
    uint _pad0;
};

static inline float2 complex_multiply(float2 left, float2 right) {
    return float2(
        left.x * right.x - left.y * right.y,
        left.x * right.y + left.y * right.x
    );
}

kernel void prepare_right_planes(
    device float2 *output [[buffer(0)]],
    constant LinearParams &params [[buffer(1)]],
    uint index [[thread_position_in_grid]]
) {
    if (index >= params.elements) {
        return;
    }
    uint plane = index / params.plane_elements;
    float real = float((plane % 13u) + 1u) * 0.0001f;
    float imag = -float(((plane * 7u) % 11u) + 1u) * 0.00005f;
    output[index] = float2(real, imag);
}

kernel void fill_complex_one(
    device float2 *output [[buffer(0)]],
    constant LinearParams &params [[buffer(1)]],
    uint index [[thread_position_in_grid]]
) {
    if (index < params.elements) {
        output[index] = float2(1.0f, 0.0f);
    }
}

kernel void mix_ordered_response(
    device const float2 *forward [[buffer(0)]],
    device const float2 *kernels [[buffer(1)]],
    device const uint *offsets [[buffer(2)]],
    device const MixerAction *actions [[buffer(3)]],
    device float2 *mixed [[buffer(4)]],
    constant MixerParams &params [[buffer(5)]],
    uint2 position [[thread_position_in_grid]]
) {
    uint pixel = position.x;
    uint output_plane = position.y;
    if (pixel >= params.pixels || output_plane >= params.output_planes) {
        return;
    }
    float2 accumulator = float2(0.0f);
    uint begin = offsets[output_plane];
    uint end = offsets[output_plane + 1u];
    for (uint action_index = begin; action_index < end; ++action_index) {
        MixerAction action = actions[action_index];
        float2 input_value =
            forward[ulong(action.input_plane) * ulong(params.pixels) + ulong(pixel)];
        float2 kernel_value =
            kernels[ulong(action.kernel_plane) * ulong(params.pixels) + ulong(pixel)];
        accumulator +=
            action.coefficient * complex_multiply(kernel_value, input_value);
    }
    mixed[ulong(output_plane) * ulong(params.pixels) + ulong(pixel)] = accumulator;
}

kernel void reduce_active_left_response(
    device const float2 *inverse [[buffer(0)]],
    device const float2 *left_factors [[buffer(1)]],
    device const uint *active_indices [[buffer(2)]],
    device float2 *feedback [[buffer(3)]],
    constant ReductionParams &params [[buffer(4)]],
    uint2 position [[thread_position_in_grid]]
) {
    uint active = position.x;
    uint output_term = position.y;
    if (active >= params.active_pixels || output_term >= 2u) {
        return;
    }
    uint pixel = active_indices[active];
    float2 accumulator = float2(0.0f);
    for (uint imaging_state = 0u; imaging_state < params.imaging_states; ++imaging_state) {
        for (uint left_power = 0u; left_power < params.eta_powers; ++left_power) {
            uint plane =
                (imaging_state * params.eta_powers + left_power) * 2u + output_term;
            ulong index = ulong(plane) * ulong(params.pixels) + ulong(pixel);
            accumulator += complex_multiply(left_factors[index], inverse[index]);
        }
    }
    feedback[ulong(output_term) * ulong(params.active_pixels) + ulong(active)] =
        accumulator;
}

kernel void construct_ordered_response_output_owner(
    device const uint *bucket_offsets [[buffer(0)]],
    device const short2 *group_meta [[buffer(1)]],
    device const float2 *group_coefficients [[buffer(2)]],
    device const float *kernel_lut [[buffer(3)]],
    device float2 *output [[buffer(4)]],
    constant ConstructionParams &params [[buffer(5)]],
    uint3 position [[thread_position_in_grid]]
) {
    uint x = position.x;
    uint y = position.y;
    uint state = position.z;
    if (x >= params.side || y >= params.side || state >= params.states) {
        return;
    }
    float2 accumulators[9];
    for (uint coefficient = 0u; coefficient < 9u; ++coefficient) {
        accumulators[coefficient] = float2(0.0f);
    }
    int radius = int(params.support_width >> 1);
    int first_x = max(0, int(x) - radius);
    int last_x = min(int(params.side) - 1, int(x) + radius);
    int first_y = max(0, int(y) - radius);
    int last_y = min(int(params.side) - 1, int(y) + radius);
    ulong pixels = ulong(params.side) * ulong(params.side);
    for (int center_y = first_y; center_y <= last_y; ++center_y) {
        for (int center_x = first_x; center_x <= last_x; ++center_x) {
            ulong bucket =
                ulong(state) * pixels
                + ulong(center_y) * ulong(params.side)
                + ulong(center_x);
            uint begin = bucket_offsets[bucket];
            uint end = bucket_offsets[bucket + 1ul];
            int delta_x = int(x) - center_x;
            int delta_y = int(y) - center_y;
            for (uint group = begin; group < end; ++group) {
                short2 subpixel = group_meta[group];
                uint lut_x =
                    uint(int(subpixel.x) + int(params.offset_bias))
                        * params.support_width
                    + uint(delta_x + radius);
                uint lut_y =
                    uint(int(subpixel.y) + int(params.offset_bias))
                        * params.support_width
                    + uint(delta_y + radius);
                float weight = kernel_lut[lut_x] * kernel_lut[lut_y];
                ulong coefficient_base =
                    ulong(group) * ulong(params.coefficients);
                for (uint coefficient = 0u;
                     coefficient < params.coefficients;
                     ++coefficient) {
                    accumulators[coefficient] +=
                        weight * group_coefficients[coefficient_base + coefficient];
                }
            }
        }
    }
    ulong pixel = ulong(y) * ulong(params.side) + ulong(x);
    for (uint coefficient = 0u;
         coefficient < params.coefficients;
         ++coefficient) {
        ulong plane = ulong(state) * ulong(params.coefficients) + coefficient;
        output[plane * pixels + pixel] = accumulators[coefficient];
    }
}

kernel void gather_ordered_response_samples(
    device const float2 *input [[buffer(0)]],
    device const SamplePoint *points [[buffer(1)]],
    device float2 *samples [[buffer(2)]],
    constant GatherParams &params [[buffer(3)]],
    uint2 position [[thread_position_in_grid]]
) {
    uint sample = position.x;
    uint coefficient = position.y;
    if (sample >= params.samples || coefficient >= params.coefficients) {
        return;
    }
    SamplePoint point = points[sample];
    ulong pixels = ulong(params.side) * ulong(params.side);
    ulong plane =
        ulong(point.state) * ulong(params.coefficients) + ulong(coefficient);
    ulong pixel = ulong(point.y) * ulong(params.side) + ulong(point.x);
    samples[ulong(sample) * ulong(params.coefficients) + ulong(coefficient)] =
        input[plane * pixels + pixel];
}
"#;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct LinearParams {
    elements: u32,
    plane_elements: u32,
    _pad0: u32,
    _pad1: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MixerAction {
    input_plane: u32,
    kernel_plane: u32,
    coefficient: f32,
    _pad0: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct MixerParams {
    pixels: u32,
    output_planes: u32,
    _pad0: u32,
    _pad1: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ReductionParams {
    pixels: u32,
    active_pixels: u32,
    imaging_states: u32,
    eta_powers: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct ConstructionParams {
    side: u32,
    states: u32,
    coefficients: u32,
    support_width: u32,
    oversampling: u32,
    offset_bias: u32,
    group_count: u32,
    _pad0: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct SamplePoint {
    state: u32,
    x: u32,
    y: u32,
    _pad0: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct GatherParams {
    side: u32,
    coefficients: u32,
    samples: u32,
    _pad0: u32,
}

#[derive(Clone)]
struct OrderedResponsePipelines {
    prepare: MetalComputePipeline,
    fill_one: MetalComputePipeline,
    mix: MetalComputePipeline,
    reduce: MetalComputePipeline,
    construct: MetalComputePipeline,
    gather: MetalComputePipeline,
}

#[derive(Clone, Copy, Debug)]
struct OrderedResponseSample {
    encode: Duration,
    commit_to_completion: Duration,
    total: Duration,
    device: Duration,
}

fn checked_u32(value: usize, context: &str) -> u32 {
    u32::try_from(value).unwrap_or_else(|_| panic!("{context} exceeds u32"))
}

fn input_plane_value(plane: usize) -> Complex32 {
    Complex32::new(
        ((plane % 13) + 1) as f32 * 0.0001,
        -((((plane * 7) % 11) + 1) as f32 * 0.00005),
    )
}

fn binomial(order: usize, left_power: usize) -> f32 {
    match (order, left_power) {
        (0, 0) | (1, 0) | (1, 1) | (2, 0) | (2, 2) => 1.0,
        (2, 1) => 2.0,
        _ => panic!("unsupported total-order-two binomial"),
    }
}

fn ordered_response_actions() -> (Vec<u32>, Vec<MixerAction>, [Complex32; OUTPUT_TERMS]) {
    let mut grouped = vec![Vec::new(); INVERSE_PLANES];
    for pair in 0..ORDERED_PAIRS {
        let imaging_state = pair % IMAGING_STATES;
        let prediction_state = (pair * 17 + pair / IMAGING_STATES) % PREDICTION_STATES;
        for output_term in 0..OUTPUT_TERMS {
            for model_term in 0..MODEL_TERMS {
                let moment = output_term + model_term;
                for order in 0..W_ORDERS {
                    for left_power in 0..=order {
                        let right_power = order - left_power;
                        let output_plane =
                            (imaging_state * ETA_POWERS + left_power) * OUTPUT_TERMS + output_term;
                        let input_plane = (prediction_state * ETA_POWERS + right_power)
                            * MODEL_TERMS
                            + model_term;
                        let kernel_plane = (pair * W_ORDERS + order) * RESPONSE_MOMENTS + moment;
                        grouped[output_plane].push(MixerAction {
                            input_plane: checked_u32(input_plane, "input plane"),
                            kernel_plane: checked_u32(kernel_plane, "kernel plane"),
                            coefficient: binomial(order, left_power),
                            _pad0: 0,
                        });
                    }
                }
            }
        }
    }

    let mut offsets = Vec::with_capacity(INVERSE_PLANES + 1);
    let mut actions = Vec::with_capacity(1_296);
    let mut expected = [Complex32::new(0.0, 0.0); OUTPUT_TERMS];
    offsets.push(0);
    for (output_plane, output_actions) in grouped.into_iter().enumerate() {
        let output_term = output_plane % OUTPUT_TERMS;
        for action in &output_actions {
            expected[output_term] +=
                action.coefficient * input_plane_value(action.input_plane as usize);
        }
        actions.extend(output_actions);
        offsets.push(checked_u32(actions.len(), "action offset"));
    }
    assert_eq!(actions.len(), 1_296);
    assert_eq!(offsets.len(), INVERSE_PLANES + 1);
    (offsets, actions, expected)
}

fn make_pipeline(
    device: &MetalDevice,
    library: &Retained<ProtocolObject<dyn MTLLibrary>>,
    name: &str,
) -> MetalComputePipeline {
    let function = library
        .newFunctionWithName(&NSString::from_str(name))
        .unwrap_or_else(|| panic!("missing ordered-response function {name}"));
    device
        .newComputePipelineStateWithFunction_error(&function)
        .unwrap_or_else(|_| panic!("cannot compile ordered-response pipeline {name}"))
}

fn make_pipelines(device: &MetalDevice) -> OrderedResponsePipelines {
    let library = device
        .newLibraryWithSource_options_error(&NSString::from_str(ORDERED_RESPONSE_SHADER), None)
        .expect("compile ordered-response Metal library");
    OrderedResponsePipelines {
        prepare: make_pipeline(device, &library, "prepare_right_planes"),
        fill_one: make_pipeline(device, &library, "fill_complex_one"),
        mix: make_pipeline(device, &library, "mix_ordered_response"),
        reduce: make_pipeline(device, &library, "reduce_active_left_response"),
        construct: make_pipeline(device, &library, "construct_ordered_response_output_owner"),
        gather: make_pipeline(device, &library, "gather_ordered_response_samples"),
    }
}

fn set_bytes<T>(encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>, value: &T, index: usize) {
    let pointer = NonNull::new((value as *const T).cast_mut().cast())
        .expect("ordered-response parameter pointer");
    unsafe {
        encoder.setBytes_length_atIndex(pointer, mem::size_of::<T>(), index);
    }
}

fn dispatch_linear(
    encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
    pipeline: &ProtocolObject<dyn MTLComputePipelineState>,
    elements: usize,
) {
    let width = pipeline
        .threadExecutionWidth()
        .max(1)
        .min(pipeline.maxTotalThreadsPerThreadgroup().max(1));
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: elements,
            height: 1,
            depth: 1,
        },
        MTLSize {
            width,
            height: 1,
            depth: 1,
        },
    );
}

fn initialize_resident_constants(
    queue: &MetalQueue,
    pipelines: &OrderedResponsePipelines,
    response: &MetalBuffer,
    left_factors: &MetalBuffer,
) -> Duration {
    let started = Instant::now();
    let command_buffer = queue
        .commandBuffer()
        .expect("constant initialization command");
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("constant initialization encoder");
    encoder.setComputePipelineState(&pipelines.fill_one);
    for (buffer, elements) in [
        (response, RESPONSE_PLANES * PIXELS),
        (left_factors, INVERSE_PLANES * PIXELS),
    ] {
        let params = LinearParams {
            elements: checked_u32(elements, "constant element count"),
            plane_elements: checked_u32(PIXELS, "plane elements"),
            _pad0: 0,
            _pad1: 0,
        };
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(buffer), 0, 0);
        }
        set_bytes(&encoder, &params, 1);
        dispatch_linear(&encoder, &pipelines.fill_one, elements);
    }
    encoder.endEncoding();
    command_buffer.commit();
    command_buffer.waitUntilCompleted();
    assert_ne!(
        command_buffer.status(),
        MTLCommandBufferStatus::Error,
        "ordered-response constant initialization failed"
    );
    started.elapsed()
}

fn encode_prepare(
    command_buffer: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    output: &MetalBuffer,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("right-plane preparation encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(output), 0, 0);
    }
    let params = LinearParams {
        elements: checked_u32(FORWARD_PLANES * PIXELS, "right-plane element count"),
        plane_elements: checked_u32(PIXELS, "right-plane pixel count"),
        _pad0: 0,
        _pad1: 0,
    };
    set_bytes(&encoder, &params, 1);
    dispatch_linear(&encoder, pipeline, FORWARD_PLANES * PIXELS);
    encoder.endEncoding();
}

#[allow(clippy::too_many_arguments)]
fn encode_mixer(
    command_buffer: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    forward: &MetalBuffer,
    response: &MetalBuffer,
    offsets: &MetalBuffer,
    actions: &MetalBuffer,
    mixed: &MetalBuffer,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("ordered-response mixer encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(forward), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(response), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(offsets), 0, 2);
        encoder.setBuffer_offset_atIndex(Some(actions), 0, 3);
        encoder.setBuffer_offset_atIndex(Some(mixed), 0, 4);
    }
    let params = MixerParams {
        pixels: checked_u32(PIXELS, "mixer pixels"),
        output_planes: checked_u32(INVERSE_PLANES, "mixer output planes"),
        _pad0: 0,
        _pad1: 0,
    };
    set_bytes(&encoder, &params, 5);
    let (group_width, group_height) = threadgroup_2d(pipeline, PIXELS, INVERSE_PLANES);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: PIXELS,
            height: INVERSE_PLANES,
            depth: 1,
        },
        MTLSize {
            width: group_width,
            height: group_height,
            depth: 1,
        },
    );
    encoder.endEncoding();
}

#[allow(clippy::too_many_arguments)]
fn encode_reduction(
    command_buffer: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    inverse: &MetalBuffer,
    left_factors: &MetalBuffer,
    active_indices: &MetalBuffer,
    feedback: &MetalBuffer,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("ordered-response reduction encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(inverse), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(left_factors), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(active_indices), 0, 2);
        encoder.setBuffer_offset_atIndex(Some(feedback), 0, 3);
    }
    let params = ReductionParams {
        pixels: checked_u32(PIXELS, "reduction pixels"),
        active_pixels: checked_u32(ACTIVE_PIXELS, "active pixels"),
        imaging_states: checked_u32(IMAGING_STATES, "imaging states"),
        eta_powers: checked_u32(ETA_POWERS, "eta powers"),
    };
    set_bytes(&encoder, &params, 4);
    let (group_width, group_height) = threadgroup_2d(pipeline, ACTIVE_PIXELS, OUTPUT_TERMS);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: ACTIVE_PIXELS,
            height: OUTPUT_TERMS,
            depth: 1,
        },
        MTLSize {
            width: group_width,
            height: group_height,
            depth: 1,
        },
    );
    encoder.endEncoding();
}

#[allow(clippy::too_many_arguments)]
fn execute_ordered_response(
    queue: &MetalQueue,
    pipelines: &OrderedResponsePipelines,
    forward: &ResidentFftExecutable,
    inverse: &ResidentFftExecutable,
    buffer_a: &MetalBuffer,
    buffer_b: &MetalBuffer,
    response: &MetalBuffer,
    left_factors: &MetalBuffer,
    action_offsets: &MetalBuffer,
    actions: &MetalBuffer,
    active_indices: &MetalBuffer,
    feedback: &MetalBuffer,
    forward_inputs: &NSArray<MPSGraphTensorData>,
    forward_results: &NSArray<MPSGraphTensorData>,
    inverse_inputs: &NSArray<MPSGraphTensorData>,
    inverse_results: &NSArray<MPSGraphTensorData>,
    execution: &MPSGraphExecutableExecutionDescriptor,
) -> OrderedResponseSample {
    let total_started = Instant::now();
    let command_buffer = unsafe { MPSCommandBuffer::commandBufferFromCommandQueue(queue) };
    let initial_root = unsafe { command_buffer.rootCommandBuffer() };
    let encode_started = Instant::now();

    encode_prepare(&initial_root, &pipelines.prepare, buffer_a);
    let _forward_result = unsafe {
        forward
            .executable
            .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                &command_buffer,
                forward_inputs,
                Some(forward_results),
                Some(execution),
            )
    };
    let after_forward = unsafe { command_buffer.rootCommandBuffer() };
    encode_mixer(
        &after_forward,
        &pipelines.mix,
        buffer_b,
        response,
        action_offsets,
        actions,
        buffer_a,
    );
    let _inverse_result = unsafe {
        inverse
            .executable
            .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                &command_buffer,
                inverse_inputs,
                Some(inverse_results),
                Some(execution),
            )
    };
    let after_inverse = unsafe { command_buffer.rootCommandBuffer() };
    encode_reduction(
        &after_inverse,
        &pipelines.reduce,
        buffer_b,
        left_factors,
        active_indices,
        feedback,
    );
    let final_root = unsafe { command_buffer.rootCommandBuffer() };
    let encode = encode_started.elapsed();
    assert!(
        std::ptr::eq(&*initial_root, &*after_forward)
            && std::ptr::eq(&*initial_root, &*after_inverse)
            && std::ptr::eq(&*initial_root, &*final_root),
        "MPSGraph committed-and-continued inside the ordered-response boundary"
    );

    let completion_started = Instant::now();
    final_root.commit();
    final_root.waitUntilCompleted();
    let commit_to_completion = completion_started.elapsed();
    assert_ne!(
        final_root.status(),
        MTLCommandBufferStatus::Error,
        "ordered-response command failed"
    );
    let gpu_start = final_root.GPUStartTime();
    let gpu_end = final_root.GPUEndTime();
    assert!(
        gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start,
        "ordered-response command lacks a complete GPU interval"
    );
    OrderedResponseSample {
        encode,
        commit_to_completion,
        total: total_started.elapsed(),
        device: Duration::from_secs_f64(gpu_end - gpu_start),
    }
}

fn shared_slice<T>(buffer: &MetalBuffer, count: usize) -> &[T] {
    unsafe { slice::from_raw_parts(buffer.contents().as_ptr().cast::<T>(), count) }
}

fn validate_feedback(feedback: &MetalBuffer, expected: [Complex32; OUTPUT_TERMS]) -> f32 {
    let values = shared_slice::<Complex32>(feedback, ACTIVE_PIXELS * OUTPUT_TERMS);
    let mut worst_relative = 0.0_f32;
    for output_term in 0..OUTPUT_TERMS {
        let reference = expected[output_term];
        for &value in &values[output_term * ACTIVE_PIXELS..(output_term + 1) * ACTIVE_PIXELS] {
            worst_relative = worst_relative
                .max((value - reference).norm() / reference.norm().max(f32::MIN_POSITIVE));
        }
    }
    assert!(
        worst_relative <= 5.0e-4,
        "ordered-response GPU result differs from the synthetic exact contraction: {worst_relative}"
    );
    worst_relative
}

fn construction_artifact_dir() -> PathBuf {
    env::var_os("CASA_RS_VLASS_ORDERED_RESPONSE_CONSTRUCTION_DIR")
        .map(PathBuf::from)
        .expect("set CASA_RS_VLASS_ORDERED_RESPONSE_CONSTRUCTION_DIR to the v2 artifact")
}

fn artifact_bytes(directory: &Path, name: &str) -> Vec<u8> {
    let path = directory.join(name);
    fs::read(&path).unwrap_or_else(|error| panic!("cannot read {}: {error}", path.display()))
}

fn little_u32_at(values: &[u8], index: usize) -> u32 {
    let offset = index * mem::size_of::<u32>();
    u32::from_le_bytes(
        values[offset..offset + mem::size_of::<u32>()]
            .try_into()
            .expect("complete little-endian u32"),
    )
}

fn validate_group_artifacts(
    offsets: &[u8],
    meta: &[u8],
    coefficients: &[u8],
    states: usize,
    coefficient_count: usize,
) -> usize {
    assert_eq!(offsets.len(), (states * PIXELS + 1) * mem::size_of::<u32>());
    assert_eq!(meta.len() % 4, 0);
    let groups = meta.len() / 4;
    assert_eq!(
        coefficients.len(),
        groups * coefficient_count * COMPLEX_BYTES
    );
    assert_eq!(little_u32_at(offsets, states * PIXELS) as usize, groups);
    for encoded in meta.chunks_exact(mem::size_of::<i16>()) {
        let offset = i16::from_le_bytes(encoded.try_into().expect("complete i16"));
        assert!(
            (-50..=50).contains(&offset),
            "subpixel offset {offset} is outside the rounded 100x grid"
        );
    }
    groups
}

fn controlled_kernel_lut() -> Vec<f32> {
    (-50_i32..=50)
        .flat_map(|offset| {
            (-3_i32..=3).map(move |delta| {
                let relative = f64::from(delta) + f64::from(offset) / 100.0;
                (-0.5 * (relative / 1.15).powi(2)).exp() as f32
            })
        })
        .collect()
}

fn no_copy_bytes(device: &MetalDevice, values: &[u8]) -> MetalBuffer {
    let pointer = NonNull::new(values.as_ptr().cast::<c_void>().cast_mut())
        .expect("non-empty construction artifact");
    unsafe {
        device
            .newBufferWithBytesNoCopy_length_options_deallocator(
                pointer,
                values.len(),
                MTLResourceOptions::StorageModeShared,
                None,
            )
            .expect("wrap construction artifact in a shared Metal buffer")
    }
}

#[allow(clippy::too_many_arguments)]
fn encode_output_owner_construction(
    command_buffer: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    bucket_offsets: &MetalBuffer,
    group_meta: &MetalBuffer,
    group_coefficients: &MetalBuffer,
    kernel_lut: &MetalBuffer,
    output: &MetalBuffer,
    states: usize,
    coefficient_count: usize,
    group_count: usize,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("output-owner construction encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(bucket_offsets), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(group_meta), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(group_coefficients), 0, 2);
        encoder.setBuffer_offset_atIndex(Some(kernel_lut), 0, 3);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 4);
    }
    let params = ConstructionParams {
        side: checked_u32(SIDE, "construction side"),
        states: checked_u32(states, "construction states"),
        coefficients: checked_u32(coefficient_count, "construction coefficients"),
        support_width: 7,
        oversampling: 100,
        offset_bias: 50,
        group_count: checked_u32(group_count, "construction groups"),
        _pad0: 0,
    };
    set_bytes(&encoder, &params, 5);
    let (group_width, group_height) = threadgroup_2d(pipeline, SIDE, SIDE);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: SIDE,
            height: SIDE,
            depth: states,
        },
        MTLSize {
            width: group_width,
            height: group_height,
            depth: 1,
        },
    );
    encoder.endEncoding();
}

fn sample_references(
    manifest: &serde_json::Value,
    family: &str,
    coefficient_count: usize,
) -> (Vec<SamplePoint>, Vec<Complex64>) {
    let entries = manifest["sampled_f64_output"][family]
        .as_array()
        .unwrap_or_else(|| panic!("manifest lacks {family} f64 samples"));
    let mut points = Vec::with_capacity(entries.len());
    let mut expected = Vec::with_capacity(entries.len() * coefficient_count);
    for entry in entries {
        let state = entry["state"].as_u64().expect("sample state") as usize;
        let x = entry["x"].as_u64().expect("sample x") as usize;
        let y = entry["y"].as_u64().expect("sample y") as usize;
        assert!(x < SIDE && y < SIDE);
        points.push(SamplePoint {
            state: checked_u32(state, "sample state"),
            x: checked_u32(x, "sample x"),
            y: checked_u32(y, "sample y"),
            _pad0: 0,
        });
        let values = entry["values"].as_array().expect("sample values");
        assert_eq!(values.len(), coefficient_count);
        for value in values {
            let parts = value.as_array().expect("complex sample pair");
            assert_eq!(parts.len(), 2);
            expected.push(Complex64::new(
                parts[0].as_f64().expect("sample real"),
                parts[1].as_f64().expect("sample imaginary"),
            ));
        }
    }
    (points, expected)
}

fn gather_constructed_samples(
    device: &MetalDevice,
    queue: &MetalQueue,
    pipeline: &MetalComputePipeline,
    input: &MetalBuffer,
    points: &[SamplePoint],
    coefficient_count: usize,
) -> (Vec<Complex32>, Duration) {
    let point_buffer = buffer_from_slice_no_copy(device, points).expect("sample-point buffer");
    let sample_count = points.len() * coefficient_count;
    let output = device
        .newBufferWithLength_options(
            sample_count * COMPLEX_BYTES,
            MTLResourceOptions::StorageModeShared,
        )
        .expect("sample readback buffer");
    let started = Instant::now();
    let command_buffer = queue.commandBuffer().expect("sample gather command");
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("sample gather encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(input), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(&point_buffer), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(&output), 0, 2);
    }
    let params = GatherParams {
        side: checked_u32(SIDE, "gather side"),
        coefficients: checked_u32(coefficient_count, "gather coefficients"),
        samples: checked_u32(points.len(), "gather samples"),
        _pad0: 0,
    };
    set_bytes(&encoder, &params, 3);
    let (group_width, group_height) = threadgroup_2d(pipeline, points.len(), coefficient_count);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: points.len(),
            height: coefficient_count,
            depth: 1,
        },
        MTLSize {
            width: group_width,
            height: group_height,
            depth: 1,
        },
    );
    encoder.endEncoding();
    command_buffer.commit();
    command_buffer.waitUntilCompleted();
    assert_ne!(
        command_buffer.status(),
        MTLCommandBufferStatus::Error,
        "sample gather failed"
    );
    (
        shared_slice::<Complex32>(&output, sample_count).to_vec(),
        started.elapsed(),
    )
}

fn sampled_construction_error(
    actual: &[Complex32],
    expected: &[Complex64],
    coefficient_count: usize,
) -> (f64, f64) {
    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual.len() % coefficient_count, 0);
    let mut worst_l2 = 0.0_f64;
    let mut worst_linf = 0.0_f64;
    for coefficient in 0..coefficient_count {
        let mut reference_l2 = 0.0_f64;
        let mut difference_l2 = 0.0_f64;
        let mut reference_linf = 0.0_f64;
        let mut difference_linf = 0.0_f64;
        for sample in 0..(actual.len() / coefficient_count) {
            let index = sample * coefficient_count + coefficient;
            let reference = expected[index];
            let candidate =
                Complex64::new(f64::from(actual[index].re), f64::from(actual[index].im));
            let difference = (candidate - reference).norm();
            reference_l2 += reference.norm_sqr();
            difference_l2 += difference * difference;
            reference_linf = reference_linf.max(reference.norm());
            difference_linf = difference_linf.max(difference);
        }
        worst_l2 = worst_l2.max(difference_l2.sqrt() / reference_l2.sqrt().max(f64::MIN_POSITIVE));
        worst_linf = worst_linf.max(difference_linf / reference_linf.max(f64::MIN_POSITIVE));
    }
    assert!(
        worst_l2 <= 2.0e-4 && worst_linf <= 3.0e-4,
        "output-owner construction differs from sampled f64 sums: l2={worst_l2} linf={worst_linf}"
    );
    (worst_l2, worst_linf)
}

#[test]
#[serial]
#[ignore = "production-inert VLASS complete resident ordered-response gate"]
fn vlass_complete_private_resident_ordered_response_probe() {
    assert!(
        mpsgraph_f32_available(),
        "VLASS ordered-response gate requires a visible Metal device"
    );
    assert_eq!(FORWARD_PLANES, 192);
    assert_eq!(INVERSE_PLANES, 168);
    assert_eq!(RESPONSE_PLANES, 486);
    assert_eq!(FFT_BUFFER_BYTES, 56_623_104);
    assert_eq!(RESPONSE_BYTES, 143_327_232);
    assert_eq!(LEFT_FACTOR_BYTES, 49_545_216);
    assert_eq!(FEEDBACK_BYTES, 116_864);

    let device = MTLCreateSystemDefaultDevice().expect("default Metal device");
    let queue = device.newCommandQueue().expect("Metal command queue");
    let graph_device = unsafe { MPSGraphDevice::deviceWithMTLDevice(&device) };
    let (forward, forward_compile) = compile_resident_fft_executable(
        &device,
        &graph_device,
        FORWARD_PLANES,
        FftDirection::Forward,
    );
    let (inverse, inverse_compile) = compile_resident_fft_executable(
        &device,
        &graph_device,
        INVERSE_PLANES,
        FftDirection::Inverse,
    );
    let pipeline_compile_started = Instant::now();
    let pipelines = make_pipelines(&device);
    let pipeline_compile = pipeline_compile_started.elapsed();

    let buffer_a = device
        .newBufferWithLength_options(FFT_BUFFER_BYTES, MTLResourceOptions::StorageModePrivate)
        .expect("ordered-response private buffer A");
    let buffer_b = device
        .newBufferWithLength_options(FFT_BUFFER_BYTES, MTLResourceOptions::StorageModePrivate)
        .expect("ordered-response private buffer B");
    let response = device
        .newBufferWithLength_options(RESPONSE_BYTES, MTLResourceOptions::StorageModePrivate)
        .expect("ordered-response private response bank");
    let left_factors = device
        .newBufferWithLength_options(LEFT_FACTOR_BYTES, MTLResourceOptions::StorageModePrivate)
        .expect("ordered-response private left factors");
    let feedback = device
        .newBufferWithLength_options(FEEDBACK_BYTES, MTLResourceOptions::StorageModeShared)
        .expect("ordered-response feedback buffer");

    let (offset_values, action_values, expected) = ordered_response_actions();
    let active_values: Vec<u32> = (0..ACTIVE_PIXELS)
        .map(|index| checked_u32(index * PIXELS / ACTIVE_PIXELS, "active pixel index"))
        .collect();
    let action_offsets =
        buffer_from_slice_no_copy(&device, &offset_values).expect("action-offset buffer");
    let actions = buffer_from_slice_no_copy(&device, &action_values).expect("action buffer");
    let active_indices =
        buffer_from_slice_no_copy(&device, &active_values).expect("active-index buffer");

    let constant_initialization =
        initialize_resident_constants(&queue, &pipelines, &response, &left_factors);

    let forward_shape = shape_array_batch(FORWARD_PLANES, SIDE, SIDE);
    let inverse_shape = shape_array_batch(INVERSE_PLANES, SIDE, SIDE);
    let forward_input = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &buffer_a,
            &forward_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let forward_output = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &buffer_b,
            &forward_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let inverse_input = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &buffer_a,
            &inverse_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let inverse_output = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &buffer_b,
            &inverse_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let forward_inputs = NSArray::from_slice(&[&*forward_input]);
    let forward_results = NSArray::from_slice(&[&*forward_output]);
    let inverse_inputs = NSArray::from_slice(&[&*inverse_input]);
    let inverse_results = NSArray::from_slice(&[&*inverse_output]);
    let execution = unsafe { MPSGraphExecutableExecutionDescriptor::new() };
    unsafe {
        execution.setWaitUntilCompleted(false);
    }

    let first_use_started = Instant::now();
    let warmup = execute_ordered_response(
        &queue,
        &pipelines,
        &forward,
        &inverse,
        &buffer_a,
        &buffer_b,
        &response,
        &left_factors,
        &action_offsets,
        &actions,
        &active_indices,
        &feedback,
        &forward_inputs,
        &forward_results,
        &inverse_inputs,
        &inverse_results,
        &execution,
    );
    let first_use = first_use_started.elapsed();
    let warmup_error = validate_feedback(&feedback, expected);

    let samples: Vec<_> = (0..MEASURED_APPLICATIONS)
        .map(|_| {
            execute_ordered_response(
                &queue,
                &pipelines,
                &forward,
                &inverse,
                &buffer_a,
                &buffer_b,
                &response,
                &left_factors,
                &action_offsets,
                &actions,
                &active_indices,
                &feedback,
                &forward_inputs,
                &forward_results,
                &inverse_inputs,
                &inverse_results,
                &execution,
            )
        })
        .collect();
    let final_error = validate_feedback(&feedback, expected);

    let total: Vec<_> = samples.iter().map(|sample| sample.total).collect();
    let completion: Vec<_> = samples
        .iter()
        .map(|sample| sample.commit_to_completion)
        .collect();
    let device_times: Vec<_> = samples.iter().map(|sample| sample.device).collect();
    let encode: Vec<_> = samples.iter().map(|sample| sample.encode).collect();
    let total_p50 = duration_nearest_rank(&total, 5, 10);
    let total_p90 = duration_nearest_rank(&total, 9, 10);
    let completion_p50 = duration_nearest_rank(&completion, 5, 10);
    let completion_p90 = duration_nearest_rank(&completion, 9, 10);
    let device_p50 = duration_nearest_rank(&device_times, 5, 10);
    let device_p90 = duration_nearest_rank(&device_times, 9, 10);
    let encode_p50 = duration_nearest_rank(&encode, 5, 10);
    let encode_p90 = duration_nearest_rank(&encode, 9, 10);
    let trajectory_total: Duration = total.iter().sum();
    let resident_bytes = 2 * FFT_BUFFER_BYTES
        + RESPONSE_BYTES
        + LEFT_FACTOR_BYTES
        + FEEDBACK_BYTES
        + mem::size_of_val(offset_values.as_slice())
        + mem::size_of_val(action_values.as_slice())
        + mem::size_of_val(active_values.as_slice());

    eprintln!(
        "vlass_complete_private_resident_ordered_response_probe \
         forward_compile_s={:.9} inverse_compile_s={:.9} pipeline_compile_s={:.9} \
         constant_initialization_s={:.9} first_use_s={:.9} \
         warmup_total_s={:.9} warmup_device_s={:.9} \
         measured_applications={} logical_actions_per_application={} \
         resident_bytes={} warmup_relative_error={:.9e} final_relative_error={:.9e} \
         total_p50_s={:.9} total_p90_s={:.9} trajectory_total_s={:.9} \
         commit_to_completion_p50_s={:.9} commit_to_completion_p90_s={:.9} \
         device_p50_s={:.9} device_p90_s={:.9} \
         encode_p50_s={:.9} encode_p90_s={:.9}",
        forward_compile.as_secs_f64(),
        inverse_compile.as_secs_f64(),
        pipeline_compile.as_secs_f64(),
        constant_initialization.as_secs_f64(),
        first_use.as_secs_f64(),
        warmup.total.as_secs_f64(),
        warmup.device.as_secs_f64(),
        MEASURED_APPLICATIONS,
        action_values.len(),
        resident_bytes,
        warmup_error,
        final_error,
        total_p50.as_secs_f64(),
        total_p90.as_secs_f64(),
        trajectory_total.as_secs_f64(),
        completion_p50.as_secs_f64(),
        completion_p90.as_secs_f64(),
        device_p50.as_secs_f64(),
        device_p90.as_secs_f64(),
        encode_p50.as_secs_f64(),
        encode_p90.as_secs_f64(),
    );
    eprintln!(
        "vlass_complete_private_resident_ordered_response_samples \
         total_s={:?} device_s={:?} encode_s={:?}",
        total.iter().map(Duration::as_secs_f64).collect::<Vec<_>>(),
        device_times
            .iter()
            .map(Duration::as_secs_f64)
            .collect::<Vec<_>>(),
        encode.iter().map(Duration::as_secs_f64).collect::<Vec<_>>(),
    );
}

#[test]
#[serial]
#[ignore = "production-inert VLASS real-route output-owner construction gate"]
fn vlass_real_route_output_owner_construction_probe() {
    assert!(
        mpsgraph_f32_available(),
        "VLASS output-owner construction gate requires a visible Metal device"
    );
    let artifact_dir = construction_artifact_dir();
    let read_started = Instant::now();
    let manifest_bytes = artifact_bytes(&artifact_dir, "manifest.json");
    let manifest: serde_json::Value =
        serde_json::from_slice(&manifest_bytes).expect("construction manifest JSON");
    assert_eq!(
        manifest["schema"].as_str(),
        Some("casa-rs-vlass-ordered-response-segmented-construction/v1")
    );
    let response_offsets = artifact_bytes(&artifact_dir, "response-bucket-offsets-u32-le.bin");
    let response_meta = artifact_bytes(&artifact_dir, "response-group-meta-i16-le.bin");
    let response_coefficients =
        artifact_bytes(&artifact_dir, "response-group-coefficients-c64-le.bin");
    let rhs_offsets = artifact_bytes(&artifact_dir, "rhs-bucket-offsets-u32-le.bin");
    let rhs_meta = artifact_bytes(&artifact_dir, "rhs-group-meta-i16-le.bin");
    let rhs_coefficients = artifact_bytes(&artifact_dir, "rhs-group-coefficients-c64-le.bin");
    let artifact_read = read_started.elapsed();

    let response_groups = validate_group_artifacts(
        &response_offsets,
        &response_meta,
        &response_coefficients,
        ORDERED_PAIRS,
        RESPONSE_COEFFICIENTS,
    );
    let rhs_groups = validate_group_artifacts(
        &rhs_offsets,
        &rhs_meta,
        &rhs_coefficients,
        IMAGING_STATES,
        RHS_COEFFICIENTS,
    );
    assert_eq!(response_groups, 372_870);
    assert_eq!(rhs_groups, 370_650);
    let (response_points, response_expected) =
        sample_references(&manifest, "response", RESPONSE_COEFFICIENTS);
    let (rhs_points, rhs_expected) = sample_references(&manifest, "rhs", RHS_COEFFICIENTS);
    assert_eq!(response_points.len(), 4 * ORDERED_PAIRS);
    assert_eq!(rhs_points.len(), 4 * IMAGING_STATES);

    let device = MTLCreateSystemDefaultDevice().expect("default Metal device");
    let queue = device.newCommandQueue().expect("Metal command queue");
    let pipeline_compile_started = Instant::now();
    let pipelines = make_pipelines(&device);
    let pipeline_compile = pipeline_compile_started.elapsed();

    let wrap_started = Instant::now();
    let response_offsets_buffer = no_copy_bytes(&device, &response_offsets);
    let response_meta_buffer = no_copy_bytes(&device, &response_meta);
    let response_coefficients_buffer = no_copy_bytes(&device, &response_coefficients);
    let rhs_offsets_buffer = no_copy_bytes(&device, &rhs_offsets);
    let rhs_meta_buffer = no_copy_bytes(&device, &rhs_meta);
    let rhs_coefficients_buffer = no_copy_bytes(&device, &rhs_coefficients);
    let kernel_values = controlled_kernel_lut();
    assert_eq!(kernel_values.len(), 101 * 7);
    let kernel_buffer =
        buffer_from_slice_no_copy(&device, &kernel_values).expect("construction kernel LUT");
    let input_wrap = wrap_started.elapsed();

    let response_output_bytes = RESPONSE_PLANES * PIXELS * COMPLEX_BYTES;
    let rhs_output_bytes = IMAGING_STATES * RHS_COEFFICIENTS * PIXELS * COMPLEX_BYTES;
    assert_eq!(response_output_bytes, 143_327_232);
    assert_eq!(rhs_output_bytes, 49_545_216);
    let allocation_started = Instant::now();
    let response_output = device
        .newBufferWithLength_options(
            response_output_bytes,
            MTLResourceOptions::StorageModePrivate,
        )
        .expect("private response output");
    let rhs_output = device
        .newBufferWithLength_options(rhs_output_bytes, MTLResourceOptions::StorageModePrivate)
        .expect("private RHS output");
    let output_allocation = allocation_started.elapsed();

    let total_started = Instant::now();
    let command_buffer = queue
        .commandBuffer()
        .expect("output-owner construction command");
    let encode_started = Instant::now();
    encode_output_owner_construction(
        &command_buffer,
        &pipelines.construct,
        &response_offsets_buffer,
        &response_meta_buffer,
        &response_coefficients_buffer,
        &kernel_buffer,
        &response_output,
        ORDERED_PAIRS,
        RESPONSE_COEFFICIENTS,
        response_groups,
    );
    encode_output_owner_construction(
        &command_buffer,
        &pipelines.construct,
        &rhs_offsets_buffer,
        &rhs_meta_buffer,
        &rhs_coefficients_buffer,
        &kernel_buffer,
        &rhs_output,
        IMAGING_STATES,
        RHS_COEFFICIENTS,
        rhs_groups,
    );
    let encode = encode_started.elapsed();
    let completion_started = Instant::now();
    command_buffer.commit();
    command_buffer.waitUntilCompleted();
    let commit_to_completion = completion_started.elapsed();
    let metal_total = total_started.elapsed();
    assert_ne!(
        command_buffer.status(),
        MTLCommandBufferStatus::Error,
        "output-owner construction command failed"
    );
    let gpu_start = command_buffer.GPUStartTime();
    let gpu_end = command_buffer.GPUEndTime();
    assert!(gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start);
    let device_time = Duration::from_secs_f64(gpu_end - gpu_start);

    let (response_actual, response_gather) = gather_constructed_samples(
        &device,
        &queue,
        &pipelines.gather,
        &response_output,
        &response_points,
        RESPONSE_COEFFICIENTS,
    );
    let (rhs_actual, rhs_gather) = gather_constructed_samples(
        &device,
        &queue,
        &pipelines.gather,
        &rhs_output,
        &rhs_points,
        RHS_COEFFICIENTS,
    );
    let (response_l2, response_linf) =
        sampled_construction_error(&response_actual, &response_expected, RESPONSE_COEFFICIENTS);
    let (rhs_l2, rhs_linf) =
        sampled_construction_error(&rhs_actual, &rhs_expected, RHS_COEFFICIENTS);

    let segmented_s = manifest["timings"]["in_memory_total_s"]
        .as_f64()
        .expect("segmented construction timing");
    let combined_s = segmented_s + output_allocation.as_secs_f64() + metal_total.as_secs_f64();
    let resident_bytes = response_offsets.len()
        + response_meta.len()
        + response_coefficients.len()
        + rhs_offsets.len()
        + rhs_meta.len()
        + rhs_coefficients.len()
        + mem::size_of_val(kernel_values.as_slice())
        + response_output_bytes
        + rhs_output_bytes;

    eprintln!(
        "vlass_real_route_output_owner_construction_probe \
         artifact_read_s={:.9} pipeline_compile_s={:.9} input_wrap_s={:.9} \
         output_allocation_s={:.9} encode_s={:.9} \
         commit_to_completion_s={:.9} device_s={:.9} metal_total_s={:.9} \
         response_gather_s={:.9} rhs_gather_s={:.9} \
         segmented_in_memory_s={:.9} combined_segmented_plus_metal_s={:.9} \
         response_groups={} rhs_groups={} resident_bytes={} \
         response_sample_l2={:.9e} response_sample_linf={:.9e} \
         rhs_sample_l2={:.9e} rhs_sample_linf={:.9e}",
        artifact_read.as_secs_f64(),
        pipeline_compile.as_secs_f64(),
        input_wrap.as_secs_f64(),
        output_allocation.as_secs_f64(),
        encode.as_secs_f64(),
        commit_to_completion.as_secs_f64(),
        device_time.as_secs_f64(),
        metal_total.as_secs_f64(),
        response_gather.as_secs_f64(),
        rhs_gather.as_secs_f64(),
        segmented_s,
        combined_s,
        response_groups,
        rhs_groups,
        resident_bytes,
        response_l2,
        response_linf,
        rhs_l2,
        rhs_linf,
    );
}
