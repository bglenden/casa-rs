// SPDX-License-Identifier: LGPL-3.0-or-later
//! Production-inert complete-boundary probe for the VLASS ordered response.

use std::{
    collections::HashMap,
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
use sha2::{Digest, Sha256};

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
const CONSTRUCTION_OVERSAMPLING: usize = 2;
const OVERSAMPLED_SIDE: usize = SIDE * CONSTRUCTION_OVERSAMPLING;
const OVERSAMPLED_PIXELS: usize = OVERSAMPLED_SIDE * OVERSAMPLED_SIDE;
const RESPONSE_BATCH_PAIRS: usize = 6;
const RESPONSE_BATCH_PLANES: usize = RESPONSE_BATCH_PAIRS * W_ORDERS * RESPONSE_MOMENTS;
const ES_SUPPORT_WIDTH: usize = 15;
const ES_LUT_INTERVALS: usize = 65_536;
const ES_WIDTH: f64 = 14.0;
const ES_BETA: f64 = 32.2;
const COMPLEX_BYTES: usize = mem::size_of::<Complex32>();
const FFT_BUFFER_BYTES: usize = FORWARD_PLANES * PIXELS * COMPLEX_BYTES;
const RESPONSE_BYTES: usize = RESPONSE_PLANES * PIXELS * COMPLEX_BYTES;
const RIGHT_FACTOR_BYTES: usize = PREDICTION_STATES * ETA_POWERS * PIXELS * COMPLEX_BYTES;
const LEFT_FACTOR_BYTES: usize = IMAGING_STATES * ETA_POWERS * PIXELS * COMPLEX_BYTES;
const MODEL_BYTES: usize = MODEL_TERMS * PIXELS * mem::size_of::<f32>();
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
    float response_scale;
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
    uint state_base;
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

struct CompactCorrectionParams {
    uint input_side;
    uint output_side;
    uint planes;
    uint _pad0;
};

struct PlaneCopyParams {
    uint elements;
    uint plane_elements;
    uint output_plane_base;
    uint _pad0;
};

static inline float2 complex_multiply(float2 left, float2 right) {
    return float2(
        left.x * right.x - left.y * right.y,
        left.x * right.y + left.y * right.x
    );
}

kernel void prepare_right_planes(
    device const float *model [[buffer(0)]],
    device const float2 *right_factors [[buffer(1)]],
    device float2 *output [[buffer(2)]],
    constant LinearParams &params [[buffer(3)]],
    uint index [[thread_position_in_grid]]
) {
    if (index >= params.elements) {
        return;
    }
    uint plane = index / params.plane_elements;
    uint pixel = index - plane * params.plane_elements;
    uint model_term = plane & 1u;
    uint factor_plane = plane >> 1u;
    float amplitude =
        model[ulong(model_term) * ulong(params.plane_elements) + ulong(pixel)];
    output[index] =
        amplitude
        * right_factors[
            ulong(factor_plane) * ulong(params.plane_elements) + ulong(pixel)
        ];
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
    mixed[ulong(output_plane) * ulong(params.pixels) + ulong(pixel)] =
        params.response_scale * accumulator;
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
            uint inverse_plane =
                (imaging_state * params.eta_powers + left_power) * 2u + output_term;
            uint factor_plane = imaging_state * params.eta_powers + left_power;
            ulong inverse_index =
                ulong(inverse_plane) * ulong(params.pixels) + ulong(pixel);
            ulong factor_index =
                ulong(factor_plane) * ulong(params.pixels) + ulong(pixel);
            accumulator += complex_multiply(
                left_factors[factor_index],
                inverse[inverse_index]
            );
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
                ulong(state + params.state_base) * pixels
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
    uint shifted_x = (x + (params.side >> 1u)) % params.side;
    uint shifted_y = (y + (params.side >> 1u)) % params.side;
    ulong pixel = ulong(shifted_y) * ulong(params.side) + ulong(shifted_x);
    for (uint coefficient = 0u;
         coefficient < params.coefficients;
         ++coefficient) {
        ulong plane = ulong(state) * ulong(params.coefficients) + coefficient;
        output[plane * pixels + pixel] = accumulators[coefficient];
    }
}

kernel void construct_ordered_response_output_owner_es(
    device const uint *bucket_offsets [[buffer(0)]],
    device const float2 *group_meta [[buffer(1)]],
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
                ulong(state + params.state_base) * pixels
                + ulong(center_y) * ulong(params.side)
                + ulong(center_x);
            uint begin = bucket_offsets[bucket];
            uint end = bucket_offsets[bucket + 1ul];
            int delta_x = int(x) - center_x;
            int delta_y = int(y) - center_y;
            for (uint group = begin; group < end; ++group) {
                float2 subpixel = group_meta[group];
                float lut_position_x =
                    clamp(subpixel.x + 0.5f, 0.0f, 1.0f)
                    * float(params.oversampling);
                float lut_position_y =
                    clamp(subpixel.y + 0.5f, 0.0f, 1.0f)
                    * float(params.oversampling);
                uint lut_low_x = min(
                    uint(floor(lut_position_x)),
                    params.oversampling - 1u
                );
                uint lut_low_y = min(
                    uint(floor(lut_position_y)),
                    params.oversampling - 1u
                );
                float fraction_x = lut_position_x - float(lut_low_x);
                float fraction_y = lut_position_y - float(lut_low_y);
                uint tap_x = uint(delta_x + radius);
                uint tap_y = uint(delta_y + radius);
                uint lut_x0 = lut_low_x * params.support_width + tap_x;
                uint lut_x1 = (lut_low_x + 1u) * params.support_width + tap_x;
                uint lut_y0 = lut_low_y * params.support_width + tap_y;
                uint lut_y1 = (lut_low_y + 1u) * params.support_width + tap_y;
                float weight_x = mix(
                    kernel_lut[lut_x0],
                    kernel_lut[lut_x1],
                    fraction_x
                );
                float weight_y = mix(
                    kernel_lut[lut_y0],
                    kernel_lut[lut_y1],
                    fraction_y
                );
                float weight = weight_x * weight_y;
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
    uint shifted_x = (x + (params.side >> 1u)) % params.side;
    uint shifted_y = (y + (params.side >> 1u)) % params.side;
    ulong pixel = ulong(shifted_y) * ulong(params.side) + ulong(shifted_x);
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

kernel void compact_deapodize_ordered_response_lag_domain(
    device const float2 *input [[buffer(0)]],
    device const float *axis_correction [[buffer(1)]],
    device float2 *output [[buffer(2)]],
    constant CompactCorrectionParams &params [[buffer(3)]],
    uint index [[thread_position_in_grid]]
) {
    uint output_pixels = params.output_side * params.output_side;
    uint elements = params.planes * output_pixels;
    if (index >= elements) {
        return;
    }
    uint plane = index / output_pixels;
    uint pixel = index - plane * output_pixels;
    uint x = pixel % params.output_side;
    uint y = pixel / params.output_side;
    int signed_x = int(x);
    int signed_y = int(y);
    if (x >= (params.output_side >> 1u)) {
        signed_x -= int(params.output_side);
    }
    if (y >= (params.output_side >> 1u)) {
        signed_y -= int(params.output_side);
    }
    uint input_x =
        signed_x >= 0 ? uint(signed_x) : uint(int(params.input_side) + signed_x);
    uint input_y =
        signed_y >= 0 ? uint(signed_y) : uint(int(params.input_side) + signed_y);
    uint input_pixels = params.input_side * params.input_side;
    ulong input_index =
        ulong(plane) * ulong(input_pixels)
        + ulong(input_y) * ulong(params.input_side)
        + ulong(input_x);
    output[index] =
        float(input_pixels)
        * axis_correction[x]
        * axis_correction[y]
        * input[input_index];
}

kernel void copy_ordered_response_planes(
    device const float2 *input [[buffer(0)]],
    device float2 *output [[buffer(1)]],
    constant PlaneCopyParams &params [[buffer(2)]],
    uint index [[thread_position_in_grid]]
) {
    if (index >= params.elements) {
        return;
    }
    uint plane = index / params.plane_elements;
    uint pixel = index - plane * params.plane_elements;
    ulong output_index =
        ulong(params.output_plane_base + plane) * ulong(params.plane_elements)
        + ulong(pixel);
    output[output_index] = input[index];
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
    response_scale: f32,
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
    state_base: u32,
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

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct CompactCorrectionParams {
    input_side: u32,
    output_side: u32,
    planes: u32,
    _pad0: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
struct PlaneCopyParams {
    elements: u32,
    plane_elements: u32,
    output_plane_base: u32,
    _pad0: u32,
}

#[derive(Clone)]
struct OrderedResponsePipelines {
    prepare: MetalComputePipeline,
    fill_one: MetalComputePipeline,
    mix: MetalComputePipeline,
    reduce: MetalComputePipeline,
    construct: MetalComputePipeline,
    construct_es: MetalComputePipeline,
    gather: MetalComputePipeline,
    compact_deapodize_response: MetalComputePipeline,
    copy_response_planes: MetalComputePipeline,
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

fn synthetic_right_factor_value(factor_plane: usize) -> Complex32 {
    Complex32::new(
        ((factor_plane % 13) + 1) as f32 * 0.0001,
        -((((factor_plane * 7) % 11) + 1) as f32 * 0.00005),
    )
}

fn synthetic_model_value(model_term: usize) -> f32 {
    match model_term {
        0 => 1.0,
        1 => 0.75,
        _ => panic!("unsupported synthetic model term"),
    }
}

fn input_plane_value(plane: usize) -> Complex32 {
    let model_term = plane % MODEL_TERMS;
    synthetic_model_value(model_term) * synthetic_right_factor_value(plane / MODEL_TERMS)
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
        construct_es: make_pipeline(
            device,
            &library,
            "construct_ordered_response_output_owner_es",
        ),
        gather: make_pipeline(device, &library, "gather_ordered_response_samples"),
        compact_deapodize_response: make_pipeline(
            device,
            &library,
            "compact_deapodize_ordered_response_lag_domain",
        ),
        copy_response_planes: make_pipeline(device, &library, "copy_ordered_response_planes"),
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
        (left_factors, IMAGING_STATES * ETA_POWERS * PIXELS),
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
    model: &MetalBuffer,
    right_factors: &MetalBuffer,
    output: &MetalBuffer,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("right-plane preparation encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(model), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(right_factors), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 2);
    }
    let params = LinearParams {
        elements: checked_u32(FORWARD_PLANES * PIXELS, "right-plane element count"),
        plane_elements: checked_u32(PIXELS, "right-plane pixel count"),
        _pad0: 0,
        _pad1: 0,
    };
    set_bytes(&encoder, &params, 3);
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
    response_scale: f32,
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
        response_scale,
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
    model: &MetalBuffer,
    right_factors: &MetalBuffer,
    response: &MetalBuffer,
    left_factors: &MetalBuffer,
    action_offsets: &MetalBuffer,
    actions: &MetalBuffer,
    active_indices: &MetalBuffer,
    feedback: &MetalBuffer,
    response_scale: f32,
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

    encode_prepare(
        &initial_root,
        &pipelines.prepare,
        model,
        right_factors,
        buffer_a,
    );
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
        response_scale,
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

fn physical_semantic_receipt_path() -> PathBuf {
    env::var_os("CASA_RS_VLASS_ORDERED_RESPONSE_PHYSICAL_RECEIPT")
        .map(PathBuf::from)
        .expect("set CASA_RS_VLASS_ORDERED_RESPONSE_PHYSICAL_RECEIPT to the v2 semantic receipt")
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
    side: usize,
    coefficient_count: usize,
) -> usize {
    let pixels = side * side;
    assert_eq!(offsets.len(), (states * pixels + 1) * mem::size_of::<u32>());
    assert_eq!(meta.len() % 4, 0);
    let groups = meta.len() / 4;
    assert_eq!(
        coefficients.len(),
        groups * coefficient_count * COMPLEX_BYTES
    );
    assert_eq!(little_u32_at(offsets, states * pixels) as usize, groups);
    for encoded in meta.chunks_exact(mem::size_of::<i16>()) {
        let offset = i16::from_le_bytes(encoded.try_into().expect("complete i16"));
        assert!(
            (-50..=50).contains(&offset),
            "subpixel offset {offset} is outside the rounded 100x grid"
        );
    }
    groups
}

fn validate_float_group_artifacts(
    offsets: &[u8],
    meta: &[u8],
    coefficients: &[u8],
    states: usize,
    side: usize,
    coefficient_count: usize,
) -> usize {
    let pixels = side * side;
    assert_eq!(offsets.len(), (states * pixels + 1) * mem::size_of::<u32>());
    assert_eq!(meta.len() % (2 * mem::size_of::<f32>()), 0);
    let groups = meta.len() / (2 * mem::size_of::<f32>());
    assert_eq!(
        coefficients.len(),
        groups * coefficient_count * COMPLEX_BYTES
    );
    assert_eq!(little_u32_at(offsets, states * pixels) as usize, groups);
    for encoded in meta.chunks_exact(mem::size_of::<f32>()) {
        let offset = f32::from_le_bytes(encoded.try_into().expect("complete f32"));
        assert!(
            offset.is_finite() && (-0.5..=0.5).contains(&offset),
            "ES subpixel offset {offset} is outside one half-cell"
        );
    }
    groups
}

fn standard_grdsf(nu: f64) -> f64 {
    const P0: [f64; 5] = [
        8.203_343e-2,
        -3.644_705e-1,
        6.278_660e-1,
        -5.335_581e-1,
        2.312_756e-1,
    ];
    const P1: [f64; 5] = [
        4.028_559e-3,
        -3.697_768e-2,
        1.021_332e-1,
        -1.201_436e-1,
        6.412_774e-2,
    ];
    const Q0: [f64; 3] = [1.0, 8.212_018e-1, 2.078_043e-1];
    const Q1: [f64; 3] = [1.0, 9.599_102e-1, 2.918_724e-1];
    if !(0.0..=1.0).contains(&nu) {
        return 0.0;
    }
    let (p, q, nu_end) = if nu < 0.75 {
        (&P0, &Q0, 0.75)
    } else {
        (&P1, &Q1, 1.0)
    };
    let delta = nu * nu - nu_end * nu_end;
    let numerator = p
        .iter()
        .enumerate()
        .map(|(order, value)| value * delta.powi(order as i32))
        .sum::<f64>();
    let denominator = q
        .iter()
        .enumerate()
        .map(|(order, value)| value * delta.powi(order as i32))
        .sum::<f64>();
    if denominator == 0.0 {
        0.0
    } else {
        numerator / denominator
    }
}

fn controlled_kernel_lut() -> Vec<f32> {
    let mut kernel_table = vec![0.0_f32; 400];
    for (index, value) in kernel_table.iter_mut().enumerate().take(300) {
        let distance = index as f64 / 100.0;
        let nu = distance / 3.0;
        *value = ((1.0 - nu * nu) * standard_grdsf(nu)) as f32;
    }
    let mut output = Vec::with_capacity(101 * 7);
    for offset in -50_i32..=50 {
        let mut weights = [0.0_f32; 7];
        let mut normalization = 0.0_f32;
        for (tap, delta) in (-3_i32..=3).enumerate() {
            let lookup = (delta * 100 + offset).unsigned_abs() as usize;
            let weight = kernel_table.get(lookup).copied().unwrap_or(0.0);
            weights[tap] = weight;
            normalization += weight;
        }
        if normalization > 0.0 {
            for weight in &mut weights {
                *weight /= normalization;
            }
        }
        output.extend(weights);
    }
    output
}

fn es_kernel_weight(offset: f64, delta: isize) -> f64 {
    let distance = delta as f64 + offset;
    let normalized = 2.0 * distance / ES_WIDTH;
    if normalized.abs() >= 1.0 {
        0.0
    } else {
        (ES_BETA * ((1.0 - normalized * normalized).sqrt() - 1.0)).exp()
    }
}

fn es_kernel_lut() -> Vec<f32> {
    let radius = ES_SUPPORT_WIDTH / 2;
    let mut values = Vec::with_capacity((ES_LUT_INTERVALS + 1) * ES_SUPPORT_WIDTH);
    for offset_index in 0..=ES_LUT_INTERVALS {
        let offset = offset_index as f64 / ES_LUT_INTERVALS as f64 - 0.5;
        for delta in -(radius as isize)..=radius as isize {
            values.push(es_kernel_weight(offset, delta) as f32);
        }
    }
    values
}

fn es_response_lag_correction_axis(construction_side: usize) -> Vec<f32> {
    assert!(construction_side >= SIDE);
    let radius = ES_SUPPORT_WIDTH / 2;
    (0..SIDE)
        .map(|index| {
            let signed = if index < SIDE / 2 {
                index as isize
            } else {
                index as isize - SIDE as isize
            };
            let phase_scale = std::f64::consts::TAU * signed as f64 / construction_side as f64;
            let transform = (-(radius as isize)..=radius as isize)
                .map(|delta| es_kernel_weight(0.0, delta) * (phase_scale * delta as f64).cos())
                .sum::<f64>();
            assert!(
                transform > 1.0e-12,
                "ES lag correction is singular at {signed}"
            );
            (1.0 / transform) as f32
        })
        .collect()
}

fn compile_square_resident_fft_executable(
    device: &MetalDevice,
    graph_device: &MPSGraphDevice,
    batch: usize,
    side: usize,
    direction: FftDirection,
) -> (ResidentFftExecutable, Duration) {
    let graph = unsafe { MPSGraph::new() };
    let shape = shape_array_batch(batch, side, side);
    let placeholder = unsafe {
        graph.placeholderWithShape_dataType_name(Some(&shape), MPSDataType::ComplexFloat32, None)
    };
    let descriptor = unsafe { MPSGraphFFTDescriptor::descriptor() }.expect("square FFT descriptor");
    unsafe {
        descriptor.setInverse(direction == FftDirection::Inverse);
        descriptor.setScalingMode(if direction == FftDirection::Inverse {
            MPSGraphFFTScalingMode::Size
        } else {
            MPSGraphFFTScalingMode::None
        });
    }
    let axes = axes_array_batch();
    let output = unsafe {
        graph.fastFourierTransformWithTensor_axes_descriptor_name(
            &placeholder,
            &axes,
            &descriptor,
            None,
        )
    };
    let target_tensors = NSArray::from_slice(&[&*output]);
    let shaped_type = unsafe {
        MPSGraphShapedType::initWithShape_dataType(
            MPSGraphShapedType::alloc(),
            Some(&shape),
            MPSDataType::ComplexFloat32,
        )
    };
    let feeds: Retained<MPSGraphTensorShapedTypeDictionary> =
        NSDictionary::from_slices(&[&*placeholder], &[&*shaped_type]);
    let compile_started = Instant::now();
    let executable = unsafe {
        graph.compileWithDevice_feeds_targetTensors_targetOperations_compilationDescriptor(
            Some(graph_device),
            &feeds,
            &target_tensors,
            None,
            None,
        )
    };
    let compile = compile_started.elapsed();
    assert_eq!(
        unsafe { executable.feedTensors() }
            .expect("square FFT feeds")
            .len(),
        1
    );
    assert_eq!(
        unsafe { executable.targetTensors() }
            .expect("square FFT targets")
            .len(),
        1
    );
    let _ = device;
    (
        ResidentFftExecutable {
            _graph: graph,
            executable,
        },
        compile,
    )
}

fn encode_compact_response_deapodization(
    command_buffer: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    input: &MetalBuffer,
    correction_axis: &MetalBuffer,
    output: &MetalBuffer,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("compact response deapodization encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(input), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(correction_axis), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 2);
    }
    let params = CompactCorrectionParams {
        input_side: checked_u32(OVERSAMPLED_SIDE, "oversampled response side"),
        output_side: checked_u32(SIDE, "compact response side"),
        planes: checked_u32(RESPONSE_BATCH_PLANES, "response batch planes"),
        _pad0: 0,
    };
    set_bytes(&encoder, &params, 3);
    dispatch_linear(&encoder, pipeline, RESPONSE_BATCH_PLANES * PIXELS);
    encoder.endEncoding();
}

fn encode_copy_response_planes(
    command_buffer: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    input: &MetalBuffer,
    output: &MetalBuffer,
    output_plane_base: usize,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("response plane copy encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(input), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 1);
    }
    let params = PlaneCopyParams {
        elements: checked_u32(RESPONSE_BATCH_PLANES * PIXELS, "response copy elements"),
        plane_elements: checked_u32(PIXELS, "response copy plane pixels"),
        output_plane_base: checked_u32(output_plane_base, "response output plane base"),
        _pad0: 0,
    };
    set_bytes(&encoder, &params, 2);
    dispatch_linear(&encoder, pipeline, RESPONSE_BATCH_PLANES * PIXELS);
    encoder.endEncoding();
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

fn copied_shared_buffer_from_slice<T>(
    device: &MetalDevice,
    values: &[T],
) -> Result<MetalBuffer, &'static str> {
    let byte_len = mem::size_of_val(values);
    if byte_len == 0 {
        return Err("copied ordered-response input requires non-empty values");
    }
    let pointer = NonNull::new(values.as_ptr().cast::<c_void>().cast_mut())
        .ok_or("copied ordered-response input pointer was null")?;
    unsafe {
        device
            .newBufferWithBytes_length_options(
                pointer,
                byte_len,
                MTLResourceOptions::StorageModeShared,
            )
            .ok_or("failed to allocate copied ordered-response input")
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
    side: usize,
    states: usize,
    state_base: usize,
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
        side: checked_u32(side, "construction side"),
        states: checked_u32(states, "construction states"),
        coefficients: checked_u32(coefficient_count, "construction coefficients"),
        support_width: 7,
        oversampling: 100,
        offset_bias: 50,
        group_count: checked_u32(group_count, "construction groups"),
        state_base: checked_u32(state_base, "construction state base"),
    };
    set_bytes(&encoder, &params, 5);
    let (group_width, group_height) = threadgroup_2d(pipeline, side, side);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: side,
            height: side,
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

#[allow(clippy::too_many_arguments)]
fn encode_output_owner_es_construction(
    command_buffer: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    bucket_offsets: &MetalBuffer,
    group_meta: &MetalBuffer,
    group_coefficients: &MetalBuffer,
    kernel_lut: &MetalBuffer,
    output: &MetalBuffer,
    side: usize,
    states: usize,
    state_base: usize,
    coefficient_count: usize,
    group_count: usize,
) {
    let encoder = command_buffer
        .computeCommandEncoder()
        .expect("ES output-owner construction encoder");
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(bucket_offsets), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(group_meta), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(group_coefficients), 0, 2);
        encoder.setBuffer_offset_atIndex(Some(kernel_lut), 0, 3);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 4);
    }
    let params = ConstructionParams {
        side: checked_u32(side, "ES construction side"),
        states: checked_u32(states, "ES construction states"),
        coefficients: checked_u32(coefficient_count, "ES construction coefficients"),
        support_width: checked_u32(ES_SUPPORT_WIDTH, "ES support width"),
        oversampling: checked_u32(ES_LUT_INTERVALS, "ES LUT intervals"),
        offset_bias: 0,
        group_count: checked_u32(group_count, "ES construction groups"),
        state_base: checked_u32(state_base, "ES construction state base"),
    };
    set_bytes(&encoder, &params, 5);
    let (group_width, group_height) = threadgroup_2d(pipeline, side, side);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: side,
            height: side,
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

fn fft_order_sample_points(points: &[SamplePoint]) -> Vec<SamplePoint> {
    points
        .iter()
        .map(|point| SamplePoint {
            state: point.state,
            x: (point.x + checked_u32(SIDE / 2, "half construction side"))
                % checked_u32(SIDE, "construction side"),
            y: (point.y + checked_u32(SIDE / 2, "half construction side"))
                % checked_u32(SIDE, "construction side"),
            _pad0: 0,
        })
        .collect()
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

#[derive(Clone, Copy, Debug)]
struct PhysicalPairMap {
    pair_index: usize,
    imaging_state: usize,
    prediction_state: usize,
    imaging_screen_state: usize,
    prediction_screen_state: usize,
}

struct PhysicalResidentFixture {
    active_indices: Vec<u32>,
    probe_active_ordinals: Vec<usize>,
    probe_receipt_indices: Vec<usize>,
    pair_map: Vec<PhysicalPairMap>,
    right_factors: Vec<Complex32>,
    left_factors: Vec<Complex32>,
    model_cases: [Vec<f32>; MODEL_TERMS],
    expected_contracted: Vec<Complex64>,
    expected_exact_w: Vec<Complex64>,
    receipt_sha256: String,
}

fn sha256_bytes(values: &[u8]) -> String {
    format!("{:x}", Sha256::digest(values))
}

fn json_i32_pair(value: &serde_json::Value, context: &str) -> [i32; 2] {
    let values = value
        .as_array()
        .unwrap_or_else(|| panic!("{context} must be a JSON pair"));
    assert_eq!(values.len(), 2, "{context} must have two entries");
    [
        i32::try_from(values[0].as_i64().expect("integer pair x"))
            .unwrap_or_else(|_| panic!("{context} x exceeds i32")),
        i32::try_from(values[1].as_i64().expect("integer pair y"))
            .unwrap_or_else(|_| panic!("{context} y exceeds i32")),
    ]
}

fn json_f64_pair(value: &serde_json::Value, context: &str) -> [f64; 2] {
    let values = value
        .as_array()
        .unwrap_or_else(|| panic!("{context} must be a JSON pair"));
    assert_eq!(values.len(), 2, "{context} must have two entries");
    [
        values[0]
            .as_f64()
            .unwrap_or_else(|| panic!("{context} x must be numeric")),
        values[1]
            .as_f64()
            .unwrap_or_else(|| panic!("{context} y must be numeric")),
    ]
}

fn json_i32_pairs(value: &serde_json::Value, context: &str) -> Vec<[i32; 2]> {
    value
        .as_array()
        .unwrap_or_else(|| panic!("{context} must be an array"))
        .iter()
        .map(|entry| json_i32_pair(entry, context))
        .collect()
}

fn resolve_json_artifact(manifest_path: &Path, value: &serde_json::Value) -> PathBuf {
    let value = value.as_str().expect("artifact path must be a string");
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        manifest_path
            .parent()
            .expect("manifest path has a parent")
            .join(path)
    }
}

fn complex32_values_from_le_bytes(values: &[u8]) -> Vec<Complex32> {
    assert_eq!(values.len() % COMPLEX_BYTES, 0);
    values
        .chunks_exact(COMPLEX_BYTES)
        .map(|value| {
            Complex32::new(
                f32::from_le_bytes(value[..4].try_into().expect("complex real f32")),
                f32::from_le_bytes(value[4..].try_into().expect("complex imaginary f32")),
            )
        })
        .collect()
}

fn facet_eta(pixel: [i32; 2], center: [f64; 2], image_reference: f64, cell_rad: f64) -> f64 {
    fn direction(pixel: [f64; 2], image_reference: f64, cell_rad: f64) -> [f64; 3] {
        let l = (pixel[0] - image_reference) * cell_rad;
        let m = (image_reference - pixel[1]) * cell_rad;
        let n = (1.0 - l * l - m * m).sqrt();
        [l, m, n]
    }
    let pixel_direction = direction(
        [f64::from(pixel[0]), f64::from(pixel[1])],
        image_reference,
        cell_rad,
    );
    let normal = direction(center, image_reference, cell_rad);
    pixel_direction
        .iter()
        .zip(normal)
        .map(|(value, basis)| value * basis)
        .sum::<f64>()
        - 1.0
}

struct ScreenSampler<'a> {
    values: &'a [Complex32],
    states: usize,
    side: usize,
    uv_reference: [f64; 2],
    crop_start: [f64; 2],
    sky_increment: [f64; 2],
    pointing_pixel: [f64; 2],
    cell_rad: f64,
}

impl ScreenSampler<'_> {
    fn sample(&self, state: usize, pixel: [i32; 2]) -> Complex32 {
        assert!(state < self.states);
        let x = self.uv_reference[0]
            + (f64::from(pixel[0]) - self.pointing_pixel[0]) * self.cell_rad
                / self.sky_increment[0].abs()
            - self.crop_start[0];
        let y = self.uv_reference[1]
            + (f64::from(pixel[1]) - self.pointing_pixel[1]) * self.cell_rad
                / self.sky_increment[1].abs()
            - self.crop_start[1];
        let x0 = x.floor() as isize;
        let y0 = y.floor() as isize;
        assert!(
            x0 >= 0 && y0 >= 0 && x0 + 1 < self.side as isize && y0 + 1 < self.side as isize,
            "physical factor pixel escapes screen crop"
        );
        let fx = x - x0 as f64;
        let fy = y - y0 as f64;
        let read = |sample_x: isize, sample_y: isize| {
            let index =
                state * self.side * self.side + sample_y as usize * self.side + sample_x as usize;
            let value = self.values[index];
            Complex64::new(f64::from(value.re), f64::from(value.im))
        };
        let value = read(x0, y0) * ((1.0 - fx) * (1.0 - fy))
            + read(x0 + 1, y0) * (fx * (1.0 - fy))
            + read(x0, y0 + 1) * ((1.0 - fx) * fy)
            + read(x0 + 1, y0 + 1) * (fx * fy);
        Complex32::new(value.re as f32, value.im as f32)
    }
}

fn embedding_index(pixel: [i32; 2], origin: [i32; 2]) -> usize {
    let x = pixel[0] - origin[0];
    let y = pixel[1] - origin[1];
    assert!(
        (0..SIDE as i32).contains(&x) && (0..SIDE as i32).contains(&y),
        "physical fixture pixel escapes the resident embedding"
    );
    y as usize * SIDE + x as usize
}

fn parse_expected_complex(values: &serde_json::Value) -> Vec<Complex64> {
    let shape = values["shape"].as_array().expect("complex fixture shape");
    assert_eq!(
        shape
            .iter()
            .map(|value| value.as_u64().expect("complex shape extent") as usize)
            .collect::<Vec<_>>(),
        [MODEL_TERMS, OUTPUT_TERMS, 28]
    );
    values["values"]
        .as_array()
        .expect("complex fixture values")
        .iter()
        .map(|value| {
            let pair = json_f64_pair(value, "complex fixture value");
            Complex64::new(pair[0], pair[1])
        })
        .collect()
}

fn load_physical_resident_fixture(receipt_path: &Path) -> PhysicalResidentFixture {
    let receipt_bytes = fs::read(receipt_path).expect("read physical semantic receipt");
    let receipt_sha256 = sha256_bytes(&receipt_bytes);
    let receipt: serde_json::Value =
        serde_json::from_slice(&receipt_bytes).expect("physical semantic receipt JSON");
    assert_eq!(
        receipt["schema"].as_str(),
        Some("casa-rs-vlass-ordered-response-physical-semantic-gate/v2")
    );
    assert_eq!(receipt["gate"]["passed"].as_bool(), Some(true));
    let fixture = &receipt["resident_integration_fixture"];
    assert_eq!(fixture["embedding"]["side"].as_u64(), Some(SIDE as u64));
    let origin = json_i32_pair(
        &fixture["embedding"]["origin_image_pixel"],
        "embedding origin",
    );
    let active_pixels = json_i32_pairs(&fixture["active_pixels"], "active pixels");
    assert_eq!(active_pixels.len(), ACTIVE_PIXELS);
    let source_pixels = json_i32_pairs(&fixture["source_pixels"], "source pixels");
    let model_values = fixture["model_values"]
        .as_array()
        .expect("model fixture values")
        .iter()
        .map(|value| value.as_f64().expect("model value") as f32)
        .collect::<Vec<_>>();
    assert_eq!(source_pixels.len(), model_values.len());
    assert_eq!(source_pixels.len(), 507);
    let output_probes = json_i32_pairs(&fixture["output_probe_pixels"], "output probe pixels");
    assert_eq!(output_probes.len(), 28);

    let pair_map = fixture["ordered_pair_map"]
        .as_array()
        .expect("ordered pair map")
        .iter()
        .map(|entry| PhysicalPairMap {
            pair_index: entry["pair_index"].as_u64().expect("pair index") as usize,
            imaging_state: entry["imaging_state"].as_u64().expect("imaging state") as usize,
            prediction_state: entry["prediction_state"]
                .as_u64()
                .expect("prediction state") as usize,
            imaging_screen_state: entry["imaging_screen_state"]
                .as_u64()
                .expect("imaging screen state") as usize,
            prediction_screen_state: entry["prediction_screen_state"]
                .as_u64()
                .expect("prediction screen state") as usize,
        })
        .collect::<Vec<_>>();
    assert_eq!(pair_map.len(), ORDERED_PAIRS);
    for (pair, mapping) in pair_map.iter().enumerate() {
        assert_eq!(mapping.pair_index, pair);
        assert!(mapping.imaging_state < IMAGING_STATES);
        assert!(mapping.prediction_state < PREDICTION_STATES);
        assert!(mapping.imaging_screen_state < PREDICTION_STATES);
        assert!(mapping.prediction_screen_state < PREDICTION_STATES);
    }

    let screen_manifest_path = PathBuf::from(
        receipt["sources"]["screen_manifest"]
            .as_str()
            .expect("physical screen manifest path"),
    );
    let screen_manifest_bytes =
        fs::read(&screen_manifest_path).expect("read physical screen manifest");
    assert_eq!(
        sha256_bytes(&screen_manifest_bytes),
        receipt["sources"]["screen_manifest_sha256"]
            .as_str()
            .expect("physical screen manifest hash")
    );
    let screen_manifest: serde_json::Value =
        serde_json::from_slice(&screen_manifest_bytes).expect("physical screen manifest JSON");
    let crop_shape = json_i32_pair(&screen_manifest["crop_shape"], "screen crop shape");
    assert_eq!(crop_shape, [512, 512]);
    let forward_path =
        resolve_json_artifact(&screen_manifest_path, &screen_manifest["forward_path"]);
    let forward_bytes = fs::read(&forward_path).expect("read physical forward screens");
    assert_eq!(
        sha256_bytes(&forward_bytes),
        receipt["sources"]["screen_artifact_sha256"]["forward"]
            .as_str()
            .expect("forward screen hash")
    );
    let screen_values = complex32_values_from_le_bytes(&forward_bytes);
    assert_eq!(screen_values.len(), PREDICTION_STATES * 512 * 512);
    let uv_reference = json_f64_pair(
        &screen_manifest["uv_reference_pixel"],
        "screen UV reference",
    );
    let crop_start_i32 = json_i32_pair(&screen_manifest["crop_start"], "screen crop start");
    let crop_start = [f64::from(crop_start_i32[0]), f64::from(crop_start_i32[1])];
    let sky_increment = json_f64_pair(
        &screen_manifest["derived_sky_increment_rad"],
        "screen sky increment",
    );
    let pointing_pixel = json_f64_pair(&receipt["geometry"]["pointing_pixel"], "POINTING pixel");
    let cell_rad = receipt["geometry"]["cell_arcsec"]
        .as_f64()
        .expect("cell arcsec")
        * std::f64::consts::PI
        / (180.0 * 3600.0);
    let facet_center = json_f64_pair(&receipt["geometry"]["facet_center_pixel"], "facet center");
    let image_reference = receipt["geometry"]["image_reference_pixel"]
        .as_f64()
        .expect("image reference pixel");
    let sampler = ScreenSampler {
        values: &screen_values,
        states: PREDICTION_STATES,
        side: 512,
        uv_reference,
        crop_start,
        sky_increment,
        pointing_pixel,
        cell_rad,
    };

    let mut imaging_screen_states = [usize::MAX; IMAGING_STATES];
    let mut prediction_screen_states = [usize::MAX; PREDICTION_STATES];
    for mapping in &pair_map {
        let imaging = &mut imaging_screen_states[mapping.imaging_state];
        assert!(
            *imaging == usize::MAX || *imaging == mapping.imaging_screen_state,
            "compact imaging state maps to multiple physical screens"
        );
        *imaging = mapping.imaging_screen_state;
        let prediction = &mut prediction_screen_states[mapping.prediction_state];
        assert!(
            *prediction == usize::MAX || *prediction == mapping.prediction_screen_state,
            "compact prediction state maps to multiple physical screens"
        );
        *prediction = mapping.prediction_screen_state;
    }
    assert!(!imaging_screen_states.contains(&usize::MAX));
    assert!(!prediction_screen_states.contains(&usize::MAX));

    let active_indices = active_pixels
        .iter()
        .map(|&pixel| checked_u32(embedding_index(pixel, origin), "active embedding index"))
        .collect::<Vec<_>>();
    let active_ordinal = active_pixels
        .iter()
        .enumerate()
        .map(|(ordinal, &pixel)| (pixel, ordinal))
        .collect::<HashMap<_, _>>();
    let mut probe_active_ordinals = Vec::new();
    let mut probe_receipt_indices = Vec::new();
    for (receipt_index, pixel) in output_probes.iter().enumerate() {
        if let Some(&ordinal) = active_ordinal.get(pixel) {
            probe_active_ordinals.push(ordinal);
            probe_receipt_indices.push(receipt_index);
        }
    }
    assert_eq!(probe_active_ordinals.len(), 11);

    let mut left_factors = vec![Complex32::new(0.0, 0.0); IMAGING_STATES * ETA_POWERS * PIXELS];
    for (imaging_state, &screen_state) in imaging_screen_states.iter().enumerate() {
        for &pixel in &active_pixels {
            let embedding = embedding_index(pixel, origin);
            let eta = facet_eta(pixel, facet_center, image_reference, cell_rad) as f32;
            let screen = sampler.sample(screen_state, pixel).conj();
            let mut eta_power = 1.0_f32;
            for power in 0..ETA_POWERS {
                let plane = imaging_state * ETA_POWERS + power;
                left_factors[plane * PIXELS + embedding] = screen * eta_power;
                eta_power *= eta;
            }
        }
    }

    let mut right_factors = vec![Complex32::new(0.0, 0.0); PREDICTION_STATES * ETA_POWERS * PIXELS];
    for (prediction_state, &screen_state) in prediction_screen_states.iter().enumerate() {
        for &pixel in &source_pixels {
            let embedding = embedding_index(pixel, origin);
            let eta = -(facet_eta(pixel, facet_center, image_reference, cell_rad) as f32);
            let screen = sampler.sample(screen_state, pixel);
            let mut eta_power = 1.0_f32;
            for power in 0..ETA_POWERS {
                let plane = prediction_state * ETA_POWERS + power;
                right_factors[plane * PIXELS + embedding] = screen * eta_power;
                eta_power *= eta;
            }
        }
    }

    let model_cases = std::array::from_fn(|model_case| {
        let mut values = vec![0.0_f32; MODEL_TERMS * PIXELS];
        for (&pixel, &amplitude) in source_pixels.iter().zip(&model_values) {
            let embedding = embedding_index(pixel, origin);
            values[model_case * PIXELS + embedding] = amplitude;
        }
        values
    });
    PhysicalResidentFixture {
        active_indices,
        probe_active_ordinals,
        probe_receipt_indices,
        pair_map,
        right_factors,
        left_factors,
        model_cases,
        expected_contracted: parse_expected_complex(&fixture["aggregate_total_order_two"]),
        expected_exact_w: parse_expected_complex(&fixture["aggregate_direct_exact_w"]),
        receipt_sha256,
    }
}

fn physical_ordered_response_actions(pair_map: &[PhysicalPairMap]) -> (Vec<u32>, Vec<MixerAction>) {
    assert_eq!(pair_map.len(), ORDERED_PAIRS);
    let mut grouped = vec![Vec::new(); INVERSE_PLANES];
    for mapping in pair_map {
        for output_term in 0..OUTPUT_TERMS {
            for model_term in 0..MODEL_TERMS {
                let moment = output_term + model_term;
                for order in 0..W_ORDERS {
                    for left_power in 0..=order {
                        let right_power = order - left_power;
                        let output_plane = (mapping.imaging_state * ETA_POWERS + left_power)
                            * OUTPUT_TERMS
                            + output_term;
                        let input_plane = (mapping.prediction_state * ETA_POWERS + right_power)
                            * MODEL_TERMS
                            + model_term;
                        let kernel_plane =
                            (mapping.pair_index * W_ORDERS + order) * RESPONSE_MOMENTS + moment;
                        grouped[output_plane].push(MixerAction {
                            input_plane: checked_u32(input_plane, "physical input plane"),
                            kernel_plane: checked_u32(kernel_plane, "physical kernel plane"),
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
    offsets.push(0);
    for output_actions in grouped {
        actions.extend(output_actions);
        offsets.push(checked_u32(actions.len(), "physical action offset"));
    }
    assert_eq!(actions.len(), 1_296);
    (offsets, actions)
}

fn physical_feedback_metrics(
    actual: &[Complex64],
    fixture: &PhysicalResidentFixture,
) -> (f64, f64, f64, f64) {
    let probes = fixture.probe_receipt_indices.len();
    assert_eq!(actual.len(), MODEL_TERMS * OUTPUT_TERMS * probes);
    let mut contracted = Vec::with_capacity(actual.len());
    let mut exact = Vec::with_capacity(actual.len());
    for model_case in 0..MODEL_TERMS {
        for output_term in 0..OUTPUT_TERMS {
            for &receipt_probe in &fixture.probe_receipt_indices {
                let index = (model_case * OUTPUT_TERMS + output_term) * 28 + receipt_probe;
                contracted.push(fixture.expected_contracted[index]);
                exact.push(fixture.expected_exact_w[index]);
            }
        }
    }
    let metrics = |reference: &[Complex64]| {
        let difference = actual
            .iter()
            .zip(reference)
            .map(|(candidate, reference)| (*candidate - *reference).norm_sqr())
            .sum::<f64>()
            .sqrt();
        let reference_l2 = reference
            .iter()
            .map(Complex64::norm_sqr)
            .sum::<f64>()
            .sqrt()
            .max(f64::MIN_POSITIVE);
        let difference_linf = actual
            .iter()
            .zip(reference)
            .map(|(candidate, reference)| (*candidate - *reference).norm())
            .fold(0.0_f64, f64::max);
        let reference_linf = reference
            .iter()
            .map(|value| value.norm())
            .fold(0.0_f64, f64::max)
            .max(f64::MIN_POSITIVE);
        (difference / reference_l2, difference_linf / reference_linf)
    };
    let (contracted_l2, contracted_linf) = metrics(&contracted);
    let (exact_l2, exact_linf) = metrics(&exact);
    (contracted_l2, contracted_linf, exact_l2, exact_linf)
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
    assert_eq!(RIGHT_FACTOR_BYTES, 28_311_552);
    assert_eq!(LEFT_FACTOR_BYTES, 24_772_608);
    assert_eq!(MODEL_BYTES, 294_912);
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
    let right_factor_values: Vec<_> = (0..PREDICTION_STATES * ETA_POWERS)
        .flat_map(|factor_plane| {
            std::iter::repeat_n(synthetic_right_factor_value(factor_plane), PIXELS)
        })
        .collect();
    let model_values: Vec<_> = (0..MODEL_TERMS)
        .flat_map(|model_term| std::iter::repeat_n(synthetic_model_value(model_term), PIXELS))
        .collect();
    let right_factors = copied_shared_buffer_from_slice(&device, &right_factor_values)
        .expect("ordered-response synthetic right factors");
    let model = copied_shared_buffer_from_slice(&device, &model_values)
        .expect("ordered-response synthetic model");
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
        &model,
        &right_factors,
        &response,
        &left_factors,
        &action_offsets,
        &actions,
        &active_indices,
        &feedback,
        1.0,
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
                &model,
                &right_factors,
                &response,
                &left_factors,
                &action_offsets,
                &actions,
                &active_indices,
                &feedback,
                1.0,
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
        + RIGHT_FACTOR_BYTES
        + LEFT_FACTOR_BYTES
        + MODEL_BYTES
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
        Some("casa-rs-vlass-ordered-response-segmented-construction/v7")
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
        SIDE,
        RESPONSE_COEFFICIENTS,
    );
    let rhs_groups = validate_group_artifacts(
        &rhs_offsets,
        &rhs_meta,
        &rhs_coefficients,
        IMAGING_STATES,
        SIDE,
        RHS_COEFFICIENTS,
    );
    assert_eq!(response_groups, 372_910);
    assert_eq!(rhs_groups, 370_730);
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
        SIDE,
        ORDERED_PAIRS,
        0,
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
        SIDE,
        IMAGING_STATES,
        0,
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

    let response_fft_points = fft_order_sample_points(&response_points);
    let rhs_fft_points = fft_order_sample_points(&rhs_points);
    let (response_actual, response_gather) = gather_constructed_samples(
        &device,
        &queue,
        &pipelines.gather,
        &response_output,
        &response_fft_points,
        RESPONSE_COEFFICIENTS,
    );
    let (rhs_actual, rhs_gather) = gather_constructed_samples(
        &device,
        &queue,
        &pipelines.gather,
        &rhs_output,
        &rhs_fft_points,
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

#[test]
#[serial]
#[ignore = "production-inert VLASS physical resident ordered-response gate"]
fn vlass_physical_private_resident_ordered_response_probe() {
    assert!(
        mpsgraph_f32_available(),
        "VLASS physical resident gate requires a visible Metal device"
    );
    let fixture_started = Instant::now();
    let mut fixture = load_physical_resident_fixture(&physical_semantic_receipt_path());
    let fixture_load = fixture_started.elapsed();

    let artifact_dir = construction_artifact_dir();
    let construction_manifest: serde_json::Value =
        serde_json::from_slice(&artifact_bytes(&artifact_dir, "manifest.json"))
            .expect("construction manifest JSON");
    assert_eq!(
        construction_manifest["schema"].as_str(),
        Some("casa-rs-vlass-ordered-response-segmented-construction/v9")
    );
    assert_eq!(
        construction_manifest["geometry"]["construction_grid_side"].as_u64(),
        Some(OVERSAMPLED_SIDE as u64)
    );
    let response_offsets = artifact_bytes(&artifact_dir, "response-bucket-offsets-u32-le.bin");
    let response_meta = artifact_bytes(&artifact_dir, "response-group-meta-f32-le.bin");
    let response_coefficients =
        artifact_bytes(&artifact_dir, "response-group-coefficients-c64-le.bin");
    let response_groups = validate_float_group_artifacts(
        &response_offsets,
        &response_meta,
        &response_coefficients,
        ORDERED_PAIRS,
        OVERSAMPLED_SIDE,
        RESPONSE_COEFFICIENTS,
    );
    assert_eq!(response_groups, 771_724);

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
    let (response_inverse, response_inverse_compile) = compile_square_resident_fft_executable(
        &device,
        &graph_device,
        RESPONSE_BATCH_PLANES,
        OVERSAMPLED_SIDE,
        FftDirection::Inverse,
    );
    let (response_forward, response_forward_compile) = compile_square_resident_fft_executable(
        &device,
        &graph_device,
        RESPONSE_BATCH_PLANES,
        SIDE,
        FftDirection::Forward,
    );
    let pipeline_started = Instant::now();
    let pipelines = make_pipelines(&device);
    let pipeline_compile = pipeline_started.elapsed();

    let buffer_a = device
        .newBufferWithLength_options(FFT_BUFFER_BYTES, MTLResourceOptions::StorageModePrivate)
        .expect("physical ordered-response private buffer A");
    let buffer_b = device
        .newBufferWithLength_options(FFT_BUFFER_BYTES, MTLResourceOptions::StorageModePrivate)
        .expect("physical ordered-response private buffer B");
    let response = device
        .newBufferWithLength_options(RESPONSE_BYTES, MTLResourceOptions::StorageModePrivate)
        .expect("physical ordered-response response bank");
    let oversampled_batch_bytes = RESPONSE_BATCH_PLANES * OVERSAMPLED_PIXELS * COMPLEX_BYTES;
    let compact_batch_bytes = RESPONSE_BATCH_PLANES * PIXELS * COMPLEX_BYTES;
    let response_high_frequency = device
        .newBufferWithLength_options(
            oversampled_batch_bytes,
            MTLResourceOptions::StorageModePrivate,
        )
        .expect("physical oversampled response frequency batch");
    let response_high_lag = device
        .newBufferWithLength_options(
            oversampled_batch_bytes,
            MTLResourceOptions::StorageModePrivate,
        )
        .expect("physical oversampled response lag batch");
    let response_compact_lag = device
        .newBufferWithLength_options(compact_batch_bytes, MTLResourceOptions::StorageModePrivate)
        .expect("physical compact response lag batch");
    let response_compact_frequency = device
        .newBufferWithLength_options(compact_batch_bytes, MTLResourceOptions::StorageModePrivate)
        .expect("physical compact response frequency batch");
    let right_factors = copied_shared_buffer_from_slice(&device, &fixture.right_factors)
        .expect("physical resident right factors");
    let left_factors = copied_shared_buffer_from_slice(&device, &fixture.left_factors)
        .expect("physical resident left factors");
    let model_buffers = fixture.model_cases.each_ref().map(|values| {
        copied_shared_buffer_from_slice(&device, values).expect("physical resident model")
    });
    fixture.right_factors = Vec::new();
    fixture.left_factors = Vec::new();
    fixture.model_cases = std::array::from_fn(|_| Vec::new());
    let feedback = device
        .newBufferWithLength_options(FEEDBACK_BYTES, MTLResourceOptions::StorageModeShared)
        .expect("physical ordered-response feedback");

    let response_offsets_buffer = no_copy_bytes(&device, &response_offsets);
    let response_meta_buffer = no_copy_bytes(&device, &response_meta);
    let response_coefficients_buffer = no_copy_bytes(&device, &response_coefficients);
    let kernel_values = es_kernel_lut();
    let kernel_buffer =
        buffer_from_slice_no_copy(&device, &kernel_values).expect("physical response kernel LUT");
    let correction_axis = es_response_lag_correction_axis(OVERSAMPLED_SIDE);
    let correction_axis_buffer =
        buffer_from_slice_no_copy(&device, &correction_axis).expect("response correction axis");
    let high_shape = shape_array_batch(RESPONSE_BATCH_PLANES, OVERSAMPLED_SIDE, OVERSAMPLED_SIDE);
    let compact_shape = shape_array_batch(RESPONSE_BATCH_PLANES, SIDE, SIDE);
    let high_frequency_data = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &response_high_frequency,
            &high_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let high_lag_data = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &response_high_lag,
            &high_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let compact_lag_data = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &response_compact_lag,
            &compact_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let compact_frequency_data = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &response_compact_frequency,
            &compact_shape,
            MPSDataType::ComplexFloat32,
        )
    };
    let high_frequency_array = NSArray::from_slice(&[&*high_frequency_data]);
    let high_lag_array = NSArray::from_slice(&[&*high_lag_data]);
    let compact_lag_array = NSArray::from_slice(&[&*compact_lag_data]);
    let compact_frequency_array = NSArray::from_slice(&[&*compact_frequency_data]);
    let response_execution = unsafe { MPSGraphExecutableExecutionDescriptor::new() };
    unsafe {
        response_execution.setWaitUntilCompleted(false);
    }
    let response_build_started = Instant::now();
    for pair_base in (0..ORDERED_PAIRS).step_by(RESPONSE_BATCH_PAIRS) {
        let construction_command =
            unsafe { MPSCommandBuffer::commandBufferFromCommandQueue(&queue) };
        let construction_root = unsafe { construction_command.rootCommandBuffer() };
        encode_output_owner_es_construction(
            &construction_root,
            &pipelines.construct_es,
            &response_offsets_buffer,
            &response_meta_buffer,
            &response_coefficients_buffer,
            &kernel_buffer,
            &response_high_frequency,
            OVERSAMPLED_SIDE,
            RESPONSE_BATCH_PAIRS,
            pair_base,
            RESPONSE_COEFFICIENTS,
            response_groups,
        );
        let _response_inverse_result = unsafe {
            response_inverse
                .executable
                .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                    &construction_command,
                    &high_frequency_array,
                    Some(&high_lag_array),
                    Some(&response_execution),
                )
        };
        let after_response_inverse = unsafe { construction_command.rootCommandBuffer() };
        encode_compact_response_deapodization(
            &after_response_inverse,
            &pipelines.compact_deapodize_response,
            &response_high_lag,
            &correction_axis_buffer,
            &response_compact_lag,
        );
        assert!(
            std::ptr::eq(&*construction_root, &*after_response_inverse),
            "MPSGraph committed inside oversampled response construction"
        );
        after_response_inverse.commit();
        after_response_inverse.waitUntilCompleted();
        assert_ne!(
            after_response_inverse.status(),
            MTLCommandBufferStatus::Error,
            "oversampled response construction/deapodization failed"
        );
        let compact_command = unsafe { MPSCommandBuffer::commandBufferFromCommandQueue(&queue) };
        let compact_root = unsafe { compact_command.rootCommandBuffer() };
        let _response_forward_result = unsafe {
            response_forward
                .executable
                .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                    &compact_command,
                    &compact_lag_array,
                    Some(&compact_frequency_array),
                    Some(&response_execution),
                )
        };
        let after_compact_forward = unsafe { compact_command.rootCommandBuffer() };
        encode_copy_response_planes(
            &after_compact_forward,
            &pipelines.copy_response_planes,
            &response_compact_frequency,
            &response,
            pair_base * RESPONSE_COEFFICIENTS,
        );
        assert!(
            std::ptr::eq(&*compact_root, &*after_compact_forward),
            "MPSGraph committed inside compact response finalization"
        );
        after_compact_forward.commit();
        after_compact_forward.waitUntilCompleted();
        assert_ne!(
            after_compact_forward.status(),
            MTLCommandBufferStatus::Error,
            "compact response finalization failed"
        );
    }
    let response_construction = response_build_started.elapsed();
    let construction_input_bytes = response_offsets.len()
        + response_meta.len()
        + response_coefficients.len()
        + mem::size_of_val(kernel_values.as_slice())
        + mem::size_of_val(correction_axis.as_slice());
    drop(high_frequency_array);
    drop(high_lag_array);
    drop(compact_lag_array);
    drop(compact_frequency_array);
    drop(high_frequency_data);
    drop(high_lag_data);
    drop(compact_lag_data);
    drop(compact_frequency_data);
    drop(response_execution);
    drop(response_inverse);
    drop(response_forward);
    drop(response_offsets_buffer);
    drop(response_meta_buffer);
    drop(response_coefficients_buffer);
    drop(kernel_buffer);
    drop(correction_axis_buffer);
    drop(response_high_frequency);
    drop(response_high_lag);
    drop(response_compact_lag);
    drop(response_compact_frequency);
    drop(response_offsets);
    drop(response_meta);
    drop(response_coefficients);
    drop(kernel_values);
    drop(correction_axis);

    let (offset_values, action_values) = physical_ordered_response_actions(&fixture.pair_map);
    let action_offsets =
        buffer_from_slice_no_copy(&device, &offset_values).expect("physical action offsets");
    let actions = buffer_from_slice_no_copy(&device, &action_values).expect("physical actions");
    let active_indices = buffer_from_slice_no_copy(&device, &fixture.active_indices)
        .expect("physical active indices");

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

    let mut actual =
        Vec::with_capacity(MODEL_TERMS * OUTPUT_TERMS * fixture.probe_active_ordinals.len());
    let mut application_samples = Vec::with_capacity(MODEL_TERMS);
    for model in &model_buffers {
        let sample = execute_ordered_response(
            &queue,
            &pipelines,
            &forward,
            &inverse,
            &buffer_a,
            &buffer_b,
            model,
            &right_factors,
            &response,
            &left_factors,
            &action_offsets,
            &actions,
            &active_indices,
            &feedback,
            1.0,
            &forward_inputs,
            &forward_results,
            &inverse_inputs,
            &inverse_results,
            &execution,
        );
        application_samples.push(sample);
        let values = shared_slice::<Complex32>(&feedback, ACTIVE_PIXELS * OUTPUT_TERMS);
        for output_term in 0..OUTPUT_TERMS {
            for &active_ordinal in &fixture.probe_active_ordinals {
                let value = values[output_term * ACTIVE_PIXELS + active_ordinal];
                actual.push(Complex64::new(f64::from(value.re), f64::from(value.im)));
            }
        }
    }
    let (contracted_l2, contracted_linf, exact_l2, exact_linf) =
        physical_feedback_metrics(&actual, &fixture);
    let mut selected_contracted = Vec::with_capacity(actual.len());
    for model_case in 0..MODEL_TERMS {
        for output_term in 0..OUTPUT_TERMS {
            for &receipt_probe in &fixture.probe_receipt_indices {
                selected_contracted.push(
                    fixture.expected_contracted
                        [(model_case * OUTPUT_TERMS + output_term) * 28 + receipt_probe],
                );
            }
        }
    }
    let scale_numerator = actual
        .iter()
        .zip(&selected_contracted)
        .map(|(candidate, reference)| reference.conj() * candidate)
        .sum::<Complex64>();
    let scale_denominator = selected_contracted
        .iter()
        .map(Complex64::norm_sqr)
        .sum::<f64>();
    let fitted_scale = scale_numerator / scale_denominator;
    eprintln!(
        "physical_ordered_response_diagnostic contracted_l2={contracted_l2:.9e} \
         contracted_linf={contracted_linf:.9e} exact_l2={exact_l2:.9e} \
         exact_linf={exact_linf:.9e} fitted_scale={fitted_scale:?}",
    );
    assert!(
        contracted_l2 <= 2.0e-5 && contracted_linf <= 2.0e-5,
        "physical resident operator differs from the contracted f64 reference: \
         l2={contracted_l2} linf={contracted_linf}"
    );
    assert!(
        exact_l2 <= 2.0e-5 && exact_linf <= 2.0e-5,
        "physical resident operator differs from the direct exact-W f64 reference: \
         l2={exact_l2} linf={exact_linf}"
    );

    let hot_samples = (0..MEASURED_APPLICATIONS)
        .map(|_| {
            execute_ordered_response(
                &queue,
                &pipelines,
                &forward,
                &inverse,
                &buffer_a,
                &buffer_b,
                &model_buffers[0],
                &right_factors,
                &response,
                &left_factors,
                &action_offsets,
                &actions,
                &active_indices,
                &feedback,
                1.0,
                &forward_inputs,
                &forward_results,
                &inverse_inputs,
                &inverse_results,
                &execution,
            )
        })
        .collect::<Vec<_>>();
    let hot_total = hot_samples
        .iter()
        .map(|sample| sample.total)
        .collect::<Vec<_>>();
    let hot_device = hot_samples
        .iter()
        .map(|sample| sample.device)
        .collect::<Vec<_>>();
    let hot_total_p50 = duration_nearest_rank(&hot_total, 5, 10);
    let hot_total_p90 = duration_nearest_rank(&hot_total, 9, 10);
    let hot_device_p50 = duration_nearest_rank(&hot_device, 5, 10);
    let hot_device_p90 = duration_nearest_rank(&hot_device, 9, 10);

    let hot_resident_bytes = 2 * FFT_BUFFER_BYTES
        + RESPONSE_BYTES
        + RIGHT_FACTOR_BYTES
        + LEFT_FACTOR_BYTES
        + MODEL_TERMS * MODEL_BYTES
        + FEEDBACK_BYTES
        + mem::size_of_val(offset_values.as_slice())
        + mem::size_of_val(action_values.as_slice())
        + mem::size_of_val(fixture.active_indices.as_slice());
    let construction_transient_gpu_bytes = 2 * oversampled_batch_bytes + 2 * compact_batch_bytes;
    let construction_peak_ledger_bytes =
        hot_resident_bytes + construction_transient_gpu_bytes + construction_input_bytes;
    eprintln!(
        "vlass_physical_private_resident_ordered_response_probe \
         receipt_sha256={} fixture_load_s={:.9} \
         forward_compile_s={:.9} inverse_compile_s={:.9} \
         response_inverse_compile_s={:.9} response_forward_compile_s={:.9} \
         pipeline_compile_s={:.9} response_construction_s={:.9} \
         response_groups={} logical_actions={} hot_resident_bytes={} \
         construction_transient_gpu_bytes={} construction_input_bytes={} \
         construction_peak_ledger_bytes={} correctness_application_total_s={:?} \
         hot_applications={} hot_total_p50_s={:.9} hot_total_p90_s={:.9} \
         hot_device_p50_s={:.9} hot_device_p90_s={:.9} \
         contracted_relative_l2={:.9e} contracted_normalized_linf={:.9e} \
         exact_w_relative_l2={:.9e} exact_w_normalized_linf={:.9e}",
        fixture.receipt_sha256,
        fixture_load.as_secs_f64(),
        forward_compile.as_secs_f64(),
        inverse_compile.as_secs_f64(),
        response_inverse_compile.as_secs_f64(),
        response_forward_compile.as_secs_f64(),
        pipeline_compile.as_secs_f64(),
        response_construction.as_secs_f64(),
        response_groups,
        action_values.len(),
        hot_resident_bytes,
        construction_transient_gpu_bytes,
        construction_input_bytes,
        construction_peak_ledger_bytes,
        application_samples
            .iter()
            .map(|sample| sample.total.as_secs_f64())
            .collect::<Vec<_>>(),
        MEASURED_APPLICATIONS,
        hot_total_p50.as_secs_f64(),
        hot_total_p90.as_secs_f64(),
        hot_device_p50.as_secs_f64(),
        hot_device_p90.as_secs_f64(),
        contracted_l2,
        contracted_linf,
        exact_l2,
        exact_linf,
    );
}
