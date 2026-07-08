// SPDX-License-Identifier: LGPL-3.0-or-later
//! Apple-native FFT adapters used behind the shared imaging FFT contract.

use std::{
    cell::RefCell,
    collections::{HashMap, hash_map::Entry},
    env,
    ffi::c_void,
    mem,
    ptr::{NonNull, null_mut},
    slice, thread,
    time::{Duration, Instant},
};

use ndarray::Array2;
use num_complex::{Complex32, Complex64};
use objc2::runtime::ProtocolObject;
use objc2::{AnyThread, rc::Retained};
use objc2_foundation::{NSArray, NSDictionary, NSNumber, NSString};
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
    MTLComputeCommandEncoder, MTLComputePipelineState, MTLCreateSystemDefaultDevice, MTLDevice,
    MTLLibrary, MTLResourceOptions, MTLSize,
};
use objc2_metal_performance_shaders::MPSDataType;
use objc2_metal_performance_shaders_graph::{
    MPSGraph, MPSGraphFFTDescriptor, MPSGraphFFTScalingMode, MPSGraphTensor, MPSGraphTensorData,
    MPSGraphTensorDataDictionary,
};

use crate::fft::{fftshift2, ifftshift2};
use crate::fft_backend::{Fft2Spec, FftBackendSelection, FftDirection, FftTiming};

type MetalDevice = Retained<ProtocolObject<dyn MTLDevice>>;
type MetalQueue = Retained<ProtocolObject<dyn MTLCommandQueue>>;
type MetalBuffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type MetalCommandBuffer = Retained<ProtocolObject<dyn MTLCommandBuffer>>;
type MetalComputePipeline = Retained<ProtocolObject<dyn MTLComputePipelineState>>;

const APPLE_FFT_PACK_THREADS_ENV: &str = "CASA_RS_APPLE_FFT_PACK_THREADS";
const APPLE_FFT_FUSED_PACK_ENV: &str = "CASA_RS_APPLE_FFT_FUSED_PACK";
const APPLE_FFT_PARALLEL_PACK_MIN_ELEMENTS: usize = 1_000_000;
const APPLE_FFT_FUSED_PACK_SHADER: &str = r#"
#include <metal_stdlib>
using namespace metal;

struct FftPackParams {
    uint rows;
    uint columns;
    uint plane;
    uint _pad0;
};

static inline uint round_shift_right_to_uint(ulong value, uint shift) {
    if (shift == 0) {
        return uint(value);
    }
    if (shift >= 64) {
        return 0;
    }
    ulong truncated = value >> shift;
    ulong mask = (ulong(1) << shift) - ulong(1);
    ulong remainder = value & mask;
    ulong halfway = ulong(1) << (shift - 1);
    if (remainder > halfway || (remainder == halfway && (truncated & ulong(1)) != 0)) {
        truncated += ulong(1);
    }
    return uint(truncated);
}

static inline float narrow_f64_bits_to_f32(ulong bits) {
    uint sign = uint(bits >> 63);
    uint exponent = uint((bits >> 52) & ulong(0x7ff));
    ulong fraction = bits & ulong(0x000fffffffffffff);
    uint output_sign = sign << 31;

    if (exponent == 0x7ff) {
        uint output_fraction = fraction == 0 ? 0 : (uint(fraction >> 29) | 1u);
        return as_type<float>(output_sign | 0x7f800000u | (output_fraction & 0x007fffffu));
    }
    if (exponent == 0) {
        return as_type<float>(output_sign);
    }

    int unbiased = int(exponent) - 1023;
    int output_exponent = unbiased + 127;
    ulong mantissa = (ulong(1) << 52) | fraction;

    if (output_exponent >= 255) {
        return as_type<float>(output_sign | 0x7f800000u);
    }
    if (output_exponent > 0) {
        uint mantissa24 = round_shift_right_to_uint(mantissa, 29);
        if (mantissa24 == 0x01000000u) {
            output_exponent += 1;
            if (output_exponent >= 255) {
                return as_type<float>(output_sign | 0x7f800000u);
            }
            return as_type<float>(output_sign | (uint(output_exponent) << 23));
        }
        return as_type<float>(
            output_sign | (uint(output_exponent) << 23) | (mantissa24 & 0x007fffffu)
        );
    }

    int subnormal_shift = -unbiased - 97;
    if (subnormal_shift >= 64) {
        return as_type<float>(output_sign);
    }
    uint subnormal = round_shift_right_to_uint(mantissa, uint(subnormal_shift));
    if (subnormal == 0) {
        return as_type<float>(output_sign);
    }
    if (subnormal >= 0x00800000u) {
        return as_type<float>(output_sign | 0x00800000u);
    }
    return as_type<float>(output_sign | subnormal);
}

kernel void pack_ifftshifted_f32(
    device const float *source [[buffer(0)]],
    device float *output [[buffer(1)]],
    constant FftPackParams &params [[buffer(2)]],
    uint2 position [[thread_position_in_grid]]
) {
    uint column = position.x;
    uint row = position.y;
    if (row >= params.rows || column >= params.columns) {
        return;
    }
    uint source_row = (row + ((params.rows + 1) >> 1)) % params.rows;
    uint source_column = (column + ((params.columns + 1) >> 1)) % params.columns;
    ulong source_index = (ulong(source_row) * ulong(params.columns) + ulong(source_column)) * 2;
    ulong output_index =
        (ulong(params.plane) * ulong(params.rows) * ulong(params.columns)
            + ulong(row) * ulong(params.columns)
            + ulong(column)) * 2;
    output[output_index] = source[source_index];
    output[output_index + 1] = source[source_index + 1];
}

kernel void pack_ifftshifted_f64_to_f32(
    device const ulong *source [[buffer(0)]],
    device float *output [[buffer(1)]],
    constant FftPackParams &params [[buffer(2)]],
    uint2 position [[thread_position_in_grid]]
) {
    uint column = position.x;
    uint row = position.y;
    if (row >= params.rows || column >= params.columns) {
        return;
    }
    uint source_row = (row + ((params.rows + 1) >> 1)) % params.rows;
    uint source_column = (column + ((params.columns + 1) >> 1)) % params.columns;
    ulong source_index = (ulong(source_row) * ulong(params.columns) + ulong(source_column)) * 2;
    ulong output_index =
        (ulong(params.plane) * ulong(params.rows) * ulong(params.columns)
            + ulong(row) * ulong(params.columns)
            + ulong(column)) * 2;
    output[output_index] = narrow_f64_bits_to_f32(source[source_index]);
    output[output_index + 1] = narrow_f64_bits_to_f32(source[source_index + 1]);
}
"#;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MpsGraphPlanKey {
    rows: usize,
    columns: usize,
    direction: FftDirection,
}

struct MpsGraphF32Plan {
    device: MetalDevice,
    queue: MetalQueue,
    graph: Retained<MPSGraph>,
    shape: Retained<NSArray<NSNumber>>,
    placeholder: Retained<MPSGraphTensor>,
    output: Retained<MPSGraphTensor>,
    target_tensors: Retained<NSArray<MPSGraphTensor>>,
    fused_pack: Option<FusedPackPipelines>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct MpsGraphBatchPlanKey {
    rows: usize,
    columns: usize,
    batch: usize,
    direction: FftDirection,
    fused_pack: bool,
}

struct FusedPackPipelines {
    f32: MetalComputePipeline,
    f64_to_f32: MetalComputePipeline,
}

struct PendingFusedPack {
    command_buffer: MetalCommandBuffer,
    _source_buffers: Vec<MetalBuffer>,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct FusedPackParams {
    rows: u32,
    columns: u32,
    plane: u32,
    _pad0: u32,
}

thread_local! {
    static MPSGRAPH_F32_PLANS: RefCell<HashMap<MpsGraphPlanKey, MpsGraphF32Plan>> =
        RefCell::new(HashMap::new());
    static MPSGRAPH_F32_BATCH_PLANS: RefCell<HashMap<MpsGraphBatchPlanKey, MpsGraphF32Plan>> =
        RefCell::new(HashMap::new());
}

pub(crate) fn mpsgraph_f32_available() -> bool {
    MTLCreateSystemDefaultDevice().is_some()
}

pub(crate) fn centered_transform_f32(
    input: &Array2<Complex32>,
    spec: Fft2Spec,
    selection: FftBackendSelection,
) -> Result<(Array2<Complex32>, FftTiming), &'static str> {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    if rows == 0 || columns == 0 {
        return Err("mpsgraph_fft_requires_non_empty_shape");
    }
    if spec.shape.batch != 1 {
        return Err("mpsgraph_adapter_currently_supports_single_plane");
    }

    let mut timing = FftTiming::new(spec, selection);
    let total_start = Instant::now();
    let key = MpsGraphPlanKey {
        rows,
        columns,
        direction: spec.direction,
    };

    let output = MPSGRAPH_F32_PLANS.with(|plans| {
        let mut plans = plans.borrow_mut();
        let plan = match plans.entry(key) {
            Entry::Occupied(entry) => {
                timing.plan_cache_hit = true;
                entry.into_mut()
            }
            Entry::Vacant(entry) => {
                let plan_start = Instant::now();
                let plan = make_plan(key)?;
                timing.plan += plan_start.elapsed();
                entry.insert(plan)
            }
        };
        execute_with_plan(plan, input, &mut timing)
    })?;
    timing.total = total_start.elapsed();
    Ok((output, timing))
}

pub(crate) fn centered_transform_f32_batch(
    inputs: &[Array2<Complex32>],
    spec: Fft2Spec,
    selection: FftBackendSelection,
) -> Result<(Vec<Array2<Complex32>>, FftTiming), &'static str> {
    if inputs.is_empty() {
        return Err("mpsgraph_batch_fft_requires_non_empty_batch");
    }
    let rows = inputs[0].shape()[0];
    let columns = inputs[0].shape()[1];
    if rows == 0 || columns == 0 {
        return Err("mpsgraph_fft_requires_non_empty_shape");
    }
    if spec.shape.batch != inputs.len() {
        return Err("mpsgraph_batch_fft_spec_batch_mismatch");
    }
    for input in inputs {
        if input.shape() != [rows, columns] {
            return Err("mpsgraph_batch_fft_requires_uniform_shape");
        }
    }

    let fused_pack = apple_fft_fused_pack_enabled();
    let selection = selection_for_fused_pack(selection, fused_pack);
    let mut timing = FftTiming::new(spec, selection);
    let total_start = Instant::now();
    let key = MpsGraphBatchPlanKey {
        rows,
        columns,
        batch: inputs.len(),
        direction: spec.direction,
        fused_pack,
    };

    let output = MPSGRAPH_F32_BATCH_PLANS.with(|plans| {
        let mut plans = plans.borrow_mut();
        let plan = match plans.entry(key) {
            Entry::Occupied(entry) => {
                timing.plan_cache_hit = true;
                entry.into_mut()
            }
            Entry::Vacant(entry) => {
                let plan_start = Instant::now();
                let plan = make_batch_plan(key)?;
                timing.plan += plan_start.elapsed();
                entry.insert(plan)
            }
        };
        execute_batch_with_plan(plan, inputs, &mut timing)
    })?;
    timing.total = total_start.elapsed();
    Ok((output, timing))
}

pub(crate) fn centered_transform_f64_to_f32_batch(
    inputs: &[Array2<Complex64>],
    spec: Fft2Spec,
    selection: FftBackendSelection,
) -> Result<(Vec<Array2<Complex32>>, FftTiming), &'static str> {
    if inputs.is_empty() {
        return Err("mpsgraph_batch_fft_requires_non_empty_batch");
    }
    let rows = inputs[0].shape()[0];
    let columns = inputs[0].shape()[1];
    if rows == 0 || columns == 0 {
        return Err("mpsgraph_fft_requires_non_empty_shape");
    }
    if spec.shape.batch != inputs.len() {
        return Err("mpsgraph_batch_fft_spec_batch_mismatch");
    }
    for input in inputs {
        if input.shape() != [rows, columns] {
            return Err("mpsgraph_batch_fft_requires_uniform_shape");
        }
        if input.as_slice_memory_order().is_none() {
            return Err("mpsgraph_f64_to_f32_batch_requires_contiguous_inputs");
        }
    }

    let fused_pack = apple_fft_fused_pack_enabled();
    let selection = selection_for_fused_pack(selection, fused_pack);
    let mut timing = FftTiming::new(spec, selection);
    let total_start = Instant::now();
    let key = MpsGraphBatchPlanKey {
        rows,
        columns,
        batch: inputs.len(),
        direction: spec.direction,
        fused_pack,
    };

    let output = MPSGRAPH_F32_BATCH_PLANS.with(|plans| {
        let mut plans = plans.borrow_mut();
        let plan = match plans.entry(key) {
            Entry::Occupied(entry) => {
                timing.plan_cache_hit = true;
                entry.into_mut()
            }
            Entry::Vacant(entry) => {
                let plan_start = Instant::now();
                let plan = make_batch_plan(key)?;
                timing.plan += plan_start.elapsed();
                entry.insert(plan)
            }
        };
        execute_f64_to_f32_batch_with_plan(plan, inputs, &mut timing)
    })?;
    timing.total = total_start.elapsed();
    Ok((output, timing))
}

fn make_plan(key: MpsGraphPlanKey) -> Result<MpsGraphF32Plan, &'static str> {
    let device = MTLCreateSystemDefaultDevice().ok_or("mpsgraph_no_default_metal_device")?;
    let queue = device
        .newCommandQueue()
        .ok_or("mpsgraph_failed_to_create_command_queue")?;
    let graph = unsafe { MPSGraph::new() };
    let shape = shape_array(key.rows, key.columns);
    let placeholder = unsafe {
        graph.placeholderWithShape_dataType_name(Some(&shape), MPSDataType::ComplexFloat32, None)
    };
    let descriptor =
        unsafe { MPSGraphFFTDescriptor::descriptor() }.ok_or("mpsgraph_fft_descriptor_failed")?;
    unsafe {
        descriptor.setInverse(key.direction == FftDirection::Inverse);
        descriptor.setScalingMode(if key.direction == FftDirection::Inverse {
            MPSGraphFFTScalingMode::Size
        } else {
            MPSGraphFFTScalingMode::None
        });
    }
    let axes = axes_array();
    let output = unsafe {
        graph.fastFourierTransformWithTensor_axes_descriptor_name(
            &placeholder,
            &axes,
            &descriptor,
            None,
        )
    };
    let target_tensors = NSArray::from_slice(&[&*output]);
    Ok(MpsGraphF32Plan {
        device,
        queue,
        graph,
        shape,
        placeholder,
        output,
        target_tensors,
        fused_pack: None,
    })
}

fn make_batch_plan(key: MpsGraphBatchPlanKey) -> Result<MpsGraphF32Plan, &'static str> {
    let device = MTLCreateSystemDefaultDevice().ok_or("mpsgraph_no_default_metal_device")?;
    let queue = device
        .newCommandQueue()
        .ok_or("mpsgraph_failed_to_create_command_queue")?;
    let graph = unsafe { MPSGraph::new() };
    let shape = shape_array_batch(key.batch, key.rows, key.columns);
    let placeholder = unsafe {
        graph.placeholderWithShape_dataType_name(Some(&shape), MPSDataType::ComplexFloat32, None)
    };
    let descriptor =
        unsafe { MPSGraphFFTDescriptor::descriptor() }.ok_or("mpsgraph_fft_descriptor_failed")?;
    unsafe {
        descriptor.setInverse(key.direction == FftDirection::Inverse);
        descriptor.setScalingMode(if key.direction == FftDirection::Inverse {
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
    let fused_pack = if key.fused_pack {
        Some(make_fused_pack_pipelines(&device)?)
    } else {
        None
    };
    Ok(MpsGraphF32Plan {
        device,
        queue,
        graph,
        shape,
        placeholder,
        output,
        target_tensors,
        fused_pack,
    })
}

fn execute_with_plan(
    plan: &MpsGraphF32Plan,
    input: &Array2<Complex32>,
    timing: &mut FftTiming,
) -> Result<Array2<Complex32>, &'static str> {
    let rows = input.shape()[0];
    let columns = input.shape()[1];
    let element_count = rows * columns;

    let pack_start = Instant::now();
    let shifted = ifftshift2(input);
    let mut packed = Vec::with_capacity(element_count * 2);
    for value in shifted.iter() {
        packed.push(value.re);
        packed.push(value.im);
    }
    timing.pack += pack_start.elapsed();

    let transfer_start = Instant::now();
    let input_buffer = buffer_from_f32_slice(&plan.device, &packed)?;
    let output_buffer = empty_buffer(&plan.device, packed.len() * mem::size_of::<f32>())?;
    let tensor_data = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &input_buffer,
            &plan.shape,
            MPSDataType::ComplexFloat32,
        )
    };
    timing.transfer_to_device += transfer_start.elapsed();

    let exec_start = Instant::now();
    let feeds: Retained<MPSGraphTensorDataDictionary> =
        NSDictionary::from_slices(&[&*plan.placeholder], &[&*tensor_data]);
    let results = unsafe {
        plan.graph
            .runWithMTLCommandQueue_feeds_targetTensors_targetOperations(
                &plan.queue,
                &feeds,
                &plan.target_tensors,
                None,
            )
    };
    timing.exec += exec_start.elapsed();

    let result = results
        .objectForKey(&plan.output)
        .ok_or("mpsgraph_fft_missing_output_tensor")?;
    let export_start = Instant::now();
    let ndarray = unsafe { result.mpsndarray() };
    let command_buffer = plan
        .queue
        .commandBuffer()
        .ok_or("mpsgraph_failed_to_create_export_command_buffer")?;
    unsafe {
        ndarray.exportDataWithCommandBuffer_toBuffer_destinationDataType_offset_rowStrides(
            &command_buffer,
            &output_buffer,
            MPSDataType::ComplexFloat32,
            0,
            null_mut(),
        );
    }
    command_buffer.commit();
    timing.transfer_from_device += export_start.elapsed();

    let sync_start = Instant::now();
    command_buffer.waitUntilCompleted();
    timing.sync += sync_start.elapsed();

    let pack_start = Instant::now();
    let values = unsafe {
        slice::from_raw_parts(
            output_buffer.contents().as_ptr().cast::<f32>(),
            packed.len(),
        )
    };
    let mut unshifted = Array2::<Complex32>::zeros((rows, columns));
    for (cell, pair) in unshifted.iter_mut().zip(values.chunks_exact(2)) {
        *cell = Complex32::new(pair[0], pair[1]);
    }
    let output = fftshift2(&unshifted);
    timing.pack += pack_start.elapsed();
    Ok(output)
}

fn execute_batch_with_plan(
    plan: &MpsGraphF32Plan,
    inputs: &[Array2<Complex32>],
    timing: &mut FftTiming,
) -> Result<Vec<Array2<Complex32>>, &'static str> {
    let rows = inputs[0].shape()[0];
    let columns = inputs[0].shape()[1];
    let element_count = rows * columns;
    let packed_len = inputs.len() * element_count * 2;

    let transfer_start = Instant::now();
    let input_buffer = empty_buffer(&plan.device, packed_len * mem::size_of::<f32>())?;
    let output_buffer = empty_buffer(&plan.device, packed_len * mem::size_of::<f32>())?;
    timing.transfer_to_device += transfer_start.elapsed();

    let pending_fused_pack = if let Some(pipelines) = plan.fused_pack.as_ref() {
        Some(pack_ifftshifted_f32_batch_with_metal(
            plan,
            &pipelines.f32,
            inputs,
            rows,
            columns,
            &input_buffer,
            timing,
        )?)
    } else {
        let pack_start = Instant::now();
        let input_values = unsafe {
            slice::from_raw_parts_mut(input_buffer.contents().as_ptr().cast::<f32>(), packed_len)
        };
        pack_ifftshifted_f32_batch_into(inputs, rows, columns, input_values)?;
        timing.pack += pack_start.elapsed();
        None
    };

    let transfer_start = Instant::now();
    let tensor_data = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &input_buffer,
            &plan.shape,
            MPSDataType::ComplexFloat32,
        )
    };
    timing.transfer_to_device += transfer_start.elapsed();

    let exec_start = Instant::now();
    let feeds: Retained<MPSGraphTensorDataDictionary> =
        NSDictionary::from_slices(&[&*plan.placeholder], &[&*tensor_data]);
    let results = unsafe {
        plan.graph
            .runWithMTLCommandQueue_feeds_targetTensors_targetOperations(
                &plan.queue,
                &feeds,
                &plan.target_tensors,
                None,
            )
    };
    timing.exec += exec_start.elapsed();

    let result = results
        .objectForKey(&plan.output)
        .ok_or("mpsgraph_fft_missing_output_tensor")?;
    let export_start = Instant::now();
    let ndarray = unsafe { result.mpsndarray() };
    let command_buffer = plan
        .queue
        .commandBuffer()
        .ok_or("mpsgraph_failed_to_create_export_command_buffer")?;
    unsafe {
        ndarray.exportDataWithCommandBuffer_toBuffer_destinationDataType_offset_rowStrides(
            &command_buffer,
            &output_buffer,
            MPSDataType::ComplexFloat32,
            0,
            null_mut(),
        );
    }
    command_buffer.commit();
    timing.transfer_from_device += export_start.elapsed();

    let sync_start = Instant::now();
    command_buffer.waitUntilCompleted();
    timing.sync += sync_start.elapsed();
    if let Some(pending) = pending_fused_pack {
        finish_fused_pack(pending, timing)?;
    }

    let pack_start = Instant::now();
    let values = unsafe {
        slice::from_raw_parts(output_buffer.contents().as_ptr().cast::<f32>(), packed_len)
    };
    let outputs = unpack_fftshifted_f32_batch(values, inputs.len(), element_count, rows, columns);
    timing.pack += pack_start.elapsed();
    Ok(outputs)
}

fn execute_f64_to_f32_batch_with_plan(
    plan: &MpsGraphF32Plan,
    inputs: &[Array2<Complex64>],
    timing: &mut FftTiming,
) -> Result<Vec<Array2<Complex32>>, &'static str> {
    let rows = inputs[0].shape()[0];
    let columns = inputs[0].shape()[1];
    let element_count = rows * columns;
    let packed_len = inputs.len() * element_count * 2;

    let transfer_start = Instant::now();
    let input_buffer = empty_buffer(&plan.device, packed_len * mem::size_of::<f32>())?;
    let output_buffer = empty_buffer(&plan.device, packed_len * mem::size_of::<f32>())?;
    timing.transfer_to_device += transfer_start.elapsed();

    let pending_fused_pack = if let Some(pipelines) = plan.fused_pack.as_ref() {
        Some(pack_ifftshifted_f64_batch_as_f32_with_metal(
            plan,
            &pipelines.f64_to_f32,
            inputs,
            rows,
            columns,
            &input_buffer,
            timing,
        )?)
    } else {
        let pack_start = Instant::now();
        let input_values = unsafe {
            slice::from_raw_parts_mut(input_buffer.contents().as_ptr().cast::<f32>(), packed_len)
        };
        pack_ifftshifted_f64_batch_as_f32_into(inputs, rows, columns, input_values)?;
        timing.pack += pack_start.elapsed();
        None
    };

    let transfer_start = Instant::now();
    let tensor_data = unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            &input_buffer,
            &plan.shape,
            MPSDataType::ComplexFloat32,
        )
    };
    timing.transfer_to_device += transfer_start.elapsed();

    let exec_start = Instant::now();
    let feeds: Retained<MPSGraphTensorDataDictionary> =
        NSDictionary::from_slices(&[&*plan.placeholder], &[&*tensor_data]);
    let results = unsafe {
        plan.graph
            .runWithMTLCommandQueue_feeds_targetTensors_targetOperations(
                &plan.queue,
                &feeds,
                &plan.target_tensors,
                None,
            )
    };
    timing.exec += exec_start.elapsed();

    let result = results
        .objectForKey(&plan.output)
        .ok_or("mpsgraph_fft_missing_output_tensor")?;
    let export_start = Instant::now();
    let ndarray = unsafe { result.mpsndarray() };
    let command_buffer = plan
        .queue
        .commandBuffer()
        .ok_or("mpsgraph_failed_to_create_export_command_buffer")?;
    unsafe {
        ndarray.exportDataWithCommandBuffer_toBuffer_destinationDataType_offset_rowStrides(
            &command_buffer,
            &output_buffer,
            MPSDataType::ComplexFloat32,
            0,
            null_mut(),
        );
    }
    command_buffer.commit();
    timing.transfer_from_device += export_start.elapsed();

    let sync_start = Instant::now();
    command_buffer.waitUntilCompleted();
    timing.sync += sync_start.elapsed();
    if let Some(pending) = pending_fused_pack {
        finish_fused_pack(pending, timing)?;
    }

    let pack_start = Instant::now();
    let values = unsafe {
        slice::from_raw_parts(output_buffer.contents().as_ptr().cast::<f32>(), packed_len)
    };
    let outputs = unpack_fftshifted_f32_batch(values, inputs.len(), element_count, rows, columns);
    timing.pack += pack_start.elapsed();
    Ok(outputs)
}

fn shape_array(rows: usize, columns: usize) -> Retained<NSArray<NSNumber>> {
    NSArray::from_retained_slice(&[
        NSNumber::new_i64(rows as i64),
        NSNumber::new_i64(columns as i64),
    ])
}

fn shape_array_batch(batch: usize, rows: usize, columns: usize) -> Retained<NSArray<NSNumber>> {
    NSArray::from_retained_slice(&[
        NSNumber::new_i64(batch as i64),
        NSNumber::new_i64(rows as i64),
        NSNumber::new_i64(columns as i64),
    ])
}

fn axes_array() -> Retained<NSArray<NSNumber>> {
    NSArray::from_retained_slice(&[NSNumber::new_i64(0), NSNumber::new_i64(1)])
}

fn axes_array_batch() -> Retained<NSArray<NSNumber>> {
    NSArray::from_retained_slice(&[NSNumber::new_i64(1), NSNumber::new_i64(2)])
}

fn buffer_from_f32_slice(
    device: &MetalDevice,
    values: &[f32],
) -> Result<MetalBuffer, &'static str> {
    let byte_len = mem::size_of_val(values);
    let pointer = NonNull::new(values.as_ptr().cast::<c_void>() as *mut c_void)
        .ok_or("mpsgraph_input_buffer_pointer_was_null")?;
    unsafe {
        device
            .newBufferWithBytes_length_options(
                pointer,
                byte_len,
                MTLResourceOptions::StorageModeShared,
            )
            .ok_or("mpsgraph_failed_to_allocate_input_buffer")
    }
}

fn empty_buffer(device: &MetalDevice, byte_len: usize) -> Result<MetalBuffer, &'static str> {
    device
        .newBufferWithLength_options(byte_len, MTLResourceOptions::StorageModeShared)
        .ok_or("mpsgraph_failed_to_allocate_output_buffer")
}

fn buffer_from_slice_no_copy<T>(
    device: &MetalDevice,
    values: &[T],
) -> Result<MetalBuffer, &'static str> {
    let byte_len = mem::size_of_val(values);
    if byte_len == 0 {
        return Err("mpsgraph_no_copy_buffer_requires_non_empty_input");
    }
    let pointer = NonNull::new(values.as_ptr().cast::<c_void>() as *mut c_void)
        .ok_or("mpsgraph_input_buffer_pointer_was_null")?;
    unsafe {
        device
            .newBufferWithBytesNoCopy_length_options_deallocator(
                pointer,
                byte_len,
                MTLResourceOptions::StorageModeShared,
                None,
            )
            .ok_or("mpsgraph_failed_to_wrap_input_buffer_no_copy")
    }
}

fn make_fused_pack_pipelines(device: &MetalDevice) -> Result<FusedPackPipelines, &'static str> {
    let source = NSString::from_str(APPLE_FFT_FUSED_PACK_SHADER);
    let library = device
        .newLibraryWithSource_options_error(&source, None)
        .map_err(|_| "mpsgraph_fused_pack_shader_compile_failed")?;
    let f32_function_name = NSString::from_str("pack_ifftshifted_f32");
    let f32_function = library
        .newFunctionWithName(&f32_function_name)
        .ok_or("mpsgraph_fused_f32_pack_function_not_found")?;
    let f32 = device
        .newComputePipelineStateWithFunction_error(&f32_function)
        .map_err(|_| "mpsgraph_fused_f32_pack_pipeline_failed")?;
    let f64_function_name = NSString::from_str("pack_ifftshifted_f64_to_f32");
    let f64_function = library
        .newFunctionWithName(&f64_function_name)
        .ok_or("mpsgraph_fused_f64_pack_function_not_found")?;
    let f64_to_f32 = device
        .newComputePipelineStateWithFunction_error(&f64_function)
        .map_err(|_| "mpsgraph_fused_f64_pack_pipeline_failed")?;
    Ok(FusedPackPipelines { f32, f64_to_f32 })
}

fn pack_ifftshifted_f32_batch_with_metal(
    plan: &MpsGraphF32Plan,
    pipeline: &MetalComputePipeline,
    inputs: &[Array2<Complex32>],
    rows: usize,
    columns: usize,
    output_buffer: &MetalBuffer,
    timing: &mut FftTiming,
) -> Result<PendingFusedPack, &'static str> {
    let mut source_buffers = Vec::with_capacity(inputs.len());
    for input in inputs {
        if input.shape() != [rows, columns] {
            return Err("mpsgraph_batch_fft_requires_uniform_shape");
        }
        let input = input
            .as_slice_memory_order()
            .ok_or("mpsgraph_batch_fft_requires_contiguous_inputs")?;
        source_buffers.push(buffer_from_slice_no_copy(&plan.device, input)?);
    }
    dispatch_fused_pack_kernel(
        plan,
        pipeline,
        &source_buffers,
        rows,
        columns,
        output_buffer,
        timing,
    )
}

fn pack_ifftshifted_f64_batch_as_f32_with_metal(
    plan: &MpsGraphF32Plan,
    pipeline: &MetalComputePipeline,
    inputs: &[Array2<Complex64>],
    rows: usize,
    columns: usize,
    output_buffer: &MetalBuffer,
    timing: &mut FftTiming,
) -> Result<PendingFusedPack, &'static str> {
    let mut source_buffers = Vec::with_capacity(inputs.len());
    for input in inputs {
        if input.shape() != [rows, columns] {
            return Err("mpsgraph_batch_fft_requires_uniform_shape");
        }
        let input = input
            .as_slice_memory_order()
            .ok_or("mpsgraph_f64_to_f32_batch_requires_contiguous_inputs")?;
        source_buffers.push(buffer_from_slice_no_copy(&plan.device, input)?);
    }
    dispatch_fused_pack_kernel(
        plan,
        pipeline,
        &source_buffers,
        rows,
        columns,
        output_buffer,
        timing,
    )
}

fn dispatch_fused_pack_kernel(
    plan: &MpsGraphF32Plan,
    pipeline: &MetalComputePipeline,
    source_buffers: &[MetalBuffer],
    rows: usize,
    columns: usize,
    output_buffer: &MetalBuffer,
    timing: &mut FftTiming,
) -> Result<PendingFusedPack, &'static str> {
    let rows_u32 = u32::try_from(rows).map_err(|_| "mpsgraph_fused_pack_rows_exceed_u32")?;
    let columns_u32 =
        u32::try_from(columns).map_err(|_| "mpsgraph_fused_pack_columns_exceed_u32")?;
    let pack_start = Instant::now();
    let command_buffer = plan
        .queue
        .commandBuffer()
        .ok_or("mpsgraph_fused_pack_failed_to_create_command_buffer")?;
    let encoder = command_buffer
        .computeCommandEncoder()
        .ok_or("mpsgraph_fused_pack_failed_to_create_compute_encoder")?;
    encoder.setComputePipelineState(pipeline);
    let thread_width = pipeline.threadExecutionWidth().max(1);
    let max_threads = pipeline.maxTotalThreadsPerThreadgroup().max(1);
    let group_width = thread_width.min(columns).min(max_threads).max(1);
    let group_height = (max_threads / group_width).max(1).min(rows).max(1);
    for (plane, source_buffer) in source_buffers.iter().enumerate() {
        let plane_u32 =
            u32::try_from(plane).map_err(|_| "mpsgraph_fused_pack_plane_exceeds_u32")?;
        let params = FusedPackParams {
            rows: rows_u32,
            columns: columns_u32,
            plane: plane_u32,
            _pad0: 0,
        };
        let params_pointer = NonNull::new((&params as *const FusedPackParams).cast_mut().cast())
            .ok_or("mpsgraph_fused_pack_params_pointer_was_null")?;
        unsafe {
            encoder.setBuffer_offset_atIndex(Some(source_buffer), 0, 0);
            encoder.setBuffer_offset_atIndex(Some(output_buffer), 0, 1);
            encoder.setBytes_length_atIndex(params_pointer, mem::size_of::<FusedPackParams>(), 2);
        }
        encoder.dispatchThreads_threadsPerThreadgroup(
            MTLSize {
                width: columns,
                height: rows,
                depth: 1,
            },
            MTLSize {
                width: group_width,
                height: group_height,
                depth: 1,
            },
        );
    }
    encoder.endEncoding();
    command_buffer.commit();
    timing.pack += pack_start.elapsed();
    Ok(PendingFusedPack {
        command_buffer,
        _source_buffers: source_buffers.to_vec(),
    })
}

fn finish_fused_pack(
    pending: PendingFusedPack,
    timing: &mut FftTiming,
) -> Result<(), &'static str> {
    pending.command_buffer.waitUntilCompleted();
    if pending.command_buffer.status() == MTLCommandBufferStatus::Error {
        return Err("mpsgraph_fused_pack_command_failed");
    }
    let gpu_start = pending.command_buffer.GPUStartTime();
    let gpu_end = pending.command_buffer.GPUEndTime();
    if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end > gpu_start {
        timing.device_exec += Duration::from_secs_f64(gpu_end - gpu_start);
    }
    Ok(())
}

fn pack_ifftshifted_f32_batch_into(
    inputs: &[Array2<Complex32>],
    rows: usize,
    columns: usize,
    packed: &mut [f32],
) -> Result<(), &'static str> {
    let mut input_slices = Vec::with_capacity(inputs.len());
    for input in inputs {
        if input.shape() != [rows, columns] {
            return Err("mpsgraph_batch_fft_requires_uniform_shape");
        }
        input_slices.push(
            input
                .as_slice_memory_order()
                .ok_or("mpsgraph_batch_fft_requires_contiguous_inputs")?,
        );
    }

    let global_rows = inputs.len() * rows;
    let row_stride = columns * 2;
    if packed.len() != global_rows * row_stride {
        return Err("mpsgraph_batch_fft_pack_buffer_size_mismatch");
    }
    let thread_count = apple_fft_pack_threads(global_rows * columns);
    let x_shift = rows.div_ceil(2);
    let y_shift = columns.div_ceil(2);

    if thread_count <= 1 || global_rows <= 1 {
        for global_row in 0..global_rows {
            let plane = global_row / rows;
            let dst_x = global_row % rows;
            let src_x = (dst_x + x_shift) % rows;
            let input = input_slices[plane];
            let output_row = &mut packed[global_row * row_stride..(global_row + 1) * row_stride];
            let input_row = &input[src_x * columns..(src_x + 1) * columns];
            pack_shifted_complex32_row_as_f32(input_row, y_shift, output_row);
        }
        return Ok(());
    }

    let chunk_rows = global_rows.div_ceil(thread_count).max(1);
    thread::scope(|scope| {
        for (chunk_index, output_rows) in packed.chunks_mut(chunk_rows * row_stride).enumerate() {
            let input_slices = &input_slices;
            scope.spawn(move || {
                let start_global_row = chunk_index * chunk_rows;
                for (local_row, output_row) in output_rows.chunks_mut(row_stride).enumerate() {
                    let global_row = start_global_row + local_row;
                    let plane = global_row / rows;
                    let dst_x = global_row % rows;
                    let src_x = (dst_x + x_shift) % rows;
                    let input = input_slices[plane];
                    let input_row = &input[src_x * columns..(src_x + 1) * columns];
                    pack_shifted_complex32_row_as_f32(input_row, y_shift, output_row);
                }
            });
        }
    });

    Ok(())
}

fn pack_ifftshifted_f64_batch_as_f32_into(
    inputs: &[Array2<Complex64>],
    rows: usize,
    columns: usize,
    packed: &mut [f32],
) -> Result<(), &'static str> {
    let mut input_slices = Vec::with_capacity(inputs.len());
    for input in inputs {
        if input.shape() != [rows, columns] {
            return Err("mpsgraph_batch_fft_requires_uniform_shape");
        }
        input_slices.push(
            input
                .as_slice_memory_order()
                .ok_or("mpsgraph_f64_to_f32_batch_requires_contiguous_inputs")?,
        );
    }

    let global_rows = inputs.len() * rows;
    let row_stride = columns * 2;
    if packed.len() != global_rows * row_stride {
        return Err("mpsgraph_batch_fft_pack_buffer_size_mismatch");
    }
    let thread_count = apple_fft_pack_threads(global_rows * columns);
    let x_shift = rows.div_ceil(2);
    let y_shift = columns.div_ceil(2);

    if thread_count <= 1 || global_rows <= 1 {
        for global_row in 0..global_rows {
            let plane = global_row / rows;
            let dst_x = global_row % rows;
            let src_x = (dst_x + x_shift) % rows;
            let input = input_slices[plane];
            let output_row = &mut packed[global_row * row_stride..(global_row + 1) * row_stride];
            let input_row = &input[src_x * columns..(src_x + 1) * columns];
            pack_shifted_complex64_row_as_f32(input_row, y_shift, output_row);
        }
        return Ok(());
    }

    let chunk_rows = global_rows.div_ceil(thread_count).max(1);
    thread::scope(|scope| {
        for (chunk_index, output_rows) in packed.chunks_mut(chunk_rows * row_stride).enumerate() {
            let input_slices = &input_slices;
            scope.spawn(move || {
                let start_global_row = chunk_index * chunk_rows;
                for (local_row, output_row) in output_rows.chunks_mut(row_stride).enumerate() {
                    let global_row = start_global_row + local_row;
                    let plane = global_row / rows;
                    let dst_x = global_row % rows;
                    let src_x = (dst_x + x_shift) % rows;
                    let input = input_slices[plane];
                    let input_row = &input[src_x * columns..(src_x + 1) * columns];
                    pack_shifted_complex64_row_as_f32(input_row, y_shift, output_row);
                }
            });
        }
    });

    Ok(())
}

fn unpack_fftshifted_f32_batch(
    values: &[f32],
    batch: usize,
    element_count: usize,
    rows: usize,
    columns: usize,
) -> Vec<Array2<Complex32>> {
    let mut outputs: Vec<_> = (0..batch)
        .map(|_| Array2::<Complex32>::zeros((rows, columns)))
        .collect();
    unpack_fftshifted_f32_batch_into(values, &mut outputs, element_count, rows, columns);
    outputs
}

fn unpack_fftshifted_f32_batch_into(
    values: &[f32],
    outputs: &mut [Array2<Complex32>],
    element_count: usize,
    rows: usize,
    columns: usize,
) {
    let row_stride = columns;
    let value_row_stride = columns * 2;
    let global_rows = outputs.len() * rows;
    let thread_count = apple_fft_pack_threads(global_rows * columns);
    let x_shift = rows / 2;
    let y_shift = columns / 2;

    if thread_count <= 1 || global_rows <= 1 {
        for (plane, output) in outputs.iter_mut().enumerate() {
            let output = output
                .as_slice_memory_order_mut()
                .expect("newly allocated ndarray output should be contiguous");
            let plane_offset = plane * element_count * 2;
            for dst_x in 0..rows {
                let src_x = (dst_x + x_shift) % rows;
                let values_row_start = plane_offset + src_x * value_row_stride;
                let values_row = &values[values_row_start..values_row_start + value_row_stride];
                let output_row = &mut output[dst_x * row_stride..(dst_x + 1) * row_stride];
                unpack_shifted_interleaved_f32_row(values_row, y_shift, output_row);
            }
        }
        return;
    }

    let output_ptrs: Vec<usize> = outputs
        .iter_mut()
        .map(|output| {
            output
                .as_slice_memory_order_mut()
                .expect("newly allocated ndarray output should be contiguous")
                .as_mut_ptr() as usize
        })
        .collect();
    let chunk_rows = global_rows.div_ceil(thread_count).max(1);
    thread::scope(|scope| {
        for chunk_index in 0..thread_count {
            let start_global_row = chunk_index * chunk_rows;
            let end_global_row = ((chunk_index + 1) * chunk_rows).min(global_rows);
            if start_global_row >= end_global_row {
                continue;
            }
            let output_ptrs = &output_ptrs;
            scope.spawn(move || {
                for global_row in start_global_row..end_global_row {
                    let plane = global_row / rows;
                    let dst_x = global_row % rows;
                    let src_x = (dst_x + x_shift) % rows;
                    let values_row_start = plane * element_count * 2 + src_x * value_row_stride;
                    let values_row = &values[values_row_start..values_row_start + value_row_stride];
                    // SAFETY: global-row chunks are disjoint, and each row maps to exactly one
                    // `(plane, dst_x)` output row. No two scoped workers write the same row.
                    let output_row = unsafe {
                        slice::from_raw_parts_mut(
                            (output_ptrs[plane] as *mut Complex32).add(dst_x * row_stride),
                            row_stride,
                        )
                    };
                    unpack_shifted_interleaved_f32_row(values_row, y_shift, output_row);
                }
            });
        }
    });
}

fn pack_shifted_complex32_row_as_f32(
    input_row: &[Complex32],
    y_shift: usize,
    output_row: &mut [f32],
) {
    debug_assert_eq!(output_row.len(), input_row.len() * 2);
    let (first, second) = input_row.split_at(y_shift);
    let second_len = second.len() * 2;
    write_complex32_as_f32(second, &mut output_row[..second_len]);
    write_complex32_as_f32(first, &mut output_row[second_len..]);
}

fn pack_shifted_complex64_row_as_f32(
    input_row: &[Complex64],
    y_shift: usize,
    output_row: &mut [f32],
) {
    debug_assert_eq!(output_row.len(), input_row.len() * 2);
    let (first, second) = input_row.split_at(y_shift);
    let second_len = second.len() * 2;
    write_complex64_as_f32(second, &mut output_row[..second_len]);
    write_complex64_as_f32(first, &mut output_row[second_len..]);
}

fn write_complex32_as_f32(values: &[Complex32], output: &mut [f32]) {
    debug_assert_eq!(output.len(), values.len() * 2);
    for (index, value) in values.iter().copied().enumerate() {
        let dst = index * 2;
        output[dst] = value.re;
        output[dst + 1] = value.im;
    }
}

fn write_complex64_as_f32(values: &[Complex64], output: &mut [f32]) {
    debug_assert_eq!(output.len(), values.len() * 2);
    for (index, value) in values.iter().copied().enumerate() {
        let dst = index * 2;
        output[dst] = value.re as f32;
        output[dst + 1] = value.im as f32;
    }
}

fn unpack_shifted_interleaved_f32_row(
    values_row: &[f32],
    y_shift: usize,
    output_row: &mut [Complex32],
) {
    debug_assert_eq!(values_row.len(), output_row.len() * 2);
    let first_output_len = output_row.len() - y_shift;
    let split = y_shift * 2;
    unpack_interleaved_f32_as_complex32(&values_row[split..], &mut output_row[..first_output_len]);
    unpack_interleaved_f32_as_complex32(&values_row[..split], &mut output_row[first_output_len..]);
}

fn unpack_interleaved_f32_as_complex32(values: &[f32], output: &mut [Complex32]) {
    debug_assert_eq!(values.len(), output.len() * 2);
    for (index, output_cell) in output.iter_mut().enumerate() {
        let src = index * 2;
        *output_cell = Complex32::new(values[src], values[src + 1]);
    }
}

fn apple_fft_pack_threads(work_items: usize) -> usize {
    if work_items < APPLE_FFT_PARALLEL_PACK_MIN_ELEMENTS {
        return 1;
    }
    if let Ok(value) = env::var(APPLE_FFT_PACK_THREADS_ENV) {
        if let Ok(parsed) = value.trim().parse::<usize>() {
            return parsed.max(1);
        }
    }
    thread::available_parallelism().map_or(1, |count| count.get().max(1))
}

fn apple_fft_fused_pack_enabled() -> bool {
    env::var_os(APPLE_FFT_FUSED_PACK_ENV).is_some()
}

fn selection_for_fused_pack(
    selection: FftBackendSelection,
    fused_pack: bool,
) -> FftBackendSelection {
    if fused_pack {
        FftBackendSelection {
            reason: "metal_mpsgraph_complex_f32_host_batch_fused_pack_experiment",
            ..selection
        }
    } else {
        selection
    }
}
