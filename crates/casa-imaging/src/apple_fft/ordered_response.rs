// SPDX-License-Identifier: LGPL-3.0-or-later
//! Compact source-order AWProject normal operator for repeated major cycles.
//!
//! The first production integration deliberately accepts only the frozen VLASS
//! development artifact contract. The operator fails closed on every geometry
//! or artifact mismatch. This lets the reduced real-data row measure the
//! end-to-end architecture before the row-to-operator compiler is generalized.

use std::{
    collections::HashSet,
    ffi::c_void,
    fs, mem,
    path::{Path, PathBuf},
    ptr::NonNull,
    slice,
    time::{Duration, Instant},
};

use ndarray::Array2;
use num_complex::Complex32;
use objc2::{AnyThread, rc::Retained, runtime::ProtocolObject};
use objc2_foundation::{NSArray, NSDictionary, NSString};
use objc2_metal::{
    MTLBuffer, MTLCommandBuffer, MTLCommandBufferStatus, MTLCommandEncoder, MTLCommandQueue,
    MTLComputeCommandEncoder, MTLComputePipelineState, MTLCreateSystemDefaultDevice, MTLDevice,
    MTLLibrary, MTLResourceOptions, MTLSize,
};
use objc2_metal_performance_shaders::MPSCommandBuffer;
use objc2_metal_performance_shaders::MPSDataType;
use objc2_metal_performance_shaders_graph::{
    MPSGraph, MPSGraphDevice, MPSGraphExecutable, MPSGraphExecutableExecutionDescriptor,
    MPSGraphFFTDescriptor, MPSGraphFFTScalingMode, MPSGraphShapedType, MPSGraphTensorData,
    MPSGraphTensorShapedTypeDictionary,
};
use sha2::{Digest, Sha256};

use super::{axes_array_batch, shape_array_batch, threadgroup_2d};
use crate::{ImagingError, fft_backend::FftDirection};

type MetalDevice = Retained<ProtocolObject<dyn MTLDevice>>;
type MetalQueue = Retained<ProtocolObject<dyn MTLCommandQueue>>;
type MetalBuffer = Retained<ProtocolObject<dyn MTLBuffer>>;
type MetalCommandBuffer = Retained<ProtocolObject<dyn MTLCommandBuffer>>;
type MetalComputePipeline = Retained<ProtocolObject<dyn MTLComputePipelineState>>;

const PHYSICAL_RECEIPT_ENV: &str = "CASA_RS_VLASS_ORDERED_RESPONSE_PHYSICAL_RECEIPT";
const CONSTRUCTION_DIRECTORY_ENV: &str = "CASA_RS_VLASS_ORDERED_RESPONSE_CONSTRUCTION_DIR";
const PHYSICAL_RECEIPT_SCHEMA: &str = "casa-rs-vlass-ordered-response-physical-semantic-gate/v3";
const CONSTRUCTION_SCHEMA: &str = "casa-rs-vlass-ordered-response-segmented-construction/v12";

const SIDE: usize = 192;
const PIXELS: usize = SIDE * SIDE;
const CONSTRUCTION_SIDE: usize = 384;
const CONSTRUCTION_PIXELS: usize = CONSTRUCTION_SIDE * CONSTRUCTION_SIDE;
const IMAGING_STATES: usize = 28;
const PREDICTION_STATES: usize = 32;
const ETA_POWERS: usize = 3;
const MODEL_TERMS: usize = 2;
const OUTPUT_TERMS: usize = 2;
const ORDERED_PAIRS: usize = 54;
const W_ORDERS: usize = 3;
const RESPONSE_MOMENTS: usize = 3;
const RESPONSE_COEFFICIENTS: usize = W_ORDERS * RESPONSE_MOMENTS;
const FORWARD_PLANES: usize = PREDICTION_STATES * ETA_POWERS * MODEL_TERMS;
const INVERSE_PLANES: usize = IMAGING_STATES * ETA_POWERS * OUTPUT_TERMS;
const RESPONSE_PLANES: usize = ORDERED_PAIRS * RESPONSE_COEFFICIENTS;
const RESPONSE_BATCH_PAIRS: usize = 6;
const RESPONSE_BATCH_PLANES: usize = RESPONSE_BATCH_PAIRS * RESPONSE_COEFFICIENTS;
const ES_SUPPORT_WIDTH: usize = 15;
const ES_LUT_INTERVALS: usize = 65_536;
const ES_WIDTH: f64 = 14.0;
const ES_BETA: f64 = 32.2;
const COMPLEX_BYTES: usize = mem::size_of::<Complex32>();
const FFT_BUFFER_BYTES: usize = FORWARD_PLANES * PIXELS * COMPLEX_BYTES;
const RESPONSE_BYTES: usize = RESPONSE_PLANES * PIXELS * COMPLEX_BYTES;

const ORDERED_RESPONSE_SHADER: &str = include_str!("ordered_response.metal");

#[repr(C)]
#[derive(Clone, Copy)]
struct LinearParams {
    elements: u32,
    plane_elements: u32,
    _pad0: u32,
    _pad1: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct MixerAction {
    input_plane: u32,
    kernel_plane: u32,
    coefficient: f32,
    _pad0: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct MixerParams {
    pixels: u32,
    output_planes: u32,
    response_scale: f32,
    _pad1: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct ReductionParams {
    pixels: u32,
    active_pixels: u32,
    imaging_states: u32,
    eta_powers: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
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
#[derive(Clone, Copy)]
struct CompactCorrectionParams {
    input_side: u32,
    output_side: u32,
    planes: u32,
    _pad0: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct PlaneCopyParams {
    elements: u32,
    plane_elements: u32,
    output_plane_base: u32,
    _pad0: u32,
}

#[derive(Clone, Copy)]
struct PhysicalPairMap {
    pair_index: usize,
    imaging_state: usize,
    prediction_state: usize,
    imaging_screen_state: usize,
    prediction_screen_state: usize,
}

struct PhysicalFixture {
    origin: [i32; 2],
    image_shape: [usize; 2],
    active_pixels: Vec<[usize; 2]>,
    active_indices: Vec<u32>,
    pair_map: Vec<PhysicalPairMap>,
    right_factors: Vec<Complex32>,
    left_factors: Vec<Complex32>,
    receipt_sha256: String,
}

struct ScreenSampler<'a> {
    values: &'a [Complex32],
    side: usize,
    uv_reference: [f64; 2],
    crop_start: [f64; 2],
    sky_increment: [f64; 2],
    pointing_pixel: [f64; 2],
    cell_rad: f64,
}

impl ScreenSampler<'_> {
    fn sample(&self, state: usize, pixel: [i32; 2]) -> Result<Complex32, ImagingError> {
        if state >= PREDICTION_STATES {
            return Err(invalid("ordered-response screen state is out of range"));
        }
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
        if x0 < 0 || y0 < 0 || x0 + 1 >= self.side as isize || y0 + 1 >= self.side as isize {
            return Err(unsupported(
                "ordered-response embedding escapes the validated AW screen crop",
            ));
        }
        let fx = x - x0 as f64;
        let fy = y - y0 as f64;
        let read = |sample_x: isize, sample_y: isize| {
            let index =
                state * self.side * self.side + sample_y as usize * self.side + sample_x as usize;
            let value = self.values[index];
            (f64::from(value.re), f64::from(value.im))
        };
        let samples = [
            (read(x0, y0), (1.0 - fx) * (1.0 - fy)),
            (read(x0 + 1, y0), fx * (1.0 - fy)),
            (read(x0, y0 + 1), (1.0 - fx) * fy),
            (read(x0 + 1, y0 + 1), fx * fy),
        ];
        let (re, im) = samples.into_iter().fold(
            (0.0_f64, 0.0_f64),
            |(sum_re, sum_im), ((re, im), weight)| (sum_re + weight * re, sum_im + weight * im),
        );
        Ok(Complex32::new(re as f32, im as f32))
    }
}

struct ResidentFftExecutable {
    _graph: Retained<MPSGraph>,
    executable: Retained<MPSGraphExecutable>,
}

struct OrderedResponsePipelines {
    prepare: MetalComputePipeline,
    mix: MetalComputePipeline,
    reduce: MetalComputePipeline,
    construct_es: MetalComputePipeline,
    compact_deapodize_response: MetalComputePipeline,
    copy_response_planes: MetalComputePipeline,
}

/// Timing and numerical diagnostics for one resident operator application.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OrderedResponseApplication {
    /// Host-observed wall time including the compact model upload and readback.
    pub(crate) elapsed: Duration,
    /// Metal command-buffer device interval.
    pub(crate) device: Duration,
    /// Largest discarded imaginary component across active output pixels.
    pub(crate) max_imaginary: f32,
}

#[derive(Debug)]
pub(crate) struct OrderedResponseShadowTerm {
    pub(crate) residual_relative_l2: f64,
    pub(crate) residual_normalized_linf: f64,
    pub(crate) response_relative_l2: f64,
    pub(crate) response_normalized_linf: f64,
    pub(crate) response_fit_scale: f64,
    pub(crate) response_correlation: f64,
    pub(crate) resident_response_l2: f64,
    pub(crate) exact_response_l2: f64,
}

/// Frozen-contract resident operator used by the first real-row integration.
pub(crate) struct VlassOrderedResponseOperator {
    queue: MetalQueue,
    pipelines: OrderedResponsePipelines,
    forward: ResidentFftExecutable,
    inverse: ResidentFftExecutable,
    buffer_a: MetalBuffer,
    buffer_b: MetalBuffer,
    model: MetalBuffer,
    right_factors: MetalBuffer,
    response: MetalBuffer,
    left_factors: MetalBuffer,
    action_offsets: MetalBuffer,
    actions: MetalBuffer,
    active_indices: MetalBuffer,
    feedback: MetalBuffer,
    forward_inputs: Retained<NSArray<MPSGraphTensorData>>,
    forward_results: Retained<NSArray<MPSGraphTensorData>>,
    inverse_inputs: Retained<NSArray<MPSGraphTensorData>>,
    inverse_results: Retained<NSArray<MPSGraphTensorData>>,
    execution: Retained<MPSGraphExecutableExecutionDescriptor>,
    image_shape: [usize; 2],
    origin: [i32; 2],
    active_pixels: Vec<[usize; 2]>,
    dirty_active: Vec<f32>,
    resident_bytes: usize,
    construction_peak_bytes: usize,
    receipt_sha256: String,
}

pub(crate) struct OrderedResponseRequest<'a> {
    pub(crate) image_shape: [usize; 2],
    pub(crate) nterms: usize,
    pub(crate) clean_mask: Option<&'a Array2<bool>>,
    pub(crate) dirty_terms: &'a [Array2<f32>],
    pub(crate) normalization_sumwt: f32,
    pub(crate) weight_image: &'a Array2<f32>,
    pub(crate) pb_limit: f32,
    pub(crate) source_rows_sha256: &'a str,
}

impl VlassOrderedResponseOperator {
    /// Construct the validated reduced-row operator from the current
    /// development artifacts.
    pub(crate) fn from_environment(
        request: OrderedResponseRequest<'_>,
    ) -> Result<Self, ImagingError> {
        let receipt = required_absolute_path(PHYSICAL_RECEIPT_ENV)?;
        let construction = required_absolute_path(CONSTRUCTION_DIRECTORY_ENV)?;
        Self::from_artifacts(&receipt, &construction, request)
    }

    fn from_artifacts(
        receipt_path: &Path,
        construction_directory: &Path,
        request: OrderedResponseRequest<'_>,
    ) -> Result<Self, ImagingError> {
        let OrderedResponseRequest {
            image_shape,
            nterms,
            clean_mask,
            dirty_terms,
            normalization_sumwt,
            weight_image,
            pb_limit,
            source_rows_sha256,
        } = request;
        if image_shape != [4_096, 4_096] || nterms != MODEL_TERMS {
            return Err(unsupported(
                "ordered-response v1 requires the 4096-square, nterms=2 VLASS development row",
            ));
        }
        let fixture = load_physical_fixture(receipt_path)?;
        if fixture.image_shape != image_shape {
            return Err(unsupported(
                "ordered-response receipt image geometry differs from the imaging request",
            ));
        }
        validate_clean_mask(clean_mask, image_shape, &fixture.active_pixels)?;
        validate_term_shapes(dirty_terms, image_shape, "dirty")?;
        if weight_image.dim() != (image_shape[0], image_shape[1])
            || !(normalization_sumwt.is_finite() && normalization_sumwt > 0.0)
        {
            return Err(invalid(
                "ordered-response normalization inputs do not match the requested image",
            ));
        }
        let dirty_active = dirty_terms
            .iter()
            .flat_map(|term| fixture.active_pixels.iter().map(|&[x, y]| term[(x, y)]))
            .collect::<Vec<_>>();
        let construction = load_construction(construction_directory, source_rows_sha256)?;

        let device = MTLCreateSystemDefaultDevice().ok_or_else(|| {
            unsupported("ordered-response requires a visible default Metal device")
        })?;
        let queue = device.newCommandQueue().ok_or_else(|| {
            unsupported("ordered-response could not create a Metal command queue")
        })?;
        let graph_device = unsafe { MPSGraphDevice::deviceWithMTLDevice(&device) };
        let forward =
            compile_fft_executable(&graph_device, FORWARD_PLANES, SIDE, FftDirection::Forward)?;
        let inverse =
            compile_fft_executable(&graph_device, INVERSE_PLANES, SIDE, FftDirection::Inverse)?;
        let pipelines = make_pipelines(&device)?;

        let buffer_a = private_buffer(&device, FFT_BUFFER_BYTES, "forward/inverse buffer A")?;
        let buffer_b = private_buffer(&device, FFT_BUFFER_BYTES, "forward/inverse buffer B")?;
        let response = private_buffer(&device, RESPONSE_BYTES, "resident response bank")?;
        let construction_peak_bytes = build_response_bank(
            &device,
            &queue,
            &graph_device,
            &pipelines,
            &response,
            construction,
        )?;

        let model = shared_buffer(
            &device,
            MODEL_TERMS * PIXELS * mem::size_of::<f32>(),
            "compact model",
        )?;
        let mut normalized_right_factors = fixture.right_factors;
        apply_model_prediction_normalization_to_right_factors(
            &mut normalized_right_factors,
            fixture.origin,
            image_shape,
            weight_image,
            pb_limit,
        )?;
        let right_factors = copied_shared_buffer(
            &device,
            &normalized_right_factors,
            "normalized right factors",
        )?;
        let mut normalized_left_factors = fixture.left_factors;
        apply_residual_normalization_to_left_factors(
            &mut normalized_left_factors,
            &fixture.active_pixels,
            &fixture.active_indices,
            normalization_sumwt,
            weight_image,
            pb_limit,
        )?;
        let left_factors =
            copied_shared_buffer(&device, &normalized_left_factors, "normalized left factors")?;
        let (offset_values, action_values) = physical_actions(&fixture.pair_map)?;
        let action_offsets = copied_shared_buffer(&device, &offset_values, "action offsets")?;
        let actions = copied_shared_buffer(&device, &action_values, "mixer actions")?;
        let active_indices =
            copied_shared_buffer(&device, &fixture.active_indices, "active indices")?;
        let feedback = shared_buffer(
            &device,
            fixture.active_pixels.len() * OUTPUT_TERMS * COMPLEX_BYTES,
            "active feedback",
        )?;

        let forward_shape = shape_array_batch(FORWARD_PLANES, SIDE, SIDE);
        let inverse_shape = shape_array_batch(INVERSE_PLANES, SIDE, SIDE);
        let forward_input = tensor_data(&buffer_a, &forward_shape)?;
        let forward_output = tensor_data(&buffer_b, &forward_shape)?;
        let inverse_input = tensor_data(&buffer_a, &inverse_shape)?;
        let inverse_output = tensor_data(&buffer_b, &inverse_shape)?;
        let forward_inputs = NSArray::from_slice(&[&*forward_input]);
        let forward_results = NSArray::from_slice(&[&*forward_output]);
        let inverse_inputs = NSArray::from_slice(&[&*inverse_input]);
        let inverse_results = NSArray::from_slice(&[&*inverse_output]);
        let execution = unsafe { MPSGraphExecutableExecutionDescriptor::new() };
        unsafe {
            execution.setWaitUntilCompleted(false);
        }

        let resident_bytes = 2 * FFT_BUFFER_BYTES
            + RESPONSE_BYTES
            + MODEL_TERMS * PIXELS * mem::size_of::<f32>()
            + mem::size_of_val(normalized_right_factors.as_slice())
            + mem::size_of_val(normalized_left_factors.as_slice())
            + mem::size_of_val(offset_values.as_slice())
            + mem::size_of_val(action_values.as_slice())
            + mem::size_of_val(fixture.active_pixels.as_slice())
            + mem::size_of_val(fixture.active_indices.as_slice())
            + mem::size_of_val(dirty_active.as_slice())
            + fixture.active_pixels.len() * OUTPUT_TERMS * COMPLEX_BYTES;

        Ok(Self {
            queue,
            pipelines,
            forward,
            inverse,
            buffer_a,
            buffer_b,
            model,
            right_factors,
            response,
            left_factors,
            action_offsets,
            actions,
            active_indices,
            feedback,
            forward_inputs,
            forward_results,
            inverse_inputs,
            inverse_results,
            execution,
            image_shape,
            origin: fixture.origin,
            active_pixels: fixture.active_pixels,
            dirty_active,
            resident_bytes,
            construction_peak_bytes: resident_bytes.saturating_add(construction_peak_bytes),
            receipt_sha256: fixture.receipt_sha256,
        })
    }

    /// Apply `H m` and replace active residual pixels with `b - H m`.
    pub(crate) fn refresh_residual(
        &mut self,
        model_terms: &[Array2<f32>],
        residual_terms: &mut [Array2<f32>],
    ) -> Result<OrderedResponseApplication, ImagingError> {
        validate_term_shapes(model_terms, self.image_shape, "model")?;
        validate_term_shapes(residual_terms, self.image_shape, "residual")?;
        copy_model_crop(&mut self.model, model_terms, self.origin)?;

        let total_started = Instant::now();
        let command = unsafe { MPSCommandBuffer::commandBufferFromCommandQueue(&self.queue) };
        let initial_root = unsafe { command.rootCommandBuffer() };
        encode_prepare(
            &initial_root,
            &self.pipelines.prepare,
            &self.model,
            &self.right_factors,
            &self.buffer_a,
        )?;
        let _ = unsafe {
            self.forward
                .executable
                .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                    &command,
                    &self.forward_inputs,
                    Some(&self.forward_results),
                    Some(&self.execution),
                )
        };
        let after_forward = unsafe { command.rootCommandBuffer() };
        encode_mixer(
            &after_forward,
            &self.pipelines.mix,
            &self.buffer_b,
            &self.response,
            &self.action_offsets,
            &self.actions,
            &self.buffer_a,
        )?;
        let _ = unsafe {
            self.inverse
                .executable
                .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                    &command,
                    &self.inverse_inputs,
                    Some(&self.inverse_results),
                    Some(&self.execution),
                )
        };
        let after_inverse = unsafe { command.rootCommandBuffer() };
        encode_reduction(
            &after_inverse,
            &self.pipelines.reduce,
            &self.buffer_b,
            &self.left_factors,
            &self.active_indices,
            &self.feedback,
            self.active_pixels.len(),
        )?;
        let final_root = unsafe { command.rootCommandBuffer() };
        if !std::ptr::eq(&*initial_root, &*after_forward)
            || !std::ptr::eq(&*initial_root, &*after_inverse)
            || !std::ptr::eq(&*initial_root, &*final_root)
        {
            return Err(unsupported(
                "MPSGraph committed inside the ordered-response command boundary",
            ));
        }
        final_root.commit();
        final_root.waitUntilCompleted();
        validate_command(&final_root, "ordered-response application")?;

        let values =
            shared_slice::<Complex32>(&self.feedback, self.active_pixels.len() * OUTPUT_TERMS);
        let mut max_imaginary = 0.0_f32;
        for (term, residual) in residual_terms.iter_mut().enumerate() {
            for (ordinal, &[x, y]) in self.active_pixels.iter().enumerate() {
                let response = values[term * self.active_pixels.len() + ordinal];
                max_imaginary = max_imaginary.max(response.im.abs());
                residual[(x, y)] =
                    self.dirty_active[term * self.active_pixels.len() + ordinal] - response.re;
            }
        }
        let gpu_start = final_root.GPUStartTime();
        let gpu_end = final_root.GPUEndTime();
        let device = if gpu_start.is_finite() && gpu_end.is_finite() && gpu_end >= gpu_start {
            Duration::from_secs_f64(gpu_end - gpu_start)
        } else {
            Duration::ZERO
        };
        Ok(OrderedResponseApplication {
            elapsed: total_started.elapsed(),
            device,
            max_imaginary,
        })
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub(crate) fn construction_peak_bytes(&self) -> usize {
        self.construction_peak_bytes
    }

    pub(crate) fn active_pixel_count(&self) -> usize {
        self.active_pixels.len()
    }

    pub(crate) fn receipt_sha256(&self) -> &str {
        &self.receipt_sha256
    }

    pub(crate) fn compare_shadow_exact(
        &self,
        resident_terms: &[Array2<f32>],
        exact_terms: &[Array2<f32>],
    ) -> Result<Vec<OrderedResponseShadowTerm>, ImagingError> {
        validate_term_shapes(resident_terms, self.image_shape, "resident shadow residual")?;
        validate_term_shapes(exact_terms, self.image_shape, "exact shadow residual")?;
        let active_count = self.active_pixels.len();
        let mut terms = Vec::with_capacity(OUTPUT_TERMS);
        for term in 0..OUTPUT_TERMS {
            let mut residual_difference_squared = 0.0_f64;
            let mut residual_reference_squared = 0.0_f64;
            let mut residual_difference_max = 0.0_f64;
            let mut residual_reference_max = 0.0_f64;
            let mut response_difference_squared = 0.0_f64;
            let mut exact_response_squared = 0.0_f64;
            let mut resident_response_squared = 0.0_f64;
            let mut response_difference_max = 0.0_f64;
            let mut exact_response_max = 0.0_f64;
            let mut response_dot = 0.0_f64;
            for (ordinal, &[x, y]) in self.active_pixels.iter().enumerate() {
                let resident_residual = f64::from(resident_terms[term][(x, y)]);
                let exact_residual = f64::from(exact_terms[term][(x, y)]);
                let dirty = f64::from(self.dirty_active[term * active_count + ordinal]);
                let resident_response = dirty - resident_residual;
                let exact_response = dirty - exact_residual;
                let residual_difference = resident_residual - exact_residual;
                let response_difference = resident_response - exact_response;
                residual_difference_squared += residual_difference * residual_difference;
                residual_reference_squared += exact_residual * exact_residual;
                residual_difference_max = residual_difference_max.max(residual_difference.abs());
                residual_reference_max = residual_reference_max.max(exact_residual.abs());
                response_difference_squared += response_difference * response_difference;
                resident_response_squared += resident_response * resident_response;
                exact_response_squared += exact_response * exact_response;
                response_difference_max = response_difference_max.max(response_difference.abs());
                exact_response_max = exact_response_max.max(exact_response.abs());
                response_dot += resident_response * exact_response;
            }
            let residual_reference_l2 = residual_reference_squared.sqrt().max(f64::MIN_POSITIVE);
            let exact_response_l2 = exact_response_squared.sqrt().max(f64::MIN_POSITIVE);
            let resident_response_l2 = resident_response_squared.sqrt().max(f64::MIN_POSITIVE);
            terms.push(OrderedResponseShadowTerm {
                residual_relative_l2: residual_difference_squared.sqrt() / residual_reference_l2,
                residual_normalized_linf: residual_difference_max
                    / residual_reference_max.max(f64::MIN_POSITIVE),
                response_relative_l2: response_difference_squared.sqrt() / exact_response_l2,
                response_normalized_linf: response_difference_max
                    / exact_response_max.max(f64::MIN_POSITIVE),
                response_fit_scale: response_dot / resident_response_squared.max(f64::MIN_POSITIVE),
                response_correlation: response_dot / (resident_response_l2 * exact_response_l2),
                resident_response_l2,
                exact_response_l2,
            });
        }
        Ok(terms)
    }
}

struct ConstructionArtifacts {
    offsets: Vec<u8>,
    meta: Vec<u8>,
    coefficients: Vec<u8>,
    groups: usize,
}

fn load_construction(
    directory: &Path,
    source_rows_sha256: &str,
) -> Result<ConstructionArtifacts, ImagingError> {
    let manifest_path = directory.join("manifest.json");
    let manifest_bytes = fs::read(&manifest_path).map_err(|error| {
        invalid(format!(
            "read ordered-response construction manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).map_err(|error| {
        invalid(format!(
            "parse ordered-response construction manifest {}: {error}",
            manifest_path.display()
        ))
    })?;
    if manifest["schema"].as_str() != Some(CONSTRUCTION_SCHEMA)
        || manifest["geometry"]["construction_grid_side"].as_u64() != Some(CONSTRUCTION_SIDE as u64)
    {
        return Err(unsupported(
            "ordered-response construction does not match the validated v12 384-square contract",
        ));
    }
    if manifest["source"]["row_payload_sha256"].as_str() != Some(source_rows_sha256) {
        return Err(unsupported(
            "ordered-response construction source SHA-256 differs from the accepted initial-dirty stream",
        ));
    }
    let offsets = read_artifact(directory, "response-bucket-offsets-u32-le.bin")?;
    let meta = read_artifact(directory, "response-group-meta-f32-le.bin")?;
    let coefficients = read_artifact(directory, "response-group-coefficients-c64-le.bin")?;
    let expected_offset_bytes = (ORDERED_PAIRS * CONSTRUCTION_PIXELS + 1)
        .checked_mul(mem::size_of::<u32>())
        .ok_or_else(|| invalid("ordered-response offset byte count overflowed"))?;
    if offsets.len() != expected_offset_bytes || meta.len() % (2 * mem::size_of::<f32>()) != 0 {
        return Err(invalid(
            "ordered-response construction artifact dimensions are inconsistent",
        ));
    }
    let groups = meta.len() / (2 * mem::size_of::<f32>());
    if coefficients.len()
        != groups
            .checked_mul(RESPONSE_COEFFICIENTS * COMPLEX_BYTES)
            .ok_or_else(|| invalid("ordered-response coefficient byte count overflowed"))?
        || little_u32_at(&offsets, ORDERED_PAIRS * CONSTRUCTION_PIXELS)? as usize != groups
    {
        return Err(invalid(
            "ordered-response construction group inventory is inconsistent",
        ));
    }
    Ok(ConstructionArtifacts {
        offsets,
        meta,
        coefficients,
        groups,
    })
}

fn load_physical_fixture(receipt_path: &Path) -> Result<PhysicalFixture, ImagingError> {
    let receipt_bytes = fs::read(receipt_path).map_err(|error| {
        invalid(format!(
            "read ordered-response physical receipt {}: {error}",
            receipt_path.display()
        ))
    })?;
    let receipt_sha256 = format!("{:x}", Sha256::digest(&receipt_bytes));
    let receipt: serde_json::Value = serde_json::from_slice(&receipt_bytes).map_err(|error| {
        invalid(format!(
            "parse ordered-response physical receipt {}: {error}",
            receipt_path.display()
        ))
    })?;
    if receipt["schema"].as_str() != Some(PHYSICAL_RECEIPT_SCHEMA)
        || receipt["gate"]["passed"].as_bool() != Some(true)
    {
        return Err(unsupported(
            "ordered-response physical receipt is not the passing v3 semantic gate",
        ));
    }
    let fixture = &receipt["resident_integration_fixture"];
    if fixture["embedding"]["side"].as_u64() != Some(SIDE as u64) {
        return Err(unsupported(
            "ordered-response physical receipt uses a different resident embedding",
        ));
    }
    let origin = json_i32_pair(
        &fixture["embedding"]["origin_image_pixel"],
        "embedding origin",
    )?;
    let image_shape_i32 = json_i32_pair(&receipt["geometry"]["image_shape"], "image shape")?;
    let image_shape = [
        usize::try_from(image_shape_i32[0]).map_err(|_| invalid("negative image width"))?,
        usize::try_from(image_shape_i32[1]).map_err(|_| invalid("negative image height"))?,
    ];
    let active_i32 = json_i32_pairs(&fixture["active_pixels"], "active pixels")?;
    if active_i32.is_empty() {
        return Err(invalid("ordered-response receipt has no active pixels"));
    }
    let active_pixels = active_i32
        .iter()
        .map(|&[x, y]| {
            Ok([
                usize::try_from(x).map_err(|_| invalid("negative active x"))?,
                usize::try_from(y).map_err(|_| invalid("negative active y"))?,
            ])
        })
        .collect::<Result<Vec<_>, ImagingError>>()?;
    let active_indices = active_i32
        .iter()
        .map(|&pixel| embedding_index(pixel, origin).and_then(|value| checked_u32(value, "active")))
        .collect::<Result<Vec<_>, ImagingError>>()?;
    let pair_entries = fixture["ordered_pair_map"]
        .as_array()
        .ok_or_else(|| invalid("ordered-response receipt lacks its ordered pair map"))?;
    let pair_map = pair_entries
        .iter()
        .map(|entry| {
            Ok(PhysicalPairMap {
                pair_index: json_usize(entry, "pair_index")?,
                imaging_state: json_usize(entry, "imaging_state")?,
                prediction_state: json_usize(entry, "prediction_state")?,
                imaging_screen_state: json_usize(entry, "imaging_screen_state")?,
                prediction_screen_state: json_usize(entry, "prediction_screen_state")?,
            })
        })
        .collect::<Result<Vec<_>, ImagingError>>()?;
    validate_pair_map(&pair_map)?;

    let screen_manifest_path = PathBuf::from(
        receipt["sources"]["screen_manifest"]
            .as_str()
            .ok_or_else(|| invalid("ordered-response receipt lacks its screen manifest path"))?,
    );
    let screen_manifest_bytes = fs::read(&screen_manifest_path).map_err(|error| {
        invalid(format!(
            "read ordered-response screen manifest {}: {error}",
            screen_manifest_path.display()
        ))
    })?;
    verify_sha256(
        &screen_manifest_bytes,
        receipt["sources"]["screen_manifest_sha256"].as_str(),
        "screen manifest",
    )?;
    let screen_manifest: serde_json::Value = serde_json::from_slice(&screen_manifest_bytes)
        .map_err(|error| {
            invalid(format!(
                "parse ordered-response screen manifest {}: {error}",
                screen_manifest_path.display()
            ))
        })?;
    let crop_shape = json_i32_pair(&screen_manifest["crop_shape"], "screen crop shape")?;
    if crop_shape[0] != crop_shape[1] || crop_shape[0] <= 1 {
        return Err(invalid(
            "ordered-response screen manifest requires a non-empty square crop",
        ));
    }
    let screen_side =
        usize::try_from(crop_shape[0]).map_err(|_| invalid("negative screen side"))?;
    let forward_path =
        resolve_json_artifact(&screen_manifest_path, &screen_manifest["forward_path"])?;
    let forward_bytes = fs::read(&forward_path).map_err(|error| {
        invalid(format!(
            "read ordered-response forward screens {}: {error}",
            forward_path.display()
        ))
    })?;
    verify_sha256(
        &forward_bytes,
        receipt["sources"]["screen_artifact_sha256"]["forward"].as_str(),
        "forward screens",
    )?;
    let screen_values = complex32_values_from_le_bytes(&forward_bytes)?;
    if screen_values.len() != PREDICTION_STATES * screen_side * screen_side {
        return Err(invalid(
            "ordered-response forward screen inventory has the wrong dimensions",
        ));
    }
    let uv_reference = json_f64_pair(
        &screen_manifest["uv_reference_pixel"],
        "screen UV reference",
    )?;
    let crop_start_i32 = json_i32_pair(&screen_manifest["crop_start"], "screen crop start")?;
    let crop_start = [f64::from(crop_start_i32[0]), f64::from(crop_start_i32[1])];
    let sky_increment = json_f64_pair(
        &screen_manifest["derived_sky_increment_rad"],
        "screen sky increment",
    )?;
    let pointing_pixel = json_f64_pair(&receipt["geometry"]["pointing_pixel"], "pointing pixel")?;
    let cell_rad = receipt["geometry"]["cell_arcsec"]
        .as_f64()
        .ok_or_else(|| invalid("ordered-response receipt lacks cell size"))?
        * std::f64::consts::PI
        / (180.0 * 3600.0);
    let facet_center = json_f64_pair(&receipt["geometry"]["facet_center_pixel"], "facet center")?;
    let image_reference = receipt["geometry"]["image_reference_pixel"]
        .as_f64()
        .ok_or_else(|| invalid("ordered-response receipt lacks image reference pixel"))?;
    let sampler = ScreenSampler {
        values: &screen_values,
        side: screen_side,
        uv_reference,
        crop_start,
        sky_increment,
        pointing_pixel,
        cell_rad,
    };
    let (imaging_screen_states, prediction_screen_states) = screen_state_maps(&pair_map)?;

    let mut left_factors = vec![Complex32::new(0.0, 0.0); IMAGING_STATES * ETA_POWERS * PIXELS];
    for (imaging_state, &screen_state) in imaging_screen_states.iter().enumerate() {
        for &pixel in &active_i32 {
            let embedding = embedding_index(pixel, origin)?;
            let eta = facet_eta(pixel, facet_center, image_reference, cell_rad)? as f32;
            let screen = sampler.sample(screen_state, pixel)?.conj();
            let mut eta_power = 1.0_f32;
            for power in 0..ETA_POWERS {
                left_factors[(imaging_state * ETA_POWERS + power) * PIXELS + embedding] =
                    screen * eta_power;
                eta_power *= eta;
            }
        }
    }
    let mut right_factors = vec![Complex32::new(0.0, 0.0); PREDICTION_STATES * ETA_POWERS * PIXELS];
    for (prediction_state, &screen_state) in prediction_screen_states.iter().enumerate() {
        for local_y in 0..SIDE {
            for local_x in 0..SIDE {
                let pixel = [origin[0] + local_x as i32, origin[1] + local_y as i32];
                let eta = -(facet_eta(pixel, facet_center, image_reference, cell_rad)? as f32);
                let screen = sampler.sample(screen_state, pixel)?;
                let embedding = local_y * SIDE + local_x;
                let mut eta_power = 1.0_f32;
                for power in 0..ETA_POWERS {
                    right_factors[(prediction_state * ETA_POWERS + power) * PIXELS + embedding] =
                        screen * eta_power;
                    eta_power *= eta;
                }
            }
        }
    }
    Ok(PhysicalFixture {
        origin,
        image_shape,
        active_pixels,
        active_indices,
        pair_map,
        right_factors,
        left_factors,
        receipt_sha256,
    })
}

fn validate_clean_mask(
    clean_mask: Option<&Array2<bool>>,
    image_shape: [usize; 2],
    active_pixels: &[[usize; 2]],
) -> Result<(), ImagingError> {
    let Some(mask) = clean_mask else {
        return Err(unsupported(
            "ordered-response v1 requires the deterministic VLASS clean mask",
        ));
    };
    if mask.dim() != (image_shape[0], image_shape[1]) {
        return Err(invalid(
            "ordered-response clean mask dimensions differ from the image",
        ));
    }
    let active = active_pixels.iter().copied().collect::<HashSet<_>>();
    if mask
        .indexed_iter()
        .any(|((x, y), &enabled)| enabled && !active.contains(&[x, y]))
    {
        return Err(unsupported(
            "ordered-response active output domain does not cover every clean-mask pixel",
        ));
    }
    Ok(())
}

fn validate_term_shapes(
    terms: &[Array2<f32>],
    image_shape: [usize; 2],
    role: &str,
) -> Result<(), ImagingError> {
    if terms.len() != MODEL_TERMS
        || terms
            .iter()
            .any(|term| term.dim() != (image_shape[0], image_shape[1]))
    {
        return Err(invalid(format!(
            "ordered-response {role} terms must contain two full-size image planes"
        )));
    }
    Ok(())
}

fn apply_residual_normalization_to_left_factors(
    left_factors: &mut [Complex32],
    active_pixels: &[[usize; 2]],
    active_indices: &[u32],
    normalization_sumwt: f32,
    weight_image: &Array2<f32>,
    pb_limit: f32,
) -> Result<(), ImagingError> {
    if active_pixels.len() != active_indices.len()
        || left_factors.len() != IMAGING_STATES * ETA_POWERS * PIXELS
    {
        return Err(invalid(
            "ordered-response left-factor normalization dimensions are inconsistent",
        ));
    }
    let weight_peak = crate::mosaic_residual_weight_peak(
        weight_image,
        "ordered-response residual weight peak is non-finite or zero",
    )?;
    for (&[x, y], &embedding) in active_pixels.iter().zip(active_indices) {
        let embedding = usize::try_from(embedding)
            .map_err(|_| invalid("ordered-response active index is not a usize"))?;
        let scale =
            crate::mosaic_residual_weight_multiplier(weight_image[(x, y)], weight_peak, pb_limit)
                / normalization_sumwt;
        for state in 0..IMAGING_STATES {
            for power in 0..ETA_POWERS {
                left_factors[(state * ETA_POWERS + power) * PIXELS + embedding] *= scale;
            }
        }
    }
    Ok(())
}

fn apply_model_prediction_normalization_to_right_factors(
    right_factors: &mut [Complex32],
    origin: [i32; 2],
    image_shape: [usize; 2],
    weight_image: &Array2<f32>,
    pb_limit: f32,
) -> Result<(), ImagingError> {
    if right_factors.len() != PREDICTION_STATES * ETA_POWERS * PIXELS
        || weight_image.dim() != (image_shape[0], image_shape[1])
    {
        return Err(invalid(
            "ordered-response right-factor normalization dimensions are inconsistent",
        ));
    }
    let weight_peak = crate::mosaic_residual_weight_peak(
        weight_image,
        "ordered-response model prediction weight peak is non-finite or zero",
    )?;
    let pb_scale_factor = f64::from(weight_peak.sqrt());
    let pb_limit = pb_limit.abs();
    for local_y in 0..SIDE {
        for local_x in 0..SIDE {
            let x = usize::try_from(origin[0] + local_x as i32)
                .map_err(|_| invalid("ordered-response model crop has negative x"))?;
            let y = usize::try_from(origin[1] + local_y as i32)
                .map_err(|_| invalid("ordered-response model crop has negative y"))?;
            if x >= image_shape[0] || y >= image_shape[1] {
                return Err(unsupported(
                    "ordered-response model crop escapes the requested image",
                ));
            }
            let denominator = crate::casa_flat_sky_model_prediction_denominator(
                weight_image[(x, y)],
                pb_scale_factor,
            );
            let multiplier = if denominator > pb_limit {
                denominator.recip()
            } else {
                0.0
            };
            let embedding = local_y * SIDE + local_x;
            for state in 0..PREDICTION_STATES {
                for power in 0..ETA_POWERS {
                    right_factors[(state * ETA_POWERS + power) * PIXELS + embedding] *= multiplier;
                }
            }
        }
    }
    Ok(())
}

fn copy_model_crop(
    buffer: &mut MetalBuffer,
    terms: &[Array2<f32>],
    origin: [i32; 2],
) -> Result<(), ImagingError> {
    let values = shared_slice_mut::<f32>(buffer, MODEL_TERMS * PIXELS);
    values.fill(0.0);
    for (term_index, term) in terms.iter().enumerate() {
        for local_y in 0..SIDE {
            for local_x in 0..SIDE {
                let x = usize::try_from(origin[0] + local_x as i32)
                    .map_err(|_| invalid("ordered-response model crop has negative x"))?;
                let y = usize::try_from(origin[1] + local_y as i32)
                    .map_err(|_| invalid("ordered-response model crop has negative y"))?;
                if x >= term.dim().0 || y >= term.dim().1 {
                    return Err(unsupported(
                        "ordered-response model crop escapes the requested image",
                    ));
                }
                values[term_index * PIXELS + local_y * SIDE + local_x] = term[(x, y)];
            }
        }
    }
    Ok(())
}

fn physical_actions(
    pair_map: &[PhysicalPairMap],
) -> Result<(Vec<u32>, Vec<MixerAction>), ImagingError> {
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
                            input_plane: checked_u32(input_plane, "input plane")?,
                            kernel_plane: checked_u32(kernel_plane, "kernel plane")?,
                            coefficient: binomial(order, left_power)?,
                            _pad0: 0,
                        });
                    }
                }
            }
        }
    }
    let mut offsets = Vec::with_capacity(INVERSE_PLANES + 1);
    let mut actions = Vec::new();
    offsets.push(0);
    for output_actions in grouped {
        actions.extend(output_actions);
        offsets.push(checked_u32(actions.len(), "action offset")?);
    }
    Ok((offsets, actions))
}

fn build_response_bank(
    device: &MetalDevice,
    queue: &MetalQueue,
    graph_device: &MPSGraphDevice,
    pipelines: &OrderedResponsePipelines,
    response: &MetalBuffer,
    construction: ConstructionArtifacts,
) -> Result<usize, ImagingError> {
    let response_inverse = compile_fft_executable(
        graph_device,
        RESPONSE_BATCH_PLANES,
        CONSTRUCTION_SIDE,
        FftDirection::Inverse,
    )?;
    let response_forward = compile_fft_executable(
        graph_device,
        RESPONSE_BATCH_PLANES,
        SIDE,
        FftDirection::Forward,
    )?;
    let oversampled_bytes = RESPONSE_BATCH_PLANES * CONSTRUCTION_PIXELS * COMPLEX_BYTES;
    let compact_bytes = RESPONSE_BATCH_PLANES * PIXELS * COMPLEX_BYTES;
    let high_frequency = private_buffer(device, oversampled_bytes, "response high frequency")?;
    let high_lag = private_buffer(device, oversampled_bytes, "response high lag")?;
    let compact_lag = private_buffer(device, compact_bytes, "response compact lag")?;
    let compact_frequency = private_buffer(device, compact_bytes, "response compact frequency")?;
    let offsets = copied_shared_bytes(device, &construction.offsets, "construction offsets")?;
    let meta = copied_shared_bytes(device, &construction.meta, "construction metadata")?;
    let coefficients = copied_shared_bytes(
        device,
        &construction.coefficients,
        "construction coefficients",
    )?;
    let kernel_values = es_kernel_lut();
    let kernel = copied_shared_buffer(device, &kernel_values, "ES kernel LUT")?;
    let correction_values = es_response_lag_correction_axis()?;
    let correction = copied_shared_buffer(device, &correction_values, "ES correction axis")?;

    let high_shape = shape_array_batch(RESPONSE_BATCH_PLANES, CONSTRUCTION_SIDE, CONSTRUCTION_SIDE);
    let compact_shape = shape_array_batch(RESPONSE_BATCH_PLANES, SIDE, SIDE);
    let high_frequency_data = tensor_data(&high_frequency, &high_shape)?;
    let high_lag_data = tensor_data(&high_lag, &high_shape)?;
    let compact_lag_data = tensor_data(&compact_lag, &compact_shape)?;
    let compact_frequency_data = tensor_data(&compact_frequency, &compact_shape)?;
    let high_frequency_array = NSArray::from_slice(&[&*high_frequency_data]);
    let high_lag_array = NSArray::from_slice(&[&*high_lag_data]);
    let compact_lag_array = NSArray::from_slice(&[&*compact_lag_data]);
    let compact_frequency_array = NSArray::from_slice(&[&*compact_frequency_data]);
    let execution = unsafe { MPSGraphExecutableExecutionDescriptor::new() };
    unsafe {
        execution.setWaitUntilCompleted(false);
    }
    for pair_base in (0..ORDERED_PAIRS).step_by(RESPONSE_BATCH_PAIRS) {
        let command = unsafe { MPSCommandBuffer::commandBufferFromCommandQueue(queue) };
        let root = unsafe { command.rootCommandBuffer() };
        encode_es_construction(
            &root,
            &pipelines.construct_es,
            &offsets,
            &meta,
            &coefficients,
            &kernel,
            &high_frequency,
            pair_base,
            construction.groups,
        )?;
        let _ = unsafe {
            response_inverse
                .executable
                .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                    &command,
                    &high_frequency_array,
                    Some(&high_lag_array),
                    Some(&execution),
                )
        };
        let after_inverse = unsafe { command.rootCommandBuffer() };
        encode_compact_deapodization(
            &after_inverse,
            &pipelines.compact_deapodize_response,
            &high_lag,
            &correction,
            &compact_lag,
        )?;
        if !std::ptr::eq(&*root, &*after_inverse) {
            return Err(unsupported(
                "MPSGraph committed inside ordered-response construction",
            ));
        }
        after_inverse.commit();
        after_inverse.waitUntilCompleted();
        validate_command(&after_inverse, "ordered-response construction")?;

        let compact_command = unsafe { MPSCommandBuffer::commandBufferFromCommandQueue(queue) };
        let compact_root = unsafe { compact_command.rootCommandBuffer() };
        let _ = unsafe {
            response_forward
                .executable
                .encodeToCommandBuffer_inputsArray_resultsArray_executionDescriptor(
                    &compact_command,
                    &compact_lag_array,
                    Some(&compact_frequency_array),
                    Some(&execution),
                )
        };
        let after_forward = unsafe { compact_command.rootCommandBuffer() };
        encode_copy_planes(
            &after_forward,
            &pipelines.copy_response_planes,
            &compact_frequency,
            response,
            pair_base * RESPONSE_COEFFICIENTS,
        )?;
        if !std::ptr::eq(&*compact_root, &*after_forward) {
            return Err(unsupported(
                "MPSGraph committed inside ordered-response compact finalization",
            ));
        }
        after_forward.commit();
        after_forward.waitUntilCompleted();
        validate_command(&after_forward, "ordered-response compact finalization")?;
    }
    Ok(2 * oversampled_bytes
        + 2 * compact_bytes
        + construction.offsets.len()
        + construction.meta.len()
        + construction.coefficients.len()
        + mem::size_of_val(kernel_values.as_slice())
        + mem::size_of_val(correction_values.as_slice()))
}

fn compile_fft_executable(
    graph_device: &MPSGraphDevice,
    batch: usize,
    side: usize,
    direction: FftDirection,
) -> Result<ResidentFftExecutable, ImagingError> {
    let graph = unsafe { MPSGraph::new() };
    let shape = shape_array_batch(batch, side, side);
    let placeholder = unsafe {
        graph.placeholderWithShape_dataType_name(Some(&shape), MPSDataType::ComplexFloat32, None)
    };
    let descriptor = unsafe { MPSGraphFFTDescriptor::descriptor() }.ok_or_else(|| {
        unsupported("ordered-response could not create an MPSGraph FFT descriptor")
    })?;
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
    let targets = NSArray::from_slice(&[&*output]);
    let shaped_type = unsafe {
        MPSGraphShapedType::initWithShape_dataType(
            MPSGraphShapedType::alloc(),
            Some(&shape),
            MPSDataType::ComplexFloat32,
        )
    };
    let feeds: Retained<MPSGraphTensorShapedTypeDictionary> =
        NSDictionary::from_slices(&[&*placeholder], &[&*shaped_type]);
    let executable = unsafe {
        graph.compileWithDevice_feeds_targetTensors_targetOperations_compilationDescriptor(
            Some(graph_device),
            &feeds,
            &targets,
            None,
            None,
        )
    };
    Ok(ResidentFftExecutable {
        _graph: graph,
        executable,
    })
}

fn make_pipelines(device: &MetalDevice) -> Result<OrderedResponsePipelines, ImagingError> {
    let library = device
        .newLibraryWithSource_options_error(&NSString::from_str(ORDERED_RESPONSE_SHADER), None)
        .map_err(|error| {
            unsupported(format!(
                "ordered-response Metal shader compilation failed: {error:?}"
            ))
        })?;
    Ok(OrderedResponsePipelines {
        prepare: make_pipeline(device, &library, "prepare_right_planes")?,
        mix: make_pipeline(device, &library, "mix_ordered_response")?,
        reduce: make_pipeline(device, &library, "reduce_active_left_response")?,
        construct_es: make_pipeline(
            device,
            &library,
            "construct_ordered_response_output_owner_es",
        )?,
        compact_deapodize_response: make_pipeline(
            device,
            &library,
            "compact_deapodize_ordered_response_lag_domain",
        )?,
        copy_response_planes: make_pipeline(device, &library, "copy_ordered_response_planes")?,
    })
}

fn make_pipeline(
    device: &MetalDevice,
    library: &ProtocolObject<dyn MTLLibrary>,
    name: &str,
) -> Result<MetalComputePipeline, ImagingError> {
    let function = library
        .newFunctionWithName(&NSString::from_str(name))
        .ok_or_else(|| unsupported(format!("ordered-response shader lacks {name}")))?;
    device
        .newComputePipelineStateWithFunction_error(&function)
        .map_err(|error| {
            unsupported(format!(
                "ordered-response pipeline {name} compilation failed: {error:?}"
            ))
        })
}

fn tensor_data(
    buffer: &MetalBuffer,
    shape: &NSArray<objc2_foundation::NSNumber>,
) -> Result<Retained<MPSGraphTensorData>, ImagingError> {
    Ok(unsafe {
        MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
            MPSGraphTensorData::alloc(),
            buffer,
            shape,
            MPSDataType::ComplexFloat32,
        )
    })
}

fn private_buffer(
    device: &MetalDevice,
    bytes: usize,
    role: &str,
) -> Result<MetalBuffer, ImagingError> {
    device
        .newBufferWithLength_options(bytes, MTLResourceOptions::StorageModePrivate)
        .ok_or_else(|| unsupported(format!("ordered-response could not allocate {role}")))
}

fn shared_buffer(
    device: &MetalDevice,
    bytes: usize,
    role: &str,
) -> Result<MetalBuffer, ImagingError> {
    device
        .newBufferWithLength_options(bytes, MTLResourceOptions::StorageModeShared)
        .ok_or_else(|| unsupported(format!("ordered-response could not allocate {role}")))
}

fn copied_shared_buffer<T>(
    device: &MetalDevice,
    values: &[T],
    role: &str,
) -> Result<MetalBuffer, ImagingError> {
    let bytes = mem::size_of_val(values);
    if bytes == 0 {
        return Err(invalid(format!("ordered-response {role} source is empty")));
    }
    let pointer = NonNull::new(values.as_ptr().cast::<c_void>().cast_mut())
        .ok_or_else(|| invalid(format!("ordered-response {role} pointer is null")))?;
    unsafe {
        device
            .newBufferWithBytes_length_options(
                pointer,
                bytes,
                MTLResourceOptions::StorageModeShared,
            )
            .ok_or_else(|| unsupported(format!("ordered-response could not allocate {role}")))
    }
}

fn copied_shared_bytes(
    device: &MetalDevice,
    values: &[u8],
    role: &str,
) -> Result<MetalBuffer, ImagingError> {
    copied_shared_buffer(device, values, role)
}

fn set_bytes<T>(
    encoder: &ProtocolObject<dyn MTLComputeCommandEncoder>,
    value: &T,
    index: usize,
) -> Result<(), ImagingError> {
    let pointer = NonNull::new((value as *const T).cast_mut().cast())
        .ok_or_else(|| invalid("ordered-response parameter pointer is null"))?;
    unsafe {
        encoder.setBytes_length_atIndex(pointer, mem::size_of::<T>(), index);
    }
    Ok(())
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

fn encode_prepare(
    command: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    model: &MetalBuffer,
    right_factors: &MetalBuffer,
    output: &MetalBuffer,
) -> Result<(), ImagingError> {
    let encoder = command
        .computeCommandEncoder()
        .ok_or_else(|| unsupported("ordered-response could not create prepare encoder"))?;
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(model), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(right_factors), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 2);
    }
    set_bytes(
        &encoder,
        &LinearParams {
            elements: checked_u32(FORWARD_PLANES * PIXELS, "forward elements")?,
            plane_elements: checked_u32(PIXELS, "forward plane pixels")?,
            _pad0: 0,
            _pad1: 0,
        },
        3,
    )?;
    dispatch_linear(&encoder, pipeline, FORWARD_PLANES * PIXELS);
    encoder.endEncoding();
    Ok(())
}

fn encode_mixer(
    command: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    forward: &MetalBuffer,
    response: &MetalBuffer,
    offsets: &MetalBuffer,
    actions: &MetalBuffer,
    mixed: &MetalBuffer,
) -> Result<(), ImagingError> {
    let encoder = command
        .computeCommandEncoder()
        .ok_or_else(|| unsupported("ordered-response could not create mixer encoder"))?;
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(forward), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(response), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(offsets), 0, 2);
        encoder.setBuffer_offset_atIndex(Some(actions), 0, 3);
        encoder.setBuffer_offset_atIndex(Some(mixed), 0, 4);
    }
    set_bytes(
        &encoder,
        &MixerParams {
            pixels: checked_u32(PIXELS, "mixer pixels")?,
            output_planes: checked_u32(INVERSE_PLANES, "mixer output planes")?,
            response_scale: 1.0,
            _pad1: 0,
        },
        5,
    )?;
    let (width, height) = threadgroup_2d(pipeline, PIXELS, INVERSE_PLANES);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: PIXELS,
            height: INVERSE_PLANES,
            depth: 1,
        },
        MTLSize {
            width,
            height,
            depth: 1,
        },
    );
    encoder.endEncoding();
    Ok(())
}

fn encode_reduction(
    command: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    inverse: &MetalBuffer,
    left_factors: &MetalBuffer,
    active_indices: &MetalBuffer,
    feedback: &MetalBuffer,
    active_pixels: usize,
) -> Result<(), ImagingError> {
    let encoder = command
        .computeCommandEncoder()
        .ok_or_else(|| unsupported("ordered-response could not create reduction encoder"))?;
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(inverse), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(left_factors), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(active_indices), 0, 2);
        encoder.setBuffer_offset_atIndex(Some(feedback), 0, 3);
    }
    set_bytes(
        &encoder,
        &ReductionParams {
            pixels: checked_u32(PIXELS, "reduction pixels")?,
            active_pixels: checked_u32(active_pixels, "active pixels")?,
            imaging_states: checked_u32(IMAGING_STATES, "imaging states")?,
            eta_powers: checked_u32(ETA_POWERS, "eta powers")?,
        },
        4,
    )?;
    let (width, height) = threadgroup_2d(pipeline, active_pixels, OUTPUT_TERMS);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: active_pixels,
            height: OUTPUT_TERMS,
            depth: 1,
        },
        MTLSize {
            width,
            height,
            depth: 1,
        },
    );
    encoder.endEncoding();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn encode_es_construction(
    command: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    offsets: &MetalBuffer,
    meta: &MetalBuffer,
    coefficients: &MetalBuffer,
    kernel: &MetalBuffer,
    output: &MetalBuffer,
    state_base: usize,
    groups: usize,
) -> Result<(), ImagingError> {
    let encoder = command
        .computeCommandEncoder()
        .ok_or_else(|| unsupported("ordered-response could not create construction encoder"))?;
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(offsets), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(meta), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(coefficients), 0, 2);
        encoder.setBuffer_offset_atIndex(Some(kernel), 0, 3);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 4);
    }
    set_bytes(
        &encoder,
        &ConstructionParams {
            side: checked_u32(CONSTRUCTION_SIDE, "construction side")?,
            states: checked_u32(RESPONSE_BATCH_PAIRS, "construction states")?,
            coefficients: checked_u32(RESPONSE_COEFFICIENTS, "response coefficients")?,
            support_width: checked_u32(ES_SUPPORT_WIDTH, "ES support width")?,
            oversampling: checked_u32(ES_LUT_INTERVALS, "ES LUT intervals")?,
            offset_bias: 0,
            group_count: checked_u32(groups, "response groups")?,
            state_base: checked_u32(state_base, "construction state base")?,
        },
        5,
    )?;
    let (width, height) = threadgroup_2d(pipeline, CONSTRUCTION_SIDE, CONSTRUCTION_SIDE);
    encoder.dispatchThreads_threadsPerThreadgroup(
        MTLSize {
            width: CONSTRUCTION_SIDE,
            height: CONSTRUCTION_SIDE,
            depth: RESPONSE_BATCH_PAIRS,
        },
        MTLSize {
            width,
            height,
            depth: 1,
        },
    );
    encoder.endEncoding();
    Ok(())
}

fn encode_compact_deapodization(
    command: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    input: &MetalBuffer,
    correction: &MetalBuffer,
    output: &MetalBuffer,
) -> Result<(), ImagingError> {
    let encoder = command
        .computeCommandEncoder()
        .ok_or_else(|| unsupported("ordered-response could not create deapodization encoder"))?;
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(input), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(correction), 0, 1);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 2);
    }
    set_bytes(
        &encoder,
        &CompactCorrectionParams {
            input_side: checked_u32(CONSTRUCTION_SIDE, "construction side")?,
            output_side: checked_u32(SIDE, "resident side")?,
            planes: checked_u32(RESPONSE_BATCH_PLANES, "response batch planes")?,
            _pad0: 0,
        },
        3,
    )?;
    dispatch_linear(&encoder, pipeline, RESPONSE_BATCH_PLANES * PIXELS);
    encoder.endEncoding();
    Ok(())
}

fn encode_copy_planes(
    command: &MetalCommandBuffer,
    pipeline: &MetalComputePipeline,
    input: &MetalBuffer,
    output: &MetalBuffer,
    output_plane_base: usize,
) -> Result<(), ImagingError> {
    let encoder = command
        .computeCommandEncoder()
        .ok_or_else(|| unsupported("ordered-response could not create plane-copy encoder"))?;
    encoder.setComputePipelineState(pipeline);
    unsafe {
        encoder.setBuffer_offset_atIndex(Some(input), 0, 0);
        encoder.setBuffer_offset_atIndex(Some(output), 0, 1);
    }
    set_bytes(
        &encoder,
        &PlaneCopyParams {
            elements: checked_u32(RESPONSE_BATCH_PLANES * PIXELS, "response copy elements")?,
            plane_elements: checked_u32(PIXELS, "response copy plane pixels")?,
            output_plane_base: checked_u32(output_plane_base, "response output plane base")?,
            _pad0: 0,
        },
        2,
    )?;
    dispatch_linear(&encoder, pipeline, RESPONSE_BATCH_PLANES * PIXELS);
    encoder.endEncoding();
    Ok(())
}

fn validate_command(command: &MetalCommandBuffer, role: &str) -> Result<(), ImagingError> {
    if command.status() == MTLCommandBufferStatus::Error {
        Err(unsupported(format!("{role} failed on the Metal device")))
    } else {
        Ok(())
    }
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

fn es_response_lag_correction_axis() -> Result<Vec<f32>, ImagingError> {
    let radius = ES_SUPPORT_WIDTH / 2;
    (0..SIDE)
        .map(|index| {
            let signed = if index < SIDE / 2 {
                index as isize
            } else {
                index as isize - SIDE as isize
            };
            let phase_scale = std::f64::consts::TAU * signed as f64 / CONSTRUCTION_SIDE as f64;
            let transform = (-(radius as isize)..=radius as isize)
                .map(|delta| es_kernel_weight(0.0, delta) * (phase_scale * delta as f64).cos())
                .sum::<f64>();
            if transform <= 1.0e-12 {
                Err(invalid("ordered-response ES lag correction is singular"))
            } else {
                Ok((1.0 / transform) as f32)
            }
        })
        .collect()
}

fn validate_pair_map(pair_map: &[PhysicalPairMap]) -> Result<(), ImagingError> {
    if pair_map.len() != ORDERED_PAIRS {
        return Err(invalid(
            "ordered-response physical pair inventory has the wrong length",
        ));
    }
    for (pair, mapping) in pair_map.iter().enumerate() {
        if mapping.pair_index != pair
            || mapping.imaging_state >= IMAGING_STATES
            || mapping.prediction_state >= PREDICTION_STATES
            || mapping.imaging_screen_state >= PREDICTION_STATES
            || mapping.prediction_screen_state >= PREDICTION_STATES
        {
            return Err(invalid(
                "ordered-response physical pair map is not dense and in source order",
            ));
        }
    }
    Ok(())
}

fn screen_state_maps(
    pair_map: &[PhysicalPairMap],
) -> Result<([usize; IMAGING_STATES], [usize; PREDICTION_STATES]), ImagingError> {
    let mut imaging = [usize::MAX; IMAGING_STATES];
    let mut prediction = [usize::MAX; PREDICTION_STATES];
    for mapping in pair_map {
        let imaging_state = &mut imaging[mapping.imaging_state];
        if *imaging_state != usize::MAX && *imaging_state != mapping.imaging_screen_state {
            return Err(invalid(
                "ordered-response compact imaging state maps to multiple screens",
            ));
        }
        *imaging_state = mapping.imaging_screen_state;
        let prediction_state = &mut prediction[mapping.prediction_state];
        if *prediction_state != usize::MAX && *prediction_state != mapping.prediction_screen_state {
            return Err(invalid(
                "ordered-response compact prediction state maps to multiple screens",
            ));
        }
        *prediction_state = mapping.prediction_screen_state;
    }
    if imaging.contains(&usize::MAX) || prediction.contains(&usize::MAX) {
        return Err(invalid(
            "ordered-response physical pair map does not cover every compact state",
        ));
    }
    Ok((imaging, prediction))
}

fn facet_eta(
    pixel: [i32; 2],
    center: [f64; 2],
    image_reference: f64,
    cell_rad: f64,
) -> Result<f64, ImagingError> {
    fn direction(
        pixel: [f64; 2],
        image_reference: f64,
        cell_rad: f64,
    ) -> Result<[f64; 3], ImagingError> {
        let l = (pixel[0] - image_reference) * cell_rad;
        let m = (image_reference - pixel[1]) * cell_rad;
        let n_sq = 1.0 - l * l - m * m;
        if n_sq <= 0.0 {
            return Err(unsupported(
                "ordered-response embedding escapes the direction-cosine hemisphere",
            ));
        }
        Ok([l, m, n_sq.sqrt()])
    }
    let pixel_direction = direction(
        [f64::from(pixel[0]), f64::from(pixel[1])],
        image_reference,
        cell_rad,
    )?;
    let normal = direction(center, image_reference, cell_rad)?;
    Ok(pixel_direction
        .iter()
        .zip(normal)
        .map(|(value, basis)| value * basis)
        .sum::<f64>()
        - 1.0)
}

fn embedding_index(pixel: [i32; 2], origin: [i32; 2]) -> Result<usize, ImagingError> {
    let x = pixel[0] - origin[0];
    let y = pixel[1] - origin[1];
    if !(0..SIDE as i32).contains(&x) || !(0..SIDE as i32).contains(&y) {
        return Err(unsupported(
            "ordered-response active pixel escapes the resident embedding",
        ));
    }
    Ok(y as usize * SIDE + x as usize)
}

fn complex32_values_from_le_bytes(values: &[u8]) -> Result<Vec<Complex32>, ImagingError> {
    if values.len() % COMPLEX_BYTES != 0 {
        return Err(invalid(
            "ordered-response complex artifact has a partial element",
        ));
    }
    Ok(values
        .chunks_exact(COMPLEX_BYTES)
        .map(|value| {
            Complex32::new(
                f32::from_le_bytes(value[..4].try_into().expect("four-byte real")),
                f32::from_le_bytes(value[4..].try_into().expect("four-byte imaginary")),
            )
        })
        .collect())
}

fn json_i32_pair(value: &serde_json::Value, role: &str) -> Result<[i32; 2], ImagingError> {
    let values = value
        .as_array()
        .ok_or_else(|| invalid(format!("ordered-response {role} is not a pair")))?;
    if values.len() != 2 {
        return Err(invalid(format!(
            "ordered-response {role} does not have two entries"
        )));
    }
    Ok([
        i32::try_from(
            values[0]
                .as_i64()
                .ok_or_else(|| invalid(format!("ordered-response {role} x is not an integer")))?,
        )
        .map_err(|_| invalid(format!("ordered-response {role} x exceeds i32")))?,
        i32::try_from(
            values[1]
                .as_i64()
                .ok_or_else(|| invalid(format!("ordered-response {role} y is not an integer")))?,
        )
        .map_err(|_| invalid(format!("ordered-response {role} y exceeds i32")))?,
    ])
}

fn json_i32_pairs(value: &serde_json::Value, role: &str) -> Result<Vec<[i32; 2]>, ImagingError> {
    value
        .as_array()
        .ok_or_else(|| invalid(format!("ordered-response {role} is not an array")))?
        .iter()
        .map(|value| json_i32_pair(value, role))
        .collect()
}

fn json_f64_pair(value: &serde_json::Value, role: &str) -> Result<[f64; 2], ImagingError> {
    let values = value
        .as_array()
        .ok_or_else(|| invalid(format!("ordered-response {role} is not a pair")))?;
    if values.len() != 2 {
        return Err(invalid(format!(
            "ordered-response {role} does not have two entries"
        )));
    }
    Ok([
        values[0]
            .as_f64()
            .ok_or_else(|| invalid(format!("ordered-response {role} x is not numeric")))?,
        values[1]
            .as_f64()
            .ok_or_else(|| invalid(format!("ordered-response {role} y is not numeric")))?,
    ])
}

fn json_usize(entry: &serde_json::Value, name: &str) -> Result<usize, ImagingError> {
    usize::try_from(
        entry[name]
            .as_u64()
            .ok_or_else(|| invalid(format!("ordered-response pair map lacks {name}")))?,
    )
    .map_err(|_| invalid(format!("ordered-response pair-map {name} exceeds usize")))
}

fn resolve_json_artifact(
    manifest_path: &Path,
    value: &serde_json::Value,
) -> Result<PathBuf, ImagingError> {
    let path = PathBuf::from(
        value
            .as_str()
            .ok_or_else(|| invalid("ordered-response artifact path is not a string"))?,
    );
    Ok(if path.is_absolute() {
        path
    } else {
        manifest_path
            .parent()
            .ok_or_else(|| invalid("ordered-response manifest has no parent directory"))?
            .join(path)
    })
}

fn verify_sha256(values: &[u8], expected: Option<&str>, role: &str) -> Result<(), ImagingError> {
    let expected = expected
        .ok_or_else(|| invalid(format!("ordered-response receipt lacks the {role} SHA-256")))?;
    let actual = format!("{:x}", Sha256::digest(values));
    if actual == expected {
        Ok(())
    } else {
        Err(invalid(format!("ordered-response {role} SHA-256 mismatch")))
    }
}

fn read_artifact(directory: &Path, name: &str) -> Result<Vec<u8>, ImagingError> {
    let path = directory.join(name);
    fs::read(&path).map_err(|error| {
        invalid(format!(
            "read ordered-response artifact {}: {error}",
            path.display()
        ))
    })
}

fn little_u32_at(values: &[u8], index: usize) -> Result<u32, ImagingError> {
    let offset = index
        .checked_mul(mem::size_of::<u32>())
        .ok_or_else(|| invalid("ordered-response u32 offset overflowed"))?;
    let end = offset
        .checked_add(mem::size_of::<u32>())
        .ok_or_else(|| invalid("ordered-response u32 end overflowed"))?;
    let bytes = values
        .get(offset..end)
        .ok_or_else(|| invalid("ordered-response u32 offset escaped its artifact"))?;
    Ok(u32::from_le_bytes(bytes.try_into().map_err(|_| {
        invalid("ordered-response u32 is incomplete")
    })?))
}

fn required_absolute_path(name: &str) -> Result<PathBuf, ImagingError> {
    let path = std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| invalid(format!("ordered-response requires {name}")))?;
    if !path.is_absolute() {
        return Err(invalid(format!("{name} must be an absolute path")));
    }
    Ok(path)
}

fn checked_u32(value: usize, role: &str) -> Result<u32, ImagingError> {
    u32::try_from(value).map_err(|_| invalid(format!("ordered-response {role} exceeds u32")))
}

fn binomial(order: usize, left_power: usize) -> Result<f32, ImagingError> {
    match (order, left_power) {
        (0, 0) | (1, 0) | (1, 1) | (2, 0) | (2, 2) => Ok(1.0),
        (2, 1) => Ok(2.0),
        _ => Err(invalid("ordered-response binomial order is unsupported")),
    }
}

fn shared_slice<T>(buffer: &MetalBuffer, count: usize) -> &[T] {
    unsafe { slice::from_raw_parts(buffer.contents().as_ptr().cast::<T>(), count) }
}

fn shared_slice_mut<T>(buffer: &mut MetalBuffer, count: usize) -> &mut [T] {
    unsafe { slice::from_raw_parts_mut(buffer.contents().as_ptr().cast::<T>(), count) }
}

fn invalid(message: impl Into<String>) -> ImagingError {
    ImagingError::InvalidRequest(message.into())
}

fn unsupported(message: impl Into<String>) -> ImagingError {
    ImagingError::Unsupported(message.into())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use num_complex::Complex64;

    use super::*;

    #[test]
    fn left_factors_fold_sumwt_and_pb_normalization_once() {
        let mut factors = vec![Complex32::new(0.0, 0.0); IMAGING_STATES * ETA_POWERS * PIXELS];
        let active_pixels = [[1, 1]];
        let active_indices = [7_u32];
        for state in 0..IMAGING_STATES {
            for power in 0..ETA_POWERS {
                factors[(state * ETA_POWERS + power) * PIXELS + 7] = Complex32::new(1.0, -2.0);
            }
        }
        let mut weight = Array2::<f32>::ones((2, 2));
        weight[(0, 0)] = 4.0;

        apply_residual_normalization_to_left_factors(
            &mut factors,
            &active_pixels,
            &active_indices,
            10.0,
            &weight,
            0.0001,
        )
        .expect("fold residual normalization");

        for state in 0..IMAGING_STATES {
            for power in 0..ETA_POWERS {
                assert_eq!(
                    factors[(state * ETA_POWERS + power) * PIXELS + 7],
                    Complex32::new(0.05, -0.1),
                );
            }
        }
    }

    #[test]
    fn right_factors_fold_flat_sky_model_prediction_once() {
        let mut factors = vec![Complex32::new(1.0, -2.0); PREDICTION_STATES * ETA_POWERS * PIXELS];
        let mut weight = Array2::<f32>::ones((SIDE, SIDE));
        weight[(0, 0)] = 4.0;
        weight[(2, 3)] = 0.0;

        apply_model_prediction_normalization_to_right_factors(
            &mut factors,
            [0, 0],
            [SIDE, SIDE],
            &weight,
            0.0001,
        )
        .expect("fold model prediction normalization");

        let doubled = SIDE + 1;
        let masked = 3 * SIDE + 2;
        for state in 0..PREDICTION_STATES {
            for power in 0..ETA_POWERS {
                assert_eq!(
                    factors[(state * ETA_POWERS + power) * PIXELS + doubled],
                    Complex32::new(2.0, -4.0),
                );
                assert_eq!(
                    factors[(state * ETA_POWERS + power) * PIXELS + masked],
                    Complex32::new(0.0, -0.0),
                );
            }
        }
    }

    #[test]
    #[ignore = "requires the frozen external VLASS receipt, v12 construction, and Metal"]
    fn production_operator_matches_physical_semantic_receipt() {
        let image_shape = [4_096, 4_096];
        let mask = Array2::<bool>::from_elem((image_shape[0], image_shape[1]), false);
        let dirty = vec![Array2::<f32>::zeros((4_096, 4_096)); 2];
        let weight = Array2::<f32>::ones((4_096, 4_096));
        let construction_directory =
            required_absolute_path(CONSTRUCTION_DIRECTORY_ENV).expect("construction path");
        let construction_manifest: serde_json::Value = serde_json::from_slice(
            &fs::read(construction_directory.join("manifest.json"))
                .expect("read construction manifest"),
        )
        .expect("parse construction manifest");
        let source_rows_sha256 = construction_manifest["source"]["row_payload_sha256"]
            .as_str()
            .expect("construction source hash");
        let mut operator = VlassOrderedResponseOperator::from_environment(OrderedResponseRequest {
            image_shape,
            nterms: 2,
            clean_mask: Some(&mask),
            dirty_terms: &dirty,
            normalization_sumwt: 1.0,
            weight_image: &weight,
            pb_limit: 0.0001,
            source_rows_sha256,
        })
        .expect("construct production ordered-response operator");
        let receipt_path = required_absolute_path(PHYSICAL_RECEIPT_ENV).expect("receipt path");
        let receipt: serde_json::Value =
            serde_json::from_slice(&fs::read(receipt_path).expect("read receipt"))
                .expect("parse receipt");
        let fixture = &receipt["resident_integration_fixture"];
        let source_pixels =
            json_i32_pairs(&fixture["source_pixels"], "source pixels").expect("source pixels");
        let model_values = fixture["model_values"]
            .as_array()
            .expect("model values")
            .iter()
            .map(|value| value.as_f64().expect("model value") as f32)
            .collect::<Vec<_>>();
        let output_probes = json_i32_pairs(&fixture["output_probe_pixels"], "output probe pixels")
            .expect("output probes");
        let expected = fixture["aggregate_total_order_two"]["values"]
            .as_array()
            .expect("expected aggregate values")
            .iter()
            .map(|value| {
                let pair = json_f64_pair(value, "expected complex value").expect("complex pair");
                (pair[0], pair[1])
            })
            .collect::<Vec<_>>();
        assert_eq!(source_pixels.len(), model_values.len());
        assert_eq!(
            expected.len(),
            MODEL_TERMS * OUTPUT_TERMS * output_probes.len()
        );
        let active_ordinals = operator
            .active_pixels
            .iter()
            .enumerate()
            .map(|(ordinal, pixel)| (*pixel, ordinal))
            .collect::<HashMap<_, _>>();

        let mut actual_values = Vec::<Complex64>::new();
        let mut reference_values = Vec::<Complex64>::new();
        for model_case in 0..MODEL_TERMS {
            let mut model = vec![Array2::<f32>::zeros((4_096, 4_096)); MODEL_TERMS];
            for (&[x, y], &amplitude) in source_pixels.iter().zip(&model_values) {
                model[model_case][(x as usize, y as usize)] = amplitude;
            }
            let mut residual = dirty.clone();
            let application = operator
                .refresh_residual(&model, &mut residual)
                .expect("apply production ordered-response operator");
            assert!(application.elapsed > Duration::ZERO);

            let feedback = shared_slice::<Complex32>(
                &operator.feedback,
                operator.active_pixels.len() * OUTPUT_TERMS,
            );
            for output_term in 0..OUTPUT_TERMS {
                for (probe_index, &[x, y]) in output_probes.iter().enumerate() {
                    let Some(&active_ordinal) = active_ordinals.get(&[x as usize, y as usize])
                    else {
                        continue;
                    };
                    let actual =
                        feedback[output_term * operator.active_pixels.len() + active_ordinal];
                    let (expected_re, expected_im) =
                        expected[(model_case * OUTPUT_TERMS + output_term) * output_probes.len()
                            + probe_index];
                    actual_values.push(Complex64::new(f64::from(actual.re), f64::from(actual.im)));
                    reference_values.push(Complex64::new(expected_re, expected_im));
                }
            }
        }
        let difference_l2 = actual_values
            .iter()
            .zip(&reference_values)
            .map(|(actual, reference)| (*actual - *reference).norm_sqr())
            .sum::<f64>()
            .sqrt();
        let reference_l2 = reference_values
            .iter()
            .map(Complex64::norm_sqr)
            .sum::<f64>()
            .sqrt()
            .max(f64::MIN_POSITIVE);
        let difference_linf = actual_values
            .iter()
            .zip(&reference_values)
            .map(|(actual, reference)| (*actual - *reference).norm())
            .fold(0.0_f64, f64::max);
        let reference_linf = reference_values
            .iter()
            .map(|value| value.norm())
            .fold(0.0_f64, f64::max)
            .max(f64::MIN_POSITIVE);
        let relative_l2 = difference_l2 / reference_l2;
        let normalized_linf = difference_linf / reference_linf;
        eprintln!(
            "production_ordered_response_physical_semantic relative_l2={relative_l2:.9e} \
             normalized_linf={normalized_linf:.9e}"
        );
        assert!(
            relative_l2 <= 2.0e-5 && normalized_linf <= 2.0e-5,
            "production ordered-response differs from physical receipt: \
             l2={relative_l2} linf={normalized_linf}"
        );
    }
}
