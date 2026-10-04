// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imaging_reconstruction::{MinorCycleError, runtime_adapter::ClarkRefresh};
use objc2::{AnyThread, rc::Retained, runtime::ProtocolObject};
use objc2_foundation::{NSArray, NSDictionary, NSError, NSNumber};
use objc2_metal::{
    MTLBuffer, MTLCommandQueue, MTLCreateSystemDefaultDevice, MTLDevice, MTLResourceOptions,
};
use objc2_metal_performance_shaders::MPSDataType;
use objc2_metal_performance_shaders_graph::{
    MPSGraph, MPSGraphCompilationDescriptor, MPSGraphDevice, MPSGraphExecutable,
    MPSGraphExecutableExecutionDescriptor, MPSGraphFFTDescriptor, MPSGraphFFTScalingMode,
    MPSGraphOptions, MPSGraphShapedType, MPSGraphTensor, MPSGraphTensorData,
};
use std::{
    ptr::NonNull,
    sync::{Arc, Mutex, MutexGuard},
};

fn error(message: impl Into<String>) -> MinorCycleError {
    MinorCycleError::ClarkRefresh(message.into())
}

pub(super) struct Workspace {
    shape: [usize; 2],
    pub(super) padded: [usize; 2],
    limit: u64,
    device: Retained<ProtocolObject<dyn MTLDevice>>,
    queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    input: Retained<ProtocolObject<dyn MTLBuffer>>,
    // TensorData may not retain its backing buffers.
    _spectrum: Retained<ProtocolObject<dyn MTLBuffer>>,
    output: Retained<ProtocolObject<dyn MTLBuffer>>,
    psf: Retained<MPSGraphExecutable>,
    psf_inputs: Retained<NSArray<MPSGraphTensorData>>,
    psf_outputs: Retained<NSArray<MPSGraphTensorData>>,
    convolution: Retained<MPSGraphExecutable>,
    inputs: Retained<NSArray<MPSGraphTensorData>>,
    outputs: Retained<NSArray<MPSGraphTensorData>>,
}

// SAFETY: The run owner holds a mutex for the complete solve. Metal and graph
// objects have no thread affinity; neither native handles nor mutable contents
// escape this module. All graph executions and error callbacks finish before
// host access, workspace release or lease release.
unsafe impl Send for Workspace {}

fn shape(values: &[usize]) -> Retained<NSArray<NSNumber>> {
    NSArray::from_retained_slice(
        &values
            .iter()
            .map(|n| NSNumber::new_i64(*n as i64))
            .collect::<Vec<_>>(),
    )
}

fn compile(
    graph: &MPSGraph,
    device: &MPSGraphDevice,
    tensors: &[&MPSGraphTensor],
    types: &[&MPSGraphShapedType],
    target: &MPSGraphTensor,
) -> Result<Retained<MPSGraphExecutable>, MinorCycleError> {
    let failure = Arc::new(Mutex::new(None));
    let reported = failure.clone();
    let handler = block2::RcBlock::new(
        move |_: NonNull<MPSGraphExecutable>, failure: *mut NSError| {
            if let Some(failure) = unsafe { failure.as_ref() } {
                *reported.lock().expect("callback lock") =
                    Some(failure.localizedDescription().to_string());
            }
        },
    );
    let descriptor = unsafe { MPSGraphCompilationDescriptor::new() };
    unsafe {
        graph.setOptions(MPSGraphOptions::SynchronizeResults);
        descriptor.setWaitForCompilationCompletion(true);
        descriptor.setCompilationCompletionHandler(block2::RcBlock::as_ptr(&handler));
    }
    let feeds = NSDictionary::from_slices(tensors, types);
    let targets = NSArray::from_slice(&[target]);
    let result = unsafe {
        graph.compileWithDevice_feeds_targetTensors_targetOperations_compilationDescriptor(
            Some(device),
            &feeds,
            &targets,
            None,
            Some(&descriptor),
        )
    };
    if let Some(message) = failure
        .lock()
        .map_err(|_| error("compilation error lock poisoned"))?
        .take()
    {
        return Err(error(message));
    }
    Ok(result)
}

fn run(
    executable: &MPSGraphExecutable,
    queue: &ProtocolObject<dyn MTLCommandQueue>,
    inputs: &NSArray<MPSGraphTensorData>,
    outputs: &NSArray<MPSGraphTensorData>,
) -> Result<(), MinorCycleError> {
    let failure = Arc::new(Mutex::new(None));
    let reported = failure.clone();
    let handler = block2::RcBlock::new(
        move |_: NonNull<NSArray<MPSGraphTensorData>>, failure: *mut NSError| {
            if let Some(failure) = unsafe { failure.as_ref() } {
                *reported.lock().expect("callback lock") =
                    Some(failure.localizedDescription().to_string());
            }
        },
    );
    let descriptor = unsafe { MPSGraphExecutableExecutionDescriptor::new() };
    unsafe {
        descriptor.setWaitUntilCompleted(true);
        descriptor.setCompletionHandler(block2::RcBlock::as_ptr(&handler));
        executable.runWithMTLCommandQueue_inputsArray_resultsArray_executionDescriptor(
            queue,
            inputs,
            Some(outputs),
            Some(&descriptor),
        );
    }
    if let Some(message) = failure
        .lock()
        .map_err(|_| error("execution error lock poisoned"))?
        .take()
    {
        return Err(error(message));
    }
    Ok(())
}

impl Workspace {
    #[cfg(test)]
    pub(super) fn input_identity(&self) -> usize {
        self.input.contents().as_ptr() as usize
    }
    pub(super) fn new(
        image: [usize; 2],
        padded: [usize; 2],
        limit: u64,
    ) -> Result<Self, MinorCycleError> {
        let primary = (padded[0] * padded[1] * 4
            + padded[0] * (padded[1] / 2 + 1) * 8
            + image[0] * image[1] * 4) as u64;
        if primary > limit {
            return Err(error("explicit Metal buffers exceed admitted workspace"));
        }
        objc2::rc::autoreleasepool(|_| unsafe {
            let device =
                MTLCreateSystemDefaultDevice().ok_or_else(|| error("Metal device unavailable"))?;
            if !device.hasUnifiedMemory() {
                return Err(error("Metal device does not have unified memory"));
            }
            let queue = device
                .newCommandQueue()
                .ok_or_else(|| error("Metal queue unavailable"))?;
            let real_shape = shape(&[1, padded[0], padded[1]]);
            let half_shape = shape(&[1, padded[0], padded[1] / 2 + 1]);
            let image_shape = shape(&[1, image[0], image[1]]);
            let buffer = |bytes| {
                device
                    .newBufferWithLength_options(bytes, MTLResourceOptions::StorageModeShared)
                    .ok_or_else(|| error("Metal buffer allocation failed"))
            };
            let input = buffer(padded[0] * padded[1] * 4)?;
            let spectrum = buffer(padded[0] * (padded[1] / 2 + 1) * 8)?;
            let output = buffer(image[0] * image[1] * 4)?;
            let tensor_data =
                |buffer: &ProtocolObject<dyn MTLBuffer>, shape: &NSArray<NSNumber>, data_type| {
                    MPSGraphTensorData::initWithMTLBuffer_shape_dataType(
                        MPSGraphTensorData::alloc(),
                        buffer,
                        shape,
                        data_type,
                    )
                };
            let real_data = tensor_data(&input, &real_shape, MPSDataType::Float32);
            let half_data = tensor_data(&spectrum, &half_shape, MPSDataType::ComplexFloat32);
            let output_data = tensor_data(&output, &image_shape, MPSDataType::Float32);
            let real_type = MPSGraphShapedType::initWithShape_dataType(
                MPSGraphShapedType::alloc(),
                Some(&real_shape),
                MPSDataType::Float32,
            );
            let half_type = MPSGraphShapedType::initWithShape_dataType(
                MPSGraphShapedType::alloc(),
                Some(&half_shape),
                MPSDataType::ComplexFloat32,
            );
            let axes = shape(&[1, 2]);
            let forward = MPSGraphFFTDescriptor::descriptor()
                .ok_or_else(|| error("FFT descriptor unavailable"))?;
            let inverse = MPSGraphFFTDescriptor::descriptor()
                .ok_or_else(|| error("FFT descriptor unavailable"))?;
            inverse.setInverse(true);
            inverse.setRoundToOddHermitean(padded[1] % 2 == 1);
            forward.setScalingMode(MPSGraphFFTScalingMode::None);
            inverse.setScalingMode(MPSGraphFFTScalingMode::None);
            let graph_device = MPSGraphDevice::deviceWithMTLDevice(&device);
            let prepare = MPSGraph::new();
            let psf_input = prepare.placeholderWithShape_dataType_name(
                Some(&real_shape),
                MPSDataType::Float32,
                None,
            );
            let psf_spectrum = prepare.realToHermiteanFFTWithTensor_axes_descriptor_name(
                &psf_input, &axes, &forward, None,
            );
            let psf = compile(
                &prepare,
                &graph_device,
                &[&psf_input],
                &[&real_type],
                &psf_spectrum,
            )?;
            let graph = MPSGraph::new();
            let real = graph.placeholderWithShape_dataType_name(
                Some(&real_shape),
                MPSDataType::Float32,
                None,
            );
            let kernel = graph.placeholderWithShape_dataType_name(
                Some(&half_shape),
                MPSDataType::ComplexFloat32,
                None,
            );
            let fft = graph
                .realToHermiteanFFTWithTensor_axes_descriptor_name(&real, &axes, &forward, None);
            let product =
                graph.multiplicationWithPrimaryTensor_secondaryTensor_name(&fft, &kernel, None);
            let convolved = graph
                .HermiteanToRealFFTWithTensor_axes_descriptor_name(&product, &axes, &inverse, None);
            let crop_x = graph.sliceTensor_dimension_start_length_name(
                &convolved,
                1,
                0,
                image[0] as isize,
                None,
            );
            let crop = graph.sliceTensor_dimension_start_length_name(
                &crop_x,
                2,
                0,
                image[1] as isize,
                None,
            );
            let convolution = compile(
                &graph,
                &graph_device,
                &[&real, &kernel],
                &[&real_type, &half_type],
                &crop,
            )?;
            let mut ordered = Vec::new();
            let feeds = convolution
                .feedTensors()
                .ok_or_else(|| error("compiled FFT omitted feeds"))?;
            for feed in feeds.iter() {
                if feed == real {
                    ordered.push(real_data.clone());
                } else if feed == kernel {
                    ordered.push(half_data.clone());
                } else {
                    return Err(error("unexpected compiled FFT feed"));
                }
            }
            let workspace = Self {
                shape: image,
                padded,
                limit,
                device,
                queue,
                input,
                _spectrum: spectrum,
                output,
                psf,
                psf_inputs: NSArray::from_retained_slice(&[real_data]),
                psf_outputs: NSArray::from_retained_slice(&[half_data]),
                convolution,
                inputs: NSArray::from_retained_slice(&ordered),
                outputs: NSArray::from_retained_slice(&[output_data]),
            };
            workspace.check_residency()?;
            Ok(workspace)
        })
    }

    fn input_values(&mut self) -> &mut [f32] {
        // The solve's mutex and synchronous graph boundary protect this view.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.input.contents().as_ptr().cast(),
                self.padded[0] * self.padded[1],
            )
        }
    }

    fn check_residency(&self) -> Result<(), MinorCycleError> {
        let bytes = self.device.currentAllocatedSize() as u64;
        if bytes > self.limit {
            return Err(error(format!(
                "Metal device allocations {bytes} exceed admitted library workspace {}",
                self.limit
            )));
        }
        Ok(())
    }

    pub(super) fn prepare_psf(
        &mut self,
        psf: &[f32],
        center: [usize; 2],
    ) -> Result<(), MinorCycleError> {
        let padded = self.padded;
        let image = self.shape;
        let values = self.input_values();
        values.fill(0.0);
        for x in 0..image[0] {
            for y in 0..image[1] {
                let px = (x + padded[0] - center[0]) % padded[0];
                let py = (y + padded[1] - center[1]) % padded[1];
                values[px * padded[1] + py] = psf[x * image[1] + y];
            }
        }
        objc2::rc::autoreleasepool(|_| {
            run(&self.psf, &self.queue, &self.psf_inputs, &self.psf_outputs)
        })?;
        self.input_values().fill(0.0);
        self.check_residency()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn clark_refresh_rejects_unadmitted_explicit_buffers_without_execution() {
        assert!(Workspace::new([4096, 4096], [6144, 6144], 1).is_err());
    }

    #[test]
    fn clark_refresh_odd_padding_matches_cpu_at_asymmetric_origin() {
        use casa_imaging_reconstruction::runtime_adapter::{CpuClarkRefresh, clark_padded_shape};
        let image = [24, 32];
        let center = [4, 10];
        let padded = clark_padded_shape(image, center).unwrap();
        assert_eq!(padded, [43, 53]);
        let mut psf = vec![0.0; image[0] * image[1]];
        psf[center[0] * image[1] + center[1]] = 1.0;
        psf[0] = 0.1;
        let last = psf.len() - 1;
        psf[last] = -0.05;
        let mut cpu = CpuClarkRefresh::new(&psf, image, center, 1).unwrap();
        let mut workspace =
            Workspace::new(image, padded, super::super::residency_bytes(image).unwrap()).unwrap();
        workspace.prepare_psf(&psf, center).unwrap();
        let owner = Mutex::new(Some(workspace));
        let mut metal = Session {
            state: owner.lock().unwrap(),
        };
        let mut expected = vec![0.0; psf.len()];
        let mut actual = expected.clone();
        for (index, flux) in [(0, 0.8), (31, -0.3), (psf.len() - 1, 0.4)] {
            cpu.add(index, flux);
            metal.add(index, flux);
        }
        cpu.refresh(&mut expected).unwrap();
        metal.refresh(&mut actual).unwrap();
        let error = actual
            .iter()
            .zip(&expected)
            .map(|(a, b)| (a - b).powi(2))
            .sum::<f64>();
        let power = expected.iter().map(|v| v * v).sum::<f64>();
        assert!((error / power).sqrt() <= 1e-3);
    }

    #[test]
    #[ignore = "full 4096-square convolution requires the bounded 8-GiB guard"]
    fn clark_refresh_full4096_matches_production_cpu() {
        use casa_imaging_reconstruction::runtime_adapter::{CpuClarkRefresh, clark_padded_shape};
        let image = [4096, 4096];
        let center = [2048, 2048];
        let mut psf = vec![0.0_f32; image[0] * image[1]];
        for x in 0..image[0] {
            for y in 0..image[1] {
                let dx = x as f64 - center[0] as f64;
                let dy = y as f64 - center[1] as f64;
                psf[x * image[1] + y] = ((-dx * dx / 32.0 - dy * dy / 72.0).exp()
                    + 0.07
                        * (0.17 * dx + 0.113 * dy).cos()
                        * (-(dx * dx + dy * dy) / 16200.0).exp())
                    as f32;
            }
        }
        let mut cpu = CpuClarkRefresh::new(&psf, image, center, 4).unwrap();
        let padded = clark_padded_shape(image, center).unwrap();
        let mut workspace =
            Workspace::new(image, padded, super::super::residency_bytes(image).unwrap()).unwrap();
        workspace.prepare_psf(&psf, center).unwrap();
        let owner = Mutex::new(Some(workspace));
        let mut metal = Session {
            state: owner.lock().unwrap(),
        };
        let mut expected = vec![0.0; psf.len()];
        let mut actual = expected.clone();
        for batch in 0..3 {
            for (index, flux) in [
                (0, 0.8),
                (4095, -0.3),
                (psf.len() - 1, 0.4),
                (2048 * 4096 + 2048, 1.0),
            ] {
                cpu.add(index, flux * (batch + 1) as f64);
                metal.add(index, flux * (batch + 1) as f64);
            }
            cpu.refresh(&mut expected).unwrap();
            metal.refresh(&mut actual).unwrap();
            let error = actual
                .iter()
                .zip(&expected)
                .map(|(a, b)| (a - b).powi(2))
                .sum::<f64>();
            let power = expected.iter().map(|v| v * v).sum::<f64>();
            let peak_error = actual
                .iter()
                .zip(&expected)
                .map(|(a, b)| (a - b).abs())
                .fold(0.0_f64, f64::max);
            let peak = expected.iter().map(|v| v.abs()).fold(0.0_f64, f64::max);
            eprintln!(
                "clark_metal_precision batch={batch} relative_l2={} max_error_over_peak={} device_allocated_bytes={}",
                (error / power).sqrt(),
                peak_error / peak,
                metal.state.as_ref().unwrap().device.currentAllocatedSize()
            );
            assert!((error / power).sqrt() <= 1e-3);
            assert!(peak_error / peak <= 1e-3);
        }
    }
}

pub(super) struct Session<'a> {
    pub(super) state: MutexGuard<'a, Option<Workspace>>,
}

impl ClarkRefresh for Session<'_> {
    fn add(&mut self, index: usize, flux: f64) {
        let workspace = self.state.as_mut().expect("borrowed workspace");
        let offset =
            (index / workspace.shape[1]) * workspace.padded[1] + index % workspace.shape[1];
        workspace.input_values()[offset] += flux as f32;
    }

    fn refresh(&mut self, residual: &mut [f64]) -> Result<(), MinorCycleError> {
        let workspace = self.state.as_mut().expect("borrowed workspace");
        if residual.len() != workspace.shape[0] * workspace.shape[1] {
            return Err(MinorCycleError::ModelShapeMismatch);
        }
        objc2::rc::autoreleasepool(|_| {
            run(
                &workspace.convolution,
                &workspace.queue,
                &workspace.inputs,
                &workspace.outputs,
            )
        })?;
        workspace.check_residency()?;
        let normalization = (workspace.padded[0] * workspace.padded[1]) as f64;
        let output = unsafe {
            std::slice::from_raw_parts(
                workspace.output.contents().as_ptr().cast::<f32>(),
                residual.len(),
            )
        };
        for (residual, value) in residual.iter_mut().zip(output) {
            *residual -= f64::from(*value) / normalization;
            if !residual.is_finite() {
                return Err(MinorCycleError::GeneratedNonfinite);
            }
        }
        workspace.input_values().fill(0.0);
        Ok(())
    }
}
