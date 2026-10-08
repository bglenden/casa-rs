// SPDX-License-Identifier: LGPL-3.0-or-later
//! The process's Metal device, command queue and compiled kernels, and the
//! shared-memory buffers the host and device both address.

use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::OnceLock;

use casa_imaging_operator::{DeviceFailure, OperatorError};
use objc2::rc::Retained;
use objc2::runtime::ProtocolObject;
use objc2_foundation::NSString;
use objc2_metal::{
    MTLBuffer, MTLCommandQueue, MTLCompileOptions, MTLComputePipelineState,
    MTLCreateSystemDefaultDevice, MTLDevice, MTLGPUFamily, MTLLibrary, MTLResourceOptions,
};

const SOURCE: &str = include_str!("kernels.metal");

/// Threads per threadgroup; each SIMD group of it serves one sample.
const THREADGROUP_THREADS: usize = 128;

/// One compiled kernel and how many samples a threadgroup serves.
pub(crate) struct Pipeline {
    pub state: Retained<ProtocolObject<dyn MTLComputePipelineState>>,
    pub threads: usize,
    pub samples_per_group: usize,
}

/// The device, its queue and the three kernels of the contract.
pub(crate) struct Device {
    pub device: Retained<ProtocolObject<dyn MTLDevice>>,
    pub queue: Retained<ProtocolObject<dyn MTLCommandQueue>>,
    pub spread: Pipeline,
    pub predict: Pipeline,
    pub residual: Pipeline,
}

// SAFETY: Metal documents `MTLDevice`, `MTLCommandQueue` and
// `MTLComputePipelineState` as thread-safe; command buffers made from the
// queue are created, encoded and committed by one thread at a time.
unsafe impl Send for Device {}
// SAFETY: as for `Send`.
unsafe impl Sync for Device {}

impl Device {
    /// The system default device with the kernels compiled, made once per
    /// process.
    pub(crate) fn shared() -> Result<&'static Self, OperatorError> {
        static SHARED: OnceLock<Option<Device>> = OnceLock::new();
        SHARED
            .get_or_init(|| objc2::rc::autoreleasepool(|_| Self::open()))
            .as_ref()
            .ok_or(OperatorError::Device(DeviceFailure::Unavailable))
    }

    /// `None` when there is no device or it lacks Metal 3 (atomic `float`
    /// adds on device memory).
    fn open() -> Option<Self> {
        let device = MTLCreateSystemDefaultDevice()?;
        if !device.supportsFamily(MTLGPUFamily::Metal3) {
            return None;
        }
        let queue = device.newCommandQueue()?;
        let options = MTLCompileOptions::new();
        #[allow(deprecated, reason = "the replacement `setMathMode` needs macOS 15")]
        options.setFastMathEnabled(false);
        let library = device
            .newLibraryWithSource_options_error(&NSString::from_str(SOURCE), Some(&options))
            .unwrap_or_else(|error| {
                panic!("the bundled Metal kernels must compile on a Metal 3 device: {error}")
            });
        let pipeline = |name: &str| {
            let function = library
                .newFunctionWithName(&NSString::from_str(name))
                .unwrap_or_else(|| panic!("the bundled Metal kernels define {name}"));
            let state = device
                .newComputePipelineStateWithFunction_error(&function)
                .unwrap_or_else(|error| panic!("Metal pipeline {name}: {error}"));
            let width = state.threadExecutionWidth();
            let threads =
                THREADGROUP_THREADS.min(state.maxTotalThreadsPerThreadgroup()) / width * width;
            assert!(
                threads >= width,
                "a threadgroup holds at least one SIMD group"
            );
            Pipeline {
                samples_per_group: threads / width,
                threads,
                state,
            }
        };
        Some(Self {
            spread: pipeline("spread"),
            predict: pipeline("predict"),
            residual: pipeline("residual"),
            device,
            queue,
        })
    }

    /// A zeroed buffer of at least `bytes` (never empty) in memory the host
    /// and the device share.
    pub(crate) fn buffer(&self, bytes: usize) -> Result<Buffer, OperatorError> {
        let length = bytes.max(16);
        let failure = OperatorError::Device(DeviceFailure::Allocation {
            bytes: length as u64,
        });
        if length > self.device.maxBufferLength() {
            return Err(failure);
        }
        let raw = self
            .device
            .newBufferWithLength_options(length, MTLResourceOptions::StorageModeShared)
            .ok_or(failure)?;
        // SAFETY: the buffer was just allocated with `length` bytes and no
        // command uses it yet.
        unsafe { raw.contents().cast::<u8>().as_ptr().write_bytes(0, length) };
        Ok(Buffer { raw, bytes: length })
    }
}

/// A shared-storage Metal buffer.
pub(crate) struct Buffer {
    pub raw: Retained<ProtocolObject<dyn MTLBuffer>>,
    pub bytes: usize,
}

// SAFETY: `MTLBuffer` objects are thread-safe; their contents are reached
// only through `&mut self` (writes) or `&self` (reads) while no command that
// writes them is pending, which every caller in this crate guarantees by
// completing its commands before handing the buffer back.
unsafe impl Send for Buffer {}
// SAFETY: as for `Send`.
unsafe impl Sync for Buffer {}

impl Buffer {
    /// The contents as `len` values of `T`.
    pub(crate) fn slice<T: Copy>(&self, len: usize) -> &[T] {
        assert!(
            len * size_of::<T>() <= self.bytes,
            "buffer read past its end"
        );
        // SAFETY: the buffer holds `bytes` initialised bytes, aligned for
        // any `T` of this crate (Metal aligns buffers to at least 16 bytes),
        // and no pending command writes them (type invariant).
        unsafe { std::slice::from_raw_parts(self.raw.contents().cast::<T>().as_ptr(), len) }
    }

    /// The contents as `len` values of `T`, mutably.
    pub(crate) fn slice_mut<T: Copy>(&mut self, len: usize) -> &mut [T] {
        assert!(
            len * size_of::<T>() <= self.bytes,
            "buffer write past its end"
        );
        // SAFETY: as for `slice`; `&mut self` is the only host access.
        unsafe { std::slice::from_raw_parts_mut(self.raw.contents().cast::<T>().as_ptr(), len) }
    }

    /// Start of the contents.
    pub(crate) fn contents(&self) -> NonNull<c_void> {
        self.raw.contents()
    }
}
