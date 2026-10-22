// SPDX-License-Identifier: LGPL-3.0-or-later
//! The backend on hosts without Metal: it cannot be constructed.

use std::convert::Infallible;
use std::marker::PhantomData;

use casa_imaging_operator::{
    AccumulatorLayout, ConvolutionFunctionSet, DeviceFailure, DeviceNativeCells, GridAccumulator,
    GridBackend, OperatorError, SampleBlock, Work,
};

/// The Metal gridding backend; this host has no Metal, so
/// [`MetalBackend::new`] always fails with [`DeviceFailure::Unavailable`].
pub struct MetalBackend<'cf> {
    never: Infallible,
    kernels: PhantomData<&'cf ()>,
}

impl<'cf> MetalBackend<'cf> {
    /// Always [`DeviceFailure::Unavailable`] on this host.
    pub fn new(
        _kernels: &'cf dyn ConvolutionFunctionSet,
        _stage_timing: bool,
    ) -> Result<Self, OperatorError> {
        Err(OperatorError::Device(DeviceFailure::Unavailable))
    }

    /// Always [`DeviceFailure::Unavailable`] on this host.
    pub fn accumulator(_layout: AccumulatorLayout) -> Result<GridAccumulator, OperatorError> {
        Err(OperatorError::Device(DeviceFailure::Unavailable))
    }

    /// Always [`DeviceFailure::Unavailable`] on this host.
    pub fn native_cells(_len: usize) -> Result<DeviceNativeCells, OperatorError> {
        Err(OperatorError::Device(DeviceFailure::Unavailable))
    }

    /// Unreachable: no backend exists on this host.
    #[must_use]
    pub fn with_deferred_grid_completion(self) -> Self {
        match self.never {}
    }

    /// Unreachable: no backend exists on this host.
    #[must_use]
    pub fn shared_samples(&self) -> u64 {
        match self.never {}
    }

    /// Unreachable: no backend exists on this host.
    #[must_use]
    pub fn kernel_bytes_held(&self) -> u64 {
        match self.never {}
    }

    /// Unreachable: no backend exists on this host.
    ///
    /// # Errors
    ///
    /// None; no backend exists on this host.
    pub fn complete(&mut self) -> Result<(), OperatorError> {
        match self.never {}
    }

    /// Unreachable: no backend exists on this host.
    ///
    /// # Errors
    ///
    /// None; no backend exists on this host.
    pub fn apply_parts(
        &mut self,
        _parts: &[SampleBlock<'_>],
        _cf: &dyn ConvolutionFunctionSet,
        _work: Work<'_>,
    ) -> Result<(), OperatorError> {
        match self.never {}
    }
}

impl GridBackend for MetalBackend<'_> {
    fn apply(
        &mut self,
        _block: &SampleBlock<'_>,
        _cf: &dyn ConvolutionFunctionSet,
        _work: Work<'_>,
    ) -> Result<(), OperatorError> {
        let _ = self.kernels;
        match self.never {}
    }
}
