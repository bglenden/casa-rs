// SPDX-License-Identifier: LGPL-3.0-or-later
//! The backend on hosts without Metal: it cannot be constructed.

use std::convert::Infallible;
use std::marker::PhantomData;

use casa_imaging_operator::{
    AccumulatorLayout, ConvolutionFunctionSet, DeviceFailure, GridAccumulator, GridBackend,
    OperatorError, PreparedModelGrids, SampleBlock, Work,
};
use num_complex::Complex32;

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

    /// Unreachable: no backend exists on this host.
    #[must_use]
    pub fn with_deferred_grid_completion(self) -> Self {
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
    pub fn submit_prediction(
        &mut self,
        _block: &SampleBlock<'_>,
        _model: &PreparedModelGrids,
        _out: Vec<Complex32>,
    ) -> Result<(), OperatorError> {
        match self.never {}
    }

    /// Unreachable: no backend exists on this host.
    ///
    /// # Errors
    ///
    /// None; no backend exists on this host.
    pub fn collect_prediction(
        &mut self,
        _block: &SampleBlock<'_>,
    ) -> Result<Vec<Complex32>, OperatorError> {
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
