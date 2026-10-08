// SPDX-License-Identifier: LGPL-3.0-or-later
//! The backend on hosts without Metal: it cannot be constructed.

use std::convert::Infallible;
use std::marker::PhantomData;

use casa_imaging_operator::{
    AccumulatorLayout, ConvolutionFunctionSet, DeviceFailure, GridAccumulator, GridBackend,
    OperatorError, SampleBlock, Work,
};

/// The Metal gridding backend; this host has no Metal, so
/// [`MetalBackend::new`] always fails with [`DeviceFailure::Unavailable`].
pub struct MetalBackend<'cf> {
    never: Infallible,
    kernels: PhantomData<&'cf ()>,
}

impl<'cf> MetalBackend<'cf> {
    /// Always [`DeviceFailure::Unavailable`] on this host.
    pub fn new(_kernels: &'cf dyn ConvolutionFunctionSet) -> Result<Self, OperatorError> {
        Err(OperatorError::Device(DeviceFailure::Unavailable))
    }

    /// Always [`DeviceFailure::Unavailable`] on this host.
    pub fn accumulator(_layout: AccumulatorLayout) -> Result<GridAccumulator, OperatorError> {
        Err(OperatorError::Device(DeviceFailure::Unavailable))
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
