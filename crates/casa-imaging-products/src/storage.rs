// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-owned shape, coverage, and bounded write-only physical windows.

use std::ops::Range;

use casa_imaging_model::{ImageAxis, ProductAxes};

use crate::{PlannedMember, ProductsError, RestoringBeam};

/// Explicit admitted channel-window bound for generation and staging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStoragePlan {
    maximum_channels: usize,
}

impl ProductStoragePlan {
    /// Select a positive channel-window capacity; one channel is the atomic plane.
    pub fn new(maximum_channels: usize) -> Result<Self, ProductsError> {
        if maximum_channels == 0 {
            return Err(ProductsError::InvalidWindow);
        }
        Ok(Self { maximum_channels })
    }

    /// Return the selected maximum number of resident output channels.
    #[must_use]
    pub const fn maximum_channels(self) -> usize {
        self.maximum_channels
    }

    /// Bind the selected window to one member's exact storage axes.
    pub fn layout(self, axes: &ProductAxes) -> Result<ProductWindowLayout, ProductsError> {
        let spectral_axis = axes
            .order()
            .positions()
            .iter()
            .position(|axis| *axis == ImageAxis::Spectral)
            .ok_or(ProductsError::SourceLineageMismatch)?;
        let shape = axes.shape();
        let maximum_channels = self.maximum_channels.min(shape[spectral_axis]);
        let mut window_shape = shape;
        window_shape[spectral_axis] = maximum_channels;
        let maximum_values = values_for(window_shape)?;
        values_for(shape)?;
        Ok(ProductWindowLayout {
            shape,
            spectral_axis,
            maximum_channels,
            maximum_values,
        })
    }
}

/// Exact logical shape and active-window limits supplied to a physical output owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductWindowLayout {
    shape: [usize; 4],
    spectral_axis: usize,
    maximum_channels: usize,
    maximum_values: usize,
}

impl ProductWindowLayout {
    /// Return the full logical storage shape, not a resident allocation request.
    #[must_use]
    pub const fn shape(self) -> [usize; 4] {
        self.shape
    }
    /// Return the spectral-axis position in storage order.
    #[must_use]
    pub const fn spectral_axis(self) -> usize {
        self.spectral_axis
    }
    /// Return the admitted channel capacity, clipped to this member's axis.
    #[must_use]
    pub const fn maximum_channels(self) -> usize {
        self.maximum_channels
    }
    /// Return the largest active rectangular channel-window shape.
    #[must_use]
    pub fn window_shape(self) -> [usize; 4] {
        let mut shape = self.shape;
        shape[self.spectral_axis] = self.maximum_channels;
        shape
    }
    /// Return exact maximum values in one active window.
    #[must_use]
    pub const fn maximum_values(self) -> usize {
        self.maximum_values
    }
    /// Return the complete logical value count with checked arithmetic.
    pub fn values(self) -> Result<usize, ProductsError> {
        values_for(self.shape)
    }

    fn window(self, channels: Range<usize>) -> Result<ProductWindow, ProductsError> {
        if channels.start >= channels.end
            || channels.end > self.shape[self.spectral_axis]
            || channels.len() > self.maximum_channels
        {
            return Err(ProductsError::InvalidWindow);
        }
        let mut start = [0; 4];
        start[self.spectral_axis] = channels.start;
        let mut shape = self.shape;
        shape[self.spectral_axis] = channels.len();
        let values = values_for(shape)?;
        Ok(ProductWindow {
            start,
            shape,
            payload: vec![0.0; values],
            validity: vec![true; values],
        })
    }
}

/// Write-only output owner for one product generation.
pub trait ProductOutput {
    /// Begin one member in the exact planned order and storage shape.
    fn begin_member<'a>(
        &'a self,
        member: &PlannedMember,
        layout: ProductWindowLayout,
        beams: &[Option<RestoringBeam>],
    ) -> Result<Box<dyn ProductWriter + 'a>, ProductsError>;
}

/// Bounded write-only destination for one product member.
pub trait ProductWriter {
    /// Transfer one owned window into the destination.
    fn write(&mut self, window: ProductWindow) -> Result<(), ProductsError>;
    /// Flush the destination and complete the member.
    fn finish(self: Box<Self>) -> Result<(), ProductsError>;
}

/// One owned, explicitly bounded product window and its exact storage coordinates.
#[derive(Debug)]
pub struct ProductWindow {
    pub(crate) start: [usize; 4],
    pub(crate) shape: [usize; 4],
    pub(crate) payload: Vec<f32>,
    pub(crate) validity: Vec<bool>,
}

impl ProductWindow {
    /// Return the window's start in full-member storage coordinates.
    #[must_use]
    pub const fn start(&self) -> [usize; 4] {
        self.start
    }
    /// Return the local window shape in storage-axis order.
    #[must_use]
    pub const fn shape(&self) -> [usize; 4] {
        self.shape
    }
    /// Borrow binary32 values in local storage order.
    #[must_use]
    pub fn payload(&self) -> &[f32] {
        &self.payload
    }
    /// Borrow stored-mask support independently of numerical blanking.
    #[must_use]
    pub fn validity(&self) -> &[bool] {
        &self.validity
    }
    /// Transfer arrays into a storage sink without duplicating the window.
    #[must_use]
    pub fn into_parts(self) -> ([usize; 4], [usize; 4], Vec<f32>, Vec<bool>) {
        (self.start, self.shape, self.payload, self.validity)
    }
}

pub(crate) struct ProductMemberWriter<'a> {
    layout: ProductWindowLayout,
    writer: Box<dyn ProductWriter + 'a>,
    next_channel: usize,
}

impl<'a> ProductMemberWriter<'a> {
    pub(crate) fn new(
        layout: ProductWindowLayout,
        writer: Box<dyn ProductWriter + 'a>,
    ) -> Result<Self, ProductsError> {
        Ok(Self {
            layout,
            writer,
            next_channel: 0,
        })
    }

    pub(crate) fn window(&self, channels: Range<usize>) -> Result<ProductWindow, ProductsError> {
        self.layout.window(channels)
    }

    pub(crate) fn write(&mut self, window: ProductWindow) -> Result<(), ProductsError> {
        let start = window.start[self.layout.spectral_axis];
        if start != self.next_channel {
            return Err(ProductsError::WindowCoverage {
                expected: self.next_channel,
                actual: start,
            });
        }
        let end = start
            .checked_add(window.shape[self.layout.spectral_axis])
            .ok_or(ProductsError::InvalidWindow)?;
        if end > self.layout.shape[self.layout.spectral_axis]
            || window.shape[self.layout.spectral_axis] > self.layout.maximum_channels
            || window.shape[self.layout.spectral_axis] == 0
            || (0..4).any(|axis| {
                axis != self.layout.spectral_axis
                    && (window.start[axis] != 0 || window.shape[axis] != self.layout.shape[axis])
            })
            || window.payload.len() != values_for(window.shape)?
            || window.validity.len() != window.payload.len()
        {
            return Err(ProductsError::InvalidWindow);
        }
        if window.payload.iter().any(|value| !value.is_finite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        self.writer.write(window)?;
        self.next_channel = end;
        Ok(())
    }

    pub(crate) fn finish(self) -> Result<(), ProductsError> {
        if self.next_channel != self.layout.shape[self.layout.spectral_axis] {
            return Err(ProductsError::WindowCoverage {
                expected: self.layout.shape[self.layout.spectral_axis],
                actual: self.next_channel,
            });
        }
        self.writer.finish()
    }

    pub(crate) fn write_coupled(
        &mut self,
        payload: &[f32],
        validity: &[bool],
    ) -> Result<(), ProductsError> {
        if payload.len() != self.layout.values()? || validity.len() != payload.len() {
            return Err(ProductsError::InvalidWindow);
        }
        let channels = self.layout.shape[self.layout.spectral_axis];
        for start in (0..channels).step_by(self.layout.maximum_channels) {
            let mut window =
                self.window(start..(start + self.layout.maximum_channels).min(channels))?;
            for local in 0..window.payload.len() {
                let mut remainder = local;
                let mut coordinates = [0; 4];
                for axis in (0..4).rev() {
                    coordinates[axis] = remainder % window.shape[axis] + window.start[axis];
                    remainder /= window.shape[axis];
                }
                let index = coordinates
                    .into_iter()
                    .zip(self.layout.shape)
                    .fold(0, |index, (coordinate, extent)| index * extent + coordinate);
                window.payload[local] = payload[index];
                window.validity[local] = validity[index];
            }
            self.write(window)?;
        }
        Ok(())
    }
}

fn values_for(shape: [usize; 4]) -> Result<usize, ProductsError> {
    shape.into_iter().try_fold(1usize, |values, extent| {
        values
            .checked_mul(extent)
            .filter(|values| *values > 0)
            .ok_or(ProductsError::ResourceDemandOverflow(
                "product window shape",
            ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixtureWriter;

    impl ProductWriter for FixtureWriter {
        fn write(&mut self, _: ProductWindow) -> Result<(), ProductsError> {
            Ok(())
        }

        fn finish(self: Box<Self>) -> Result<(), ProductsError> {
            Ok(())
        }
    }

    #[test]
    fn coupled_generation_rejects_nonfinite_values_at_the_window_boundary() {
        let layout = ProductWindowLayout {
            shape: [2, 1, 1, 1],
            spectral_axis: 3,
            maximum_channels: 1,
            maximum_values: 2,
        };
        for invalid in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mut writer = ProductMemberWriter::new(layout, Box::new(FixtureWriter)).unwrap();
            assert!(matches!(
                writer.write_coupled(&[1.0, invalid], &[true, true]),
                Err(ProductsError::GeneratedNonfinite)
            ));
        }
    }

    #[test]
    fn incomplete_or_repeated_windows_cannot_finish_a_member() {
        let layout = ProductWindowLayout {
            shape: [2, 2, 1, 3],
            spectral_axis: 3,
            maximum_channels: 1,
            maximum_values: 4,
        };
        let mut writer = ProductMemberWriter::new(layout, Box::new(FixtureWriter)).unwrap();
        let first = writer.window(0..1).unwrap();
        writer.write(first).unwrap();
        let repeated = writer.window(0..1).unwrap();
        assert!(matches!(
            writer.write(repeated),
            Err(ProductsError::WindowCoverage {
                expected: 1,
                actual: 0
            })
        ));
        assert!(matches!(
            writer.finish(),
            Err(ProductsError::WindowCoverage {
                expected: 3,
                actual: 1
            })
        ));
        let mut writer = ProductMemberWriter::new(layout, Box::new(FixtureWriter)).unwrap();
        for channel in 0..3 {
            let window = writer.window(channel..channel + 1).unwrap();
            writer.write(window).unwrap();
        }
        assert!(writer.finish().is_ok());
    }
}
