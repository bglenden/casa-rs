// SPDX-License-Identifier: LGPL-3.0-or-later

//! Bounded output windows: each member is generated and written a few
//! channels at a time, in channel order.

use std::ops::Range;

use casa_imaging_model::{ImageAxis, ProductAxes};

use crate::ProductsError;
use crate::writer::MemberImage;

/// Explicit admitted channel-window bound for generation and staging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProductStoragePlan {
    maximum_channels: usize,
    maximum_workers: usize,
}

impl ProductStoragePlan {
    /// Select a positive channel-window capacity; one channel is the atomic plane.
    ///
    /// # Errors
    ///
    /// A zero channel or worker count.
    pub fn new(maximum_channels: usize, maximum_workers: usize) -> Result<Self, ProductsError> {
        if maximum_channels == 0 || maximum_workers == 0 {
            return Err(ProductsError::InvalidWindow);
        }
        Ok(Self {
            maximum_channels,
            maximum_workers,
        })
    }

    /// Admitted CPU budget, shared by output-window lanes and inner FFTs.
    #[must_use]
    pub const fn maximum_workers(self) -> usize {
        self.maximum_workers
    }

    pub(crate) fn fft_threads(self, window_count: usize) -> usize {
        if cfg!(unix) && window_count == 1 {
            self.maximum_workers
        } else {
            1
        }
    }

    /// Return the selected maximum number of resident output channels.
    #[must_use]
    pub const fn maximum_channels(self) -> usize {
        self.maximum_channels
    }

    /// Bind the selected window to one member's exact storage axes.
    ///
    /// # Errors
    ///
    /// The member's value count overflows.
    pub fn layout(self, axes: &ProductAxes) -> Result<ProductWindowLayout, ProductsError> {
        let spectral_axis = axes
            .order()
            .positions()
            .iter()
            .position(|axis| *axis == ImageAxis::Spectral)
            .expect("an image domain's axes include its spectral axis");
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

/// One member's storage shape and the largest window written to it.
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
    ///
    /// # Errors
    ///
    /// The value count overflows.
    pub fn values(self) -> Result<usize, ProductsError> {
        values_for(self.shape)
    }

    pub(crate) fn window(self, channels: Range<usize>) -> Result<ProductWindow, ProductsError> {
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

/// Runs a bounded wave of independent window preparations.
///
/// The runtime's worker team implements it; the trait lets products run
/// their windows on that team without depending on the runtime, which
/// depends on products. Implementations join every preparation before
/// returning, including on failure; image writes stay with the caller.
pub trait ProductWindowExecutor {
    /// Fill each slot using its wave-local index, or return a joined failure.
    ///
    /// # Errors
    ///
    /// The first preparation that failed.
    fn prepare<T: Send>(
        &self,
        slots: &mut [Option<T>],
        operation: &(dyn Fn(usize) -> Result<T, ProductsError> + Sync),
    ) -> Result<(), ProductsError>;
}

impl ProductWindowExecutor for () {
    fn prepare<T: Send>(
        &self,
        slots: &mut [Option<T>],
        operation: &(dyn Fn(usize) -> Result<T, ProductsError> + Sync),
    ) -> Result<(), ProductsError> {
        for (index, slot) in slots.iter_mut().enumerate() {
            *slot = Some(operation(index)?);
        }
        Ok(())
    }
}

/// One generated window of a member and where it goes.
#[derive(Debug)]
pub struct ProductWindow {
    pub(crate) start: [usize; 4],
    pub(crate) shape: [usize; 4],
    pub(crate) payload: Vec<f32>,
    pub(crate) validity: Vec<bool>,
}

impl ProductWindow {
    pub(crate) fn into_parts(self) -> ([usize; 4], [usize; 4], Vec<f32>, Vec<bool>) {
        (self.start, self.shape, self.payload, self.validity)
    }
}

/// The channels of one member written so far: windows arrive in channel
/// order, each within the layout, and the member finishes only when every
/// channel is written.
#[derive(Debug)]
struct ChannelCoverage {
    layout: ProductWindowLayout,
    next_channel: usize,
}

impl ChannelCoverage {
    fn accept(&mut self, window: &ProductWindow) -> Result<(), ProductsError> {
        let layout = self.layout;
        let start = window.start[layout.spectral_axis];
        if start != self.next_channel {
            return Err(ProductsError::WindowCoverage {
                expected: self.next_channel,
                actual: start,
            });
        }
        let channels = window.shape[layout.spectral_axis];
        let end = start
            .checked_add(channels)
            .ok_or(ProductsError::InvalidWindow)?;
        if end > layout.shape[layout.spectral_axis]
            || channels > layout.maximum_channels
            || channels == 0
            || (0..4).any(|axis| {
                axis != layout.spectral_axis
                    && (window.start[axis] != 0 || window.shape[axis] != layout.shape[axis])
            })
            || window.payload.len() != values_for(window.shape)?
            || window.validity.len() != window.payload.len()
        {
            return Err(ProductsError::InvalidWindow);
        }
        if window.payload.iter().any(|value| !value.is_finite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        self.next_channel = end;
        Ok(())
    }

    fn complete(&self) -> Result<(), ProductsError> {
        let channels = self.layout.shape[self.layout.spectral_axis];
        if self.next_channel != channels {
            return Err(ProductsError::WindowCoverage {
                expected: channels,
                actual: self.next_channel,
            });
        }
        Ok(())
    }
}

/// Writes one member's windows to its image, in channel order.
pub(crate) struct ProductMemberWriter<'a> {
    coverage: ChannelCoverage,
    image: MemberImage<'a>,
}

impl<'a> ProductMemberWriter<'a> {
    pub(crate) fn new(layout: ProductWindowLayout, image: MemberImage<'a>) -> Self {
        Self {
            coverage: ChannelCoverage {
                layout,
                next_channel: 0,
            },
            image,
        }
    }

    pub(crate) fn write(&mut self, window: ProductWindow) -> Result<(), ProductsError> {
        self.coverage.accept(&window)?;
        self.image.write(window)
    }

    pub(crate) fn finish(self) -> Result<(), ProductsError> {
        self.coverage.complete()?;
        self.image.finish()
    }

    /// Write a whole member's values and validity, a window at a time.
    pub(crate) fn write_coupled(
        &mut self,
        payload: &[f32],
        validity: &[bool],
    ) -> Result<(), ProductsError> {
        let layout = self.coverage.layout;
        if payload.len() != layout.values()? || validity.len() != payload.len() {
            return Err(ProductsError::InvalidWindow);
        }
        let channels = layout.shape[layout.spectral_axis];
        for start in (0..channels).step_by(layout.maximum_channels) {
            let mut window =
                layout.window(start..(start + layout.maximum_channels).min(channels))?;
            for local in 0..window.payload.len() {
                let mut remainder = local;
                let mut coordinates = [0; 4];
                for axis in (0..4).rev() {
                    coordinates[axis] = remainder % window.shape[axis] + window.start[axis];
                    remainder /= window.shape[axis];
                }
                let index = coordinates
                    .into_iter()
                    .zip(layout.shape)
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

    #[test]
    fn inner_fft_workers_do_not_overlap_parallel_output_windows() {
        for workers in [1, 2, 4, 8, 16] {
            let plan = ProductStoragePlan::new(1, workers).unwrap();
            assert_eq!(plan.fft_threads(1), if cfg!(unix) { workers } else { 1 });
            for windows in [2, 4, 512] {
                assert_eq!(plan.fft_threads(windows), 1);
            }
        }
    }

    fn coverage(shape: [usize; 4], maximum_channels: usize) -> ChannelCoverage {
        ChannelCoverage {
            layout: ProductWindowLayout {
                shape,
                spectral_axis: 3,
                maximum_channels,
                maximum_values: shape[0] * shape[1] * shape[2] * maximum_channels,
            },
            next_channel: 0,
        }
    }

    #[test]
    fn nonfinite_values_are_refused_at_the_window_boundary() {
        for invalid in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let mut coverage = coverage([2, 1, 1, 1], 1);
            let mut window = coverage.layout.window(0..1).unwrap();
            window.payload[1] = invalid;
            assert!(matches!(
                coverage.accept(&window),
                Err(ProductsError::GeneratedNonfinite)
            ));
        }
    }

    #[test]
    fn incomplete_or_repeated_windows_cannot_finish_a_member() {
        let mut coverage = coverage([2, 2, 1, 3], 1);
        let first = coverage.layout.window(0..1).unwrap();
        coverage.accept(&first).unwrap();
        let repeated = coverage.layout.window(0..1).unwrap();
        assert!(matches!(
            coverage.accept(&repeated),
            Err(ProductsError::WindowCoverage {
                expected: 1,
                actual: 0
            })
        ));
        assert!(matches!(
            coverage.complete(),
            Err(ProductsError::WindowCoverage {
                expected: 3,
                actual: 1
            })
        ));
        for channel in 1..3 {
            let window = coverage.layout.window(channel..channel + 1).unwrap();
            coverage.accept(&window).unwrap();
        }
        assert!(coverage.complete().is_ok());
    }
}
