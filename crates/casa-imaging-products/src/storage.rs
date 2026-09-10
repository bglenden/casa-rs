// SPDX-License-Identifier: LGPL-3.0-or-later

//! Product-owned shape, coverage, and canonical hashing over fallible physical windows.

use std::{fmt::Debug, ops::Range, sync::Arc};

use casa_imaging_model::{ImageAxis, ProductAxes};

use crate::{ProductsError, digest::Encoder};

/// Explicit admitted channel-window bound for generation, sealing, and staging.
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

/// Exact logical shape and active-window limits supplied to a physical backing owner.
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

/// Physical allocation capability supplied only after runtime admission.
pub trait ProductStorageFactory: Debug + Send + Sync {
    /// Create one exact member backing with the supplied immutable window contract.
    fn create(
        &self,
        layout: ProductWindowLayout,
    ) -> Result<Box<dyn ProductArrayStorage>, ProductsError>;
}

/// Fallible rectangular access to one product's numeric and stored-mask arrays.
/// Implementations own I/O, caches, descriptors, and private storage cleanup.
pub trait ProductArrayStorage: Debug + Send + Sync {
    /// Return the logical storage shape established at creation.
    fn shape(&self) -> [usize; 4];
    /// Read binary32 values in canonical C storage order into the exact-sized slice.
    fn read_payload(
        &self,
        start: [usize; 4],
        shape: [usize; 4],
        values: &mut [f32],
    ) -> Result<(), ProductsError>;
    /// Read byte-addressed validity in the same storage order.
    fn read_validity(
        &self,
        start: [usize; 4],
        shape: [usize; 4],
        values: &mut [bool],
    ) -> Result<(), ProductsError>;
    /// Write one bounded rectangular product window before scientific sealing.
    fn write(&mut self, window: &ProductWindow) -> Result<(), ProductsError>;
    /// Make every prior window readable or fail before minting completion.
    fn flush(&mut self) -> Result<(), ProductsError>;
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

pub(crate) struct ProductMemberWriter {
    layout: ProductWindowLayout,
    storage: Box<dyn ProductArrayStorage>,
    next_channel: usize,
}

impl ProductMemberWriter {
    pub(crate) fn new(
        layout: ProductWindowLayout,
        factory: &dyn ProductStorageFactory,
    ) -> Result<Self, ProductsError> {
        let storage = factory.create(layout)?;
        if storage.shape() != layout.shape {
            return Err(ProductsError::InvalidWindow);
        }
        Ok(Self {
            layout,
            storage,
            next_channel: 0,
        })
    }

    pub(crate) fn window(&self, channels: Range<usize>) -> Result<ProductWindow, ProductsError> {
        self.layout.window(channels)
    }

    pub(crate) fn write(&mut self, window: &ProductWindow) -> Result<(), ProductsError> {
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
        if window.payload.iter().any(|value| value.is_infinite()) {
            return Err(ProductsError::GeneratedNonfinite);
        }
        self.storage.write(window)?;
        self.next_channel = end;
        Ok(())
    }

    pub(crate) fn finish(mut self) -> Result<Arc<ProductMemberBacking>, ProductsError> {
        if self.next_channel != self.layout.shape[self.layout.spectral_axis] {
            return Err(ProductsError::WindowCoverage {
                expected: self.layout.shape[self.layout.spectral_axis],
                actual: self.next_channel,
            });
        }
        self.storage.flush()?;
        Ok(Arc::new(ProductMemberBacking {
            layout: self.layout,
            storage: self.storage,
        }))
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
            self.write(&window)?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) struct ProductMemberBacking {
    pub(crate) layout: ProductWindowLayout,
    storage: Box<dyn ProductArrayStorage>,
}

impl ProductMemberBacking {
    pub(crate) fn read_window(
        &self,
        channels: Range<usize>,
    ) -> Result<ProductWindow, ProductsError> {
        let mut window = self.layout.window(channels)?;
        self.storage
            .read_payload(window.start, window.shape, &mut window.payload)?;
        self.storage
            .read_validity(window.start, window.shape, &mut window.validity)?;
        Ok(window)
    }

    pub(crate) fn content_digest(&self) -> Result<[u8; 32], ProductsError> {
        let mut payload = Encoder::new(b"casa-rs-product-plane-content", 1);
        payload.usize(self.layout.values()?);
        let mut values = vec![0.0; self.layout.maximum_values];
        for (start, shape) in CanonicalWindows::new(self.layout)? {
            let count = values_for(shape)?;
            self.storage
                .read_payload(start, shape, &mut values[..count])?;
            payload.f32_bits_slice(&values[..count]);
        }
        drop(values);
        let mut member = Encoder::new(b"casa-rs-product-member-content", 1);
        member.identity(payload.finish());
        member.usize(self.layout.values()?);
        let mut validity = vec![false; self.layout.maximum_values];
        for (start, shape) in CanonicalWindows::new(self.layout)? {
            let count = values_for(shape)?;
            self.storage
                .read_validity(start, shape, &mut validity[..count])?;
            member.validity_slice(&validity[..count]);
        }
        Ok(member.finish())
    }
}

/// Bounded rectangles whose concatenation is exactly the canonical scalar order.
struct CanonicalWindows {
    full_shape: [usize; 4],
    start: [usize; 4],
    split_axis: usize,
    split_extent: usize,
    complete: bool,
}

impl CanonicalWindows {
    fn new(layout: ProductWindowLayout) -> Result<Self, ProductsError> {
        let mut split_axis = 3;
        let mut trailing = 1usize;
        while split_axis > 0 {
            let next = trailing
                .checked_mul(layout.shape[split_axis])
                .ok_or(ProductsError::InvalidWindow)?;
            if next > layout.maximum_values {
                break;
            }
            trailing = next;
            split_axis -= 1;
        }
        let split_extent = (layout.maximum_values / trailing).min(layout.shape[split_axis]);
        if split_extent == 0 {
            return Err(ProductsError::InvalidWindow);
        }
        Ok(Self {
            full_shape: layout.shape,
            start: [0; 4],
            split_axis,
            split_extent,
            complete: false,
        })
    }
}

impl Iterator for CanonicalWindows {
    type Item = ([usize; 4], [usize; 4]);
    fn next(&mut self) -> Option<Self::Item> {
        if self.complete {
            return None;
        }
        let start = self.start;
        let mut shape = self.full_shape;
        shape[..self.split_axis].fill(1);
        shape[self.split_axis] = self
            .split_extent
            .min(self.full_shape[self.split_axis] - start[self.split_axis]);
        self.start[self.split_axis] += shape[self.split_axis];
        for axis in (0..=self.split_axis).rev() {
            if self.start[axis] < self.full_shape[axis] {
                break;
            }
            self.start[axis] = 0;
            if axis == 0 {
                self.complete = true;
            } else {
                self.start[axis - 1] += 1;
            }
        }
        Some((start, shape))
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

    #[derive(Debug)]
    struct FixtureStorage {
        shape: [usize; 4],
        fail_read: bool,
    }
    #[derive(Debug)]
    struct FixtureFactory;
    impl ProductStorageFactory for FixtureFactory {
        fn create(
            &self,
            layout: ProductWindowLayout,
        ) -> Result<Box<dyn ProductArrayStorage>, ProductsError> {
            Ok(Box::new(FixtureStorage {
                shape: layout.shape(),
                fail_read: false,
            }))
        }
    }
    impl ProductArrayStorage for FixtureStorage {
        fn shape(&self) -> [usize; 4] {
            self.shape
        }
        fn read_payload(
            &self,
            start: [usize; 4],
            shape: [usize; 4],
            values: &mut [f32],
        ) -> Result<(), ProductsError> {
            if self.fail_read {
                return Err(ProductsError::Storage("injected read failure".into()));
            }
            for (local, value) in values.iter_mut().enumerate() {
                *value = index(self.shape, start, shape, local) as f32;
            }
            Ok(())
        }
        fn read_validity(
            &self,
            start: [usize; 4],
            shape: [usize; 4],
            values: &mut [bool],
        ) -> Result<(), ProductsError> {
            for (local, value) in values.iter_mut().enumerate() {
                *value = index(self.shape, start, shape, local) % 3 != 0;
            }
            Ok(())
        }
        fn write(&mut self, _: &ProductWindow) -> Result<(), ProductsError> {
            Ok(())
        }
        fn flush(&mut self) -> Result<(), ProductsError> {
            Ok(())
        }
    }
    fn index(full: [usize; 4], start: [usize; 4], shape: [usize; 4], mut local: usize) -> usize {
        let mut coordinates = start;
        for axis in (0..4).rev() {
            coordinates[axis] += local % shape[axis];
            local /= shape[axis];
        }
        coordinates
            .into_iter()
            .zip(full)
            .fold(0, |value, (coordinate, extent)| value * extent + coordinate)
    }
    #[test]
    fn canonical_hash_is_independent_of_spectral_position_and_window_partition() {
        let shape = [2, 3, 4, 5];
        let mut reference = None;
        for spectral_axis in 0..4 {
            for maximum_channels in 1..=shape[spectral_axis] {
                let maximum_values =
                    shape.iter().product::<usize>() / shape[spectral_axis] * maximum_channels;
                let layout = ProductWindowLayout {
                    shape,
                    spectral_axis,
                    maximum_channels,
                    maximum_values,
                };
                let backing = ProductMemberBacking {
                    layout,
                    storage: Box::new(FixtureStorage {
                        shape,
                        fail_read: false,
                    }),
                };
                let digest = backing.content_digest().unwrap();
                match reference {
                    Some(expected) => assert_eq!(digest, expected),
                    None => reference = Some(digest),
                }
            }
        }
    }
    #[test]
    fn hashing_propagates_physical_read_failure() {
        let layout = ProductWindowLayout {
            shape: [1; 4],
            spectral_axis: 3,
            maximum_channels: 1,
            maximum_values: 1,
        };
        let backing = ProductMemberBacking {
            layout,
            storage: Box::new(FixtureStorage {
                shape: [1; 4],
                fail_read: true,
            }),
        };
        assert!(matches!(
            backing.content_digest(),
            Err(ProductsError::Storage(_))
        ));
    }

    #[test]
    fn incomplete_or_repeated_windows_cannot_finish_a_member() {
        let layout = ProductWindowLayout {
            shape: [2, 2, 1, 3],
            spectral_axis: 3,
            maximum_channels: 1,
            maximum_values: 4,
        };
        let mut writer = ProductMemberWriter::new(layout, &FixtureFactory).unwrap();
        let first = writer.window(0..1).unwrap();
        writer.write(&first).unwrap();
        assert!(matches!(
            writer.write(&first),
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
        let mut writer = ProductMemberWriter::new(layout, &FixtureFactory).unwrap();
        for channel in 0..3 {
            writer
                .write(&writer.window(channel..channel + 1).unwrap())
                .unwrap();
        }
        assert!(writer.finish().is_ok());
    }
}
