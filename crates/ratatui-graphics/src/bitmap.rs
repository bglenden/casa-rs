// SPDX-License-Identifier: LGPL-3.0-or-later
//! Plotters-specific bitmap helpers.

use image::{DynamicImage, RgbImage};
#[cfg(feature = "plotters")]
use plotters::prelude::BitMapBackend;
use thiserror::Error;

/// Errors returned by [`PlottersBitmap`].
#[derive(Debug, Error)]
pub enum PlottersBitmapError {
    /// The bitmap dimensions must be non-zero.
    #[error("plot bitmap dimensions must be non-zero, got {width}x{height}")]
    InvalidDimensions { width: u32, height: u32 },
    /// The required RGB buffer would overflow addressable memory.
    #[error("plot bitmap buffer size overflow for dimensions {width}x{height}")]
    BufferSizeOverflow { width: u32, height: u32 },
    /// Plotters returned an unexpected buffer size.
    #[error("plotters returned an unexpected RGB buffer size")]
    InvalidRgbBuffer,
}

/// Owns an RGB bitmap buffer that can be handed to a `plotters` bitmap backend.
#[derive(Debug)]
pub struct PlottersBitmap {
    width: u32,
    height: u32,
    buffer: Vec<u8>,
}

impl PlottersBitmap {
    /// Create a new RGB bitmap for plotters rendering.
    pub fn new(width: u32, height: u32) -> Result<Self, PlottersBitmapError> {
        if width == 0 || height == 0 {
            return Err(PlottersBitmapError::InvalidDimensions { width, height });
        }

        let len = u64::from(width)
            .checked_mul(u64::from(height))
            .and_then(|pixels| pixels.checked_mul(3))
            .ok_or(PlottersBitmapError::BufferSizeOverflow { width, height })?;
        let len = usize::try_from(len)
            .map_err(|_| PlottersBitmapError::BufferSizeOverflow { width, height })?;

        Ok(Self {
            width,
            height,
            buffer: vec![0_u8; len],
        })
    }

    /// Return a `plotters` bitmap backend that writes into this buffer.
    pub fn backend(&mut self) -> BitMapBackend<'_> {
        BitMapBackend::with_buffer(&mut self.buffer, (self.width, self.height))
    }

    /// Consume the bitmap and return it as a dynamic RGB image.
    pub fn into_dynamic_image(self) -> Result<DynamicImage, PlottersBitmapError> {
        Ok(DynamicImage::ImageRgb8(self.into_rgb_image()?))
    }

    /// Consume the bitmap and return it as an `image` crate RGB image.
    pub fn into_rgb_image(self) -> Result<RgbImage, PlottersBitmapError> {
        RgbImage::from_raw(self.width, self.height, self.buffer)
            .ok_or(PlottersBitmapError::InvalidRgbBuffer)
    }
}

#[cfg(test)]
mod tests {
    use super::{PlottersBitmap, PlottersBitmapError};

    #[test]
    fn rejects_zero_dimensions() {
        let err = PlottersBitmap::new(0, 10).unwrap_err();
        assert!(matches!(
            err,
            PlottersBitmapError::InvalidDimensions {
                width: 0,
                height: 10
            }
        ));
    }

    #[test]
    fn allocates_checked_buffer() {
        let bitmap = PlottersBitmap::new(4, 3).unwrap();
        let image = bitmap.into_rgb_image().unwrap();
        assert_eq!(image.width(), 4);
        assert_eq!(image.height(), 3);
    }
}
