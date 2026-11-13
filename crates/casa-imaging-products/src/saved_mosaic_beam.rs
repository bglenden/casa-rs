// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_images::{ImageError, PagedImage};

use crate::ProductsError;

/// The primary beam and default mask left by an earlier mosaic run.
/// Product workers open and read one channel and Stokes plane at a time.
#[derive(Clone, Debug)]
pub struct SavedMosaicBeam {
    path: PathBuf,
    shape: [usize; 4],
    reading_bytes: u64,
}

impl SavedMosaicBeam {
    /// Tile cache per reader, for the image and its default mask.
    pub const CACHE_BYTES: usize = 64 * 1024;

    /// Record the dimensions and per-plane read bound from the saved image.
    ///
    /// # Errors
    /// The image does not have four axes or its mask cannot be opened.
    pub fn new(path: &Path, image: &PagedImage<f32>) -> Result<Self, ImageError> {
        let shape: [usize; 4] = image.shape().try_into().map_err(|_| {
            ImageError::InvalidMetadata("the saved mosaic PB must have four axes".into())
        })?;
        let cells = (shape[0] * shape[1]) as u64;
        let pixel_backing = if image.tiled_io_stats().is_some() {
            let tile = 4 * image.tile_shape().iter().product::<usize>() as u64;
            Self::CACHE_BYTES as u64 + 2 * tile
        } else {
            // A legacy untiled image holds a full table cell while slicing.
            12 * shape.iter().product::<usize>() as u64
        };
        let mask_backing = image.mask_slice_working_bytes()?;
        Ok(Self {
            path: path.to_path_buf(),
            shape,
            reading_bytes: 8 * cells + pixel_backing + mask_backing,
        })
    }

    /// CASA axis order, `[x, y, Stokes, channel]`.
    #[must_use]
    pub const fn shape(&self) -> [usize; 4] {
        self.shape
    }

    /// Scratch per lane for the pixel or mask read, including the x-major
    /// copy and both backing stores' bounds. The value copy is the larger.
    #[must_use]
    pub const fn reading_bytes(&self) -> u64 {
        self.reading_bytes
    }

    /// Read a saved PB plane in x-major order.
    ///
    /// # Errors
    /// The image cannot be opened, sliced, or contains invalid beam values.
    pub fn read_values(
        &self,
        channel: usize,
        polarization: usize,
    ) -> Result<Vec<f32>, ProductsError> {
        let [nx, ny, _, _] = self.shape;
        let values =
            self.read(|image| image.get_slice(&[0, 0, polarization, channel], &[nx, ny, 1, 1]))?;
        if values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(ProductsError::InvalidSavedMosaicBeam);
        }
        Ok(values.iter().copied().collect())
    }

    /// Read the default pixel mask of the same saved plane in x-major order.
    ///
    /// # Errors
    /// The image or mask cannot be opened or sliced.
    pub fn read_mask(
        &self,
        channel: usize,
        polarization: usize,
    ) -> Result<Vec<bool>, ProductsError> {
        let [nx, ny, _, _] = self.shape;
        let mask = self.read(|image| {
            image.get_mask_slice(&[0, 0, polarization, channel], &[nx, ny, 1, 1], &[1; 4])
        })?;
        Ok(mask.map_or_else(
            || vec![true; nx * ny],
            |mask| mask.iter().copied().collect(),
        ))
    }

    fn read<T>(
        &self,
        slice: impl FnOnce(PagedImage<f32>) -> Result<T, ImageError>,
    ) -> Result<T, ProductsError> {
        PagedImage::open_with_cache(&self.path, Self::CACHE_BYTES)
            .and_then(slice)
            .map_err(|source| ProductsError::SavedMosaicBeam {
                path: self.path.clone(),
                source,
            })
    }
}
