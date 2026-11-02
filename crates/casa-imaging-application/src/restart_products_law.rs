// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting-law access to the reader `calcpsf=False` and `calcres=False`
//! runs read an earlier run's PSF, sums of weights and residual with.

use std::ops::Range;
use std::path::Path;

use casa_imaging_deconvolution::PassImages;

use crate::restart::{ExistingProducts, Family};

/// The products an earlier run left at a prefix, their headers checked.
pub struct Input(ExistingProducts);

impl Input {
    /// Open the products at `prefix` of a run of `terms` Taylor terms (one
    /// for a single image) of `shape`, `polarizations` and `channels`; the
    /// residual too when `residual`.
    ///
    /// # Errors
    /// An image cannot be opened, or its sums of weights are not one per
    /// Stokes plane and channel.
    pub fn open(
        prefix: &Path,
        terms: usize,
        (shape, polarizations, channels): ([usize; 2], usize, usize),
        residual: bool,
    ) -> Result<Self, crate::PrepareError> {
        let family = if terms > 1 {
            Family::Taylor { terms }
        } else {
            Family::Single
        };
        ExistingProducts::new(
            prefix,
            family,
            (shape, polarizations, channels),
            (true, residual),
        )
        .map(Self)
    }

    /// Bytes admitted for reading `channels` channels, besides the images.
    #[must_use]
    pub fn reading_bytes(&self, channels: usize) -> u64 {
        self.0.reading_bytes(channels)
    }

    /// Bytes admitted for the images of `channels` channels.
    #[must_use]
    pub fn images_bytes(&self, channels: usize) -> u64 {
        self.0.images_bytes(channels)
    }

    /// Read `channels` as the production reader does.
    ///
    /// # Errors
    /// Pixel I/O fails.
    pub fn read(&self, channels: Range<usize>) -> Result<PassImages, casa_images::ImageError> {
        self.0.pass_images(channels)
    }
}
