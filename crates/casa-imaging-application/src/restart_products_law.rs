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
    /// Open the products at `prefix` of a run of `taylor_terms` Taylor terms
    /// (`.psf.tt0`, ...; `None` for a single image, `.psf`) of `shape`,
    /// `polarizations` and `channels`; the residual too when `residual`.
    ///
    /// # Errors
    /// An image cannot be opened, or its sums of weights are not one per
    /// Stokes plane and channel.
    pub fn open(
        prefix: &Path,
        taylor_terms: Option<usize>,
        (shape, polarizations, channels): ([usize; 2], usize, usize),
        residual: bool,
    ) -> Result<Self, crate::PrepareError> {
        let family = taylor_terms.map_or(Family::Single, |terms| Family::Taylor { terms });
        ExistingProducts::new(
            prefix,
            family,
            (shape, polarizations, channels),
            (true, residual),
        )
        .map(Self)
    }

    /// Admission for reading a PSF's complete beam metadata.
    #[must_use]
    pub fn beam_reading_bytes(channels: usize, polarizations: usize) -> u64 {
        ExistingProducts::beam_reading_bytes(channels, polarizations)
    }

    /// Read and retain the PSF beam metadata as the production restart does.
    ///
    /// # Errors
    /// The metadata is unreadable or contains an invalid beam.
    pub fn read_psf_beams(&mut self, channels: usize) -> Result<(), crate::PrepareError> {
        self.0.read_psf_beams(channels, 1e-5)
    }

    /// Read and retain the residual beam metadata for automatic masking.
    ///
    /// # Errors
    /// The metadata is unreadable or contains an invalid beam.
    pub fn read_mask_beams(&mut self, channels: usize) -> Result<(), crate::PrepareError> {
        self.0.read_mask_beams(channels, 1e-5)
    }

    /// A residual beam in pixels, for the reader's law.
    #[must_use]
    pub fn mask_beam(&self, index: usize) -> Option<casa_imaging_deconvolution::RestoringBeam> {
        self.0.mask_beams()?.get(index).copied()
    }

    /// The angular and pixel beam at `index`, for the reader's law.
    #[must_use]
    pub fn beam_pair(
        &self,
        index: usize,
    ) -> Option<(
        casa_imaging_deconvolution::RestoringBeam,
        casa_imaging_deconvolution::RestoringBeam,
    )> {
        Some((
            *self.0.psf_beams()?.get(index)?,
            *self.0.psf_pixel_beams()?.get(index)?,
        ))
    }

    /// Retained beam-grid allocation.
    #[must_use]
    pub fn beam_resident_bytes(&self) -> u64 {
        self.0.beam_resident_bytes()
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
