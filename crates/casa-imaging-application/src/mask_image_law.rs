// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting-law access to the mask-image reader compile uses.

use std::path::Path;

use casa_imaging_deconvolution::ReconstructionMaskPlan;
use casa_imaging_model::DirectionCoordinateSpec;

/// A mask image opened onto its own direction grid, before any pixels are
/// read.
pub struct Input(crate::compile::MaskImage);

impl Input {
    /// Open the mask image at `path` for an image of `channels` channels on
    /// its own spectral coordinate and `polarizations` Stokes planes, as
    /// compile opens it, with its bounded tile cache.
    ///
    /// # Errors
    /// The image cannot be opened, or tclean refuses it on such an image.
    pub fn open(
        path: &Path,
        channels: usize,
        polarizations: usize,
    ) -> Result<Self, crate::PrepareError> {
        crate::compile::open_mask_image_onto_itself(path, channels, polarizations).map(Self)
    }

    /// Set the output direction coordinate, keeping the source shape and
    /// the existing channel and polarization plan.
    #[must_use]
    pub fn with_target_direction(self, target: DirectionCoordinateSpec) -> Self {
        Self(self.0.with_target_direction(target))
    }

    /// Bytes admitted before the pixels are read.
    pub fn reading_bytes(&self) -> u64 {
        self.0.reading_bytes()
    }

    /// Bytes the plan keeps.
    pub fn retained_bytes(&self) -> u64 {
        self.0.retained_bytes()
    }

    /// Read the mask into its plan with optional pixel boxes before and after it.
    ///
    /// # Errors
    /// Pixel I/O or reprojection fails.
    pub fn read(
        self,
        regions: [&[[i64; 4]]; 2],
    ) -> Result<ReconstructionMaskPlan, crate::PrepareError> {
        self.0.read(regions)
    }
}
