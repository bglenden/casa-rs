// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting-law access to the mask readers compile uses.

use std::path::Path;
use std::sync::Arc;

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

/// Run the same admitted sky-polygon read, returning its support and the
/// read and retained byte bounds for the allocator's check.
///
/// # Errors
/// A vertex cannot be projected, or the polygon's crossings are invalid.
pub fn read_polygon(
    vertices: Vec<[f64; 2]>,
    direction: DirectionCoordinateSpec,
    shape: [usize; 2],
) -> Result<(Arc<[bool]>, u64, u64), crate::PrepareError> {
    use crate::compile::mask_polygon;
    let polygon = crate::tclean::MaskPolygon {
        vertices,
        j2000: true,
    };
    let (read, kept) = mask_polygon::bytes(&polygon, shape);
    Ok((
        mask_polygon::support(&polygon, direction, shape)?,
        read,
        kept,
    ))
}
