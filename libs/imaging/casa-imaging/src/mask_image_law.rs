// SPDX-License-Identifier: LGPL-3.0-or-later
//! Counting-law access to the mask readers compile uses.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use casa_imaging_deconvolution::ReconstructionMaskPlan;
use casa_imaging_model::DirectionCoordinateSpec;
use casa_imaging_runtime::Reservation;

use crate::RunContext;
pub(crate) use crate::compile::ChannelMaskBox;

#[cfg(test)]
mod memory_tests;

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

    /// Bytes admitted before reading, then bytes the plan keeps.
    pub fn bytes(&self, regions: [&[ChannelMaskBox]; 2]) -> (u64, u64) {
        self.0.bytes(regions)
    }

    /// Read the mask into its plan with optional pixel boxes before and after it.
    ///
    /// # Errors
    /// Pixel I/O or reprojection fails.
    pub fn read(
        self,
        regions: [&[ChannelMaskBox]; 2],
    ) -> Result<ReconstructionMaskPlan, crate::PrepareError> {
        self.0.read(regions)
    }
}

/// Read the mask list using compile's aligned Stokes I plan and reservation.
///
/// # Errors
/// A mask cannot be admitted or read, or does not align with the output.
pub fn read_list(
    paths: &[PathBuf],
    direction: DirectionCoordinateSpec,
    shape: [usize; 2],
    context: &RunContext,
) -> Result<(ReconstructionMaskPlan, Reservation), crate::PrepareError> {
    crate::compile::read_aligned_list(paths, direction, shape, context)
}

/// Run the same admitted sky-polygon read, returning its support and the
/// read and retained byte bounds for the allocator's check.
///
/// # Errors
/// A vertex cannot be projected, or the polygon's crossings are invalid.
pub fn read_polygon(
    vertices: Arc<[[f64; 2]]>,
    direction: DirectionCoordinateSpec,
    shape: [usize; 2],
) -> Result<(Arc<[bool]>, u64, u64), crate::PrepareError> {
    use crate::compile::mask_polygon;
    let polygon = crate::tclean::MaskPolygon {
        vertices,
        j2000: true,
    };
    // As in request_mask and mask_plans, the domain and plan share the
    // request's immutable vertices. Charge the raster separately.
    let domain = polygon.clone();
    let input = domain.clone();
    let (read, kept) = mask_polygon::bytes(&input, shape);
    Ok((mask_polygon::support(&input, direction, shape)?, read, kept))
}
