// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_types::ArrayD;

use crate::image_expr::validate_slice_request;
use crate::{ImageError, ImageInterface, ImagePixel};

use super::{
    SubimageRegion,
    region::{SubimageSelection, saved::lattice_pixel_section},
};

/// Pixel section enclosing a selected region in the source image.
pub struct ImageRegionSection {
    /// First source pixel on each axis.
    pub start: Vec<usize>,
    /// Number of sampled pixels on each axis.
    pub count: Vec<usize>,
    /// Distance between adjacent samples.
    pub stride: Vec<usize>,
    /// Membership of the sampled pixels when requested for a nonrectangular region.
    pub membership: Option<ArrayD<bool>>,
    selection: Option<SubimageSelection>,
}

impl ImageRegionSection {
    /// Resolve an image region using the shared subimage geometry.
    ///
    /// # Errors
    /// The region cannot be resolved on the supplied image.
    pub fn resolve<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
        include_membership: bool,
    ) -> Result<Self, ImageError> {
        // Pixel-only regions also work when the image retains only one axis
        // of a direction coordinate and has no complete direction plane.
        if let Some(SubimageRegion::Record(record)) = region
            && let Some(section) =
                lattice_pixel_section(record, image.coordinates(), image.shape())?
        {
            return Ok(Self {
                start: section.start,
                count: section.count,
                stride: section.stride,
                membership: None,
                selection: None,
            });
        }
        let section = SubimageSelection::resolve_section(image, None, region, None, None)?;
        let mut membership = None;
        if include_membership && section.stride.is_none() {
            section.apply_selection_masks(&mut membership);
        }
        let count = section.packed_shape();
        Ok(Self {
            start: section.start.clone(),
            count,
            stride: section
                .stride
                .clone()
                .unwrap_or_else(|| vec![1; image.ndim()]),
            membership,
            selection: section.stride.is_none().then_some(section),
        })
    }

    /// Compute region membership in a window of the packed section.
    /// `start` is relative to the selected section's first output pixel.
    ///
    /// # Errors
    /// The window is outside the selected section.
    pub fn mask_window(
        &self,
        start: &[usize],
        count: &[usize],
    ) -> Result<Option<ArrayD<bool>>, ImageError> {
        validate_slice_request(&self.count, start, count, &vec![1; self.count.len()])?;
        let mut mask = None;
        if let Some(selection) = &self.selection {
            let source = self
                .start
                .iter()
                .zip(start)
                .map(|(&base, &offset)| base + offset)
                .collect::<Vec<_>>();
            selection.apply_window_masks(&mut mask, &source, count);
        }
        Ok(mask)
    }
}
