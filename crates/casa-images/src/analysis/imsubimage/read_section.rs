// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_core::ArrayD;

use crate::image_expr::validate_slice_request;
use crate::{ImageError, ImageInterface, ImagePixel};

use super::{
    SubimageRegion,
    region::{
        CopyTextInput, SubimageSelection, TextRegionContext, copy_text_input, invalid_region_file,
        saved::lattice_pixel_section,
    },
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
        Ok(Self::from_selection(
            section,
            image.ndim(),
            include_membership,
        ))
    }

    /// Resolve inline CRTF, a region file, or a named table region for a copy.
    ///
    /// Use the optional origin coordinate system and lattice shape to interpret
    /// CRTF before mapping it onto the image being copied. Without that origin,
    /// CRTF is in the copied image's own coordinates. A named table region is
    /// always looked up in the supplied table.
    ///
    /// # Errors
    /// The text, region file, geometry, or named region is invalid.
    pub fn resolve_copy_text<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        text: &str,
        origin: Option<(&CoordinateSystem, &[usize])>,
        table: Option<&Path>,
    ) -> Result<Self, ImageError> {
        let (text, file) = match copy_text_input(text, table)? {
            CopyTextInput::Record(record) => {
                return Self::resolve(image, Some(SubimageRegion::Record(&record)), false);
            }
            CopyTextInput::Document { text, file } => (text, file),
        };
        let context = TextRegionContext {
            coordinates: origin.map(|(coordinates, _)| coordinates),
            shape: Some(origin.map_or_else(|| image.shape(), |(_, shape)| shape)),
        };
        let section = SubimageSelection::resolve_with_origin(
            image,
            None,
            Some(SubimageRegion::Text(&text)),
            None,
            None,
            Some(context),
        )
        .map_err(|error| file.map_or(error, invalid_region_file))?;
        Ok(Self::from_selection(section, image.ndim(), false))
    }

    fn from_selection(section: SubimageSelection, ndim: usize, include_membership: bool) -> Self {
        let mut membership = None;
        if include_membership && section.stride.is_none() {
            section.apply_selection_masks(&mut membership);
        }
        let count = section.packed_shape();
        Self {
            start: section.start.clone(),
            count,
            stride: section.stride.clone().unwrap_or_else(|| vec![1; ndim]),
            membership,
            selection: section.stride.is_none().then_some(section),
        }
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

#[cfg(test)]
mod tests {
    use ndarray::IxDyn;

    use super::{ImageRegionSection, SubimageRegion};
    use crate::{TempImage, TempStoragePolicy};
    use casa_coordinates::{CoordinateSystem, LinearCoordinate};

    #[test]
    fn membership_windows_use_source_offsets_on_both_spatial_axes() {
        let mut coordinates = CoordinateSystem::new();
        coordinates.add_coordinate(LinearCoordinate::new(2, Vec::new(), Vec::new()));
        let image =
            TempImage::<f32>::new(vec![18, 20], coordinates, TempStoragePolicy::Memory).unwrap();
        let region = SubimageRegion::Text("poly[[4pix,5pix],[10pix,5pix],[4pix,12pix]]");
        let selection = ImageRegionSection::resolve(&image, Some(region), false).unwrap();
        assert_eq!(selection.start, [4, 5]);
        assert_eq!(selection.count, [7, 8]);
        for (start, count) in [([0, 2], [3, 4]), ([3, 2], [4, 4])] {
            let expected = ndarray::ArrayD::from_shape_fn(IxDyn(&count), |index| {
                7 * (start[0] + index[0]) + 6 * (start[1] + index[1]) <= 42
            });
            assert_eq!(
                selection.mask_window(&start, &count).unwrap(),
                Some(expected)
            );
        }
        assert!(selection.mask_window(&[4, 4], &[4, 4]).is_err());
    }
}
