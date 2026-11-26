// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_coordinates::format_world_display;

use casa_images::{ImageError, ImageInterface, ImagePixel};

use super::{SubimageRegion, region::StridedSection};

/// Pixel geometry and formatted world corners for a CASA image region.
pub struct ImageRegionBounds {
    /// Zero-based inclusive lower pixel corner.
    pub lower: Vec<usize>,
    /// Zero-based inclusive upper pixel corner after applying the stride.
    pub upper: Vec<usize>,
    /// Sampling step along each pixel axis.
    pub stride: Vec<usize>,
    /// Number of selected positions per axis.
    pub sampled_shape: Vec<usize>,
    /// Shape of the full image.
    pub image_shape: Vec<usize>,
    /// World coordinates of the lower corner in CASA display notation.
    pub lower_world: String,
    /// World coordinates of the upper corner in CASA display notation.
    pub upper_world: String,
}

impl ImageRegionBounds {
    /// Resolve a region and format its pixel corners without reading image values.
    ///
    /// # Errors
    /// Returns an error if the region is invalid or the corners cannot be converted.
    pub fn resolve<T: ImagePixel>(
        image: &dyn ImageInterface<T>,
        region: Option<SubimageRegion<'_>>,
    ) -> Result<Self, ImageError> {
        let section = StridedSection::resolve(image, region)?;
        let upper = section
            .start
            .iter()
            .zip(&section.count)
            .zip(&section.stride)
            .map(|((&first, &count), &step)| first + (count - 1) * step)
            .collect::<Vec<_>>();
        let world = |corner: &[usize]| {
            let pixel = corner.iter().map(|&axis| axis as f64).collect::<Vec<_>>();
            format_world_display(image.coordinates(), &pixel)
        };
        Ok(Self {
            lower_world: world(&section.start)?,
            upper_world: world(&upper)?,
            lower: section.start,
            upper,
            stride: section.stride,
            sampled_shape: section.count,
            image_shape: image.shape().to_vec(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use casa_images::fits_read::fits_section_image;
    use casa_images::{PagedImage, TempImage, TempStoragePolicy};

    #[test]
    fn compound_geometry_does_not_allocate_cube_membership() {
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../casacore/casa-images/tests/data/imsubimage");
        let cube = PagedImage::<f32>::open(fixtures.join("cube.im")).unwrap();
        let mask =
            casa_tables::read_table_record_file(&fixtures.join("saved_compound_complement.rgn"))
                .unwrap();
        let metadata = TempImage::new(
            cube.shape().to_vec(),
            cube.coordinates().clone(),
            TempStoragePolicy::Memory,
        )
        .unwrap();
        let shape = [1024, 1024, 4, 1024];
        let image = fits_section_image(&shape, &metadata, Path::new("logical.fits"), |_, _, _| {
            panic!("bounding box read image pixels")
        });
        let bounds =
            ImageRegionBounds::resolve(&image, Some(SubimageRegion::Record(&mask))).unwrap();
        assert_eq!(bounds.lower, [0, 0, 0, 0]);
        assert_eq!(bounds.sampled_shape, shape);
    }
}
