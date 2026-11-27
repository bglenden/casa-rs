// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_lattices::TempStoragePolicy;

use crate::image_expr::{ImageExprValue, ImageExpression};
use crate::virtual_concat::ConcatImage;
use crate::{ImageError, ImageInterface, ImagePixel};

use super::TempImage;

pub(super) struct ReadOnlyImage<T: ImagePixel> {
    pub(super) source: Box<dyn ImageInterface<T> + Send + Sync>,
    pub(super) name: Option<PathBuf>,
    pub(super) image_type: &'static str,
    pub(super) write_error: &'static str,
    pub(super) has_mask: bool,
}

impl<T: ImagePixel> TempImage<T> {
    /// Rebuild the attached sampler after a validated coordinate edit.
    /// # Errors
    /// Returns an error if the source cannot update its geometry.
    pub fn set_source_coordinates(
        &mut self,
        coordinates: casa_coordinates::CoordinateSystem,
        record: &casa_core::RecordValue,
    ) -> Result<(), ImageError> {
        self.read_only
            .as_mut()
            .ok_or_else(|| ImageError::CasaSelection("A component-list source is required".into()))?
            .source
            .update_source_coordinates(coordinates.clone(), record)?;
        self.coords = coordinates;
        Ok(())
    }

    /// Enable or disable evaluated section caching for the attached pixel source.
    /// # Errors
    /// Returns an error if the image source does not support caching.
    pub fn set_source_cache(&mut self, enabled: bool) -> Result<(), ImageError> {
        self.read_only
            .as_mut()
            .ok_or_else(|| ImageError::CasaSelection("A component-list source is required".into()))?
            .source
            .set_source_cache(enabled)
    }
}

impl<T: ImagePixel> TempImage<T> {
    /// Attach a native virtual concatenation with its initial beam record.
    ///
    /// # Errors
    /// Returns an error for invalid image metadata or geometry.
    pub fn from_virtual_concat(
        source: ConcatImage<T>,
        info: casa_core::RecordValue,
    ) -> Result<Self, ImageError> {
        let mut image = Self::from_read_only_image(
            source,
            "ImageConcat",
            "Some of the underlying lattices are not writable",
        )?;
        image.image_info = crate::ImageInfo::from_record(&info)?;
        image.image_info_record = Some(info);
        Ok(image)
    }

    /// Attach an expression as a read-only pixel source with editable image metadata.
    /// Reads evaluate the requested region of the expression on demand.
    ///
    /// # Errors
    /// Returns an error if the expression has no lattice shape.
    pub fn from_expression(expression: ImageExpression<T>) -> Result<Self, ImageError>
    where
        T: ImageExprValue,
    {
        Self::with_expression(expression, None)
    }

    /// Attach a saved expression with its own persistent directory name.
    ///
    /// # Errors
    /// Returns an error if the expression has no lattice shape.
    pub fn from_saved_expression(
        expression: ImageExpression<T>,
        name: PathBuf,
    ) -> Result<Self, ImageError>
    where
        T: ImageExprValue,
    {
        Self::with_expression(expression, Some(name))
    }

    /// Borrow the captured graph behind a named saved expression.
    pub fn saved_expression(&self) -> Option<&ImageExpression<T>>
    where
        T: ImageExprValue,
    {
        let source = self.read_only.as_ref()?;
        if source.name.is_none() || source.image_type != "ImageExpr" {
            return None;
        }
        source.source.as_any()?.downcast_ref()
    }

    fn with_expression(
        expression: ImageExpression<T>,
        name: Option<PathBuf>,
    ) -> Result<Self, ImageError>
    where
        T: ImageExprValue,
    {
        let has_mask = expression.may_have_mask();
        let info = expression.raw_image_info();
        let mut image = Self::with_read_only_source(
            expression,
            name,
            "ImageExpr",
            "ImageExpr::putSlice - is not possible as ImageExpr is not writable",
            has_mask,
        )?;
        image.image_info_record = info;
        Ok(image)
    }

    /// Attach a read-only image source and keep its persistent name.
    ///
    /// Sections are read through the source. The attachment holds a separate
    /// view of the source metadata for the lifetime of the tool.
    ///
    /// # Errors
    /// The source has no image shape or its metadata cannot be read.
    pub fn from_read_only_image(
        source: impl ImageInterface<T> + Send + Sync + 'static,
        image_type: &'static str,
        write_error: &'static str,
    ) -> Result<Self, ImageError> {
        let name = source.name().map(Path::to_path_buf);
        let has_mask = source.has_pixel_mask();
        Self::with_read_only_source(source, name, image_type, write_error, has_mask)
    }

    fn with_read_only_source(
        source: impl ImageInterface<T> + Send + Sync + 'static,
        name: Option<PathBuf>,
        image_type: &'static str,
        write_error: &'static str,
        has_mask: bool,
    ) -> Result<Self, ImageError> {
        let mut image = Self::new(
            vec![1; source.ndim()],
            source.coordinates().clone(),
            TempStoragePolicy::Memory,
        )?;
        image.units = source.units().to_owned();
        image.misc_info = source.misc_info();
        image.image_info = source.image_info()?;
        image.read_only = Some(ReadOnlyImage {
            source: Box::new(source),
            name,
            image_type,
            write_error,
            has_mask,
        });
        Ok(image)
    }
}
