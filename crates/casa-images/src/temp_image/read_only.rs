// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_lattices::TempStoragePolicy;

use crate::image_expr::{ImageExprValue, ImageExpression};
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
    /// Attach an expression as a read-only pixel source with editable image metadata.
    /// Reads evaluate the requested region of the expression on demand.
    ///
    /// # Errors
    /// Returns an error if the expression has no lattice shape.
    pub fn from_expression(expression: ImageExpression<T>) -> Result<Self, ImageError>
    where
        T: ImageExprValue,
    {
        let has_mask = expression.may_have_mask();
        let info = expression.raw_image_info();
        let mut image = Self::with_read_only_source(
            expression,
            None,
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
