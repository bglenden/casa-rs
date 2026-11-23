// SPDX-License-Identifier: LGPL-3.0-or-later
//! Geometry checks for operations that validate a mask without using its values.

use std::any::Any;
use std::path::{Path, PathBuf};

use casa_coordinates::CoordinateSystem;
use casa_lattices::{Lattice, LatticeError};
use casa_types::{ArrayD, Complex64, RecordValue};

use crate::expr_parser::{HashMapResolver, referenced_images, subimage_mask_builder};
use crate::fits_read::FitsToolImport;
use crate::image_expr_lel::{broadcast_shapes, mask_coordinate_axes};
use crate::{AnyPagedImage, ImageError, ImageInfo, ImageInterface, ImagePixel, PagedImage};

use super::{
    mask_expression, mask_parse_error, open_mask_image, resolve_named_operand, stretch_axes,
    validate_mask_name,
};

pub(super) fn image_tool_mask_axes(
    source_coordinates: Option<&CoordinateSystem>,
    source_shape: &[usize],
    target_coordinates: &CoordinateSystem,
    target_shape: &[usize],
) -> Result<Vec<usize>, ImageError> {
    let target_axes = mask_coordinate_axes(target_coordinates);
    let incompatible = || {
        ImageError::CasaSelection(if source_shape == target_shape {
            "WCRegion::toLCRegion - a region axis is unknown or inconsistent in target coordinate system".into()
        } else {
            "Unable to extend mask: ExtendImage - new csys or shape incompatible with old ones"
                .into()
        })
    };
    let axes = if let Some(coordinates) = source_coordinates {
        let mapping = mask_coordinate_axes(coordinates)
            .iter()
            .map(|axis| target_axes.iter().position(|candidate| candidate == axis))
            .collect::<Option<Vec<_>>>()
            .filter(|map| map.len() == source_shape.len())
            .ok_or_else(incompatible)?;
        if mapping.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(ImageError::CasaSelection(
                "WCLELMask::toLCRegion - the order of the mask axes cannot be changed".into(),
            ));
        }
        mapping
    } else {
        (0..source_shape.len()).collect()
    };
    if axes.iter().zip(source_shape).any(|(&axis, &length)| {
        target_shape
            .get(axis)
            .is_none_or(|&target_length| length != 1 && length != target_length)
    }) {
        return Err(ImageError::CasaSelection(
            "Unable to extend mask: ExtendSpecifier - lengths of old axis mismatch".into(),
        ));
    }
    Ok(axes)
}

struct MaskGeometry {
    path: PathBuf,
    shape: Vec<usize>,
    coordinates: CoordinateSystem,
    unit: String,
    mask: Option<String>,
    complex: bool,
}

impl MaskGeometry {
    fn open(name: &str) -> Result<Self, ImageError> {
        let base = std::env::current_dir().map_err(|error| ImageError::Io(error.to_string()))?;
        let operand = resolve_named_operand(&base, name.to_owned())?;
        let path = operand.path.as_path();
        let mask = operand.mask.as_deref();
        if path.is_file() {
            if mask.is_some() {
                return Err(ImageError::CasaSelection(format!(
                    "FITS image '{name}' does not hold named masks"
                )));
            }
            let fits = FitsToolImport::open(path, 0, -1)?;
            return Ok(Self {
                path: path.to_path_buf(),
                shape: fits.shape().to_vec(),
                coordinates: fits.coordinates().clone(),
                unit: fits.units.clone(),
                mask: fits.has_implicit_mask().then(|| "mask0".into()),
                complex: false,
            });
        }
        match open_mask_image(path)? {
            AnyPagedImage::Float32(image) => Self::native(&image, path, mask, false),
            AnyPagedImage::Float64(_) if mask.is_some() => Err(ImageError::CasaSelection(format!(
                "Input mask specification is incorrect: ImageExprParse: {} is a PagedImage with an unsupported data type",
                path.display()
            ))),
            AnyPagedImage::Float64(image) => Self::native(&image, path, mask, false),
            AnyPagedImage::Complex32(image) => Self::native(&image, path, mask, true),
            AnyPagedImage::Complex64(image) => Self::native(&image, path, mask, true),
        }
    }

    fn native<T: ImagePixel>(
        image: &PagedImage<T>,
        path: &Path,
        mask: Option<&str>,
        complex: bool,
    ) -> Result<Self, ImageError> {
        validate_mask_name(image, mask)?;
        Ok(Self {
            path: path.to_path_buf(),
            shape: image.shape().to_vec(),
            coordinates: image.coordinates().clone(),
            unit: image.units().into(),
            mask: mask
                .map(str::to_owned)
                .or_else(|| image.default_mask_name()),
            complex,
        })
    }
}

impl<T: ImagePixel> Lattice<T> for MaskGeometry {
    fn shape(&self) -> &[usize] {
        &self.shape
    }

    fn get_at(&self, _: &[usize]) -> Result<T, LatticeError> {
        Err(LatticeError::Table(
            "mask geometry has no pixel data".into(),
        ))
    }

    fn get_slice(&self, _: &[usize], _: &[usize], _: &[usize]) -> Result<ArrayD<T>, LatticeError> {
        Err(LatticeError::Table(
            "mask geometry has no pixel data".into(),
        ))
    }
}

impl<T: ImagePixel> ImageInterface<T> for MaskGeometry {
    fn as_any(&self) -> Option<&dyn Any> {
        None
    }

    fn coordinates(&self) -> &CoordinateSystem {
        &self.coordinates
    }

    fn units(&self) -> &str {
        &self.unit
    }

    fn misc_info(&self) -> RecordValue {
        RecordValue::default()
    }

    fn image_info(&self) -> Result<ImageInfo, ImageError> {
        Ok(ImageInfo::default())
    }

    fn name(&self) -> Option<&Path> {
        Some(&self.path)
    }

    fn default_mask_name(&self) -> Option<String> {
        self.mask.clone()
    }
}

pub(in crate::analysis) fn validate_mask(
    mask_expr: Option<&str>,
    target_shape: &[usize],
    target_coordinates: &CoordinateSystem,
    stretch: bool,
) -> Result<(), ImageError> {
    let Some(raw) = mask_expr.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(());
    };
    let expression = mask_expression(raw);
    let opened = referenced_images(&expression)
        .map_err(mask_parse_error)?
        .into_iter()
        .map(|name| MaskGeometry::open(&name).map(|geometry| (name, geometry)))
        .collect::<Result<Vec<_>, _>>()?;
    let real = HashMapResolver(
        opened
            .iter()
            .filter(|(_, image)| !image.complex)
            .map(|(name, image)| (name.clone(), image as &dyn ImageInterface<f64>))
            .collect(),
    );
    let complex = HashMapResolver(
        opened
            .iter()
            .filter(|(_, image)| image.complex)
            .map(|(name, image)| (name.clone(), image as &dyn ImageInterface<Complex64>))
            .collect(),
    );
    let builder =
        subimage_mask_builder(&expression, &real, &complex, None).map_err(mask_parse_error)?;
    let shape = builder.shape();
    if shape.is_empty() || shape == target_shape {
        return Ok(());
    }
    if !stretch {
        return Err(ImageError::CasaSelection(format!(
            "The input image shape ({target_shape:?}) and mask shape ({shape:?}) are different, and it was specified that the mask should not be extended, so the mask cannot be applied to the (sub)image"
        )));
    }
    if let Some((_, image)) = opened.iter().find(|(_, image)| image.shape == shape) {
        stretch_axes(&image.coordinates, target_coordinates, shape, target_shape)?;
    } else if broadcast_shapes(shape, target_shape)? != target_shape {
        return Err(ImageError::CasaSelection(
            "Unable to extend mask: axis lengths do not conform".into(),
        ));
    }
    Ok(())
}
