// SPDX-License-Identifier: LGPL-3.0-or-later
//! Sectioned evaluation and writing for the public image pixel calculator.

use std::path::Path;

use casa_core::{ArrayD, Complex32, Complex64};
use casa_lattices::{TiledShape, TraversalCursorIter, TraversalSpec};
use ndarray::{IxDyn, Zip};

use crate::image_expr::ImageExprValue;
use crate::{
    ImageError, ImageExpression, ImageInterface, MutableImageInterface, PagedImage, ScratchSpace,
    TempImage, TempStoragePolicy,
};

/// Write a public image calculator product to a new paged image.
///
/// # Errors
/// Returns an error if evaluating or writing a section fails.
pub fn materialize_image_tool_expression<T: ImageExprValue>(
    expression: &ImageExpression<T>,
    path: &Path,
) -> Result<PagedImage<T>, ImageError> {
    let (mut image, _) = expression.save_as_with_full_metadata(
        path,
        expression.coordinates(),
        expression.units(),
        expression.image_info_ref(),
        expression.misc_info_ref(),
        expression.legacy_coords_ref().cloned(),
    )?;
    if let Some(record) = expression.raw_image_info() {
        image.set_image_info_record(record)?;
    }
    if expression.has_mask() && image.default_mask_name().is_none() {
        initialize_default_mask(&mut image, Some("mask0"))?;
    }
    image.save()?;
    Ok(image)
}

/// Apply a compiled real expression to an attached single-precision image.
///
/// # Errors
/// Returns an error if the shapes differ, evaluation fails or the image cannot be written.
pub fn calculate_real_pixels(
    image: &mut impl MutableImageInterface<f32>,
    expression: &ImageExpression<f64>,
) -> Result<(), ImageError> {
    calculate_pixels(image, expression, |value| value as f32)
}

/// Apply a compiled complex expression to an attached single-precision image.
///
/// # Errors
/// Returns an error if the shapes differ, evaluation fails or the image cannot be written.
pub fn calculate_complex_pixels(
    image: &mut impl MutableImageInterface<Complex32>,
    expression: &ImageExpression<Complex64>,
) -> Result<(), ImageError> {
    calculate_pixels(image, expression, |value| {
        Complex32::new(value.re as f32, value.im as f32)
    })
}

fn calculate_pixels<S: ImageExprValue, T: ImageExprValue>(
    image: &mut impl MutableImageInterface<T>,
    expression: &ImageExpression<S>,
    convert: impl Fn(S) -> T + Copy,
) -> Result<(), ImageError> {
    if !image.is_writable() {
        return Err(ImageError::CasaSelection(
            "Failed AlwaysAssert to.isWritable()".into(),
        ));
    }
    let shape = image.shape().to_vec();
    let scalar = expression.shape().is_empty();
    if !scalar && expression.shape() != shape {
        return Err(ImageError::CasaSelection(format!(
            "The shape of the expression does not conform with the shape of the output imageExpression shape = {:?}Image shape = {shape:?}",
            expression.shape(),
        )));
    }

    let chunk = TiledShape::new(shape.clone(), std::mem::size_of::<S>())?
        .tile_shape()
        .to_vec();
    let cursors = || {
        TraversalCursorIter::new(
            shape.clone(),
            chunk.clone(),
            TraversalSpec::chunks(chunk.clone()),
        )
    };
    let stride = vec![1; shape.len()];
    let has_default_mask = image.active_pixel_mask_name().is_some();
    let new_mask = (expression.may_have_mask() && !has_default_mask)
        .then(|| fresh_default_mask_name(image))
        .transpose()?;
    let scalar_value = if scalar {
        let valid = ImageInterface::get_mask_slice(expression, &[], &[], &[])?
            .is_none_or(|mask| mask[IxDyn(&[])]);
        if !valid {
            return initialize_default_mask(image, new_mask.as_deref());
        }
        Some(convert(expression.get_at(&[])?))
    } else {
        None
    };
    if let Some(value) = scalar_value
        && !has_default_mask
    {
        initialize_default_mask(image, new_mask.as_deref())?;
        for cursor in cursors() {
            let cursor = cursor?;
            let values = ArrayD::from_elem(IxDyn(&cursor.shape), value);
            image.put_slice(&values, &cursor.position)?;
        }
        return Ok(());
    }

    // Finish sampling before writing the attachment. This preserves the
    // original values when an operand aliases the output or a read fails.
    let mut staged = TempImage::new(
        shape.clone(),
        image.coordinates().clone(),
        TempStoragePolicy::Auto {
            memory_budget_bytes: 64 * 1024 * 1024,
            scratch: ScratchSpace::SystemTemp,
        },
    )?;
    if let Some(value) = scalar_value {
        for cursor in cursors() {
            let cursor = cursor?;
            let values = ArrayD::from_elem(IxDyn(&cursor.shape), value);
            let values = keep_masked_destination(image, &cursor.position, values, &stride)?;
            staged.put_slice(&values, &cursor.position)?;
        }
    } else {
        expression.visit_pixel_sections(|start, values| {
            let values = keep_masked_destination(image, start, values.mapv(convert), &stride)?;
            staged.put_slice(&values, start)?;
            Ok(())
        })?;
    }
    initialize_default_mask(image, new_mask.as_deref())?;
    for cursor in cursors() {
        let cursor = cursor?;
        let values =
            casa_lattices::Lattice::get_slice(&staged, &cursor.position, &cursor.shape, &stride)?;
        image.put_slice(&values, &cursor.position)?;
    }
    Ok(())
}

fn fresh_default_mask_name<T: ImageExprValue>(
    image: &impl ImageInterface<T>,
) -> Result<String, ImageError> {
    let mut occupied = image.mask_names();
    if let Some(paged) = image
        .as_any()
        .and_then(|image| image.downcast_ref::<PagedImage<T>>())
    {
        occupied.extend(paged.region_names());
    }
    let name = (0..)
        .map(|number| format!("mask{number}"))
        .find(|name| !occupied.contains(name))
        .expect("a fresh mask name");
    ensure_mask_path_available(image, &name)?;
    Ok(name)
}

fn ensure_mask_path_available<T: ImageExprValue>(
    image: &impl ImageInterface<T>,
    name: &str,
) -> Result<(), ImageError> {
    let Some(path) = image.name() else {
        return Ok(());
    };
    let path = path.join(name);
    match std::fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(ImageError::Io(error.to_string())),
        Ok(_) => Err(ImageError::CasaSelection(format!(
            "Table {} already exists (and is not a true table directory)",
            path.display(),
        ))),
    }
}

fn initialize_default_mask<T: ImageExprValue>(
    image: &mut impl MutableImageInterface<T>,
    name: Option<&str>,
) -> Result<(), ImageError> {
    let Some(name) = name else { return Ok(()) };
    ensure_mask_path_available(image, name)?;
    let shape = image.shape().to_vec();
    let tile = TiledShape::new(shape.clone(), std::mem::size_of::<bool>())?
        .tile_shape()
        .to_vec();
    let result: Result<(), ImageError> = (|| {
        for cursor in TraversalCursorIter::new(shape, tile.clone(), TraversalSpec::chunks(tile)) {
            let cursor = cursor?;
            let mask = ArrayD::from_elem(IxDyn(&cursor.shape), true);
            image.put_mask_slice(name, &mask, &cursor.position)?;
        }
        image.set_default_mask(name)
    })();
    if result.is_err() && image.mask_names().iter().any(|stored| stored == name) {
        let _ = image.remove_mask(name);
    }
    result
}

fn keep_masked_destination<T: ImageExprValue>(
    image: &impl ImageInterface<T>,
    start: &[usize],
    mut values: ArrayD<T>,
    stride: &[usize],
) -> Result<ArrayD<T>, ImageError> {
    if let Some(valid) = image.get_mask_slice(start, values.shape(), stride)?
        && valid.iter().any(|&value| !value)
    {
        let previous = image.get_slice(start, values.shape(), stride)?;
        Zip::from(&mut values)
            .and(&valid)
            .and(&previous)
            .for_each(|value, &valid, &previous| {
                if !valid {
                    *value = previous;
                }
            });
    }
    Ok(values)
}
