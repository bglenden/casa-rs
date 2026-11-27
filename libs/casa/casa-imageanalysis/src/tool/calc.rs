// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel recalculation via LEL expression (`ia.calc`).

use crate::analysis::{ImageToolPixelExpression, calculate_complex_pixels, calculate_real_pixels};

use super::attachment::AttachedImage;
use super::conformance::coordinates_conform;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, with_storage};
use super::saved_expression;

/// Evaluates `pixels` expression and overwrites pixel values of `attached` (`ia.calc`).
pub fn calc_attached_pixels(
    attached: &AttachedImage,
    pixels: &str,
    verbose: bool,
    record_history: bool,
) -> ImageToolResult<(bool, Vec<ToolLogLine>)> {
    let mut logs = Vec::new();
    let mut image = attached.lock();
    image.check_single_precision("calc")?;
    let expression = saved_expression::compile_edit(pixels)?;
    let (shape, coordinates) = match (&*image, &expression) {
        (_, ImageToolPixelExpression::Boolean(_)) => {
            return Err(ImageToolError::new(
                "Exception: Unsupported node data type Bool.",
            ));
        }
        (ImageHandle::Float(_), ImageToolPixelExpression::Real(expression)) => {
            (expression.shape(), expression.coordinates())
        }
        (ImageHandle::Complex(_), ImageToolPixelExpression::Complex(expression)) => {
            (expression.shape(), expression.coordinates())
        }
        (ImageHandle::Complex(_), _) => {
            return Err(ImageToolError::new(
                "Exception: Resulting image is real valued butthe attached image is complex valued.",
            ));
        }
        (ImageHandle::Float(_), _) => {
            return Err(ImageToolError::new(
                "Exception: Resulting image is complex valued butthe attached image is real valued.",
            ));
        }
        _ => unreachable!("single precision checked"),
    };
    if verbose {
        logs.push(ToolLogLine::warning(
            "Overwriting pixel values of the currently attached image",
        ));
    }
    if !shape.is_empty() && shape != image.shape() {
        return Err(ImageToolError::new(format!(
            "The shape of the expression does not conform with the shape of the output imageExpression shape = {shape:?}Image shape      = {:?}",
            image.shape(),
        )));
    }
    if shape == image.shape() {
        if coordinates.n_pixel_axes() == 0 {
            logs.push(ToolLogLine::warning(
                "Images in expression have no coordinates",
            ));
        } else if !coordinates_conform(&image.coordinate_record(), &coordinates.to_record(), false)?
        {
            logs.push(ToolLogLine::warning(
                "The coordinates of the expression do not conform with the coordinates of the output image. Proceeding with output image coordinates",
            ));
        }
    }
    if image.fits().is_some() {
        return Err(ImageToolError::new("Failed AlwaysAssert to.isWritable()"));
    }
    attached.require_lock(&mut image, casa_tables::LockType::Write)?;
    match (&mut *image, expression) {
        (ImageHandle::Float(storage), ImageToolPixelExpression::Real(expression)) => {
            with_storage!(storage, target => calculate_real_pixels(target.as_mut(), &expression))
        }
        (ImageHandle::Complex(storage), ImageToolPixelExpression::Complex(expression)) => {
            with_storage!(storage, target => calculate_complex_pixels(target.as_mut(), &expression))
        }
        _ => unreachable!("compatible expression type checked"),
    }?;
    if record_history {
        let text = if pixels.len() > 300 { "..." } else { pixels };
        image.record_history(
            "calc",
            format!("ia.calc(expr=\"{text}\", verbose={verbose})"),
        )?;
    }
    image.save()?;
    Ok((true, logs))
}
