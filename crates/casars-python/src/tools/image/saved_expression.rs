// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_images::analysis::{ImageToolExpressionInputs, ImageToolPixelExpression};
use casa_images::expr_file::{is_image_expr, read_tool_manifest};
use casa_images::{ImageExpression, TempImage, image_expr::ImageExprValue};
use casa_types::{Complex32, RecordValue};
use pyo3::prelude::*;

use super::attachment::AttachedImage;
use super::handle::{ImageHandle, Storage, raised};

pub(super) fn open(path: &Path) -> PyResult<AttachedImage> {
    let manifest = read_tool_manifest(path).map_err(raised)?;
    let operands = ImageToolExpressionInputs::resolve(&manifest.expression).map_err(raised)?;
    if operands.paths().any(is_image_expr) {
        return Err(raised(
            "Nested saved image expressions are not implemented yet (#1959)",
        ));
    }
    let retained = AttachedImage::open_expression_inputs(operands.paths())?;
    let (pixels, single_precision) = operands.compile_lazy_inferred().map_err(raised)?;
    super::imagecalc::check_geometry(&pixels, operands.paths().next().is_some())?;
    let misc = manifest.misc_info().map_err(raised)?;
    let image = match pixels {
        ImageToolPixelExpression::Real(expression) if single_precision => {
            ImageHandle::Float(expression_storage(
                expression
                    .convert_pixels(|value| value as f32)
                    .map_err(raised)?,
                path,
                misc,
            )?)
        }
        ImageToolPixelExpression::Real(expression) => {
            ImageHandle::Double(expression_storage(expression, path, misc)?)
        }
        ImageToolPixelExpression::Complex(expression) if single_precision => {
            ImageHandle::Complex(expression_storage(
                expression
                    .convert_pixels(|value| Complex32::new(value.re as f32, value.im as f32))
                    .map_err(raised)?,
                path,
                misc,
            )?)
        }
        ImageToolPixelExpression::Complex(expression) => {
            ImageHandle::DComplex(expression_storage(expression, path, misc)?)
        }
        ImageToolPixelExpression::Boolean(_) => unreachable!("saved image geometry is numeric"),
    };
    let mut attachment = AttachedImage::temporary(image);
    attachment.retain_expression_inputs(retained, operands.paths().map(PathBuf::from).collect());
    Ok(attachment)
}

fn expression_storage<T: ImageExprValue>(
    expression: ImageExpression<T>,
    path: &Path,
    misc: Option<RecordValue>,
) -> PyResult<Storage<T>> {
    let record = expression.coordinate_record();
    let mut image =
        TempImage::from_saved_expression(expression, path.to_path_buf()).map_err(raised)?;
    if let Some(misc) = misc {
        image.set_misc_info(misc).map_err(raised)?;
    }
    Ok(Storage::Temp {
        image: Box::new(image),
        record,
        fits: None,
    })
}
