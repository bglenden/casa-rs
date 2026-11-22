// SPDX-License-Identifier: LGPL-3.0-or-later

use std::path::{Path, PathBuf};

use casa_images::TempImage;
use casa_images::analysis::{ImageToolExpressionInputs, ImageToolPixelExpression};
use casa_images::expr_file::{is_image_expr, read_tool_manifest};
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
    let ImageToolPixelExpression::Real(expression) = pixels else {
        return Err(raised(
            "Complex saved image expressions are not implemented yet (#1959)",
        ));
    };
    if !single_precision {
        return Err(raised(
            "Double saved image expressions are not implemented yet (#1959)",
        ));
    }
    let expression = expression
        .convert_pixels(|value| value as f32)
        .map_err(raised)?;
    let record = expression.coordinate_record();
    let mut image =
        TempImage::from_saved_expression(expression, path.to_path_buf()).map_err(raised)?;
    if let Some(misc) = manifest.misc_info().map_err(raised)? {
        image.set_misc_info(misc).map_err(raised)?;
    }
    let mut attachment = AttachedImage::temporary(ImageHandle::Float(Storage::Temp {
        image: Box::new(image),
        record,
        fits: None,
    }));
    attachment.retain_expression_inputs(retained, operands.paths().map(PathBuf::from).collect());
    Ok(attachment)
}
