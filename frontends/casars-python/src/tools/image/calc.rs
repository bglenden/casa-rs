// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imageanalysis::analysis::{
    ImageToolPixelExpression, calculate_complex_pixels, calculate_real_pixels,
};
use pyo3::prelude::*;

use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::conformance::coordinates_conform;
use super::handle::{ImageHandle, Storage, raised, with_storage};

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (pixels, verbose=true), text_signature = "($self, pixels, verbose=True)")]
    fn calc(&self, py: Python<'_>, pixels: &str, verbose: bool) -> PyResult<bool> {
        let mut logs = Vec::new();
        let outcome = py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else { return Ok(false) };
            let mut image = attachment.lock();
            image.check_single_precision("calc")?;
            let expression = super::saved_expression::compile_edit(pixels)?;
            let (shape, coordinates) = match (&*image, &expression) {
                (_, ImageToolPixelExpression::Boolean(_)) => return Err(raised("Exception: Unsupported node data type Bool.")),
                (ImageHandle::Float(_), ImageToolPixelExpression::Real(expression)) =>
                    (expression.shape(), expression.coordinates()),
                (ImageHandle::Complex(_), ImageToolPixelExpression::Complex(expression)) =>
                    (expression.shape(), expression.coordinates()),
                (ImageHandle::Complex(_), _) => return Err(raised(
                    "Exception: Resulting image is real valued butthe attached image is complex valued."
                )),
                (ImageHandle::Float(_), _) => return Err(raised(
                    "Exception: Resulting image is complex valued butthe attached image is real valued."
                )),
                _ => unreachable!("single precision checked"),
            };
            if verbose {
                logs.push(LogLine::warning("Overwriting pixel values of the currently attached image"));
            }
            if !shape.is_empty() && shape != image.shape() {
                return Err(raised(format!(
                    "The shape of the expression does not conform with the shape of the output imageExpression shape = {shape:?}Image shape      = {:?}", image.shape(),
                )));
            }
            if shape == image.shape() {
                if coordinates.n_pixel_axes() == 0 {
                    logs.push(LogLine::warning("Images in expression have no coordinates"));
                } else if !coordinates_conform(&image.coordinate_record(), &coordinates.to_record(), false)? {
                    logs.push(LogLine::warning(
                        "The coordinates of the expression do not conform with the coordinates of the output image. Proceeding with output image coordinates"
                    ));
                }
            }
            if image.fits().is_some() {
                return Err(raised("Failed AlwaysAssert to.isWritable()"));
            }
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            match (&mut *image, expression) {
                (ImageHandle::Float(storage), ImageToolPixelExpression::Real(expression)) =>
                    with_storage!(storage, target => calculate_real_pixels(target.as_mut(), &expression)),
                (ImageHandle::Complex(storage), ImageToolPixelExpression::Complex(expression)) =>
                    with_storage!(storage, target => calculate_complex_pixels(target.as_mut(), &expression)),
                _ => unreachable!("compatible expression type checked"),
            }.map_err(raised)?;
            if state.history {
                let text = if pixels.len() > 300 { "..." } else { pixels };
                image.record_history("calc", format!("ia.calc(expr=\"{text}\", verbose={verbose})"))?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        });
        emit(py, "casars.tools.image", &logs)?;
        outcome
    }
}
