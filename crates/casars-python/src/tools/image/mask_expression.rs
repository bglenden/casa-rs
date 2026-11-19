// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;

use casa_images::{
    AnyPagedImage, ImageInterface,
    expr_parser::{ExprValueConvert, HashMapResolver, parse_mask_expr, referenced_images},
    image_expr::ImageExprValue,
};
use ndarray::{ArrayD, IxDyn, ShapeBuilder};
use pyo3::prelude::*;

use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::masks::{has_region, validate_name};
use super::{PyImageTool, stub};

fn evaluate_typed<T: ImageExprValue + PartialOrd + ExprValueConvert>(
    expression: &str,
    sources: HashMap<String, &dyn ImageInterface<T>>,
    shape: &[usize],
) -> PyResult<ArrayD<bool>> {
    let compiled = parse_mask_expr(expression, &HashMapResolver(sources)).map_err(|error| {
        if error.message.contains("boolean") || error.message.contains("Boolean") {
            raised("Exception: The expression type must be Boolean.")
        } else {
            raised(format!("Image Expression: {error}"))
        }
    })?;
    if compiled.shape().is_empty() {
        return Ok(ArrayD::from_elem(
            IxDyn(shape).f(),
            compiled.get_at(&[]).map_err(raised)?,
        ));
    }
    if compiled.shape() != shape {
        return Err(raised(format!(
            "The shape of the expression does not conform with the shape of the output imageExpression shape = {:?}Image shape      = {shape:?}",
            compiled.shape()
        )));
    }
    compiled.get().map_err(raised)
}

pub(super) fn evaluate(expression: &str, shape: &[usize]) -> PyResult<ArrayD<bool>> {
    if expression.is_empty() {
        return Err(raised("Exception: You must specify an expression."));
    }
    let images = referenced_images(expression).map_err(raised)?
        .into_iter()
        .map(|name| {
            let image = AnyPagedImage::open(&name).map_err(|_| {
                raised(format!("ImageExprParse: '{name}' is an unknown lattice or image or it is an unqualified region"))
            })?;
            Ok((name, image))
        })
        .collect::<PyResult<Vec<_>>>()?;
    if images
        .iter()
        .all(|(_, image)| matches!(image, AnyPagedImage::Float32(_)))
    {
        let images = images
            .iter()
            .filter_map(|(name, image)| match image {
                AnyPagedImage::Float32(image) => {
                    Some((name.clone(), image as &dyn ImageInterface<f32>))
                }
                _ => None,
            })
            .collect();
        evaluate_typed(expression, images, shape)
    } else if images
        .iter()
        .all(|(_, image)| matches!(image, AnyPagedImage::Float64(_)))
    {
        let images = images
            .iter()
            .filter_map(|(name, image)| match image {
                AnyPagedImage::Float64(image) => {
                    Some((name.clone(), image as &dyn ImageInterface<f64>))
                }
                _ => None,
            })
            .collect();
        evaluate_typed(expression, images, shape)
    } else {
        Err(stub("calcmask").unwrap_err())
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (mask="", name="", asdefault=true), text_signature = "($self, mask='', name='', asdefault=True)")]
    fn calcmask(&self, py: Python<'_>, mask: &str, name: &str, asdefault: bool) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(image) = state.image.as_ref() else { return Ok(false) };
            let mut image = image.lock();
            if !mask.is_empty() && image.fits().is_some() {
                return Err(raised("Exception: Cannot make requested mask for this image type which is FITSImage."));
            }
            let values = evaluate(mask, image.shape())?;
            let generated;
            let mask_name = if name.is_empty() {
                generated = (0..)
                    .map(|index| format!("mask{index}"))
                    .find(|name| !has_region(&image, name))
                    .expect("a fresh mask name");
                generated.as_str()
            } else {
                validate_name(name)?;
                name
            };
            with_pixels!(&mut *image, storage => with_storage!(storage, image => image.put_mask(mask_name, &values))).map_err(raised)?;
            if asdefault {
                with_pixels!(&mut *image, storage => with_storage!(storage, image => image.set_default_mask(mask_name))).map_err(raised)?;
            }
            if state.history {
                image.record_history("calcmask", format!(
                    "ia.calcmask(mask={mask:?}, name={name:?}, asdefault={asdefault})"
                ))?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        })
    }
}
