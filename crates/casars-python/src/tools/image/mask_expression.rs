// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::HashMap;

use casa_coordinates::{CoordinateModel, CoordinateSystem, CoordinateType};
use casa_images::analysis::evaluate_image_tool_mask;
use casa_images::{
    AnyPagedImage, ImageInterface,
    expr_parser::{ExprValueConvert, HashMapResolver, parse_mask_expr, referenced_images},
    image_expr::ImageExprValue,
};
use ndarray::{ArrayD, Axis, IxDyn, ShapeBuilder};
use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::masks::{has_region, validate_name};
use super::pixels::Chunk;
use super::stub;

pub(super) struct MaskTarget<'a> {
    pub shape: &'a [usize],
    pub section: Option<(&'a Chunk, &'a CoordinateSystem)>,
    pub stretch: bool,
    pub method: &'static str,
}

fn mask_axes(system: &CoordinateSystem) -> Vec<(CoordinateType, usize, String)> {
    (0..system.n_coordinates())
        .flat_map(|index| {
            let coordinate = system.coordinate(index);
            let mut names = coordinate.axis_names();
            match coordinate {
                CoordinateModel::Direction(direction) => {
                    names.fill(direction.conversion_direction_ref().to_string());
                }
                CoordinateModel::Spectral(spectral) => {
                    names.fill(spectral.world_frequency_ref().to_string());
                }
                _ => {}
            }
            names
                .into_iter()
                .take(coordinate.n_pixel_axes())
                .enumerate()
                .map(move |(axis, name)| (coordinate.coordinate_type(), axis, name))
        })
        .collect()
}

fn evaluate_typed<T: ImageExprValue + PartialOrd + ExprValueConvert>(
    expression: &str,
    sources: HashMap<String, &dyn ImageInterface<T>>,
    target: &MaskTarget<'_>,
) -> PyResult<ArrayD<bool>> {
    let resolver = HashMapResolver(sources);
    let compiled = parse_mask_expr(expression, &resolver).map_err(|error| {
        if error.message.contains("boolean") || error.message.contains("Boolean") {
            raised("Exception: The expression type must be Boolean.")
        } else {
            raised(format!("Image Expression: {error}"))
        }
    })?;
    let shape = target.shape;
    if let Some((section, coordinates)) = target.section {
        let source_shape = compiled.shape();
        if source_shape.is_empty() {
            return compiled
                .eval_section_for_shape(&section.start, &section.size, &section.stride, shape)
                .map_err(raised);
        }
        if source_shape != shape && !target.stretch {
            return Err(raised(format!(
                "The input image shape ({shape:?}) and mask shape ({source_shape:?}) are different, and it was specified that the mask should not be extended"
            )));
        }
        let target_axes = mask_axes(coordinates);
        let incompatible = if source_shape == shape {
            "WCRegion::toLCRegion - a region axis is unknown or inconsistent in target coordinate system"
        } else {
            "Unable to extend mask: ExtendImage - new csys or shape incompatible with old ones"
        };
        let mut axes = None;
        for source in resolver
            .0
            .values()
            .filter(|source| source.shape() == source_shape)
        {
            let mapping = mask_axes(source.coordinates())
                .iter()
                .map(|axis| target_axes.iter().position(|candidate| candidate == axis))
                .collect::<Option<Vec<_>>>()
                .filter(|map| map.len() == source_shape.len())
                .ok_or_else(|| raised(incompatible))?;
            if mapping.windows(2).any(|pair| pair[0] >= pair[1]) {
                return Err(raised(
                    "WCLELMask::toLCRegion - the order of the mask axes cannot be changed",
                ));
            }
            if axes.as_ref().is_some_and(|previous| *previous != mapping) {
                return Err(raised(incompatible));
            }
            axes = Some(mapping);
        }
        let axes = axes.unwrap_or_else(|| (0..source_shape.len()).collect());
        if axes.iter().zip(source_shape).any(|(&axis, &length)| {
            shape
                .get(axis)
                .is_none_or(|&target_length| length != 1 && length != target_length)
        }) {
            return Err(raised(
                "Unable to extend mask: ExtendSpecifier - lengths of old axis mismatch",
            ));
        }
        let select = |values: &[usize]| axes.iter().map(|&axis| values[axis]).collect::<Vec<_>>();
        let mut mask = compiled
            .eval_section_for_shape(
                &select(&section.start),
                &select(&section.size),
                &select(&section.stride),
                &select(shape),
            )
            .map_err(raised)?;
        for axis in 0..shape.len() {
            if axes.binary_search(&axis).is_err() {
                mask = mask.insert_axis(Axis(axis));
            }
        }
        if mask.shape() == section.size {
            return Ok(mask);
        }
        return mask
            .broadcast(IxDyn(&section.size))
            .map(|view| view.to_owned())
            .ok_or_else(|| raised(incompatible));
    }
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

pub(super) fn evaluate_mask(expression: &str, target: MaskTarget<'_>) -> PyResult<ArrayD<bool>> {
    if expression.is_empty() {
        return Err(raised("Exception: You must specify an expression."));
    }
    let images = referenced_images(expression).map_err(raised)?
        .into_iter()
        .map(|name| {
            #[cfg(unix)]
            let opened = if target.section.is_some() {
                AnyPagedImage::open_with_lock(&name, 0)
            } else {
                AnyPagedImage::open(&name)
            };
            #[cfg(not(unix))]
            let opened = AnyPagedImage::open(&name);
            let image = opened.map_err(|_| {
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
        evaluate_typed(expression, images, &target)
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
        evaluate_typed(expression, images, &target)
    } else {
        Err(stub(target.method).unwrap_err())
    }
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (mask="", name="", asdefault=true), text_signature = "($self, mask='', name='', asdefault=True)")]
    fn calcmask(&self, py: Python<'_>, mask: &str, name: &str, asdefault: bool) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else { return Ok(false) };
            let mut image = attachment.lock();
            let kind = image.image_type_name();
            if !mask.is_empty() && matches!(kind, "FITSImage" | "ImageExpr") {
                return Err(raised(format!("Exception: Cannot make requested mask for this image type which is {kind}.")));
            }
            if mask.is_empty() {
                return Err(raised("Exception: You must specify an expression."));
            }
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            let values = evaluate_image_tool_mask(mask, image.shape()).map_err(raised)?;
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
                let mask = if mask.len() > 300 { "..." } else { mask };
                let name = if name.len() > 300 { "..." } else { name };
                image.record_history("calcmask", format!(
                    "ia.calcmask(mask=\"{mask}\", name=\"{name}\", asdefault={asdefault})"
                ))?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        })
    }
}
