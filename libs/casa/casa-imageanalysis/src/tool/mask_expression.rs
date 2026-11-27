// SPDX-License-Identifier: LGPL-3.0-or-later

//! Mask expression validation and evaluation (`ia.calcmask`).

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{Storage, with_pixels, with_storage};
use super::masks::{has_region_or_mask, validate_mask_name};
use super::saved_expression;

/// Evaluates a LEL Boolean mask expression and installs/applies it to `attached`.
pub fn calc_mask(
    attached: &AttachedImage,
    mask_expr: &str,
    name: &str,
    asdefault: bool,
    record_history: bool,
) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    let kind = image.image_type_name();
    let kind = if kind == "ComponentListImage" && image.path().is_none() {
        "Temporary ComponentListImage"
    } else {
        kind
    };
    if !mask_expr.is_empty()
        && matches!(
            kind,
            "FITSImage"
                | "ImageExpr"
                | "ImageConcat"
                | "MIRIADImage"
                | "Temporary ComponentListImage"
        )
    {
        return Err(ImageToolError::new(format!(
            "Exception: Cannot make requested mask for this image type which is {kind}."
        )));
    }
    if mask_expr.is_empty() {
        return Err(ImageToolError::new(
            "Exception: You must specify an expression.",
        ));
    }
    attached.require_lock(&mut image, casa_tables::LockType::Write)?;
    let values = saved_expression::evaluate_mask(mask_expr, image.shape(), None)?;
    let explicit = (!name.is_empty()).then_some(name);
    if let Some(name) = explicit {
        validate_mask_name(name)?;
    }
    let component_mask = with_pixels!(&mut *image, storage => match storage {
        Storage::Temp { image, .. } => image.define_stored_mask(explicit, true, asdefault, &values),
        Storage::Paged(_) => Ok(false),
    })?;
    if !component_mask {
        let generated;
        let mask_name = if let Some(name) = explicit {
            name
        } else {
            generated = (0..)
                .map(|index| format!("mask{index}"))
                .find(|name| !has_region_or_mask(&image, name))
                .expect("a fresh mask name");
            &generated
        };
        with_pixels!(&mut *image, storage => with_storage!(storage, image => image.put_mask(mask_name, &values)))?;
        if asdefault {
            with_pixels!(&mut *image, storage => with_storage!(storage, image => image.set_default_mask(mask_name)))?;
        }
    }
    if record_history {
        let mask = if mask_expr.len() > 300 {
            "..."
        } else {
            mask_expr
        };
        let name = if name.len() > 300 { "..." } else { name };
        image.record_history(
            "calcmask",
            format!("ia.calcmask(mask=\"{mask}\", name=\"{name}\", asdefault={asdefault})"),
        )?;
    }
    image.save()?;
    Ok(true)
}
