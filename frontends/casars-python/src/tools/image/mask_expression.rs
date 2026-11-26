// SPDX-License-Identifier: LGPL-3.0-or-later

use pyo3::prelude::*;

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::masks::{has_region, validate_name};

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (mask="", name="", asdefault=true), text_signature = "($self, mask='', name='', asdefault=True)")]
    fn calcmask(&self, py: Python<'_>, mask: &str, name: &str, asdefault: bool) -> PyResult<bool> {
        py.detach(|| {
            let state = self.state();
            let Some(attachment) = state.image.as_ref() else { return Ok(false) };
            let mut image = attachment.lock();
            let kind = image.image_type_name();
            let kind = if kind == "ComponentListImage" && image.path().is_none() {
                "Temporary ComponentListImage"
            } else {
                kind
            };
            if !mask.is_empty() && matches!(kind, "FITSImage" | "ImageExpr" | "ImageConcat" | "MIRIADImage" | "Temporary ComponentListImage") {
                return Err(raised(format!("Exception: Cannot make requested mask for this image type which is {kind}.")));
            }
            if mask.is_empty() {
                return Err(raised("Exception: You must specify an expression."));
            }
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            let values = super::saved_expression::evaluate_mask(mask, image.shape(), None)?;
            let explicit = (!name.is_empty()).then_some(name);
            if let Some(name) = explicit {
                validate_name(name)?;
            }
            let component_mask = with_pixels!(&mut *image, storage => match storage {
                Storage::Temp { image, .. } => image.define_component_mask(explicit, true, asdefault, &values),
                Storage::Paged(_) => Ok(false),
            }).map_err(raised)?;
            if !component_mask {
                let generated;
                let mask_name = if let Some(name) = explicit {
                    name
                } else {
                    generated = (0..)
                        .map(|index| format!("mask{index}"))
                        .find(|name| !has_region(&image, name))
                        .expect("a fresh mask name");
                    &generated
                };
                with_pixels!(&mut *image, storage => with_storage!(storage, image => image.put_mask(mask_name, &values))).map_err(raised)?;
                if asdefault {
                    with_pixels!(&mut *image, storage => with_storage!(storage, image => image.set_default_mask(mask_name))).map_err(raised)?;
                }
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
