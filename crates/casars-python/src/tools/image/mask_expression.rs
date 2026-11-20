// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::analysis::evaluate_image_tool_mask;
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
            let Some(image) = state.image.as_ref() else { return Ok(false) };
            let mut image = image.lock();
            if !mask.is_empty() && image.fits().is_some() {
                return Err(raised("Exception: Cannot make requested mask for this image type which is FITSImage."));
            }
            if mask.is_empty() {
                return Err(raised("Exception: You must specify an expression."));
            }
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
