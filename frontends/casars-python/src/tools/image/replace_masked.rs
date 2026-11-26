// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_imageanalysis::analysis::{
    MaskReplacementOptions, MaskReplacementOutcome, SubimageRegion, replace_masked_complex_pixels,
    replace_masked_real_pixels, validate_mask_replacement_region,
};
use pyo3::prelude::*;

use crate::convert::Variant;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_pixels, with_storage};
use super::subimage::{Region, mask_argument};

fn pixel_argument(pixels: Option<&Bound<'_, PyAny>>) -> PyResult<(Variant, String)> {
    let variant = pixels
        .map(|value| value.extract::<Variant>())
        .transpose()?
        .unwrap_or(Variant::Empty);
    let label = match &variant {
        Variant::Number(value) if pixels.is_some_and(|v| v.extract::<i64>().is_err()) => {
            format!("{value:.6}")
        }
        Variant::Text(text) => format!("\"{text}\""),
        other => other.describe(),
    };
    Ok((variant, label))
}

#[pymethods]
impl PyImageTool {
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (pixels=None, region=None, mask=None, update=false, list=false, stretch=false), text_signature = "($self, pixels=[], region={}, mask='', update=False, list=False, stretch=False)")]
    fn replacemaskedpixels(
        &self,
        py: Python<'_>,
        pixels: Option<&Bound<'_, PyAny>>,
        region: Option<&Bound<'_, PyAny>>,
        mask: Option<&Bound<'_, PyAny>>,
        update: bool,
        list: bool,
        stretch: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let (variant, pixel_history) = pixel_argument(pixels)?;
        let expression = match &variant {
            Variant::Text(text) => text.as_str(),
            _ => &pixel_history,
        };
        let region = Region::argument(py, region, "replacemaskedpixels")?;
        let mask = mask_argument(mask)?;
        let mut logs = Vec::new();
        let outcome = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else {
                return Ok(false);
            };
            let mut image = attachment.lock();
            image.check_single_precision("replacemaskedpixels")?;
            if expression.is_empty() {
                return Err(raised("You must specify an expression"));
            }
            if image.fits().is_some() {
                return Err(raised("This image is not writable.  It is probably a reference or expression virtual image"));
            }
            Region::reject_compound_record(region.as_ref())?;
            if let Some(Region::Text(text)) = &region {
                // Reject compound text before upgrading the attachment's lock.
                with_pixels!(&*image, storage => with_storage!(storage, pixels =>
                    validate_mask_replacement_region(pixels.as_ref(), SubimageRegion::Text(text))
                )).map_err(raised)?;
            }
            attachment.require_lock(&mut image, casa_tables::LockType::Write)?;
            let selection = MaskReplacementOptions {
                expression,
                region: region.as_ref().map(Region::as_region),
                mask: &mask,
                update,
                stretch,
            };
            let applied = match &mut *image {
                ImageHandle::Float(storage) => with_storage!(storage, image =>
                    replace_masked_real_pixels(image.as_mut(), selection)),
                ImageHandle::Complex(storage) => with_storage!(storage, image =>
                    replace_masked_complex_pixels(image.as_mut(), selection)),
                _ => unreachable!("single precision checked"),
            }.map_err(raised)?;
            if matches!(applied, MaskReplacementOutcome::MissingNativeMask) {
                image.save().map_err(raised)?;
                return Err(raised("SubLattice::pixelMask - no pixelmask available"));
            }
            if list {
                logs.push(LogLine::info(format!("Replaced values of masked pixels by {expression}")));
            }
            if state.history {
                let region = region.as_ref().map_or_else(|| "{...}".into(), Region::history);
                image.record_history("replacemaskedpixels", format!(
                    "ia.replacemaskedpixels(pixels={pixel_history}, region={region}, mask=\"{mask}\", update={update}, list={list}, stretch={stretch})"
                ))?;
            }
            image.save().map_err(raised)?;
            Ok(true)
        });
        emit(py, "casars.tools.image", &logs)?;
        outcome
    }
}
