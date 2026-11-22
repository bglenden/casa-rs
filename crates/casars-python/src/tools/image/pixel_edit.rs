// SPDX-License-Identifier: LGPL-3.0-or-later

use casa_images::MutableImageInterface;
use casa_images::analysis::{ImagePixelSelection, evaluate_pixel_expression};
use ndarray::{ArrayD, IxDyn};
use pyo3::prelude::*;

use crate::convert::Variant;
use crate::tools::{LogLine, emit};

use super::PyImageTool;
use super::handle::{ImageHandle, Storage, raised, with_storage};
use super::masks::has_region;
use super::subimage::Region;

fn set_value(pixels: &Variant, formatted_pixels: &str) -> PyResult<Option<f32>> {
    if pixels.is_empty_sequence() {
        return Ok(None);
    }
    match pixels {
        Variant::Number(value) if value.is_finite() => {
            return evaluate_pixel_expression(formatted_pixels)
                .map(Some)
                .map_err(raised);
        }
        Variant::Text(text) if text.is_empty() || text == "[]" => return Ok(None),
        Variant::Texts(_)
        | Variant::Numbers(_)
        | Variant::Complexes(_)
        | Variant::Array { .. }
        | Variant::ComplexArray { .. } => {
            return Err(raised("The pixels expression must be scalar"));
        }
        _ => (),
    }
    evaluate_pixel_expression(&pixels.describe())
        .map(Some)
        .map_err(raised)
}

#[pymethods]
impl PyImageTool {
    #[pyo3(signature = (pixels=None, pixelmask=-1, region=None, list=false), text_signature = "($self, pixels=[], pixelmask=-1, region={}, list=False)")]
    fn set(
        &self,
        py: Python<'_>,
        pixels: Option<&Bound<'_, PyAny>>,
        pixelmask: i64,
        region: Option<&Bound<'_, PyAny>>,
        list: bool,
    ) -> PyResult<bool> {
        if self.state().image.is_none() {
            return Ok(false);
        }
        let variant = pixels
            .map(|value| value.extract::<Variant>())
            .transpose()?
            .unwrap_or(Variant::Empty);
        let pixel_history = match &variant {
            Variant::Number(value) if pixels.is_some_and(|v| v.extract::<i64>().is_err()) => {
                format!("{value:.6}")
            }
            Variant::Text(text) => format!("\"{text}\""),
            other => other.describe(),
        };
        let region = Region::argument(py, region, "set")?;
        let mut logs = Vec::new();
        let outcome = py.detach(|| {
            let state = self.state();
            let Some(attachment) = &state.image else { return Ok(false); };
            let mut handle = attachment.lock();
            handle.check_single_precision("set")?;
            let set_mask = pixelmask != -1;
            let has_pixels = !variant.is_empty_sequence() && !matches!(&variant, Variant::Text(text) if text.is_empty() || text == "[]");
            let ImageHandle::Float(storage) = &*handle else {
                return Err(raised("set only supports float-valued images"));
            };
            let text_selection = if let Some(region @ Region::Text(_)) = &region {
                if handle.fits().is_none() {
                    Some(with_storage!(storage, image =>
                        ImagePixelSelection::new(image.as_ref(), Some(region.as_region()))
                    ).map_err(raised)?)
                } else {
                    None
                }
            } else {
                None
            };
            if !has_pixels && !set_mask {
                logs.push(LogLine::warning("You must specify at least either the pixels or the mask to set"));
                return Ok(false);
            }
            if let Some(kind) = handle.read_only_file_type() {
                if kind == "MIRIADImage" {
                    return Err(raised(if has_pixels {
                        "Failed AlwaysAssert to.isWritable()"
                    } else {
                        "SubLattice::pixelMask - no pixelmask available"
                    }));
                }
                return Err(raised(if set_mask {
                    format!("Cannot make requested mask for this image type which is {kind}")
                } else {
                    format!("{kind} is not writable")
                }));
            }
            attachment.require_lock(&mut handle, casa_tables::LockType::Write)?;
            let ImageHandle::Float(storage) = &*handle else { unreachable!("float attachment checked") };
            let default = with_storage!(storage, image => image.active_pixel_mask_name());
            let mask_name = if set_mask {
                if let Some(name) = default {
                    Some(name)
                } else {
                    let name = (0..).map(|index| format!("mask{index}")).find(|name| !has_region(&handle, name)).expect("a fresh mask name");
                    let mask = ArrayD::from_elem(IxDyn(handle.shape()), true);
                    let ImageHandle::Float(storage) = &mut *handle else { unreachable!() };
                    with_storage!(storage, image => image.put_mask(&name, &mask)).map_err(raised)?;
                    with_storage!(storage, image => image.set_default_mask(&name)).map_err(raised)?;
                    handle.save().map_err(raised)?;
                    Some(name)
                }
            } else { None };
            let mask = mask_name.as_deref().map(|name| (name, pixelmask > 0));
            let ImageHandle::Float(storage) = &mut *handle else { unreachable!() };
            let (start, shape) = with_storage!(storage, image => {
                let selection = match text_selection {
                    Some(selection) => selection,
                    None => ImagePixelSelection::new(image.as_ref(), region.as_ref().map(Region::as_region)).map_err(raised)?,
                };
                let value = set_value(&variant, &pixel_history)?;
                selection.apply(image.as_mut(), value, mask).map_err(raised)?
            });
            if list {
                logs.push(LogLine::info(format!("Selected region at {start:?} with shape {shape:?}")));
            }
            if state.history {
                let region = region.as_ref().map_or_else(|| "{}".into(), Region::history);
                handle.record_history("set", format!("ia.set(pixels={pixel_history}, pixelmask={pixelmask}, region={region}, list={list})"))?;
            }
            handle.save().map_err(raised)?;
            Ok(true)
        });
        emit(py, "casars.tools.image", &logs)?;
        outcome
    }
}
