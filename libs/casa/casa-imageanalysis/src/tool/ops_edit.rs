// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel-editing operations (`addnoise`, `calc`, `insert`, `putregion`, `replacemaskedpixels`, `set`).

use std::cmp::Ordering;
use std::path::Path;

use casa_core::{Complex32, Complex64};
use casa_images::{
    ImageError, ImageInterface, ImagePixel, MutableImageInterface, TempImage, TempStoragePolicy,
};
use casa_tables::LockType;
use ndarray::{ArrayD, IxDyn};

use crate::analysis::evaluate_pixel_expression;
use crate::analysis::imsubimage::{
    ImageInsertion, ImageNoise, ImagePixelSelection, MaskReplacementOptions,
    MaskReplacementOutcome, SubimageRegion, put_image_region, replace_masked_complex_pixels,
    replace_masked_real_pixels, validate_mask_replacement_region, validate_put_region_shapes,
};

use super::attachment::AttachedImage;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels, with_storage};
use super::masks::has_region_or_mask;
use super::pixels::PixelChunk;

/// Adds synthetic random noise to an attached image (`ia.addnoise`).
pub fn add_noise(
    attached: &AttachedImage,
    kind: &str,
    parameters: &[f64],
    seeds: &[i64],
    region: Option<SubimageRegion<'_>>,
    zero: bool,
    history_command: Option<String>,
) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    if image.fits().is_some() {
        return Err(ImageToolError::new(
            "LatticeIterator cannot be constructed; lattice is not writable",
        ));
    }
    let mut noise = ImageNoise::new(kind, parameters, seeds)?;
    attached.require_lock(&mut image, LockType::Write)?;
    match &mut *image {
        ImageHandle::Float(storage) => with_storage!(storage, target => {
            noise.apply(target.as_mut(), region, zero, |value, gen_draw| {
                (f64::from(value) + gen_draw.draw()) as f32
            })
        }),
        ImageHandle::Double(storage) => with_storage!(storage, target => {
            noise.apply(target.as_mut(), region, zero, |value, gen_draw| {
                value + gen_draw.draw()
            })
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, target => {
            noise.apply(target.as_mut(), region, zero, |value, gen_draw| {
                Complex32::new(
                    (f64::from(value.re) + gen_draw.draw()) as f32,
                    (f64::from(value.im) + gen_draw.draw()) as f32,
                )
            })
        }),
        ImageHandle::DComplex(storage) => with_storage!(storage, target => {
            noise.apply(target.as_mut(), region, zero, |value, gen_draw| {
                Complex64::new(value.re + gen_draw.draw(), value.im + gen_draw.draw())
            })
        }),
    }?;
    if let Some(cmd) = history_command {
        image.record_history("addnoise", cmd)?;
    }
    image.save()?;
    Ok(true)
}

/// Inserts pixels from `infile` into `attached` (`ia.insert`).
pub fn insert_image(
    attached: &AttachedImage,
    infile: &str,
    region: Option<SubimageRegion<'_>>,
    locate: &[f64],
    verbose: bool,
    history_command: Option<String>,
) -> ImageToolResult<(bool, Vec<ToolLogLine>)> {
    attached.lock().check_single_precision("insert")?;
    let source_path = Path::new(infile);
    let lock_order = attached
        .native_path()
        .zip(source_path.canonicalize().ok())
        .map(|(target, source)| target.cmp(&source));

    let (mut target, copied) = if lock_order == Some(Ordering::Equal) {
        let mut target = attached.lock();
        attached.require_write_lock(&mut target, "insert")?;
        let snapshot = snapshot_single_precision_handle(&target)?;
        let copied = copy_input_handle(&mut target, &snapshot, infile, region, locate);
        let release = attached.release_requested_lock(&mut target);
        let copied = copied?;
        release?;
        (target, copied)
    } else {
        let source = AttachedImage::open_for_ordered_read(source_path)?;
        let (mut target, mut source_image) = if lock_order == Some(Ordering::Less) {
            let mut target = attached.lock();
            let mut source_image = source.lock();
            attached.require_write_lock(&mut target, "insert")?;
            source.require_pixel_read_lock(&mut source_image)?;
            (target, source_image)
        } else {
            let mut source_image = source.lock();
            let mut target = attached.lock();
            source.require_pixel_read_lock(&mut source_image)?;
            attached.require_write_lock(&mut target, "insert")?;
            (target, source_image)
        };
        let copied = copy_input_handle(&mut target, &source_image, infile, region, locate);
        let release_source = source.release_requested_lock(&mut source_image);
        let release_target = attached.release_requested_lock(&mut target);
        let copied = copied?;
        release_source?;
        release_target?;
        (target, copied)
    };

    let mut logs = Vec::new();
    if verbose && copied {
        logs.push(ToolLogLine::info(format!("Inserted pixels from {infile}")));
    }
    if let Some(cmd) = history_command {
        target.record_history("insert", cmd)?;
    }
    target.save()?;
    Ok((true, logs))
}

fn snapshot_single_precision_handle(source: &ImageHandle) -> ImageToolResult<ImageHandle> {
    let shape = source.shape().to_vec();
    let coordinates = source.coordinates().clone();
    let record = source.coordinate_record();
    match source {
        ImageHandle::Float(storage) => {
            let mut temp = TempImage::<f32>::new(shape, coordinates, TempStoragePolicy::Memory)?;
            storage.with_float_view(|view| super::create::copy_typed_storage(view, &mut temp))?;
            Ok(ImageHandle::Float(Storage::Temp {
                image: Box::new(temp),
                record,
                fits: None,
            }))
        }
        ImageHandle::Complex(storage) => {
            let mut temp =
                TempImage::<Complex32>::new(shape, coordinates, TempStoragePolicy::Memory)?;
            with_storage!(storage, img => {
                super::create::copy_typed_storage(img.as_ref(), &mut temp)
            })?;
            Ok(ImageHandle::Complex(Storage::Temp {
                image: Box::new(temp),
                record,
                fits: None,
            }))
        }
        ImageHandle::Double(_) | ImageHandle::DComplex(_) => Err(ImageToolError::new(
            "Exception: Unsupported image data type.",
        )),
    }
}

fn copy_input_handle(
    target: &mut ImageHandle,
    source: &ImageHandle,
    infile: &str,
    region: Option<SubimageRegion<'_>>,
    locate: &[f64],
) -> ImageToolResult<bool> {
    match (target, source) {
        (ImageHandle::Float(target), ImageHandle::Float(source)) => {
            Ok(source.with_float_view(|src| {
                if matches!(target, Storage::Temp { fits: Some(_), .. }) {
                    target.with_float_view(|dst| {
                        if ImageInsertion::new(src, dst, region, locate)?.is_some() {
                            return Err(ImageError::CasaSelection(
                                "This image is not writable".into(),
                            ));
                        }
                        Ok(false)
                    })
                } else {
                    copy_into(target, src, region, locate)
                }
            })?)
        }
        (ImageHandle::Complex(target), ImageHandle::Complex(source)) => {
            Ok(with_storage!(source, image => {
                copy_into(target, image.as_ref(), region, locate)
            })?)
        }
        (_, ImageHandle::Double(_) | ImageHandle::DComplex(_)) => Err(ImageToolError::new(
            "Exception: Unsupported image data type.",
        )),
        _ => Err(ImageToolError::new(format!(
            "Attached image pixel data type differs from that of {infile}"
        ))),
    }
}

fn copy_into<T: ImagePixel>(
    target: &mut Storage<T>,
    source: &dyn ImageInterface<T>,
    region: Option<SubimageRegion<'_>>,
    locate: &[f64],
) -> Result<bool, ImageError> {
    let plan = with_storage!(target, image => ImageInsertion::new(source, image.as_ref(), region, locate))?;
    if let Some(plan) = plan {
        with_storage!(target, image => plan.copy(source, image.as_mut()))?;
        Ok(true)
    } else {
        Ok(false)
    }
}

/// Writes pixel and/or mask arrays into a region of `attached` (`ia.putregion`).
#[allow(clippy::too_many_arguments)]
pub fn put_region(
    attached: &AttachedImage,
    pixels: Option<&PixelChunk>,
    pixelmask: Option<&ArrayD<bool>>,
    region: Option<SubimageRegion<'_>>,
    list: bool,
    usemask: bool,
    replicate: bool,
    history_command: Option<String>,
) -> ImageToolResult<Vec<ToolLogLine>> {
    let mut handle = attached.lock();
    if handle.read_only_file_type().is_some() {
        return Err(ImageToolError::new("This image is not writable"));
    }
    if let Some(pixels) = pixels {
        let complex = matches!(*handle, ImageHandle::Complex(_) | ImageHandle::DComplex(_));
        let input_complex = matches!(pixels, PixelChunk::Complex(_) | PixelChunk::DComplex(_));
        if input_complex != complex {
            return Err(ImageToolError::new(if complex {
                "Real values cannot be put in images with complex valued pixels"
            } else {
                "Complex values cannot be put in images with real valued pixels"
            }));
        }
    }
    validate_put_region_shapes(pixels.map(PixelChunk::shape), pixelmask.map(ArrayD::shape))?;
    attached.require_lock(&mut handle, LockType::Write)?;
    let fresh_mask_name = pixelmask.map(|_| {
        (0..)
            .map(|index| format!("mask{index}"))
            .find(|name| !has_region_or_mask(&handle, name))
            .expect("an unused mask name")
    });
    let mask_name = fresh_mask_name.as_deref().unwrap_or("");
    let mask = pixelmask.cloned();
    match &mut *handle {
        ImageHandle::Float(storage) => {
            let arr = pixels_as_f32(pixels)?;
            with_storage!(storage, image => put_image_region(image.as_mut(), arr, mask, region, mask_name, usemask, replicate))?;
        }
        ImageHandle::Double(storage) => {
            let arr = pixels_as_f64(pixels)?;
            with_storage!(storage, image => put_image_region(image.as_mut(), arr, mask, region, mask_name, usemask, replicate))?;
        }
        ImageHandle::Complex(storage) => {
            let arr = pixels_as_c32(pixels)?;
            with_storage!(storage, image => put_image_region(image.as_mut(), arr, mask, region, mask_name, usemask, replicate))?;
        }
        ImageHandle::DComplex(storage) => {
            let arr = pixels_as_c64(pixels)?;
            with_storage!(storage, image => put_image_region(image.as_mut(), arr, mask, region, mask_name, usemask, replicate))?;
        }
    }
    if let Some(cmd) = history_command {
        handle.record_history("putregion", cmd)?;
    }
    handle.save()?;
    attached.release_requested_lock(&mut handle)?;
    let logs = list
        .then(|| ToolLogLine::info("Wrote the selected image region"))
        .into_iter()
        .collect();
    Ok(logs)
}

fn pixels_as_f32(pixels: Option<&PixelChunk>) -> ImageToolResult<Option<ArrayD<f32>>> {
    match pixels {
        None => Ok(None),
        Some(PixelChunk::Float(a)) => Ok(Some(a.clone())),
        Some(PixelChunk::Double(a)) => Ok(Some(a.mapv(|v| v as f32))),
        _ => Err(ImageToolError::new("Expected real pixel array.")),
    }
}

fn pixels_as_f64(pixels: Option<&PixelChunk>) -> ImageToolResult<Option<ArrayD<f64>>> {
    match pixels {
        None => Ok(None),
        Some(PixelChunk::Double(a)) => Ok(Some(a.clone())),
        Some(PixelChunk::Float(a)) => Ok(Some(a.mapv(f64::from))),
        _ => Err(ImageToolError::new("Expected real pixel array.")),
    }
}

fn pixels_as_c32(pixels: Option<&PixelChunk>) -> ImageToolResult<Option<ArrayD<Complex32>>> {
    match pixels {
        None => Ok(None),
        Some(PixelChunk::Complex(a)) => Ok(Some(a.clone())),
        Some(PixelChunk::DComplex(a)) => {
            Ok(Some(a.mapv(|v| Complex32::new(v.re as f32, v.im as f32))))
        }
        _ => Err(ImageToolError::new("Expected complex pixel array.")),
    }
}

fn pixels_as_c64(pixels: Option<&PixelChunk>) -> ImageToolResult<Option<ArrayD<Complex64>>> {
    match pixels {
        None => Ok(None),
        Some(PixelChunk::DComplex(a)) => Ok(Some(a.clone())),
        Some(PixelChunk::Complex(a)) => {
            Ok(Some(a.mapv(|v| Complex64::new(v.re.into(), v.im.into()))))
        }
        _ => Err(ImageToolError::new("Expected complex pixel array.")),
    }
}

/// Replaces masked pixel values in `attached` (`ia.replacemaskedpixels`).
#[allow(clippy::too_many_arguments)]
pub fn replace_masked_pixels(
    attached: &AttachedImage,
    expression: &str,
    region: Option<SubimageRegion<'_>>,
    mask: &str,
    update: bool,
    list: bool,
    stretch: bool,
    history_command: Option<String>,
) -> ImageToolResult<(Vec<ToolLogLine>, bool)> {
    let mut image = attached.lock();
    image.check_single_precision("replacemaskedpixels")?;
    if expression.is_empty() {
        return Err(ImageToolError::new("You must specify an expression"));
    }
    if image.fits().is_some() {
        return Err(ImageToolError::new(
            "This image is not writable.  It is probably a reference or expression virtual image",
        ));
    }
    if let Some(SubimageRegion::Text(text)) = &region {
        with_pixels!(&*image, storage => with_storage!(storage, pixels => {
            validate_mask_replacement_region(pixels.as_ref(), SubimageRegion::Text(text))
        }))?;
    }
    attached.require_lock(&mut image, LockType::Write)?;
    let options = MaskReplacementOptions {
        expression,
        region,
        mask,
        update,
        stretch,
    };
    let applied = match &mut *image {
        ImageHandle::Float(storage) => with_storage!(storage, pixels => {
            replace_masked_real_pixels(pixels.as_mut(), options)
        }),
        ImageHandle::Complex(storage) => with_storage!(storage, pixels => {
            replace_masked_complex_pixels(pixels.as_mut(), options)
        }),
        _ => unreachable!("single precision checked"),
    }?;
    if matches!(applied, MaskReplacementOutcome::MissingNativeMask) {
        image.save()?;
        return Err(ImageToolError::new(
            "SubLattice::pixelMask - no pixelmask available",
        ));
    }
    let mut logs = Vec::new();
    if list {
        logs.push(ToolLogLine::info(format!(
            "Replaced values of masked pixels by {expression}"
        )));
    }
    if let Some(cmd) = history_command {
        image.record_history("replacemaskedpixels", cmd)?;
    }
    image.save()?;
    Ok((logs, true))
}

/// Fills pixels and/or pixel mask over a region with scalar values (`ia.set`).
#[allow(clippy::too_many_arguments)]
pub fn set_pixels_and_mask(
    attached: &AttachedImage,
    pixels_expr: Option<&str>,
    pixelmask: i64,
    region: Option<SubimageRegion<'_>>,
    list: bool,
    history_command: Option<String>,
) -> ImageToolResult<(bool, Vec<ToolLogLine>)> {
    let mut handle = attached.lock();
    handle.check_single_precision("set")?;
    let set_mask = pixelmask != -1;
    let has_pixels = pixels_expr.is_some();
    let ImageHandle::Float(storage) = &*handle else {
        return Err(ImageToolError::new("set only supports float-valued images"));
    };
    let text_selection = if let Some(SubimageRegion::Text(text)) = region
        && handle.fits().is_none()
    {
        Some(with_storage!(storage, image => {
            ImagePixelSelection::new(image.as_ref(), Some(SubimageRegion::Text(text)))
        })?)
    } else {
        None
    };
    if !has_pixels && !set_mask {
        return Ok((
            false,
            vec![ToolLogLine::warning(
                "You must specify at least either the pixels or the mask to set",
            )],
        ));
    }
    if let Some(kind) = handle.read_only_file_type() {
        if kind == "MIRIADImage" {
            return Err(ImageToolError::new(if has_pixels {
                "Failed AlwaysAssert to.isWritable()"
            } else {
                "SubLattice::pixelMask - no pixelmask available"
            }));
        }
        return Err(ImageToolError::new(if set_mask {
            format!("Cannot make requested mask for this image type which is {kind}")
        } else {
            format!("{kind} is not writable")
        }));
    }
    attached.require_lock(&mut handle, LockType::Write)?;
    let ImageHandle::Float(storage) = &*handle else {
        unreachable!("float attachment checked");
    };
    let default = with_storage!(storage, image => image.active_pixel_mask_name());
    let mask_name = if set_mask {
        if let Some(name) = default {
            Some(name)
        } else {
            let name = (0..)
                .map(|index| format!("mask{index}"))
                .find(|name| !has_region_or_mask(&handle, name))
                .expect("a fresh mask name");
            let mask = ArrayD::from_elem(IxDyn(handle.shape()), true);
            let ImageHandle::Float(storage) = &mut *handle else {
                unreachable!();
            };
            with_storage!(storage, image => image.put_mask(&name, &mask))?;
            with_storage!(storage, image => image.set_default_mask(&name))?;
            handle.save()?;
            Some(name)
        }
    } else {
        None
    };
    let mask = mask_name.as_deref().map(|name| (name, pixelmask > 0));
    let ImageHandle::Float(storage) = &mut *handle else {
        unreachable!();
    };
    let (start, shape) = with_storage!(storage, image => {
        let selection = match text_selection {
            Some(selection) => selection,
            None => ImagePixelSelection::new(image.as_ref(), region)?,
        };
        let value = pixels_expr.map(evaluate_pixel_expression).transpose()?;
        selection.apply(image.as_mut(), value, mask)
    })?;
    let mut logs = Vec::new();
    if list {
        logs.push(ToolLogLine::info(format!(
            "Selected region at {start:?} with shape {shape:?}"
        )));
    }
    if let Some(cmd) = history_command {
        handle.record_history("set", cmd)?;
    }
    handle.save()?;
    Ok((true, logs))
}
