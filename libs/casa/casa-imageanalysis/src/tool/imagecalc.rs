// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel expression evaluation and image creation (`ia.imagecalc`).

use std::path::{Path, PathBuf};

use casa_core::Complex32;
use casa_images::{ImageError, ImageInterface, PagedImage, TempImage};

use crate::analysis::{
    ImageToolExpressionInputs, ImageToolPixelExpression, calculate_complex_pixels,
    calculate_real_pixels, materialize_image_tool_expression,
};

use super::attachment::{ExternalPathCheck, ensure_outfile_does_not_alias_input};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage};
use super::publish::publish_image;

/// Evaluates a LEL pixel expression (`ia.imagecalc`) into a paged or temporary [`ImageHandle`].
pub fn image_calc(
    outfile: &str,
    pixels: &str,
    overwrite: bool,
    imagemd: &str,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(ImageHandle, Vec<ToolLogLine>)> {
    if pixels.trim().is_empty() {
        return Err(ImageToolError::new(
            "imagecalc requires a non-empty pixel expression.",
        ));
    }

    let inputs = ImageToolExpressionInputs::resolve(pixels)?;
    let input_paths: Vec<PathBuf> = inputs.paths().map(Path::to_path_buf).collect();
    if !outfile.is_empty() {
        let out_path = Path::new(outfile);
        for src in &input_paths {
            ensure_outfile_does_not_alias_input(out_path, src)?;
        }
    }

    let (compiled, inferred_single) = inputs.compile_lazy_inferred()?;
    let md_handle = if !imagemd.trim().is_empty() {
        Some(ImageHandle::open_read(Path::new(imagemd))?)
    } else {
        None
    };

    let mut logs = Vec::new();
    let handle = publish_image(outfile, overwrite, is_external_open, |dest| {
        let mut h = match compiled {
            ImageToolPixelExpression::Real(expr) => {
                if inferred_single {
                    let shape = expr.shape().to_vec();
                    let coords = expr.coordinates().clone();
                    let record = coords.to_record();
                    let units = expr.units().to_owned();
                    let info = expr.image_info()?.to_record();
                    let misc = expr.misc_info();
                    let mask = expr
                        .has_pixel_mask()
                        .then(|| expr.get_mask_slice(&vec![0; shape.len()], &shape))
                        .transpose()?
                        .flatten();
                    match dest {
                        Some(path) => {
                            let mut paged = PagedImage::<f32>::create(shape.clone(), coords, path)?;
                            calculate_real_pixels(&mut paged, &expr)?;
                            paged.set_units(units)?;
                            paged.set_image_info_record(info)?;
                            paged.set_misc_info(misc)?;
                            if let Some(m) = &mask {
                                paged.put_mask_slice("mask0", m, &vec![0; shape.len()])?;
                                paged.set_default_mask("mask0")?;
                            }
                            ImageHandle::Float(Storage::Paged(Box::new(paged)))
                        }
                        None => {
                            let mut temp = TempImage::<f32>::new(
                                shape.clone(),
                                coords,
                                casa_images::TempStoragePolicy::Memory,
                            )?;
                            calculate_real_pixels(&mut temp, &expr)?;
                            temp.set_units(units)?;
                            temp.set_image_info_record(info)?;
                            temp.set_misc_info(misc)?;
                            if let Some(m) = &mask {
                                temp.put_mask_slice("mask0", m, &vec![0; shape.len()])?;
                                temp.set_default_mask("mask0")?;
                            }
                            ImageHandle::Float(Storage::Temp {
                                image: Box::new(temp),
                                record,
                                fits: None,
                            })
                        }
                    }
                } else {
                    match dest {
                        Some(path) => {
                            let paged = materialize_image_tool_expression(&expr, path)?;
                            ImageHandle::Double(Storage::Paged(Box::new(paged)))
                        }
                        None => {
                            let record = expr.coordinates().to_record();
                            let temp = TempImage::from_expression(expr)?;
                            ImageHandle::Double(Storage::Temp {
                                image: Box::new(temp),
                                record,
                                fits: None,
                            })
                        }
                    }
                }
            }
            ImageToolPixelExpression::Complex(expr) => {
                if inferred_single {
                    let shape = expr.shape().to_vec();
                    let coords = expr.coordinates().clone();
                    let record = coords.to_record();
                    let units = expr.units().to_owned();
                    let info = expr.image_info()?.to_record();
                    let misc = expr.misc_info();
                    let mask = expr
                        .has_pixel_mask()
                        .then(|| expr.get_mask_slice(&vec![0; shape.len()], &shape))
                        .transpose()?
                        .flatten();
                    match dest {
                        Some(path) => {
                            let mut paged =
                                PagedImage::<Complex32>::create(shape.clone(), coords, path)?;
                            calculate_complex_pixels(&mut paged, &expr)?;
                            paged.set_units(units)?;
                            paged.set_image_info_record(info)?;
                            paged.set_misc_info(misc)?;
                            if let Some(m) = &mask {
                                paged.put_mask_slice("mask0", m, &vec![0; shape.len()])?;
                                paged.set_default_mask("mask0")?;
                            }
                            ImageHandle::Complex(Storage::Paged(Box::new(paged)))
                        }
                        None => {
                            let mut temp = TempImage::<Complex32>::new(
                                shape.clone(),
                                coords,
                                casa_images::TempStoragePolicy::Memory,
                            )?;
                            calculate_complex_pixels(&mut temp, &expr)?;
                            temp.set_units(units)?;
                            temp.set_image_info_record(info)?;
                            temp.set_misc_info(misc)?;
                            if let Some(m) = &mask {
                                temp.put_mask_slice("mask0", m, &vec![0; shape.len()])?;
                                temp.set_default_mask("mask0")?;
                            }
                            ImageHandle::Complex(Storage::Temp {
                                image: Box::new(temp),
                                record,
                                fits: None,
                            })
                        }
                    }
                } else {
                    match dest {
                        Some(path) => {
                            let paged = materialize_image_tool_expression(&expr, path)?;
                            ImageHandle::DComplex(Storage::Paged(Box::new(paged)))
                        }
                        None => {
                            let record = expr.coordinates().to_record();
                            let temp = TempImage::from_expression(expr)?;
                            ImageHandle::DComplex(Storage::Temp {
                                image: Box::new(temp),
                                record,
                                fits: None,
                            })
                        }
                    }
                }
            }
            ImageToolPixelExpression::Boolean(_) => {
                return Err(ImageToolError::from(ImageError::CasaSelection(
                    "imagecalc requires a numeric image expression.".into(),
                )));
            }
        };

        if let Some(md) = &md_handle {
            h.set_coordinate_record(md.coordinate_record())?;
            h.set_units(md.units())?;
            h.set_misc_info(md.misc_info())?;
            let info_rec = super::beams::image_info(md)?.to_record();
            h.set_image_info_record(info_rec)?;
        }
        if record_history {
            h.record_history(
                "imagecalc",
                format!(
                    "ia.imagecalc(outfile={outfile:?}, pixels={pixels:?}, overwrite={overwrite}, imagemd={imagemd:?})"
                ),
            )?;
        }
        h.save()?;
        logs.push(ToolLogLine::info(format!(
            "Evaluated expression '{pixels}' into shape {:?}",
            h.shape()
        )));
        Ok(h)
    })?;

    Ok((handle, logs))
}
