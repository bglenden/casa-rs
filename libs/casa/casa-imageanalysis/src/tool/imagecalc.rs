// SPDX-License-Identifier: LGPL-3.0-or-later

//! Pixel expression evaluation and image creation (`ia.imagecalc`).

use std::path::{Path, PathBuf};

use casa_coordinates::{CoordinateSystem, CoordinateType, StokesCoordinate, StokesType};
use casa_core::{Complex32, RecordValue};
use casa_images::{
    ImageError, ImageExprValue, ImageExpression, ImageInfo, ImageInterface, ImagePixelType,
    PagedImage, TempImage,
};

use crate::analysis::{
    ImageToolExpressionInputs, ImageToolPixelExpression, calculate_complex_pixels,
    calculate_real_pixels, materialize_image_tool_expression,
};

use super::attachment::{AttachedImage, ExternalPathCheck, ensure_outfile_does_not_alias_input};
use super::beams::{image_info, image_info_record};
use super::concatenate::virtual_image;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels, with_storage};
use super::publish::publish_image;
use super::saved_expression::{self, StagedOutput, check_expression_geometry};

/// Evaluates a LEL pixel expression (`ia.imagecalc`) into a paged or temporary [`AttachedImage`].
pub fn image_calc(
    outfile: &str,
    pixels: &str,
    overwrite: bool,
    imagemd: &str,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
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

fn storage_from_expression<T: ImageExprValue>(
    expression: ImageExpression<T>,
    path: Option<&Path>,
) -> ImageToolResult<Storage<T>> {
    if let Some(path) = path {
        let image = materialize_image_tool_expression(&expression, path)?;
        return Ok(Storage::Paged(Box::new(image)));
    }
    let record = expression.coordinate_record();
    let image = TempImage::from_expression(expression)?;
    Ok(Storage::Temp {
        image: Box::new(image),
        record,
        fits: None,
    })
}

struct CalcMetadata {
    coordinates: Option<RecordValue>,
    image_info: RecordValue,
    misc_info: RecordValue,
    units: String,
}

impl CalcMetadata {
    fn read(name: &str) -> ImageToolResult<Self> {
        let path = Path::new(name);
        if !path.exists() {
            return Err(ImageToolError::new(format!(
                "Exception: Cannot access {name} so cannot copy its metadata to output image."
            )));
        }
        if casa_images::expr_file::is_image_expr(path) {
            let source = saved_expression::open(path)?;
            return Self::from_image(&source.lock());
        }
        Self::from_image(&ImageHandle::open(path)?)
    }

    fn read_staged(name: &Path, output: &StagedOutput<'_>) -> ImageToolResult<Self> {
        let entry = casa_tables::lexical_absolute(name);
        let referent = name.canonicalize()?;
        saved_expression::check_output_dependencies(output.target, &[entry, referent], false)?;
        let image = if casa_images::is_image_concat(name) {
            virtual_image::open_metadata(name, output)
        } else {
            saved_expression::open_metadata(name, output)
        }?;
        Self::from_image(&image)
    }

    fn from_image(image: &ImageHandle) -> ImageToolResult<Self> {
        if !matches!(
            image.kind(),
            ImagePixelType::Float32 | ImagePixelType::Complex32
        ) {
            return Err(ImageToolError::new(
                "Exception: Unsupported image pixel data type.",
            ));
        }
        let info = match image_info_record(image) {
            Some(record) => record.clone(),
            None => image_info(image)?.to_record(),
        };
        let (misc_info, units) = with_pixels!(
            image,
            storage => with_storage!(storage, image => (image.misc_info(), image.units().to_owned()))
        );
        Ok(Self {
            coordinates: Some(image.coordinate_record()),
            image_info: info,
            misc_info,
            units,
        })
    }

    fn new_output() -> Self {
        Self {
            coordinates: None,
            image_info: ImageInfo::default().to_record(),
            misc_info: RecordValue::default(),
            units: String::new(),
        }
    }

    fn apply(self, image: &mut ImageHandle) -> ImageToolResult<()> {
        if let Some(coordinates) = self.coordinates {
            let system = CoordinateSystem::from_record(&coordinates)?;
            if system.n_pixel_axes() == image.shape().len() {
                image.set_coordinate_record(coordinates)?;
            }
        }
        with_pixels!(image, storage => with_storage!(storage, image => {
            image.set_units(self.units)?;
            image.set_misc_info(self.misc_info)?;
            image.set_image_info_record(self.image_info)
        }))?;
        Ok(())
    }
}

fn calls_function(expression: &str, function: &str) -> bool {
    let mut chars = expression.char_indices().peekable();
    while let Some((start, ch)) = chars.next() {
        if matches!(ch, '\'' | '"') {
            for (_, next) in chars.by_ref() {
                if next == ch {
                    break;
                }
            }
        } else if ch.is_ascii_alphabetic() || ch == '_' {
            while chars
                .peek()
                .is_some_and(|(_, ch)| ch.is_alphanumeric() || matches!(ch, '_' | '.' | '/' | '~'))
            {
                chars.next();
            }
            let end = chars.peek().map_or(expression.len(), |(index, _)| *index);
            if expression[start..end].eq_ignore_ascii_case(function)
                && expression[end..].trim_start().starts_with('(')
            {
                return true;
            }
        }
    }
    false
}

fn apply_expression_metadata(image: &mut ImageHandle, expression: &str) -> ImageToolResult<()> {
    let units = if calls_function(expression, "spectralindex") {
        ""
    } else if calls_function(expression, "pa") {
        let mut system = CoordinateSystem::from_record(&image.coordinate_record())?;
        let index = system.find_coordinate(CoordinateType::Stokes).ok_or_else(|| {
            ImageToolError::new(
                "Failed AlwaysAssert which < nCoordinates() && newCoordinate.nPixelAxes() == coordinates_p[which]->nPixelAxes() && newCoordinate.nWorldAxes() == coordinates_p[which]->nWorldAxes()",
            )
        })?;
        system.replace_coordinate(index, StokesCoordinate::new(vec![StokesType::Pangle]));
        image.set_coordinate_record(system.to_record())?;
        "deg"
    } else {
        return Ok(());
    };
    with_pixels!(image, storage => with_storage!(storage, image => image.set_units(units)))?;
    Ok(())
}

fn same_file_path(first: &Path, second: &Path) -> bool {
    first == second
        || path_identity(first)
            .zip(path_identity(second))
            .is_some_and(|(first, second)| first == second)
}

fn path_identity(path: &Path) -> Option<PathBuf> {
    path.canonicalize().ok().or_else(|| {
        let parent = path.parent()?;
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        Some(parent.canonicalize().ok()?.join(path.file_name()?))
    })
}

/// Evaluates a LEL pixel expression matching full CASA `ia.imagecalc` precision/metadata/saved-expression semantics.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_image_calc_casa(
    outfile: &str,
    pixels: &str,
    overwrite: bool,
    imagemd: &str,
    prec: &str,
    history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    if pixels.is_empty() {
        return Err(ImageToolError::new(
            "Exception: You must provide an expression using the pixels parameter.",
        ));
    }
    let as_float = match prec.as_bytes().first().map(u8::to_ascii_lowercase) {
        Some(b'f') => true,
        Some(b'd') => false,
        _ => {
            return Err(ImageToolError::new(
                "Exception: Unsupported value for type, it must be 'float' or 'double'.",
            ));
        }
    };
    let inputs = ImageToolExpressionInputs::resolve(pixels)?;
    let has_sources = inputs.paths().next().is_some();
    let uses_bound_sources = saved_expression::has_manifest_operands(&inputs);
    let (expression, retained, paths) = if uses_bound_sources {
        saved_expression::compile_inline(&inputs, outfile.is_empty())?
    } else {
        let retained = AttachedImage::open_expression_inputs(inputs.paths())?;
        let expression = if outfile.is_empty() {
            inputs.compile_lazy()
        } else {
            inputs.compile()
        }?;
        (
            expression,
            retained,
            inputs.paths().map(Path::to_path_buf).collect(),
        )
    };
    if !outfile.is_empty() && !overwrite && Path::new(outfile).symlink_metadata().is_ok() {
        return Err(ImageToolError::new(format!(
            "Exception: File {outfile} exists, and the user does not want to remove it.."
        )));
    }
    check_expression_geometry(&expression, has_sources)?;
    if !outfile.is_empty() {
        let output = Path::new(outfile);
        let overlap_error = || {
            ImageToolError::new(
                "Exception: Caught TableError. This often means the table you are trying to overwrite has been opened by another method and so cannot be overwritten at this time. Try closing it and rerunning.",
            )
        };
        if uses_bound_sources {
            saved_expression::check_output_dependencies(output, &paths, false)
                .map_err(|_| overlap_error())?;
        } else {
            for input in &paths {
                ensure_outfile_does_not_alias_input(output, input).map_err(|_| overlap_error())?;
            }
        }
    }
    let metadata_path = Path::new(imagemd);
    let output_path = Path::new(outfile);
    let same_output = !outfile.is_empty() && same_file_path(output_path, metadata_path);
    let staged_metadata = !outfile.is_empty()
        && !same_output
        && (casa_images::expr_file::is_image_expr(metadata_path)
            || casa_images::is_image_concat(metadata_path));
    let metadata = if imagemd.is_empty() || staged_metadata {
        None
    } else if same_output {
        if !as_float {
            return Err(ImageToolError::new(
                "Exception: Unsupported image pixel data type.",
            ));
        }
        Some(CalcMetadata::new_output())
    } else {
        Some(CalcMetadata::read(imagemd)?)
    };
    let mut result = publish_image(outfile, overwrite, is_external_open, |path| {
        let mut output = match expression {
            ImageToolPixelExpression::Real(expression) if as_float => ImageHandle::Float(
                storage_from_expression(expression.convert_pixels(|value| value as f32)?, path)?,
            ),
            ImageToolPixelExpression::Real(expression) => {
                ImageHandle::Double(storage_from_expression(expression, path)?)
            }
            ImageToolPixelExpression::Complex(expression) if as_float => {
                ImageHandle::Complex(storage_from_expression(
                    expression
                        .convert_pixels(|value| Complex32::new(value.re as f32, value.im as f32))?,
                    path,
                )?)
            }
            ImageToolPixelExpression::Complex(expression) => {
                ImageHandle::DComplex(storage_from_expression(expression, path)?)
            }
            ImageToolPixelExpression::Boolean(_) => unreachable!("numeric expression checked"),
        };
        if staged_metadata {
            let staged = StagedOutput::new(output_path, path.expect("named staged image"))?;
            CalcMetadata::read_staged(metadata_path, &staged)?.apply(&mut output)?;
        } else if let Some(metadata) = metadata {
            metadata.apply(&mut output)?;
        }
        apply_expression_metadata(&mut output, pixels)?;
        if history {
            let [outfile, pixels, imagemd] = [outfile, pixels, imagemd]
                .map(|value| if value.len() > 300 { "..." } else { value });
            output.record_history(
                "imagecalc",
                format!(
                    "ia.imagecalc(outfile=\"{outfile}\", pixels=\"{pixels}\", overwrite={overwrite}, imagemd=\"{imagemd}\")"
                ),
            )?;
        }
        output.save()?;
        Ok(output)
    })?;
    if outfile.is_empty() {
        result.retain_expression_inputs(retained, paths);
    }
    Ok(result)
}
