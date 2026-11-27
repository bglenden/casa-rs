// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image concatenation (`ia.imageconcat`): virtual and materialized paged modes.

pub mod inputs;
pub mod owned_members;
pub mod relocation;
pub mod virtual_image;

use std::path::{Path, PathBuf};

use casa_core::RecordValue;
use casa_images::{
    ConcatInput, ConcatManifest, ConcatenationPlan, ImageError, ImageInfo, ImagePixel,
    ImagePixelType, MutableImageInterface, copy_concatenated_input,
};
use casa_tables::lexical_absolute;
use num_complex::{Complex32, Complex64};

use inputs::{
    ConcatInputEntry, beam_records, coordinate_record, merge_misc, protect_expression_sources,
};
use owned_members::ConcatMode;

use super::attachment::{ExternalPathCheck, ensure_outfile_does_not_alias_input};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels, with_storage};
use super::publish::{publish_directory, publish_image};

/// Request parameters for `ia.imageconcat`.
#[derive(Debug, Clone)]
pub struct ImageConcatRequest {
    /// Destination path (empty string creates an in-memory temporary image when `mode='paged'`/`relax=false`).
    pub outfile: String,
    /// Ordered list of input image paths.
    pub infiles: Vec<PathBuf>,
    /// Axis index (`-1` auto-detects the differing/spectral/stokes axis).
    pub axis: i64,
    /// Allow non-monotonic/non-contiguous coordinate concatenation.
    pub relax: bool,
    /// Temporarily close members after reads in virtual mode.
    pub tempclose: bool,
    /// Overwrite existing `outfile`.
    pub overwrite: bool,
    /// Concatenation storage policy (`"nomovevirtual"`, `"movevirtual"`, `"copyvirtual"`, `"paged"`).
    pub mode: String,
    /// Sort inputs along the concatenation axis if `true`.
    pub reorder: bool,
}

/// Concatenates `request.infiles` into a virtual or materialized image handle.
pub fn concatenate_images(
    request: &ImageConcatRequest,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(ImageHandle, Vec<ToolLogLine>)> {
    if request.infiles.is_empty() {
        return Err(ImageToolError::new(
            "imageconcat requires at least one input image.",
        ));
    }
    let mode = ConcatMode::parse(&request.mode)?;
    if !request.outfile.is_empty() {
        let out_path = Path::new(&request.outfile);
        for infile in &request.infiles {
            ensure_outfile_does_not_alias_input(out_path, infile)?;
        }
    } else if !matches!(mode, ConcatMode::PagedCopy) {
        return Err(ImageToolError::new(
            "Virtual imageconcat requires a non-empty outfile path.",
        ));
    }

    let mut opened = Vec::with_capacity(request.infiles.len());
    for path in &request.infiles {
        opened.push(ImageHandle::open_for_concatenation(path)?);
    }

    let pixel_type = opened[0].kind();
    for handle in &opened[1..] {
        if handle.kind() != pixel_type {
            return Err(ImageToolError::new(format!(
                "All concatenated images must have matching pixel types (expected {:?}, found {:?}).",
                pixel_type,
                handle.kind()
            )));
        }
    }

    let infos: Vec<ImageInfo> = opened
        .iter()
        .map(super::beams::image_info)
        .collect::<Result<_, _>>()?;
    let inputs: Vec<ConcatInput<'_>> = opened
        .iter()
        .zip(request.infiles.iter())
        .zip(infos.iter())
        .map(|((handle, path), info)| ConcatInput {
            path: path.as_path(),
            shape: handle.shape(),
            coordinates: handle.coordinates(),
            image_info: info,
            brightness_unit: handle.units(),
        })
        .collect();
    let plan = ConcatenationPlan::new(&inputs, request.axis, request.relax, request.reorder)?;
    let mut logs: Vec<ToolLogLine> = plan
        .warnings
        .iter()
        .cloned()
        .map(ToolLogLine::warning)
        .collect();

    if matches!(mode, ConcatMode::PagedCopy) {
        let history_cmd = format!(
            "ia.imageconcat(outfile={:?}, infiles={:?}, axis={}, relax={}, reorder={})",
            request.outfile, request.infiles, request.axis, request.relax, request.reorder
        );
        let handle = publish_image(
            &request.outfile,
            request.overwrite,
            is_external_open,
            |dest| {
                materialize_concat(
                    &plan,
                    &opened,
                    pixel_type,
                    dest,
                    record_history,
                    history_cmd,
                )
            },
        )?;
        logs.push(ToolLogLine::info(format!(
            "Concatenated {} images into shape {:?}",
            request.infiles.len(),
            plan.shape
        )));
        return Ok((handle, logs));
    }

    let outfile = PathBuf::from(&request.outfile);
    let abs_out = lexical_absolute(&outfile);
    let ordered_infiles: Vec<PathBuf> = plan
        .order
        .iter()
        .map(|&idx| request.infiles[idx].clone())
        .collect();
    let misc = opened[0].misc_info();
    drop(opened);

    publish_directory(&outfile, request.overwrite, is_external_open, |stage| {
        let moving = matches!(mode, ConcatMode::MoveVirtual);
        let members = owned_members::OwnedOperands::inspect(&ordered_infiles, &abs_out, moving)?;
        let entries: Vec<PathBuf> = match mode {
            ConcatMode::NoMoveVirtual | ConcatMode::PagedCopy => ordered_infiles.clone(),
            ConcatMode::CopyVirtual | ConcatMode::MoveVirtual => (0..ordered_infiles.len())
                .map(|index| members.destination(&abs_out, index))
                .collect(),
        };
        if !matches!(mode, ConcatMode::NoMoveVirtual | ConcatMode::PagedCopy) {
            let mut moved = owned_members::MemberMoves::default();
            let registry = super::attachment::registry();
            members.stage(&abs_out, stage, &registry, &mut moved, is_external_open)?;
            let _ = moved.finish(&abs_out);
        }
        let manifest = ConcatManifest::new(
            &abs_out,
            pixel_type,
            plan.axis,
            request.tempclose,
            entries,
            &misc,
        )?;
        manifest.write(stage)?;
        Ok(())
    })?;

    logs.push(ToolLogLine::info(format!(
        "Created virtual concatenation {} of shape {:?}",
        outfile.display(),
        plan.shape
    )));
    let handle = ImageHandle::open(&outfile)?;
    Ok((handle, logs))
}

/// Materializes a paged or temporary concatenation image with full metadata, beams, and mask merging.
pub fn build_paged_concat(
    source_paths: &[PathBuf],
    outfile: &str,
    options: &virtual_image::VirtualConcatOptions,
    dest: Option<&Path>,
    logs: &mut Vec<ToolLogLine>,
) -> ImageToolResult<ImageHandle> {
    let inputs = source_paths
        .iter()
        .map(|path| ConcatInputEntry::open(path, options.tempclose))
        .collect::<ImageToolResult<Vec<_>>>()?;
    if !outfile.is_empty() {
        protect_expression_sources(Path::new(outfile), &inputs)?;
    }
    let first_kind = inputs[0].kind;
    if inputs.iter().any(|input| input.kind != first_kind) {
        return Err(ImageToolError::new(
            "Concatenation of images of different data types is not supported",
        ));
    }
    let geometry: Vec<_> = inputs.iter().map(ConcatInputEntry::geometry).collect();
    let plan = ConcatenationPlan::new(&geometry, options.axis, options.relax, options.reorder)?;
    logs.extend(plan.warnings.iter().cloned().map(ToolLogLine::warning));
    let first = &inputs[plan.order[0]];
    let mask = inputs.iter().any(|input| input.has_mask).then_some("mask0");
    let mut output = match first.kind {
        ImagePixelType::Float32 => {
            Storage::create(plan.shape.clone(), plan.coordinates.clone(), dest, None)
                .map(ImageHandle::Float)
        }
        ImagePixelType::Float64 => {
            Storage::create(plan.shape.clone(), plan.coordinates.clone(), dest, None)
                .map(ImageHandle::Double)
        }
        ImagePixelType::Complex32 => {
            Storage::create(plan.shape.clone(), plan.coordinates.clone(), dest, None)
                .map(ImageHandle::Complex)
        }
        ImagePixelType::Complex64 => {
            Storage::create(plan.shape.clone(), plan.coordinates.clone(), dest, None)
                .map(ImageHandle::DComplex)
        }
    }?;
    output.set_coordinate_record(coordinate_record(first, &plan)?)?;
    let mut misc = RecordValue::default();
    let mut offset = 0;
    for &index in &plan.order {
        let entry = &inputs[index];
        let reopened;
        let image = if let Some(opened) = &entry.opened {
            opened
        } else {
            reopened = ImageHandle::open_for_concatenation(entry.path)?;
            &reopened
        };
        let same_revision = match &entry.revision {
            Some(revision) => revision.matches(&inputs::InputRevision::read(entry.path, image)?),
            None => true,
        };
        if !same_revision || image.shape() != entry.shape || image.kind() != entry.kind {
            return Err(ImageToolError::new(format!(
                "Image {} changed while it was being concatenated",
                entry.path.display()
            )));
        }
        match (image, &mut output) {
            (ImageHandle::Float(input), ImageHandle::Float(output)) => input.with_float_view(|input| {
                with_storage!(output, output => copy_concatenated_input(input, output.as_mut(), plan.axis, offset, mask))
            }),
            (ImageHandle::Double(input), ImageHandle::Double(output)) => {
                copy_native(input, output, plan.axis, offset, mask)
            }
            (ImageHandle::Complex(input), ImageHandle::Complex(output)) => {
                copy_native(input, output, plan.axis, offset, mask)
            }
            (ImageHandle::DComplex(input), ImageHandle::DComplex(output)) => {
                copy_native(input, output, plan.axis, offset, mask)
            }
            _ => unreachable!("all pixel types checked"),
        }?;
        offset += entry.shape[plan.axis];
        merge_misc(&mut misc, &entry.misc);
        with_pixels!(&mut output, storage => with_storage!(storage, image => image.append_history_entries(&entry.history)))?;
    }
    let beams = beam_records(&plan, &inputs)?;
    with_pixels!(&mut output, storage => with_storage!(storage, image => {
        image.set_units(&first.units)?;
        image.set_misc_info(misc)?;
        image.set_image_info_record(beams)?;
        if let Some(name) = mask {
            image.set_default_mask(name)?;
        }
        Ok::<_, ImageError>(())
    }))?;
    Ok(output)
}

fn copy_native<T: ImagePixel>(
    input: &Storage<T>,
    output: &mut Storage<T>,
    axis: usize,
    offset: usize,
    mask: Option<&str>,
) -> Result<(), ImageError> {
    with_storage!(input, input_image => with_storage!(output, output_image =>
        copy_concatenated_input(input_image.as_ref(), output_image.as_mut(), axis, offset, mask)
    ))
}

fn materialize_concat(
    plan: &ConcatenationPlan,
    opened: &[ImageHandle],
    pixel_type: ImagePixelType,
    destination: Option<&Path>,
    record_history: bool,
    history_command: String,
) -> ImageToolResult<ImageHandle> {
    let mut handle = match pixel_type {
        ImagePixelType::Float32 => materialize_float(plan, opened, destination)?,
        ImagePixelType::Float64 => {
            materialize_typed::<f64>(plan, opened, destination, ImageHandle::Double)?
        }
        ImagePixelType::Complex32 => {
            materialize_typed::<Complex32>(plan, opened, destination, ImageHandle::Complex)?
        }
        ImagePixelType::Complex64 => {
            materialize_typed::<Complex64>(plan, opened, destination, ImageHandle::DComplex)?
        }
    };
    let info_record = plan.image_info.to_record();
    handle.set_image_info_record(info_record)?;
    if let Some(first) = opened.first() {
        handle.set_units(first.units())?;
        handle.set_misc_info(first.misc_info())?;
    }
    if record_history {
        handle.record_history("imageconcat", history_command)?;
    }
    handle.save()?;
    Ok(handle)
}

fn materialize_float(
    plan: &ConcatenationPlan,
    opened: &[ImageHandle],
    destination: Option<&Path>,
) -> ImageToolResult<ImageHandle> {
    let mut output = Storage::<f32>::create(
        plan.shape.clone(),
        plan.coordinates.clone(),
        destination,
        None,
    )?;
    let mut offset = 0usize;
    for &idx in &plan.order {
        let member = &opened[idx];
        let slice_len = member.shape()[plan.axis];
        let dest_iface: &mut dyn MutableImageInterface<f32> = match &mut output {
            Storage::Paged(paged) => paged.as_mut(),
            Storage::Temp { image, .. } => image.as_mut(),
        };
        match member {
            ImageHandle::Float(storage) => {
                if let Some(fits) = member.fits() {
                    let meta = match storage {
                        Storage::Temp { image, .. } => image.as_ref(),
                        Storage::Paged(_) => unreachable!(),
                    };
                    copy_concatenated_input(
                        &fits.image_view(meta),
                        dest_iface,
                        plan.axis,
                        offset,
                        None,
                    )?;
                } else {
                    with_storage!(storage, src => {
                        copy_concatenated_input(src.as_ref(), dest_iface, plan.axis, offset, None)?;
                    });
                }
            }
            _ => return Err(ImageToolError::new("Mismatched concatenation pixel type")),
        }
        offset += slice_len;
    }
    Ok(ImageHandle::Float(output))
}

fn materialize_typed<T: ImagePixel>(
    plan: &ConcatenationPlan,
    opened: &[ImageHandle],
    destination: Option<&Path>,
    wrap: fn(Storage<T>) -> ImageHandle,
) -> ImageToolResult<ImageHandle> {
    let mut output = Storage::<T>::create(
        plan.shape.clone(),
        plan.coordinates.clone(),
        destination,
        None,
    )?;
    let mut offset = 0usize;
    for &idx in &plan.order {
        let member = &opened[idx];
        let slice_len = member.shape()[plan.axis];
        match &mut output {
            Storage::Paged(paged) => {
                copy_from_handle::<T>(member, paged.as_mut(), plan.axis, offset)?;
            }
            Storage::Temp { image, .. } => {
                copy_from_handle::<T>(member, image.as_mut(), plan.axis, offset)?;
            }
        }
        offset += slice_len;
    }
    Ok(wrap(output))
}

fn copy_from_handle<T: ImagePixel>(
    source: &ImageHandle,
    dest: &mut dyn MutableImageInterface<T>,
    axis: usize,
    offset: usize,
) -> ImageToolResult<()> {
    with_pixels!(source, storage => {
        with_storage!(storage, src => {
            let src_any: &dyn std::any::Any = src.as_ref();
            let src_typed = src_any
                .downcast_ref::<casa_images::PagedImage<T>>()
                .map(|p| p as &dyn casa_images::ImageInterface<T>)
                .or_else(|| {
                    src_any
                        .downcast_ref::<casa_images::TempImage<T>>()
                        .map(|t| t as &dyn casa_images::ImageInterface<T>)
                })
                .ok_or_else(|| ImageToolError::new("Mismatched concatenation pixel type"))?;
            copy_concatenated_input(src_typed, dest, axis, offset, None)?;
            Ok(())
        })
    })
}
