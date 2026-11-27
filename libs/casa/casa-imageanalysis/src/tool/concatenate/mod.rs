// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image concatenation (`ia.imageconcat`): virtual and materialized paged modes.

pub(crate) mod inputs;
pub(crate) mod owned_members;
pub(crate) mod relocation;
pub(crate) mod virtual_image;

use std::path::{Path, PathBuf};

use casa_core::RecordValue;
use casa_images::{
    ConcatenationPlan, ImageError, ImagePixel, ImagePixelType, copy_concatenated_input,
};

use inputs::{
    ConcatInputEntry, beam_records, coordinate_record, merge_misc, protect_expression_sources,
};

use super::attachment::{
    self, AttachedImage, ExternalPathCheck, ensure_outfile_does_not_alias_input,
};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_pixels, with_storage};
use super::publish::{publish_image, publish_owned_virtual_concat, publish_virtual_concat};

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

/// Concatenates input images in paged or virtual (`nomovevirtual`, `copyvirtual`, `movevirtual`)
/// mode, returning the registered [`AttachedImage`] and tool logs (`ia.imageconcat`).
#[allow(clippy::too_many_arguments)]
pub fn concatenate_casa_images(
    outfile: &str,
    expanded: &str,
    paths: &[PathBuf],
    files_history: &str,
    axis: i64,
    relax: bool,
    tempclose: bool,
    overwrite: bool,
    reorder: bool,
    mode: &str,
    history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(AttachedImage, Vec<ToolLogLine>)> {
    if paths.len() < 2 {
        return Err(ImageToolError::new(
            "Exception: You must provide at least two images to concatentate.",
        ));
    }
    if !outfile.is_empty() && Path::new(outfile).exists() && !overwrite {
        return Err(ImageToolError::new(format!(
            "Exception: {outfile} exists and overwrite is false."
        )));
    }
    let mode_letter = mode.as_bytes().first().map(u8::to_ascii_lowercase);
    match mode_letter {
        Some(b'p') => {}
        Some(b'n' | b'c' | b'm') if outfile.is_empty() => {
            return Err(ImageToolError::new(
                "Exception: An empty outname can be used only if mode == PAGED.",
            ));
        }
        Some(b'n' | b'c' | b'm') => {}
        _ => return Err(ImageToolError::new(format!("Unsupported mode {mode}"))),
    }
    if !expanded.is_empty() {
        let destination = Path::new(expanded);
        let existing = destination
            .exists()
            .then(|| attachment::key(destination))
            .transpose()?;
        for path in paths {
            if let Some(existing) = &existing
                && &attachment::key(path)? == existing
            {
                return Err(ImageToolError::new(format!(
                    "Invalid Table operation: SetupNewTable {} is already opened (is in the table cache)",
                    existing.display()
                )));
            }
            ensure_outfile_does_not_alias_input(destination, path)?;
        }
    }

    let mut logs = Vec::new();
    let options = virtual_image::VirtualConcatOptions {
        axis,
        relax,
        tempclose,
        reorder,
    };
    let record_history = |output: &mut ImageHandle| -> ImageToolResult<()> {
        let name = if outfile.is_empty() {
            "Temporary_Image".into()
        } else {
            std::path::absolute(outfile)?.display().to_string()
        };
        output
            .append_history_at(
                "::image::imageconcat",
                &[
                    format!("Ran ia.imageconcat on {name}"),
                    format!(
                        "ia.imageconcat(outfile='{outfile}', infiles={files_history}, axis={axis}, relax={relax}, tempclose={tempclose}, overwrite={overwrite}, reorder={reorder}, mode='{mode}')"
                    ),
                ],
            )
    };

    let attached = if mode_letter != Some(b'p') {
        let attached = if mode_letter == Some(b'n') {
            publish_virtual_concat(
                outfile,
                expanded,
                overwrite,
                is_external_open,
                |final_path, stage| {
                    virtual_image::stage_nomove(&options, paths, final_path, stage, &mut logs)
                },
            )?
        } else {
            let (attached, cleanup_warnings) = publish_owned_virtual_concat(
                outfile,
                expanded,
                overwrite,
                is_external_open,
                |final_path, stage| {
                    virtual_image::stage_owned(
                        &options,
                        paths,
                        final_path,
                        stage,
                        mode_letter == Some(b'm'),
                        &mut logs,
                    )
                },
            )?;
            logs.extend(cleanup_warnings.into_iter().map(ToolLogLine::warning));
            attached
        };
        if history {
            record_history(&mut attached.lock())?;
        }
        attached
    } else {
        publish_image(outfile, overwrite, is_external_open, |path| {
            let mut output = build_paged_concat(paths, outfile, &options, path, &mut logs)?;
            if history {
                record_history(&mut output)?;
            }
            output.save()?;
            Ok(output)
        })?
    };
    Ok((attached, logs))
}
