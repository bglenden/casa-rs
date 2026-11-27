// SPDX-License-Identifier: LGPL-3.0-or-later

//! Shared execution helpers for image analysis product wrappers (`ops_*` modules).

use std::path::Path;

use casa_coordinates::CoordinateSystem;
use casa_core::RecordValue;
use casa_images::{
    ImageHistoryRow, ImageInterface, ImagePixel, PagedImage, TempImage, fits_read::FitsToolImport,
};
use ndarray::ArrayD;

use crate::analysis::SubimageRegion;
use crate::analysis::imsubimage::ensure_artifacts_do_not_overlap;

use super::attachment::{AttachedImage, ExternalPathCheck, ensure_outfile_does_not_alias_input};
use super::beam_view::BeamView;
use super::beams::subset_beam_record;
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_storage};
use super::publish::publish_image;

/// Checks that non-empty `outfile` does not alias `attached` or any concatenation operand.
pub fn check_output_aliasing(attached: &AttachedImage, outfile: &str) -> ImageToolResult<()> {
    if outfile.is_empty() {
        return Ok(());
    }
    let out = Path::new(outfile);
    let image = attached.lock();
    check_handle_aliasing(&image, attached, out, None)
}

/// Validates output path overwrite precondition before staging work.
pub fn check_output_overwrite(outfile: &str, overwrite: bool) -> ImageToolResult<()> {
    if !outfile.is_empty() && !overwrite && Path::new(outfile).symlink_metadata().is_ok() {
        return Err(ImageToolError::new(format!(
            "Replaceable flag is false and there is already a file or directory named {outfile} so the output image will not be written"
        )));
    }
    Ok(())
}

/// Checks `outfile` aliasing against `handle`, `attached.operand_paths()`, and an optional second handle.
pub fn check_handle_aliasing(
    handle: &ImageHandle,
    attached: &AttachedImage,
    outfile: &Path,
    extra: Option<&ImageHandle>,
) -> ImageToolResult<()> {
    for src in handle
        .path()
        .into_iter()
        .chain(extra.and_then(ImageHandle::path))
        .chain(
            attached
                .operand_paths()
                .iter()
                .map(std::path::PathBuf::as_path),
        )
    {
        ensure_outfile_does_not_alias_input(outfile, src)?;
    }
    Ok(())
}

/// Checks `outfile` against any external table path referenced by `region`.
pub fn check_region_overlap(
    outfile: &str,
    region: Option<SubimageRegion<'_>>,
) -> ImageToolResult<()> {
    if outfile.is_empty() {
        return Ok(());
    }
    if let Some(ext) = region.and_then(SubimageRegion::external_path) {
        ensure_artifacts_do_not_overlap(Path::new(outfile), ext)?;
    }
    Ok(())
}

/// Applies either multi-plane beam subsetting or single-beam copy into `metadata` and stores it on `output`.
pub fn apply_beam_subset<T: ImagePixel>(
    output: &mut Storage<T>,
    mut metadata: RecordValue,
    beams: &BeamView,
    channels: &[usize],
    polarizations: &[usize],
) -> ImageToolResult<()> {
    let source_beams = beams.beam_set()?;
    if source_beams.is_multi() {
        subset_beam_record(
            &mut metadata,
            Some(beams.record()),
            &source_beams,
            channels,
            polarizations,
        )?;
    } else {
        beams.apply_to(&mut metadata);
    }
    with_storage!(output, img => img.set_image_info_record(metadata))?;
    Ok(())
}

/// Sets `coords` on a [`Storage<T>`] product when present.
pub fn set_storage_coordinate_record<T: ImagePixel>(
    output: &mut Storage<T>,
    record: Option<RecordValue>,
) -> ImageToolResult<()> {
    if let Some(record) = record {
        match output {
            Storage::Paged(image) => image.set_coordinate_record(record)?,
            Storage::Temp { record: slot, .. } => *slot = record,
        }
    }
    Ok(())
}

/// Writes `mask` to a paged (`mask0` default) or temporary anonymous pixel mask.
pub fn set_product_mask<T: ImagePixel>(
    output: &mut Storage<T>,
    mask: ArrayD<bool>,
) -> ImageToolResult<()> {
    match output {
        Storage::Paged(image) => {
            image.put_mask("mask0", &mask)?;
            image.set_default_mask("mask0")?;
        }
        Storage::Temp { image, .. } => image.set_anonymous_mask(mask)?,
    }
    Ok(())
}

/// Copies inherited history entries, records `method` history if `command` is `Some`,
/// appends any `extra` history section, and saves `output`.
pub fn finalize_output_history(
    output: &mut ImageHandle,
    inherited: &[ImageHistoryRow],
    method: &str,
    command: Option<String>,
    extra: Option<(&str, &[String])>,
) -> ImageToolResult<()> {
    if !inherited.is_empty() {
        output.append_history_entries(inherited)?;
    }
    if let Some(cmd) = command {
        output.record_history(method, cmd)?;
    }
    if let Some((origin, lines)) = extra {
        output.append_history_at(origin, lines)?;
    }
    output.save()?;
    Ok(())
}

/// Executes `f` with a read-locked `f32` [`ImageInterface`] view (supporting FITS, MIRIAD,
/// ComponentList, SavedExpr, VirtualConcat, Temp, and Paged images uniformly).
pub fn with_float_image_view<R>(
    attached: &AttachedImage,
    f: impl FnOnce(&dyn ImageInterface<f32>, &ImageHandle) -> ImageToolResult<R>,
) -> ImageToolResult<R> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    match &*image {
        ImageHandle::Float(storage) => {
            if let Some(fits) = image.fits() {
                let metadata = match storage {
                    Storage::Temp { image: temp, .. } => temp.as_ref(),
                    Storage::Paged(_) => unreachable!(),
                };
                let view = fits.image_view(metadata);
                f(&view, &image)
            } else {
                with_storage!(storage, img => f(img.as_ref(), &image))
            }
        }
        _ => Err(ImageToolError::new(
            "Operation requires a single-precision real (float) image.",
        )),
    }
}

/// Materializes an existing [`TempImage<T>`] product into a paged or temporary [`ImageHandle`]
/// using [`publish_image`], copying metadata and history cleanly.
#[allow(clippy::too_many_arguments)]
pub fn publish_temp_product<T: ImagePixel>(
    source: &TempImage<T>,
    outfile: &str,
    overwrite: bool,
    coord_record: Option<casa_core::RecordValue>,
    history_entries: &[ImageHistoryRow],
    record_history: Option<(&str, String)>,
    wrap: fn(Storage<T>) -> ImageHandle,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<ImageHandle> {
    publish_image(outfile, overwrite, is_external_open, |dest| {
        let mut storage = match dest {
            Some(path) => {
                let mut paged = PagedImage::create(
                    source.shape().to_vec(),
                    source.coordinates().clone(),
                    path,
                )?;
                super::create::copy_typed_storage(source, &mut paged)?;
                paged.set_units(source.units())?;
                paged.set_misc_info(source.misc_info())?;
                if let Some(info_rec) = source.image_info_record() {
                    paged.set_image_info_record(info_rec.clone())?;
                }
                Storage::Paged(Box::new(paged))
            }
            None => {
                let rec = coord_record
                    .clone()
                    .unwrap_or_else(|| source.coordinates().to_record());
                let mut temp = TempImage::new(
                    source.shape().to_vec(),
                    source.coordinates().clone(),
                    casa_images::TempStoragePolicy::Memory,
                )?;
                super::create::copy_typed_storage(source, &mut temp)?;
                temp.set_units(source.units())?;
                temp.set_misc_info(source.misc_info())?;
                if let Some(info_rec) = source.image_info_record() {
                    temp.set_image_info_record(info_rec.clone())?;
                }
                Storage::Temp {
                    image: Box::new(temp),
                    record: rec,
                    fits: None,
                }
            }
        };
        if let Some(rec) = &coord_record {
            match &mut storage {
                Storage::Paged(p) => p.set_coordinate_record(rec.clone())?,
                Storage::Temp { record, .. } => *record = rec.clone(),
            }
        }
        let mut handle = wrap(storage);
        if !history_entries.is_empty() {
            handle.append_history_entries(history_entries)?;
        }
        if let Some((method, cmd)) = &record_history {
            handle.record_history(method, cmd.clone())?;
        }
        handle.save()?;
        Ok(handle)
    })
}

/// Validates and opens an auxiliary FITS/native image path as an [`ImageHandle`] for read.
pub fn open_aux_image(path: &Path) -> ImageToolResult<ImageHandle> {
    if path.is_file() {
        return Ok(ImageHandle::open_fits(
            path,
            FitsToolImport::open_attachment(path)?,
        )?);
    }
    Ok(ImageHandle::open_read(path)?)
}

/// Returns the default pixel coordinate system for an attached image.
pub fn image_coordinates(attached: &AttachedImage) -> CoordinateSystem {
    attached.lock().coordinates().clone()
}
