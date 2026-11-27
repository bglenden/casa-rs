// SPDX-License-Identifier: LGPL-3.0-or-later

//! FITS export and FITS header inspection (`ia.tofits`, `ia.fitsheader`).

use std::path::{Path, PathBuf};

use casa_core::RecordValue;
use casa_images::fits_read::FitsToolImport;
use casa_images::image_history_entries;

use crate::analysis::{
    ExportFitsRequest, ImageFitsExportOptions, export_attached_fits, image_fits_header_with_beams,
    validate_attached_fits_target,
};

use super::attachment::{AttachedImage, ExternalPathCheck};
use super::error::{ImageToolError, ImageToolResult};
use super::handle::{ImageHandle, Storage, with_storage};
use super::publish::publish_image;

/// Result of `ia.fitsheader`: formatted header string and key-value record.
#[derive(Debug, Clone)]
pub enum FitsHeaderOutput {
    /// Text string of FITS cards (`retstr=True`).
    CardString(String),
    /// Parsed header key-value record (`retstr=False`).
    Record(RecordValue),
}

/// Exports `attached` to a FITS file matching CASA's `ia.tofits`.
#[allow(clippy::too_many_arguments)]
pub fn export_attached_to_fits(
    attached: &AttachedImage,
    outfile: &str,
    velocity: bool,
    optical: bool,
    bitpix: i32,
    minpix: f64,
    maxpix: f64,
    overwrite: bool,
    dropdeg: bool,
    dropstokes: bool,
    stokeslast: bool,
    history: bool,
    options: ImageFitsExportOptions<'_>,
) -> ImageToolResult<bool> {
    if outfile.is_empty() {
        return Err(ImageToolError::new("fitsfile must be specified"));
    }
    if matches!(outfile, "." | "..") {
        return Err(ImageToolError::new(format!(
            "Invalid fitsfile name {outfile}"
        )));
    }
    let mut handle = attached.lock();
    if !matches!(*handle, ImageHandle::Float(_)) {
        return Err(ImageToolError::new(
            "Only writing float-valued images to FITS is supported",
        ));
    }
    let request = ExportFitsRequest {
        imagename: handle.path().map_or_else(PathBuf::new, Path::to_path_buf),
        fitsimage: PathBuf::from(outfile),
        velocity,
        optical,
        bitpix,
        minpix,
        maxpix,
        overwrite,
        dropstokes,
        stokeslast,
        history,
        dropdeg,
    };
    let operand_paths = attached.operand_paths();
    validate_attached_fits_target(
        &request,
        handle
            .path()
            .into_iter()
            .chain(operand_paths.iter().map(PathBuf::as_path)),
    )?;
    attached.require_pixel_read_lock(&mut handle)?;
    let ImageHandle::Float(storage) = &*handle else {
        unreachable!("image storage stays fixed while locked");
    };
    let beams = attached.beam_view();
    let options = ImageFitsExportOptions {
        beam_record: Some(beams.record()),
        operand_paths,
        ..options
    };
    let entries = if history {
        match storage {
            Storage::Paged(_) => image_history_entries(&request.imagename)?,
            Storage::Temp { image, .. } => image
                .history_rows()?
                .iter()
                .map(|row| row.entry.clone())
                .collect(),
        }
    } else {
        Vec::new()
    };
    let coordinates = handle.coordinate_record();
    let result = match storage {
        Storage::Paged(image) => {
            export_attached_fits(&request, image.as_ref(), coordinates, &entries, options)
        }
        Storage::Temp {
            image,
            fits: Some(fits),
            ..
        } => {
            let view = fits.image_view(image);
            export_attached_fits(&request, &view, coordinates, &entries, options)
        }
        Storage::Temp { image, .. } => {
            export_attached_fits(&request, image.as_ref(), coordinates, &entries, options)
        }
    };
    let release = attached.release_requested_lock(&mut handle);
    result?;
    release?;
    Ok(true)
}

/// Inspects the FITS header cards for `attached` (`ia.fitsheader`).
pub fn fits_header(
    attached: &AttachedImage,
    retstr: bool,
    exclude: &[String],
) -> ImageToolResult<FitsHeaderOutput> {
    let handle = attached.lock();
    let ImageHandle::Float(storage) = &*handle else {
        return Err(ImageToolError::new(
            "fitsheader only supports floating point images",
        ));
    };
    let name = handle.path().map_or_else(
        || "Temporary_Image".to_owned(),
        |path| path.display().to_string(),
    );
    let needs_history = retstr || !exclude.iter().any(|name| name == "HISTORY");
    let history = if needs_history {
        match storage {
            Storage::Paged(_) => image_history_entries(handle.path().expect("persistent image"))?,
            Storage::Temp { image, .. } => image
                .history_rows()?
                .iter()
                .map(|row| row.entry.clone())
                .collect(),
        }
    } else {
        Vec::new()
    };
    let beams = attached.beam_view();
    let header = with_storage!(storage, image =>
        image_fits_header_with_beams(
            image.as_ref(), handle.shape(), &name, &history, Some(beams.record())
        )
    )?;
    if retstr {
        Ok(FitsHeaderOutput::CardString(header.card_string()))
    } else {
        Ok(FitsHeaderOutput::Record(header.record(exclude)))
    }
}

/// Request specification for importing a FITS image (`ia.fromfits` / `ia.newimagefromfits` / `ia.maketestimage`).
pub struct ImportFitsRequest<'a> {
    /// Destination path (`""` for temporary in-memory image).
    pub outfile: &'a str,
    /// Display input path for history/diagnostics.
    pub infile: &'a str,
    /// Resolved filesystem path to the FITS file.
    pub resolved_infile: &'a Path,
    /// Coordinate representation index (`>= 0`).
    pub whichrep: i64,
    /// HDU index (`>= 0`).
    pub whichhdu: i64,
    /// Replace blanked/NaN values with zeros when `true`.
    pub zeroblanks: bool,
    /// Overwrite `outfile` when it already exists.
    pub overwrite: bool,
}

/// Imports a FITS image according to `request`, returning the attached [`AttachedImage`].
pub fn import_fits(
    request: &ImportFitsRequest<'_>,
    history: Option<(&str, String)>,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<AttachedImage> {
    let whichrep = usize::try_from(request.whichrep).map_err(|_| {
        ImageToolError::new("Exception: The Coordinate Representation index must be non-negative.")
    })?;
    let mut source = FitsToolImport::open_named(
        request.resolved_infile,
        Path::new(request.infile),
        whichrep,
        request.whichhdu,
    )?;
    if source.is_tile_compressed() {
        return Err(ImageToolError::new(format!(
            "Exception: No image at specified location in file {}.",
            request.infile
        )));
    }
    if request.overwrite
        && !request.outfile.is_empty()
        && let Ok(target) = Path::new(request.outfile).canonicalize()
        && request.resolved_infile.ancestors().any(|part| {
            part.canonicalize()
                .is_ok_and(|resolved| resolved.starts_with(&target))
        })
    {
        return Err(ImageToolError::new(
            "Cannot overwrite an output that contains the source FITS file",
        ));
    }
    publish_image(
        request.outfile,
        request.overwrite,
        is_external_open,
        |path| {
            let mut storage = Storage::create(
                source.shape().to_vec(),
                source.coordinates().clone(),
                path,
                None,
            )?;
            with_storage!(&mut storage, image => source.copy_pixels(&mut **image, request.zeroblanks))?;
            with_storage!(&mut storage, image => image.set_units(source.units))?;
            with_storage!(&mut storage, image => image.set_image_info_record(source.image_info))?;
            with_storage!(&mut storage, image => image.set_misc_info(source.misc_info))?;
            with_storage!(&mut storage, image => image.append_history_entries(&source.history))?;
            let mut image = ImageHandle::Float(storage);
            image.set_coordinate_record(source.coordinate_record)?;
            if let Some((method, command)) = &history {
                image.record_history(method, command.clone())?;
            }
            image.save()?;
            Ok(image)
        },
    )
}
