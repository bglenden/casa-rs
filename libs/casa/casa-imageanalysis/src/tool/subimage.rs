// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image subregion extraction (`ia.subimage`).

use std::path::Path;

use casa_core::{RecordValue, Value};
use casa_images::{ImageInterface, ImagePixel, image_history_rows};

use crate::analysis::imsubimage::{
    SubimageOptions, SubimagePlan, SubimageRegion, ensure_outfile_does_not_alias_input,
};

use super::attachment::{AttachedImage, ExternalPathCheck};
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{ImageHandle, Storage, with_storage};
use super::publish::publish_image;

/// Request parameters for `ia.subimage`.
pub struct SubimageRequest<'a> {
    /// Destination path (empty creates a temporary in-memory image).
    pub outfile: &'a str,
    /// Optional region selector (CRTF text, path, or casacore record).
    pub region: Option<SubimageRegion<'a>>,
    /// Optional LEL mask expression.
    pub mask: Option<&'a str>,
    /// Drop singleton axes from output.
    pub dropdeg: bool,
    /// Overwrite `outfile` if it exists.
    pub overwrite: bool,
    /// Emit informational log messages.
    pub list: bool,
    /// Stretch lower-dimensional masks along remaining axes.
    pub stretch: bool,
    /// Return a new handle (`false` only creates the file on disk).
    pub wantreturn: bool,
    /// Singleton axes to retain when `dropdeg=true`.
    pub keepaxes: &'a [i64],
}

/// Extracts a subimage from `attached` according to `request`.
pub fn extract_subimage(
    attached: &AttachedImage,
    request: &SubimageRequest<'_>,
    record_history: bool,
    is_external_open: ExternalPathCheck<'_>,
) -> ImageToolResult<(Option<ImageHandle>, Vec<ToolLogLine>)> {
    let mut logs = Vec::new();
    if request.outfile.is_empty() && !request.wantreturn {
        logs.push(ToolLogLine::warning(
            "outfile was not specified and wantreturn is false. The resulting image will be inaccessible",
        ));
    }
    let handle = publish_image(
        request.outfile,
        request.overwrite,
        is_external_open,
        |path| {
            // Release the source handle before publication takes the image registry lock.
            let mut original = attached.lock();
            attached.require_pixel_read_lock(&mut original)?;
            let beams = attached.beam_view().clone();
            if !request.outfile.is_empty()
                && let Some(source) = original.path()
            {
                ensure_outfile_does_not_alias_input(Path::new(request.outfile), source)?;
            }
            let mut copy = match &*original {
                ImageHandle::Float(Storage::Temp {
                    image,
                    record,
                    fits: Some(fits),
                }) => {
                    let source = fits.image_view(image);
                    let mut storage = copy_selection(
                        request,
                        &source,
                        Some(record.clone()),
                        beams.record(),
                        path,
                        &mut logs,
                    )?;
                    with_storage!(&mut storage, product => {
                        product.append_history_entries(&image.history_rows()?)
                    })?;
                    ImageHandle::Float(storage)
                }
                ImageHandle::Float(source) => {
                    ImageHandle::Float(build(request, source, beams.record(), path, &mut logs)?)
                }
                ImageHandle::Double(source) => {
                    ImageHandle::Double(build(request, source, beams.record(), path, &mut logs)?)
                }
                ImageHandle::Complex(source) => {
                    ImageHandle::Complex(build(request, source, beams.record(), path, &mut logs)?)
                }
                ImageHandle::DComplex(source) => {
                    ImageHandle::DComplex(build(request, source, beams.record(), path, &mut logs)?)
                }
            };
            if record_history {
                let region_hist = match request.region {
                    None => "\"\"".to_owned(),
                    Some(SubimageRegion::Text(text)) => format!("{text:?}"),
                    Some(SubimageRegion::Record(_)) => "{...}".to_owned(),
                };
                copy.record_history(
                    "subimage",
                    format!(
                        "ia.subimage(outfile={:?}, region={region_hist}, mask={:?}, dropdeg={}, overwrite={}, list={}, stretch={}, wantreturn={}, keepaxes={:?})",
                        request.outfile,
                        request.mask.unwrap_or(""),
                        request.dropdeg,
                        request.overwrite,
                        request.list,
                        request.stretch,
                        request.wantreturn,
                        request.keepaxes
                    ),
                )?;
            }
            copy.save()?;
            Ok(copy)
        },
    )?;

    if request.wantreturn {
        Ok((Some(handle), logs))
    } else {
        Ok((None, logs))
    }
}

fn copy_selection<T: ImagePixel + Copy>(
    request: &SubimageRequest<'_>,
    source: &dyn ImageInterface<T>,
    coordinates: Option<RecordValue>,
    beams: &RecordValue,
    path: Option<&Path>,
    logs: &mut Vec<ToolLogLine>,
) -> ImageToolResult<Storage<T>> {
    let options = SubimageOptions {
        region: request.region,
        dropdeg: request.dropdeg,
        keepaxes: request.keepaxes,
    };
    let plan = SubimagePlan::new_preserving_beams(source, coordinates, options, beams)?;
    if request.list {
        let (blc, trc) = plan.bounds();
        logs.push(ToolLogLine::info(format!(
            "Selected bounding box {blc:?} to {trc:?}"
        )));
    }
    let mut result = Storage::create(plan.shape.clone(), plan.coordinates.clone(), path, None)?;
    let expression = request.mask.filter(|mask| !mask.is_empty());
    with_storage!(&mut result, copy =>
        plan.copy_to(source, copy.as_mut(), expression, request.stretch)
    )?;
    if let Some(record) = plan.coordinate_record {
        match &mut result {
            Storage::Paged(image) => image.set_coordinate_record(record)?,
            Storage::Temp {
                record: coordinates,
                ..
            } => *coordinates = record,
        }
    }
    Ok(result)
}

fn build<T: ImagePixel + Copy>(
    request: &SubimageRequest<'_>,
    source: &Storage<T>,
    beams: &RecordValue,
    path: Option<&Path>,
    logs: &mut Vec<ToolLogLine>,
) -> ImageToolResult<Storage<T>> {
    let coordinates = match source {
        Storage::Paged(image) => match image.table().keywords().get("coords") {
            Some(Value::Record(record)) => Some(record.clone()),
            _ => None,
        },
        Storage::Temp { record, .. } => Some(record.clone()),
    };
    let mut result = with_storage!(
        source, image => copy_selection(request, image.as_ref(), coordinates, beams, path, logs)?
    );
    let rows = match source {
        Storage::Paged(image) => image_history_rows(
            image
                .name()
                .ok_or_else(|| ImageToolError::new("Persistent image has no directory path"))?,
        )?,
        Storage::Temp { image, .. } => image.history_rows()?.into_owned(),
    };
    with_storage!(&mut result, image => image.append_history_entries(&rows))?;
    Ok(result)
}
