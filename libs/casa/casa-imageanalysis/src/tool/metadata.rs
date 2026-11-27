// SPDX-License-Identifier: LGPL-3.0-or-later

//! Image metadata inspection and updates (`brightnessunit`, `miscinfo`, `history`, `coordsys`, `shape`, `type`).

use casa_coordinates::{CoordinateModel, CoordinateSystem};
use casa_core::RecordValue;
use casa_core::quanta::{Unit, unit::normalize_casa_unit_name};

use super::attachment::AttachedImage;
use super::coords::validate_restored_linear_coordinates;
use super::error::{ImageToolError, ImageToolResult, ToolLogLine};
use super::handle::{with_pixels, with_storage};

/// Returns the image shape (`ia.shape()`).
pub fn image_shape(attached: &AttachedImage) -> Vec<usize> {
    attached.lock().shape().to_vec()
}

/// Returns the CASA image type string (`ia.type()`).
pub fn image_type(attached: &AttachedImage) -> &'static str {
    attached.lock().image_type_name()
}

/// Returns the image pixel value precision tag (`"float"`, `"double"`, `"complex"`, `"dcomplex"`).
pub fn pixel_type_name(attached: &AttachedImage) -> &'static str {
    match attached.lock().kind() {
        casa_images::ImagePixelType::Float32 => "float",
        casa_images::ImagePixelType::Float64 => "double",
        casa_images::ImagePixelType::Complex32 => "complex",
        casa_images::ImagePixelType::Complex64 => "dcomplex",
    }
}

/// Returns the brightness unit string (`ia.brightnessunit()`).
pub fn brightness_unit(attached: &AttachedImage) -> String {
    with_pixels!(&*attached.lock(), storage => with_storage!(storage, image => image.units().to_owned()))
}

/// Updates the brightness unit string (`ia.setbrightnessunit()`).
pub fn set_brightness_unit(
    attached: &AttachedImage,
    unit: &str,
    record_history: bool,
) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    image.check_single_precision("setbrightnessunit")?;
    let normalized = normalize_casa_unit_name(unit.trim());
    Unit::new(&normalized)
        .map_err(|_| ImageToolError::new(format!("Unit::check Illegal unit string '{unit}'")))?;
    if image.image_type_name() == "ImageConcat" {
        attached.check_bound_name(&image)?;
    } else {
        attached.require_lock(&mut image, casa_tables::LockType::Write)?;
    }
    image.set_units(&normalized)?;
    if record_history {
        image.record_history(
            "setbrightnessunit",
            format!("ia.setbrightnessunit(unit={unit:?})"),
        )?;
    }
    image.save()?;
    Ok(true)
}

/// Returns the `miscinfo` record (`ia.miscinfo()`), enforcing single precision.
pub fn misc_info_casa(
    attached: &AttachedImage,
) -> ImageToolResult<(RecordValue, Option<std::path::PathBuf>)> {
    let image = attached.lock();
    image.check_single_precision("miscinfo")?;
    let record =
        with_pixels!(&*image, storage => with_storage!(storage, image => image.misc_info()));
    Ok((record, image.path().map(std::path::Path::to_path_buf)))
}

/// Returns the `miscinfo` record (`ia.miscinfo()`).
pub fn misc_info(attached: &AttachedImage) -> ImageToolResult<RecordValue> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    Ok(image.misc_info())
}

/// Replaces the `miscinfo` record (`ia.setmiscinfo()`).
pub fn set_misc_info(
    attached: &AttachedImage,
    info: RecordValue,
    record_history: bool,
) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    if let Some(dir) = attached.saved_expression_directory() {
        let path = attached
            .name_path(&image)
            .or_else(|| image.path())
            .ok_or_else(|| ImageToolError::new("Saved image expression has no name"))?
            .to_path_buf();
        dir.write_metadata(&path, image.kind(), &info)?;
    } else if image.image_type_name() == "ImageConcat" {
        attached.check_bound_name(&image)?;
    } else {
        attached.require_lock(&mut image, casa_tables::LockType::Write)?;
    }
    // CASA accepts this call on a MIRIAD attachment and records it
    // in history. Subsequent reads and exports see the empty record.
    if image.image_type_name() != "MIRIADImage" {
        image.set_misc_info(info)?;
    }
    if record_history {
        image.record_history("setmiscinfo", "ia.setmiscinfo(info={...})".to_owned())?;
    }
    image.save()?;
    Ok(true)
}

/// Returns the image history strings (`ia.history()`).
pub fn image_history(attached: &AttachedImage) -> ImageToolResult<Vec<String>> {
    let image = attached.lock();
    with_pixels!(&*image, storage => with_storage!(storage, image => image.history()))
        .map_err(ImageToolError::from)
}

/// Appends custom history lines to the image (`ia.sethistory()`).
pub fn set_history(
    attached: &AttachedImage,
    origin: &str,
    history: &[String],
) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    attached.require_write_lock(&mut image, "sethistory")?;
    let origin = if origin.is_empty() {
        "::image::sethistory"
    } else {
        origin
    };
    image.append_history_at(origin, history)?;
    image.save()?;
    Ok(true)
}

/// Appends custom history lines matching CASA `ia.sethistory(origin, history)` behavior.
pub fn set_history_casa(
    attached: &AttachedImage,
    origin: &str,
    mut messages: Vec<String>,
) -> ImageToolResult<Vec<ToolLogLine>> {
    let only_empty_string = matches!(messages.as_slice(), [message] if message.is_empty());
    messages.retain(|message| !message.is_empty());
    if !messages.is_empty() {
        let location = if origin.is_empty() {
            "ImageHistory::addHistory".to_owned()
        } else {
            format!("::{origin}")
        };
        let mut handle = attached.lock();
        attached.check_bound_name(&handle)?;
        handle.append_history_at(&location, &messages)?;
    }
    let logs = if only_empty_string {
        vec![ToolLogLine::info("history string is empty")]
    } else {
        Vec::new()
    };
    Ok(logs)
}

/// Returns whether the image has any non-trivial mask.
pub fn has_mask(attached: &AttachedImage) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    if let Some(fits) = image.fits() {
        return Ok(fits.has_mask);
    }
    Ok(with_pixels!(&*image, storage => {
        with_storage!(storage, img => img.has_pixel_mask())
    }))
}

/// Extracts a coordinate system and its raw record, optionally projected onto `axes`.
pub fn coordsys_for_axes(
    attached: &AttachedImage,
    axes: &[i64],
) -> ImageToolResult<(CoordinateSystem, RecordValue)> {
    let image = attached.lock();
    let (coordinates, record) = attached.coordinate_view(&image);
    if axes.is_empty() || axes == [-1] {
        return Ok((coordinates.clone(), record));
    }
    let placements = coordinates.coordinates_in_record(&record)?;
    let mut result = CoordinateSystem::new().with_obs_info(coordinates.obs_info().clone());
    let mut offsets = vec![None; placements.len()];
    let mut keep = Vec::new();
    for &requested in axes {
        let axis = usize::try_from(requested)
            .ok()
            .filter(|&axis| axis < coordinates.n_pixel_axes())
            .ok_or_else(|| {
                ImageToolError::new(format!(
                    "Specified zero-based pixel axis {requested} is not a valid pixel axis"
                ))
            })?;
        let (index, _) = coordinates.pixel_axis_owner(axis).ok_or_else(|| {
            ImageToolError::new(format!(
                "Zero-based pixel axis {requested} has been removed"
            ))
        })?;
        let placement = &placements[index];
        let world = coordinates
            .pixel_axis_to_world_axis(axis)
            .and_then(|world| {
                placement
                    .world_axes
                    .iter()
                    .position(|axis| *axis == Some(world))
            })
            .ok_or_else(|| {
                ImageToolError::new(format!(
                    "World axis corresponding to zero-based pixel axis {requested} has been removed"
                ))
            })?;
        let base = match offsets[index] {
            Some(base) => base,
            None => {
                let base = result.n_world_axes();
                let model = coordinates.coordinate(index);
                let model = match model {
                    CoordinateModel::Spectral(spectral) if !spectral.has_linear_mapping() => {
                        model.clone()
                    }
                    CoordinateModel::Tabular(_) => model.clone(),
                    _ => placement.model(&record)?,
                };
                result.add_coordinate(model);
                offsets[index] = Some(base);
                base
            }
        };
        keep.push(base + world);
    }
    for (axis, replacement) in result.reference_value().into_iter().enumerate().rev() {
        if !keep.contains(&axis) {
            result.remove_world_axis(axis, replacement)?;
        }
    }
    let out_record = result.to_record();
    Ok((result, out_record))
}

/// Replaces the coordinate system record (`ia.setcoordsys`).
pub fn set_coordinate_record(
    attached: &AttachedImage,
    record: RecordValue,
    record_history: bool,
) -> ImageToolResult<bool> {
    let mut image = attached.lock();
    if record.is_empty() {
        return Err(ImageToolError::new("Exception: Record is empty."));
    }
    let coordinates = CoordinateSystem::from_record(&record)?;
    validate_restored_linear_coordinates(&coordinates, &record)?;
    if coordinates.n_pixel_axes() != image.shape().len() {
        return Err(ImageToolError::new(
            "Exception: Supplied CoordinateSystem and image shape are inconsistent",
        ));
    }
    if image.image_type_name() == "ImageConcat" {
        attached.check_bound_name(&image)?;
    } else {
        attached.require_lock(&mut image, casa_tables::LockType::Write)?;
    }
    image.set_coordinate_record(record)?;
    attached.coordinates_changed();
    if record_history {
        image.record_history("setcoordsys", "ia.setcoordsys(csys={...})".to_owned())?;
    }
    image.save()?;
    Ok(true)
}

/// Returns the active coordinate system record (`ia.coordsys`).
pub fn coordinate_record(attached: &AttachedImage) -> ImageToolResult<RecordValue> {
    let mut image = attached.lock();
    attached.require_pixel_read_lock(&mut image)?;
    Ok(image.coordinate_record())
}
