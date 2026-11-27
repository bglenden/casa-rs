// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) trait PixelTypeLabel {
    fn pixel_type_label(&self) -> String;
}

impl PixelTypeLabel for ImagePixelType {
    fn pixel_type_label(&self) -> String {
        match self {
            Self::Float32 => "Float32",
            Self::Float64 => "Float64",
            Self::Complex32 => "Complex32",
            Self::Complex64 => "Complex64",
        }
        .to_string()
    }
}

impl PixelTypeLabel for PagedImage<f32> {
    fn pixel_type_label(&self) -> String {
        "Float32".to_string()
    }
}

impl PixelTypeLabel for PagedImage<f64> {
    fn pixel_type_label(&self) -> String {
        "Float64".to_string()
    }
}

/// Apply a supported string-valued CASA `imhead(..., mode="put")` update.
pub fn imhead_put(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: impl Into<String>,
) -> Result<ImageHeaderSummary, ImageError> {
    imhead_change_string(path, "put", hdkey, &hdvalue.into())
}

/// Set a CASA `crpixN` image-header value, preserving coordinate axis maps.
///
/// `hdvalue` may be a finite number or a numeric string. The change is saved
/// with an image history entry and the updated header is returned.
///
/// # Errors
///
/// Returns an error for an invalid or polarization axis, non-numeric value,
/// malformed metadata, or image I/O failure. As in CASA, a history I/O error
/// can be reported after the metadata has been saved.
pub fn imhead_put_reference_pixel(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let axis = key
        .strip_prefix("crpix")
        .and_then(imhead_axis_index)
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let mut image = AnyPagedImage::open(path)?;
    let coordinates = image.coordinates();
    if coordinates.pixel_axis_owner(axis).is_none() {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid axis keyword {hdkey:?}"
        )));
    }
    if coordinates
        .world_axis_owner(axis)
        .is_some_and(|(index, _)| {
            coordinates.coordinate(index).coordinate_type() == CoordinateType::Stokes
        })
    {
        return Err(ImageError::InvalidMetadata(
            "A polarization axis cannot have a reference pixel".into(),
        ));
    }
    let (value, rendered) = match hdvalue {
        ImheadValue::Int(value) => (f64::from(*value), value.to_string()),
        ImheadValue::Float(value) if value.is_finite() => {
            (*value, projection_header::cpp_number_precision(*value, 12))
        }
        ImheadValue::String(value) => (
            value.parse::<f64>().map_err(|_| {
                ImageError::InvalidMetadata(format!("fromString failure for string {value:?}"))
            })?,
            format!("\"{value}\""),
        ),
        _ => {
            return Err(ImageError::InvalidMetadata(
                "For crpix, value must be numeric".into(),
            ));
        }
    };
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_reference_pixel(axis, value)?,
        AnyPagedImage::Float64(image) => image.set_reference_pixel(axis, value)?,
        AnyPagedImage::Complex32(image) => image.set_reference_pixel(axis, value)?,
        AnyPagedImage::Complex64(image) => image.set_reference_pixel(axis, value)?,
    };
    image.save()?;
    drop(image);
    let old = projection_header::cpp_number_precision(old, 12);
    casa_images::add_image_history(
        path,
        "ImageMetaDataRW::_setCoordinateValue",
        &[format!("Modified {key} from {old} to {rendered}")],
    )?;
    imhead(path)
}

/// Parse the one-based digit prefix used by CASA's image-header axis keys.
pub fn imhead_axis_index(suffix: &str) -> Option<usize> {
    let digits = suffix.bytes().take_while(u8::is_ascii_digit).count();
    suffix[..digits].parse::<usize>().ok()?.checked_sub(1)
}

/// Set `cdeltN` or `crvalN` from a CASA quantity or Stokes names.
///
/// # Errors
///
/// Returns an error for invalid or unsupported axes, quantities, metadata,
/// or image I/O. History I/O may fail after metadata has been saved.
pub fn imhead_put_axis_quantity(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let (prefix, suffix) = key
        .get(..5)
        .zip(key.get(5..))
        .filter(|(prefix, _)| ["cdelt", "crval"].contains(prefix))
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let axis = imhead_axis_index(suffix)
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let summary = imhead(path)?;
    let header = summary
        .world_axes
        .get(axis)
        .filter(|_| axis < summary.shape.len())
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    if header.coordinate_type == "Stokes" {
        if prefix == "cdelt" {
            return Err(ImageError::InvalidMetadata(
                "A polarization axis cannot have an increment".into(),
            ));
        }
        return imhead_put_stokes_values(path, &key, axis, hdvalue);
    }
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    let value = quantity_input::in_axis_unit(&quantity, &header.unit)?;
    let old = if prefix == "cdelt" {
        header.increment
    } else {
        header.reference_value
    };
    let old = format!(
        "{}{}",
        projection_header::cpp_number_precision(old, 6),
        header.unit
    );
    let mut image = AnyPagedImage::open(path)?;
    let set_value = if prefix == "cdelt" {
        CoordinateSystem::set_world_increment_in_record
    } else {
        CoordinateSystem::set_world_reference_value_in_record
    };
    let edit = |record: &mut RecordValue| set_value(record, axis, value);
    match &mut image {
        AnyPagedImage::Float32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Float64(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex64(image) => image.edit_coordinates(edit)?,
    };
    image.save()?;
    drop(image);
    casa_images::add_image_history(
        path,
        "ImageMetaDataRW::_setCoordinateValue",
        &[format!("Modified {key} from \"{old}\" to {rendered}")],
    )?;
    imhead(path)
}

pub(super) fn imhead_put_stokes_values(
    path: &Path,
    key: &str,
    axis: usize,
    value: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let (names, rendered) = match value {
        ImheadValue::String(value) if !value.is_empty() => {
            (vec![value.clone()], format!("\"{value}\""))
        }
        ImheadValue::List(values) if !values.is_empty() => {
            let mixed = values
                .iter()
                .any(|value| matches!(value, ImheadValue::String(_)))
                && values
                    .iter()
                    .any(|value| !matches!(value, ImheadValue::String(_)));
            if mixed {
                return Err(ImageError::InvalidMetadata(
                    "Mixed-type polarization arrays are unsupported (#977)".into(),
                ));
            }
            let names = values
                .iter()
                .map(|value| match value {
                    ImheadValue::String(value) => Ok(value.clone()),
                    _ => Err(ImageError::InvalidMetadata(
                        "Polarization values must be strings".into(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let rendered = format!("[{}]", names.join(", "));
            (names, rendered)
        }
        ImheadValue::Array(casa_core::ArrayValue::String(values)) if !values.is_empty() => {
            let names: Vec<String> = values.iter().cloned().collect();
            let rendered = format!("[{}]", names.join(", "));
            (names, rendered)
        }
        ImheadValue::Array(array)
            if !matches!(
                array,
                casa_core::ArrayValue::Bool(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_core::ArrayValue::Int32(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_core::ArrayValue::Float64(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_core::ArrayValue::Complex64(values) if values.is_empty()
            ) && !matches!(
                array,
                casa_core::ArrayValue::String(values) if values.is_empty()
            ) =>
        {
            return Err(ImageError::InvalidMetadata(
                "Polarization values must be strings".into(),
            ));
        }
        _ => {
            return Err(ImageError::InvalidMetadata(
                "Polarization values must be a string or string array".into(),
            ));
        }
    };
    if names.iter().any(|name| name.contains('\0')) {
        return Err(ImageError::InvalidMetadata(
            "Polarization values containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let edit = |record: &mut RecordValue| {
        CoordinateSystem::set_world_stokes_types_in_record(record, axis, &names)
    };
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Float64(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex32(image) => image.edit_coordinates(edit)?,
        AnyPagedImage::Complex64(image) => image.edit_coordinates(edit)?,
    };
    image.save()?;
    drop(image);
    casa_images::add_image_history(
        path,
        "ImageMetaDataRW::_setCoordinateValue",
        &[format!(
            "Modified {key} from [{}] to {rendered}",
            old.join(", ")
        )],
    )?;
    imhead(path)
}

/// Set a CASA `restfreq` value from a quantity string or value-and-unit record.
///
/// # Errors
///
/// Returns an error for an invalid quantity, absent spectral metadata, or
/// image I/O failure. History I/O may fail after the metadata is saved.
pub fn imhead_put_rest_frequency(
    path: impl AsRef<Path>,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let summary = imhead(path)?;
    let spectral = summary
        .spectral
        .as_ref()
        .ok_or_else(|| ImageError::InvalidMetadata("Image has no spectral coordinate".into()))?;
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    if quantity.value() <= 0.0 {
        return Err(ImageError::InvalidMetadata(
            "Rest frequency must be positive".into(),
        ));
    }
    // CASA interprets an unqualified rest frequency as Hz, unlike axis
    // increments and reference values, which default to their axis units.
    let quantity = if quantity.unit().name().is_empty() {
        casa_core::quanta::Quantity::new(quantity.value(), "Hz")
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?
    } else {
        quantity
    };
    let frequency = quantity_input::in_unit(&quantity, &spectral.unit)?;
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_rest_frequency(frequency)?,
        AnyPagedImage::Float64(image) => image.set_rest_frequency(frequency)?,
        AnyPagedImage::Complex32(image) => image.set_rest_frequency(frequency)?,
        AnyPagedImage::Complex64(image) => image.set_rest_frequency(frequency)?,
    };
    image.save()?;
    drop(image);
    let old = projection_header::cpp_number_precision(old, 12);
    casa_images::add_image_history(
        path,
        "ImageMetaDataRW::set",
        &[format!("Modified restfreq from {old} to {rendered}")],
    )?;
    imhead(path)
}

/// Add a rest frequency if the image does not already have a positive one.
///
/// # Errors
///
/// Returns an error if a rest frequency exists, or if the quantity or image
/// cannot be updated. History I/O may fail after the metadata is saved.
pub fn imhead_add_rest_frequency(
    path: impl AsRef<Path>,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    let summary = imhead(path)?;
    if summary
        .spectral
        .as_ref()
        .is_some_and(|spectral| spectral.rest_frequency > 0.0)
    {
        return Err(ImageError::InvalidMetadata(
            "Rest frequency already exists; it may be modified but not added".into(),
        ));
    }
    imhead_put_rest_frequency(path, hdvalue)
}

/// Set CASA's observation date from a time quantity or date string.
///
/// # Errors
///
/// Returns an error for an invalid time quantity or image I/O failure.
/// History I/O may fail after the metadata is saved.
pub fn imhead_put_observation_date(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    use casa_core::measures::{EpochRef, MEpoch};
    use casa_core::quanta::{Unit, casacore::get_conformant_value, format};

    let path = path.as_ref();
    validate_imhead_image(path)?;
    if matches!(hdvalue, ImheadValue::String(value) if value.is_empty()) {
        return Err(ImageError::InvalidMetadata(format!(
            "{hdkey} value not specified"
        )));
    }
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    let days = get_conformant_value(&quantity, &Unit::new("d").expect("valid day unit")).map_err(
        |_| ImageError::InvalidMetadata(format!("{hdkey} value must have units of time")),
    )?;
    // MVTime formatting uses integer calendar arithmetic. Refuse extreme
    // values before writing rather than overflowing while building the reply.
    if !valid_imhead_calendar_date(days) {
        return Err(ImageError::InvalidMetadata(
            "Non-finite or extreme observation dates are unsupported (#977)".into(),
        ));
    }
    let epoch = MEpoch::from_mjd(days, EpochRef::UTC);
    let mut image = AnyPagedImage::open(path)?;
    macro_rules! set_date {
        ($image:expr) => {{
            let old = $image
                .coordinates()
                .obs_info()
                .date
                .as_ref()
                .map_or(0.0, |epoch| epoch.value().as_mjd());
            if !valid_imhead_calendar_date(old) {
                return Err(ImageError::InvalidMetadata(
                    "Existing observation date is non-finite or extreme (#977)".into(),
                ));
            }
            $image.set_observation_date(epoch)?;
            old
        }};
    }
    let old = match &mut image {
        AnyPagedImage::Float32(image) => set_date!(image),
        AnyPagedImage::Float64(image) => set_date!(image),
        AnyPagedImage::Complex32(image) => set_date!(image),
        AnyPagedImage::Complex64(image) => set_date!(image),
    };
    image.save()?;
    drop(image);
    let old = format::format_time(old, format::MJD, 10).replacen('/', "::", 1);
    casa_images::add_image_history(
        path,
        "ImageMetaDataRW::set",
        &[format!(
            "Modified {} from \"Epoch: {old}\n\" to {rendered}",
            hdkey
        )],
    )?;
    imhead(path)
}

pub(super) fn valid_imhead_calendar_date(days: f64) -> bool {
    // Casacore's ifloor decrements negative whole numbers too. MVTime uses
    // Int for the MJD and Julian day and Long for the Gregorian correction.
    let casa_floor = |value: f64| {
        if value < 0.0 {
            (value - 1.0).trunc()
        } else {
            value.trunc()
        }
    };
    let day = casa_floor(days);
    let julian_day = casa_floor(day + 2_400_001.0);
    let calendar_range = f64::from(i32::MIN)..=f64::from(i32::MAX);
    if !calendar_range.contains(&day) || !calendar_range.contains(&julian_day) {
        return false;
    }
    let gregorian = if julian_day >= 2_299_161.0 {
        let centuries = ((julian_day - 1_867_216.25) / 36_524.25).floor();
        1.0 + centuries - (centuries / 4.0).floor()
    } else {
        0.0
    };
    calendar_range.contains(&(julian_day + gregorian + 1524.0))
}

/// Set CASA's native direction reference while retaining its conversion frame.
///
/// # Errors
///
/// Returns an error for an invalid or unsupported frame or image I/O.
/// History I/O may fail after the metadata is saved.
pub fn imhead_put_direction_reference(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let ImheadValue::String(reference) = hdvalue else {
        return Err(ImageError::InvalidMetadata(
            "equinox value must be a string".into(),
        ));
    };
    if reference.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "direction frames containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_direction_reference(reference)?,
        AnyPagedImage::Float64(image) => image.set_direction_reference(reference)?,
        AnyPagedImage::Complex32(image) => image.set_direction_reference(reference)?,
        AnyPagedImage::Complex64(image) => image.set_direction_reference(reference)?,
    };
    if old.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some(old) = old {
        casa_images::add_image_history(
            path,
            "ImageMetaDataRW::set",
            &[format!(
                "Modified {hdkey} from \"{old}\" to \"{reference}\""
            )],
        )?;
    }
    imhead(path)
}

/// Set CASA's direction projection where its coordinate model is supported.
///
/// # Errors
///
/// Returns an error for an invalid or unsupported projection or image I/O.
/// History I/O may fail after the metadata is saved.
pub fn imhead_put_projection(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let ImheadValue::String(projection) = hdvalue else {
        return Err(ImageError::InvalidMetadata(
            "projection value must be a string".into(),
        ));
    };
    if projection.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "projection names containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_direction_projection(projection)?,
        AnyPagedImage::Float64(image) => image.set_direction_projection(projection)?,
        AnyPagedImage::Complex32(image) => image.set_direction_projection(projection)?,
        AnyPagedImage::Complex64(image) => image.set_direction_projection(projection)?,
    };
    if old.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some(old) = old {
        casa_images::add_image_history(
            path,
            "ImageMetaDataRW::set",
            &[format!(
                "Modified {hdkey} from \"{old}\" to \"{projection}\""
            )],
        )?;
    }
    imhead(path)
}

/// Set CASA's `reffreqtype`, resetting the spectral conversion frame.
///
/// # Errors
///
/// Returns an error for an invalid frame, absent or malformed spectral
/// metadata, or image I/O failure. History I/O may fail after metadata is saved.
pub fn imhead_put_spectral_reference(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let ImheadValue::String(reference) = hdvalue else {
        return Err(ImageError::InvalidMetadata(
            "reffreqtype value must be a string".into(),
        ));
    };
    if reference.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "spectral frames containing NUL are unsupported (#977)".into(),
        ));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &mut image {
        AnyPagedImage::Float32(image) => image.set_spectral_reference(reference)?,
        AnyPagedImage::Float64(image) => image.set_spectral_reference(reference)?,
        AnyPagedImage::Complex32(image) => image.set_spectral_reference(reference)?,
        AnyPagedImage::Complex64(image) => image.set_spectral_reference(reference)?,
    };
    if old.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some(old) = old {
        casa_images::add_image_history(
            path,
            "ImageMetaDataRW::set",
            &[format!(
                "Modified {hdkey} from \"{old}\" to \"{reference}\""
            )],
        )?;
    }
    imhead(path)
}

/// Add, modify, or remove restoring-beam header metadata.
///
/// The boolean indicates whether CASA would append task history. In
/// particular, deleting a beam from an image without one is a no-op.
///
/// # Errors
///
/// Returns an error for invalid quantities, unsupported operations, malformed
/// beam metadata, or image I/O failure. History I/O may fail after saving.
pub fn imhead_change_beam(
    path: impl AsRef<Path>,
    mode: &str,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<(ImageHeaderSummary, bool), ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let component = match key.as_str() {
        "beammajor" | "bmaj" => casa_images::BeamComponent::Major,
        "beamminor" | "bmin" => casa_images::BeamComponent::Minor,
        "beampa" | "bpa" => casa_images::BeamComponent::PositionAngle,
        _ => {
            return Err(ImageError::InvalidMetadata(format!(
                "invalid beam keyword {hdkey:?}"
            )));
        }
    };
    let mut image = AnyPagedImage::open(path)?;
    let history = match &mut image {
        AnyPagedImage::Float32(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
        AnyPagedImage::Float64(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
        AnyPagedImage::Complex32(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
        AnyPagedImage::Complex64(image) => edit_beam(image, mode, hdkey, component, hdvalue)?,
    };
    if history.is_some() {
        image.save()?;
    }
    drop(image);
    if let Some((origin, message)) = history {
        casa_images::add_image_history(path, origin, &[message])?;
        Ok((imhead(path)?, true))
    } else {
        Ok((imhead(path)?, false))
    }
}
