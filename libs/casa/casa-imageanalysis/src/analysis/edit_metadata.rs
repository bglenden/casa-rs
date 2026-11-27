// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

pub(super) fn edit_beam<T: ImagePixel>(
    image: &mut PagedImage<T>,
    mode: &str,
    hdkey: &str,
    component: casa_images::BeamComponent,
    hdvalue: &ImheadValue,
) -> Result<Option<(&'static str, String)>, ImageError> {
    use casa_images::BeamComponent;
    let info = image.image_info()?;
    if mode == "del" {
        let multiple = info.beam_set.is_multi();
        if !image.remove_restoring_beams()? {
            return Ok(None);
        }
        let message = if multiple {
            "Removed image's multiple beams"
        } else {
            "Removed image's single beam"
        };
        return Ok(Some(("ImageMetaDataRW::remove", message.into())));
    }
    if mode != "add" && mode != "put" {
        return Err(ImageError::InvalidMetadata(format!(
            "invalid beam operation {mode:?}"
        )));
    }
    if mode == "add" && component == BeamComponent::PositionAngle {
        return Err(ImageError::InvalidMetadata(
            "Cannot add a beam position angle".into(),
        ));
    }
    if mode == "add" && !info.beam_set.is_empty() {
        return Err(ImageError::InvalidMetadata(
            "This image already has a beam(s)".into(),
        ));
    }
    if info.beam_set.is_multi() {
        return Err(ImageError::InvalidMetadata(
            "This image has multiple beams".into(),
        ));
    }
    let (quantity, rendered) = quantity_input::parse(hdvalue)?;
    let old = image.stored_restoring_beam()?;
    let quantity_text = |quantity: &casa_core::quanta::Quantity| {
        format!(
            "{} {}",
            projection_header::cpp_number_precision(quantity.value(), 6),
            quantity.unit().name()
        )
    };
    let (beam, message) = if let Some(old) = old {
        let suffix = if component == BeamComponent::PositionAngle {
            ""
        } else {
            "\n"
        };
        let previous = format!("{}{suffix}", quantity_text(old.component(component)));
        let beam = old.with_component(component, quantity)?;
        (
            beam,
            format!("Modified {hdkey} from \"{previous}\" to {rendered}"),
        )
    } else {
        if component == BeamComponent::PositionAngle {
            return Err(ImageError::InvalidMetadata(
                "This image has no beam; add a beam axis before its position angle".into(),
            ));
        }
        let beam = casa_images::StoredGaussianBeam::new(
            quantity.clone(),
            quantity,
            casa_core::quanta::Quantity::new(0.0, "deg").expect("built-in degrees"),
        )?;
        let message = format!(
            "Added beam major: {}, minor: {}, pa: {}",
            quantity_text(beam.component(BeamComponent::Major)),
            quantity_text(beam.component(BeamComponent::Minor)),
            quantity_text(beam.component(BeamComponent::PositionAngle))
        );
        (beam, message)
    };
    image.set_stored_restoring_beam(&beam)?;
    Ok(Some(("ImageMetaDataRW::set", message)))
}

/// Set a CASA `ctypeN` axis name and record the image history.
///
/// # Errors
///
/// Returns an error for an invalid axis or value, malformed metadata, or an
/// image I/O failure. History I/O may fail after the metadata is saved.
pub fn imhead_put_axis_name(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    imhead_put_axis_text(path.as_ref(), hdkey, hdvalue, "ctype")
}

/// Set a CASA `cunitN` axis unit and rescale its coordinate values.
///
/// # Errors
///
/// Returns an error for invalid units, unsupported axes, malformed metadata,
/// or image I/O failure. History I/O may fail after the metadata is saved.
pub fn imhead_put_axis_unit(
    path: impl AsRef<Path>,
    hdkey: &str,
    hdvalue: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    imhead_put_axis_text(path.as_ref(), hdkey, hdvalue, "cunit")
}

pub(super) fn imhead_put_axis_text(
    path: &Path,
    hdkey: &str,
    hdvalue: &ImheadValue,
    prefix: &str,
) -> Result<ImageHeaderSummary, ImageError> {
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    let axis = key
        .strip_prefix(prefix)
        .and_then(imhead_axis_index)
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    let mut image = AnyPagedImage::open(path)?;
    let coordinates = image.coordinates();
    let (index, _) = coordinates
        .world_axis_owner(axis)
        .filter(|_| axis < image.shape().len())
        .ok_or_else(|| ImageError::InvalidMetadata(format!("invalid axis keyword {hdkey:?}")))?;
    if prefix == "cunit"
        && coordinates.coordinate(index).coordinate_type() == CoordinateType::Stokes
    {
        return Err(ImageError::InvalidMetadata(
            "A polarization axis cannot have a unit".into(),
        ));
    }
    let ImheadValue::String(name) = hdvalue else {
        return Err(ImageError::InvalidMetadata(format!(
            "{key} value must be a string"
        )));
    };
    if name.contains('\0') {
        return Err(ImageError::InvalidMetadata(
            "Axis names and units containing NUL are not supported (#977)".into(),
        ));
    }
    let set_label = if prefix == "cunit" {
        CoordinateSystem::set_world_axis_unit_in_record
    } else {
        CoordinateSystem::set_world_axis_name_in_record
    };
    let edit = |record: &mut RecordValue| set_label(record, axis, name);
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
        &[format!("Modified {key} from \"{old}\" to \"{name}\"")],
    )?;
    imhead(path)
}

/// Add or remove a supported string-valued CASA image header field.
/// For `del` of `masks`, an empty value removes all masks.
pub fn imhead_add_delete(
    path: impl AsRef<Path>,
    mode: &str,
    hdkey: &str,
    hdvalue: &str,
) -> Result<ImageHeaderSummary, ImageError> {
    if !["add", "del"].contains(&mode) {
        return Err(ImageError::InvalidMetadata(format!(
            "unknown imhead operation {mode:?}"
        )));
    }
    if mode == "del" && hdkey == "masks" {
        let path = path.as_ref();
        validate_imhead_writable(path)?;
        imhead_masks::remove(path, hdvalue)?;
        return imhead(path);
    }
    imhead_change_string(path, mode, hdkey, if mode == "del" { "" } else { hdvalue })
}

/// Whether a name is available for CASA's user-defined metadata fields.
pub fn imhead_is_custom_key(key: &str) -> bool {
    let lower = key.to_ascii_lowercase();
    !["cdelt", "crpix", "crval", "ctype", "cunit"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
        && ![
            "bunit",
            "object",
            "imtype",
            "observer",
            "telescope",
            "date-obs",
            "epoch",
            "equinox",
            "projection",
            "reffreqtype",
            "restfreq",
            "masks",
            "shape",
            "datamin",
            "datamax",
            "minpixpos",
            "maxpixpos",
            "minpos",
            "maxpos",
            "beammajor",
            "beamminor",
            "beampa",
            "bmaj",
            "bmin",
            "bpa",
        ]
        .contains(&lower.as_str())
}

/// Read a user-defined scalar field, preserving its stored casacore type.
/// CASA checks the exact field name first and then its lowercase spelling.
///
/// # Errors
///
/// Returns an error for a missing field, a type CASA cannot read, an
/// unsupported field type, or an unreadable image.
pub fn imhead_custom_value(path: impl AsRef<Path>, key: &str) -> Result<Value, ImageError> {
    let path = path.as_ref();
    if path.is_file() {
        return imhead_fits::custom_value(path, key);
    }
    fn get<T: ImagePixel>(image: &PagedImage<T>, key: &str) -> Result<Value, ImageError> {
        let info = image.misc_info();
        let value = info
            .get(key)
            .or_else(|| info.get(&key.to_ascii_lowercase()))
            .ok_or_else(|| imhead_unknown_keyword(key))?;
        match value {
            Value::Scalar(ScalarValue::String(_))
            | Value::Scalar(ScalarValue::Int32(_))
            | Value::Scalar(ScalarValue::Float64(_))
            | Value::Record(_) => Ok(value.clone()),
            Value::Scalar(ScalarValue::Bool(_)) => Err(ImageError::InvalidMetadata(
                "Unhandled data type Bool for user defined type. Send us a bug report".into(),
            )),
            Value::Array(array)
                if matches!(
                    array,
                    casa_core::ArrayValue::Bool(_)
                        | casa_core::ArrayValue::Int32(_)
                        | casa_core::ArrayValue::Float64(_)
                        | casa_core::ArrayValue::String(_)
                ) =>
            {
                let kind = match array {
                    casa_core::ArrayValue::Bool(_) => "Bool",
                    casa_core::ArrayValue::Int32(_) => "Int",
                    casa_core::ArrayValue::Float64(_) => "double",
                    casa_core::ArrayValue::String(_) => "String",
                    _ => unreachable!(),
                };
                Err(ImageError::InvalidMetadata(format!(
                    "Unhandled data type Array<{kind}> for user defined type. Send us a bug report"
                )))
            }
            _ => Err(ImageError::InvalidMetadata(format!(
                "Reading this user-defined value is not implemented (#977): {key}"
            ))),
        }
    }
    let image = AnyPagedImage::open(path)?;
    match &image {
        AnyPagedImage::Float32(image) => get(image, key),
        AnyPagedImage::Float64(image) => get(image, key),
        AnyPagedImage::Complex32(image) => get(image, key),
        AnyPagedImage::Complex64(image) => get(image, key),
    }
}

pub(super) fn imhead_unknown_keyword(key: &str) -> ImageError {
    ImageError::InvalidMetadata(format!(
        "Unknown keyword {key}. If you are trying to use a key name from the imhead summary \
         dictionary, note that some keys in mode='put'/'get' are different from mode='summary'. \
         Please see imhead description in the CASA online documentation for complete details."
    ))
}

/// Write or remove user-defined scalar, record, and array metadata.
///
/// `mode` is `"add"`, `"put"`, or `"del"`. Adds refuse an existing field;
/// puts preserve its existing type. Values may be strings, booleans, 32-bit
/// integers, finite doubles, or rectangular arrays of those. Deleting ignores `value`.
///
/// # Errors
///
/// Returns an error for a reserved key, unsupported value, duplicate add,
/// missing delete, incompatible existing field, or image I/O failure. The
/// metadata may already be updated if appending image history fails, as in
/// CASA's metadata writer.
pub fn imhead_custom_value_change(
    path: impl AsRef<Path>,
    mode: &str,
    key: &str,
    value: &ImheadValue,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    if !imhead_is_custom_key(key) || !["add", "put", "del"].contains(&mode) {
        return Err(ImageError::InvalidMetadata(format!(
            "Unsupported custom metadata operation {mode:?} for {key:?} (#977)"
        )));
    }
    let (stored, rendered) = if mode == "del" {
        (
            Value::Scalar(ScalarValue::String(String::new())),
            String::new(),
        )
    } else {
        match value {
            ImheadValue::String(value) => (
                Value::Scalar(ScalarValue::String(value.clone())),
                format!("\"{value}\""),
            ),
            ImheadValue::Int(value) => {
                (Value::Scalar(ScalarValue::Int32(*value)), value.to_string())
            }
            ImheadValue::Float(value) => (
                Value::Scalar(ScalarValue::Float64(*value)),
                self::projection_header::cpp_number_precision(*value, 12),
            ),
            ImheadValue::Bool(value) => (
                Value::Scalar(ScalarValue::Bool(*value)),
                if *value { "1" } else { "0" }.to_string(),
            ),
            ImheadValue::Array(array) => {
                if matches!(array, casa_core::ArrayValue::Complex64(_)) {
                    return Err(ImageError::InvalidMetadata(format!(
                        "Unhandled value type Array<DComplex> for {key}"
                    )));
                }
                let rendered = custom_values::format_array(array);
                (Value::Array(array.clone()), rendered)
            }
            ImheadValue::Record(record) => {
                let rendered = custom_values::format_record(record);
                (Value::Record(record.clone()), rendered)
            }
            _ => {
                return Err(ImageError::InvalidMetadata(
                    "This user-defined value type is not implemented (#977)".into(),
                ));
            }
        }
    };
    fn change<T: ImagePixel>(
        image: &mut PagedImage<T>,
        mode: &str,
        key: &str,
        value: &Value,
        rendered: &str,
    ) -> Result<String, ImageError> {
        let mut info = image.misc_info();
        let lower = key.to_ascii_lowercase();
        let existing = if info.get(key).is_some() {
            Some(key)
        } else if info.get(&lower).is_some() {
            Some(lower.as_str())
        } else {
            None
        };
        let message = match mode {
            "add" if existing.is_some() => {
                return Err(ImageError::InvalidMetadata(format!(
                    "Keyword {key} already exists so cannot be added."
                )));
            }
            "del" => {
                let name = existing.ok_or_else(|| {
                    ImageError::InvalidMetadata(format!("Unknown keyword {lower}"))
                })?;
                info.remove(name);
                format!("Removed user-defined keyword {name}")
            }
            _ => {
                if info.get(key).is_some_and(|old| {
                    old.kind() != value.kind() || old.type_tag() != value.type_tag()
                }) {
                    return Err(ImageError::InvalidMetadata(format!(
                        "incorrect data type used for field {key}"
                    )));
                }
                info.upsert(key, value.clone());
                format!("Added {key} = {rendered}")
            }
        };
        image.set_misc_info(info)?;
        Ok(message)
    }
    let mut image = AnyPagedImage::open(path)?;
    let message = match &mut image {
        AnyPagedImage::Float32(image) => change(image, mode, key, &stored, &rendered)?,
        AnyPagedImage::Float64(image) => change(image, mode, key, &stored, &rendered)?,
        AnyPagedImage::Complex32(image) => change(image, mode, key, &stored, &rendered)?,
        AnyPagedImage::Complex64(image) => change(image, mode, key, &stored, &rendered)?,
    };
    image.save()?;
    drop(image);
    let origin = if mode == "del" {
        "ImageMetaDataRW::remove"
    } else {
        "ImageMetaDataRW::_setUserDefined"
    };
    casa_images::add_image_history(path, origin, &[message])?;
    imhead(path)
}

pub(super) fn imhead_change_string(
    path: impl AsRef<Path>,
    mode: &str,
    hdkey: &str,
    hdvalue: &str,
) -> Result<ImageHeaderSummary, ImageError> {
    let path = path.as_ref();
    if imhead_is_custom_key(hdkey) {
        return imhead_custom_value_change(
            path,
            mode,
            hdkey,
            &ImheadValue::String(hdvalue.to_string()),
        );
    }
    validate_imhead_image(path)?;
    let key = hdkey.to_ascii_lowercase();
    if !["bunit", "object", "observer", "telescope"].contains(&key.as_str())
        && !(mode == "put" && key == "imtype")
    {
        return Err(ImageError::InvalidMetadata(format!(
            "imhead mode={mode:?} does not yet support hdkey={hdkey:?} (#977)"
        )));
    }
    let mut image = AnyPagedImage::open(path)?;
    let old = match &image {
        AnyPagedImage::Float32(image) => header_string(image, &key)?,
        AnyPagedImage::Float64(image) => header_string(image, &key)?,
        AnyPagedImage::Complex32(image) => header_string(image, &key)?,
        AnyPagedImage::Complex64(image) => header_string(image, &key)?,
    };
    if mode == "add" && !old.is_empty() {
        return Err(ImageError::InvalidMetadata(format!(
            "{hdkey} is already present and has value {old}. It may be modified but not added."
        )));
    }
    let value = if key == "bunit" {
        let unit = casa_core::quanta::unit::normalize_casa_unit_name(hdvalue);
        casa_core::quanta::Unit::new(&unit)
            .map_err(|error| ImageError::InvalidMetadata(error.to_string()))?;
        unit
    } else if key == "imtype" {
        hdvalue
            .parse::<ImageType>()
            .ok()
            .filter(|kind| kind.to_string().eq_ignore_ascii_case(hdvalue))
            .unwrap_or(ImageType::Intensity)
            .to_string()
    } else {
        hdvalue.to_string()
    };
    match &mut image {
        AnyPagedImage::Float32(image) => put_header_string(image, &key, &value)?,
        AnyPagedImage::Float64(image) => put_header_string(image, &key, &value)?,
        AnyPagedImage::Complex32(image) => put_header_string(image, &key, &value)?,
        AnyPagedImage::Complex64(image) => put_header_string(image, &key, &value)?,
    }
    image.save()?;
    drop(image);
    let origin = match (mode, key.as_str()) {
        ("del", _) => "ImageMetaDataRW::remove",
        ("add", "bunit") => "ImageMetaDataRW::add",
        _ => "ImageMetaDataRW::set",
    };
    let history_key = if mode == "put" { hdkey } else { &key };
    casa_images::add_image_history(
        path,
        origin,
        &[format!(
            "Modified {history_key} from \"{old}\" to \"{hdvalue}\""
        )],
    )?;
    imhead(path)
}

/// Validate image metadata required when CASA opens an image for `imhead`.
///
/// # Errors
///
/// Returns an error for an unreadable image or an invalid restoring-beam set.
pub fn validate_imhead_image(path: impl AsRef<Path>) -> Result<(), ImageError> {
    let path = path.as_ref();
    if path.is_file() {
        PrimaryHdu::open(path)?;
        return Ok(());
    }
    fn validate<T: ImagePixel>(image: &PagedImage<T>) -> Result<(), ImageError> {
        let info = image.image_info()?;
        per_plane_beam_summary(image, &info.beam_set)?;
        Ok(())
    }
    match &AnyPagedImage::open(path)? {
        AnyPagedImage::Float32(image) => validate(image),
        AnyPagedImage::Float64(image) => validate(image),
        AnyPagedImage::Complex32(image) => validate(image),
        AnyPagedImage::Complex64(image) => validate(image),
    }
}

/// Validate metadata and require a writable CASA image for an imhead edit.
///
/// # Errors
///
/// Returns an error for unreadable metadata or a read-only FITS image.
pub fn validate_imhead_writable(path: impl AsRef<Path>) -> Result<(), ImageError> {
    let path = path.as_ref();
    validate_imhead_image(path)?;
    if path.is_file() {
        return Err(ImageError::ReadOnly("FITS image"));
    }
    Ok(())
}

pub(super) fn header_string<T: ImagePixel>(
    image: &PagedImage<T>,
    key: &str,
) -> Result<String, ImageError> {
    Ok(match key {
        "bunit" => image.units().to_string(),
        "object" => image.image_info()?.object_name,
        "imtype" => header_image_type(image)?.to_string(),
        "observer" => image.coordinates().obs_info().observer.clone(),
        "telescope" => image.coordinates().obs_info().telescope.clone(),
        _ => unreachable!("the caller checked the key"),
    })
}

// casacore defaults ImageInfo to Intensity when its record or type is absent.
pub(super) fn header_image_type<T: ImagePixel>(
    image: &PagedImage<T>,
) -> Result<ImageType, ImageError> {
    if image.has_image_type_record() {
        Ok(image.image_info()?.image_type)
    } else {
        Ok(ImageType::Intensity)
    }
}

pub(super) fn put_header_string<T: ImagePixel>(
    image: &mut PagedImage<T>,
    key: &str,
    value: &str,
) -> Result<(), ImageError> {
    match key {
        "bunit" => image.set_units(value),
        "object" => {
            image.set_image_info_string("objectname", value);
            Ok(())
        }
        "imtype" => {
            image.set_image_info_string("imagetype", value);
            Ok(())
        }
        "observer" | "telescope" => image.set_observation_string(key, value),
        _ => unreachable!("the caller checked the key"),
    }
}
