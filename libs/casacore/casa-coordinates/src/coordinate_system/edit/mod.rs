// SPDX-License-Identifier: LGPL-3.0-or-later
//! Edits of coordinate records that preserve their original axis maps.

use casa_core::{ArrayValue, RecordValue, ScalarValue, Value};

use super::{CoordinateSystem, RecordCoordinate};
use crate::{Coordinate, CoordinateError, CoordinateType, ProjectionType};

fn recorded_axis(
    coordinates: &[RecordCoordinate],
    axis: usize,
    pixel: bool,
    naxes: usize,
) -> Result<(&RecordCoordinate, usize), CoordinateError> {
    coordinates
        .iter()
        .find_map(|coordinate| {
            let axes = if pixel {
                &coordinate.pixel_axes
            } else {
                &coordinate.world_axes
            };
            let local = axes.iter().position(|&placed| placed == Some(axis))?;
            Some((coordinate, local))
        })
        .ok_or(CoordinateError::AxisOutOfRange { index: axis, naxes })
}

fn edit_nested_axis<T>(
    record: &mut RecordValue,
    field: &str,
    edit: impl FnOnce(&mut RecordValue) -> Result<(CoordinateSystem, T), CoordinateError>,
) -> Result<(CoordinateSystem, T), CoordinateError> {
    let mut updated = record.clone();
    let Some(Value::Record(nested)) = updated.get_mut(field) else {
        return Err(CoordinateError::InvalidRecord(
            "missing nested coordinate record".into(),
        ));
    };
    let (_, previous) = edit(nested)?;
    let system = CoordinateSystem::from_record(&updated)?;
    *record = updated;
    Ok((system, previous))
}

/// Casacore's Projection constructor resizes a short vector without copying
/// its original contents. Vectors already long enough retain all entries.
fn apply_casa_projection_defaults(kind: ProjectionType, parameters: &mut Vec<f64>) {
    let defaults: &[f64] = match kind {
        ProjectionType::AZP | ProjectionType::SIN => &[0.0, 0.0],
        ProjectionType::SZP => &[0.0, 0.0, 90.0],
        ProjectionType::AIR => &[90.0],
        _ => &[],
    };
    if parameters.len() < defaults.len() {
        parameters.clear();
        parameters.extend_from_slice(defaults);
    }
}

/// Store WCSLIB's resolved latitude of the native pole after changing a
/// direction's projection or reference value.
fn refresh_direction_pole(record: &mut RecordValue) -> Result<(), CoordinateError> {
    let direction = crate::DirectionCoordinate::from_record(record)?;
    validate_direction_increments(&direction)?;
    let pole = direction.native_pole()[1];
    direction.validate_native_pole()?;
    let casa_style = record.get("units").is_some()
        || record.get("conversionSystem").is_some()
        || record.get("axes").is_some();
    record.upsert(
        "latpole",
        Value::Scalar(ScalarValue::Float64(if casa_style {
            pole.to_degrees()
        } else {
            pole
        })),
    );
    Ok(())
}

fn validate_direction_increments(
    direction: &crate::DirectionCoordinate,
) -> Result<(), CoordinateError> {
    if direction
        .increment()
        .iter()
        .any(|value| !value.is_finite() || *value == 0.0)
    {
        return Err(CoordinateError::InvalidRecord(
            "direction increments must be finite and nonzero".into(),
        ));
    }
    Ok(())
}

/// Casacore applies every direction reference value when any coordinate's
/// reference value changes. SFL moves latitude into the reference pixel.
fn rebuild_direction_reference(record: &mut RecordValue) -> Result<(), CoordinateError> {
    normalize_direction_matrix(record)?;
    let sfl = matches!(record.get("projection"), Some(Value::Scalar(ScalarValue::String(name))) if name.eq_ignore_ascii_case("SFL"));
    if sfl {
        let crval = crate::record_utils::get_required_vec_f64(record, "crval")?;
        let cdelt = crate::record_utils::get_required_vec_f64(record, "cdelt")?;
        let crpix = crate::record_utils::get_required_vec_f64(record, "crpix")?;
        if crval.len() != 2 || cdelt.len() != 2 || crpix.len() != 2 {
            return Err(CoordinateError::InvalidRecord(
                "invalid direction reference values".into(),
            ));
        }
        if crval[1] != 0.0 && cdelt[1] != 0.0 {
            let pixel = crpix[1] - crval[1] / cdelt[1];
            if !pixel.is_finite() {
                return Err(CoordinateError::InvalidRecord(
                    "SFL reference pixel is not finite (#977)".into(),
                ));
            }
            let Some(Value::Array(ArrayValue::Float64(values))) = record.get_mut("crpix") else {
                return Err(CoordinateError::InvalidRecord(
                    "invalid direction crpix".into(),
                ));
            };
            if values.shape() != [2] {
                return Err(CoordinateError::InvalidRecord(
                    "invalid direction crpix".into(),
                ));
            }
            values[[1]] = pixel;
            let Some(Value::Array(ArrayValue::Float64(values))) = record.get_mut("crval") else {
                return Err(CoordinateError::InvalidRecord(
                    "invalid direction crval".into(),
                ));
            };
            if values.shape() != [2] {
                return Err(CoordinateError::InvalidRecord(
                    "invalid direction crval".into(),
                ));
            }
            values[[1]] = 0.0;
        }
    }
    refresh_direction_pole(record)
}

/// Match casacore's direction-coordinate restore, which normalizes the rows
/// of its WCS PC matrix and absorbs their lengths into the increments. CASA
/// records store this matrix transposed relative to WCS.
fn normalize_direction_matrix(record: &mut RecordValue) -> Result<(), CoordinateError> {
    let mut matrix = match record.get("pc").cloned() {
        None => return Ok(()),
        Some(Value::Array(ArrayValue::Float64(matrix))) => matrix,
        _ => {
            return Err(CoordinateError::InvalidRecord(
                "unsupported direction PC matrix (#977)".into(),
            ));
        }
    };
    if matrix.shape() != [2, 2] {
        return Err(CoordinateError::InvalidRecord(
            "invalid direction PC matrix".into(),
        ));
    }
    let Some(Value::Array(ArrayValue::Float64(mut increments))) = record.get("cdelt").cloned()
    else {
        return Err(CoordinateError::InvalidRecord(
            "invalid direction increments".into(),
        ));
    };
    if increments.shape() != [2] {
        return Err(CoordinateError::InvalidRecord(
            "invalid direction increments".into(),
        ));
    }
    for axis in 0..2 {
        let norm_squared = matrix[[0, axis]].powi(2) + matrix[[1, axis]].powi(2);
        if norm_squared != 0.0 && norm_squared != 1.0 {
            let diagonal = matrix[[axis, axis]];
            if diagonal == 0.0 || !norm_squared.is_finite() {
                return Err(CoordinateError::InvalidRecord(
                    "unsupported direction PC normalization (#977)".into(),
                ));
            }
            let norm = norm_squared.sqrt().copysign(diagonal);
            matrix[[0, axis]] /= norm;
            matrix[[1, axis]] /= norm;
            increments[[axis]] *= norm;
        }
    }
    record.upsert("pc", Value::Array(ArrayValue::Float64(matrix)));
    record.upsert("cdelt", Value::Array(ArrayValue::Float64(increments)));
    Ok(())
}

fn rebuild_directions(record: &mut RecordValue, fields: &[String]) -> Result<(), CoordinateError> {
    for field in fields {
        let Some(Value::Record(direction)) = record.get_mut(field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing direction coordinate".into(),
            ));
        };
        rebuild_direction_reference(direction)?;
    }
    Ok(())
}

/// Edit a spectral lookup table in its displayed units, preserving
/// unrelated outer and table metadata.
fn tabular_edit(field: &str, value: f64) -> Result<crate::tabular::TabularEdit, CoordinateError> {
    use crate::tabular::TabularEdit;
    Ok(match field {
        "crpix" => TabularEdit::ReferencePixel(value),
        "crval" => TabularEdit::ReferenceValue(value),
        "cdelt" => TabularEdit::Increment(value),
        _ => {
            return Err(CoordinateError::InvalidRecord(format!(
                "invalid spectral field {field}"
            )));
        }
    })
}

fn copy_tabular_fields(
    target: &mut RecordValue,
    source: &RecordValue,
) -> Result<(), CoordinateError> {
    for key in [
        "crval",
        "crpix",
        "cdelt",
        "pc",
        "axes",
        "units",
        "pixelvalues",
        "worldvalues",
    ] {
        let value = source
            .get(key)
            .ok_or_else(|| CoordinateError::InvalidRecord(format!("missing tabular {key}")))?;
        target.upsert(key, value.clone());
    }
    Ok(())
}

fn edit_spectral_table(
    record: &mut RecordValue,
    field: &str,
    value: f64,
) -> Result<f64, CoordinateError> {
    use crate::spectral::SpectralCoordinate;
    let mut spectral = SpectralCoordinate::from_record(record)?;
    let old = spectral.edit_tabular(tabular_edit(field, value)?)?;
    let generated = spectral.to_record();
    let Some(Value::Record(source)) = generated.get("tabular") else {
        return Err(CoordinateError::InvalidRecord(
            "missing generated spectral table".into(),
        ));
    };
    let Some(Value::Record(target)) = record.get_mut("tabular") else {
        return Err(CoordinateError::InvalidRecord(
            "missing spectral table".into(),
        ));
    };
    copy_tabular_fields(target, source)?;
    Ok(old)
}

fn edit_generic_table(
    record: &mut RecordValue,
    field: &str,
    value: f64,
) -> Result<f64, CoordinateError> {
    let mut table = crate::TabularCoordinate::from_record(record)?;
    let old = table.edit(tabular_edit(field, value)?)?;
    copy_tabular_fields(record, &table.to_record())?;
    Ok(old)
}

/// Match casacore's restore: clamp the list and select the entry nearest to
/// the stored selector. Keep that active value consistent with the list.
fn select_active_rest_frequency(
    record: &mut RecordValue,
) -> Result<(f64, Option<usize>), CoordinateError> {
    let raw = crate::record_utils::get_optional_f64(record, "restfreq")
        .ok_or_else(|| CoordinateError::InvalidRecord("missing rest frequency".into()))?;
    if !raw.is_finite() || raw < 0.0 {
        return Err(CoordinateError::InvalidRecord(
            "invalid rest frequency".into(),
        ));
    }
    let (active, index) = if let Some(field) = record.get("restfreqs") {
        if matches!(field, Value::Array(array) if array.shape().len() != 1) {
            return Err(CoordinateError::InvalidRecord(
                "invalid rest frequency list".into(),
            ));
        }
        let mut values = crate::record_utils::get_optional_vec_f64(record, "restfreqs")
            .ok_or_else(|| CoordinateError::InvalidRecord("invalid rest frequency list".into()))?;
        if values.is_empty() || values.iter().any(|v| !v.is_finite()) {
            return Err(CoordinateError::InvalidRecord(
                "invalid rest frequency list".into(),
            ));
        }
        for value in values.iter_mut() {
            *value = value.max(0.0);
        }
        let index =
            crate::spectral::closest_rest_frequency(&values, raw).expect("nonempty frequency list");
        let active = values[index];
        record.upsert("restfreqs", Value::Array(ArrayValue::from_f64_vec(values)));
        (active, Some(index))
    } else {
        (raw.max(0.0), None)
    };
    record.upsert("restfreq", Value::Scalar(ScalarValue::Float64(active)));
    Ok((active, index))
}

fn scale_axis_field(
    record: &mut RecordValue,
    field: &str,
    axis: Option<usize>,
    factor: f64,
) -> Result<(), CoordinateError> {
    let scale = |value: &mut f64| {
        let scaled = *value * factor;
        if !scaled.is_finite() || (*value != 0.0 && scaled == 0.0) {
            return Err(CoordinateError::InvalidRecord(format!(
                "scaling {field} is not supported (#977)"
            )));
        }
        *value = scaled;
        Ok(())
    };
    match record.get_mut(field) {
        Some(Value::Scalar(ScalarValue::Float64(value))) if axis.is_none() || axis == Some(0) => {
            scale(value)
        }
        Some(Value::Array(ArrayValue::Float64(values)))
            if values.ndim() == 1 && !values.is_empty() =>
        {
            if let Some(axis) = axis {
                let value = values.iter_mut().nth(axis).ok_or_else(|| {
                    CoordinateError::InvalidRecord(format!("{field} is missing an axis"))
                })?;
                scale(value)
            } else {
                values.iter_mut().try_for_each(scale)
            }
        }
        _ => Err(CoordinateError::InvalidRecord(format!(
            "invalid {field} record"
        ))),
    }
}

impl CoordinateSystem {
    /// Set the first direction coordinate's native reference frame while
    /// retaining its reference-conversion frame and stored axis maps.
    ///
    /// Returns the previous frame, or `None` for a no-op. Frames whose
    /// conversion requires unavailable context are rejected (#977).
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown or unsupported frame or coordinate.
    pub fn set_direction_reference_in_record(
        record: &mut RecordValue,
        reference: &str,
    ) -> Result<(Self, Option<String>), CoordinateError> {
        use casa_core::measures::MeasFrame;
        use casa_core::measures::direction::{DirectionRef, MDirection, casa_direction_frame_name};
        use std::str::FromStr;
        let system = Self::from_record(record)?;
        if system
            .coordinates_in_record(record)?
            .iter()
            .filter(|coordinate| coordinate.field.starts_with("direction"))
            .count()
            > 1
        {
            return Err(CoordinateError::InvalidRecord(
                "multiple direction coordinates are unsupported (#977)".into(),
            ));
        }
        let (field, direction) = Self::first_direction_in_record(record)?
            .ok_or_else(|| CoordinateError::InvalidRecord("no direction coordinate".into()))?;
        let canonical = casa_direction_frame_name(reference).ok_or_else(|| {
            CoordinateError::InvalidRecord(format!(
                "unknown direction reference frame {reference:?}"
            ))
        })?;
        let target = DirectionRef::from_str(canonical).map_err(|_| {
            CoordinateError::InvalidRecord(format!(
                "direction frame {canonical} is unsupported (#977)"
            ))
        })?;
        let old = direction.direction_ref();
        if target == old {
            return Ok((system, None));
        }
        let conversion = direction.conversion_direction_ref();
        let frame = MeasFrame::new();
        let direction_value = direction.reference_value();
        for (from, to) in [(target, conversion), (conversion, target)] {
            MDirection::from_angles(direction_value[0], direction_value[1], from)
                .convert_to(to, &frame)
                .map_err(|error| {
                    CoordinateError::InvalidRecord(format!(
                        "direction conversion is unsupported: {error} (#977)"
                    ))
                })?;
        }
        let mut updated = record.clone();
        let Some(Value::Record(direction)) = updated.get_mut(&field) else {
            unreachable!()
        };
        if direction.get("direction_ref").is_some() {
            direction.upsert(
                "direction_ref",
                Value::Scalar(ScalarValue::String(canonical.into())),
            );
        }
        direction.upsert(
            "system",
            Value::Scalar(ScalarValue::String(canonical.into())),
        );
        if direction.get("conversionSystem").is_none() {
            direction.upsert(
                "conversionSystem",
                Value::Scalar(ScalarValue::String(conversion.as_str().into())),
            );
        }
        let system = Self::from_record(&updated)?;
        *record = updated;
        Ok((system, Some(old.as_str().to_string())))
    }

    /// Replace the Stokes coordinate on a pixel axis, preserving axis maps.
    ///
    /// Returns the previous Stokes names. Unsupported names and multiple
    /// polarization coordinates are refused (#977).
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid axis, count, Stokes name, or record.
    pub fn set_stokes_types_in_record(
        record: &mut RecordValue,
        pixel_axis: usize,
        names: &[String],
    ) -> Result<(Self, Vec<String>), CoordinateError> {
        Self::set_stokes_types(record, pixel_axis, names, true)
    }

    /// Replace the Stokes labels on a public world axis, preserving its maps.
    ///
    /// # Errors
    /// Returns an error for an invalid axis, count, Stokes name, or record.
    pub fn set_world_stokes_types_in_record(
        record: &mut RecordValue,
        world_axis: usize,
        names: &[String],
    ) -> Result<(Self, Vec<String>), CoordinateError> {
        Self::set_stokes_types(record, world_axis, names, false)
    }

    fn set_stokes_types(
        record: &mut RecordValue,
        axis: usize,
        names: &[String],
        pixel: bool,
    ) -> Result<(Self, Vec<String>), CoordinateError> {
        let system = Self::from_record(record)?;
        let coordinates = system.coordinates_in_record(record)?;
        if coordinates
            .iter()
            .filter(|coordinate| coordinate.field.starts_with("stokes"))
            .count()
            > 1
        {
            return Err(CoordinateError::InvalidRecord(
                "multiple polarization coordinates are unsupported (#977)".into(),
            ));
        }
        let coordinate = coordinates
            .iter()
            .find(|coordinate| {
                let axes = if pixel {
                    &coordinate.pixel_axes
                } else {
                    &coordinate.world_axes
                };
                axes.contains(&Some(axis))
            })
            .ok_or_else(|| {
                CoordinateError::InvalidRecord(
                    if pixel {
                        "invalid pixel axis"
                    } else {
                        "invalid world axis"
                    }
                    .into(),
                )
            })?;
        if !coordinate.field.starts_with("stokes") {
            return Err(CoordinateError::InvalidRecord(
                "axis is not a polarization coordinate".into(),
            ));
        }
        let Some(Value::Record(stored)) = record.get(&coordinate.field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing polarization record".into(),
            ));
        };
        let old = crate::StokesCoordinate::from_record(stored)?;
        if names.len() != old.n_stokes() {
            return Err(CoordinateError::InvalidRecord(format!(
                "expected {} polarization values",
                old.n_stokes()
            )));
        }
        let types = names
            .iter()
            .map(|name| {
                crate::StokesType::from_name(name).ok_or_else(|| {
                    CoordinateError::InvalidRecord(format!(
                        "unsupported polarization value {name:?} (#977)"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        if types
            .iter()
            .enumerate()
            .any(|(index, value)| types[..index].contains(value))
        {
            return Err(CoordinateError::InvalidRecord(
                "polarization values must be unique".into(),
            ));
        }
        let old_names = old
            .stokes()
            .iter()
            .map(|value| value.name().to_string())
            .collect();
        let mut updated = record.clone();
        updated.upsert(
            &coordinate.field,
            Value::Record(crate::StokesCoordinate::new(types).to_record()),
        );
        let system = Self::from_record(&updated)?;
        *record = updated;
        Ok((system, old_names))
    }

    /// Change the first direction coordinate's projection, preserving axis maps.
    ///
    /// Returns the old projection when it changed. CASA carries the old
    /// parameters when the vector is long enough, otherwise it uses the new
    /// projection's defaults. The direction conversion frame is preserved.
    ///
    /// # Errors
    ///
    /// Returns an error for an unknown or unsupported projection, or invalid
    /// direction metadata. The input record is unchanged on error.
    pub fn set_direction_projection_in_record(
        record: &mut RecordValue,
        projection: &str,
    ) -> Result<(Self, Option<String>), CoordinateError> {
        let system = Self::from_record(record)?;
        if system
            .coordinates_in_record(record)?
            .iter()
            .filter(|coordinate| coordinate.field.starts_with("direction"))
            .count()
            > 1
        {
            return Err(CoordinateError::InvalidRecord(
                "projection changes with multiple direction coordinates are not implemented (#977)"
                    .into(),
            ));
        }
        let (field, direction) = Self::first_direction_in_record(record)?
            .ok_or_else(|| CoordinateError::InvalidRecord("no direction coordinate".into()))?;
        let name = projection.to_ascii_uppercase();
        if name == "NCP" {
            return Err(CoordinateError::InvalidRecord(format!(
                "unknown projection {projection:?}"
            )));
        }
        let kind = ProjectionType::from_name(&name).ok_or_else(|| {
            CoordinateError::InvalidRecord(format!(
                "unknown or unsupported projection {projection:?} (#977)"
            ))
        })?;
        let previous = direction.projection();
        let old = previous.name().to_string();
        if old == name {
            return Ok((system, None));
        }
        let mut parameters = previous.parameters().to_vec();
        apply_casa_projection_defaults(previous.projection_type(), &mut parameters);
        apply_casa_projection_defaults(kind, &mut parameters);
        let mut updated = record.clone();
        let Some(Value::Record(direction)) = updated.get_mut(&field) else {
            unreachable!()
        };
        normalize_direction_matrix(direction)?;
        direction.upsert("projection", Value::Scalar(ScalarValue::String(name)));
        direction.upsert(
            "projection_parameters",
            Value::Array(ArrayValue::from_f64_vec(parameters)),
        );
        refresh_direction_pole(direction)?;
        let system = Self::from_record(&updated)?;
        *record = updated;
        Ok((system, Some(old)))
    }

    /// Set the native spectral reference frame and reset its conversion frame.
    ///
    /// Returns the previous frame when it changed, or `None` for a no-op.
    /// Unrelated coordinate metadata and axis maps are retained.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid frame, missing spectral coordinate,
    /// or malformed metadata.
    pub fn set_spectral_reference_in_record(
        record: &mut RecordValue,
        reference: &str,
    ) -> Result<(Self, Option<String>), CoordinateError> {
        use casa_core::measures::frequency::CasaFrequencyFrame;
        use std::str::FromStr;
        let system = Self::from_record(record)?;
        let (field, spectral) = Self::first_spectral_in_record(record)?
            .ok_or_else(|| CoordinateError::InvalidRecord("no spectral coordinate".into()))?;
        let old = if spectral.has_undefined_frame() {
            "Undefined".to_string()
        } else {
            spectral.frequency_ref().as_str().to_string()
        };
        let canonical = CasaFrequencyFrame::from_str(reference)
            .map_err(|_| {
                CoordinateError::InvalidRecord(format!(
                    "unknown spectral reference frame {reference:?}"
                ))
            })?
            .as_str();
        if old == canonical {
            return Ok((system, None));
        }
        let mut updated = record.clone();
        let Some(Value::Record(spectral)) = updated.get_mut(&field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing spectral record".into(),
            ));
        };
        if spectral.get("frequency_ref").is_some() {
            spectral.upsert(
                "frequency_ref",
                Value::Scalar(ScalarValue::String(canonical.into())),
            );
        }
        spectral.upsert(
            "system",
            Value::Scalar(ScalarValue::String(canonical.into())),
        );
        if let Some(conversion) = spectral.get_mut("conversion") {
            let Value::Record(conversion) = conversion else {
                return Err(CoordinateError::InvalidRecord(
                    "invalid spectral conversion record".into(),
                ));
            };
            conversion.upsert(
                "system",
                Value::Scalar(ScalarValue::String(canonical.into())),
            );
        }
        let system = Self::from_record(&updated)?;
        *record = updated;
        Ok((system, Some(old)))
    }
    /// Change an axis's world unit and rescale its stored coordinate values.
    ///
    /// Returns the updated system and old unit. Unrelated metadata and axis
    /// maps are preserved; the original record changes only on success.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid axis or unit, nonconformant units,
    /// malformed metadata, or a coordinate layout not yet supported.
    pub fn set_axis_unit_in_record(
        record: &mut RecordValue,
        pixel_axis: usize,
        unit: &str,
    ) -> Result<(Self, String), CoordinateError> {
        Self::set_axis_unit(record, pixel_axis, unit, true)
    }

    /// Update the unit addressed by a public world axis while retaining the
    /// coordinate's original native record and maps.
    ///
    /// # Errors
    /// Returns an error for an invalid axis, unit, or coordinate record.
    pub fn set_world_axis_unit_in_record(
        record: &mut RecordValue,
        world_axis: usize,
        unit: &str,
    ) -> Result<(Self, String), CoordinateError> {
        Self::set_axis_unit(record, world_axis, unit, false)
    }

    fn set_axis_unit(
        record: &mut RecordValue,
        axis: usize,
        unit: &str,
        pixel: bool,
    ) -> Result<(Self, String), CoordinateError> {
        use casa_core::quanta::casacore::casacore_unit;
        let system = Self::from_record(record)?;
        let naxes = if pixel {
            system.n_pixel_axes()
        } else {
            system.n_world_axes()
        };
        let world = if pixel {
            system.pixel_axis_to_world_axis(axis)
        } else {
            Some(axis)
        };
        let (index, local) = world
            .and_then(|world| system.world_axis_owner(world))
            .ok_or(CoordinateError::AxisOutOfRange { index: axis, naxes })?;
        let model = system.coordinate(index);
        let coordinates = system.coordinates_in_record(record)?;
        let (coordinate, local_axis) = recorded_axis(&coordinates, axis, pixel, naxes)?;
        if model.coordinate_type() == CoordinateType::System {
            return edit_nested_axis(record, &coordinate.field, |nested| {
                Self::set_axis_unit(nested, local_axis, unit, pixel)
            });
        }
        let old = match model {
            crate::CoordinateModel::Direction(direction) => direction.visible_world_units(),
            _ => model.axis_units(),
        }[local]
            .clone();
        if matches!(
            model.coordinate_type(),
            CoordinateType::Stokes | CoordinateType::Quality
        ) {
            return Ok((system, old));
        }
        let before = casacore_unit(&old)
            .map_err(|error| CoordinateError::InvalidRecord(error.to_string()))?;
        let after = casacore_unit(unit)
            .map_err(|error| CoordinateError::InvalidRecord(error.to_string()))?;
        let native = casa_core::quanta::Unit::new(unit).map_err(|_| {
            CoordinateError::InvalidRecord(
                "this unit spelling is not supported by the coordinate reader (#977)".into(),
            )
        })?;
        if native.val().dim != after.val().dim
            || native.val().factor.to_bits() != after.val().factor.to_bits()
        {
            return Err(CoordinateError::InvalidRecord(
                "this unit spelling is not supported by the coordinate reader (#977)".into(),
            ));
        }
        if before.val().dim != after.val().dim {
            return Err(CoordinateError::InvalidRecord(
                "Units are not compatible dimensionally".into(),
            ));
        }
        let factor = before.val().factor / after.val().factor;
        if !factor.is_normal() {
            return Err(CoordinateError::InvalidRecord(
                "unit scale is not supported (#977)".into(),
            ));
        }
        let mut updated = record.clone();
        let Some(Value::Record(target)) = updated.get_mut(&coordinate.field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing coordinate record".into(),
            ));
        };
        if coordinate.field.starts_with("spectral") {
            select_active_rest_frequency(target)?;
            for field in ["restfreq", "restfreqs"] {
                if target.get(field).is_some() {
                    scale_axis_field(target, field, None, factor)?;
                }
            }
            if let Some(Value::Record(wcs)) = target.get_mut("wcs") {
                for field in ["crval", "cdelt"] {
                    scale_axis_field(wcs, field, Some(0), factor)?;
                }
            } else if let Some(Value::Record(tabular)) = target.get_mut("tabular") {
                // The nested table uses its own frequency unit; casacore
                // serializes it in Hz regardless of the displayed unit.
                let table_unit = crate::record_utils::get_optional_vec_string(tabular, "units")
                    .and_then(|units| units.into_iter().next())
                    .unwrap_or_else(|| "Hz".into());
                let unit = casacore_unit(&table_unit)
                    .map_err(|error| CoordinateError::InvalidRecord(error.to_string()))?;
                let hz = casacore_unit("Hz").expect("built-in frequency unit");
                if !unit.conformant(&hz) {
                    return Err(CoordinateError::InvalidRecord(
                        "invalid tabular frequency unit".into(),
                    ));
                }
                let to_hz = unit.val().factor / hz.val().factor;
                for field in ["crval", "cdelt"] {
                    scale_axis_field(tabular, field, Some(0), to_hz)?;
                }
                scale_axis_field(tabular, "worldvalues", None, to_hz)?;
                tabular.upsert(
                    "units",
                    Value::Array(ArrayValue::from_string_vec(vec!["Hz".into()])),
                );
            } else {
                return Err(CoordinateError::InvalidRecord(
                    "missing spectral WCS".into(),
                ));
            }
            target.upsert(
                "unit",
                Value::Scalar(ScalarValue::String(after.name().into())),
            );
        } else {
            for field in ["crval", "cdelt"] {
                scale_axis_field(target, field, Some(local_axis), factor)?;
            }
            if coordinate.field.starts_with("tabular") {
                if !matches!(target.get("worldvalues"), Some(Value::Array(ArrayValue::Float64(values))) if values.is_empty())
                {
                    scale_axis_field(target, "worldvalues", None, factor)?;
                }
                if target.get("unit").is_some() {
                    target.upsert("unit", Value::Scalar(ScalarValue::String(unit.into())));
                }
            }
            let Some(Value::Array(ArrayValue::String(units))) = target.get_mut("units") else {
                return Err(CoordinateError::InvalidRecord(
                    "coordinate units are missing".into(),
                ));
            };
            let axis_unit = units.iter_mut().nth(local_axis).ok_or_else(|| {
                CoordinateError::InvalidRecord("coordinate units are missing an axis".into())
            })?;
            *axis_unit = unit.into();
        }
        let system = Self::from_record(&updated)?;
        for index in 0..system.n_coordinates() {
            if let crate::CoordinateModel::Direction(direction) = system.coordinate(index)
                && direction.increment().iter().any(|value| !value.is_normal())
            {
                return Err(CoordinateError::InvalidRecord(
                    "coordinate unit would make the direction increment invalid (#977)".into(),
                ));
            }
        }
        *record = updated;
        Ok((system, old))
    }
    /// Set the active rest frequency in the spectral coordinate's stored unit.
    ///
    /// Other listed rest frequencies and the coordinate's original mappings
    /// remain intact. Zero leaves the rest frequency unspecified. Returns the
    /// updated system and previous active value.
    ///
    /// # Errors
    ///
    /// Returns an error for missing or malformed spectral metadata, or a
    /// negative or non-finite frequency.
    pub fn set_rest_frequency_in_record(
        record: &mut RecordValue,
        value: f64,
    ) -> Result<(Self, f64), CoordinateError> {
        if !value.is_finite() || value < 0.0 {
            return Err(CoordinateError::InvalidRecord(
                "rest frequency must be non-negative and finite".into(),
            ));
        }
        let (field, _) = Self::first_spectral_in_record(record)?
            .ok_or_else(|| CoordinateError::InvalidRecord("no spectral coordinate".into()))?;
        let mut updated = record.clone();
        let Some(Value::Record(spectral)) = updated.get_mut(&field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing spectral record".into(),
            ));
        };
        let (old, active_index) = select_active_rest_frequency(spectral)?;
        if let Some(index) = active_index {
            let Some(Value::Array(ArrayValue::Float64(values))) = spectral.get_mut("restfreqs")
            else {
                unreachable!("selector validated the frequency list")
            };
            *values
                .iter_mut()
                .nth(index)
                .expect("selected frequency exists") = value;
        }
        spectral.upsert("restfreq", Value::Scalar(ScalarValue::Float64(value)));
        let system = Self::from_record(&updated)?;
        *record = updated;
        Ok((system, old))
    }
    /// Update a stored world-axis name while preserving coordinate axis maps.
    ///
    /// Returns the updated coordinate system and the previous name. The
    /// record is changed only after the updated system parses successfully.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid axis or unsupported coordinate record.
    pub fn set_axis_name_in_record(
        record: &mut RecordValue,
        pixel_axis: usize,
        name: &str,
    ) -> Result<(Self, String), CoordinateError> {
        Self::set_axis_name(record, pixel_axis, name, true)
    }

    /// Update the name addressed by a public world axis while retaining the
    /// coordinate's original native record and maps.
    ///
    /// # Errors
    /// Returns an error for an invalid axis, name, or coordinate record.
    pub fn set_world_axis_name_in_record(
        record: &mut RecordValue,
        world_axis: usize,
        name: &str,
    ) -> Result<(Self, String), CoordinateError> {
        Self::set_axis_name(record, world_axis, name, false)
    }

    fn set_axis_name(
        record: &mut RecordValue,
        axis: usize,
        name: &str,
        pixel: bool,
    ) -> Result<(Self, String), CoordinateError> {
        let system = Self::from_record(record)?;
        let naxes = if pixel {
            system.n_pixel_axes()
        } else {
            system.n_world_axes()
        };
        let world = if pixel {
            system.pixel_axis_to_world_axis(axis)
        } else {
            Some(axis)
        };
        let (index, local) = world
            .and_then(|world| system.world_axis_owner(world))
            .ok_or(CoordinateError::AxisOutOfRange { index: axis, naxes })?;
        let model = system.coordinate(index);
        let coordinates = system.coordinates_in_record(record)?;
        let (coordinate, local_axis) = recorded_axis(&coordinates, axis, pixel, naxes)?;
        if model.coordinate_type() == CoordinateType::System {
            return edit_nested_axis(record, &coordinate.field, |nested| {
                Self::set_axis_name(nested, local_axis, name, pixel)
            });
        }
        let old = model.axis_names()[local].clone();
        let mut updated = record.clone();
        let Some(Value::Record(target)) = updated.get_mut(&coordinate.field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing coordinate record".into(),
            ));
        };
        if coordinate.field.starts_with("spectral") {
            target.upsert("name", Value::Scalar(ScalarValue::String(name.into())));
            if let Some(Value::Record(tabular)) = target.get_mut("tabular")
                && let Some(Value::Array(ArrayValue::String(names))) = tabular.get_mut("axes")
                && let Some(first) = names.iter_mut().next()
            {
                *first = name.into();
            }
        } else if coordinate.field.starts_with("tabular") {
            target.upsert("name", Value::Scalar(ScalarValue::String(name.into())));
            if let Some(Value::Array(ArrayValue::String(names))) = target.get_mut("axes")
                && let Some(first) = names.iter_mut().next()
            {
                *first = name.into();
            }
        } else {
            let Some(Value::Array(ArrayValue::String(names))) = target.get_mut("axes") else {
                return Err(CoordinateError::InvalidRecord(
                    "coordinate names are missing".into(),
                ));
            };
            let axis_name = names.iter_mut().nth(local_axis).ok_or_else(|| {
                CoordinateError::InvalidRecord("coordinate names are missing an axis".into())
            })?;
            *axis_name = name.into();
        }
        let system = Self::from_record(&updated)?;
        *record = updated;
        Ok((system, old))
    }
    /// Return the coordinate type owning a zero-based pixel axis.
    #[must_use]
    pub fn pixel_axis_type(&self, pixel_axis: usize) -> Option<CoordinateType> {
        let (coordinate, _) = self.pixel_axis_owner(pixel_axis)?;
        Some(self.coordinate(coordinate).coordinate_type())
    }

    /// Update one zero-based pixel axis's reference pixel in a stored record.
    ///
    /// Returns the updated coordinate system and the previous reference pixel.
    /// The original record's coordinate order, axis maps, and unrelated fields
    /// are preserved. The record changes only if the resulting system parses
    /// successfully. As in casacore, a polarization-axis update is a no-op.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid axis, non-finite value, malformed
    /// record, or unsupported axis layout.
    pub fn set_reference_pixel_in_record(
        record: &mut RecordValue,
        pixel_axis: usize,
        value: f64,
    ) -> Result<(Self, f64), CoordinateError> {
        Self::set_axis_scalar_in_record(record, pixel_axis, "crpix", value, true)
    }

    /// Update an axis increment in its stored world unit, preserving axis maps.
    /// A polarization-coordinate update is a no-op, as in casacore.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid axis or value, malformed metadata, or
    /// an unsupported coordinate layout.
    pub fn set_increment_in_record(
        record: &mut RecordValue,
        pixel_axis: usize,
        value: f64,
    ) -> Result<(Self, f64), CoordinateError> {
        Self::set_axis_scalar_in_record(record, pixel_axis, "cdelt", value, true)
    }

    /// Update an axis reference value in its stored world unit, preserving
    /// axis maps. A polarization-coordinate update is a no-op, as in casacore.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid axis or value, malformed metadata, or
    /// an unsupported coordinate layout.
    pub fn set_reference_value_in_record(
        record: &mut RecordValue,
        pixel_axis: usize,
        value: f64,
    ) -> Result<(Self, f64), CoordinateError> {
        Self::set_axis_scalar_in_record(record, pixel_axis, "crval", value, true)
    }

    /// Update a public world axis's reference value in its declared unit.
    ///
    /// # Errors
    /// Returns an error for an invalid axis, value, or coordinate record.
    pub fn set_world_reference_value_in_record(
        record: &mut RecordValue,
        world_axis: usize,
        value: f64,
    ) -> Result<(Self, f64), CoordinateError> {
        Self::set_axis_scalar_in_record(record, world_axis, "crval", value, false)
    }

    /// Update a public world axis's increment in its declared unit.
    ///
    /// # Errors
    /// Returns an error for an invalid axis, value, or coordinate record.
    pub fn set_world_increment_in_record(
        record: &mut RecordValue,
        world_axis: usize,
        value: f64,
    ) -> Result<(Self, f64), CoordinateError> {
        Self::set_axis_scalar_in_record(record, world_axis, "cdelt", value, false)
    }

    fn set_axis_scalar_in_record(
        record: &mut RecordValue,
        axis: usize,
        field: &str,
        value: f64,
        pixel: bool,
    ) -> Result<(Self, f64), CoordinateError> {
        if !value.is_finite() {
            return Err(CoordinateError::InvalidRecord(
                "coordinate value must be finite".into(),
            ));
        }
        let system = Self::from_record(record)?;
        let coordinates = system.coordinates_in_record(record)?;
        let naxes = if pixel {
            system.n_pixel_axes()
        } else {
            system.n_world_axes()
        };
        let (coordinate, local_axis) = recorded_axis(&coordinates, axis, pixel, naxes)?;
        let direction_fields: Vec<String> = coordinates
            .iter()
            .filter(|entry| entry.field.starts_with("direction"))
            .map(|entry| entry.field.clone())
            .collect();
        let owner = if pixel {
            system.pixel_axis_owner(axis)
        } else {
            system.world_axis_owner(axis)
        };
        let (index, local) = owner.expect("validated public coordinate axis");
        let model = system.coordinate(index);
        if model.coordinate_type() == CoordinateType::System {
            return edit_nested_axis(record, &coordinate.field, |nested| {
                Self::set_axis_scalar_in_record(nested, local_axis, field, value, pixel)
            });
        }
        if matches!(
            model.coordinate_type(),
            CoordinateType::Stokes | CoordinateType::Quality
        ) {
            let values = match field {
                "crpix" => model.reference_pixel(),
                "cdelt" => model.increment(),
                _ => model.reference_value(),
            };
            return Ok((system, values[local]));
        }
        if field == "cdelt" && value == 0.0 {
            return Err(CoordinateError::InvalidRecord(
                "axis increment cannot be zero".into(),
            ));
        }
        if coordinate.field.starts_with("spectral")
            && matches!(record.get(&coordinate.field), Some(Value::Record(inner)) if inner.get("tabular").is_some())
        {
            let mut updated = record.clone();
            let Some(Value::Record(target)) = updated.get_mut(&coordinate.field) else {
                unreachable!()
            };
            let old = edit_spectral_table(target, field, value)?;
            if field == "crval" {
                rebuild_directions(&mut updated, &direction_fields)?;
            }
            let system = Self::from_record(&updated)?;
            *record = updated;
            return Ok((system, old));
        }
        if coordinate.field.starts_with("tabular") {
            let mut updated = record.clone();
            let Some(Value::Record(target)) = updated.get_mut(&coordinate.field) else {
                unreachable!()
            };
            let old = edit_generic_table(target, field, value)?;
            if field == "crval" {
                rebuild_directions(&mut updated, &direction_fields)?;
            }
            let system = Self::from_record(&updated)?;
            *record = updated;
            return Ok((system, old));
        }
        let mut updated = record.clone();
        let Some(Value::Record(coordinate_record)) = updated.get_mut(&coordinate.field) else {
            return Err(CoordinateError::InvalidRecord(
                "missing coordinate record".into(),
            ));
        };
        let nested = if coordinate.field.starts_with("spectral") {
            if coordinate_record.get("wcs").is_some() {
                Some("wcs")
            } else if coordinate_record.get("tabular").is_some() {
                Some("tabular")
            } else {
                None
            }
        } else {
            None
        };
        let target = if let Some(name) = nested {
            match coordinate_record.get_mut(name) {
                Some(Value::Record(record)) => record,
                _ => {
                    return Err(CoordinateError::InvalidRecord(format!(
                        "invalid {name} coordinate record"
                    )));
                }
            }
        } else {
            coordinate_record
        };
        let old = match target.get_mut(field) {
            Some(Value::Scalar(ScalarValue::Float64(old))) if local_axis == 0 => {
                std::mem::replace(old, value)
            }
            Some(Value::Array(ArrayValue::Float64(values))) if values.ndim() == 1 => {
                let old = values.iter_mut().nth(local_axis).ok_or_else(|| {
                    CoordinateError::InvalidRecord(format!("{field} is missing an axis"))
                })?;
                std::mem::replace(old, value)
            }
            _ => {
                return Err(CoordinateError::InvalidRecord(format!(
                    "invalid {field} record"
                )));
            }
        };
        if field == "crval" {
            rebuild_directions(&mut updated, &direction_fields)?;
        }
        let system = Self::from_record(&updated)?;
        for index in 0..system.n_coordinates() {
            // WCSLIB accepts latitudes up to 1e-10 degrees beyond a pole.
            let pole_limit =
                std::f64::consts::FRAC_PI_2 + 1e-10_f64.to_radians() + 8.0 * f64::EPSILON;
            if field == "crval"
                && let crate::CoordinateModel::Direction(direction) = system.coordinate(index)
                && direction.projection_reference()[1].abs() > pole_limit
            {
                return Err(CoordinateError::InvalidRecord(
                    "direction latitude is out of range".into(),
                ));
            }
            if field == "cdelt"
                && let crate::CoordinateModel::Direction(direction) = system.coordinate(index)
            {
                validate_direction_increments(direction)?;
            }
        }
        *record = updated;
        Ok((system, old))
    }
}

#[cfg(test)]
mod tests;
