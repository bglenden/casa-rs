// SPDX-License-Identifier: LGPL-3.0-or-later

use super::*;

/// Structural type validation shared with the runtime.
pub fn validate_parameter_value(
    value: &ParameterValue,
    domain: &ParameterType,
) -> Result<(), String> {
    match (value, domain) {
        (value, ParameterType::Variant) => validate_variant_value(value),
        (ParameterValue::Bool(_), ParameterType::Bool)
        | (ParameterValue::Integer(_), ParameterType::Integer)
        | (ParameterValue::String(_), ParameterType::String)
        | (ParameterValue::String(_), ParameterType::Path { .. }) => Ok(()),
        (
            ParameterValue::String(value),
            ParameterType::Quantity {
                dimension,
                canonical_unit,
                special_values,
            },
        ) => validate_quantity_value(value, *dimension, canonical_unit, special_values),
        // A bare number is a quantity in its canonical unit, as CASA's
        // `threshold=0.0` is in Jy (#931).
        (ParameterValue::Integer(_), ParameterType::Quantity { .. }) => Ok(()),
        (ParameterValue::Float(value), ParameterType::Quantity { .. }) if value.is_finite() => {
            Ok(())
        }
        (ParameterValue::Integer(_), ParameterType::Float)
        | (ParameterValue::Float(_), ParameterType::Float) => Ok(()),
        (ParameterValue::String(value), ParameterType::Choice { values }) => {
            if values.iter().any(|choice| choice == value) {
                Ok(())
            } else {
                Err(format!("{value:?} is not one of {}", values.join(", ")))
            }
        }
        (
            ParameterValue::Array(values),
            ParameterType::Array {
                element,
                min_items,
                max_items,
                ..
            },
        ) => {
            if values.len() < *min_items || max_items.is_some_and(|max| values.len() > max) {
                return Err(format!(
                    "array length {} is outside {}..={}",
                    values.len(),
                    min_items,
                    max_items.map_or_else(|| "unbounded".to_string(), |max| max.to_string())
                ));
            }
            for value in values {
                validate_parameter_value(value, element)?;
            }
            Ok(())
        }
        (
            value,
            ParameterType::Array {
                element,
                allow_scalar: true,
                ..
            },
        ) => validate_parameter_value(value, element),
        (ParameterValue::Table(values), ParameterType::Table { fields }) => {
            for (name, domain) in fields {
                let value = values
                    .get(name)
                    .ok_or_else(|| format!("missing table field {name}"))?;
                validate_parameter_value(value, domain)?;
            }
            if values.keys().any(|name| !fields.contains_key(name)) {
                return Err("table contains an unknown field".to_string());
            }
            Ok(())
        }
        (ParameterValue::String(value), ParameterType::Optional { states, .. })
            if states.contains(value) =>
        {
            Ok(())
        }
        (value, ParameterType::Optional { value: domain, .. }) => {
            validate_parameter_value(value, domain)
        }
        (ParameterValue::Float(value), _) if !value.is_finite() => {
            Err("non-finite numbers are forbidden".to_string())
        }
        _ => Err(format!("value {value:?} does not match {domain:?}")),
    }
}

pub(super) fn validate_variant_value(value: &ParameterValue) -> Result<(), String> {
    // Each typed array or record introduces two JSON containers. Leave room
    // for request and profile envelopes under serde_json's recursion limit.
    if value.requires_typed_transport() && value.nesting_depth() > 48 {
        return Err("typed values nested beyond 48 levels are unsupported (#977)".into());
    }
    fn validate(value: &ParameterValue) -> Result<(), String> {
        match value {
            ParameterValue::NumpyScalar { dtype, value } => {
                use NumpyScalarType as N;
                let fits_f32 = |value: f64| value.is_nan() || f64::from(value as f32) == value;
                let valid = match (dtype, value.as_ref()) {
                    (N::Bool, ParameterValue::Bool(_)) => true,
                    (N::Int8, ParameterValue::Integer(v)) => i8::try_from(*v).is_ok(),
                    (N::Int16, ParameterValue::Integer(v)) => i16::try_from(*v).is_ok(),
                    (N::Int32, ParameterValue::Integer(v)) => i32::try_from(*v).is_ok(),
                    (N::Int64, ParameterValue::Integer(_)) => true,
                    (N::UInt8, ParameterValue::Integer(v)) => u8::try_from(*v).is_ok(),
                    (N::UInt16, ParameterValue::Integer(v)) => u16::try_from(*v).is_ok(),
                    (N::UInt32, ParameterValue::Integer(v)) => u32::try_from(*v).is_ok(),
                    (N::UInt64, ParameterValue::Integer(v)) => *v >= 0,
                    (N::Float32, ParameterValue::Float(value)) => fits_f32(*value),
                    (N::Complex64, ParameterValue::Complex(value)) => value.iter().all(|part| fits_f32(part.0)),
                    (N::Complex128, ParameterValue::Complex(_)) => true,
                    _ => false,
                };
                if valid { Ok(()) } else { Err("NumPy scalar dtype and value do not match (#977)".into()) }
            }
            ParameterValue::ScalarArray(value)
                if !matches!(value.as_ref(), ParameterValue::Bool(_) | ParameterValue::Integer(_) | ParameterValue::Float(_) | ParameterValue::Complex(_) | ParameterValue::String(_)) =>
            {
                Err("zero-dimensional array must contain a scalar (#977)".into())
            }
            ParameterValue::EmptyArray { shape, .. }
                if shape.is_empty() || shape.len() > 32 || !shape.contains(&0) || shape.iter().any(|&size| i32::try_from(size).is_err()) =>
            {
                Err(
                    "empty array must have 1 to 32 dimensions, 32-bit dimension sizes, and a zero dimension (#977)"
                        .into(),
                )
            }
            ParameterValue::Array(values) => values.iter().try_for_each(validate),
            ParameterValue::Table(values) => values.values().try_for_each(validate),
            _ => Ok(()),
        }
    }
    validate(value)
}

pub(super) fn validate_parameter_domain(domain: &ParameterType) -> Result<(), String> {
    match domain {
        ParameterType::Quantity {
            dimension,
            canonical_unit,
            ..
        } => validate_quantity_domain(*dimension, canonical_unit).map(|_| ()),
        ParameterType::Array { element, .. } | ParameterType::Optional { value: element, .. } => {
            validate_parameter_domain(element)
        }
        ParameterType::Table { fields } => {
            for field in fields.values() {
                validate_parameter_domain(field)?;
            }
            Ok(())
        }
        ParameterType::Variant
        | ParameterType::Bool
        | ParameterType::Integer
        | ParameterType::Float
        | ParameterType::String
        | ParameterType::Path { .. }
        | ParameterType::Choice { .. } => Ok(()),
    }
}

pub(super) fn validate_quantity_value(
    value: &str,
    dimension: UnitDimension,
    canonical_unit: &str,
    special_values: &[String],
) -> Result<(), String> {
    let canonical = validate_quantity_domain(dimension, canonical_unit)?;
    let value = value.trim();
    if special_values.iter().any(|special| special == value) {
        return Ok(());
    }
    let quantity_text = if value.parse::<f64>().is_ok() {
        format!("{value}{canonical_unit}")
    } else {
        value.to_string()
    };
    let quantity = quantity_text
        .parse::<Quantity>()
        .map_err(|error| format!("invalid quantity {value:?}: {error}"))?;
    if !quantity.value().is_finite() {
        return Err(format!("quantity {value:?} is not finite"));
    }
    let canonical_value = quantity.get_value_in(&canonical).map_err(|error| {
        format!(
            "quantity {value:?} is not conformant with canonical unit {canonical_unit:?}: {error}"
        )
    })?;
    if !canonical_value.is_finite() {
        return Err(format!(
            "quantity {value:?} is non-finite in canonical unit {canonical_unit:?}"
        ));
    }
    Ok(())
}

pub(super) fn validate_quantity_domain(
    dimension: UnitDimension,
    canonical_unit: &str,
) -> Result<Unit, String> {
    let canonical = parse_unit(canonical_unit)
        .map_err(|error| format!("invalid canonical unit {canonical_unit:?}: {error}"))?;
    let expected = match dimension {
        UnitDimension::Dimensionless => Some(Unit::dimensionless()),
        UnitDimension::Angle => Some(parse_builtin_unit("rad")?),
        UnitDimension::Time => Some(parse_builtin_unit("s")?),
        UnitDimension::Frequency => Some(parse_builtin_unit("Hz")?),
        UnitDimension::Velocity => Some(parse_builtin_unit("m/s")?),
        UnitDimension::Length => Some(parse_builtin_unit("m")?),
        UnitDimension::FluxDensity => Some(parse_builtin_unit("Jy")?),
        UnitDimension::FluxDensityPerBeam => Some(parse_builtin_unit("Jy/beam")?),
        UnitDimension::Temperature => Some(parse_builtin_unit("K")?),
        UnitDimension::DataSize => {
            return Err(
                "data-size quantities are not represented by the casa-types unit registry"
                    .to_string(),
            );
        }
        UnitDimension::Custom => None,
    };
    if expected.is_some_and(|expected| !canonical.conformant(&expected)) {
        return Err(format!(
            "canonical unit {canonical_unit:?} does not match declared dimension {dimension:?}"
        ));
    }
    Ok(canonical)
}

pub(super) fn parse_unit(name: &str) -> Result<Unit, casa_core::quanta::UnitError> {
    if name.is_empty() {
        Ok(Unit::dimensionless())
    } else {
        Unit::new(name)
    }
}

pub(super) fn parse_builtin_unit(name: &str) -> Result<Unit, String> {
    parse_unit(name).map_err(|error| format!("internal reference unit {name:?}: {error}"))
}

pub(super) fn validate_constraints(
    value: &ParameterValue,
    constraints: &[Constraint],
    surface: &str,
    binding: &SurfaceParameterBinding,
    errors: &mut Vec<ContractValidationError>,
) {
    for constraint in constraints {
        let valid = match constraint {
            Constraint::Finite => numeric_values(value).into_iter().all(f64::is_finite),
            Constraint::NonEmpty => value_len(value).is_none_or(|length| length > 0),
            Constraint::Positive => numeric_values(value).into_iter().all(|number| number > 0.0),
            Constraint::NumberRange {
                min,
                max,
                min_inclusive,
                max_inclusive,
            } => numeric_values(value).into_iter().all(|number| {
                min.is_none_or(|min| {
                    if *min_inclusive {
                        number >= min
                    } else {
                        number > min
                    }
                }) && max.is_none_or(|max| {
                    if *max_inclusive {
                        number <= max
                    } else {
                        number < max
                    }
                })
            }),
            Constraint::Length { min, max } => value_len(value)
                .is_none_or(|length| length >= *min && max.is_none_or(|max| length <= max)),
            Constraint::AllowedValues { values } => match value {
                ParameterValue::String(value) => values.contains(value),
                _ => true,
            },
        };
        if !valid {
            errors.push(
                ContractValidationError::new(
                    "default_constraint",
                    format!("default violates base constraint {constraint:?}"),
                )
                .at(surface, Some(&binding.name)),
            );
        }
    }
}

pub(super) fn validate_refinement_value(
    value: &ParameterValue,
    normalization: &NormalizationRule,
    refinements: &[NarrowingConstraint],
    surface: &str,
    binding: &SurfaceParameterBinding,
    errors: &mut Vec<ContractValidationError>,
) {
    for refinement in refinements {
        let valid = match refinement {
            NarrowingConstraint::NumberRange {
                min,
                max,
                min_inclusive,
                max_inclusive,
            } => numeric_values(value).into_iter().all(|number| {
                min.is_none_or(|min| {
                    if *min_inclusive {
                        number >= min
                    } else {
                        number > min
                    }
                }) && max.is_none_or(|max| {
                    if *max_inclusive {
                        number <= max
                    } else {
                        number < max
                    }
                })
            }),
            NarrowingConstraint::Length { min, max } => value_len(value)
                .is_none_or(|length| length >= *min && max.is_none_or(|max| length <= max)),
            NarrowingConstraint::AllowedValues { values } => match value {
                ParameterValue::String(value) => values.contains(value),
                _ => true,
            },
            NarrowingConstraint::SelectorCapabilities { capabilities } => match normalization {
                NormalizationRule::CasaSelector { grammar, .. } => {
                    validate_selector_capabilities(*grammar, value, capabilities).is_ok()
                }
                _ => false,
            },
        };
        if !valid {
            errors.push(
                ContractValidationError::new(
                    "default_refinement",
                    format!("default violates refinement {refinement:?}"),
                )
                .at(surface, Some(&binding.name)),
            );
        }
    }
}

pub(super) fn validate_refinements(
    concept: &ParameterConcept,
    binding: &SurfaceParameterBinding,
    surface: &str,
    errors: &mut Vec<ContractValidationError>,
) {
    for refinement in &binding.refinements {
        let narrowing = match refinement {
            NarrowingConstraint::NumberRange { min, max, .. } => {
                let base =
                    concept
                        .base_constraints
                        .iter()
                        .find_map(|constraint| match constraint {
                            Constraint::NumberRange { min, max, .. } => Some((*min, *max)),
                            _ => None,
                        });
                domain_is_numeric(&concept.value_domain)
                    && base.is_none_or(|(base_min, base_max)| {
                        base_min.is_none_or(|base| min.is_some_and(|value| value >= base))
                            && base_max.is_none_or(|base| max.is_some_and(|value| value <= base))
                    })
            }
            NarrowingConstraint::Length { min, max } => match &concept.value_domain {
                ParameterType::Array {
                    min_items,
                    max_items,
                    ..
                } => {
                    min >= min_items
                        && max_items.is_none_or(|base| max.is_some_and(|value| value <= base))
                }
                _ => false,
            },
            NarrowingConstraint::AllowedValues { values } => match &concept.value_domain {
                ParameterType::Choice { values: base } => {
                    values.iter().all(|value| base.contains(value))
                }
                _ => concept
                    .base_constraints
                    .iter()
                    .any(|constraint| match constraint {
                        Constraint::AllowedValues { values: base } => {
                            values.iter().all(|value| base.contains(value))
                        }
                        _ => false,
                    }),
            },
            NarrowingConstraint::SelectorCapabilities { capabilities } => {
                match &concept.normalization {
                    NormalizationRule::CasaSelector {
                        capabilities: base, ..
                    } => {
                        base.iter().all(|capability| {
                            SELECTOR_CAPABILITY_NAMES.contains(&capability.as_str())
                        }) && capabilities.iter().all(|capability| {
                            SELECTOR_CAPABILITY_NAMES.contains(&capability.as_str())
                                && base.contains(capability)
                        })
                    }
                    _ => false,
                }
            }
        };
        if !narrowing {
            errors.push(
                ContractValidationError::new(
                    "non_narrowing_refinement",
                    format!("refinement {refinement:?} is not a provable subset"),
                )
                .at(surface, Some(&binding.name)),
            );
        }
    }
}
