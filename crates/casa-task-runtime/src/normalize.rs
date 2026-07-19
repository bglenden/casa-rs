// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::{Component, Path};

use casa_provider_contracts::{
    Constraint, NarrowingConstraint, NormalizationRule, ParameterConcept, ParameterType,
    ParameterValue, validate_parameter_value, validate_selector_capabilities,
};
use casa_types::quanta::{Quantity, Unit};
use thiserror::Error;

/// Normalize a value according to the concept's invariant domain and rule.
pub fn normalize_value(
    value: &ParameterValue,
    concept: &ParameterConcept,
) -> Result<ParameterValue, NormalizationError> {
    reject_forbidden_strings(value)?;
    let typed = normalize_domain(value, &concept.value_domain)?;
    let normalized = apply_rule(typed, &concept.normalization)?;
    validate_parameter_value(&normalized, &concept.value_domain)
        .map_err(NormalizationError::Type)?;
    validate_constraints(&normalized, &concept.base_constraints)?;
    Ok(normalized)
}

fn reject_forbidden_strings(value: &ParameterValue) -> Result<(), NormalizationError> {
    match value {
        ParameterValue::String(value) => {
            if value.contains("://") {
                return Err(NormalizationError::Url(value.clone()));
            }
            if value.contains('$') || value.contains('`') {
                return Err(NormalizationError::EnvironmentExpression(value.clone()));
            }
        }
        ParameterValue::Array(values) => {
            for value in values {
                reject_forbidden_strings(value)?;
            }
        }
        ParameterValue::Table(values) => {
            for value in values.values() {
                reject_forbidden_strings(value)?;
            }
        }
        ParameterValue::Bool(_) | ParameterValue::Integer(_) | ParameterValue::Float(_) => {}
    }
    Ok(())
}

/// Validate an already normalized value against concept and binding constraints.
pub fn validate_value(
    value: &ParameterValue,
    concept: &ParameterConcept,
    refinements: &[NarrowingConstraint],
) -> Result<(), NormalizationError> {
    validate_parameter_value(value, &concept.value_domain).map_err(NormalizationError::Type)?;
    validate_constraints(value, &concept.base_constraints)?;
    validate_refinements(value, concept, refinements)?;
    Ok(())
}

/// Compare two values after invariant normalization.
pub fn semantic_eq(
    left: &ParameterValue,
    right: &ParameterValue,
    concept: &ParameterConcept,
) -> Result<bool, NormalizationError> {
    let left = normalize_value(left, concept)?;
    let right = normalize_value(right, concept)?;
    Ok(values_semantically_equal(&left, &right))
}

fn normalize_domain(
    value: &ParameterValue,
    domain: &ParameterType,
) -> Result<ParameterValue, NormalizationError> {
    match domain {
        ParameterType::Bool => match value {
            ParameterValue::Bool(value) => Ok(ParameterValue::Bool(*value)),
            _ => type_error(value, domain),
        },
        ParameterType::Integer => match value {
            ParameterValue::Integer(value) => Ok(ParameterValue::Integer(*value)),
            _ => type_error(value, domain),
        },
        ParameterType::Float => match value {
            ParameterValue::Integer(value) => Ok(ParameterValue::Float(*value as f64)),
            ParameterValue::Float(value) if value.is_finite() => Ok(ParameterValue::Float(*value)),
            ParameterValue::Float(_) => Err(NormalizationError::NonFinite),
            _ => type_error(value, domain),
        },
        ParameterType::String | ParameterType::Path { .. } => match value {
            ParameterValue::String(value) => Ok(ParameterValue::String(value.clone())),
            _ => type_error(value, domain),
        },
        ParameterType::Choice { values } => match value {
            ParameterValue::String(value) if values.contains(value) => {
                Ok(ParameterValue::String(value.clone()))
            }
            ParameterValue::String(value) => Err(NormalizationError::Choice {
                value: value.clone(),
                choices: values.clone(),
            }),
            _ => type_error(value, domain),
        },
        ParameterType::Quantity {
            canonical_unit,
            special_values,
            ..
        } => match value {
            ParameterValue::String(value) if special_values.contains(value) => {
                Ok(ParameterValue::String(value.clone()))
            }
            ParameterValue::String(value) => {
                normalize_quantity(value, canonical_unit).map(ParameterValue::String)
            }
            _ => type_error(value, domain),
        },
        ParameterType::Array {
            element,
            min_items,
            max_items,
            allow_scalar,
        } => {
            let source = match value {
                ParameterValue::Array(values) => values.clone(),
                _ if *allow_scalar => {
                    let repeat = match max_items {
                        Some(max) if *max == *min_items && *min_items > 1 => *min_items,
                        _ => 1,
                    };
                    vec![value.clone(); repeat]
                }
                _ => return type_error(value, domain),
            };
            if source.len() < *min_items || max_items.is_some_and(|max| source.len() > max) {
                return Err(NormalizationError::Cardinality {
                    actual: source.len(),
                    min: *min_items,
                    max: *max_items,
                });
            }
            source
                .iter()
                .map(|value| normalize_domain(value, element))
                .collect::<Result<Vec<_>, _>>()
                .map(ParameterValue::Array)
        }
        ParameterType::Table { fields } => match value {
            ParameterValue::Table(values) => {
                if let Some(unknown) = values.keys().find(|name| !fields.contains_key(*name)) {
                    return Err(NormalizationError::UnknownTableField(unknown.clone()));
                }
                let mut normalized = BTreeMap::new();
                for (name, domain) in fields {
                    let value = values
                        .get(name)
                        .ok_or_else(|| NormalizationError::MissingTableField(name.clone()))?;
                    normalized.insert(name.clone(), normalize_domain(value, domain)?);
                }
                Ok(ParameterValue::Table(normalized))
            }
            _ => type_error(value, domain),
        },
        ParameterType::Optional {
            value: inner,
            states,
        } => match value {
            ParameterValue::String(value) if states.contains(value) => {
                Ok(ParameterValue::String(value.clone()))
            }
            value => normalize_domain(value, inner),
        },
    }
}

fn type_error<T>(value: &ParameterValue, domain: &ParameterType) -> Result<T, NormalizationError> {
    Err(NormalizationError::Type(format!(
        "value {value:?} does not match {domain:?}"
    )))
}

fn apply_rule(
    value: ParameterValue,
    rule: &NormalizationRule,
) -> Result<ParameterValue, NormalizationError> {
    match rule {
        NormalizationRule::Identity => Ok(value),
        NormalizationRule::Trim => map_strings(value, |value| Ok(value.trim().to_string())),
        NormalizationRule::Lowercase => {
            map_strings(value, |value| Ok(value.trim().to_ascii_lowercase()))
        }
        NormalizationRule::Path => map_strings(value, normalize_path),
        NormalizationRule::CasaSelector {
            grammar,
            capabilities,
        } => {
            let value = map_strings(value, |value| Ok(normalize_selector(value)))?;
            validate_selector_capabilities(*grammar, &value, capabilities)
                .map_err(NormalizationError::Constraint)?;
            Ok(value)
        }
        NormalizationRule::Quantity { canonical_unit } => {
            map_strings(value, |value| normalize_quantity(value, canonical_unit))
        }
        NormalizationRule::Sequence { rules } => {
            let mut value = value;
            for rule in rules {
                value = apply_rule(value, rule)?;
            }
            Ok(value)
        }
    }
}

fn map_strings(
    value: ParameterValue,
    function: impl Fn(&str) -> Result<String, NormalizationError> + Copy,
) -> Result<ParameterValue, NormalizationError> {
    match value {
        ParameterValue::String(value) => function(&value).map(ParameterValue::String),
        ParameterValue::Array(values) => values
            .into_iter()
            .map(|value| map_strings(value, function))
            .collect::<Result<Vec<_>, _>>()
            .map(ParameterValue::Array),
        other => Ok(other),
    }
}

pub(crate) fn normalize_quantity_unit(
    value: ParameterValue,
    canonical_unit: &str,
) -> Result<ParameterValue, NormalizationError> {
    reject_forbidden_strings(&value)?;
    map_strings(value, |value| normalize_quantity(value, canonical_unit))
}

fn normalize_quantity(value: &str, canonical_unit: &str) -> Result<String, NormalizationError> {
    if matches!(value.trim(), "auto" | "none") {
        return Ok(value.trim().to_string());
    }
    let trimmed = value.trim();
    let quantity_text = if trimmed.parse::<f64>().is_ok() {
        format!("{trimmed}{canonical_unit}")
    } else {
        trimmed.to_string()
    };
    let quantity = quantity_text
        .parse::<Quantity>()
        .map_err(|error| NormalizationError::Quantity(error.to_string()))?;
    if !quantity.value().is_finite() {
        return Err(NormalizationError::NonFinite);
    }
    let unit = Unit::new(canonical_unit)
        .map_err(|error| NormalizationError::Quantity(error.to_string()))?;
    let value = quantity
        .get_value_in(&unit)
        .map_err(|error| NormalizationError::Quantity(error.to_string()))?;
    Ok(format!("{}{}", compact_float(value), canonical_unit))
}

fn compact_float(value: f64) -> String {
    if value == 0.0 {
        return "0".to_string();
    }
    let magnitude = value.abs().log10().floor();
    let rendered = if !(-12.0..=12.0).contains(&magnitude) {
        value.to_string()
    } else {
        let decimals = (12_i32 - magnitude as i32 - 1).clamp(0, 15) as usize;
        format!("{value:.decimals$}")
            .trim_end_matches('0')
            .trim_end_matches('.')
            .to_string()
    };
    if rendered == "-0" {
        "0".to_string()
    } else {
        rendered
    }
}

fn normalize_path(value: &str) -> Result<String, NormalizationError> {
    let value = value.trim();
    if value.contains("://") {
        return Err(NormalizationError::Url(value.to_string()));
    }
    if value.contains('$') || value.contains('`') {
        return Err(NormalizationError::EnvironmentExpression(value.to_string()));
    }
    let path = Path::new(value);
    let mut parts = Vec::<String>::new();
    let mut absolute = false;
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => parts.push(prefix.as_os_str().to_string_lossy().into()),
            Component::RootDir => absolute = true,
            Component::CurDir => {}
            Component::ParentDir => {
                if parts.last().is_some_and(|part| part != "..") {
                    parts.pop();
                } else if !absolute {
                    parts.push("..".to_string());
                }
            }
            Component::Normal(part) => parts.push(part.to_string_lossy().into()),
        }
    }
    let joined = parts.join("/");
    if absolute {
        Ok(format!("/{joined}"))
    } else if joined.is_empty() && !value.is_empty() {
        Ok(".".to_string())
    } else {
        Ok(joined)
    }
}

fn normalize_selector(value: &str) -> String {
    let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut normalized = String::with_capacity(collapsed.len());
    let mut quoted = None;
    let mut characters = collapsed.chars().peekable();
    while let Some(character) = characters.next() {
        if matches!(character, '\'' | '"') {
            if quoted == Some(character) {
                quoted = None;
            } else if quoted.is_none() {
                quoted = Some(character);
            }
            normalized.push(character);
            continue;
        }
        if character == ' ' && quoted.is_none() {
            let previous_is_separator = normalized
                .chars()
                .next_back()
                .is_some_and(is_selector_separator);
            let next_is_separator = characters
                .peek()
                .copied()
                .is_some_and(is_selector_separator);
            if previous_is_separator || next_is_separator {
                continue;
            }
        }
        normalized.push(character);
    }
    if normalized.is_empty() {
        "none".to_string()
    } else {
        normalized
    }
}

fn is_selector_separator(character: char) -> bool {
    matches!(
        character,
        ',' | ';' | '~' | ':' | '&' | '!' | '=' | '(' | ')' | '[' | ']' | '{' | '}' | '<' | '>'
    )
}

fn validate_constraints(
    value: &ParameterValue,
    constraints: &[Constraint],
) -> Result<(), NormalizationError> {
    for constraint in constraints {
        match constraint {
            Constraint::Finite => {
                if numeric_values(value)
                    .into_iter()
                    .any(|number| !number.is_finite())
                {
                    return Err(NormalizationError::NonFinite);
                }
            }
            Constraint::NonEmpty => {
                if length(value) == Some(0) {
                    return Err(NormalizationError::Constraint(
                        "value must not be empty".into(),
                    ));
                }
            }
            Constraint::Positive => {
                if numeric_values(value)
                    .into_iter()
                    .any(|number| number <= 0.0)
                {
                    return Err(NormalizationError::Constraint(
                        "value must be positive".into(),
                    ));
                }
            }
            Constraint::NumberRange {
                min,
                max,
                min_inclusive,
                max_inclusive,
            } => validate_number_range(value, *min, *max, *min_inclusive, *max_inclusive)?,
            Constraint::Length { min, max } => validate_length(value, *min, *max)?,
            Constraint::AllowedValues { values } => validate_allowed(value, values)?,
        }
    }
    Ok(())
}

fn validate_refinements(
    value: &ParameterValue,
    concept: &ParameterConcept,
    refinements: &[NarrowingConstraint],
) -> Result<(), NormalizationError> {
    for refinement in refinements {
        match refinement {
            NarrowingConstraint::NumberRange {
                min,
                max,
                min_inclusive,
                max_inclusive,
            } => validate_number_range(value, *min, *max, *min_inclusive, *max_inclusive)?,
            NarrowingConstraint::Length { min, max } => validate_length(value, *min, *max)?,
            NarrowingConstraint::AllowedValues { values } => validate_allowed(value, values)?,
            NarrowingConstraint::SelectorCapabilities { capabilities } => {
                let NormalizationRule::CasaSelector { grammar, .. } = &concept.normalization else {
                    return Err(NormalizationError::Constraint(
                        "selector-capability refinement requires CASA selector normalization"
                            .into(),
                    ));
                };
                validate_selector_capabilities(*grammar, value, capabilities)
                    .map_err(NormalizationError::Constraint)?;
            }
            NarrowingConstraint::SquarePair => match value {
                ParameterValue::Array(values)
                    if values.len() == 2 && values_semantically_equal(&values[0], &values[1]) => {}
                _ => {
                    return Err(NormalizationError::Constraint(
                        "two-axis value must be square/equal".into(),
                    ));
                }
            },
        }
    }
    Ok(())
}

fn validate_number_range(
    value: &ParameterValue,
    min: Option<f64>,
    max: Option<f64>,
    min_inclusive: bool,
    max_inclusive: bool,
) -> Result<(), NormalizationError> {
    for number in numeric_values(value) {
        let above = min.is_none_or(|min| {
            if min_inclusive {
                number >= min
            } else {
                number > min
            }
        });
        let below = max.is_none_or(|max| {
            if max_inclusive {
                number <= max
            } else {
                number < max
            }
        });
        if !above || !below {
            return Err(NormalizationError::Constraint(format!(
                "value {number} is outside the accepted range"
            )));
        }
    }
    Ok(())
}

fn validate_length(
    value: &ParameterValue,
    min: usize,
    max: Option<usize>,
) -> Result<(), NormalizationError> {
    let Some(length) = length(value) else {
        return Ok(());
    };
    if length >= min && max.is_none_or(|max| length <= max) {
        Ok(())
    } else {
        Err(NormalizationError::Constraint(format!(
            "length {length} is outside the accepted range"
        )))
    }
}

fn validate_allowed(value: &ParameterValue, values: &[String]) -> Result<(), NormalizationError> {
    match value {
        ParameterValue::String(value) if !values.contains(value) => {
            Err(NormalizationError::Choice {
                value: value.clone(),
                choices: values.to_vec(),
            })
        }
        _ => Ok(()),
    }
}

fn numeric_values(value: &ParameterValue) -> Vec<f64> {
    match value {
        ParameterValue::Integer(value) => vec![*value as f64],
        ParameterValue::Float(value) => vec![*value],
        ParameterValue::String(value) => value
            .parse::<Quantity>()
            .ok()
            .map(|quantity| vec![quantity.value()])
            .unwrap_or_default(),
        ParameterValue::Array(values) => values.iter().flat_map(numeric_values).collect(),
        ParameterValue::Table(values) => values.values().flat_map(numeric_values).collect(),
        ParameterValue::Bool(_) => Vec::new(),
    }
}

fn length(value: &ParameterValue) -> Option<usize> {
    match value {
        ParameterValue::String(value) => Some(value.len()),
        ParameterValue::Array(value) => Some(value.len()),
        ParameterValue::Table(value) => Some(value.len()),
        _ => None,
    }
}

fn values_semantically_equal(left: &ParameterValue, right: &ParameterValue) -> bool {
    match (left, right) {
        (ParameterValue::Float(left), ParameterValue::Float(right)) => {
            let scale = left.abs().max(right.abs()).max(1.0);
            (left - right).abs() <= scale * 1.0e-12
        }
        (ParameterValue::Integer(left), ParameterValue::Integer(right)) => left == right,
        (ParameterValue::Integer(left), ParameterValue::Float(right))
        | (ParameterValue::Float(right), ParameterValue::Integer(left)) => {
            let left = *left as f64;
            let scale = left.abs().max(right.abs()).max(1.0);
            (left - right).abs() <= scale * 1.0e-12
        }
        (ParameterValue::Bool(left), ParameterValue::Bool(right)) => left == right,
        (ParameterValue::String(left), ParameterValue::String(right)) => left == right,
        (ParameterValue::Array(left), ParameterValue::Array(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(left, right)| values_semantically_equal(left, right))
        }
        (ParameterValue::Table(left), ParameterValue::Table(right)) => {
            left.len() == right.len()
                && left.iter().all(|(name, left)| {
                    right
                        .get(name)
                        .is_some_and(|right| values_semantically_equal(left, right))
                })
        }
        _ => false,
    }
}

/// Typed normalization/validation failure.
#[derive(Debug, Error)]
pub enum NormalizationError {
    #[error("invalid parameter type: {0}")]
    Type(String),
    #[error("non-finite numbers are forbidden")]
    NonFinite,
    #[error("invalid quantity: {0}")]
    Quantity(String),
    #[error("invalid choice {value:?}; expected one of {choices:?}")]
    Choice { value: String, choices: Vec<String> },
    #[error("array has {actual} items; expected at least {min} and at most {max:?}")]
    Cardinality {
        actual: usize,
        min: usize,
        max: Option<usize>,
    },
    #[error("unknown table field {0:?}")]
    UnknownTableField(String),
    #[error("missing table field {0:?}")]
    MissingTableField(String),
    #[error("URL values are forbidden in profiles: {0:?}")]
    Url(String),
    #[error("environment or executable expansion is forbidden in profiles: {0:?}")]
    EnvironmentExpression(String),
    #[error("parameter constraint failed: {0}")]
    Constraint(String),
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{
        CELL_CONCEPT_ID, IMSIZE_CONCEPT_ID, ParameterConceptId, ParameterDocs, ParameterRole,
        PersistenceClass, SelectorGrammar, SemanticRevision, UnitDimension,
        builtin_surface_catalog,
    };

    use super::*;

    fn concept(domain: ParameterType, rule: NormalizationRule) -> ParameterConcept {
        ParameterConcept {
            id: ParameterConceptId::new("test.value"),
            semantic_revision: SemanticRevision(1),
            casa_name: "value".into(),
            value_domain: domain,
            normalization: rule,
            base_constraints: Vec::new(),
            unit_dimension: None,
            semantic_role: ParameterRole::Algorithm,
            documentation: ParameterDocs {
                summary: "test".into(),
                details: None,
                examples: Vec::new(),
            },
            persistence_class: PersistenceClass::Profile,
        }
    }

    #[test]
    fn scalar_imsize_normalizes_to_square_pair() {
        let concept = concept(
            ParameterType::Array {
                element: Box::new(ParameterType::Integer),
                min_items: 2,
                max_items: Some(2),
                allow_scalar: true,
            },
            NormalizationRule::Identity,
        );
        assert_eq!(
            normalize_value(&ParameterValue::Integer(512), &concept).unwrap(),
            ParameterValue::Array(vec![ParameterValue::Integer(512); 2])
        );
    }

    #[test]
    fn every_builtin_imsize_binding_normalizes_scalar_to_the_same_pair() {
        let catalog = builtin_surface_catalog().unwrap();
        let mut surfaces = Vec::new();
        for surface in &catalog.surfaces {
            for binding in surface
                .bindings()
                .iter()
                .filter(|binding| binding.name == "imsize")
            {
                assert_eq!(binding.concept.id.as_str(), IMSIZE_CONCEPT_ID);
                let concept = catalog.catalog.concept(&binding.concept).unwrap();
                let normalized = normalize_value(&ParameterValue::Integer(1024), concept).unwrap();
                assert_eq!(
                    normalized,
                    ParameterValue::Array(vec![ParameterValue::Integer(1024); 2])
                );
                validate_value(&normalized, concept, &binding.refinements).unwrap();
                surfaces.push(surface.id().to_string());
            }
        }
        surfaces.sort();
        assert_eq!(surfaces, ["imager", "simalma", "simanalyze"]);
    }

    #[test]
    fn builtin_imsize_rejects_a_nonpositive_element_and_accepts_auto() {
        let catalog = builtin_surface_catalog().unwrap();
        let concept = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == IMSIZE_CONCEPT_ID)
            .unwrap();
        assert!(normalize_value(&ParameterValue::Integer(0), concept).is_err());
        assert!(
            normalize_value(
                &ParameterValue::Array(vec![
                    ParameterValue::Integer(512),
                    ParameterValue::Integer(0),
                ]),
                concept,
            )
            .is_err()
        );
        assert_eq!(
            normalize_value(&ParameterValue::String("auto".into()), concept).unwrap(),
            ParameterValue::String("auto".into())
        );
    }

    #[test]
    fn quantity_equality_converts_units() {
        let mut concept = concept(
            ParameterType::Quantity {
                dimension: UnitDimension::Angle,
                canonical_unit: "arcsec".into(),
                special_values: Vec::new(),
            },
            NormalizationRule::Quantity {
                canonical_unit: "arcsec".into(),
            },
        );
        concept.unit_dimension = Some(UnitDimension::Angle);
        assert!(
            semantic_eq(
                &ParameterValue::String("1arcsec".into()),
                &ParameterValue::String("0.0002777777777777778deg".into()),
                &concept,
            )
            .unwrap()
        );
    }

    #[test]
    fn builtin_cell_equality_is_unit_aware_for_scalar_and_pair_forms() {
        let catalog = builtin_surface_catalog().unwrap();
        let concept = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == CELL_CONCEPT_ID)
            .unwrap();
        let scalar = ParameterValue::String("1arcsec".into());
        let mixed_unit_pair = ParameterValue::Array(vec![
            ParameterValue::String("0.0002777777777777778deg".into()),
            ParameterValue::String("1arcsec".into()),
        ]);
        let expected = ParameterValue::Array(vec![
            ParameterValue::String("1arcsec".into()),
            ParameterValue::String("1arcsec".into()),
        ]);
        assert_eq!(normalize_value(&scalar, concept).unwrap(), expected);
        assert_eq!(
            normalize_value(&mixed_unit_pair, concept).unwrap(),
            expected
        );
        assert!(semantic_eq(&scalar, &mixed_unit_pair, concept).unwrap());
    }

    #[test]
    fn builtin_cell_requires_positive_quantity_magnitudes_but_allows_auto() {
        let catalog = builtin_surface_catalog().unwrap();
        let concept = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == CELL_CONCEPT_ID)
            .unwrap();
        assert!(normalize_value(&ParameterValue::String("-1arcsec".into()), concept).is_err());
        assert!(
            normalize_value(
                &ParameterValue::Array(vec![
                    ParameterValue::String("1arcsec".into()),
                    ParameterValue::String("-0.1deg".into()),
                ]),
                concept,
            )
            .is_err()
        );
        assert_eq!(
            normalize_value(&ParameterValue::String("0.1deg".into()), concept).unwrap(),
            ParameterValue::Array(vec![ParameterValue::String("360arcsec".into()); 2])
        );
        assert_eq!(
            normalize_value(&ParameterValue::String("auto".into()), concept).unwrap(),
            ParameterValue::Array(vec![ParameterValue::String("auto".into()); 2])
        );
    }

    #[test]
    fn paths_are_lexical_and_reject_urls_or_expansion() {
        let concept = concept(
            ParameterType::Path {
                resource_kind: None,
            },
            NormalizationRule::Path,
        );
        assert_eq!(
            normalize_value(&"data/./a/../b.ms".into(), &concept).unwrap(),
            ParameterValue::String("data/b.ms".into())
        );
        assert!(normalize_value(&"https://example.invalid/a".into(), &concept).is_err());
        assert!(normalize_value(&"${HOME}/a".into(), &concept).is_err());
        assert!(normalize_value(&"$HOME/a".into(), &concept).is_err());
    }

    #[test]
    fn every_string_domain_rejects_urls_and_executable_expansion() {
        let concept = concept(ParameterType::String, NormalizationRule::Identity);
        for forbidden in [
            "https://example.invalid/value",
            "${HOME}/value",
            "$HOME/value",
            "$(hostname)",
            "`hostname`",
        ] {
            assert!(
                normalize_value(&ParameterValue::String(forbidden.into()), &concept).is_err(),
                "accepted forbidden string {forbidden:?}"
            );
        }
    }

    #[test]
    fn legacy_empty_shared_selectors_normalize_to_canonical_none() {
        let catalog = casa_provider_contracts::builtin_surface_catalog().unwrap();
        for concept in catalog.catalog.concepts.iter().filter(|concept| {
            concept.id.as_str().starts_with("ms.selection.")
                || concept.id.as_str().starts_with("image.selection.")
        }) {
            assert_eq!(
                normalize_value(&ParameterValue::String("  ".into()), concept).unwrap(),
                ParameterValue::String("none".into()),
                "{}",
                concept.id
            );
            assert!(
                semantic_eq(
                    &ParameterValue::String(String::new()),
                    &ParameterValue::String("none".into()),
                    concept
                )
                .unwrap()
            );
        }
    }

    #[test]
    fn selector_capability_refinements_reject_disabled_grammar_features() {
        let selector_concept = concept(
            ParameterType::String,
            NormalizationRule::CasaSelector {
                grammar: SelectorGrammar::Field,
                capabilities: vec![
                    "ids".into(),
                    "names".into(),
                    "ranges".into(),
                    "wildcards".into(),
                ],
            },
        );
        let refinements = vec![NarrowingConstraint::SelectorCapabilities {
            capabilities: vec!["ids".into(), "names".into()],
        }];

        for accepted in ["0,target", "17", "target"] {
            let normalized =
                normalize_value(&ParameterValue::String(accepted.into()), &selector_concept)
                    .unwrap();
            validate_value(&normalized, &selector_concept, &refinements).unwrap();
        }
        for rejected in ["0~3", "target*"] {
            let normalized =
                normalize_value(&ParameterValue::String(rejected.into()), &selector_concept)
                    .unwrap();
            assert!(
                validate_value(&normalized, &selector_concept, &refinements).is_err(),
                "accepted selector with disabled capability: {rejected}"
            );
        }

        let base_restricted = concept(
            ParameterType::String,
            NormalizationRule::CasaSelector {
                grammar: SelectorGrammar::Field,
                capabilities: vec!["ids".into(), "names".into()],
            },
        );
        assert!(normalize_value(&ParameterValue::String("0~3".into()), &base_restricted).is_err());
    }
}
