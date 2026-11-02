// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::{Component, Path};

use casa_provider_contracts::{
    Constraint, NarrowingConstraint, NormalizationRule, PROJECT_TOKEN, ParameterConcept,
    ParameterType, ParameterValue, SurfaceParameterBinding, validate_parameter_value,
    validate_selector_capabilities,
};
use casa_types::quanta::{Quantity, Unit};
use thiserror::Error;

/// Normalize a value according to the concept's invariant domain and rule.
pub fn normalize_value(
    value: &ParameterValue,
    concept: &ParameterConcept,
) -> Result<ParameterValue, NormalizationError> {
    reject_forbidden_strings(value, names_project(&concept.normalization))?;
    let value = if reads_numbers_as_text(&concept.normalization) {
        number_as_text(value)
    } else {
        value.clone()
    };
    let typed = normalize_domain(&value, &concept.value_domain)?;
    let normalized = apply_rule(typed, &concept.normalization)?;
    validate_parameter_value(&normalized, &concept.value_domain)
        .map_err(NormalizationError::Type)?;
    validate_constraints(&normalized, &concept.base_constraints)?;
    Ok(normalized)
}

/// Whether `rule` reads a number given for text as its text.
fn reads_numbers_as_text(rule: &NormalizationRule) -> bool {
    match rule {
        NormalizationRule::NumberAsText => true,
        NormalizationRule::Sequence { rules } => rules.iter().any(reads_numbers_as_text),
        _ => false,
    }
}

/// `value` with each number, alone or in a list, as its text: `3` as `"3"`.
fn number_as_text(value: &ParameterValue) -> ParameterValue {
    match value {
        ParameterValue::Integer(number) => ParameterValue::String(number.to_string()),
        ParameterValue::Float(number) => ParameterValue::String(number.to_string()),
        ParameterValue::Array(values) => {
            ParameterValue::Array(values.iter().map(number_as_text).collect())
        }
        other => other.clone(),
    }
}

/// Whether `rule` lets a value name the project as [`PROJECT_TOKEN`].
fn names_project(rule: &NormalizationRule) -> bool {
    match rule {
        NormalizationRule::ProjectPath => true,
        NormalizationRule::Sequence { rules } => rules.iter().any(names_project),
        _ => false,
    }
}

/// Refuse URLs and shell expressions; `project` lets a value hold
/// [`PROJECT_TOKEN`].
fn reject_forbidden_strings(
    value: &ParameterValue,
    project: bool,
) -> Result<(), NormalizationError> {
    match value {
        ParameterValue::String(value) => {
            if value.contains("://") {
                return Err(NormalizationError::Url(value.clone()));
            }
            let checked = if project {
                value.replace(PROJECT_TOKEN, "")
            } else {
                value.clone()
            };
            if checked.contains('$') || checked.contains('`') {
                return Err(NormalizationError::EnvironmentExpression(value.clone()));
            }
        }
        ParameterValue::Array(values) => {
            for value in values {
                reject_forbidden_strings(value, project)?;
            }
        }
        ParameterValue::Table(values) => {
            for value in values.values() {
                reject_forbidden_strings(value, project)?;
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
            // A bare number is in the canonical unit, as a bare numeric
            // text is: CASA's `threshold=0.001` is 1 mJy, `cell=2.0` 2
            // arcsec.
            ParameterValue::Integer(number) => {
                normalize_quantity(&number.to_string(), canonical_unit).map(ParameterValue::String)
            }
            ParameterValue::Float(number) if number.is_finite() => {
                normalize_quantity(&number.to_string(), canonical_unit).map(ParameterValue::String)
            }
            _ => type_error(value, domain),
        },
        ParameterType::Array {
            element,
            min_items,
            max_items,
            allow_scalar,
        } => {
            // A fixed pair whose scalar is one value for every item (`imsize`,
            // `cell`) takes a one-element list the same way, as CASA does.
            let pair = match max_items {
                Some(max) if *allow_scalar && *max == *min_items && *min_items > 1 => *min_items,
                _ => 0,
            };
            let source = match value {
                ParameterValue::Array(values) if pair > 0 && values.len() == 1 => {
                    vec![values[0].clone(); pair]
                }
                // CASA's list of lists (`spwmap`) takes a flat list as its
                // one item: `[0, 0, 1]` is the first table's map (#931).
                ParameterValue::Array(values)
                    if matches!(**element, ParameterType::Array { .. })
                        && !values.is_empty()
                        && !values
                            .iter()
                            .any(|value| matches!(value, ParameterValue::Array(_))) =>
                {
                    vec![value.clone()]
                }
                ParameterValue::Array(values) => values.clone(),
                // A path list given as one string is comma-separated, as on
                // the command line; it is split before any path is
                // normalized, so `a.exp,sub/../b.exp` names two archives.
                ParameterValue::String(text)
                    if *allow_scalar
                        && matches!(**element, ParameterType::Path { .. })
                        && text.contains(',') =>
                {
                    text.split(',')
                        .map(str::trim)
                        .filter(|item| !item.is_empty())
                        .map(|item| ParameterValue::String(item.to_string()))
                        .collect()
                }
                _ if *allow_scalar => vec![value.clone(); pair.max(1)],
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
            // CASA spells the empty value of a parameter of several types
            // both '' and [] (flagdata's timedev): where '' is an absence
            // state, [] is that state (#931).
            ParameterValue::Array(items)
                if items.is_empty() && states.iter().any(String::is_empty) =>
            {
                Ok(ParameterValue::String(String::new()))
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
        NormalizationRule::ProjectPath => map_strings(value, normalize_project_path),
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
        NormalizationRule::NumberAsText => map_strings(value, |value| Ok(value.trim().to_string())),
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
    reject_forbidden_strings(&value, false)?;
    map_strings(value, |value| normalize_quantity(value, canonical_unit))
}

/// A profile's old comma-separated numbers (`"0.1,10"`) as a list of
/// numbers, and its old explicit absence (`"none"` or empty) as CASA's
/// `-1`: the `number_list` migration transform (#931). Any other value is
/// kept.
pub(crate) fn number_list(
    value: ParameterValue,
    parameter: &str,
) -> Result<ParameterValue, crate::profile::ProfileError> {
    let ParameterValue::String(text) = value else {
        return Ok(value);
    };
    let text = text.trim();
    if text.is_empty() || text.eq_ignore_ascii_case("none") {
        return Ok(ParameterValue::Integer(-1));
    }
    text.split(',')
        .map(|part| {
            part.trim()
                .parse::<f64>()
                .map(ParameterValue::Float)
                .map_err(|error| {
                    crate::profile::ProfileError::Contract(format!(
                        "migration expected numbers for {parameter:?}, got {text:?}: {error}"
                    ))
                })
        })
        .collect::<Result<Vec<_>, _>>()
        .map(ParameterValue::Array)
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

/// [`normalize_path`] of a path that may hold [`PROJECT_TOKEN`], kept as
/// it is.
fn normalize_project_path(value: &str) -> Result<String, NormalizationError> {
    // A stand-in no path part can hold, and with no `$` for the check.
    const STAND_IN: &str = "\0project\0";
    if value.contains(STAND_IN) {
        return Err(NormalizationError::EnvironmentExpression(value.to_string()));
    }
    normalize_path(&value.replace(PROJECT_TOKEN, STAND_IN))
        .map(|path| path.replace(STAND_IN, PROJECT_TOKEN))
        .map_err(|error| match error {
            NormalizationError::EnvironmentExpression(_) => {
                NormalizationError::EnvironmentExpression(value.to_string())
            }
            other => other,
        })
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

impl NormalizationError {
    /// What a refusal of a value for `binding` ends with: for a value of a
    /// type, or a list of a length, the binding does not take, the CASA
    /// forms it does not take yet and their issue (flagdata's per-field
    /// `timedev`, #978; tclean's list of MeasurementSets, #980); otherwise
    /// nothing.
    pub fn binding_note(&self, binding: &SurfaceParameterBinding) -> String {
        match (self, &binding.unimplemented_forms) {
            (Self::Type(_) | Self::Cardinality { .. }, Some(forms)) => {
                format!("; {}", forms.refusal())
            }
            _ => String::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{
        CELL_CONCEPT_ID, IMSIZE_CONCEPT_ID, ParameterConceptId, ParameterDocs, ParameterRole,
        PersistenceClass, SelectorGrammar, SemanticRevision, UnitDimension,
        builtin_surface_catalog,
    };

    use super::*;

    #[test]
    fn an_old_number_text_migrates_to_a_list_of_numbers() {
        let text = |text: &str| ParameterValue::String(text.into());
        assert_eq!(
            number_list(text(" 0.1, 10 "), "includepix").unwrap(),
            ParameterValue::Array(vec![
                ParameterValue::Float(0.1),
                ParameterValue::Float(10.0)
            ])
        );
        assert_eq!(
            number_list(text("none"), "includepix").unwrap(),
            ParameterValue::Integer(-1)
        );
        assert_eq!(
            number_list(ParameterValue::Integer(-1), "includepix").unwrap(),
            ParameterValue::Integer(-1)
        );
        assert!(number_list(text("0.1,lots"), "includepix").is_err());
    }

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
    fn a_flat_list_is_the_one_item_of_a_list_of_lists() {
        // CASA's spwmap (#931): [0, 0, 1] maps the first table's windows,
        // [[0, 0], [1, 1]] maps two tables'.
        let concept = concept(
            ParameterType::Array {
                element: Box::new(ParameterType::Array {
                    element: Box::new(ParameterType::Integer),
                    min_items: 0,
                    max_items: None,
                    allow_scalar: false,
                }),
                min_items: 0,
                max_items: None,
                allow_scalar: false,
            },
            NormalizationRule::Identity,
        );
        let ints = |values: &[i64]| {
            ParameterValue::Array(
                values
                    .iter()
                    .copied()
                    .map(ParameterValue::Integer)
                    .collect(),
            )
        };
        let lists = |values: Vec<ParameterValue>| ParameterValue::Array(values);
        assert_eq!(
            normalize_value(&ints(&[0, 0, 1]), &concept).unwrap(),
            lists(vec![ints(&[0, 0, 1])])
        );
        let two = lists(vec![ints(&[0, 0]), ints(&[1, 1])]);
        assert_eq!(normalize_value(&two, &concept).unwrap(), two);
        assert_eq!(
            normalize_value(&lists(vec![]), &concept).unwrap(),
            lists(vec![])
        );
    }

    #[test]
    fn a_scalar_pair_value_normalizes_to_both_axes() {
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
        // CASA's one-element list is one value for both (#970 review).
        assert_eq!(
            normalize_value(
                &ParameterValue::Array(vec![ParameterValue::Integer(120)]),
                &concept
            )
            .unwrap(),
            ParameterValue::Array(vec![ParameterValue::Integer(120); 2])
        );
        assert!(
            normalize_value(
                &ParameterValue::Array(vec![ParameterValue::Integer(1); 3]),
                &concept
            )
            .is_err()
        );
    }

    #[test]
    fn a_path_list_string_is_split_before_its_paths_are_normalized() {
        // Astra's #962 F2: `a.exp,sub/../b.exp` was normalized as one path,
        // which collapsed to b.exp, before importvla split it.
        let concept = concept(
            ParameterType::Array {
                element: Box::new(ParameterType::Path {
                    resource_kind: None,
                }),
                min_items: 1,
                max_items: None,
                allow_scalar: true,
            },
            NormalizationRule::Path,
        );
        let paths = |items: &[&str]| {
            ParameterValue::Array(
                items
                    .iter()
                    .map(|item| ParameterValue::String((*item).into()))
                    .collect(),
            )
        };
        let both = paths(&["a.exp", "b.exp"]);
        for given in [
            ParameterValue::String("a.exp,sub/../b.exp".into()),
            ParameterValue::String(" a.exp , ,sub/../b.exp".into()),
            paths(&["a.exp", "sub/../b.exp"]),
        ] {
            assert_eq!(
                normalize_value(&given, &concept).unwrap(),
                both,
                "{given:?}"
            );
        }
        // A string without a comma is one path.
        assert_eq!(
            normalize_value(&ParameterValue::String("sub/../a.exp".into()), &concept).unwrap(),
            paths(&["a.exp"])
        );
    }

    /// `value` normalized and validated as `concept` with no refinements.
    fn accepted(
        value: ParameterValue,
        concept: &ParameterConcept,
    ) -> Result<ParameterValue, NormalizationError> {
        let normalized = normalize_value(&value, concept)?;
        validate_value(&normalized, concept, &[])?;
        Ok(normalized)
    }

    #[test]
    fn every_builtin_imsize_binding_takes_casas_forms() {
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
                // tclean's `imsize=1024` is its one-element list, and both
                // mean a square image.
                let normalized = normalize_value(&ParameterValue::Integer(1024), concept).unwrap();
                assert_eq!(
                    normalized,
                    ParameterValue::Array(vec![ParameterValue::Integer(1024)])
                );
                validate_value(&normalized, concept, &binding.refinements).unwrap();
                let pair = ParameterValue::Array(vec![
                    ParameterValue::Integer(192),
                    ParameterValue::Integer(128),
                ]);
                assert_eq!(normalize_value(&pair, concept).unwrap(), pair);
                surfaces.push(surface.id().to_string());
            }
        }
        surfaces.sort();
        assert_eq!(surfaces, ["tclean"]);
    }

    #[test]
    fn builtin_imsize_rejects_a_nonpositive_element_and_auto() {
        let catalog = builtin_surface_catalog().unwrap();
        let concept = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == IMSIZE_CONCEPT_ID)
            .unwrap();
        assert!(accepted(ParameterValue::Integer(0), concept).is_err());
        assert!(
            accepted(
                ParameterValue::Array(vec![
                    ParameterValue::Integer(512),
                    ParameterValue::Integer(0),
                ]),
                concept,
            )
            .is_err()
        );
        // casa-rs's "auto" is not CASA's: tclean takes no automatic size.
        assert!(accepted(ParameterValue::String("auto".into()), concept).is_err());
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
        // tclean's `cell='1arcsec'` is its one-element list.
        let scalar = ParameterValue::String("1arcsec".into());
        let degrees = ParameterValue::Array(vec![ParameterValue::String(
            "0.0002777777777777778deg".into(),
        )]);
        let one = ParameterValue::Array(vec![ParameterValue::String("1arcsec".into())]);
        assert_eq!(normalize_value(&scalar, concept).unwrap(), one);
        assert_eq!(normalize_value(&degrees, concept).unwrap(), one);
        assert!(semantic_eq(&scalar, &degrees, concept).unwrap());
        let mixed_unit_pair = ParameterValue::Array(vec![
            ParameterValue::String("0.0002777777777777778deg".into()),
            ParameterValue::String("2arcsec".into()),
        ]);
        assert_eq!(
            normalize_value(&mixed_unit_pair, concept).unwrap(),
            ParameterValue::Array(vec![
                ParameterValue::String("1arcsec".into()),
                ParameterValue::String("2arcsec".into()),
            ])
        );
        // A CASA float cell is in arcseconds.
        assert_eq!(
            normalize_value(&ParameterValue::Float(0.5), concept).unwrap(),
            ParameterValue::Array(vec![ParameterValue::String("0.5arcsec".into())])
        );
    }

    #[test]
    fn builtin_cell_requires_positive_quantity_magnitudes_and_refuses_auto() {
        let catalog = builtin_surface_catalog().unwrap();
        let concept = catalog
            .catalog
            .concepts
            .iter()
            .find(|concept| concept.id.as_str() == CELL_CONCEPT_ID)
            .unwrap();
        assert!(accepted(ParameterValue::String("-1arcsec".into()), concept).is_err());
        assert!(
            accepted(
                ParameterValue::Array(vec![
                    ParameterValue::String("1arcsec".into()),
                    ParameterValue::String("-0.1deg".into()),
                ]),
                concept,
            )
            .is_err()
        );
        assert_eq!(
            accepted(ParameterValue::String("0.1deg".into()), concept).unwrap(),
            ParameterValue::Array(vec![ParameterValue::String("360arcsec".into())])
        );
        assert!(accepted(ParameterValue::String("auto".into()), concept).is_err());
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
        // `$project` is a path's own token only where the rule allows it.
        assert!(normalize_value(&"$project.ptg.txt".into(), &concept).is_err());
    }

    #[test]
    fn project_paths_keep_the_project_token_and_nothing_else() {
        let concept = concept(
            ParameterType::Path {
                resource_kind: None,
            },
            NormalizationRule::ProjectPath,
        );
        for (given, normalized) in [
            ("$project.ptg.txt", "$project.ptg.txt"),
            ("./sims/../$project.skymodel", "$project.skymodel"),
            ("data/model.fits", "data/model.fits"),
            // CASA replaces the token as a string.
            ("$projects/a", "$projects/a"),
            ("", ""),
        ] {
            assert_eq!(
                normalize_value(&given.into(), &concept).unwrap(),
                ParameterValue::String(normalized.into()),
                "{given}"
            );
        }
        for forbidden in [
            "$HOME/a",
            "${project}.ptg.txt",
            "`date`.txt",
            "https://example.invalid/$project",
            "a\0project\0b",
        ] {
            assert!(
                normalize_value(&forbidden.into(), &concept).is_err(),
                "{forbidden:?}"
            );
        }
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
