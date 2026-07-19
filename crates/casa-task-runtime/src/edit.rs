// SPDX-License-Identifier: LGPL-3.0-or-later
//! Surface-neutral validation for one parameter text edit.

use casa_provider_contracts::{NormalizationRule, ParameterValue, SurfaceContractBundle};
use serde::{Deserialize, Serialize};

use crate::{normalize_value, parse_parameter_text, validate_value};

/// Stable category for a rejected parameter edit.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParameterEditDiagnosticCode {
    UnknownParameter,
    InvalidText,
    InvalidValue,
    InvalidSelectorSyntax,
    UnsupportedSelectorCapability,
    UnknownSelectorValue,
    SelectorValueOutOfRange,
    DatasetUnavailable,
}

/// One parameter-scoped edit diagnostic shared by every surface.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParameterEditDiagnostic {
    pub code: ParameterEditDiagnosticCode,
    pub parameter: String,
    pub message: String,
}

impl ParameterEditDiagnostic {
    pub fn new(
        code: ParameterEditDiagnosticCode,
        parameter: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            code,
            parameter: parameter.into(),
            message: message.into(),
        }
    }
}

/// One presentation-safe value suggested for an edit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParameterEditSuggestion {
    pub label: String,
    pub value: ParameterValue,
}

/// Typed result of validating one parameter text edit.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParameterEditResult {
    pub parameter: String,
    pub normalized_value: Option<ParameterValue>,
    pub diagnostics: Vec<ParameterEditDiagnostic>,
    pub supported_capabilities: Vec<String>,
    pub suggestions: Vec<ParameterEditSuggestion>,
}

impl ParameterEditResult {
    pub fn is_valid(&self) -> bool {
        self.diagnostics.is_empty()
    }

    pub fn reject(&mut self, code: ParameterEditDiagnosticCode, message: impl Into<String>) {
        self.normalized_value = None;
        self.diagnostics.push(ParameterEditDiagnostic::new(
            code,
            self.parameter.clone(),
            message,
        ));
    }
}

/// Parse, normalize, and validate one edit through its declared contract.
///
/// An empty edit represents an absent/reset value. Domain crates may extend
/// this result with syntax or dataset-aware diagnostics after this generic
/// contract pass.
pub fn validate_parameter_edit(
    bundle: &SurfaceContractBundle,
    parameter: &str,
    text: &str,
    suggestions: impl IntoIterator<Item = ParameterEditSuggestion>,
) -> ParameterEditResult {
    let mut result = ParameterEditResult {
        parameter: parameter.to_string(),
        normalized_value: None,
        diagnostics: Vec::new(),
        supported_capabilities: Vec::new(),
        suggestions: Vec::new(),
    };
    let Some(binding) = bundle
        .surface
        .bindings()
        .iter()
        .find(|binding| binding.name == parameter)
    else {
        result.reject(
            ParameterEditDiagnosticCode::UnknownParameter,
            format!("unknown {} parameter {parameter:?}", bundle.surface.id()),
        );
        return result;
    };
    let Some(concept) = bundle.catalog.concept(&binding.concept) else {
        result.reject(
            ParameterEditDiagnosticCode::UnknownParameter,
            format!("missing parameter concept for {parameter:?}"),
        );
        return result;
    };
    collect_selector_capabilities(&concept.normalization, &mut result.supported_capabilities);

    if text.trim().is_empty() {
        return result;
    }
    let parsed = match parse_parameter_text(text, &concept.value_domain) {
        Ok(value) => value,
        Err(error) => {
            result.reject(ParameterEditDiagnosticCode::InvalidText, error.to_string());
            return result;
        }
    };
    let normalized = match normalize_value(&parsed, concept) {
        Ok(value) => value,
        Err(error) => {
            let code = if error.to_string().contains("unsupported") {
                ParameterEditDiagnosticCode::UnsupportedSelectorCapability
            } else {
                ParameterEditDiagnosticCode::InvalidValue
            };
            result.reject(code, error.to_string());
            return result;
        }
    };
    if let Err(error) = validate_value(&normalized, concept, &binding.refinements) {
        result.reject(ParameterEditDiagnosticCode::InvalidValue, error.to_string());
        return result;
    }
    result.normalized_value = Some(normalized);

    result.suggestions = suggestions
        .into_iter()
        .filter_map(|suggestion| {
            let normalized = normalize_value(&suggestion.value, concept).ok()?;
            validate_value(&normalized, concept, &binding.refinements).ok()?;
            Some(ParameterEditSuggestion {
                label: suggestion.label,
                value: normalized,
            })
        })
        .collect();
    result
}

fn collect_selector_capabilities(rule: &NormalizationRule, output: &mut Vec<String>) {
    match rule {
        NormalizationRule::CasaSelector { capabilities, .. } => {
            for capability in capabilities {
                if !output.contains(capability) {
                    output.push(capability.clone());
                }
            }
        }
        NormalizationRule::Sequence { rules } => {
            for rule in rules {
                collect_selector_capabilities(rule, output);
            }
        }
        NormalizationRule::Identity
        | NormalizationRule::Trim
        | NormalizationRule::Lowercase
        | NormalizationRule::Path
        | NormalizationRule::Quantity { .. } => {}
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{ParameterValue, builtin_surface_bundle};

    use super::*;

    #[test]
    fn edit_validation_normalizes_and_enforces_surface_capabilities() {
        let bundle = builtin_surface_bundle("msexplore").unwrap();
        let valid = validate_parameter_edit(&bundle, "field", " 0, 2 ", []);
        assert!(valid.is_valid(), "{:?}", valid.diagnostics);
        assert_eq!(
            valid.normalized_value,
            Some(ParameterValue::String("0,2".to_string()))
        );

        let unknown = validate_parameter_edit(&bundle, "not_a_parameter", "0", []);
        assert_eq!(
            unknown.diagnostics[0].code,
            ParameterEditDiagnosticCode::UnknownParameter
        );
    }
}
