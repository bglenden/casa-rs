// SPDX-License-Identifier: LGPL-3.0-or-later
//! Normalizing the values a caller sets, by name, before resolution.
//!
//! Each value is normalized against its concept and checked against its
//! binding's refinements. A value of the wrong type for its parameter is an
//! [`DiagnosticCode::InvalidType`] diagnostic, which Python raises as
//! `TypeError`; any other refusal is [`DiagnosticCode::InvalidValue`],
//! `ValueError` (#931). Every refusal names its parameter.

use std::collections::BTreeMap;

use casa_provider_contracts::{ParameterValue, SurfaceContractBundle};

use crate::diagnostic::{Diagnostic, DiagnosticCode};
use crate::normalize::{NormalizationError, normalize_value, validate_value};
use crate::profile::ProfileError;

/// `values` normalized against `bundle`'s surface.
pub(crate) fn normalize_overrides(
    bundle: &SurfaceContractBundle,
    values: &BTreeMap<String, ParameterValue>,
) -> Result<BTreeMap<String, ParameterValue>, ProfileError> {
    let mut normalized = BTreeMap::new();
    let mut refusals = Vec::new();
    for (name, value) in values {
        let binding = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| &binding.name == name)
            .ok_or_else(|| ProfileError::Contract(format!("unknown parameter {name:?}")))?;
        let concept = bundle
            .catalog
            .concept(&binding.concept)
            .ok_or_else(|| ProfileError::Contract(format!("missing concept for {name}")))?;
        match normalize_value(value, concept).and_then(|value| {
            validate_value(&value, concept, &binding.refinements)?;
            Ok(value)
        }) {
            Ok(value) => {
                normalized.insert(name.clone(), value);
            }
            Err(error) => {
                let code = match error {
                    NormalizationError::Type(_) => DiagnosticCode::InvalidType,
                    _ => DiagnosticCode::InvalidValue,
                };
                let note = error.binding_note(binding);
                refusals.push(
                    Diagnostic::error(code, format!("invalid value for {name}: {error}{note}"))
                        .for_parameter(name.clone()),
                );
            }
        }
    }
    if refusals.is_empty() {
        Ok(normalized)
    } else {
        Err(ProfileError::Diagnostics(refusals))
    }
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::builtin_surface_bundle;

    use super::*;

    #[test]
    fn a_wrong_type_and_a_bad_value_are_told_apart_and_named() {
        let bundle = builtin_surface_bundle("flagmanager").unwrap();
        let values = BTreeMap::from([
            ("comment".to_string(), ParameterValue::Array(Vec::new())),
            (
                "mode".to_string(),
                ParameterValue::String("frobnicate".into()),
            ),
        ]);
        let Err(ProfileError::Diagnostics(diagnostics)) = normalize_overrides(&bundle, &values)
        else {
            panic!("both values are refused");
        };
        let found = diagnostics
            .iter()
            .map(|diagnostic| (diagnostic.code, diagnostic.parameter.as_deref()))
            .collect::<Vec<_>>();
        assert_eq!(
            found,
            [
                (DiagnosticCode::InvalidType, Some("comment")),
                (DiagnosticCode::InvalidValue, Some("mode")),
            ]
        );
    }

    #[test]
    fn casas_empty_list_is_an_absence_and_its_unimplemented_forms_name_their_issue() {
        // flagdata's timedev (#997 F9): CASA's [] is its '', and its
        // per-field thresholds are a form casa-rs does not take yet.
        let bundle = builtin_surface_bundle("flagdata").unwrap();
        let empty = BTreeMap::from([("timedev".to_string(), ParameterValue::Array(Vec::new()))]);
        assert_eq!(
            normalize_overrides(&bundle, &empty).unwrap()["timedev"],
            ParameterValue::String(String::new())
        );
        let per_field = BTreeMap::from([(
            "timedev".to_string(),
            ParameterValue::Array(vec![ParameterValue::Array(vec![
                ParameterValue::Integer(0),
                ParameterValue::Integer(0),
                ParameterValue::Float(1.5),
            ])]),
        )]);
        let Err(ProfileError::Diagnostics(diagnostics)) = normalize_overrides(&bundle, &per_field)
        else {
            panic!("per-field thresholds are refused");
        };
        assert_eq!(diagnostics[0].code, DiagnosticCode::InvalidType);
        assert!(
            diagnostics[0].message.contains("per-field") && diagnostics[0].message.contains("#978"),
            "{}",
            diagnostics[0].message
        );
        // A binding with no unimplemented forms adds nothing.
        let bundle = builtin_surface_bundle("flagmanager").unwrap();
        let wrong = BTreeMap::from([("comment".to_string(), ParameterValue::Array(Vec::new()))]);
        let Err(ProfileError::Diagnostics(diagnostics)) = normalize_overrides(&bundle, &wrong)
        else {
            panic!("refused");
        };
        assert!(
            !diagnostics[0].message.contains('#'),
            "{}",
            diagnostics[0].message
        );
    }

    #[test]
    fn an_unknown_name_is_a_contract_error() {
        let bundle = builtin_surface_bundle("flagmanager").unwrap();
        let values = BTreeMap::from([("widht".to_string(), ParameterValue::Integer(2))]);
        assert!(matches!(
            normalize_overrides(&bundle, &values),
            Err(ProfileError::Contract(_))
        ));
    }
}
