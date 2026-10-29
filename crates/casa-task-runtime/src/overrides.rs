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
                refusals.push(
                    Diagnostic::error(code, format!("invalid value for {name}: {error}"))
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
    fn an_unknown_name_is_a_contract_error() {
        let bundle = builtin_surface_bundle("flagmanager").unwrap();
        let values = BTreeMap::from([("widht".to_string(), ParameterValue::Integer(2))]);
        assert!(matches!(
            normalize_overrides(&bundle, &values),
            Err(ProfileError::Contract(_))
        ));
    }
}
