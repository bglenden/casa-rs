// SPDX-License-Identifier: LGPL-3.0-or-later
//! Normalizing the values a caller sets, by name, before resolution.
//!
//! Each value is normalized against its concept and checked against its
//! binding's refinements. A value of the wrong type for its parameter is an
//! [`DiagnosticCode::InvalidType`] diagnostic, which Python raises as
//! `TypeError`; any other refusal is [`DiagnosticCode::InvalidValue`],
//! `ValueError` (#931). Every refusal names its parameter.

use std::collections::BTreeMap;

use casa_contracts::{ParameterValue, SurfaceContractBundle};

use crate::runtime::diagnostic::{Diagnostic, DiagnosticCode};
use crate::runtime::normalize::{NormalizationError, normalize_value, validate_value};
use crate::runtime::profile::ProfileError;

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
                let message = if matches!(error, NormalizationError::Type(_)) && note.is_empty() {
                    crate::runtime::casa_type_diagnostics::direct_type_refusal(
                        bundle.surface.id(),
                        name,
                        value,
                    )
                } else {
                    None
                }
                .unwrap_or_else(|| format!("invalid value for {name}: {error}{note}"));
                refusals.push(Diagnostic::error(code, message).for_parameter(name.clone()));
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
    use casa_contracts::builtin_surface_bundle;

    use super::*;

    #[test]
    fn tclean_field_preserves_literal_dollars_and_regex_end_anchors() {
        let bundle = builtin_surface_bundle("tclean").unwrap();
        let field = ParameterValue::Array(
            ["CODE$", r#""CODE$""#, "/Alpha1$/", "/(Alpha1$|Beta)/"]
                .into_iter()
                .map(ParameterValue::from)
                .collect(),
        );
        let values = BTreeMap::from([("field".to_string(), field.clone())]);
        assert_eq!(normalize_overrides(&bundle, &values).unwrap(), values);
    }

    #[test]
    fn tclean_field_still_refuses_expansion_and_other_parameters_keep_the_guard() {
        let tclean = builtin_surface_bundle("tclean").unwrap();
        let flagdata = builtin_surface_bundle("flagdata").unwrap();
        let refuses = |bundle: &SurfaceContractBundle, name: &str, value: ParameterValue| {
            let error = normalize_overrides(bundle, &BTreeMap::from([(name.to_owned(), value)]))
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("environment or executable expansion"),
                "{error}"
            );
        };
        for expression in [
            "$HOME",
            "${HOME}",
            "$_variable",
            "$9",
            "$é",
            "$^options",
            "$=options",
            "$~options",
            "$?",
            "$$",
            "$!",
            "$#",
            "$*",
            "$@",
            "$-",
            "$(hostname)",
            "$[1+2]",
            "$'quoted'",
            r#"$"quoted""#,
            r#"/^x$(hostname)/"#,
            "`hostname`",
        ] {
            refuses(&tclean, "field", ParameterValue::from(expression));
        }
        refuses(
            &tclean,
            "field",
            ParameterValue::Array(vec!["CODE$".into(), "$HOME".into()]),
        );
        refuses(&tclean, "imagename", "literal$".into());
        refuses(&flagdata, "vis", "data$.ms".into());
    }

    #[test]
    fn casa_integer_type_refusals_name_the_parameter_and_casa_type() {
        for (surface, name, value) in [
            ("simobserve", "comp_nchan", ParameterValue::Float(-1.0)),
            (
                "simobserve",
                "sdant",
                ParameterValue::String("badname".into()),
            ),
            ("simobserve", "seed", ParameterValue::String("bad".into())),
            ("tclean", "niter", ParameterValue::String("bad".into())),
        ] {
            let bundle = builtin_surface_bundle(surface).unwrap();
            let values = BTreeMap::from([(name.to_string(), value)]);
            let Err(ProfileError::Diagnostics(diagnostics)) = normalize_overrides(&bundle, &values)
            else {
                panic!("{surface}.{name} must refuse the value");
            };
            assert_eq!(diagnostics[0].code, DiagnosticCode::InvalidType);
            assert_eq!(
                diagnostics[0].message,
                format!("{{'{name}': ['must be of cInt type']}}")
            );
        }
    }

    #[test]
    fn a_bool_casa_accepts_as_an_integer_is_not_misreported_as_a_casa_refusal() {
        let bundle = builtin_surface_bundle("simobserve").unwrap();
        let values = BTreeMap::from([("comp_nchan".to_string(), ParameterValue::Bool(true))]);
        let Err(ProfileError::Diagnostics(diagnostics)) = normalize_overrides(&bundle, &values)
        else {
            panic!("casa-rs currently refuses booleans for integers");
        };
        assert!(
            diagnostics[0]
                .message
                .contains("invalid value for comp_nchan")
        );
    }

    #[test]
    fn a_bool_casa_coerces_to_float_is_not_misreported_as_a_casa_refusal() {
        let bundle = builtin_surface_bundle("tclean").unwrap();
        let values = BTreeMap::from([("robust".to_string(), ParameterValue::Bool(true))]);
        let Err(ProfileError::Diagnostics(diagnostics)) = normalize_overrides(&bundle, &values)
        else {
            panic!("casa-rs currently refuses booleans for floats");
        };
        assert!(diagnostics[0].message.contains("invalid value for robust"));
        assert!(!diagnostics[0].message.contains("must be of cFloat type"));
    }

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
    fn flagdata_deviation_overrides_preserve_matrices_and_absence() {
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
        assert_eq!(normalize_overrides(&bundle, &per_field).unwrap(), per_field);
    }

    #[test]
    fn a_type_refusal_adds_no_issue_when_the_binding_has_no_unsupported_forms() {
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
