// SPDX-License-Identifier: LGPL-3.0-or-later
//! Refusing the CASA parameters casa-rs does not implement yet (#931).
//!
//! A surface that mimics a CASA task takes every CASA parameter, so CASA's
//! signature is exact. A parameter casa-rs does not implement yet carries
//! [`UnimplementedParameter`](casa_provider_contracts::UnimplementedParameter)
//! in the contract: resolution accepts it at its
//! default, as CASA reads the default, and refuses any other value before
//! the task starts, on the command line and from Python alike. The marker
//! states the law: where CASA ignores the parameter every value is
//! accepted, and elsewhere a value is the default when it is semantically
//! equal to the literal default or to one of CASA's equivalent spellings.

use std::collections::BTreeMap;

use casa_provider_contracts::{
    DefaultSpec, ParameterValue, SurfaceContractBundle, SurfaceParameterBinding,
};

use crate::diagnostic::{Diagnostic, DiagnosticCode};
use crate::normalize::semantic_eq;
use crate::profile::evaluate_predicate;

/// The refusal of `binding`'s resolved value, when the binding is
/// unimplemented, CASA reads it here, and the value is not the default as
/// CASA reads it.
pub(crate) fn refusal(
    bundle: &SurfaceContractBundle,
    binding: &SurfaceParameterBinding,
    values: &BTreeMap<String, ParameterValue>,
) -> Option<Diagnostic> {
    let unimplemented = binding.unimplemented.as_ref()?;
    // The contract gives every unimplemented binding a literal default.
    let DefaultSpec::Literal { value: default } = &binding.default else {
        return None;
    };
    // CASA ignores the parameter for these values, and so does casa-rs.
    // An undecided predicate is read as applying.
    if evaluate_predicate(&unimplemented.applies_when, values, bundle) == Some(false) {
        return None;
    }
    let value = values.get(&binding.name)?;
    let concept = bundle.catalog.concept(&binding.concept)?;
    let is_default =
        |default: &ParameterValue| matches!(semantic_eq(value, default, concept), Ok(true));
    if std::iter::once(default)
        .chain(&unimplemented.equivalent)
        .any(is_default)
    {
        return None;
    }
    Some(
        Diagnostic::error(
            DiagnosticCode::Unimplemented,
            unimplemented.refusal(&binding.name, default),
        )
        .for_parameter(binding.name.clone()),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::profile::{ProfileError, resolve_with_overrides};

    fn concept(id: &str, domain: serde_json::Value) -> serde_json::Value {
        json!({
            "id": id,
            "semantic_revision": 1,
            "casa_name": "text",
            "value_domain": domain,
            "normalization": {"kind": "trim"},
            "base_constraints": [],
            "unit_dimension": null,
            "semantic_role": "algorithm",
            "documentation": {"summary": "Text", "details": null, "examples": []},
            "persistence_class": "profile"
        })
    }

    /// `box` and `mode` are implemented; `stokes` is the plain marker;
    /// `varnames` takes CASA's `[]` for its `''`; `fence` is a number;
    /// `sigma` is read only when `mode` is `poli`.
    fn bundle() -> SurfaceContractBundle {
        let binding = |name: &str, order: usize, concept: &str, default: serde_json::Value| {
            json!({
                "name": name,
                "concept": {"id": concept, "semantic_revision": 1},
                "order": order,
                "default": {"kind": "literal", "value": default},
                "required_when": {"kind": "never"},
                "active_when": {"kind": "always"},
                "projections": {
                    "python": {"name": name},
                    "presentation": {"label": name, "group": "General"}
                }
            })
        };
        let text = || json!({"kind": "string", "value": ""});
        let unimplemented = |mut binding: serde_json::Value, marker: serde_json::Value| {
            binding["unimplemented"] = marker;
            binding
        };
        serde_json::from_value(json!({
            "schema_version": 1,
            "surface": {
                "kind": "task",
                "id": "example",
                "contract_version": 1,
                "display_name": "Example",
                "category": "Test",
                "summary": "Example",
                "provider_family": "test",
                "casa_task": "example",
                "bindings": [
                    binding("box", 0, "example.text", text()),
                    unimplemented(
                        binding("stokes", 1, "example.text", text()),
                        json!({"issue": 977}),
                    ),
                    binding("mode", 2, "example.text", text()),
                    unimplemented(
                        binding("varnames", 3, "example.list", text()),
                        json!({"issue": 977, "equivalent": [{"kind": "array", "value": []}]}),
                    ),
                    unimplemented(
                        binding("fence", 4, "example.number", json!({"kind": "float", "value": -1.0})),
                        json!({"issue": 977}),
                    ),
                    unimplemented(
                        binding("sigma", 5, "example.text", json!({"kind": "string", "value": "0.0mJy/beam"})),
                        json!({"issue": 977, "applies_when": {"kind": "equals", "parameter": "mode",
                               "value": {"kind": "string", "value": "poli"}}}),
                    ),
                ],
            },
            "catalog": {
                "schema_version": 1,
                "concepts": [
                    concept("example.text", json!({"kind": "string"})),
                    concept("example.list", json!({"kind": "array", "element": {"kind": "string"},
                                                   "min_items": 0, "allow_scalar": true})),
                    concept("example.number", json!({"kind": "float"})),
                ]
            }
        }))
        .unwrap()
    }

    fn resolve_values(values: &[(&str, ParameterValue)]) -> Result<(), Vec<Diagnostic>> {
        let overrides = values
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect();
        match resolve_with_overrides(&bundle(), &overrides) {
            Ok(_) => Ok(()),
            Err(ProfileError::Diagnostics(diagnostics)) => Err(diagnostics),
            Err(other) => panic!("unexpected error {other}"),
        }
    }

    fn resolve(values: &[(&str, &str)]) -> Result<(), Vec<Diagnostic>> {
        let values = values
            .iter()
            .map(|(name, value)| (*name, ParameterValue::String(value.to_string())))
            .collect::<Vec<_>>();
        resolve_values(&values)
    }

    /// The parameters `result` refuses.
    fn refused(result: Result<(), Vec<Diagnostic>>) -> Vec<String> {
        result
            .err()
            .unwrap_or_default()
            .into_iter()
            .map(|diagnostic| {
                assert_eq!(diagnostic.code, DiagnosticCode::Unimplemented);
                diagnostic.parameter.unwrap()
            })
            .collect()
    }

    #[test]
    fn an_unimplemented_parameter_is_accepted_at_its_default() {
        assert_eq!(resolve(&[]), Ok(()));
        assert_eq!(resolve(&[("stokes", "")]), Ok(()));
        // Equal after normalization is the default too.
        assert_eq!(resolve(&[("stokes", "  ")]), Ok(()));
        assert_eq!(resolve(&[("box", "0,0,3,3")]), Ok(()));
    }

    /// CASA's spellings of the default are the default: `varnames=''` and
    /// the `[]` CASA reads as it. A scalar is a one-item list here, so
    /// `['']` is `''` too, though CASA's own `['']` names an empty
    /// variable and fails.
    #[test]
    fn casas_equivalent_spellings_are_the_default() {
        use ParameterValue::{Array, String as Text};
        for value in [
            Text(String::new()),
            Array(vec![]),
            Array(vec![Text(" ".into())]),
        ] {
            assert_eq!(
                resolve_values(&[("varnames", value.clone())]),
                Ok(()),
                "{value:?}"
            );
        }
        assert_eq!(
            refused(resolve_values(&[(
                "varnames",
                Array(vec![Text("a".into())])
            )])),
            ["varnames"]
        );
        // `stokes` has no equivalent: `[]` is not its `''`.
        assert!(resolve_values(&[("stokes", Array(vec![]))]).is_err());
    }

    /// Numbers are the default within a relative 1e-12, whatever their
    /// type; nothing further.
    #[test]
    fn a_number_is_the_default_within_a_relative_1e_12() {
        use ParameterValue::{Float, Integer};
        for value in [Integer(-1), Float(-1.0), Float(-1.0 - 5.0e-13)] {
            assert_eq!(
                resolve_values(&[("fence", value.clone())]),
                Ok(()),
                "{value:?}"
            );
        }
        for value in [Float(-1.0 - 5.0e-12), Float(-1.0001), Integer(0)] {
            assert_eq!(
                refused(resolve_values(&[("fence", value.clone())])),
                ["fence"],
                "{value:?}"
            );
        }
    }

    /// Where CASA ignores a parameter casa-rs takes any value for it, and
    /// refuses all but the default where CASA reads it.
    #[test]
    fn a_parameter_casa_ignores_takes_any_value() {
        assert_eq!(resolve(&[("sigma", "5Jy/beam")]), Ok(()));
        assert_eq!(resolve(&[("mode", "evalexpr"), ("sigma", "junk")]), Ok(()));
        assert_eq!(
            refused(resolve(&[("mode", "poli"), ("sigma", "5Jy/beam")])),
            ["sigma"]
        );
        assert_eq!(
            resolve(&[("mode", "poli"), ("sigma", " 0.0mJy/beam ")]),
            Ok(())
        );
    }

    #[test]
    fn any_other_value_is_refused_naming_its_issue() {
        let diagnostics = resolve(&[("stokes", "I")]).unwrap_err();
        assert_eq!(diagnostics.len(), 1);
        assert_eq!(diagnostics[0].code, DiagnosticCode::Unimplemented);
        assert_eq!(diagnostics[0].parameter.as_deref(), Some("stokes"));
        assert_eq!(
            diagnostics[0].message,
            "parameter \"stokes\" is not implemented by casa-rs yet (#977); only CASA's \
             default '' is accepted"
        );
    }
}
