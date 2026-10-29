// SPDX-License-Identifier: LGPL-3.0-or-later
//! Refusing the CASA parameters casa-rs does not implement yet (#931).
//!
//! A surface that mimics a CASA task takes every CASA parameter, so CASA's
//! signature is exact. A parameter casa-rs does not implement yet carries
//! [`UnimplementedParameter`](casa_provider_contracts::UnimplementedParameter)
//! in the contract: resolution accepts it at its
//! default and refuses any other value before the task starts, on the
//! command line and from Python alike.

use std::collections::BTreeMap;

use casa_provider_contracts::{
    DefaultSpec, ParameterValue, SurfaceContractBundle, SurfaceParameterBinding,
};

use crate::diagnostic::{Diagnostic, DiagnosticCode};
use crate::normalize::semantic_eq;

/// The refusal of `binding`'s resolved value, when the binding is
/// unimplemented and the value is not its default.
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
    let value = values.get(&binding.name)?;
    let concept = bundle.catalog.concept(&binding.concept)?;
    if semantic_eq(value, default, concept).unwrap_or(false) {
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

    fn bundle() -> SurfaceContractBundle {
        let binding = |name: &str, order: usize, extra: serde_json::Value| {
            let mut binding = json!({
                "name": name,
                "concept": {"id": "example.text", "semantic_revision": 1},
                "order": order,
                "default": {"kind": "literal", "value": {"kind": "string", "value": ""}},
                "required_when": {"kind": "never"},
                "active_when": {"kind": "always"},
                "projections": {
                    "python": {"name": name},
                    "presentation": {"label": name, "group": "General"}
                }
            });
            if let Some(unimplemented) = extra.get("unimplemented") {
                binding["unimplemented"] = unimplemented.clone();
            }
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
                    binding("box", 0, json!({})),
                    binding("stokes", 1, json!({"unimplemented": {"issue": 977}})),
                ],
            },
            "catalog": {
                "schema_version": 1,
                "concepts": [{
                    "id": "example.text",
                    "semantic_revision": 1,
                    "casa_name": "text",
                    "value_domain": {"kind": "string"},
                    "normalization": {"kind": "trim"},
                    "base_constraints": [],
                    "unit_dimension": null,
                    "semantic_role": "algorithm",
                    "documentation": {"summary": "Text", "details": null, "examples": []},
                    "persistence_class": "profile"
                }]
            }
        }))
        .unwrap()
    }

    fn resolve(values: &[(&str, &str)]) -> Result<(), Vec<Diagnostic>> {
        let overrides = values
            .iter()
            .map(|(name, value)| (name.to_string(), ParameterValue::String(value.to_string())))
            .collect();
        match resolve_with_overrides(&bundle(), &overrides) {
            Ok(_) => Ok(()),
            Err(ProfileError::Diagnostics(diagnostics)) => Err(diagnostics),
            Err(other) => panic!("unexpected error {other}"),
        }
    }

    #[test]
    fn an_unimplemented_parameter_is_accepted_at_its_default() {
        assert_eq!(resolve(&[]), Ok(()));
        assert_eq!(resolve(&[("stokes", "")]), Ok(()));
        // Equal after normalization is the default too.
        assert_eq!(resolve(&[("stokes", "  ")]), Ok(()));
        assert_eq!(resolve(&[("box", "0,0,3,3")]), Ok(()));
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
