// SPDX-License-Identifier: LGPL-3.0-or-later
//! The rules for a surface that mimics a CASA task (#931).
//!
//! A surface whose [`casa_task`](super::TaskDefinition::casa_task) names a
//! CASA task presents that task's signature: CASA's parameters, in CASA's
//! order and with CASA's literal defaults, then casa-rs-only controls as
//! keyword-only arguments
//! ([`keyword_only`](super::PythonParameterProjection::keyword_only)). A CASA parameter that
//! must be given (CASA's `imagename=''`) keeps CASA's default and is checked
//! when the task runs, through its `required_when`.
//!
//! A CASA parameter casa-rs does not implement yet is an
//! [`UnimplementedParameter`]: it is accepted at its default, so CASA's
//! signature is exact, and any other value is refused before the task
//! starts, naming the issue that tracks it.

use std::collections::BTreeSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{
    ContractValidationError, DefaultSpec, ParameterValue, Predicate, SurfaceContractBundle,
    SurfaceDefinition,
};

/// A CASA parameter casa-rs accepts only at CASA's default until `issue`
/// implements it (#931).
///
/// "CASA's default" is the default as CASA reads it, which is this law:
///
/// - Where [`applies_when`](Self::applies_when) is false over the resolved
///   values, CASA ignores the parameter (immath's `sigma` outside its
///   polarization modes, any task's `stretch` without a `mask`), and so
///   does casa-rs: every value is accepted and none is used.
/// - Elsewhere a value is accepted when it equals the binding's literal
///   default or one of the [`equivalent`](Self::equivalent) values CASA
///   reads as that default (immath's `varnames=[]` for `''`). Equality is
///   the concept's semantic equality: both sides are first normalized by
///   the concept's rule (text trimmed or lowercased, a quantity in its
///   canonical unit, a scalar as a one-item list), then compared exactly,
///   except that numbers agree within a relative 1e-12, so `-1` and `-1.0`
///   are one value.
///
/// Any other value is refused before the task starts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct UnimplementedParameter {
    /// The casa-rs GitHub issue that tracks implementing the parameter.
    pub issue: u32,
    /// Other values CASA reads exactly as the default, compared as the
    /// default is.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub equivalent: Vec<ParameterValue>,
    /// Where CASA reads the parameter at all; elsewhere it ignores the
    /// value, and casa-rs accepts any value without using it.
    #[serde(default = "always", skip_serializing_if = "is_always")]
    pub applies_when: Predicate,
}

fn always() -> Predicate {
    Predicate::Always
}

fn is_always(predicate: &Predicate) -> bool {
    matches!(predicate, Predicate::Always)
}

impl UnimplementedParameter {
    /// The marker for a parameter `issue` tracks, accepted at its default
    /// alone wherever it is given.
    pub fn new(issue: u32) -> Self {
        Self {
            issue,
            equivalent: Vec::new(),
            applies_when: Predicate::Always,
        }
    }

    /// The refusal for `name` set to anything but its `default`.
    pub fn refusal(&self, name: &str, default: &ParameterValue) -> String {
        let equivalent = if self.equivalent.is_empty() {
            String::new()
        } else {
            let values = self.equivalent.iter().map(python_repr).collect::<Vec<_>>();
            format!(" (or {}, which CASA reads as it)", values.join(" or "))
        };
        format!(
            "parameter {name:?} is not implemented by casa-rs yet (#{}); only CASA's default \
             {}{equivalent} is accepted",
            self.issue,
            python_repr(default)
        )
    }
}

/// `value` as Python writes it, the spelling CASA's signatures show.
pub fn python_repr(value: &ParameterValue) -> String {
    match value {
        ParameterValue::Bool(true) => "True".into(),
        ParameterValue::Bool(false) => "False".into(),
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Float(value) => format!("{value:?}"),
        ParameterValue::String(value) => {
            format!("'{}'", value.replace('\\', "\\\\").replace('\'', "\\'"))
        }
        ParameterValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(python_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ParameterValue::Table(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(key, value)| format!("'{key}': {}", python_repr(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Check the CASA-signature rules of `surface`.
pub(super) fn validate_casa_signature(
    surface: &SurfaceDefinition,
    errors: &mut Vec<ContractValidationError>,
) {
    let mut bindings = surface.bindings().iter().collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mimics_casa = surface.casa_task().is_some();
    let mut first_keyword_only: Option<&str> = None;
    for binding in bindings {
        let error = |code: &str, message: String| {
            ContractValidationError::new(code, message).at(surface.id(), Some(&binding.name))
        };
        let python = binding.projections.python.as_ref();
        let keyword_only = python.is_some_and(|python| python.keyword_only);
        if binding.unimplemented.is_some()
            && !matches!(binding.default, DefaultSpec::Literal { .. })
        {
            errors.push(error(
                "unimplemented_default",
                "a parameter accepted only at its default needs a literal default".into(),
            ));
        }
        if !mimics_casa {
            if binding.unimplemented.is_some() || keyword_only {
                errors.push(error(
                    "casa_signature_only",
                    "unimplemented and keyword-only parameters belong to a surface that \
                     mimics a CASA task"
                        .into(),
                ));
            }
            continue;
        }
        if python.is_none() {
            errors.push(error(
                "casa_python_projection",
                "every parameter of a CASA task's surface has a Python projection".into(),
            ));
        }
        if keyword_only {
            if binding.unimplemented.is_some() {
                errors.push(error(
                    "unimplemented_extra",
                    "a keyword-only control is casa-rs's own; only a CASA parameter can be \
                     unimplemented"
                        .into(),
                ));
            }
            first_keyword_only.get_or_insert(&binding.name);
            continue;
        }
        if let Some(extra) = first_keyword_only {
            errors.push(error(
                "keyword_only_order",
                format!(
                    "CASA parameter {:?} follows the keyword-only {extra:?}",
                    binding.name
                ),
            ));
        }
        if !matches!(binding.default, DefaultSpec::Literal { .. }) {
            errors.push(error(
                "casa_default",
                "a CASA parameter takes CASA's literal default; one that must be given is \
                 checked when the task runs"
                    .into(),
            ));
        }
    }
}

/// Check that every [`UnimplementedParameter`] of `bundle` gives equivalent
/// defaults of its concept's type and an `applies_when` over the surface's
/// parameters (`known`).
pub(super) fn validate_unimplemented(
    bundle: &SurfaceContractBundle,
    known: &BTreeSet<String>,
    errors: &mut Vec<ContractValidationError>,
) {
    let surface = bundle.surface.id();
    for binding in bundle.surface.bindings() {
        let Some(unimplemented) = &binding.unimplemented else {
            continue;
        };
        super::validate_predicate_refs(
            &unimplemented.applies_when,
            known,
            surface,
            &binding.name,
            errors,
        );
        let Some(concept) = bundle.catalog.concept(&binding.concept) else {
            continue;
        };
        for value in &unimplemented.equivalent {
            if let Err(reason) = super::validate_parameter_value(value, &concept.value_domain) {
                errors.push(
                    ContractValidationError::new(
                        "unimplemented_equivalent",
                        format!(
                            "equivalent default {} is not a {} value: {reason}",
                            python_repr(value),
                            concept.id
                        ),
                    )
                    .at(surface, Some(&binding.name)),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;

    fn binding(name: &str, order: usize, default: Value, extra: Value) -> Value {
        let mut binding = json!({
            "name": name,
            "concept": {"id": "example.text", "semantic_revision": 1},
            "order": order,
            "default": default,
            "required_when": {"kind": "never"},
            "active_when": {"kind": "always"},
            "projections": {
                "python": {"name": name},
                "presentation": {"label": name, "group": "General"}
            }
        });
        for (key, value) in extra.as_object().unwrap() {
            if key == "keyword_only" {
                binding["projections"]["python"]["keyword_only"] = value.clone();
            } else {
                binding[key] = value.clone();
            }
        }
        binding
    }

    fn literal() -> Value {
        json!({"kind": "literal", "value": {"kind": "string", "value": ""}})
    }

    fn codes(casa_task: Option<&str>, bindings: Vec<Value>) -> Vec<String> {
        let surface: SurfaceDefinition = serde_json::from_value(json!({
            "kind": "task",
            "id": "example",
            "contract_version": 1,
            "display_name": "Example",
            "category": "Test",
            "summary": "Example",
            "provider_family": "test",
            "casa_task": casa_task,
            "bindings": bindings,
        }))
        .unwrap();
        let mut errors = Vec::new();
        validate_casa_signature(&surface, &mut errors);
        errors.into_iter().map(|error| error.code).collect()
    }

    #[test]
    fn a_casa_signature_has_literal_defaults_then_keyword_only_extras() {
        let bindings = vec![
            binding("imagename", 0, literal(), json!({})),
            binding(
                "axes",
                1,
                literal(),
                json!({"unimplemented": {"issue": 977}}),
            ),
            binding("overwrite", 2, literal(), json!({"keyword_only": true})),
        ];
        assert!(codes(Some("imstat"), bindings).is_empty());
    }

    #[test]
    fn casa_parameters_need_literal_defaults_and_python_names() {
        let mut nameless = binding("box", 1, literal(), json!({}));
        nameless["projections"]
            .as_object_mut()
            .unwrap()
            .remove("python");
        let bindings = vec![
            binding("imagename", 0, json!({"kind": "required"}), json!({})),
            nameless,
        ];
        assert_eq!(
            codes(Some("imstat"), bindings),
            ["casa_default", "casa_python_projection"]
        );
    }

    #[test]
    fn keyword_only_extras_follow_casas_parameters_and_are_implemented() {
        let bindings = vec![
            binding(
                "overwrite",
                0,
                literal(),
                json!({"keyword_only": true, "unimplemented": {"issue": 977}}),
            ),
            binding("imagename", 1, literal(), json!({})),
        ];
        assert_eq!(
            codes(Some("imstat"), bindings),
            ["unimplemented_extra", "keyword_only_order"]
        );
    }

    #[test]
    fn only_a_casa_surface_marks_parameters_unimplemented_or_keyword_only() {
        let bindings = vec![
            binding("a", 0, literal(), json!({"unimplemented": {"issue": 977}})),
            binding("b", 1, literal(), json!({"keyword_only": true})),
        ];
        assert_eq!(
            codes(None, bindings),
            ["casa_signature_only", "casa_signature_only"]
        );
    }

    #[test]
    fn an_unimplemented_parameter_needs_a_literal_default() {
        let conditional = json!({
            "kind": "conditional",
            "cases": [],
            "fallback": {"kind": "string", "value": ""}
        });
        let bindings = vec![binding(
            "axes",
            0,
            conditional,
            json!({"unimplemented": {"issue": 977}}),
        )];
        assert_eq!(
            codes(Some("imstat"), bindings),
            ["unimplemented_default", "casa_default"]
        );
    }

    #[test]
    fn the_refusal_names_the_parameter_its_default_and_its_issue() {
        let message =
            UnimplementedParameter::new(977).refusal("axes", &ParameterValue::Array(vec![]));
        assert_eq!(
            message,
            "parameter \"axes\" is not implemented by casa-rs yet (#977); only CASA's default [] \
             is accepted"
        );
        let varnames = UnimplementedParameter {
            equivalent: vec![ParameterValue::Array(vec![])],
            ..UnimplementedParameter::new(977)
        };
        assert_eq!(
            varnames.refusal("varnames", &ParameterValue::String(String::new())),
            "parameter \"varnames\" is not implemented by casa-rs yet (#977); only CASA's \
             default '' (or [], which CASA reads as it) is accepted"
        );
    }

    #[test]
    fn equivalent_defaults_are_the_concepts_values_and_applies_when_names_parameters() {
        let sigma = json!({"unimplemented": {
            "issue": 977,
            "equivalent": [{"kind": "integer", "value": 0}, {"kind": "string", "value": "0"}],
            "applies_when": {"kind": "equals", "parameter": "polmode",
                             "value": {"kind": "string", "value": "poli"}}
        }});
        let bundle: SurfaceContractBundle = serde_json::from_value(json!({
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
                    binding("mode", 0, literal(), json!({})),
                    binding("sigma", 1, literal(), sigma),
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
        .unwrap();
        let known = ["mode", "sigma"].map(String::from).into();
        let mut errors = Vec::new();
        validate_unimplemented(&bundle, &known, &mut errors);
        let codes = errors
            .iter()
            .map(|error| error.code.as_str())
            .collect::<Vec<_>>();
        // The integer is not text; "0" is. `polmode` is not a parameter.
        assert_eq!(
            codes,
            ["unknown_predicate_parameter", "unimplemented_equivalent"]
        );
        // Absent, the two fields are the plain marker, and it saves as one.
        let plain: UnimplementedParameter = serde_json::from_value(json!({"issue": 977})).unwrap();
        assert_eq!(plain, UnimplementedParameter::new(977));
        assert_eq!(serde_json::to_value(&plain).unwrap(), json!({"issue": 977}));
    }

    #[test]
    fn defaults_read_as_python_writes_them() {
        let cases = [
            (ParameterValue::Bool(false), "False"),
            (ParameterValue::Integer(-1), "-1"),
            (ParameterValue::Float(-1.0), "-1.0"),
            (ParameterValue::Float(0.5), "0.5"),
            (ParameterValue::String("it's".into()), r"'it\'s'"),
            (
                ParameterValue::Array(vec![
                    ParameterValue::Integer(0),
                    ParameterValue::String(String::new()),
                ]),
                "[0, '']",
            ),
        ];
        for (value, text) in cases {
            assert_eq!(python_repr(&value), text);
        }
    }
}
