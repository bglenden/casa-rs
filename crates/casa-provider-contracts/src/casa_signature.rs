// SPDX-License-Identifier: LGPL-3.0-or-later
//! The rules for a surface that mimics a CASA task (#931).
//!
//! A surface whose [`TaskDefinition::casa_task`] names a CASA task presents
//! that task's signature: CASA's parameters, in CASA's order and with CASA's
//! literal defaults, then casa-rs-only controls as keyword-only arguments
//! ([`PythonParameterProjection::keyword_only`]). A CASA parameter that
//! must be given (CASA's `imagename=''`) keeps CASA's default and is checked
//! when the task runs, through its `required_when`.
//!
//! A CASA parameter casa-rs does not implement yet is an
//! [`UnimplementedParameter`]: it is accepted at its default, so CASA's
//! signature is exact, and any other value is refused before the task
//! starts, naming the issue that tracks it.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::{ContractValidationError, DefaultSpec, ParameterValue, SurfaceDefinition};

/// A CASA parameter casa-rs accepts only at CASA's default until `issue`
/// implements it (#931).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct UnimplementedParameter {
    /// The casa-rs GitHub issue that tracks implementing the parameter.
    pub issue: u32,
}

impl UnimplementedParameter {
    /// The refusal for `name` set to anything but its `default`.
    pub fn refusal(&self, name: &str, default: &ParameterValue) -> String {
        format!(
            "parameter {name:?} is not implemented by casa-rs yet (#{}); only CASA's default \
             {} is accepted",
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
            UnimplementedParameter { issue: 977 }.refusal("axes", &ParameterValue::Array(vec![]));
        assert_eq!(
            message,
            "parameter \"axes\" is not implemented by casa-rs yet (#977); only CASA's default [] \
             is accepted"
        );
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
