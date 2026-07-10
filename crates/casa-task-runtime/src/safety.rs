// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_provider_contracts::{Predicate, RunSafetyClass, SurfaceContractBundle};
use thiserror::Error;

use crate::{ParameterState, semantic_eq};

/// Evaluated run-safety requirements consumed uniformly by frontends.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunSafetyRequirements {
    classes: BTreeSet<RunSafetyClass>,
}

impl RunSafetyRequirements {
    pub fn classes(&self) -> &BTreeSet<RunSafetyClass> {
        &self.classes
    }

    /// Interactive launchers confirm product creation and destructive writes.
    pub fn requires_interactive_confirmation(&self) -> bool {
        !self.classes.is_empty()
    }

    /// Runtime authorization required by `--confirm-overwrite`-style controls.
    pub fn requires_overwrite_confirmation(&self) -> bool {
        self.classes.contains(&RunSafetyClass::Overwrite)
    }

    /// Runtime authorization required by `--confirm-mutation`-style controls.
    pub fn requires_input_mutation_confirmation(&self) -> bool {
        self.classes.contains(&RunSafetyClass::InputMutation)
    }
}

/// Failure to evaluate catalog-owned run-safety rules.
#[derive(Debug, Error)]
pub enum RunSafetyEvaluationError {
    #[error("{surface} safety rule references unknown parameter {parameter:?}")]
    UnknownParameter { surface: String, parameter: String },
    #[error("{surface} safety rule cannot resolve the concept for {parameter:?}")]
    MissingConcept { surface: String, parameter: String },
    #[error("{surface} safety comparison for {parameter:?} is invalid: {reason}")]
    InvalidComparison {
        surface: String,
        parameter: String,
        reason: String,
    },
}

/// Evaluate the active task risks for resolved typed parameter states.
///
/// Predicate comparisons use concept normalization and semantic equality.
/// A predicate that depends on an inactive parameter is indeterminate and
/// therefore cannot activate a safety class, including through `not`.
pub fn required_run_safety(
    bundle: &SurfaceContractBundle,
    states: &BTreeMap<String, ParameterState>,
) -> Result<RunSafetyRequirements, RunSafetyEvaluationError> {
    let mut required = BTreeSet::new();
    for rule in bundle.surface.safety_rules() {
        if evaluate_safety_predicate(&rule.when, bundle, states)? == Some(true) {
            required.insert(rule.class);
        }
    }
    Ok(RunSafetyRequirements { classes: required })
}

fn evaluate_safety_predicate(
    predicate: &Predicate,
    bundle: &SurfaceContractBundle,
    states: &BTreeMap<String, ParameterState>,
) -> Result<Option<bool>, RunSafetyEvaluationError> {
    match predicate {
        Predicate::Always => Ok(Some(true)),
        Predicate::Never => Ok(Some(false)),
        Predicate::IsSet { parameter } => {
            let state = safety_parameter_state(bundle, states, parameter)?;
            if !state.active {
                return Ok(None);
            }
            Ok(Some(state.value.is_some()))
        }
        Predicate::Equals { parameter, value } => {
            let state = safety_parameter_state(bundle, states, parameter)?;
            if !state.active {
                return Ok(None);
            }
            let Some(actual) = state.value.as_ref() else {
                return Ok(Some(false));
            };
            let binding = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| binding.name == *parameter)
                .ok_or_else(|| RunSafetyEvaluationError::UnknownParameter {
                    surface: bundle.surface.id().to_string(),
                    parameter: parameter.clone(),
                })?;
            let concept = bundle.catalog.concept(&binding.concept).ok_or_else(|| {
                RunSafetyEvaluationError::MissingConcept {
                    surface: bundle.surface.id().to_string(),
                    parameter: parameter.clone(),
                }
            })?;
            semantic_eq(actual, value, concept)
                .map(Some)
                .map_err(|error| RunSafetyEvaluationError::InvalidComparison {
                    surface: bundle.surface.id().to_string(),
                    parameter: parameter.clone(),
                    reason: error.to_string(),
                })
        }
        Predicate::Not { predicate } => {
            Ok(evaluate_safety_predicate(predicate, bundle, states)?.map(|value| !value))
        }
        Predicate::All { predicates } => {
            let mut indeterminate = false;
            for predicate in predicates {
                match evaluate_safety_predicate(predicate, bundle, states)? {
                    Some(false) => return Ok(Some(false)),
                    Some(true) => {}
                    None => indeterminate = true,
                }
            }
            Ok((!indeterminate).then_some(true))
        }
        Predicate::Any { predicates } => {
            let mut indeterminate = false;
            for predicate in predicates {
                match evaluate_safety_predicate(predicate, bundle, states)? {
                    Some(true) => return Ok(Some(true)),
                    Some(false) => {}
                    None => indeterminate = true,
                }
            }
            Ok((!indeterminate).then_some(false))
        }
    }
}

fn safety_parameter_state<'a>(
    bundle: &SurfaceContractBundle,
    states: &'a BTreeMap<String, ParameterState>,
    parameter: &str,
) -> Result<&'a ParameterState, RunSafetyEvaluationError> {
    if !bundle
        .surface
        .bindings()
        .iter()
        .any(|binding| binding.name == parameter)
    {
        return Err(RunSafetyEvaluationError::UnknownParameter {
            surface: bundle.surface.id().to_string(),
            parameter: parameter.to_string(),
        });
    }
    states
        .get(parameter)
        .ok_or_else(|| RunSafetyEvaluationError::UnknownParameter {
            surface: bundle.surface.id().to_string(),
            parameter: parameter.to_string(),
        })
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{
        ParameterValue, Predicate, RunSafetyClass, SurfaceDefinition, TaskSafetyRule,
        builtin_surface_bundle,
    };

    use crate::ParameterSession;

    use super::*;

    #[test]
    fn product_writes_drive_only_the_interactive_gate() {
        let bundle = builtin_surface_bundle("simanalyze").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();

        let requirements = session.required_run_safety().unwrap();
        assert!(requirements.requires_interactive_confirmation());
        assert!(requirements.requires_overwrite_confirmation());
        assert!(!requirements.requires_input_mutation_confirmation());
        assert!(
            requirements
                .classes()
                .contains(&RunSafetyClass::ProductWrite)
        );

        session
            .set("overwrite", ParameterValue::Bool(false))
            .unwrap();
        let requirements = session.required_run_safety().unwrap();
        assert_eq!(
            requirements.classes(),
            &BTreeSet::from([RunSafetyClass::ProductWrite])
        );
        assert!(requirements.requires_interactive_confirmation());
        assert!(!requirements.requires_overwrite_confirmation());
        assert!(!requirements.requires_input_mutation_confirmation());
    }

    #[test]
    fn conditional_input_mutation_tracks_the_resolved_mode() {
        let bundle = builtin_surface_bundle("imhead").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        assert!(session.required_run_safety().unwrap().classes().is_empty());

        session
            .set("mode", ParameterValue::String("put".to_string()))
            .unwrap();
        let requirements = session.required_run_safety().unwrap();
        assert!(requirements.requires_interactive_confirmation());
        assert!(requirements.requires_input_mutation_confirmation());
        assert!(!requirements.requires_overwrite_confirmation());
    }

    #[test]
    fn imager_modelcolumn_write_requires_input_mutation_confirmation() {
        let bundle = builtin_surface_bundle("imager").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        assert!(
            !session
                .required_run_safety()
                .unwrap()
                .requires_input_mutation_confirmation()
        );

        session
            .set(
                "savemodel",
                ParameterValue::String("modelcolumn".to_string()),
            )
            .unwrap();
        assert!(
            session
                .required_run_safety()
                .unwrap()
                .requires_input_mutation_confirmation()
        );
    }

    #[test]
    fn inactive_values_cannot_activate_even_a_negated_safety_predicate() {
        let mut bundle = builtin_surface_bundle("calibrate").unwrap();
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!("calibrate is a task")
        };
        definition.safety_rules = vec![TaskSafetyRule {
            class: RunSafetyClass::InputMutation,
            when: Predicate::Not {
                predicate: Box::new(Predicate::Equals {
                    parameter: "spw".to_string(),
                    value: ParameterValue::String("danger".to_string()),
                }),
            },
        }];
        let mut session = ParameterSession::defaults(bundle).unwrap();
        session
            .set("selectdata", ParameterValue::Bool(false))
            .unwrap();
        assert!(!session.states()["spw"].active);
        assert!(session.required_run_safety().unwrap().classes().is_empty());
    }

    #[test]
    fn safety_comparisons_use_concept_semantic_equality() {
        let mut bundle = builtin_surface_bundle("msexplore").unwrap();
        let SurfaceDefinition::Task(definition) = &mut bundle.surface else {
            unreachable!("msexplore is a task")
        };
        definition.safety_rules = vec![TaskSafetyRule {
            class: RunSafetyClass::InputMutation,
            when: Predicate::Equals {
                parameter: "vis".to_string(),
                value: ParameterValue::String("./b.ms".to_string()),
            },
        }];
        let mut session = ParameterSession::defaults(bundle).unwrap();
        session
            .set("vis", ParameterValue::String("a/../b.ms".to_string()))
            .unwrap();
        assert!(
            session
                .required_run_safety()
                .unwrap()
                .requires_input_mutation_confirmation()
        );
    }
}
