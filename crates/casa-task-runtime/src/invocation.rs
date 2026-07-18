// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_provider_contracts::{
    ManagedOutputArgument, NarrowingConstraint, ParameterValue, Predicate, ProviderInvocation,
    ProviderInvocationAdaptation, ValueAdapter,
};
use thiserror::Error;

use crate::{ParameterSession, semantic_eq};

/// Failure to project a resolved parameter session into a provider process.
#[derive(Debug, Error)]
pub enum ProviderInvocationError {
    #[error("cannot project {parameter:?}: {reason}")]
    Parameter { parameter: String, reason: String },
    #[error("invalid managed-output invocation metadata: {0}")]
    ManagedOutput(String),
    #[error("provider invocation adapter failed: {0}")]
    Adapter(String),
    #[error(
        "provider adapter for {surface:?} did not consume active parameters with no direct CLI spelling: {parameters:?}"
    )]
    UnprojectedParameters {
        surface: String,
        parameters: Vec<String>,
    },
}

/// Build the complete provider argument vector and optional private stdin
/// payload from one resolved typed parameter session. Provider-specific
/// adapters remain outside this generic lifecycle crate; they must explicitly
/// report every active parameter they consume when that parameter has no
/// direct CLI spelling.
pub fn project_provider_invocation<F>(
    session: &ParameterSession,
    adapter: F,
) -> Result<ProviderInvocation, ProviderInvocationError>
where
    F: FnOnce(
        &str,
        &BTreeMap<String, ParameterValue>,
        ProviderInvocation,
    ) -> Result<ProviderInvocationAdaptation, String>,
{
    let mut args = session.bundle().surface.execution().fixed_args.clone();
    let (direct_args, unprojected_parameters) = project_direct_cli_args(session)?;
    args.extend(direct_args);
    append_managed_output_arguments(
        &mut args,
        session.bundle().surface.execution().managed_output.as_ref(),
    )?;

    let active_values = session
        .states()
        .iter()
        .filter_map(|(name, state)| {
            state
                .active
                .then(|| state.value.clone().map(|value| (name.clone(), value)))
                .flatten()
        })
        .collect::<BTreeMap<_, _>>();
    let adaptation = adapter(
        session.bundle().surface.provider_family(),
        &active_values,
        ProviderInvocation::direct(args),
    )
    .map_err(ProviderInvocationError::Adapter)?;
    let unconsumed = unprojected_parameters
        .difference(&adaptation.consumed_parameters)
        .cloned()
        .collect::<Vec<_>>();
    if !unconsumed.is_empty() {
        return Err(ProviderInvocationError::UnprojectedParameters {
            surface: session.bundle().surface.id().to_string(),
            parameters: unconsumed,
        });
    }
    Ok(adaptation.invocation)
}

/// Return whether one binding applies to the provider for the session's
/// current resolved values.
///
/// Interactive consumers use this to render mode-specific fields without
/// reimplementing provider predicates or failing open when a binding cannot
/// be resolved.
pub fn provider_parameter_applies(
    session: &ParameterSession,
    parameter: &str,
) -> Result<bool, ProviderInvocationError> {
    let binding = session
        .bundle()
        .surface
        .bindings()
        .iter()
        .find(|binding| binding.name == parameter)
        .ok_or_else(|| ProviderInvocationError::Parameter {
            parameter: parameter.to_string(),
            reason: "unknown surface parameter".to_string(),
        })?;
    let state =
        session
            .states()
            .get(parameter)
            .ok_or_else(|| ProviderInvocationError::Parameter {
                parameter: parameter.to_string(),
                reason: "resolved parameter state is missing".to_string(),
            })?;
    if !state.active {
        return Ok(false);
    }
    let Some(predicate) = binding
        .projections
        .provider
        .as_ref()
        .and_then(|projection| projection.emit_when.as_ref())
    else {
        return Ok(true);
    };
    Ok(evaluate_provider_predicate(predicate, session) == Some(true))
}

fn project_direct_cli_args(
    session: &ParameterSession,
) -> Result<(Vec<String>, BTreeSet<String>), ProviderInvocationError> {
    let mut bindings = session
        .bundle()
        .surface
        .bindings()
        .iter()
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mut positionals = BTreeMap::<usize, String>::new();
    let mut options = Vec::<String>::new();
    let mut unprojected = BTreeSet::<String>::new();
    for binding in bindings {
        if let Some(predicate) = binding
            .projections
            .provider
            .as_ref()
            .and_then(|projection| projection.emit_when.as_ref())
            && evaluate_provider_predicate(predicate, session) != Some(true)
        {
            continue;
        }
        let Some(state) = session.states().get(&binding.name) else {
            continue;
        };
        if !state.active {
            continue;
        }
        let Some(value) = state.value.as_ref() else {
            continue;
        };
        let projection = binding.projections.cli.as_ref();
        let directly_projectable = match value {
            ParameterValue::Bool(_) => projection.is_some_and(|projection| {
                !projection.flags.is_empty() || !projection.false_flags.is_empty()
            }),
            _ => projection.is_some_and(|projection| {
                projection.positional.is_some() || !projection.flags.is_empty()
            }),
        };
        if !directly_projectable {
            unprojected.insert(binding.name.clone());
            continue;
        }
        let projection = projection.expect("directly projectable binding has CLI metadata");
        let adapter = binding
            .projections
            .provider
            .as_ref()
            .map(|projection| &projection.adapter);
        if parameter_value_is_omitted(value, adapter) {
            continue;
        }
        if let ParameterValue::Bool(enabled) = value {
            let flags = if *enabled {
                &projection.flags
            } else {
                &projection.false_flags
            };
            if let Some(flag) = flags.first() {
                options.push(flag.clone());
            }
            continue;
        }
        let rendered = project_parameter_value(value, binding).map_err(|reason| {
            ProviderInvocationError::Parameter {
                parameter: binding.name.clone(),
                reason,
            }
        })?;
        if let Some(position) = projection.positional {
            positionals.insert(position, rendered);
        } else if let Some(flag) = projection.flags.first() {
            options.push(flag.clone());
            options.push(rendered);
        }
    }
    let mut args = positionals.into_values().collect::<Vec<_>>();
    args.extend(options);
    Ok((args, unprojected))
}

fn evaluate_provider_predicate(predicate: &Predicate, session: &ParameterSession) -> Option<bool> {
    match predicate {
        Predicate::Always => Some(true),
        Predicate::Never => Some(false),
        Predicate::IsSet { parameter } => session
            .states()
            .get(parameter)
            .and_then(|state| state.active.then_some(state.value.is_some())),
        Predicate::Equals { parameter, value } => {
            let state = session.states().get(parameter)?;
            if !state.active {
                return None;
            }
            let current = state.value.as_ref()?;
            let binding = session
                .bundle()
                .surface
                .bindings()
                .iter()
                .find(|binding| &binding.name == parameter)?;
            let concept = session.bundle().catalog.concept(&binding.concept)?;
            semantic_eq(current, value, concept).ok()
        }
        Predicate::Not { predicate } => {
            evaluate_provider_predicate(predicate, session).map(|value| !value)
        }
        Predicate::All { predicates } => evaluate_all(predicates, session),
        Predicate::Any { predicates } => evaluate_any(predicates, session),
    }
}

fn evaluate_all(predicates: &[Predicate], session: &ParameterSession) -> Option<bool> {
    let mut indeterminate = false;
    for predicate in predicates {
        match evaluate_provider_predicate(predicate, session) {
            Some(false) => return Some(false),
            Some(true) => {}
            None => indeterminate = true,
        }
    }
    (!indeterminate).then_some(true)
}

fn evaluate_any(predicates: &[Predicate], session: &ParameterSession) -> Option<bool> {
    let mut indeterminate = false;
    for predicate in predicates {
        match evaluate_provider_predicate(predicate, session) {
            Some(true) => return Some(true),
            Some(false) => {}
            None => indeterminate = true,
        }
    }
    (!indeterminate).then_some(false)
}

fn append_managed_output_arguments(
    args: &mut Vec<String>,
    managed_output: Option<&casa_provider_contracts::ManagedOutputContract>,
) -> Result<(), ProviderInvocationError> {
    let Some(managed_output) = managed_output else {
        return Ok(());
    };
    for ManagedOutputArgument { flag, value } in &managed_output.inject_arguments {
        if flag.is_empty() {
            return Err(ProviderInvocationError::ManagedOutput(
                "an injected argument flag cannot be empty".to_string(),
            ));
        }
        args.push(flag.clone());
        if let Some(value) = value {
            args.push(value.clone());
        }
    }
    Ok(())
}

/// Render one normalized catalog value through its provider adapter.
///
/// This is also used by interactive frontends when their legacy form field
/// needs the exact provider-facing textual representation. Keeping the
/// conversion here prevents each UI from acquiring its own type inference.
pub fn project_parameter_value(
    value: &ParameterValue,
    binding: &casa_provider_contracts::SurfaceParameterBinding,
) -> Result<String, String> {
    let adapter = binding
        .projections
        .provider
        .as_ref()
        .map_or(&ValueAdapter::Identity, |projection| &projection.adapter);
    match adapter {
        ValueAdapter::QuantityNumber => {
            let value = first_scalar(value)?;
            let ParameterValue::String(value) = value else {
                return Err("quantity adapter expects a string".to_string());
            };
            let split = value
                .find(|character: char| {
                    !character.is_ascii_digit()
                        && character != '.'
                        && character != '-'
                        && character != '+'
                        && character != 'e'
                        && character != 'E'
                })
                .unwrap_or(value.len());
            Ok(value[..split].to_string())
        }
        ValueAdapter::ScalarOrPairCsv => {
            let values = match value {
                ParameterValue::Array(values) => values,
                value => return scalar_string(value),
            };
            if binding
                .refinements
                .iter()
                .any(|refinement| matches!(refinement, NarrowingConstraint::SquarePair))
            {
                return values
                    .first()
                    .ok_or_else(|| "pair is empty".to_string())
                    .and_then(scalar_string);
            }
            values
                .iter()
                .map(scalar_string)
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.join(","))
        }
        ValueAdapter::StringListCsv => match value {
            ParameterValue::Array(values) => values
                .iter()
                .map(scalar_string)
                .collect::<Result<Vec<_>, _>>()
                .map(|values| values.join(",")),
            value => scalar_string(value),
        },
        ValueAdapter::Identity | ValueAdapter::OmitNone | ValueAdapter::QuantityString => {
            scalar_or_collection_string(value)
        }
    }
}

fn first_scalar(value: &ParameterValue) -> Result<&ParameterValue, String> {
    match value {
        ParameterValue::Array(values) => values.first().ok_or_else(|| "empty array".to_string()),
        value => Ok(value),
    }
}

fn scalar_string(value: &ParameterValue) -> Result<String, String> {
    match value {
        ParameterValue::Bool(value) => Ok(value.to_string()),
        ParameterValue::Integer(value) => Ok(value.to_string()),
        ParameterValue::Float(value) if value.is_finite() => Ok(value.to_string()),
        ParameterValue::String(value) => Ok(value.clone()),
        _ => Err(format!(
            "cannot project compound value {value:?} as a scalar"
        )),
    }
}

fn scalar_or_collection_string(value: &ParameterValue) -> Result<String, String> {
    match value {
        ParameterValue::Array(values) => values
            .iter()
            .map(scalar_string)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| values.join(",")),
        ParameterValue::Table(_) => serde_json::to_string(value).map_err(|error| error.to_string()),
        value => scalar_string(value),
    }
}

/// Return whether an adapter's explicit absence state should be omitted from
/// the provider invocation.
pub fn parameter_value_is_omitted(value: &ParameterValue, adapter: Option<&ValueAdapter>) -> bool {
    matches!(value, ParameterValue::Array(values) if values.is_empty())
        || (matches!(adapter, Some(ValueAdapter::OmitNone)) && is_explicit_absence(value))
}

fn is_explicit_absence(value: &ParameterValue) -> bool {
    match value {
        ParameterValue::String(state) => state == "none" || state == "auto",
        ParameterValue::Array(values) => {
            !values.is_empty() && values.iter().all(is_explicit_absence)
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use casa_provider_contracts::{
        ParameterValue, ProviderInvocationAdaptation, builtin_surface_bundle,
    };

    use crate::ParameterSession;

    use super::*;

    #[test]
    fn simobserve_run_mode_remains_a_direct_cli_invocation() {
        let bundle = builtin_surface_bundle("simobserve").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        session
            .set("model", ParameterValue::String("model.fits".into()))
            .unwrap();
        session
            .set("out", ParameterValue::String("products/run.ms".into()))
            .unwrap();

        let invocation = project_provider_invocation(&session, |family, values, direct| {
            assert_eq!(family, "simobserve");
            assert_eq!(
                values.get("request_kind"),
                Some(&ParameterValue::String("run".into()))
            );
            Ok(ProviderInvocationAdaptation {
                invocation: direct,
                consumed_parameters: BTreeSet::from(["request_kind".to_string()]),
            })
        })
        .unwrap();
        assert_eq!(invocation.stdin, None);
        assert!(!invocation.args.iter().any(|arg| arg == "--json-run"));
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| pair == ["--model", "model.fits"])
        );
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| pair == ["--out", "products/run.ms"])
        );
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| pair == ["--worker-policy", "auto"])
        );
    }

    #[test]
    fn provider_adapter_must_consume_every_active_parameter_without_cli_spelling() {
        let bundle = builtin_surface_bundle("simobserve").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        session
            .set("request_kind", ParameterValue::String("family".to_string()))
            .unwrap();
        session.render_sparse().expect("complete family draft");
        assert!(!session.states()["model"].active);
        assert!(!session.states()["out"].active);
        assert!(session.states()["source_model"].active);

        let error = project_provider_invocation(&session, |_family, _values, direct| {
            Ok(ProviderInvocationAdaptation::direct(direct))
        })
        .expect_err("family-only fields cannot be silently dropped");
        let ProviderInvocationError::UnprojectedParameters { parameters, .. } = error else {
            panic!("unexpected error: {error}")
        };
        assert!(parameters.contains(&"request_kind".to_string()));
        assert!(parameters.contains(&"source_model".to_string()));
        assert!(parameters.contains(&"output_ms".to_string()));
        assert!(!parameters.contains(&"model".to_string()));
        assert!(!parameters.contains(&"out".to_string()));

        let invocation = project_provider_invocation(&session, |_family, values, _direct| {
            Ok(ProviderInvocationAdaptation {
                invocation: ProviderInvocation {
                    args: vec!["--json-run".into(), "-".into()],
                    stdin: Some("{\"kind\":\"family\"}\n".into()),
                },
                consumed_parameters: values.keys().cloned().collect(),
            })
        })
        .expect("adapter consumed every active family value");
        assert_eq!(invocation.args, ["--json-run", "-"]);
        assert_eq!(invocation.stdin.as_deref(), Some("{\"kind\":\"family\"}\n"));
    }

    #[test]
    fn provider_applicability_uses_the_canonical_mode_predicate_and_fails_closed() {
        let bundle = builtin_surface_bundle("calibrate").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();

        session
            .set("mode", ParameterValue::String("solve_gain".into()))
            .unwrap();
        assert!(provider_parameter_applies(&session, "gain_type").unwrap());
        assert!(!provider_parameter_applies(&session, "fluxscale_input").unwrap());
        assert!(provider_parameter_applies(&session, "vis").unwrap());
        assert!(provider_parameter_applies(&session, "misspelled").is_err());

        session
            .set("mode", ParameterValue::String("fluxscale".into()))
            .unwrap();
        assert!(!provider_parameter_applies(&session, "gain_type").unwrap());
        assert!(provider_parameter_applies(&session, "fluxscale_input").unwrap());
    }

    #[test]
    fn simulator_auto_geometry_is_omitted_but_real_pairs_are_projected() {
        let bundle = builtin_surface_bundle("simanalyze").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        let invocation = project_provider_invocation(&session, |_family, _values, direct| {
            Ok(ProviderInvocationAdaptation::direct(direct))
        })
        .unwrap();
        assert!(
            !invocation
                .args
                .iter()
                .any(|argument| argument == "--imsize")
        );
        assert!(!invocation.args.iter().any(|argument| argument == "--cell"));

        session
            .set(
                "imsize",
                ParameterValue::Array(vec![
                    ParameterValue::Integer(256),
                    ParameterValue::Integer(128),
                ]),
            )
            .unwrap();
        session
            .set(
                "cell",
                ParameterValue::Array(vec![
                    ParameterValue::String("1arcsec".into()),
                    ParameterValue::String("2arcsec".into()),
                ]),
            )
            .unwrap();
        let invocation = project_provider_invocation(&session, |_family, _values, direct| {
            Ok(ProviderInvocationAdaptation::direct(direct))
        })
        .unwrap();
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| pair == ["--imsize", "256,128"])
        );
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| pair == ["--cell", "1arcsec,2arcsec"])
        );
    }

    #[test]
    fn importvla_archivefiles_round_trip_as_a_list_and_project_as_csv() {
        let bundle = builtin_surface_bundle("importvla").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        session
            .set(
                "archivefiles",
                ParameterValue::Array(vec![
                    ParameterValue::String("raw/one.exp".into()),
                    ParameterValue::String("raw/two.xp1".into()),
                ]),
            )
            .unwrap();

        let sparse = session.render_sparse().unwrap();
        assert!(sparse.contains("archivefiles = [\"raw/one.exp\", \"raw/two.xp1\"]"));

        let invocation = project_provider_invocation(&session, |_family, _values, direct| {
            Ok(ProviderInvocationAdaptation::direct(direct))
        })
        .unwrap();
        assert!(
            invocation
                .args
                .windows(2)
                .any(|pair| pair == ["--archivefiles", "raw/one.exp,raw/two.xp1"])
        );
    }
}
