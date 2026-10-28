// SPDX-License-Identifier: LGPL-3.0-or-later

use std::collections::{BTreeMap, BTreeSet};

use casa_provider_contracts::{
    ManagedOutputArgument, ParameterValue, Predicate, ProviderInvocation,
    ProviderInvocationAdaptation, SurfaceContractBundle, TaskRunRequest, ValueAdapter,
};
use thiserror::Error;

use crate::{ParameterSession, semantic_eq};

/// The provider families whose binaries run on the shared task front end
/// (#931). They take one `--json-run -` request; the list grows as the
/// calibration family, the imager and simobserve move (PR 2d) and goes once
/// every binary has.
pub const FRONT_END_FAMILIES: &[&str] = &[
    "image_analysis",
    "msexplore",
    "mstransform",
    "flagdata",
    "flagmanager",
    "importvla",
];

/// Whether `bundle`'s binary runs on the shared task front end.
pub fn runs_on_front_end(bundle: &SurfaceContractBundle) -> bool {
    FRONT_END_FAMILIES.contains(&bundle.surface.provider_family())
}

/// The invocation of a front-end binary: the surface's fixed arguments, then
/// `--json-run -` with a [`TaskRunRequest`] holding the values the caller
/// set, from the base profile and the overrides. The binary resolves them
/// against the same contract; defaults are not repeated.
pub fn project_front_end_invocation(
    session: &ParameterSession,
) -> Result<ProviderInvocation, ProviderInvocationError> {
    let values = session
        .states()
        .iter()
        .filter(|(_, state)| state.explicit && state.active)
        .filter_map(|(name, state)| Some((name.clone(), state.value.as_ref()?.to_plain_json())))
        .collect();
    let request = TaskRunRequest {
        surface: session.bundle().surface.id().to_string(),
        values,
    };
    let stdin =
        serde_json::to_string(&request).map_err(|error| ProviderInvocationError::Parameter {
            parameter: "<request>".to_string(),
            reason: error.to_string(),
        })?;
    let mut args = session.bundle().surface.execution().fixed_args.clone();
    args.extend(["--json-run".to_string(), "-".to_string()]);
    Ok(ProviderInvocation {
        args,
        stdin: Some(stdin),
    })
}

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
    if runs_on_front_end(session.bundle()) {
        return project_front_end_invocation(session);
    }
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
    use casa_provider_contracts::{
        ParameterValue, ProviderInvocationAdaptation, builtin_surface_bundle,
    };

    use crate::ParameterSession;

    use super::*;

    /// The feather surface, as a family whose binary still parses its own
    /// command line, so it is projected to flags rather than `--json-run`.
    fn direct_feather_bundle() -> casa_provider_contracts::SurfaceContractBundle {
        direct_bundle("feather")
    }

    /// The builtin `surface` in a family whose binary takes direct CLI
    /// arguments, as the families not yet on the front end do.
    fn direct_bundle(surface: &str) -> casa_provider_contracts::SurfaceContractBundle {
        let mut bundle = builtin_surface_bundle(surface).unwrap();
        let casa_provider_contracts::SurfaceDefinition::Task(definition) = &mut bundle.surface;
        definition.provider_family = "direct_cli_fixture".to_string();
        bundle
    }

    /// The feather surface, its three images set.
    fn feather_session(bundle: casa_provider_contracts::SurfaceContractBundle) -> ParameterSession {
        let mut session = ParameterSession::defaults(bundle).unwrap();
        for (name, path) in [
            ("imagename", "products/feathered.image"),
            ("highres", "high.image"),
            ("lowres", "low.image"),
        ] {
            session
                .set(name, ParameterValue::String(path.into()))
                .unwrap();
        }
        session
    }

    #[test]
    fn a_front_end_surface_is_one_json_run_of_the_values_set() {
        let session = feather_session(builtin_surface_bundle("feather").unwrap());
        let invocation = project_provider_invocation(&session, |_family, _values, _direct| {
            panic!("a front-end surface takes no family adapter")
        })
        .unwrap();
        assert_eq!(invocation.args, ["--json-run", "-"]);
        let request: TaskRunRequest =
            serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
        assert_eq!(request.surface, "feather");
        // The values set, and no defaults such as sdfactor or overwrite.
        assert_eq!(
            request.values.keys().collect::<Vec<_>>(),
            ["highres", "imagename", "lowres"]
        );

        let mut imstat =
            ParameterSession::defaults(builtin_surface_bundle("imstat").unwrap()).unwrap();
        imstat
            .set("imagename", ParameterValue::String("in.image".into()))
            .unwrap();
        let invocation = project_front_end_invocation(&imstat).unwrap();
        assert_eq!(invocation.args, ["imstat", "--json-run", "-"]);
    }

    #[test]
    fn a_surface_without_an_adapter_is_a_direct_cli_invocation() {
        let session = feather_session(direct_feather_bundle());
        let invocation = project_provider_invocation(&session, |family, _values, direct| {
            assert_eq!(family, "direct_cli_fixture");
            Ok(ProviderInvocationAdaptation::direct(direct))
        })
        .unwrap();
        assert_eq!(invocation.stdin, None);
        assert!(!invocation.args.iter().any(|arg| arg == "--json-run"));
        for pair in [
            ["--imagename", "products/feathered.image"],
            ["--highres", "high.image"],
            ["--lowres", "low.image"],
        ] {
            assert!(
                invocation.args.windows(2).any(|window| window == pair),
                "{pair:?}: {:?}",
                invocation.args
            );
        }
    }

    #[test]
    fn vlass_awproject_profile_projects_one_complete_direct_cli_request() {
        let bundle = builtin_surface_bundle("imager").unwrap();
        let mut session = ParameterSession::defaults(bundle).unwrap();
        for (name, value) in [
            ("vis", ParameterValue::String("vlass.ms".into())),
            ("imagename", ParameterValue::String("products/vlass".into())),
            ("field", ParameterValue::String("1525".into())),
            ("spw", ParameterValue::String("9".into())),
            ("uvrange", ParameterValue::String("<12km".into())),
            ("intent", ParameterValue::String("*TARGET*".into())),
            ("stokes", ParameterValue::String("I".into())),
            ("gridder", ParameterValue::String("awproject".into())),
            ("cfcache", ParameterValue::String("/cache/vlass".into())),
            ("deconvolver", ParameterValue::String("mtmfs".into())),
            ("scales", ParameterValue::String("0,5,12".into())),
            ("normtype", ParameterValue::String("flatnoise".into())),
            ("parallel", ParameterValue::Bool(false)),
            ("usepointing", ParameterValue::Bool(true)),
            ("write_pb", ParameterValue::Bool(true)),
        ] {
            session
                .set(name, value)
                .unwrap_or_else(|error| panic!("set VLASS profile parameter {name}: {error}"));
        }
        for (name, value) in [
            ("nterms", 2_i64),
            ("wprojplanes", 32),
            ("cf_resident_mb", 256),
        ] {
            session
                .set(name, ParameterValue::Integer(value))
                .unwrap_or_else(|error| panic!("set VLASS profile parameter {name}: {error}"));
        }
        session
            .set("pblimit", ParameterValue::Float(0.0001))
            .unwrap();

        let invocation = project_provider_invocation(&session, |_family, _values, direct| {
            Ok(ProviderInvocationAdaptation::direct(direct))
        })
        .unwrap();
        let has_pair =
            |flag: &str, value: &str| invocation.args.windows(2).any(|pair| pair == [flag, value]);
        for (flag, value) in [
            ("--ms", "vlass.ms"),
            ("--imagename", "products/vlass"),
            ("--field", "1525"),
            ("--spw", "9"),
            ("--uvrange", "<12km"),
            ("--intent", "*TARGET*"),
            ("--stokes", "I"),
            ("--gridder", "awproject"),
            ("--cfcache", "/cache/vlass"),
            ("--cf-resident-mb", "256"),
            ("--wprojplanes", "32"),
            ("--deconvolver", "mtmfs"),
            ("--nterms", "2"),
            ("--scales", "0,5,12"),
            ("--normtype", "flatnoise"),
            ("--pblimit", "0.0001"),
        ] {
            assert!(
                has_pair(flag, value),
                "missing {flag} {value:?}: {:?}",
                invocation.args
            );
        }
        for flag in ["--usepointing", "--write-pb", "--no-parallel"] {
            assert!(
                invocation.args.iter().any(|argument| argument == flag),
                "missing {flag}: {:?}",
                invocation.args
            );
        }
    }

    #[test]
    fn provider_adapter_must_consume_every_active_parameter_without_cli_spelling() {
        // A fixture: feather with `sdfactor` given no command-line spelling.
        let mut bundle = direct_feather_bundle();
        let casa_provider_contracts::SurfaceDefinition::Task(definition) = &mut bundle.surface;
        let sdfactor = definition
            .bindings
            .iter_mut()
            .find(|binding| binding.name == "sdfactor")
            .unwrap();
        let cli = sdfactor.projections.cli.as_mut().unwrap();
        cli.flags.clear();
        cli.false_flags.clear();
        let session = feather_session(bundle);

        let error = project_provider_invocation(&session, |_family, _values, direct| {
            Ok(ProviderInvocationAdaptation::direct(direct))
        })
        .expect_err("a value without a spelling cannot be silently dropped");
        let ProviderInvocationError::UnprojectedParameters { parameters, .. } = error else {
            panic!("unexpected error: {error}")
        };
        assert_eq!(parameters, ["sdfactor"]);

        let invocation = project_provider_invocation(&session, |_family, values, _direct| {
            Ok(ProviderInvocationAdaptation {
                invocation: ProviderInvocation {
                    args: vec!["--json-run".into(), "-".into()],
                    stdin: Some("{\"kind\":\"run\"}\n".into()),
                },
                consumed_parameters: values.keys().cloned().collect(),
            })
        })
        .expect("adapter consumed every active value");
        assert_eq!(invocation.args, ["--json-run", "-"]);
        assert_eq!(invocation.stdin.as_deref(), Some("{\"kind\":\"run\"}\n"));
    }

    #[test]
    fn auto_and_none_sentinels_are_omitted_but_real_values_are_projected() {
        let omit_none = Some(&ValueAdapter::OmitNone);
        for sentinel in ["auto", "none"] {
            let value = ParameterValue::String(sentinel.into());
            assert!(parameter_value_is_omitted(&value, omit_none), "{sentinel}");
            assert!(!parameter_value_is_omitted(
                &value,
                Some(&ValueAdapter::Identity)
            ));
        }
        assert!(!parameter_value_is_omitted(
            &ParameterValue::String("spectral".into()),
            omit_none
        ));
    }

    #[test]
    fn real_geometry_pairs_are_projected_as_one_collection_argument() {
        let pair = ParameterValue::Array(vec![
            ParameterValue::Integer(256),
            ParameterValue::Integer(128),
        ]);
        assert!(!parameter_value_is_omitted(
            &pair,
            Some(&ValueAdapter::OmitNone)
        ));
        assert_eq!(scalar_or_collection_string(&pair).unwrap(), "256,128");
        let cell = ParameterValue::Array(vec![
            ParameterValue::String("1arcsec".into()),
            ParameterValue::String("2arcsec".into()),
        ]);
        assert_eq!(
            scalar_or_collection_string(&cell).unwrap(),
            "1arcsec,2arcsec"
        );
    }

    #[test]
    fn omit_none_drops_scalar_and_collection_absence_sentinels_only() {
        let omit = Some(&ValueAdapter::OmitNone);
        let auto = || ParameterValue::String("auto".into());
        assert!(parameter_value_is_omitted(&auto(), omit));
        assert!(parameter_value_is_omitted(
            &ParameterValue::String("none".into()),
            omit
        ));
        assert!(parameter_value_is_omitted(
            &ParameterValue::Array(vec![auto(), auto()]),
            omit
        ));
        assert!(parameter_value_is_omitted(
            &ParameterValue::Array(Vec::new()),
            None
        ));
        assert!(!parameter_value_is_omitted(&auto(), None));
        assert!(!parameter_value_is_omitted(
            &ParameterValue::Array(vec![auto(), ParameterValue::String("1arcsec".into())]),
            omit
        ));
    }

    #[test]
    fn importvla_archivefiles_round_trip_as_a_list_and_project_as_csv() {
        let archivefiles = ParameterValue::Array(vec![
            ParameterValue::String("raw/one.exp".into()),
            ParameterValue::String("raw/two.xp1".into()),
        ]);
        let mut session =
            ParameterSession::defaults(builtin_surface_bundle("importvla").unwrap()).unwrap();
        session.set("archivefiles", archivefiles.clone()).unwrap();

        let sparse = session.render_sparse().unwrap();
        assert!(sparse.contains("archivefiles = [\"raw/one.exp\", \"raw/two.xp1\"]"));

        // importvla's binary takes the list itself in one request.
        let request: TaskRunRequest = serde_json::from_str(
            project_front_end_invocation(&session)
                .unwrap()
                .stdin
                .as_deref()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            request.values["archivefiles"],
            serde_json::json!(["raw/one.exp", "raw/two.xp1"])
        );

        // A direct command line carries a list as CSV.
        let mut direct = ParameterSession::defaults(direct_bundle("importvla")).unwrap();
        direct.set("archivefiles", archivefiles).unwrap();
        let invocation = project_provider_invocation(&direct, |_family, _values, direct| {
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
