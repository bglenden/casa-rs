// SPDX-License-Identifier: LGPL-3.0-or-later
//! Headless parameter/profile commands layered over the shared task runtime.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use casa_notebook::ExecutionStatus;
use casa_provider_contracts::{
    ParameterConcept, ParameterType, ParameterValue, ProviderInvocation,
    ProviderInvocationAdaptation, SurfaceContractBundle, SurfaceKind, builtin_surface_bundle,
    builtin_surface_catalog,
};
use casa_task_runtime::{
    BaseSource, ManagedProfileKind, ManagedStateStore, ParameterProfile, ParameterSession,
    ResolutionPatch, TaskLastState, parameter_value_is_omitted, parse_profile,
    project_parameter_value, project_provider_invocation, render_documented_template,
    resolve_profile, write_parameter_profile_atomic,
};

use crate::execution::{ExecutionPlan, run_process_blocking};
use crate::notebook_recording::NotebookRecording;
use crate::registry::resolve_app;
use crate::startup::{StartupLaunch, StartupPrefill, StartupValue};

/// Result handled before the full-screen launcher starts.
pub(crate) enum ParameterCliDispatch {
    Print(String),
    Done,
    Launch(Box<StartupLaunch>),
}

/// Handle `params`, `run`, and `open`; return `None` for legacy launcher syntax.
pub(crate) fn dispatch(args: &[OsString]) -> Result<Option<ParameterCliDispatch>, String> {
    let Some(first) = args.first().and_then(|value| value.to_str()) else {
        return Ok(None);
    };
    match first {
        "params" => run_params(&args[1..]).map(Some),
        "run" => run_task(&args[1..]).map(|_| Some(ParameterCliDispatch::Done)),
        "open" => open_session(&args[1..])
            .map(|launch| Some(ParameterCliDispatch::Launch(Box::new(launch)))),
        _ => Ok(None),
    }
}

fn run_params(args: &[OsString]) -> Result<ParameterCliDispatch, String> {
    let Some(command) = args.first().and_then(|value| value.to_str()) else {
        return Ok(ParameterCliDispatch::Print(params_help()));
    };
    match command {
        "validate" => {
            let path = required_path(args.get(1), "profile file")?;
            reject_trailing(args, 2, "params validate")?;
            let source = fs::read_to_string(&path)
                .map_err(|error| format!("read parameter profile {}: {error}", path.display()))?;
            let profile = parse_profile(&source).map_err(format_profile_error)?;
            let bundle = builtin_surface_bundle(&profile.header.surface)?;
            let resolved = resolve_profile(&profile, &bundle).map_err(format_profile_error)?;
            let mut text = format!(
                "valid {} {} profile (contract {}, {} explicit override{})\n",
                profile.header.kind,
                profile.header.surface,
                profile.header.contract,
                resolved.explicit_overrides.len(),
                if resolved.explicit_overrides.len() == 1 {
                    ""
                } else {
                    "s"
                }
            );
            for diagnostic in resolved.diagnostics {
                text.push_str(&format!("warning: {}\n", diagnostic.message));
            }
            Ok(ParameterCliDispatch::Print(text))
        }
        "template" => {
            let surface = required_utf8(args.get(1), "surface")?;
            reject_trailing(args, 2, "params template")?;
            let bundle = builtin_surface_bundle(&surface)?;
            Ok(ParameterCliDispatch::Print(
                render_documented_template(&bundle).map_err(format_profile_error)?,
            ))
        }
        "describe" => {
            let name = required_utf8(args.get(1), "parameter or surface name")?;
            reject_trailing(args, 2, "params describe")?;
            Ok(ParameterCliDispatch::Print(describe(&name)?))
        }
        "show" => {
            let surface = required_utf8(args.get(1), "surface")?;
            let bundle = builtin_surface_bundle(&surface)?;
            let options = parse_surface_options(&bundle, &args[2..], false)?;
            let session = load_parameter_session(bundle, &options)?;
            Ok(ParameterCliDispatch::Print(show_session(&session)))
        }
        "save" => {
            let surface = required_utf8(args.get(1), "surface")?;
            let destination = required_path(args.get(2), "destination profile")?;
            let bundle = builtin_surface_bundle(&surface)?;
            let options = parse_surface_options(&bundle, &args[3..], false)?;
            let session = load_parameter_session(bundle, &options)?;
            save_explicit(
                &destination,
                &session.render_sparse().map_err(format_session_error)?,
            )?;
            Ok(ParameterCliDispatch::Print(format!(
                "saved {}\n",
                destination.display()
            )))
        }
        "-h" | "--help" | "help" => Ok(ParameterCliDispatch::Print(params_help())),
        other => Err(format!(
            "unknown params command {other:?}; expected validate, show, save, template, or describe"
        )),
    }
}

fn run_task(args: &[OsString]) -> Result<(), String> {
    let surface_id = required_utf8(args.first(), "task name")?;
    let bundle = builtin_surface_bundle(&surface_id)?;
    if bundle.surface.kind() != SurfaceKind::Task {
        return Err(format!("{surface_id:?} is a session; use `casars open`"));
    }
    let options = parse_surface_options(&bundle, &args[1..], true)?;
    let session = load_parameter_session(bundle, &options)?;
    enforce_runtime_confirmations(&session, &options)?;
    let invocation = project_task_invocation(&session)?;

    if let Some(path) = &options.save_params {
        save_explicit(
            path,
            &session.render_sparse().map_err(format_session_error)?,
        )?;
    }

    let store = ManagedStateStore::for_workspace(&options.workspace);
    let mut last = TaskLastState::new(store, &surface_id, !options.no_save_last);
    let attempted = last
        .before_execution(&session)
        .map_err(format_session_error)?;
    if let Some(warning) = attempted.warning {
        eprintln!("Warning: could not save Last for {surface_id}: {warning}");
    }

    let mut recording = NotebookRecording::begin(
        options.workspace.clone(),
        &options.initiating_surface,
        &surface_id,
        &session,
        options.notebook.as_deref(),
        options.no_notebook_recording,
        options.confirm_overwrite || options.confirm_mutation,
    );
    if let Some(warning) = recording.take_warning() {
        eprintln!("Warning: notebook recording unavailable for {surface_id}: {warning}");
    }

    let execution = (|| {
        let app = resolve_app(Some(&surface_id))?;
        let plan = ExecutionPlan {
            command: app.resolve_command()?,
            arguments: invocation.args.iter().map(OsString::from).collect(),
            stdin: invocation.stdin,
            working_directory: options.workspace.clone(),
            renderer: None,
            file_output_path: None,
        };
        run_process_blocking(&plan).map_err(|error| format!("run {surface_id}: {error}"))
    })();
    let execution = match execution {
        Ok(execution) => execution,
        Err(error) => {
            if let Some(warning) = recording.finalize(
                ExecutionStatus::Failed,
                String::new(),
                String::new(),
                Vec::new(),
                vec![error.clone()],
            ) {
                eprintln!("Warning: notebook receipt finalization failed: {warning}");
            }
            return Err(error);
        }
    };
    let successful = execution.exit.success;
    if let Some(warning) = recording.finalize(
        if successful {
            ExecutionStatus::Succeeded
        } else {
            ExecutionStatus::Failed
        },
        execution.stdout,
        execution.stderr,
        Vec::new(),
        Vec::new(),
    ) {
        eprintln!("Warning: notebook receipt finalization failed: {warning}");
    }
    let completed = last.after_completion(successful);
    if let Some(warning) = completed.warning {
        eprintln!("Warning: could not save Last Successful for {surface_id}: {warning}");
    }
    if successful {
        Ok(())
    } else {
        Err(format!(
            "{surface_id} exited with code {}",
            execution
                .exit
                .code
                .map_or_else(|| "unknown".to_owned(), |code| code.to_string())
        ))
    }
}

fn open_session(args: &[OsString]) -> Result<StartupLaunch, String> {
    let surface_id = required_utf8(args.first(), "session name")?;
    let bundle = builtin_surface_bundle(&surface_id)?;
    if bundle.surface.kind() != SurfaceKind::Session {
        return Err(format!("{surface_id:?} is a task; use `casars run`"));
    }
    let options = parse_surface_options(&bundle, &args[1..], false)?;
    let session = load_parameter_session(bundle.clone(), &options)?;
    if let Some(path) = &options.save_params {
        save_explicit(
            path,
            &session.render_sparse().map_err(format_session_error)?,
        )?;
    }
    let prefill = session
        .states()
        .iter()
        .filter_map(|(name, state)| {
            let binding = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| &binding.name == name)?;
            let value = state.value.as_ref()?;
            if parameter_value_is_omitted(
                value,
                binding.projections.provider.as_ref().map(|p| &p.adapter),
            ) {
                return None;
            }
            let value = match value {
                ParameterValue::Bool(value) => StartupValue::Toggle(*value),
                _ => StartupValue::Text(project_parameter_value(value, binding).ok()?),
            };
            Some(StartupPrefill {
                id: name.clone(),
                value,
            })
        })
        .collect();
    Ok(StartupLaunch {
        app: resolve_app(Some(&surface_id))?,
        prefill,
        auto_run: true,
        workspace: options.workspace,
        save_last: !options.no_save_last,
        parameter_session: Some(session),
    })
}

#[derive(Debug, Clone)]
enum SourceChoice {
    Defaults,
    Last,
    LastSuccessful,
    File(PathBuf),
}

#[derive(Debug, Clone)]
struct SurfaceOptions {
    source: SourceChoice,
    workspace: PathBuf,
    notebook: Option<String>,
    initiating_surface: String,
    context: ResolutionPatch,
    overrides: ResolutionPatch,
    save_params: Option<PathBuf>,
    no_save_last: bool,
    no_notebook_recording: bool,
    confirm_overwrite: bool,
    confirm_mutation: bool,
}

fn parse_surface_options(
    bundle: &SurfaceContractBundle,
    args: &[OsString],
    allow_runtime_controls: bool,
) -> Result<SurfaceOptions, String> {
    let mut source = None;
    let mut workspace = None;
    let mut notebook = None;
    let mut initiating_surface = "cli".to_string();
    let mut overrides = ResolutionPatch::default();
    let context = ResolutionPatch::default();
    let mut save_params = None;
    let mut no_save_last = false;
    let mut no_notebook_recording = false;
    let mut confirm_overwrite = false;
    let mut confirm_mutation = false;
    let positional = bundle
        .surface
        .bindings()
        .iter()
        .filter_map(|binding| {
            binding
                .projections
                .cli
                .as_ref()
                .and_then(|projection| projection.positional)
                .map(|position| (position, binding.name.as_str()))
        })
        .collect::<BTreeMap<_, _>>();
    let mut positional_index = 0usize;
    let mut index = 0usize;
    while index < args.len() {
        let raw = args[index]
            .to_str()
            .ok_or_else(|| "parameter arguments must be valid UTF-8".to_string())?;
        if raw == "--defaults" {
            set_source(&mut source, SourceChoice::Defaults)?;
        } else if raw == "--last" {
            set_source(&mut source, SourceChoice::Last)?;
        } else if raw == "--last-successful" {
            set_source(&mut source, SourceChoice::LastSuccessful)?;
        } else if raw == "--params" {
            index += 1;
            set_source(
                &mut source,
                SourceChoice::File(required_path(args.get(index), "--params file")?),
            )?;
        } else if raw == "--workspace" {
            index += 1;
            workspace = Some(required_path(args.get(index), "--workspace directory")?);
        } else if raw == "--notebook" && allow_runtime_controls {
            index += 1;
            notebook = Some(required_utf8(args.get(index), "--notebook filename or ID")?);
        } else if raw == "--initiating-surface" && allow_runtime_controls {
            index += 1;
            initiating_surface = required_utf8(args.get(index), "--initiating-surface value")?;
            if !matches!(initiating_surface.as_str(), "cli" | "python") {
                return Err("--initiating-surface must be cli or python".to_string());
            }
        } else if raw == "--unset" {
            index += 1;
            let name = required_utf8(args.get(index), "--unset parameter")?;
            ensure_binding(bundle, &name)?;
            overrides.values.remove(&name);
            overrides.unset.insert(name);
        } else if raw == "--save-params" {
            index += 1;
            save_params = Some(required_path(args.get(index), "--save-params file")?);
        } else if raw == "--no-save-last" {
            no_save_last = true;
        } else if raw == "--no-notebook-recording" && allow_runtime_controls {
            no_notebook_recording = true;
        } else if raw == "--confirm-overwrite" && allow_runtime_controls {
            confirm_overwrite = true;
        } else if raw == "--confirm-mutation" && allow_runtime_controls {
            confirm_mutation = true;
        } else if let Some(stripped) = raw.strip_prefix("--") {
            let (flag, inline) = stripped
                .split_once('=')
                .map_or((raw.trim_start_matches("--"), None), |(flag, value)| {
                    (flag, Some(value))
                });
            let (negated, flag) = flag
                .strip_prefix("no-")
                .map_or((false, flag), |flag| (true, flag));
            let binding = find_binding_by_flag(bundle, flag).ok_or_else(|| {
                format!("unknown {} parameter flag --{flag}", bundle.surface.id())
            })?;
            let concept = bundle
                .catalog
                .concept(&binding.concept)
                .ok_or_else(|| format!("missing concept for {}", binding.name))?;
            let value = if is_bool_domain(&concept.value_domain) {
                if inline.is_some() {
                    return Err(format!("boolean --{flag} does not take a value"));
                }
                ParameterValue::Bool(!negated)
            } else {
                if negated {
                    return Err(format!("--no-{flag} is valid only for booleans"));
                }
                let value = match inline {
                    Some(value) => value.to_string(),
                    None => {
                        index += 1;
                        required_utf8(args.get(index), &format!("--{flag} value"))?
                    }
                };
                parse_cli_value(&value, &concept.value_domain)?
            };
            overrides.unset.remove(&binding.name);
            overrides.values.insert(binding.name.clone(), value);
        } else {
            let Some(name) = positional.get(&positional_index) else {
                return Err(format!("unexpected positional parameter {raw:?}"));
            };
            let binding = ensure_binding(bundle, name)?;
            let concept = bundle.catalog.concept(&binding.concept).ok_or_else(|| {
                format!("missing concept for positional parameter {}", binding.name)
            })?;
            overrides.values.insert(
                binding.name.clone(),
                parse_cli_value(raw, &concept.value_domain)?,
            );
            positional_index += 1;
        }
        index += 1;
    }
    let workspace =
        workspace.unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
    Ok(SurfaceOptions {
        source: source.unwrap_or(SourceChoice::Defaults),
        workspace,
        notebook,
        initiating_surface,
        context,
        overrides,
        save_params,
        no_save_last,
        no_notebook_recording,
        confirm_overwrite,
        confirm_mutation,
    })
}

fn set_source(target: &mut Option<SourceChoice>, source: SourceChoice) -> Result<(), String> {
    if target.is_some() {
        return Err(
            "base sources are mutually exclusive: choose exactly one of Defaults, Last, Last Successful, or --params"
                .to_string(),
        );
    }
    *target = Some(source);
    Ok(())
}

fn load_parameter_session(
    bundle: SurfaceContractBundle,
    options: &SurfaceOptions,
) -> Result<ParameterSession, String> {
    let store = ManagedStateStore::for_workspace(&options.workspace);
    let mut session = match &options.source {
        SourceChoice::Defaults => {
            ParameterSession::defaults(bundle).map_err(format_session_error)?
        }
        SourceChoice::Last => {
            let profile =
                read_managed_profile(&store, bundle.surface.id(), ManagedProfileKind::Last)?;
            ParameterSession::from_profile(bundle, BaseSource::Last, &profile)
                .map_err(format_session_error)?
        }
        SourceChoice::LastSuccessful => {
            if bundle.surface.kind() != SurfaceKind::Task {
                return Err("Last Successful exists only for task surfaces".to_string());
            }
            let profile = read_managed_profile(
                &store,
                bundle.surface.id(),
                ManagedProfileKind::LastSuccessful,
            )?;
            ParameterSession::from_profile(bundle, BaseSource::LastSuccessful, &profile)
                .map_err(format_session_error)?
        }
        SourceChoice::File(path) => {
            let text = fs::read_to_string(path)
                .map_err(|error| format!("read parameter profile {}: {error}", path.display()))?;
            let profile = parse_profile(&text).map_err(format_profile_error)?;
            ParameterSession::from_profile(bundle, BaseSource::File(path.clone()), &profile)
                .map_err(format_session_error)?
        }
    };
    if !options.context.values.is_empty() || !options.context.unset.is_empty() {
        session
            .apply_context_patch(options.context.clone())
            .map_err(format_session_error)?;
    }
    if !options.overrides.values.is_empty() || !options.overrides.unset.is_empty() {
        session
            .apply_override_patch(options.overrides.clone())
            .map_err(format_session_error)?;
    }
    Ok(session)
}

fn read_managed_profile(
    store: &ManagedStateStore,
    surface: &str,
    kind: ManagedProfileKind,
) -> Result<ParameterProfile, String> {
    let text = store
        .read(surface, kind)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!(
                "no managed {} profile exists for {surface}",
                match kind {
                    ManagedProfileKind::Last => "Last",
                    ManagedProfileKind::LastSuccessful => "Last Successful",
                }
            )
        })?;
    parse_profile(&text).map_err(format_profile_error)
}

pub(crate) fn project_task_invocation(
    session: &ParameterSession,
) -> Result<ProviderInvocation, String> {
    project_provider_invocation(session, |family, values, direct| match family {
        "simobserve" => {
            casa_ms::simulation_task::simobserve_provider_invocation(values, direct.args)
        }
        _ => Ok(ProviderInvocationAdaptation::direct(direct)),
    })
    .map_err(|error| error.to_string())
}

fn enforce_runtime_confirmations(
    session: &ParameterSession,
    options: &SurfaceOptions,
) -> Result<(), String> {
    let requirements = session
        .required_run_safety()
        .map_err(|error| format!("evaluate resolved run safety: {error}"))?;
    if requirements.requires_overwrite_confirmation() && !options.confirm_overwrite {
        return Err(
            "resolved profile requests overwrite; pass the runtime control --confirm-overwrite to authorize it"
                .to_string(),
        );
    }
    if requirements.requires_input_mutation_confirmation() && !options.confirm_mutation {
        return Err(
            "resolved profile requests a mutation; pass the runtime control --confirm-mutation to authorize it"
                .to_string(),
        );
    }
    Ok(())
}

pub(crate) fn parse_cli_value(
    value: &str,
    domain: &ParameterType,
) -> Result<ParameterValue, String> {
    match domain {
        ParameterType::Bool => match value {
            "true" => Ok(ParameterValue::Bool(true)),
            "false" => Ok(ParameterValue::Bool(false)),
            _ => Err(format!("expected true or false, got {value:?}")),
        },
        ParameterType::Integer => value
            .parse::<i64>()
            .map(ParameterValue::Integer)
            .map_err(|error| format!("parse integer {value:?}: {error}")),
        ParameterType::Float => value
            .parse::<f64>()
            .map(ParameterValue::Float)
            .map_err(|error| format!("parse number {value:?}: {error}")),
        ParameterType::String
        | ParameterType::Path { .. }
        | ParameterType::Choice { .. }
        | ParameterType::Quantity { .. } => Ok(ParameterValue::String(value.to_string())),
        ParameterType::Array { element, .. } => {
            if value.starts_with('[') {
                let parsed = format!("value = {value}")
                    .parse::<toml::Value>()
                    .map_err(|error| format!("parse array {value:?}: {error}"))?;
                let values = parsed
                    .get("value")
                    .and_then(toml::Value::as_array)
                    .ok_or_else(|| format!("expected TOML array, got {value:?}"))?;
                return values
                    .iter()
                    .map(|value| parse_toml_cli_value(value, element))
                    .collect::<Result<Vec<_>, _>>()
                    .map(ParameterValue::Array);
            }
            if value.contains(',') {
                return value
                    .split(',')
                    .map(|value| parse_cli_value(value.trim(), element))
                    .collect::<Result<Vec<_>, _>>()
                    .map(ParameterValue::Array);
            }
            parse_cli_value(value, element)
        }
        ParameterType::Table { .. } => {
            let parsed = format!("value = {value}")
                .parse::<toml::Value>()
                .map_err(|error| format!("parse table {value:?}: {error}"))?;
            let value = parsed
                .get("value")
                .ok_or_else(|| "missing parsed table value".to_string())?;
            parse_toml_cli_value(value, domain)
        }
        ParameterType::Optional {
            value: inner,
            states,
        } => {
            if states.iter().any(|state| state == value) {
                Ok(ParameterValue::String(value.to_string()))
            } else {
                parse_cli_value(value, inner)
            }
        }
    }
}

fn parse_toml_cli_value(
    value: &toml::Value,
    domain: &ParameterType,
) -> Result<ParameterValue, String> {
    match value {
        toml::Value::String(value) => parse_cli_value(value, domain),
        toml::Value::Integer(value) => Ok(ParameterValue::Integer(*value)),
        toml::Value::Float(value) if value.is_finite() => Ok(ParameterValue::Float(*value)),
        toml::Value::Boolean(value) => Ok(ParameterValue::Bool(*value)),
        toml::Value::Array(values) => values
            .iter()
            .map(|value| parse_toml_cli_value(value, domain))
            .collect::<Result<Vec<_>, _>>()
            .map(ParameterValue::Array),
        toml::Value::Table(values) => values
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    parse_toml_cli_value(value, &ParameterType::String)?,
                ))
            })
            .collect::<Result<BTreeMap<_, _>, String>>()
            .map(ParameterValue::Table),
        _ => Err("TOML datetime and non-finite values are not parameters".to_string()),
    }
}

fn is_bool_domain(domain: &ParameterType) -> bool {
    match domain {
        ParameterType::Bool => true,
        ParameterType::Optional { value, .. } => is_bool_domain(value),
        _ => false,
    }
}

fn find_binding_by_flag<'a>(
    bundle: &'a SurfaceContractBundle,
    flag: &str,
) -> Option<&'a casa_provider_contracts::SurfaceParameterBinding> {
    let normalized = flag.replace('-', "_");
    bundle.surface.bindings().iter().find(|binding| {
        binding.name == flag
            || binding.name == normalized
            || binding
                .projections
                .python
                .as_ref()
                .is_some_and(|projection| projection.name == flag || projection.name == normalized)
    })
}

fn ensure_binding<'a>(
    bundle: &'a SurfaceContractBundle,
    name: &str,
) -> Result<&'a casa_provider_contracts::SurfaceParameterBinding, String> {
    bundle
        .surface
        .bindings()
        .iter()
        .find(|binding| binding.name == name)
        .ok_or_else(|| format!("unknown {} parameter {name:?}", bundle.surface.id()))
}

fn show_session(session: &ParameterSession) -> String {
    let mut bindings = session
        .bundle()
        .surface
        .bindings()
        .iter()
        .collect::<Vec<_>>();
    bindings.sort_by_key(|binding| binding.order);
    let mut text = format!(
        "{} {} (contract {}, source {:?})\n",
        session.bundle().surface.kind(),
        session.bundle().surface.id(),
        session.bundle().surface.contract_version(),
        session.base_source()
    );
    for binding in bindings {
        let state = &session.states()[&binding.name];
        let value = state
            .value
            .as_ref()
            .map(display_value)
            .unwrap_or_else(|| "<required>".to_string());
        text.push_str(&format!(
            "{:<24} = {:<28} # {:?}{}{}\n",
            binding.name,
            value,
            state.origin,
            if state.required { ", required" } else { "" },
            if state.active { "" } else { ", inactive" }
        ));
    }
    for diagnostic in session.diagnostics() {
        text.push_str(&format!("warning: {}\n", diagnostic.message));
    }
    text
}

fn display_value(value: &ParameterValue) -> String {
    match value {
        ParameterValue::Bool(value) => value.to_string(),
        ParameterValue::Integer(value) => value.to_string(),
        ParameterValue::Float(value) => value.to_string(),
        ParameterValue::String(value) => format!("{value:?}"),
        ParameterValue::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(display_value)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        ParameterValue::Table(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(name, value)| format!("{name}={}", display_value(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

fn describe(name: &str) -> Result<String, String> {
    let aggregate = builtin_surface_catalog()?;
    if let Some(surface) = aggregate.surface(name) {
        let mut text = format!(
            "{} {} — {}\nprovider family: {}\ncontract: {}\nparameters: {}\n\n",
            surface.kind(),
            surface.id(),
            surface.summary(),
            surface.provider_family(),
            surface.contract_version(),
            surface.bindings().len()
        );
        for binding in surface.bindings() {
            text.push_str(&format!(
                "  {:<24} {}@r{}\n",
                binding.name, binding.concept.id, binding.concept.semantic_revision.0
            ));
        }
        return Ok(text);
    }
    let concepts = aggregate
        .catalog
        .concepts
        .iter()
        .filter(|concept| concept.id.as_str() == name || concept.casa_name == name)
        .collect::<Vec<_>>();
    if concepts.is_empty() {
        return Err(format!("unknown parameter concept or surface {name:?}"));
    }
    let mut text = String::new();
    for concept in concepts {
        describe_concept(&mut text, concept, aggregate);
    }
    Ok(text)
}

fn describe_concept(
    text: &mut String,
    concept: &ParameterConcept,
    aggregate: &casa_provider_contracts::SurfaceCatalogBundle,
) {
    text.push_str(&format!(
        "{}@r{} ({})\nCASA name: {}\ntype: {:?}\nnormalization: {:?}\nrole: {:?}\npersistence: {:?}\n{}\nused by:",
        concept.id,
        concept.semantic_revision.0,
        concept.documentation.summary,
        concept.casa_name,
        concept.value_domain,
        concept.normalization,
        concept.semantic_role,
        concept.persistence_class,
        concept.documentation.details.as_deref().unwrap_or("")
    ));
    for surface in &aggregate.surfaces {
        if surface
            .bindings()
            .iter()
            .any(|binding| binding.concept == concept.reference())
        {
            text.push_str(&format!(" {}", surface.id()));
        }
    }
    text.push_str("\n\n");
}

fn save_explicit(path: &Path, contents: &str) -> Result<(), String> {
    write_parameter_profile_atomic(path, contents)
        .map(|_| ())
        .map_err(|error| format!("save {}: {error}", path.display()))
}

fn required_utf8(value: Option<&OsString>, label: &str) -> Result<String, String> {
    value
        .and_then(|value| value.to_str())
        .map(str::to_string)
        .ok_or_else(|| format!("missing or non-UTF-8 {label}"))
}

fn required_path(value: Option<&OsString>, label: &str) -> Result<PathBuf, String> {
    value
        .map(PathBuf::from)
        .ok_or_else(|| format!("missing {label}"))
}

fn reject_trailing(args: &[OsString], used: usize, command: &str) -> Result<(), String> {
    if args.len() == used {
        Ok(())
    } else {
        Err(format!("unexpected arguments after `{command}`"))
    }
}

fn format_profile_error(error: casa_task_runtime::ProfileError) -> String {
    match error {
        casa_task_runtime::ProfileError::Diagnostics(diagnostics) => diagnostics
            .into_iter()
            .map(|diagnostic| {
                let location = diagnostic.location.map_or_else(String::new, |location| {
                    format!("{}:{}: ", location.line, location.column)
                });
                let suggestions = if diagnostic.suggestions.is_empty() {
                    String::new()
                } else {
                    format!("; did you mean {}?", diagnostic.suggestions.join(" or "))
                };
                format!("{location}{}{suggestions}", diagnostic.message)
            })
            .collect::<Vec<_>>()
            .join("\n"),
        other => other.to_string(),
    }
}

fn format_session_error(error: casa_task_runtime::ParameterSessionError) -> String {
    match error {
        casa_task_runtime::ParameterSessionError::Profile(error) => format_profile_error(error),
        other => other.to_string(),
    }
}

fn params_help() -> String {
    "casars parameter profiles\n\n\
Usage:\n\
  casars params validate FILE\n\
  casars params show SURFACE [SOURCE] [OVERRIDES]\n\
  casars params save SURFACE FILE [SOURCE] [OVERRIDES]\n\
  casars params template SURFACE\n\
  casars params describe NAME\n\
  casars run TASK [SOURCE] [OVERRIDES] [--workspace DIR] [--notebook FILE_OR_ID] [--save-params FILE]\n\
  casars open SESSION [SOURCE] [OVERRIDES] [--workspace DIR] [--save-params FILE]\n\n\
SOURCE is exactly one of --defaults, --last, --last-successful (tasks), or --params FILE.\n\
Overrides use CASA names such as --vis, --imsize, --cell, or --unset NAME.\n\
Runtime-only controls: --notebook FILE_OR_ID, --initiating-surface cli|python, --no-save-last, --no-notebook-recording (one run), --confirm-overwrite, --confirm-mutation.\n"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_options_are_mutually_exclusive() {
        let bundle = builtin_surface_bundle("imager").unwrap();
        let error = parse_surface_options(&bundle, &["--defaults".into(), "--last".into()], true)
            .unwrap_err();
        assert!(error.contains("mutually exclusive"));
    }

    #[test]
    fn casa_named_imsize_and_cell_overrides_are_typed() {
        let bundle = builtin_surface_bundle("imager").unwrap();
        let options = parse_surface_options(
            &bundle,
            &[
                "--vis".into(),
                "input.ms".into(),
                "--imagename".into(),
                "out".into(),
                "--imsize".into(),
                "1024".into(),
                "--cell".into(),
                "0.2arcsec".into(),
            ],
            true,
        )
        .unwrap();
        let session = load_parameter_session(bundle, &options).unwrap();
        assert_eq!(
            session.values()["imsize"],
            ParameterValue::Array(vec![ParameterValue::Integer(1024); 2])
        );
        assert_eq!(
            session.values()["cell"],
            ParameterValue::Array(vec![ParameterValue::String("0.2arcsec".into()); 2])
        );
        let args = project_task_invocation(&session).unwrap().args;
        assert!(args.windows(2).any(|args| args == ["--imsize", "1024"]));
        assert!(args.windows(2).any(|args| args == ["--cell-arcsec", "0.2"]));
    }

    #[test]
    fn cli_named_file_source_resolves_shared_cross_surface_fixture() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../resources/test-profiles/imager-cross-surface.toml");
        let expected: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../resources/test-profiles/imager-cross-surface.expected.json"
        )))
        .unwrap();
        let bundle = builtin_surface_bundle("imager").unwrap();
        let options = parse_surface_options(
            &bundle,
            &["--params".into(), fixture.into_os_string()],
            true,
        )
        .unwrap();
        let session = load_parameter_session(bundle, &options).unwrap();

        assert_eq!(
            session.values()["imsize"],
            ParameterValue::Array(vec![ParameterValue::Integer(1024); 2])
        );
        assert_eq!(
            session.values()["cell"],
            ParameterValue::Array(vec![ParameterValue::String("1arcsec".into()); 2])
        );
        assert_eq!(expected["values"]["niter"], 7);
        assert_eq!(session.values()["niter"], ParameterValue::Integer(7));
    }

    #[test]
    fn task_runtime_controls_select_named_notebook_and_python_surface() {
        let bundle = builtin_surface_bundle("imstat").unwrap();
        let options = parse_surface_options(
            &bundle,
            &[
                "--imagename".into(),
                "input.image".into(),
                "--notebook".into(),
                "Analysis.md".into(),
                "--initiating-surface".into(),
                "python".into(),
            ],
            true,
        )
        .unwrap();
        assert_eq!(options.notebook.as_deref(), Some("Analysis.md"));
        assert_eq!(options.initiating_surface, "python");
    }

    #[test]
    fn cli_projection_carries_simobserve_family_request_over_stdin() {
        let bundle = builtin_surface_bundle("simobserve").unwrap();
        let options =
            parse_surface_options(&bundle, &["--request-kind".into(), "family".into()], true)
                .unwrap();
        let session = load_parameter_session(bundle, &options).unwrap();
        let invocation = project_task_invocation(&session).unwrap();

        assert_eq!(invocation.args, ["--json-run", "-"]);
        let request: serde_json::Value =
            serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
        assert_eq!(request["kind"], "family");
        assert_eq!(request["request"]["telescope"], "VLA");
    }

    #[test]
    fn loaded_overwrite_cannot_bypass_runtime_confirmation() {
        let bundle = builtin_surface_bundle("importfits").unwrap();
        let mut options = parse_surface_options(
            &bundle,
            &[
                "--fitsimage".into(),
                "input.fits".into(),
                "--imagename".into(),
                "out".into(),
                "--overwrite".into(),
            ],
            true,
        )
        .unwrap();
        let session = load_parameter_session(bundle, &options).unwrap();
        assert!(enforce_runtime_confirmations(&session, &options).is_err());
        options.confirm_overwrite = true;
        enforce_runtime_confirmations(&session, &options).unwrap();
    }

    #[test]
    fn catalog_input_mutation_rules_gate_adapter_tasks() {
        for surface in ["statwt", "clearcal", "delmod", "ft"] {
            let bundle = builtin_surface_bundle(surface).unwrap();
            let mut options = parse_surface_options(&bundle, &[], true).unwrap();
            let session = load_parameter_session(bundle, &options).unwrap();
            let error = enforce_runtime_confirmations(&session, &options).unwrap_err();
            assert!(error.contains("--confirm-mutation"), "{surface}: {error}");
            options.confirm_mutation = true;
            enforce_runtime_confirmations(&session, &options).unwrap();
        }
    }

    #[test]
    fn describe_uses_shared_concept_catalog() {
        let text = describe("imsize").unwrap();
        assert!(text.contains("image.geometry.imsize@r1"));
        assert!(text.contains("imager"));
        assert!(text.contains("simanalyze"));
        assert!(text.contains("simalma"));
    }

    #[test]
    fn session_open_can_explicitly_save_without_rewriting_its_source() {
        let temp = tempfile::tempdir().unwrap();
        let destination = temp.path().join("profiles/image.toml");
        let args = vec![
            "imexplore".into(),
            "--defaults".into(),
            "--image".into(),
            "cube.image".into(),
            "--save-params".into(),
            destination.clone().into_os_string(),
            "--no-save-last".into(),
        ];
        let launch = open_session(&args).unwrap();
        assert!(!launch.save_last);
        assert!(matches!(
            launch
                .parameter_session
                .as_ref()
                .expect("typed session")
                .base_source(),
            BaseSource::Defaults
        ));
        let profile = parse_profile(&fs::read_to_string(destination).unwrap()).unwrap();
        assert_eq!(profile.header.surface, "imexplore");
        assert_eq!(profile.header.kind, SurfaceKind::Session);
        assert_eq!(
            profile.parameters.get("image"),
            Some(&ParameterValue::String("cube.image".into()))
        );
    }
}
