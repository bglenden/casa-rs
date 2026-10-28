// SPDX-License-Identifier: LGPL-3.0-or-later
//! `casars params` and `casars run`, over the task front end's command-line
//! parsing and views in `casa-task-runtime`.

use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use casa_provider_contracts::{
    ProviderInvocation, ProviderInvocationAdaptation, builtin_surface_bundle,
};
use casa_task_runtime::{
    OpenSessionRequest, ParameterRuntime, ParameterSession, TaskCommandLine, TaskExit,
    TaskLastCoordinator, TaskOutcome, classify_task_exit, describe, format_profile_error,
    format_session_error, project_provider_invocation, render_documented_template, render_session,
    runs_on_front_end, task_command_line_help, validate_profile_text,
    write_parameter_profile_atomic,
};

use crate::execution::{ExecutionExit, ExecutionPlan, run_process_blocking};
use crate::registry::resolve_app;

/// What a `params` or `run` command produced.
pub(crate) enum ParameterCliDispatch {
    Print(String),
    Done,
}

/// Handle `casars params ...` and `casars run ...` (`args[0]` is the
/// command word).
pub(crate) fn dispatch(args: &[OsString]) -> Result<ParameterCliDispatch, String> {
    match args.first().and_then(|value| value.to_str()) {
        Some("params") => run_params(&args[1..]),
        Some("run") => run_task(&args[1..]).map(|()| ParameterCliDispatch::Done),
        other => Err(format!("expected params or run, got {other:?}")),
    }
}

fn run_params(args: &[OsString]) -> Result<ParameterCliDispatch, String> {
    let Some(command) = args.first().and_then(|value| value.to_str()) else {
        return Ok(ParameterCliDispatch::Print(params_help()));
    };
    let text = match command {
        "validate" => {
            let path = required_path(args.get(1), "profile file")?;
            reject_trailing(args, 2, "params validate")?;
            let source = fs::read_to_string(&path)
                .map_err(|error| format!("read parameter profile {}: {error}", path.display()))?;
            validate_profile_text(&source, None)?
        }
        "template" => {
            let surface = required_utf8(args.get(1), "surface")?;
            reject_trailing(args, 2, "params template")?;
            render_documented_template(&builtin_surface_bundle(&surface)?)
                .map_err(format_profile_error)?
        }
        "describe" => {
            let name = required_utf8(args.get(1), "parameter or surface name")?;
            reject_trailing(args, 2, "params describe")?;
            describe(&name)?
        }
        "show" => {
            let bundle = builtin_surface_bundle(&required_utf8(args.get(1), "surface")?)?;
            let line = TaskCommandLine::parse(&bundle, &args[2..])?;
            render_session(&open_session(line.open_session_request(bundle))?)
        }
        "save" => {
            let bundle = builtin_surface_bundle(&required_utf8(args.get(1), "surface")?)?;
            let destination = required_path(args.get(2), "destination profile")?;
            let line = TaskCommandLine::parse(&bundle, &args[3..])?;
            let session = open_session(line.open_session_request(bundle))?;
            save_explicit(
                &destination,
                &session.render_sparse().map_err(format_session_error)?,
            )?;
            format!("saved {}\n", destination.display())
        }
        "-h" | "--help" | "help" => params_help(),
        other => {
            return Err(format!(
                "unknown params command {other:?}; expected validate, show, save, template, or describe"
            ));
        }
    };
    Ok(ParameterCliDispatch::Print(text))
}

/// `casars run` owns Last for the runs it launches: the provider binary is
/// started with projected flags or a JSON request and writes none (#931).
fn run_task(args: &[OsString]) -> Result<(), String> {
    let surface_id = required_utf8(args.first(), "task name")?;
    let app = resolve_app(&surface_id)?;
    let bundle = builtin_surface_bundle(&surface_id)?;
    let line = TaskCommandLine::parse(&bundle, &without_retired_confirmations(&args[1..]))?;
    let session = open_session(line.open_session_request(bundle))?;
    let invocation = project_task_invocation(&session)?;

    if let Some(path) = &line.save_params {
        save_explicit(
            path,
            &session.render_sparse().map_err(format_session_error)?,
        )?;
    }

    let last = TaskLastCoordinator::new();
    let attempt_id = format!("cli:{surface_id}");
    last.before_execution(
        &attempt_id,
        &line.workspace,
        &surface_id,
        !line.no_save_last,
        &session,
    )
    .map_err(format_session_error)?;
    for warning in last.take_warnings() {
        eprintln!("Warning: could not save Last for {surface_id}: {warning}");
    }

    let front_end = runs_on_front_end(session.bundle());
    let execution = app.resolve_command().and_then(|command| {
        let plan = ExecutionPlan {
            command,
            arguments: invocation.args.iter().map(OsString::from).collect(),
            stdin: invocation.stdin,
            working_directory: line.workspace.clone(),
            capture_stdout: front_end,
        };
        run_process_blocking(&plan).map_err(|error| format!("run {surface_id}: {error}"))
    });
    let outcome = match execution {
        Ok(exit) if front_end => front_end_outcome(&surface_id, &exit),
        Ok(exit) if exit.success() => Ok(()),
        Ok(exit) => Err(format!(
            "{surface_id} exited with code {}",
            exit.code()
                .map_or_else(|| "unknown".to_owned(), |code| code.to_string())
        )),
        Err(error) => Err(error),
    };
    last.after_completion(&attempt_id, outcome.is_ok());
    for warning in last.take_warnings() {
        eprintln!("Warning: could not save Last Successful for {surface_id}: {warning}");
    }
    outcome
}

/// Read a front-end binary's typed reply: print CASA's return value, or
/// report the task's own failure, or why there was no reply.
fn front_end_outcome(surface_id: &str, exit: &ExecutionExit) -> Result<(), String> {
    let stdout = exit.stdout.as_deref().unwrap_or_default();
    match classify_task_exit(TaskExit::from(exit.status), stdout) {
        TaskOutcome::Succeeded { value, .. } => {
            if !value.is_null() {
                println!("{value:#}");
            }
            Ok(())
        }
        TaskOutcome::Refused { message, .. } | TaskOutcome::Failed { message, .. } => Err(message),
        TaskOutcome::Signalled { signal } => {
            Err(format!("{surface_id} was killed by signal {signal}"))
        }
        TaskOutcome::LaunchFailed { message } => Err(format!("run {surface_id}: {message}")),
        TaskOutcome::MalformedReply { reason, .. } => Err(format!("{surface_id}: {reason}")),
    }
}

/// `--confirm-overwrite` and `--confirm-mutation` are retired (#931): CASA's
/// own parameters, such as `overwrite`, are the consent. `casars run` accepts
/// them, and they change nothing, until the launcher goes.
fn without_retired_confirmations(args: &[OsString]) -> Vec<OsString> {
    args.iter()
        .filter(|arg| {
            !matches!(
                arg.to_str(),
                Some("--confirm-overwrite" | "--confirm-mutation")
            )
        })
        .cloned()
        .collect()
}

fn open_session(request: OpenSessionRequest) -> Result<ParameterSession, String> {
    ParameterRuntime::default()
        .open_session(request)
        .map_err(|error| error.to_string())
}

pub(crate) fn project_task_invocation(
    session: &ParameterSession,
) -> Result<ProviderInvocation, String> {
    project_provider_invocation(session, |family, values, direct| match family {
        "imager" => casars_imager::imager_provider_invocation(values, direct.args),
        "simobserve" => casars_simulation::simobserve_provider_invocation(values, direct.args),
        _ => Ok(ProviderInvocationAdaptation::direct(direct)),
    })
    .map_err(|error| error.to_string())
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

fn params_help() -> String {
    format!(
        "casars parameter profiles\n\n\
Usage:\n\
  casars params validate FILE\n\
  casars params show TASK [SOURCE] [OVERRIDES]\n\
  casars params save TASK FILE [SOURCE] [OVERRIDES]\n\
  casars params template TASK\n\
  casars params describe NAME\n\
  casars run TASK [SOURCE] [OVERRIDES] [CONTROLS]\n\n{}",
        task_command_line_help()
    )
}

#[cfg(test)]
mod tests {
    use casa_provider_contracts::{ParameterValue, SurfaceContractBundle};

    use super::*;

    fn session(bundle: SurfaceContractBundle, args: &[&str]) -> ParameterSession {
        let args = args.iter().map(OsString::from).collect::<Vec<_>>();
        let line = TaskCommandLine::parse(&bundle, &without_retired_confirmations(&args)).unwrap();
        open_session(line.open_session_request(bundle)).unwrap()
    }

    #[test]
    fn casa_named_imsize_and_cell_overrides_are_typed() {
        let session = session(
            builtin_surface_bundle("imager").unwrap(),
            &[
                "--ms",
                "input.ms",
                "--imagename",
                "out",
                "--imsize",
                "1024",
                "--cell-arcsec",
                "0.2arcsec",
            ],
        );
        assert_eq!(
            session.values()["imsize"],
            ParameterValue::Array(vec![ParameterValue::Integer(1024); 2])
        );
        assert_eq!(
            session.values()["cell"],
            ParameterValue::Array(vec![ParameterValue::String("0.2arcsec".into()); 2])
        );
        let invocation = project_task_invocation(&session).unwrap();
        assert_eq!(
            invocation.args,
            ["--managed-output", "true", "--json-run", "-"]
        );
        let request: serde_json::Value =
            serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
        assert_eq!(request["kind"], "run");
        assert_eq!(
            request["request"]["imsize"],
            serde_json::json!([1024, 1024])
        );
        assert_eq!(
            request["request"]["cell"],
            serde_json::json!(["0.2arcsec", "0.2arcsec"])
        );
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
        let session = session(
            builtin_surface_bundle("imager").unwrap(),
            &["--params", fixture.to_str().unwrap()],
        );
        let invocation = project_task_invocation(&session).unwrap();
        let request: serde_json::Value =
            serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
        assert_eq!(request, expected["request"]);
    }

    #[test]
    fn cli_projection_carries_the_simobserve_request_over_stdin() {
        let session = session(
            builtin_surface_bundle("simobserve").unwrap(),
            &["--project", "psim2", "--antennalist", "alma.out20.cfg"],
        );
        let invocation = project_task_invocation(&session).unwrap();

        assert_eq!(
            invocation.args,
            ["--managed-output", "true", "--json-run", "-"]
        );
        let request: serde_json::Value =
            serde_json::from_str(invocation.stdin.as_deref().unwrap()).unwrap();
        assert_eq!(request["kind"], "run");
        assert_eq!(request["request"]["project"], "psim2");
        assert_eq!(request["request"]["antennalist"], "alma.out20.cfg");
        // CASA's defaults travel with the request.
        assert_eq!(request["request"]["ptgfile"], "$project.ptg.txt");
        assert_eq!(request["request"]["overwrite"], true);
    }

    #[test]
    fn retired_confirmation_flags_are_accepted_and_change_nothing() {
        let bundle = builtin_surface_bundle("importfits").unwrap();
        let plain = session(bundle.clone(), &["input.fits", "out", "--overwrite"]);
        let confirmed = session(
            bundle,
            &[
                "input.fits",
                "out",
                "--overwrite",
                "--confirm-overwrite",
                "--confirm-mutation",
            ],
        );
        assert_eq!(plain.values(), confirmed.values());
        assert_eq!(plain.values()["overwrite"], ParameterValue::Bool(true));
        assert_eq!(
            project_task_invocation(&plain).unwrap().args,
            project_task_invocation(&confirmed).unwrap().args
        );
    }
}
