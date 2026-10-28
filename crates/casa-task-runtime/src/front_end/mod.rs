// SPDX-License-Identifier: LGPL-3.0-or-later
//! The shared task front end (#931): the one entry every task binary's `main`
//! calls, generic over the binary's execution callback.
//!
//! A binary hosts one surface, or a family of them; the first word of its
//! command line may name a hosted surface, and the first hosted surface runs
//! otherwise. The front end owns everything around the callback:
//!
//! - logging for a run, configured by the binary's `main` and the logging
//!   flags, and written to the stderr `main` passes;
//! - the CASA-named flags the surface declares, `--unset NAME`, and one base
//!   source (`--defaults`, `--last`, `--last-successful` or `--params FILE`);
//! - `--workspace DIR`, `--save-params FILE` and `--no-save-last`;
//! - the non-executing `--show`, `--template` (also `--profile-template`),
//!   `--describe [NAME]` and `--validate FILE`;
//! - the machine actions `--json-run SOURCE`, `--json-schema` and
//!   `--protocol-info`.
//!
//! A spelling the surface declares is always its parameter: imregrid's
//! `--template` names its template image, so there `--profile-template`
//! gives the profile.
//!
//! Last has one owner on each path. A command line records Last before the
//! callback runs and Last Successful only after it succeeds, unless
//! `--no-save-last` is given. `--json-run` neither reads nor writes Last: it
//! is the path a caller that owns its own lifecycle (`casars run`, Python)
//! uses. It resolves a [`TaskRunRequest`]'s values against the named
//! surface's contract and writes one [`TaskReply`] to stdout, whose exit
//! status agrees with it; [`classify_task_exit`] reads it back.
//!
//! [`TaskRunRequest`]: casa_provider_contracts::TaskRunRequest
//! [`TaskReply`]: casa_provider_contracts::TaskReply

mod help;
mod inspect;
mod options;
mod outcome;
mod values;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, Mutex, PoisonError};

use casa_logging::{CasaLoggingConfig, CasaLoggingError, CasaLoggingGuard};
use casa_provider_contracts::{
    EXIT_EXECUTION_FAILED, EXIT_SUCCEEDED, EXIT_VALIDATION_FAILED, ParameterValue,
    SurfaceContractBundle, TaskFailureKind, TaskFrontEndSchemas, TaskProviderContract, TaskReply,
    TaskRunRequest,
};
use serde::Serialize;
use serde_json::{Map, Value};

pub use inspect::{
    describe, display_value, format_profile_error, format_session_error, render_session,
    validate_profile_text,
};
pub use options::{TaskCommandLine, task_command_line_help};
pub use outcome::{TaskExit, TaskOutcome, classify_task_exit};

use crate::{
    BaseSource, DiagnosticLevel, OpenSessionRequest, ParameterRuntime, ParameterRuntimeError,
    ParameterSession, ResolutionPatch, TaskLastCoordinator, read_task_request,
    render_documented_template, write_parameter_profile_atomic,
};

/// How a task binary's process enters a run's workspace, which its `main`
/// hands [`TaskFrontEnd::main`]: the standard library's `set_current_dir`.
/// The front end passes the workspace as an absolute path.
pub type EnterWorkspace = fn(PathBuf) -> std::io::Result<()>;

/// A task's success, as its execution callback returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskSuccess {
    value: Value,
    result: Value,
    text: Option<String>,
}

impl TaskSuccess {
    /// A success returning CASA's `value` (`&()` where CASA returns `None`)
    /// and the provider's full `result`.
    pub fn new(value: &impl Serialize, result: &impl Serialize) -> Result<Self, TaskFailure> {
        Ok(Self {
            value: to_json(value)?,
            result: to_json(result)?,
            text: None,
        })
    }

    /// What a command line prints in place of the value's JSON.
    #[must_use]
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }
}

/// Why a task did not succeed, as its execution callback reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskFailure {
    /// Whether the parameters were refused or the run failed.
    pub kind: TaskFailureKind,
    /// The reason.
    pub message: String,
    /// The parameter at fault, when there is one.
    pub parameter: Option<String>,
}

impl TaskFailure {
    /// The parameters were refused before the task changed anything.
    pub fn validation(message: impl Into<String>) -> Self {
        Self {
            kind: TaskFailureKind::Validation,
            message: message.into(),
            parameter: None,
        }
    }

    /// The task started and failed.
    pub fn execution(message: impl Into<String>) -> Self {
        Self {
            kind: TaskFailureKind::Execution,
            message: message.into(),
            parameter: None,
        }
    }

    /// Name the parameter at fault.
    #[must_use]
    pub fn at(mut self, parameter: impl Into<String>) -> Self {
        self.parameter = Some(parameter.into());
        self
    }
}

/// One resolved, validated run, as the execution callback receives it.
pub struct TaskInvocation<'a> {
    session: &'a ParameterSession,
    workspace: &'a Path,
}

impl TaskInvocation<'_> {
    /// The surface to run.
    pub fn surface(&self) -> &str {
        self.session.bundle().surface.id()
    }

    /// The resolved session, with each value's origin.
    pub fn session(&self) -> &ParameterSession {
        self.session
    }

    /// Every active parameter's resolved value, by CASA name.
    pub fn values(&self) -> BTreeMap<String, ParameterValue> {
        self.session
            .states()
            .iter()
            .filter(|(_, state)| state.active)
            .filter_map(|(name, state)| Some((name.clone(), state.value.clone()?)))
            .collect()
    }

    /// [`Self::values`] as plain JSON.
    pub fn plain_values(&self) -> Map<String, Value> {
        self.values()
            .into_iter()
            .map(|(name, value)| (name, value.to_plain_json()))
            .collect()
    }

    /// Where relative paths resolve.
    pub fn workspace(&self) -> &Path {
        self.workspace
    }
}

/// What one dispatch writes, and the exit status it ends with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrontEndOutput {
    /// Everything for stdout: a reply, a view, or the task's output.
    pub stdout: String,
    /// Lines for stderr: errors and Last warnings.
    pub stderr: Vec<String>,
    /// The process exit status.
    pub code: i32,
}

impl FrontEndOutput {
    fn print(stdout: String) -> Self {
        Self {
            stdout,
            stderr: Vec::new(),
            code: EXIT_SUCCEEDED,
        }
    }

    fn error(code: i32, message: impl Into<String>) -> Self {
        Self {
            stdout: String::new(),
            stderr: vec![format!("Error: {}", message.into())],
            code,
        }
    }

    fn refused(message: impl Into<String>) -> Self {
        Self::error(EXIT_VALIDATION_FAILED, message)
    }
}

/// The front end of one task binary.
pub struct TaskFrontEnd<F> {
    program: String,
    contract: Value,
    protocol: Value,
    surfaces: Vec<SurfaceContractBundle>,
    execute: F,
}

/// The one action a command line asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Action {
    Help,
    JsonSchema,
    ProtocolInfo,
    JsonRun(String),
    Template,
    Describe(Option<String>),
    Validate(PathBuf),
    Show,
    Run,
}

impl<F> TaskFrontEnd<F>
where
    F: Fn(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>,
{
    /// The front end of `program`, which hosts `surfaces` from `contract`; the
    /// first runs when the command line's first word names none.
    pub fn new<E: Serialize>(
        program: impl Into<String>,
        mut contract: TaskProviderContract<E>,
        surfaces: &[&str],
        execute: F,
    ) -> Result<Self, String> {
        contract.validate().map_err(|errors| {
            errors
                .into_iter()
                .map(|error| format!("{}: {}", error.code, error.message))
                .collect::<Vec<_>>()
                .join("; ")
        })?;
        let hosted = surfaces
            .iter()
            .map(|id| {
                contract
                    .parameter_surfaces
                    .iter()
                    .find(|bundle| bundle.surface.id() == *id)
                    .cloned()
                    .ok_or_else(|| format!("the provider contract carries no surface {id:?}"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if hosted.is_empty() {
            return Err("a task front end hosts at least one surface".to_string());
        }
        contract
            .semantic
            .front_end
            .get_or_insert_with(TaskFrontEndSchemas::default);
        let protocol = serde_json::to_value(&contract.protocol).map_err(|e| e.to_string())?;
        let contract = serde_json::to_value(&contract).map_err(|e| e.to_string())?;
        Ok(Self {
            program: program.into(),
            contract,
            protocol,
            surfaces: hosted,
            execute,
        })
    }

    /// Run as the binary's `main`: dispatch the command line, with the
    /// process's logging installed for a run, and write the output.
    ///
    /// The binary owns the process. Its `main` passes its arguments without
    /// the program name (`args_os().skip(1)`), the logging configuration it
    /// reads from the environment ([`CasaLoggingConfig::from_env`]), how the
    /// process enters a run's workspace ([`EnterWorkspace`]), and
    /// its standard output and error, and returns the exit code. The front
    /// end reads no environment variable, changes no process state itself
    /// and writes nowhere else (ADR-0016): a run's log records go to
    /// `stderr`, interleaved with the front end's own lines, and to a log
    /// table only when the configuration or a logging flag names one. An
    /// action that runs nothing installs no logging, so a configuration the
    /// environment gets wrong refuses only a run.
    ///
    /// A command-line run enters its workspace (`--workspace`, the current
    /// directory otherwise) before the task starts, as `casars run` starts
    /// the binary there, so every relative path the run names resolves in
    /// it: inputs and products, and the paths inside a parameter's text,
    /// such as a region file or an image in a mask expression. The files
    /// the controls name (`--params`, `--save-params`, a log table) are read
    /// from where the command was run, before it enters.
    ///
    /// A run installs the process's global `tracing` subscriber, so a process
    /// runs one.
    ///
    /// [`CasaLoggingConfig::from_env`]: casa_logging::CasaLoggingConfig::from_env
    pub fn main<W: Write + Send + 'static>(
        &self,
        args: impl IntoIterator<Item = OsString>,
        logging: Result<CasaLoggingConfig, CasaLoggingError>,
        enter_workspace: EnterWorkspace,
        stdout: &mut dyn Write,
        stderr: W,
    ) -> ExitCode {
        let stderr = Arc::new(Mutex::new(stderr));
        let (output, guard) = self.dispatch_with_logging(
            args.into_iter().collect(),
            logging,
            enter_workspace,
            &stderr,
        );
        let _ = stdout.write_all(output.stdout.as_bytes());
        let _ = stdout.flush();
        let write_lines = |lines: &[String]| {
            let mut stderr = stderr.lock().unwrap_or_else(PoisonError::into_inner);
            for line in lines {
                let _ = writeln!(stderr, "{line}");
            }
            let _ = stderr.flush();
        };
        write_lines(&output.stderr);
        if let Some(Err(error)) = guard.as_ref().map(CasaLoggingGuard::flush) {
            write_lines(&[format!("Error: failed to flush logging: {error}")]);
        }
        exit_code(output.code)
    }

    /// [`Self::dispatch`] after taking the logging flags out of `args`, with
    /// logging to `stderr` installed only for an action that runs the task.
    fn dispatch_with_logging<W: Write + Send + 'static>(
        &self,
        args: Vec<OsString>,
        logging: Result<CasaLoggingConfig, CasaLoggingError>,
        enter_workspace: EnterWorkspace,
        stderr: &Arc<Mutex<W>>,
    ) -> (FrontEndOutput, Option<CasaLoggingGuard>) {
        let stripped = match casa_logging::strip_cli_logging_args(args.clone()) {
            Ok(stripped) => stripped,
            Err(error) => return (FrontEndOutput::refused(error.to_string()), None),
        };
        let (bundle, action, rest) = match self.parse(&stripped) {
            Ok(parsed) => parsed,
            Err(refused) => return (refused, None),
        };
        let enter = Some(enter_workspace);
        if !matches!(action, Action::Run | Action::JsonRun(_)) {
            return (self.perform(bundle, action, &rest, enter), None);
        }
        let installed = logging
            .and_then(|config| config.with_cli_args(args))
            .and_then(|(_, mut config)| {
                // Named from where the command was run, before a run enters
                // its workspace.
                config.table_path = config
                    .table_path
                    .map(|path| std::path::absolute(&path).unwrap_or(path));
                casa_logging::init_global_with_stderr(config, stderr.clone())
            });
        match installed {
            Ok(guard) => (self.perform(bundle, action, &rest, enter), Some(guard)),
            Err(error) => {
                let message = format!("failed to initialize logging: {error}");
                let refused = match action {
                    Action::JsonRun(_) => reply_output(&TaskReply::Failed {
                        surface: bundle.surface.id().to_string(),
                        kind: TaskFailureKind::Validation,
                        message,
                        parameter: None,
                    }),
                    _ => FrontEndOutput::refused(message),
                };
                (refused, None)
            }
        }
    }

    /// Handle one command line, without the program name. It changes no
    /// process state: a run gets its workspace but does not enter it, as
    /// [`Self::main`]'s does.
    pub fn dispatch(&self, args: &[OsString]) -> FrontEndOutput {
        self.dispatch_entering(args, None)
    }

    /// [`Self::dispatch`], entering a command-line run's workspace with
    /// `enter` when there is one.
    fn dispatch_entering(
        &self,
        args: &[OsString],
        enter: Option<EnterWorkspace>,
    ) -> FrontEndOutput {
        match self.parse(args) {
            Ok((bundle, action, rest)) => self.perform(bundle, action, &rest, enter),
            Err(refused) => refused,
        }
    }

    /// The surface, the one action, and the arguments left for it.
    fn parse(
        &self,
        args: &[OsString],
    ) -> Result<(&SurfaceContractBundle, Action, Vec<OsString>), FrontEndOutput> {
        let (bundle, args) = self.select_surface(args);
        let (action, rest) = parse_action(bundle, args).map_err(FrontEndOutput::refused)?;
        let takes_arguments = matches!(action, Action::Show | Action::Run | Action::Help);
        if !takes_arguments && !rest.is_empty() {
            return Err(FrontEndOutput::refused(format!(
                "{} takes no other arguments",
                action_flag(&action)
            )));
        }
        Ok((bundle, action, rest))
    }

    fn perform(
        &self,
        bundle: &SurfaceContractBundle,
        action: Action,
        rest: &[OsString],
        enter: Option<EnterWorkspace>,
    ) -> FrontEndOutput {
        match action {
            Action::Help => {
                FrontEndOutput::print(help::render_help(&self.program, &self.surfaces, bundle))
            }
            Action::JsonSchema => pretty(&self.contract),
            Action::ProtocolInfo => pretty(&self.protocol),
            Action::JsonRun(source) => self.run_json(bundle, &source),
            Action::Template => match render_documented_template(bundle) {
                Ok(text) => FrontEndOutput::print(text),
                Err(error) => FrontEndOutput::refused(format_profile_error(error)),
            },
            Action::Describe(name) => {
                match describe(name.as_deref().unwrap_or(bundle.surface.id())) {
                    Ok(text) => FrontEndOutput::print(text),
                    Err(error) => FrontEndOutput::refused(error),
                }
            }
            Action::Validate(path) => match std::fs::read_to_string(&path) {
                Ok(text) => match validate_profile_text(&text, Some(bundle)) {
                    Ok(summary) => FrontEndOutput::print(summary),
                    Err(error) => FrontEndOutput::refused(error),
                },
                Err(error) => FrontEndOutput::refused(format!(
                    "read parameter profile {}: {error}",
                    path.display()
                )),
            },
            Action::Show => self.show(bundle, rest),
            Action::Run => self.run_command_line(bundle, rest, enter),
        }
    }

    fn select_surface<'a>(&self, args: &'a [OsString]) -> (&SurfaceContractBundle, &'a [OsString]) {
        let named = args
            .first()
            .and_then(|word| word.to_str())
            .and_then(|word| {
                self.surfaces
                    .iter()
                    .find(|bundle| bundle.surface.id() == word)
            });
        match named {
            Some(bundle) => (bundle, &args[1..]),
            None => (&self.surfaces[0], args),
        }
    }

    fn show(&self, bundle: &SurfaceContractBundle, args: &[OsString]) -> FrontEndOutput {
        let line = match TaskCommandLine::parse(bundle, args) {
            Ok(line) => line,
            Err(error) => return FrontEndOutput::refused(error),
        };
        let session = match open_session(line.open_session_request(bundle.clone())) {
            Ok(session) => session,
            Err(error) => return FrontEndOutput::refused(error),
        };
        let mut text = render_session(&session);
        if let Some(path) = &line.save_params {
            if let Err(error) = save_sparse(&session, path) {
                return FrontEndOutput::refused(error);
            }
            text.push_str(&format!("saved {}\n", path.display()));
        }
        FrontEndOutput::print(text)
    }

    fn run_command_line(
        &self,
        bundle: &SurfaceContractBundle,
        args: &[OsString],
        enter: Option<EnterWorkspace>,
    ) -> FrontEndOutput {
        let line = match TaskCommandLine::parse(bundle, args) {
            Ok(line) => line,
            Err(error) => return FrontEndOutput::refused(error),
        };
        let session = match open_session(line.open_session_request(bundle.clone()))
            .and_then(|session| refuse_errors(&session).map(|()| session))
        {
            Ok(session) => session,
            Err(error) => return FrontEndOutput::refused(error),
        };
        if let Some(path) = &line.save_params
            && let Err(error) = save_sparse(&session, path)
        {
            return FrontEndOutput::refused(error);
        }
        // From here on the run is in its workspace, as under `casars run`.
        let workspace = match std::path::absolute(&line.workspace) {
            Ok(workspace) => workspace,
            Err(error) => {
                return FrontEndOutput::refused(format!(
                    "workspace {}: {error}",
                    line.workspace.display()
                ));
            }
        };
        if let Some(enter) = enter
            && let Err(error) = enter(workspace.clone())
        {
            return FrontEndOutput::refused(format!(
                "cannot enter workspace {}: {error}",
                workspace.display()
            ));
        }
        let surface = bundle.surface.id();
        let last = TaskLastCoordinator::new();
        let attempt = format!("cli:{surface}");
        if let Err(error) =
            last.before_execution(&attempt, &workspace, surface, !line.no_save_last, &session)
        {
            return FrontEndOutput::refused(format_session_error(error));
        }
        let mut stderr = last
            .take_warnings()
            .into_iter()
            .map(|warning| format!("Warning: could not save Last for {surface}: {warning}"))
            .collect::<Vec<_>>();
        let outcome = self.execute(&session, &workspace);
        last.after_completion(&attempt, outcome.is_ok());
        stderr.extend(last.take_warnings().into_iter().map(|warning| {
            format!("Warning: could not save Last Successful for {surface}: {warning}")
        }));
        match outcome {
            Ok(success) => {
                let stdout = match success.text {
                    Some(text) => text,
                    None if success.value.is_null() => String::new(),
                    None => format!("{:#}\n", success.value),
                };
                FrontEndOutput {
                    stdout,
                    stderr,
                    code: EXIT_SUCCEEDED,
                }
            }
            Err(failure) => {
                stderr.push(format!("Error: {}", failure.message));
                FrontEndOutput {
                    stdout: String::new(),
                    stderr,
                    code: failure.kind.exit_code(),
                }
            }
        }
    }

    fn run_json(&self, default: &SurfaceContractBundle, source: &str) -> FrontEndOutput {
        reply_output(&self.json_reply(default, source))
    }

    fn json_reply(&self, default: &SurfaceContractBundle, source: &str) -> TaskReply {
        let refuse =
            |surface: &str, message: String, parameter: Option<String>| TaskReply::Failed {
                surface: surface.to_string(),
                kind: TaskFailureKind::Validation,
                message,
                parameter,
            };
        let request = match read_task_request(source).and_then(|text| {
            serde_json::from_str::<TaskRunRequest>(&text).map_err(crate::TaskCliError::ParseRequest)
        }) {
            Ok(request) => request,
            Err(error) => return refuse(default.surface.id(), error.to_string(), None),
        };
        let Some(bundle) = self
            .surfaces
            .iter()
            .find(|bundle| bundle.surface.id() == request.surface)
        else {
            let hosted = self
                .surfaces
                .iter()
                .map(|bundle| bundle.surface.id())
                .collect::<Vec<_>>();
            return refuse(
                &request.surface,
                format!(
                    "{} runs {}, not {:?}",
                    self.program,
                    hosted.join(", "),
                    request.surface
                ),
                None,
            );
        };
        let mut overrides = ResolutionPatch::default();
        for (name, value) in &request.values {
            let Some(value) = ParameterValue::from_plain_json(value) else {
                return refuse(
                    &request.surface,
                    format!("parameter {name:?} is null; leave it out to take its default"),
                    Some(name.clone()),
                );
            };
            // Checked here, so that a value the surface refuses is named.
            let Some(binding) = bundle
                .surface
                .bindings()
                .iter()
                .find(|binding| binding.name == *name)
            else {
                return refuse(
                    &request.surface,
                    format!("unknown {} parameter {name:?}", request.surface),
                    Some(name.clone()),
                );
            };
            if let Err(error) = crate::cli::checked_value(bundle, binding, &value) {
                return refuse(&request.surface, error, Some(name.clone()));
            }
            overrides.values.insert(name.clone(), value);
        }
        let workspace = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let session = match open_session(OpenSessionRequest {
            bundle: bundle.clone(),
            workspace: workspace.clone(),
            source: BaseSource::Defaults,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: overrides,
            managed_save: false,
        }) {
            Ok(session) => session,
            Err(error) => return refuse(&request.surface, error, None),
        };
        if let Some(diagnostic) = first_error(&session) {
            return refuse(
                &request.surface,
                diagnostic.message.clone(),
                diagnostic.parameter.clone(),
            );
        }
        match self.execute(&session, &workspace) {
            Ok(success) => TaskReply::Succeeded {
                surface: request.surface,
                value: success.value,
                result: success.result,
            },
            Err(failure) => TaskReply::Failed {
                surface: request.surface,
                kind: failure.kind,
                message: failure.message,
                parameter: failure.parameter,
            },
        }
    }

    fn execute(
        &self,
        session: &ParameterSession,
        workspace: &Path,
    ) -> Result<TaskSuccess, TaskFailure> {
        let surface = session.bundle().surface.id();
        tracing::info!(task = surface, "{surface} started");
        let outcome = (self.execute)(&TaskInvocation { session, workspace });
        match &outcome {
            Ok(_) => tracing::info!(task = surface, "{surface} completed"),
            Err(failure) => tracing::error!(
                casa.priority = "SEVERE",
                task = surface,
                error = %failure.message,
                "{surface} failed"
            ),
        }
        outcome
    }
}

/// The one action `args` asks for, and the arguments left for it. A spelling
/// the surface declares is its parameter, never an action: imregrid's
/// `--template` names its template image, and `--profile-template` reaches
/// the documented profile on every surface.
fn parse_action(
    bundle: &SurfaceContractBundle,
    args: &[OsString],
) -> Result<(Action, Vec<OsString>), String> {
    let mut action = None;
    let mut rest = Vec::new();
    let mut index = 0usize;
    while index < args.len() {
        let raw = args[index].to_str();
        if raw.is_some_and(|flag| crate::cli::surface_declares_flag(bundle, flag)) {
            rest.push(args[index].clone());
            index += 1;
            continue;
        }
        let found = match raw {
            Some("-h" | "--help") => Some(Action::Help),
            Some("--json-schema") => Some(Action::JsonSchema),
            Some("--protocol-info") => Some(Action::ProtocolInfo),
            Some("--template" | "--profile-template") => Some(Action::Template),
            Some("--show") => Some(Action::Show),
            Some("--json-run") => {
                index += 1;
                let source = args
                    .get(index)
                    .and_then(|value| value.to_str())
                    .ok_or_else(|| "missing value for --json-run".to_string())?;
                Some(Action::JsonRun(source.to_string()))
            }
            Some("--validate") => {
                index += 1;
                Some(Action::Validate(options::required_path(
                    args.get(index),
                    "--validate profile",
                )?))
            }
            Some("--describe") => {
                let name = args
                    .get(index + 1)
                    .and_then(|value| value.to_str())
                    .filter(|value| !value.starts_with('-'));
                if name.is_some() {
                    index += 1;
                }
                Some(Action::Describe(name.map(str::to_string)))
            }
            _ => None,
        };
        match found {
            Some(Action::Help) => return Ok((Action::Help, Vec::new())),
            Some(found) if action.is_some() => {
                return Err(format!(
                    "{} and {} cannot be combined",
                    action_flag(action.as_ref().expect("checked")),
                    action_flag(&found)
                ));
            }
            Some(found) => action = Some(found),
            None => rest.push(args[index].clone()),
        }
        index += 1;
    }
    Ok((action.unwrap_or(Action::Run), rest))
}

fn action_flag(action: &Action) -> &'static str {
    match action {
        Action::Help => "--help",
        Action::JsonSchema => "--json-schema",
        Action::ProtocolInfo => "--protocol-info",
        Action::JsonRun(_) => "--json-run",
        Action::Template => "--template",
        Action::Describe(_) => "--describe",
        Action::Validate(_) => "--validate",
        Action::Show => "--show",
        Action::Run => "a run",
    }
}

fn open_session(request: OpenSessionRequest) -> Result<ParameterSession, String> {
    ParameterRuntime::default()
        .open_session(request)
        .map_err(|error| match error {
            ParameterRuntimeError::Session(error) => format_session_error(error),
            ParameterRuntimeError::Profile(error) => format_profile_error(error),
            other => other.to_string(),
        })
}

fn first_error(session: &ParameterSession) -> Option<&crate::Diagnostic> {
    session
        .diagnostics()
        .iter()
        .find(|diagnostic| diagnostic.level == DiagnosticLevel::Error)
}

fn refuse_errors(session: &ParameterSession) -> Result<(), String> {
    let errors = session
        .diagnostics()
        .iter()
        .filter(|diagnostic| diagnostic.level == DiagnosticLevel::Error)
        .map(|diagnostic| diagnostic.message.clone())
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n"))
    }
}

fn save_sparse(session: &ParameterSession, path: &Path) -> Result<(), String> {
    let text = session.render_sparse().map_err(format_session_error)?;
    write_parameter_profile_atomic(path, &text)
        .map(|_| ())
        .map_err(|error| format!("save {}: {error}", path.display()))
}

/// `reply` on stdout, with the exit status that agrees with it.
fn reply_output(reply: &TaskReply) -> FrontEndOutput {
    match serde_json::to_string(reply) {
        Ok(text) => FrontEndOutput {
            stdout: text + "\n",
            stderr: Vec::new(),
            code: reply.exit_code(),
        },
        Err(error) => FrontEndOutput::error(
            EXIT_EXECUTION_FAILED,
            format!("failed to serialize the task reply: {error}"),
        ),
    }
}

fn pretty(value: &Value) -> FrontEndOutput {
    FrontEndOutput::print(format!("{value:#}\n"))
}

fn to_json(value: &impl Serialize) -> Result<Value, TaskFailure> {
    serde_json::to_value(value)
        .map_err(|error| TaskFailure::execution(format!("failed to serialize the result: {error}")))
}

fn exit_code(code: i32) -> ExitCode {
    ExitCode::from(u8::try_from(code).unwrap_or(1))
}

#[cfg(test)]
mod tests;
