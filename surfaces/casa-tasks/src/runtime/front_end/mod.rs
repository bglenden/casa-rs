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
//! is the path Python's task calls use, which save no Last. It resolves a [`TaskRunRequest`]'s values against the named
//! surface's contract and writes one [`TaskReply`] to stdout, whose exit
//! status agrees with it; [`classify_task_exit`] reads it back.
//!
//! [`TaskRunRequest`]: casa_contracts::TaskRunRequest
//! [`TaskReply`]: casa_contracts::TaskReply

mod help;
mod inspect;
mod options;
mod outcome;
mod values;

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

mod native;
#[cfg(feature = "imaging")]
pub use native::TaskCancellation;
pub use native::{NativeTaskOptions, TaskLogLineSink, run_task_in_process};

use casa_contracts::{
    CasaValue, EXIT_EXECUTION_FAILED, EXIT_SUCCEEDED, EXIT_VALIDATION_FAILED, ParameterValue,
    ProviderProtocolDescriptor, SurfaceContractBundle, TaskFailureKind, TaskFrontEndSchemas,
    TaskProviderContract, TaskReply, TaskRunRequest,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};

pub use inspect::{
    describe, display_value, format_profile_error, format_session_error, render_session,
    validate_profile_text,
};
pub use options::{TaskCommandLine, task_command_line_help};
pub use outcome::{TaskExit, TaskOutcome, classify_task_exit};

use crate::runtime::{
    BaseSource, DiagnosticLevel, OpenSessionRequest, ParameterRuntime, ParameterRuntimeError,
    ParameterSession, ParameterSessionError, ProfileError, ResolutionPatch, TaskLastCoordinator,
    read_task_request, render_documented_template, write_parameter_profile_atomic,
};

/// How a task binary's process enters a run's workspace, which its `main`
/// hands [`TaskFrontEnd::dispatch_for_cli`]: the standard library's `set_current_dir`.
/// The front end passes the workspace as an absolute path.
pub type EnterWorkspace = fn(PathBuf) -> std::io::Result<()>;

/// A task's success, as its execution callback returns it.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskSuccess {
    value: CasaValue,
    result: Value,
    text: Option<String>,
    notes: Vec<String>,
    output_lines: Vec<String>,
}

impl TaskSuccess {
    /// A success returning CASA's `value` ([`CasaValue::None`] where CASA
    /// returns `None`) and the provider's full `result`.
    pub fn new(value: CasaValue, result: &impl Serialize) -> Result<Self, TaskFailure> {
        if !value.is_consistent() {
            return Err(TaskFailure::execution(
                "the task's return value holds an array whose data does not fill its shape",
            ));
        }
        Ok(Self {
            value,
            result: to_json(result)?,
            text: None,
            notes: Vec::new(),
            output_lines: Vec::new(),
        })
    }

    /// What a command line prints in place of the value's JSON.
    #[must_use]
    pub fn with_text(mut self, text: impl Into<String>) -> Self {
        self.text = Some(text.into());
        self
    }

    /// A line a command line writes to stderr, apart from its output; a
    /// `--json-run` reply carries the result instead.
    #[must_use]
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.notes.push(note.into());
        self
    }

    /// Add a task output line to stderr, including for JSON-run requests.
    /// This is for tasks whose result is deliberately displayed as text
    /// while their CASA return value remains `None`.
    #[must_use]
    pub fn with_output_line(mut self, line: impl Into<String>) -> Self {
        self.output_lines.push(line.into());
        self
    }

    /// Replay an INFO message with its original time through the log sinks.
    /// If `fallback_to_text` is true and no INFO sink is enabled, display a
    /// timestamped line on stderr instead. The CASA return value is unchanged.
    #[must_use]
    pub fn with_replayed_info(
        self,
        time_mjd_seconds: f64,
        location: &str,
        message: &str,
        fallback_to_text: bool,
    ) -> Self {
        match casa_tables::log_table::replay_info_record(
            time_mjd_seconds,
            location,
            message,
            fallback_to_text,
        ) {
            Some(line) => self.with_output_line(line),
            None => self,
        }
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

    /// The task caught an interrupt and stopped, publishing nothing; the
    /// process exits 130, a shell's status for an interrupted command.
    pub fn interrupted(message: impl Into<String>) -> Self {
        Self {
            kind: TaskFailureKind::Interrupted,
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
    argument_text: Option<&'a BTreeMap<String, String>>,
    default_stderr_logs: bool,
    native: Option<&'a crate::NativeTaskOptions>,
}

impl<'a> TaskInvocation<'a> {
    /// A run of `session` in `workspace`, as the front end hands its
    /// callback one; a binary's tests build one to drive the callback.
    pub fn new(session: &'a ParameterSession, workspace: &'a Path) -> Self {
        Self {
            session,
            workspace,
            argument_text: None,
            default_stderr_logs: false,
            native: None,
        }
    }

    pub(crate) fn in_process(
        session: &'a ParameterSession,
        workspace: &'a Path,
        argument_text: &'a BTreeMap<String, String>,
        options: &'a crate::NativeTaskOptions,
    ) -> Self {
        Self {
            session,
            workspace,
            argument_text: Some(argument_text),
            default_stderr_logs: options.default_logs,
            native: Some(options),
        }
    }

    /// Carry the invocation's workspace and execution options into a subtask
    /// with its own resolved parameters.
    #[cfg(feature = "calibration")]
    pub(crate) fn with_session<'b>(&'b self, session: &'b ParameterSession) -> TaskInvocation<'b> {
        TaskInvocation {
            native: self.native,
            ..TaskInvocation::new(session, self.workspace)
        }
    }

    #[cfg(any(
        feature = "ms",
        feature = "imaging",
        feature = "images",
        feature = "vla"
    ))]
    pub(crate) fn native_options(&self) -> Option<&crate::NativeTaskOptions> {
        self.native
    }

    /// Resolve a task-relative path for opening or creating a file. CLI runs
    /// already enter the workspace before execution.
    pub(crate) fn io_path(&self, path: impl AsRef<Path>) -> PathBuf {
        let path = path.as_ref();
        if self.native.is_some() && !path.as_os_str().is_empty() {
            self.workspace.join(path)
        } else {
            path.to_path_buf()
        }
    }

    /// Whether the caller left logging output unconfigured so tasks that mirror
    /// CASA's default terminal diagnostics may write them to stderr.
    #[must_use]
    pub fn default_stderr_logs(&self) -> bool {
        self.default_stderr_logs
    }

    #[cfg(feature = "calibration")]
    pub(crate) fn write_default_log_line(&self, text: &str) {
        if let Some(options) = self.native {
            if let Some(write) = &options.log_line {
                write(text);
            }
        } else {
            eprintln!("{text}");
        }
    }

    /// The surface to run.
    pub fn surface(&self) -> &str {
        self.session.bundle().surface.id()
    }

    /// The resolved session, with each value's origin.
    pub fn session(&self) -> &ParameterSession {
        self.session
    }

    /// How the calling front end represents a value in a recorded command.
    pub fn argument_text(&self, name: &str) -> Option<&str> {
        self.argument_text?.get(name).map(String::as_str)
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

    #[cfg(any(feature = "simulation", feature = "imaging"))]
    pub(crate) fn resolve_file_values(&self, values: &mut Map<String, Value>, names: &[&str]) {
        if self.native.is_none() {
            return;
        }
        for name in names {
            if let Some(value) = values.get_mut(*name) {
                match value {
                    Value::Array(items) => {
                        for item in items {
                            self.resolve_file_value(item);
                        }
                    }
                    _ => self.resolve_file_value(value),
                }
            }
        }
    }

    #[cfg(any(feature = "simulation", feature = "imaging"))]
    fn resolve_file_value(&self, value: &mut Value) {
        if let Value::String(path) = value
            && !matches!(path.as_str(), "" | "none" | "default")
        {
            *path = self.io_path(path.as_str()).to_string_lossy().into_owned();
        }
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
    contract: ContractDocument,
    protocol: Value,
    surfaces: Vec<SurfaceContractBundle>,
    execute: F,
}

enum ContractDocument {
    Eager(Value),
    Deferred {
        contract: Box<TaskProviderContract>,
        value: OnceLock<Result<Value, String>>,
    },
    Compiled {
        document: &'static str,
        value: OnceLock<Result<Value, String>>,
    },
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
    pub fn new(
        program: impl Into<String>,
        contract: TaskProviderContract,
        surfaces: &[&str],
        execute: F,
    ) -> Result<Self, String> {
        Self::construct(program, contract, surfaces, execute, |contract| {
            serde_json::to_value(&contract)
                .map(ContractDocument::Eager)
                .map_err(|error| error.to_string())
        })
    }

    /// Construct a front end for a standard task contract, deferring its
    /// machine-readable schema document until it is requested.
    ///
    /// Contract validation and a serialization check still run immediately;
    /// task parameter validation is unchanged. The concrete standard contract
    /// contains only owned, deterministic values, so later schema rendering
    /// preserves the output of [`Self::new`].
    ///
    /// # Errors
    ///
    /// Returns the same contract, surface or serialization errors as
    /// [`Self::new`].
    pub fn new_with_deferred_schema(
        program: impl Into<String>,
        contract: TaskProviderContract,
        surfaces: &[&str],
        execute: F,
    ) -> Result<Self, String> {
        Self::construct(program, contract, surfaces, execute, |contract| {
            serde_json::to_writer(std::io::sink(), &contract).map_err(|error| error.to_string())?;
            Ok(ContractDocument::Deferred {
                contract: Box::new(contract),
                value: OnceLock::new(),
            })
        })
    }

    /// Construct a front end from a provider contract validated at build time.
    ///
    /// `document` must be the JSON projection of the validated provider after
    /// its front-end schema is installed. `hosted` must contain the requested
    /// surfaces from that same provider. Callers must use the ordinary
    /// constructor when build and target metadata may differ. User-provided
    /// parameters are still resolved and validated on every invocation.
    ///
    /// # Errors
    ///
    /// Returns an error if no surface is hosted or the protocol cannot be
    /// serialized.
    #[doc(hidden)]
    pub fn new_with_compiled_contract(
        program: impl Into<String>,
        protocol: ProviderProtocolDescriptor,
        hosted: Vec<SurfaceContractBundle>,
        document: &'static str,
        execute: F,
    ) -> Result<Self, String> {
        if hosted.is_empty() {
            return Err("a task front end hosts at least one surface".to_string());
        }
        Ok(Self {
            program: program.into(),
            contract: ContractDocument::Compiled {
                document,
                value: OnceLock::new(),
            },
            protocol: serde_json::to_value(protocol).map_err(|error| error.to_string())?,
            surfaces: hosted,
            execute,
        })
    }

    fn construct(
        program: impl Into<String>,
        mut contract: TaskProviderContract,
        surfaces: &[&str],
        execute: F,
        document: impl FnOnce(TaskProviderContract) -> Result<ContractDocument, String>,
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
        let contract = document(contract)?;
        Ok(Self {
            program: program.into(),
            contract,
            protocol,
            surfaces: hosted,
            execute,
        })
    }

    /// Parse `args` (with CLI logging flags already stripped) and dispatch one
    /// invocation, calling `before_run` before `Run` and `JsonRun` actions.
    ///
    /// An action that runs nothing skips `before_run`, so a logging
    /// configuration error only refuses a run. A command-line run enters its
    /// workspace via `enter_workspace` before starting the task callback.
    pub fn dispatch_for_cli<E: std::fmt::Display>(
        &self,
        args: &[OsString],
        enter_workspace: EnterWorkspace,
        default_stderr_logs: bool,
        before_run: impl FnOnce() -> Result<(), E>,
    ) -> FrontEndOutput {
        let (bundle, action, rest) = match self.parse(args) {
            Ok(parsed) => parsed,
            Err(refused) => return refused,
        };
        let enter = Some(enter_workspace);
        if !matches!(action, Action::Run | Action::JsonRun(_)) {
            return self.perform(bundle, action, &rest, enter, false);
        }
        if let Err(error) = before_run() {
            let message = format!("failed to initialize logging: {error}");
            return match action {
                Action::JsonRun(_) => reply_output(&TaskReply::Failed {
                    surface: bundle.surface.id().to_string(),
                    kind: TaskFailureKind::Validation,
                    message,
                    parameter: None,
                }),
                _ => FrontEndOutput::refused(message),
            };
        }
        self.perform(bundle, action, &rest, enter, default_stderr_logs)
    }

    /// Handle one command line, without the program name. It changes no
    /// process state: a run gets its workspace but does not enter it, as
    /// [`Self::dispatch_for_cli`] does.
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
            Ok((bundle, action, rest)) => self.perform(bundle, action, &rest, enter, false),
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
        default_stderr_logs: bool,
    ) -> FrontEndOutput {
        match action {
            Action::Help => {
                FrontEndOutput::print(help::render_help(&self.program, &self.surfaces, bundle))
            }
            Action::JsonSchema => match &self.contract {
                ContractDocument::Eager(value) => pretty(value),
                ContractDocument::Deferred { contract, value } => {
                    match value.get_or_init(|| {
                        serde_json::to_value(contract.as_ref()).map_err(|error| error.to_string())
                    }) {
                        Ok(value) => pretty(value),
                        Err(error) => FrontEndOutput::error(
                            EXIT_EXECUTION_FAILED,
                            format!("failed to serialize the task schema: {error}"),
                        ),
                    }
                }
                ContractDocument::Compiled { document, value } => {
                    match value.get_or_init(|| {
                        serde_json::from_str(document).map_err(|error| error.to_string())
                    }) {
                        Ok(value) => pretty(value),
                        Err(error) => FrontEndOutput::error(
                            EXIT_EXECUTION_FAILED,
                            format!("failed to deserialize the task schema: {error}"),
                        ),
                    }
                }
            },
            Action::ProtocolInfo => pretty(&self.protocol),
            Action::JsonRun(source) => self.run_json(bundle, &source, default_stderr_logs),
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
            Action::Run => self.run_command_line(bundle, rest, enter, default_stderr_logs),
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
        default_stderr_logs: bool,
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
        // From here on the run is in its workspace, as a Python call's is.
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
        let outcome = self.execute(&TaskInvocation {
            session: &session,
            workspace: &workspace,
            argument_text: None,
            default_stderr_logs,
            native: None,
        });
        last.after_completion(&attempt, outcome.is_ok());
        stderr.extend(last.take_warnings().into_iter().map(|warning| {
            format!("Warning: could not save Last Successful for {surface}: {warning}")
        }));
        match outcome {
            Ok(success) => {
                stderr.extend(success.notes);
                stderr.extend(success.output_lines);
                let stdout = match success.text {
                    Some(text) => text,
                    None if success.value == CasaValue::None => String::new(),
                    None => format!("{:#}\n", success.value.to_plain_json()),
                };
                FrontEndOutput {
                    stdout,
                    stderr,
                    code: EXIT_SUCCEEDED,
                }
            }
            Err(failure) => {
                stderr.push(format!("Error: {}", command_line_message(&failure)));
                FrontEndOutput {
                    stdout: String::new(),
                    stderr,
                    code: failure.kind.exit_code(),
                }
            }
        }
    }

    fn run_json(
        &self,
        default: &SurfaceContractBundle,
        source: &str,
        default_stderr_logs: bool,
    ) -> FrontEndOutput {
        let mut lines = Vec::new();
        let reply = self.json_reply(default, source, default_stderr_logs, &mut lines);
        let mut output = reply_output(&reply);
        output.stderr.extend(lines);
        output
    }

    fn json_reply(
        &self,
        default: &SurfaceContractBundle,
        source: &str,
        default_stderr_logs: bool,
        output_lines: &mut Vec<String>,
    ) -> TaskReply {
        let refuse =
            |surface: &str, message: String, parameter: Option<String>| TaskReply::Failed {
                surface: surface.to_string(),
                kind: TaskFailureKind::Validation,
                message,
                parameter,
            };
        let (request, text) = match read_task_request(source).and_then(|text| {
            serde_json::from_str::<TaskRunRequest>(&text)
                .map(|request| (request, text))
                .map_err(crate::runtime::TaskCliError::ParseRequest)
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
            let is_variant = bundle
                .catalog
                .concept(&binding.concept)
                .is_some_and(|concept| has_variant_domain(&concept.value_domain));
            let variant_value = if is_variant {
                #[derive(Deserialize)]
                struct RawValues<'a> {
                    #[serde(borrow, default)]
                    values: BTreeMap<String, &'a RawValue>,
                }
                let parsed = serde_json::from_str::<RawValues<'_>>(&text)
                    .ok()
                    .and_then(|raw| {
                        raw.values.get(name).map(|raw| {
                            crate::runtime::casa_type_diagnostics::parse_variant_json(raw.get())
                        })
                    });
                match parsed {
                    Some(Ok(value)) => Some(value),
                    Some(Err(error)) => {
                        return refuse(
                            &request.surface,
                            format!("parameter {name:?}: {error}"),
                            Some(name.clone()),
                        );
                    }
                    None => {
                        return refuse(
                            &request.surface,
                            format!("parameter {name:?} is missing from the request"),
                            Some(name.clone()),
                        );
                    }
                }
            } else {
                None
            };
            // serde_json represents an out-of-range JSON integer as a float.
            // Inspect its original spelling only on this error path: CASA
            // accepts the integer, so it must not get a CASA cInt type error.
            if value
                .as_f64()
                .is_some_and(|number| number.abs() >= 9_223_372_036_854_775_808.0)
                && crate::runtime::casa_type_diagnostics::integer_parameter(&request.surface, name)
            {
                #[derive(Deserialize)]
                struct RawValues<'a> {
                    #[serde(borrow, default)]
                    values: BTreeMap<String, &'a RawValue>,
                }
                if let Ok(raw) = serde_json::from_str::<RawValues<'_>>(&text)
                    && raw.values.get(name).is_some_and(|raw| {
                        crate::runtime::casa_type_diagnostics::integer_literal_outside_i64(
                            raw.get(),
                        )
                    })
                {
                    return refuse(
                        &request.surface,
                        format!(
                            "parameter {name:?}: integer is outside casa-rs's signed 64-bit range"
                        ),
                        Some(name.clone()),
                    );
                }
            }
            let Some(value) = variant_value.or_else(|| ParameterValue::from_plain_json(value))
            else {
                return refuse(
                    &request.surface,
                    format!("parameter {name:?} is null; leave it out to take its default"),
                    Some(name.clone()),
                );
            };
            // Checked here, so that a value the surface refuses is named.
            if let Err(error) = crate::runtime::cli::checked_value(bundle, binding, &value) {
                return refuse(&request.surface, error, Some(name.clone()));
            }
            overrides.values.insert(name.clone(), value);
        }
        if let Err((name, message)) = apply_typed_values(bundle, &request, &mut overrides) {
            return refuse(&request.surface, message, Some(name));
        }
        drop(text);
        let workspace = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
        let session = match open_named_session(OpenSessionRequest {
            bundle: bundle.clone(),
            workspace: workspace.clone(),
            source: BaseSource::Defaults,
            profile_text: None,
            context_patch: ResolutionPatch::default(),
            override_patch: overrides,
            managed_save: false,
        }) {
            Ok(session) => session,
            Err((message, parameter)) => return refuse(&request.surface, message, parameter),
        };
        if let Some(diagnostic) = first_error(&session) {
            return refuse(
                &request.surface,
                diagnostic.message.clone(),
                diagnostic.parameter.clone(),
            );
        }
        let invocation = TaskInvocation {
            session: &session,
            workspace: &workspace,
            argument_text: Some(&request.argument_text),
            default_stderr_logs,
            native: None,
        };
        let (reply, lines) = task_reply(&request.surface, self.execute(&invocation));
        output_lines.extend(lines);
        reply
    }

    fn execute(&self, invocation: &TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure> {
        observe_task(invocation, &self.execute)
    }
}

pub(crate) fn task_reply(
    surface: &str,
    outcome: Result<TaskSuccess, TaskFailure>,
) -> (TaskReply, Vec<String>) {
    let surface = surface.to_owned();
    match outcome {
        Ok(success) => (
            TaskReply::Succeeded {
                surface,
                value: success.value,
                result: success.result,
            },
            success.output_lines,
        ),
        Err(failure) => (
            TaskReply::Failed {
                surface,
                kind: failure.kind,
                message: failure.message,
                parameter: failure.parameter,
            },
            Vec::new(),
        ),
    }
}

pub(crate) fn observe_task(
    invocation: &TaskInvocation<'_>,
    execute: impl FnOnce(&TaskInvocation<'_>) -> Result<TaskSuccess, TaskFailure>,
) -> Result<TaskSuccess, TaskFailure> {
    let surface = invocation.surface();
    tracing::info!(task = surface, "{surface} started");
    let outcome = execute(invocation);
    match &outcome {
        Ok(_) => tracing::info!(task = surface, "{surface} completed"),
        Err(failure) if failure.kind == TaskFailureKind::Interrupted => tracing::warn!(
            task = surface,
            reason = %failure.message,
            "{surface} interrupted"
        ),
        Err(failure) => tracing::error!(
            casa.priority = "SEVERE",
            task = surface,
            error = %failure.message,
            "{surface} failed"
        ),
    }
    outcome
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
        if raw.is_some_and(|flag| crate::runtime::cli::surface_declares_flag(bundle, flag)) {
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
    open_named_session(request).map_err(|(message, _)| message)
}

/// The session `request` opens, or its refusal and the parameter the
/// refusal names, if any.
fn apply_typed_values(
    bundle: &SurfaceContractBundle,
    request: &TaskRunRequest,
    overrides: &mut ResolutionPatch,
) -> Result<(), (String, String)> {
    for (name, value) in &request.typed_values {
        let error = |message| (name.clone(), message);
        if request.values.contains_key(name) {
            return Err(error(format!(
                "parameter {name:?} occurs in both value maps"
            )));
        }
        let binding = bundle
            .surface
            .bindings()
            .iter()
            .find(|binding| binding.name == *name)
            .ok_or_else(|| error(format!("unknown {} parameter {name:?}", request.surface)))?;
        if !crate::runtime::invocation::preserves_model_task_boolean(&request.surface, name, value)
            && !bundle
                .catalog
                .concept(&binding.concept)
                .is_some_and(|concept| has_variant_domain(&concept.value_domain))
        {
            return Err(error(format!(
                "typed parameter {name:?} must use a variant domain"
            )));
        }
        crate::runtime::cli::checked_value(bundle, binding, value).map_err(error)?;
        overrides.values.insert(name.clone(), value.clone());
    }
    Ok(())
}

fn has_variant_domain(domain: &casa_contracts::ParameterType) -> bool {
    use casa_contracts::ParameterType;
    match domain {
        ParameterType::Variant => true,
        ParameterType::Optional { value, .. } => has_variant_domain(value),
        _ => false,
    }
}

pub(crate) fn open_named_session(
    request: OpenSessionRequest,
) -> Result<ParameterSession, (String, Option<String>)> {
    ParameterRuntime::default()
        .open_session(request)
        .map_err(|error| {
            let parameter = match &error {
                ParameterRuntimeError::Profile(ProfileError::Diagnostics(diagnostics))
                | ParameterRuntimeError::Session(ParameterSessionError::Profile(
                    ProfileError::Diagnostics(diagnostics),
                )) => diagnostics
                    .iter()
                    .find_map(|diagnostic| diagnostic.parameter.clone()),
                _ => None,
            };
            let message = match error {
                ParameterRuntimeError::Session(error) => format_session_error(error),
                ParameterRuntimeError::Profile(error) => format_profile_error(error),
                other => other.to_string(),
            };
            (message, parameter)
        })
}

pub(crate) fn first_error(session: &ParameterSession) -> Option<&crate::runtime::Diagnostic> {
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
/// `failure`'s message as a command line prints it: naming the parameter at
/// fault when the message does not, as a reply's `parameter` field does.
fn command_line_message(failure: &TaskFailure) -> String {
    match &failure.parameter {
        Some(name) if !failure.message.contains(name.as_str()) => {
            format!("parameter {name:?}: {}", failure.message)
        }
        _ => failure.message.clone(),
    }
}

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

#[cfg(test)]
mod tests;
